package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Virace/RiotManifestGo/internal/fswriter"
	"github.com/Virace/RiotManifestGo/internal/netpool"
	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// netBundleFetcherAdapter 将 *netpool.BundleClient 适配为 BundleFetcher 接口。
// 负责 core.ByteRange ↔ netpool.ByteRange 的类型转换。
type netBundleFetcherAdapter struct {
	client *netpool.BundleClient
}

func (a *netBundleFetcherAdapter) FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange) ([][]byte, error) {
	npRanges := make([]netpool.ByteRange, len(ranges))
	for i, r := range ranges {
		npRanges[i] = netpool.ByteRange{Start: r.Start, End: r.End}
	}
	return a.client.FetchRanges(ctx, bundleFilename, npRanges)
}

func (a *netBundleFetcherAdapter) Close() {
	a.client.Close()
}

// DownloadConfig 是 Downloader 的配置参数。
type DownloadConfig struct {
	// CDNBaseURL CDN Bundle 文件的基础 URL
	CDNBaseURL string

	// OutputDir 输出目录
	OutputDir string

	// Workers 并发下载 Worker 数量（默认 16）
	Workers int

	// MaxFileHandles LRU 文件句柄池上限（默认 500）
	MaxFileHandles int

	// MaxRangesPerReq 单 HTTP Range 请求的最大非连续段数（默认 30）
	MaxRangesPerReq int

	// GapTolerance Range 合并间隙容忍阈值（默认 32KB）
	GapTolerance uint32

	// MaxRetries 单个 BundleJob 失败时的最大重试次数（默认 3；负数表示不重试）
	MaxRetries int

	// RetryWait 重试指数退避的基础等待时长（默认 1s）：第 N 次重试前等待
	// RetryWait × 2^(N-1)，单次等待封顶 maxRetryWait 与 RetryWait 中的较大者。
	// CDN 冷对象回源（暂态 404）场景可调大该值拉长重试窗口。
	RetryWait time.Duration
}

// Downloader 是下载管线的顶层协调器。
type Downloader struct {
	config   DownloadConfig
	events   chan DownloadEvent
	client   BundleFetcher
	filePool *fswriter.FilePool
	decoder  *zstream.Decoder

	bytesDownloaded atomic.Int64
	chunksDone      atomic.Int32
}

// NewDownloader 创建一个新的下载协调器。
func NewDownloader(config DownloadConfig) *Downloader {
	client := &netBundleFetcherAdapter{
		client: netpool.NewBundleClient(config.CDNBaseURL, config.Workers),
	}
	return newDownloader(config, client)
}

// NewDownloaderWithFetcher 创建一个使用自定义 BundleFetcher 的下载协调器。
// 主要用于测试时注入 mock 实现。
func NewDownloaderWithFetcher(config DownloadConfig, fetcher BundleFetcher) *Downloader {
	return newDownloader(config, fetcher)
}

func newDownloader(config DownloadConfig, fetcher BundleFetcher) *Downloader {
	if config.Workers <= 0 {
		config.Workers = 16
	}
	if config.MaxFileHandles <= 0 {
		config.MaxFileHandles = 500
	}
	if config.MaxRangesPerReq <= 0 {
		config.MaxRangesPerReq = 30
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryWait <= 0 {
		config.RetryWait = time.Second
	}

	return &Downloader{
		config:   config,
		events:   make(chan DownloadEvent, config.Workers*4),
		client:   fetcher,
		filePool: fswriter.NewFilePool(config.MaxFileHandles),
		decoder:  zstream.NewDecoder(),
	}
}

// Events 返回只读事件 channel，在 Download() 完成后关闭。
// 事件用于进度展示；若消费者跟不上或未消费，下载流程会丢弃事件而不是阻塞。
func (d *Downloader) Events() <-chan DownloadEvent {
	return d.events
}

func (d *Downloader) emit(event DownloadEvent) {
	select {
	case d.events <- event:
	default:
	}
}

// Emit 向 Downloader 的事件 channel 发出一个事件，供包外的上层编排逻辑
// （如 pkg/update.Sync）复用同一条事件流，与下载管线自身发出的事件交织在一起，
// 便于消费方（CLI）只订阅一个 Events() channel 就能拿到完整的进度信息。
// 语义与内部 emit 完全一致：channel 已满时丢弃事件而不阻塞。
func (d *Downloader) Emit(event DownloadEvent) {
	d.emit(event)
}

// Download 执行完整的下载流程：预分配 staging → Mapper → DownloadTasks（自管 staging）。
//
// 接收完整文件列表，内部据此下载全部 Chunk 并覆盖 outputDir 下的最终文件。写入
// 一律先落 staging 文件，全部作业成功后才原子替换旧文件；中途失败时旧文件保持完整。
func (d *Downloader) Download(ctx context.Context, files []rman.FileEntry) error {
	defer close(d.events)
	defer d.filePool.Close()
	defer d.client.Close()

	startTime := time.Now()

	// 1. 文件预分配（落 StagingPath，旧文件在提交前不受影响）
	if err := d.preallocateFiles(ctx, files); err != nil {
		return fmt.Errorf("文件预分配失败: %w", err)
	}

	// 2. Mapper
	taskMap := Map(files)

	// 统计总量（供下方 EventComplete 使用；DownloadTasks 内部不感知“完整文件列表”
	// 这一概念，因此总量统计留在 Download 这一层）
	var totalChunks int
	var totalBytes int64
	for _, tasks := range taskMap {
		for _, task := range tasks {
			totalChunks++
			totalBytes += int64(task.CompressedSize)
		}
	}

	// 3. Scheduler + Worker Pool + staging 提交/丢弃，全部收敛到 DownloadTasks
	failed, err := d.DownloadTasks(ctx, taskMap, TaskOptions{ManageStaging: true})

	// 4. 完成事件
	d.emit(EventComplete{
		TotalFiles:  len(files),
		TotalChunks: totalChunks,
		TotalBytes:  totalBytes,
		FailedFiles: len(failed),
		Elapsed:     time.Since(startTime),
	})

	return err
}

// TaskOptions 控制 DownloadTasks 对 staging 生命周期的管理范围。
type TaskOptions struct {
	// ManageStaging 为 true 时，DownloadTasks 在全部作业处理完毕后自行提交或丢弃
	// 涉及的 staging 文件：非失败文件 ClosePath+CommitStaging（原子替换为最终文件，
	// 发 EventFileRenamed），失败文件 ClosePath+DiscardStaging（保留旧文件）。
	// staging 文件本身的预分配（创建/Truncate）不属于这一步，由调用方在此之前完成
	// （Download 通过 preallocateFiles 承担；增量更新编排器按需自行预分配）。
	//
	// ManageStaging 为 false 时，DownloadTasks 只对已经存在的 StagingPath(FilePath)
	// 执行写入，既不预分配也不提交/丢弃——staging 的创建、提交、丢弃全部归调用方
	// （增量更新编排器）负责，便于其把 Chunk 级复用写入与网络下载写入合并到同一份
	// staging 文件后再统一提交。
	ManageStaging bool
}

// DownloadTasks 执行一个预构建任务集（taskMap，通常是 Map 的输出或其子集）的
// 下载→解压→写盘流程，是 Download 与增量更新编排器共用的下载执行入口。
//
// 与 Download 不同，DownloadTasks 不做 Mapper：调用方决定 taskMap 覆盖哪些 Chunk，
// 使得增量更新场景可以只灌入本地验证 miss 的 Chunk 子集，Schedule 阶段的 Gap
// Tolerance 合并、30 段上限拆分、Bundle 级重试完全复用，不需要重新实现。
//
// 写入统一落 StagingPath(FilePath)：无论 ManageStaging 取值，处理过程中都不会
// 触碰最终路径，保证旧文件字节在提交前保持不变。
//
// failed 是关联到失败作业（重试耗尽）的目标文件 FilePath 集合：一个 BundleJob 内
// 任意 Chunk 的处理最终失败，都会导致该 Job 覆盖的全部 Targets.FilePath 计入
// failed，即便同一文件的其他 Chunk 所在 Job 已成功——一个文件只要有一个 Chunk
// 未确认写入完整，就不能判定它可信，不能提交为最终文件。
func (d *Downloader) DownloadTasks(ctx context.Context, taskMap map[uint64][]GlobalChunkTask, opts TaskOptions) (map[string]bool, error) {
	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq: d.config.MaxRangesPerReq,
		GapTolerance:    d.config.GapTolerance,
	})

	jobCh := make(chan BundleJob, len(jobs))
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	failed := make(map[string]bool)
	var failedMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < d.config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.taskWorker(ctx, jobCh, &failedMu, failed)
		}()
	}
	wg.Wait()

	if ctx.Err() != nil {
		// Context 取消时 jobCh 中可能仍有未处理的 Job（worker 提前退出），无法确认
		// 这些 Job 对应的 staging 是否完整，因此不做任何提交/丢弃，保持旧文件不变，
		// 交由调用方决定后续处理。
		return failed, ctx.Err()
	}

	if opts.ManageStaging {
		if err := d.finalizeStaging(taskMap, failed); err != nil {
			return failed, err
		}
	} else {
		// 调用方（如增量更新编排器）接下来可能要立即对这些 staging 路径执行
		// rename/remove：Windows 下这要求句柄已释放，因此即便不提交/不丢弃，
		// 也必须在返回前关闭这批任务涉及的全部 staging 句柄。
		if err := d.closeStagingHandles(taskMap); err != nil {
			return failed, err
		}
	}

	if len(failed) > 0 {
		return failed, fmt.Errorf("下载完成但有 %d 个目标文件关联的 Bundle 失败", len(failed))
	}
	return failed, nil
}

// finalizeStaging 在全部作业处理完毕后，对 taskMap 涉及的每个目标文件提交或丢弃
// 对应的 staging 文件：未失败的文件 ClosePath+CommitStaging（用 staging 内容原子
// 替换目标文件，发 EventFileRenamed）；失败的文件 ClosePath+DiscardStaging（保留
// 旧文件，删除半成品 staging）。ClosePath 必须先于 rename/remove 调用——Windows
// 下无法对仍持有打开句柄的文件执行这两个操作。
func (d *Downloader) finalizeStaging(taskMap map[uint64][]GlobalChunkTask, failed map[string]bool) error {
	for _, filePath := range taskMapFilePaths(taskMap) {
		fullPath, err := OutputPath(d.config.OutputDir, filePath)
		if err != nil {
			return err
		}
		stagingPath := fswriter.StagingPath(fullPath)

		if err := d.filePool.ClosePath(stagingPath); err != nil {
			return fmt.Errorf("关闭暂存文件句柄失败 %s: %w", stagingPath, err)
		}

		if failed[filePath] {
			if err := fswriter.DiscardStaging(fullPath); err != nil {
				return fmt.Errorf("丢弃暂存文件失败 %s: %w", fullPath, err)
			}
			continue
		}

		if err := fswriter.CommitStaging(fullPath); err != nil {
			return fmt.Errorf("提交暂存文件失败 %s: %w", fullPath, err)
		}
		d.emit(EventFileRenamed{From: stagingPath, To: fullPath})
	}
	return nil
}

// closeStagingHandles 关闭 taskMap 涉及的全部目标文件在 FilePool 中残留的 staging
// 句柄，既不提交也不丢弃——用于 ManageStaging=false 场景：staging 的提交/丢弃归调用方
// 负责，但 FilePool 是 Downloader 私有的，调用方无法自行关闭 DownloadTasks 内部
// WriteAt 时打开的句柄，只能由 DownloadTasks 自己在返回前释放。路径从未被 FilePool
// 打开过时 ClosePath 是安全的空操作（如 Gap Tolerance 之外、CompressedSize=0 被
// mergeRanges 过滤掉、从未触发 WriteAt 的目标文件）。
func (d *Downloader) closeStagingHandles(taskMap map[uint64][]GlobalChunkTask) error {
	for _, filePath := range taskMapFilePaths(taskMap) {
		fullPath, err := OutputPath(d.config.OutputDir, filePath)
		if err != nil {
			return err
		}
		stagingPath := fswriter.StagingPath(fullPath)
		if err := d.filePool.ClosePath(stagingPath); err != nil {
			return fmt.Errorf("关闭暂存文件句柄失败 %s: %w", stagingPath, err)
		}
	}
	return nil
}

// taskMapFilePaths 返回 taskMap 涉及的全部目标文件 FilePath（去重，即
// WriteTarget.FilePath，清单相对路径而非磁盘绝对路径），供 finalizeStaging 与
// closeStagingHandles 共用同一套遍历/去重逻辑。
func taskMapFilePaths(taskMap map[uint64][]GlobalChunkTask) []string {
	seen := make(map[string]bool)
	paths := make([]string, 0, len(taskMap))
	for _, tasks := range taskMap {
		for _, task := range tasks {
			for _, target := range task.Targets {
				if seen[target.FilePath] {
					continue
				}
				seen[target.FilePath] = true
				paths = append(paths, target.FilePath)
			}
		}
	}
	return paths
}

// jobFilePaths 返回 job 覆盖到的全部目标文件的 FilePath（去重，即 WriteTarget.FilePath，
// 清单相对路径而非磁盘绝对路径）。
func jobFilePaths(job BundleJob) map[string]bool {
	paths := make(map[string]bool)
	for _, r := range job.Ranges {
		for _, chunk := range r.Chunks {
			for _, target := range chunk.Targets {
				paths[target.FilePath] = true
			}
		}
	}
	return paths
}

// preallocateFiles 为每个非空文件预分配 staging 空间（创建目录 + Truncate 到目标
// 大小），写入过程中始终只触碰 StagingPath，最终路径下的旧文件保持不变直到提交。
// EventFilePreallocated.Path 报告的是最终路径而非 staging 路径——staging 后缀是
// 实现细节，对事件消费方（进度展示）没有意义。
func (d *Downloader) preallocateFiles(ctx context.Context, files []rman.FileEntry) error {
	for _, f := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if f.FileSize == 0 {
			continue
		}
		fullPath, err := OutputPath(d.config.OutputDir, f.Path)
		if err != nil {
			return err
		}
		if err := d.filePool.PreallocateFile(fswriter.StagingPath(fullPath), int64(f.FileSize)); err != nil {
			return err
		}
		d.emit(EventFilePreallocated{Path: fullPath, Size: int64(f.FileSize)})
	}
	return nil
}

// taskWorker 从 jobCh 获取 BundleJob，带重试执行下载；Job 最终失败时把其覆盖的
// 全部目标文件 FilePath 计入 failed。
func (d *Downloader) taskWorker(
	ctx context.Context,
	jobCh <-chan BundleJob,
	failedMu *sync.Mutex,
	failed map[string]bool,
) {
	for job := range jobCh {
		if ctx.Err() != nil {
			return
		}
		if err := d.processBundleWithRetry(ctx, job); err != nil {
			d.emit(EventError{
				BundleID:       job.BundleID,
				BundleFilename: job.BundleFilename,
				Err:            err,
			})
			failedMu.Lock()
			for path := range jobFilePaths(job) {
				failed[path] = true
			}
			failedMu.Unlock()
		}
	}
}

// maxRetryWait 重试等待时长的默认上限，防止指数增长失控；
// 基础等待被配置得比它还大时以基础等待为准（见 retryWait）。
const maxRetryWait = 60 * time.Second

// retryWait 计算第 attempt 次重试（1-based）前的指数退避等待时长：
// base × 2^(attempt-1)，封顶 maxRetryWait 与 base 中的较大者。
func retryWait(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	limit := maxRetryWait
	if base > limit {
		limit = base
	}
	wait := base
	for i := 1; i < attempt; i++ {
		wait *= 2
		if wait >= limit {
			return limit
		}
	}
	return wait
}

// processBundleWithRetry 带重试策略执行 BundleJob。
//
// Chunk 级重试：单个 BundleJob 失败时只重试该 Job，不回滚全局。
// 指数退避：第 N 次重试前等待 retryWait(RetryWait, N)。
// 不做断点续传：manifest 无文件级校验信息，续传容易导致文件错误。
func (d *Downloader) processBundleWithRetry(ctx context.Context, job BundleJob) error {
	var lastErr error
	maxRetries := d.config.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			wait := retryWait(d.config.RetryWait, attempt)
			d.emit(EventRetry{
				BundleID:       job.BundleID,
				BundleFilename: job.BundleFilename,
				Attempt:        attempt,
				MaxRetries:     maxRetries,
				Wait:           wait,
				Err:            lastErr,
			})
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		lastErr = d.processBundle(ctx, job)
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return fmt.Errorf("经 %d 次重试后仍失败: %w", maxRetries, lastErr)
}

// processBundle 处理单个 BundleJob 的完整下载→解压→写盘流程。
//
// 关键：使用绝对偏移（BundleOffset - RangeStart）读取 Chunk 数据，
// 兼容 Gap Tolerance 产生的间隙字节。
func (d *Downloader) processBundle(ctx context.Context, job BundleJob) error {
	chunkCount := 0
	for _, r := range job.Ranges {
		chunkCount += len(r.Chunks)
	}

	d.emit(EventBundleStart{
		BundleID:       job.BundleID,
		BundleFilename: job.BundleFilename,
		RangeCount:     len(job.Ranges),
		ChunkCount:     chunkCount,
	})

	byteRanges := make([]ByteRange, len(job.Ranges))
	for i, r := range job.Ranges {
		byteRanges[i] = ByteRange{Start: int64(r.Start), End: int64(r.End)}
	}

	rangeDatas, err := d.client.FetchRanges(ctx, job.BundleFilename, byteRanges)
	if err != nil {
		return fmt.Errorf("下载 Bundle %s 失败: %w", job.BundleFilename, err)
	}

	if len(rangeDatas) != len(job.Ranges) {
		return fmt.Errorf("Bundle %s 返回 Range 数不匹配: got=%d want=%d",
			job.BundleFilename, len(rangeDatas), len(job.Ranges))
	}

	for i, rangeData := range rangeDatas {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		chunkRange := job.Ranges[i]
		rangeStart := chunkRange.Start

		for _, task := range chunkRange.Chunks {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// 使用绝对偏移（兼容 Gap Tolerance）
			if task.BundleOffset < rangeStart {
				return fmt.Errorf("Chunk %016X 偏移 %d 小于 Range 起点 %d",
					task.Targets[0].ChunkID, task.BundleOffset, rangeStart)
			}
			chunkOffsetInRange := int(task.BundleOffset - rangeStart)
			compEnd := chunkOffsetInRange + int(task.CompressedSize)
			if compEnd > len(rangeData) {
				return fmt.Errorf("Range 数据截断: Bundle=%s chunkOffset=%d compSize=%d dataLen=%d",
					job.BundleFilename, chunkOffsetInRange, task.CompressedSize, len(rangeData))
			}

			compressed := rangeData[chunkOffsetInRange:compEnd]

			chunkID := task.Targets[0].ChunkID
			hashType := task.Targets[0].HashType
			expectedSize := task.Targets[0].ExpectedLen

			decompressed, err := d.decoder.DecompressAndValidate(
				compressed, expectedSize, chunkID, hashType,
			)
			if err != nil {
				return fmt.Errorf("Chunk %016X 处理失败: %w", chunkID, err)
			}

			for _, target := range task.Targets {
				fullPath, pathErr := OutputPath(d.config.OutputDir, target.FilePath)
				if pathErr != nil {
					return pathErr
				}
				// 写入一律落 staging 路径：无论调用方是 Download 还是增量更新编排器，
				// 都不会在这一步触碰最终文件，保证提交前旧文件字节不受影响。
				stagingPath := fswriter.StagingPath(fullPath)
				if _, writeErr := d.filePool.WriteAt(stagingPath, decompressed, target.FileOffset); writeErr != nil {
					return fmt.Errorf("写入文件 %s 偏移 %d 失败: %w",
						stagingPath, target.FileOffset, writeErr)
				}
			}

			d.bytesDownloaded.Add(int64(task.CompressedSize))
			d.chunksDone.Add(1)

			d.emit(EventChunkDone{
				ChunkID:        chunkID,
				BundleID:       job.BundleID,
				CompressedSize: task.CompressedSize,
				TargetCount:    len(task.Targets),
			})
		}
	}

	d.emit(EventBundleDone{BundleID: job.BundleID})
	return nil
}
