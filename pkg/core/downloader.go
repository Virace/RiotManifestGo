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

	// MaxRetries 单个 BundleJob 失败时的最大重试次数（默认 3）
	MaxRetries int
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
	if config.MaxRetries < 0 {
		config.MaxRetries = 3
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
func (d *Downloader) Events() <-chan DownloadEvent {
	return d.events
}

// Download 执行完整的下载流程。
func (d *Downloader) Download(ctx context.Context, files []rman.FileEntry) error {
	defer close(d.events)
	defer d.filePool.Close()
	defer d.client.Close()

	startTime := time.Now()

	// 1. 文件预分配
	if err := d.preallocateFiles(ctx, files); err != nil {
		return fmt.Errorf("文件预分配失败: %w", err)
	}

	// 2. Mapper
	taskMap := Map(files)

	// 3. Scheduler（含 Gap Tolerance）
	jobs := Schedule(taskMap, ScheduleConfig{
		MaxRangesPerReq: d.config.MaxRangesPerReq,
		GapTolerance:    d.config.GapTolerance,
	})

	// 统计总量
	var totalChunks int
	var totalBytes int64
	for _, tasks := range taskMap {
		for _, task := range tasks {
			totalChunks++
			totalBytes += int64(task.CompressedSize)
		}
	}

	// 4. Worker Pool
	jobCh := make(chan BundleJob, len(jobs))
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	var failedBundles atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < d.config.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.worker(ctx, jobCh, &failedBundles)
		}()
	}

	wg.Wait()

	// 5. 完成事件
	d.events <- EventComplete{
		TotalFiles:    len(files),
		TotalChunks:   totalChunks,
		TotalBytes:    totalBytes,
		FailedBundles: int(failedBundles.Load()),
		Elapsed:       time.Since(startTime),
	}

	if failedBundles.Load() > 0 {
		return fmt.Errorf("下载完成但有 %d 个 Bundle 失败", failedBundles.Load())
	}
	return nil
}

func (d *Downloader) preallocateFiles(ctx context.Context, files []rman.FileEntry) error {
	for _, f := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if f.FileSize == 0 {
			continue
		}
		fullPath, err := outputPath(d.config.OutputDir, f.Path)
		if err != nil {
			return err
		}
		if err := d.filePool.PreallocateFile(fullPath, int64(f.FileSize)); err != nil {
			return err
		}
		d.events <- EventFilePreallocated{Path: fullPath, Size: int64(f.FileSize)}
	}
	return nil
}

// worker 从 jobCh 获取 BundleJob，带重试执行下载。
func (d *Downloader) worker(
	ctx context.Context,
	jobCh <-chan BundleJob,
	failedBundles *atomic.Int32,
) {
	for job := range jobCh {
		if ctx.Err() != nil {
			return
		}
		if err := d.processBundleWithRetry(ctx, job); err != nil {
			failedBundles.Add(1)
			d.events <- EventError{
				BundleID:       job.BundleID,
				BundleFilename: job.BundleFilename,
				Err:            err,
			}
		}
	}
}

// processBundleWithRetry 带重试策略执行 BundleJob。
//
// Chunk 级重试：单个 BundleJob 失败时只重试该 Job，不回滚全局。
// 指数退避：第 N 次重试等待 N 秒。
// 不做断点续传：manifest 无文件级校验信息，续传容易导致文件错误。
func (d *Downloader) processBundleWithRetry(ctx context.Context, job BundleJob) error {
	var lastErr error
	for attempt := 0; attempt <= d.config.MaxRetries; attempt++ {
		if attempt > 0 {
			d.events <- EventRetry{
				BundleID:       job.BundleID,
				BundleFilename: job.BundleFilename,
				Attempt:        attempt,
				MaxRetries:     d.config.MaxRetries,
				Err:            lastErr,
			}
			// 指数退避
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
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
	return fmt.Errorf("经 %d 次重试后仍失败: %w", d.config.MaxRetries, lastErr)
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

	d.events <- EventBundleStart{
		BundleID:       job.BundleID,
		BundleFilename: job.BundleFilename,
		RangeCount:     len(job.Ranges),
		ChunkCount:     chunkCount,
	}

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
				fullPath, pathErr := outputPath(d.config.OutputDir, target.FilePath)
				if pathErr != nil {
					return pathErr
				}
				if _, writeErr := d.filePool.WriteAt(fullPath, decompressed, target.FileOffset); writeErr != nil {
					return fmt.Errorf("写入文件 %s 偏移 %d 失败: %w",
						fullPath, target.FileOffset, writeErr)
				}
			}

			d.bytesDownloaded.Add(int64(task.CompressedSize))
			d.chunksDone.Add(1)

			d.events <- EventChunkDone{
				ChunkID:        chunkID,
				BundleID:       job.BundleID,
				CompressedSize: task.CompressedSize,
				TargetCount:    len(task.Targets),
			}
		}
	}

	d.events <- EventBundleDone{BundleID: job.BundleID}
	return nil
}
