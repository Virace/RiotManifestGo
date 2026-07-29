// manifest-cli 是 RiotManifestGo 的命令行入口。
//
// 用法：
//
//	manifest-cli <manifest文件或URL> [选项]
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Virace/RiotManifestGo/pkg/core"
	"github.com/Virace/RiotManifestGo/pkg/edge"
	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/Virace/RiotManifestGo/pkg/update"
)

const defaultCDNURL = "https://lol.dyn.riotcdn.net/channels/public/bundles"

var (
	version = "dev"
	commit  = "none"
)

func main() {
	outputDir := flag.String("o", "./output", "文件输出目录")
	cdnURL := flag.String("u", defaultCDNURL, "CDN Bundle 基础 URL（逗号分隔可传多个域名，跨域名分摊流量）")
	pattern := flag.String("p", "", "路径筛选（子串或正则）")
	flags := flag.String("f", "", "Flag 过滤，逗号代表与，管道符代表或（如 de_DE,windows 或 ja_JP|ko_KR）")
	workers := flag.Int("w", 16, "并发下载 Worker 数")
	listOnly := flag.Bool("list", false, "仅列出文件，不下载")
	listLimit := flag.Int("n", 20, "列表模式下最大显示数量（-1=全部）")
	logFile := flag.String("log", "", "下载完毕保存完整日志到指定文件")
	maxRetries := flag.Int("retry", 3, "单个 Bundle 下载失败最大重试次数")
	retryWait := flag.Duration("retry-wait", time.Second, "重试指数退避基础等待，第 N 次重试前等待 base×2^(N-1)（单次封顶 60s 与 base 较大者；CDN 冷对象 404 可调大，如 4s）")
	silent := flag.Bool("s", false, "静默模式，仅输出错误")
	verbose := flag.Int("v", 0, "详细输出等级（0=默认进度条, 1=基础滚屏, 2=详细, 3=调试）")
	showVersion := flag.Bool("version", false, "显示程序版本信息并退出")
	install := flag.Bool("install", false, "受管理安装模式：维护 .rman 状态并启用受授权的增量部署与清理")
	updatePath := flag.String("update", "", "旧清单路径，用于受管理安装的增量比对（仅可与 -install 一起使用）")
	repair := flag.Bool("repair", false, "修复模式：逐文件重新校验并补齐本地缺失/损坏的内容，不做文件级跳过")
	verifyOnly := flag.Bool("verify-only", false, "仅校验本地文件完整性，不下载、不写盘")
	noVerify := flag.Bool("no-verify", false, "跳过校验，将全部匹配文件当作全新内容整体下载")
	keepRemoved := flag.Bool("keep-removed", false, "受管理安装时保留旧文件，不清理磁盘（仅可与 -install 一起使用）")
	gapTolerance := flag.Uint("gap-tolerance", uint(core.DefaultGapTolerance), "Range 合并间隙容忍字节数（间隙小于该值的相邻 Chunk 合并为一段，多下少量字节换更少段数）")
	maxRanges := flag.Int("max-ranges", 30, "单个 HTTP Range 请求的最大非连续段数（CDN 超过 30 段返回 400）")
	fullBundleThreshold := flag.Float64("full-bundle-threshold", 0, "整包下载覆盖率阈值（0=禁用；启用建议 0.7：覆盖率达标的 Bundle 改为不带 Range 头的整包 GET，多下浪费字节换 multipart 兼容性）")
	planOnly := flag.Bool("plan-only", false, "仅输出下载计划统计（作业数/整包数/段数/浪费字节/作业尺寸分位），零网络流量")
	edgeFlag := flag.Bool("edge", false, "启用 CDN 边缘 IP 优选（多源 DNS 发现+探测打分，规避劣质节点；稳定性兜底，默认关闭）")
	edgeWinners := flag.Int("edge-winners", 3, "边缘优选保留的节点数")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "RiotManifestGo CLI (%s, commit %s) — Riot 游戏资源清单解析与下载\n\n", version, commit)
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli <manifest文件或URL> [选项]\n\n")
		fmt.Fprintf(os.Stderr, "示例:\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -list\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli https://...releases/XXX.manifest -list -p Aatrox -f ja_JP|ko_KR\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -p \"\\.dll\" -o ./output -log download.log\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli new.manifest -o ./game -install          # 受管理安装（自动发现旧版本）\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli new.manifest -o ./game -install -update old.manifest\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -o ./output -verify-only    # 仅校验本地完整性\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -o ./output -repair         # 修复本地损坏内容\n\n")
		fmt.Fprintf(os.Stderr, "参数:\n")
		flag.PrintDefaults()
	}

	manifestSource, remaining := extractManifestArg(os.Args[1:])
	os.Args = append([]string{os.Args[0]}, remaining...)
	flag.Parse()

	if *showVersion {
		fmt.Printf("RiotManifestGo version %s, commit %s\n", version, commit)
		os.Exit(0)
	}

	updateMode, modeErr := resolveUpdateMode(*repair, *verifyOnly, *noVerify)
	if modeErr != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", modeErr)
		os.Exit(1)
	}
	if err := validateInstallOnlyFlags(*install, *updatePath, *keepRemoved); err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}

	// flag.Uint 底层是平台 uint（64 位平台下宽于 uint32），而 DownloadConfig.GapTolerance
	// 与 ScheduleConfig.GapTolerance 均为 uint32，超限值会静默截断，因此显式校验拒绝。
	if uint64(*gapTolerance) > math.MaxUint32 {
		fmt.Fprintf(os.Stderr, "❌ -gap-tolerance 超出 uint32 范围（最大 %d）: %d\n", uint32(math.MaxUint32), *gapTolerance)
		os.Exit(1)
	}
	gapToleranceU32 := uint32(*gapTolerance)
	cdnURLs := parseCDNURLs(*cdnURL)

	if manifestSource == "" {
		flag.Usage()
		os.Exit(1)
	}

	printer := &outputPrinter{silent: *silent, verbose: *verbose}

	// 1. 解析 Manifest
	printer.info("📄 解析 manifest: %s\n", manifestSource)
	manifest, manifestRaw, srcMeta, err := loadManifest(manifestSource)
	if err != nil {
		log.Fatalf("❌ 解析失败: %v", err)
	}
	for _, line := range manifestInfoLines(manifest, srcMeta) {
		printer.info("%s\n", line)
	}
	printer.info("\n")

	// 2. 过滤
	files := manifest.Files
	if *pattern != "" || *flags != "" {
		files, err = applyFilters(files, *pattern, *flags)
		if err != nil {
			log.Fatalf("❌ 筛选条件无效: %v", err)
		}
		printer.info("🔍 筛选结果: %d 个文件匹配\n", len(files))
	}

	if len(files) == 0 {
		printer.info("⚠️  无匹配文件\n")
		return
	}

	// 3. 列表模式
	if *listOnly {
		printFileList(files, *listLimit)
		return
	}

	// 3.5 plan-only：零网络流量的下载计划统计。与 -p/-f 及全部粒度 flag 组合生效，
	// 计算路径与真实下载共用 core.Map/core.Schedule，不发起任何网络请求。
	if *planOnly {
		taskMap := core.Map(files)
		jobs := core.Schedule(taskMap, core.ScheduleConfig{
			MaxRangesPerReq:     *maxRanges,
			GapTolerance:        gapToleranceU32,
			FullBundleThreshold: *fullBundleThreshold,
			BundleSizes:         manifest.BundleSizes,
		})
		printPlanSummary(printer, summarizePlan(jobs))
		return
	}

	opts := buildSyncOptions(updateMode, *install, *updatePath, *keepRemoved)
	if err := guardStandaloneManagedRoot(*outputDir, opts.Operation, updateMode); err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}

	// 4. 处理计划预览。待下载的 Chunk 集合由 Sync 按新旧清单差异动态决定，可能
	// 远小于 files 全集，因此预览只给出清单声明的总大小，不代表实际下载量。
	var totalDeclaredSize uint64
	for _, f := range files {
		totalDeclaredSize += f.FileSize
	}
	printer.info("📦 处理计划:\n")
	printer.info("   文件数: %d\n", len(files))
	printer.info("   声明总大小: %s\n", humanSize(int64(totalDeclaredSize)))
	printer.info("   操作: %s\n", operationDescription(opts.Operation))
	printer.info("   模式: %s\n", modeDescription(updateMode))
	cdnDisplay := *cdnURL
	if len(cdnURLs) > 0 {
		cdnDisplay = strings.Join(cdnURLs, ", ")
	}
	printer.info("   CDN: %s\n", cdnDisplay)
	printer.info("   Worker 数: %d\n\n", *workers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		printer.info("\n⏹️  收到中断信号，正在优雅退出...\n")
		cancel()
	}()

	// 5. 边缘优选（-edge，默认关闭）：以 BundleSizes 中挑选的探测目标为基准，
	// 对 cdnURLs 的每个域名做候选发现+探测打分，成功则用 selector.DialContext
	// 接管后续 Bundle 请求的拨号；发现/探测全灭、初始化失败均回退系统 DNS，
	// 不中断下载——dialContext 保持 nil 时 DownloadConfig 走 netpool 默认拨号。
	var dialContext func(ctx context.Context, network, addr string) (net.Conn, error)
	if *edgeFlag {
		if probeID, ok := pickProbeBundle(manifest.BundleSizes); ok {
			probeURLs := make([]string, len(cdnURLs))
			for i, u := range cdnURLs {
				probeURLs[i] = u + "/" + core.BundleFilename(probeID)
			}
			var logf func(string, ...any)
			if *verbose >= 2 && !*silent {
				logf = func(f string, a ...any) { fmt.Printf("  [EDGE] "+f+"\n", a...) }
			}
			selector, err := edge.NewSelector(edge.Config{ProbeURLs: probeURLs, Winners: *edgeWinners, Logf: logf})
			if err != nil {
				fmt.Fprintf(os.Stderr, "⚠️  边缘优选初始化失败，回退系统 DNS: %v\n", err)
			} else {
				selector.Start(ctx)
				if ws := selector.Winners(); len(ws) == 0 {
					printer.info("⚠️  边缘优选未发现可用节点，回退系统 DNS\n")
				} else {
					printer.info("🌐 边缘优选启用: %d 个节点 %v\n", len(ws), ws)
					dialContext = selector.DialContext
				}
			}
		}
	}

	dlConfig := core.DownloadConfig{
		CDNBaseURL:          *cdnURL,
		CDNBaseURLs:         cdnURLs,
		OutputDir:           *outputDir,
		Workers:             *workers,
		MaxFileHandles:      500,
		MaxRangesPerReq:     *maxRanges,
		GapTolerance:        gapToleranceU32,
		MaxRetries:          *maxRetries,
		RetryWait:           *retryWait,
		FullBundleThreshold: *fullBundleThreshold,
		BundleSizes:         manifest.BundleSizes,
		DialContext:         dialContext,
	}
	dl := core.NewDownloader(dlConfig)

	dlLog := &downloadLog{startTime: time.Now(), files: files}
	go consumeEvents(dl.Events(), dlLog, printer)

	if updateMode == update.ModeVerifyOnly {
		printer.info("🔍 开始验证...\n")
	} else if opts.Operation == update.OperationInstall {
		printer.info("🚀 开始安装...\n")
	} else {
		printer.info("🚀 开始下载...\n")
	}

	// 心跳 ticker：每 2 秒刷新一次，即使没有新 Chunk 完成也显示 elapsed；
	// ModeVerifyOnly 零下载、零写盘，进度以逐文件验证事件呈现，不需要心跳。
	stopHeartbeat := make(chan struct{})
	if !*silent && *verbose == 0 && updateMode != update.ModeVerifyOnly {
		go heartbeat(dlLog, stopHeartbeat)
	}

	stats, syncErr := update.Sync(ctx, manifest, manifestRaw, manifestSource, *outputDir, files, dl, opts)
	close(stopHeartbeat)

	printer.info("\n")

	elapsed := time.Since(dlLog.startTime)
	if syncErr != nil {
		if ctx.Err() != nil {
			printer.info("⏹️  操作已取消\n")
		} else {
			fmt.Fprintf(os.Stderr, "❌ 操作失败: %v\n", syncErr)
		}
	} else {
		printSyncSummary(printer, opts.Operation, updateMode, stats, elapsed)
	}

	// 保存日志
	if *logFile != "" {
		dlLog.elapsed = elapsed
		if writeErr := saveLog(*logFile, dlLog, stats, opts.Operation, updateMode, files, *verbose, dlConfig); writeErr != nil {
			fmt.Fprintf(os.Stderr, "⚠️  保存日志失败: %v\n", writeErr)
		} else {
			printer.info("📝 日志已保存: %s\n", *logFile)
		}
	}

	if syncErr != nil || len(stats.Failed) > 0 {
		os.Exit(1)
	}
}

// ---- 输出控制 ----

type outputPrinter struct {
	silent  bool
	verbose int
}

func (p *outputPrinter) info(format string, args ...interface{}) {
	if !p.silent {
		fmt.Printf(format, args...)
	}
}

func (p *outputPrinter) detail(format string, args ...interface{}) {
	if !p.silent && p.verbose >= 2 {
		fmt.Printf(format, args...)
	}
}

// ---- 事件消费 ----

type downloadLog struct {
	mu               sync.Mutex
	startTime        time.Time
	elapsed          time.Duration
	files            []rman.FileEntry
	chunksProcessed  int
	bytesDownloaded  int64
	bytesReused      int64
	bytesFetched     int64 // Σ EventBundleDone.FetchedBytes：实际网络流量（含 Range 合并/整包多下的字节）
	bundlesProcessed int
	bundlesFailed    int
	retries          int
	errors           []string
	lastSpeedCheck   time.Time
	lastSpeedBytes   int64
	currentSpeedBps  float64
}

func consumeEvents(events <-chan core.DownloadEvent, dl *downloadLog, printer *outputPrinter) {
	dl.lastSpeedCheck = time.Now()
	lastPrint := time.Now()

	for evt := range events {
		switch e := evt.(type) {
		case core.EventChunkDone:
			dl.mu.Lock()
			dl.chunksProcessed++
			dl.bytesDownloaded += int64(e.CompressedSize)

			now := time.Now()
			elapsed := now.Sub(dl.lastSpeedCheck).Seconds()
			if elapsed >= 0.5 {
				bytesDelta := dl.bytesDownloaded - dl.lastSpeedBytes
				dl.currentSpeedBps = float64(bytesDelta) / elapsed
				dl.lastSpeedCheck = now
				dl.lastSpeedBytes = dl.bytesDownloaded
			}
			dl.mu.Unlock()

			if printer.verbose >= 2 {
				printer.detail("  [CHUNK] %016X 完成 (%s, %d 目标)\n",
					e.ChunkID, humanSize(int64(e.CompressedSize)), e.TargetCount)
			} else if printer.verbose == 0 && !printer.silent && time.Since(lastPrint) > 100*time.Millisecond {
				printProgress(dl)
				lastPrint = time.Now()
			}

		case core.EventBundleStart:
			dl.mu.Lock()
			dl.mu.Unlock()
			if printer.verbose >= 1 {
				fullBundleMark := ""
				if e.FullBundle {
					fullBundleMark = "（整包）"
				}
				printer.info("  [BUNDLE] %s%s 开始 (%d Range, %d Chunk)\n",
					e.BundleFilename, fullBundleMark, e.RangeCount, e.ChunkCount)
			}

		case core.EventBundleDone:
			dl.mu.Lock()
			dl.bundlesProcessed++
			dl.bytesFetched += e.FetchedBytes
			dl.mu.Unlock()
			if printer.verbose >= 1 {
				printer.info("  [BUNDLE] %016X 完成\n", e.BundleID)
			}

		case core.EventRetry:
			dl.mu.Lock()
			dl.retries++
			dl.mu.Unlock()
			fmt.Fprintf(os.Stderr, "  🔄 重试 Bundle %s (%d/%d, 等待 %s): %v\n",
				e.BundleFilename, e.Attempt, e.MaxRetries, e.Wait, e.Err)

		case core.EventError:
			dl.mu.Lock()
			dl.bundlesFailed++
			errMsg := fmt.Sprintf("Bundle %s: %v", e.BundleFilename, e.Err)
			dl.errors = append(dl.errors, errMsg)
			dl.mu.Unlock()
			fmt.Fprintf(os.Stderr, "  ❌ %s\n", errMsg)

		case core.EventFileSkipped:
			if printer.verbose >= 1 {
				printer.info("  [SKIP] %s 内容未变，已跳过\n", e.Path)
			}

		case core.EventChunkReused:
			dl.mu.Lock()
			dl.bytesReused += e.Bytes
			dl.mu.Unlock()
			printer.detail("  [REUSE] %s 本地复用 %s\n", e.Path, humanSize(e.Bytes))

		case core.EventFileRenamed:
			if printer.verbose >= 1 {
				printer.info("  [DONE] %s\n", filepath.Base(e.To))
			}

		case core.EventFileRemoved:
			if printer.verbose >= 1 {
				printer.info("  [REMOVE] %s\n", e.Path)
			}
		}
	}
}

// printProgress 自刷新进度显示（\r 模式）。
func printProgress(dl *downloadLog) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	speedStr := ""
	elapsed := time.Since(dl.startTime)
	if dl.currentSpeedBps > 0 {
		speedStr = fmt.Sprintf(" | %s/s", humanSize(int64(dl.currentSpeedBps)))
	}
	fmt.Printf("\r   📥 下载 %s | 复用 %s | Chunks: %d | Bundles: %d%s | %s    ",
		humanSize(dl.bytesDownloaded), humanSize(dl.bytesReused), dl.chunksProcessed, dl.bundlesProcessed,
		speedStr, elapsed.Round(time.Second))
}

// heartbeat 每 2 秒刷新进度（即使没有新事件），防止看似卡住。
func heartbeat(dl *downloadLog, stop <-chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			printProgress(dl)
		case <-stop:
			return
		}
	}
}

// ---- 远程 Manifest 获取 ----

// manifestSourceMeta 记录清单来源的旁路信息（大小与时间），供启动横幅展示。
// RMAN 格式本身不含任何时间字段，modTime 只能取自来源侧旁证：URL 下载读 CDN
// 的 Last-Modified 响应头，本地文件读文件系统 mtime（通常是下载到本地的时刻）。
// 两者都是推测值，展示时必须标注来源，不能当作清单官方字段。
type manifestSourceMeta struct {
	size    int64
	modTime time.Time // 零值表示未知
	fromURL bool
}

// loadManifest 解析 manifest，同时返回其原始字节（供调用方在需要时存档，
// 写入 update.Archive）与来源旁路信息。
func loadManifest(source string) (*rman.Manifest, []byte, manifestSourceMeta, error) {
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		return loadManifestFromURL(source)
	}
	return loadManifestFromFile(source)
}

func loadManifestFromFile(path string) (*rman.Manifest, []byte, manifestSourceMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, manifestSourceMeta{}, fmt.Errorf("读取 manifest 文件失败: %w", err)
	}
	meta := manifestSourceMeta{size: int64(len(data))}
	if fi, statErr := os.Stat(path); statErr == nil {
		meta.modTime = fi.ModTime()
	}
	manifest, err := rman.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, nil, meta, err
	}
	return manifest, data, meta, nil
}

func loadManifestFromURL(url string) (*rman.Manifest, []byte, manifestSourceMeta, error) {
	client := &http.Client{
		Timeout: 60 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, nil, manifestSourceMeta{}, fmt.Errorf("下载 manifest 失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, manifestSourceMeta{}, fmt.Errorf("下载 manifest 失败: HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, manifestSourceMeta{}, fmt.Errorf("读取 manifest 数据失败: %w", err)
	}

	meta := manifestSourceMeta{size: int64(len(data)), fromURL: true}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, parseErr := http.ParseTime(lm); parseErr == nil {
			meta.modTime = t
		}
	}

	manifest, err := rman.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, nil, meta, err
	}
	return manifest, data, meta, nil
}

// manifestInfoLines 生成启动横幅的清单元信息行。除"清单时间"外的所有信息都
// 直接来自清单本身或其原始字节；时间是推测值，行内附带来源说明，避免被当作
// RMAN 官方字段误读。
func manifestInfoLines(m *rman.Manifest, meta manifestSourceMeta) []string {
	// 全清单聚合：唯一 Chunk/Bundle 计数、声明内容总量、压缩总量。
	// 同一 Chunk 可被多个文件引用，压缩总量按唯一 ChunkID 去重统计。
	uniqueChunks := make(map[uint64]uint32)
	uniqueBundles := make(map[uint64]struct{})
	var contentSize uint64
	filesNoHash := 0
	for i := range m.Files {
		f := &m.Files[i]
		contentSize += f.FileSize
		if len(f.Chunks) > 0 && f.Chunks[0].HashType == rman.HashTypeNone {
			filesNoHash++
		}
		for _, c := range f.Chunks {
			uniqueChunks[c.ChunkID] = c.CompressedSize
			uniqueBundles[c.BundleID] = struct{}{}
		}
	}
	var compressedSize uint64
	for _, size := range uniqueChunks {
		compressedSize += uint64(size)
	}

	lines := []string{fmt.Sprintf("✅ ManifestID: %016X | RMAN v%d.%d | 文件: %s",
		m.ManifestID, m.MajorVersion, m.MinorVersion, humanCount(len(m.Files)))}

	avgChunk := "-"
	if len(uniqueChunks) > 0 {
		avgChunk = humanSize(int64(compressedSize / uint64(len(uniqueChunks))))
	}
	lines = append(lines, fmt.Sprintf("   内容大小: %s | 压缩大小: %s | Chunk: %s | Bundle: %s | 平均 Chunk: %s",
		humanSize(int64(contentSize)), humanSize(int64(compressedSize)),
		humanCount(len(uniqueChunks)), humanCount(len(uniqueBundles)), avgChunk))

	if len(m.Params) == 0 {
		lines = append(lines, "   哈希算法: 未声明（清单无 Parameters 表，校验时按穷举推断）")
	} else {
		for i, p := range m.Params {
			prefix := "   "
			if len(m.Params) > 1 {
				prefix = fmt.Sprintf("   参数[%d] ", i)
			}
			lines = append(lines, fmt.Sprintf("%s哈希算法: %s | 分块范围: %s ~ %s | 单块最大原始: %s",
				prefix, p.HashType,
				humanSize(int64(p.MinChunkSize)), humanSize(int64(p.MaxChunkSize)),
				humanSize(int64(p.MaxUncompressed))))
		}
		if filesNoHash > 0 {
			lines = append(lines, fmt.Sprintf(
				"   注: %d 个文件的 Chunk 哈希算法未声明（param_index 未命中有效条目，校验时按穷举推断）",
				filesNoHash))
		}
	}

	if len(m.Flags) > 0 {
		lines = append(lines, fmt.Sprintf("   标记: %s（共 %d 个）",
			strings.Join(m.Flags, ", "), len(m.Flags)))
	}

	timeLine := "   清单大小: " + humanSize(meta.size)
	switch {
	case meta.modTime.IsZero():
		timeLine += " | 清单时间: 未知（RMAN 格式无时间字段）"
	case meta.fromURL:
		timeLine += fmt.Sprintf(" | 清单时间: %s（推测：来自 CDN Last-Modified 响应头，RMAN 格式无时间字段）",
			meta.modTime.Local().Format("2006-01-02 15:04:05 -0700"))
	default:
		timeLine += fmt.Sprintf(" | 清单时间: %s（推测：本地文件修改时间，通常为下载时刻，RMAN 格式无时间字段）",
			meta.modTime.Local().Format("2006-01-02 15:04:05 -0700"))
	}
	lines = append(lines, timeLine)

	return lines
}

func applyFilters(files []rman.FileEntry, pattern, flags string) ([]rman.FileEntry, error) {
	opt := core.FilterOption{Pattern: pattern}
	if flags != "" {
		opt.Flags = strings.Split(flags, ",")
	}
	return core.FilterWithError(files, opt)
}

// ---- 更新模式解析 ----

// resolveUpdateMode 将 -repair/-verify-only/-no-verify 三个互斥 flag 映射为
// update.Mode；三者两两互斥，同时指定多个视为用户输入错误。全部为 false 时
// 退化为 update.ModeAuto（默认增量更新）。
func resolveUpdateMode(repair, verifyOnly, noVerify bool) (update.Mode, error) {
	count := 0
	if repair {
		count++
	}
	if verifyOnly {
		count++
	}
	if noVerify {
		count++
	}
	if count > 1 {
		return update.ModeAuto, fmt.Errorf("-repair / -verify-only / -no-verify 互斥，只能指定一个")
	}

	switch {
	case repair:
		return update.ModeRepair, nil
	case verifyOnly:
		return update.ModeVerifyOnly, nil
	case noVerify:
		return update.ModeForceFull, nil
	default:
		return update.ModeAuto, nil
	}
}

// validateInstallOnlyFlags 拒绝让具有受管理语义的参数在默认独立下载中悄悄失效。
func validateInstallOnlyFlags(install bool, updatePath string, keepRemoved bool) error {
	if install {
		return nil
	}
	if updatePath != "" {
		return fmt.Errorf("-update 仅可与 -install 一起使用")
	}
	if keepRemoved {
		return fmt.Errorf("-keep-removed 仅可与 -install 一起使用")
	}
	return nil
}

// buildSyncOptions 将操作意图与策略参数映射为 update.Options。
func buildSyncOptions(mode update.Mode, install bool, updatePath string, keepRemoved bool) update.Options {
	operation := update.OperationDownload
	if install {
		operation = update.OperationInstall
	}
	return update.Options{
		Operation:       operation,
		Mode:            mode,
		OldManifestPath: updatePath,
		RemoveDeleted:   install && !keepRemoved,
	}
}

// guardStandaloneManagedRoot 防止默认下载/修复/强制写入一个已有 installed.json
// 的受管理根。VERIFY_ONLY 是只读操作，始终允许。
func guardStandaloneManagedRoot(outputDir string, operation update.Operation, mode update.Mode) error {
	if operation == update.OperationInstall || mode == update.ModeVerifyOnly {
		return nil
	}
	exists, err := update.NewArchive(outputDir).HasInstalledState()
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("输出目录已包含受管理安装状态；请使用 -install，或改用其他 -o 目录")
	}
	return nil
}

func operationDescription(operation update.Operation) string {
	if operation == update.OperationInstall {
		return "受管理安装"
	}
	return "单独下载"
}

// modeDescription 返回 update.Mode 面向终端用户的中文描述。
func modeDescription(mode update.Mode) string {
	switch mode {
	case update.ModeRepair:
		return "修复（逐文件重新校验补洞）"
	case update.ModeVerifyOnly:
		return "仅验证（不下载不写盘）"
	case update.ModeForceFull:
		return "强制全量（跳过校验）"
	default:
		return "自动（校验补洞）"
	}
}

// printSyncSummary 输出一次 Sync 的汇总结果。ModeVerifyOnly 下 Stats.Failed
// 表示"校验发现待修复"而非下载失败，因此单列逐文件校验结果、用不同措辞呈现；
// 其余模式按跳过/修复/新建/移动/删除/失败分类汇总文件数与复用/下载字节数。
func printSyncSummary(printer *outputPrinter, operation update.Operation, mode update.Mode, stats update.Stats, elapsed time.Duration) {
	if mode == update.ModeVerifyOnly {
		printer.info("🔍 验证结果:\n")
		for _, p := range stats.Skipped {
			printer.info("   ✅ %s\n", p)
		}
		for _, p := range stats.Failed {
			printer.info("   ⚠️  待修复: %s\n", p)
		}
		printer.info("\n📊 验证完成，耗时 %s\n", elapsed.Round(time.Millisecond))
		printer.info("   正常: %d | 待修复: %d\n", len(stats.Skipped), len(stats.Failed))
		return
	}

	printer.info("✅ %s完成！耗时 %s\n", operationDescription(operation), elapsed.Round(time.Millisecond))
	printer.info("📊 处理统计:\n")
	printer.info("   跳过（内容未变）: %d\n", len(stats.Skipped))
	printer.info("   修复/补丁: %d\n", len(stats.Patched))
	printer.info("   新建: %d\n", len(stats.Created))
	printer.info("   移动: %d\n", len(stats.Moved))
	if len(stats.Removed) > 0 {
		printer.info("   删除: %d\n", len(stats.Removed))
	}
	printer.info("   本地复用: %s\n", humanSize(stats.ReusedBytes))
	printer.info("   网络下载: %s\n", humanSize(stats.DownloadedBytes))
	if len(stats.Failed) > 0 {
		printer.info("   失败: %d\n", len(stats.Failed))
		for _, p := range stats.Failed {
			printer.info("     ✗ %s\n", p)
		}
	}
}

// ---- 日志保存 ----

func saveLog(path string, dl *downloadLog, stats update.Stats, operation update.Operation, mode update.Mode, files []rman.FileEntry, verboseLevel int, cfg core.DownloadConfig) error {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		os.MkdirAll(dir, 0755)
	}

	var sb strings.Builder

	sb.WriteString("═══════════════════════════════════════\n")
	sb.WriteString("  RiotManifestGo 下载/安装日志\n")
	sb.WriteString(fmt.Sprintf("  时间: %s\n", dl.startTime.Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("  操作: %s\n", operationDescription(operation)))
	sb.WriteString(fmt.Sprintf("  模式: %s\n", modeDescription(mode)))
	sb.WriteString("═══════════════════════════════════════\n\n")

	sb.WriteString(fmt.Sprintf("## 匹配文件列表 (%d 个)\n\n", len(files)))
	for _, f := range files {
		flagStr := ""
		if len(f.Flags) > 0 {
			flagStr = " [" + strings.Join(f.Flags, ",") + "]"
		}
		sb.WriteString(fmt.Sprintf("  %s (%s, %d chunks)%s\n",
			f.Path, humanSize(int64(f.FileSize)), len(f.Chunks), flagStr))
	}

	// ModeVerifyOnly 下 Stats.Failed 表示"校验发现待修复"而非下载失败，日志措辞
	// 需要与 printSyncSummary 保持一致，避免误导为下载出错。
	failedLabel := "失败"
	if mode == update.ModeVerifyOnly {
		failedLabel = "待修复"
	}

	sb.WriteString("\n## 处理统计\n\n")
	sb.WriteString(fmt.Sprintf("  耗时: %s\n", dl.elapsed.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("  本地复用: %s\n", humanSize(stats.ReusedBytes)))
	sb.WriteString(fmt.Sprintf("  网络下载: %s\n", humanSize(stats.DownloadedBytes)))
	sb.WriteString(fmt.Sprintf("  实际网络流量: %s\n", humanSize(dl.bytesFetched)))
	wastedBytes := dl.bytesFetched - dl.bytesDownloaded
	if wastedBytes < 0 {
		wastedBytes = 0
	}
	sb.WriteString(fmt.Sprintf("  合并/整包多下: %s\n", humanSize(wastedBytes)))
	sb.WriteString(fmt.Sprintf("  跳过: %d\n", len(stats.Skipped)))
	sb.WriteString(fmt.Sprintf("  修复/补丁: %d\n", len(stats.Patched)))
	sb.WriteString(fmt.Sprintf("  新建: %d\n", len(stats.Created)))
	sb.WriteString(fmt.Sprintf("  移动: %d\n", len(stats.Moved)))
	sb.WriteString(fmt.Sprintf("  删除: %d\n", len(stats.Removed)))
	sb.WriteString(fmt.Sprintf("  %s: %d\n", failedLabel, len(stats.Failed)))
	sb.WriteString(fmt.Sprintf("  Chunk 处理数: %d\n", dl.chunksProcessed))
	sb.WriteString(fmt.Sprintf("  Bundle 完成数: %d\n", dl.bundlesProcessed))
	sb.WriteString(fmt.Sprintf("  Bundle 失败数: %d\n", dl.bundlesFailed))
	sb.WriteString(fmt.Sprintf("  重试次数: %d\n", dl.retries))
	if dl.elapsed.Seconds() > 0 && stats.DownloadedBytes > 0 {
		avgSpeed := float64(stats.DownloadedBytes) / dl.elapsed.Seconds()
		sb.WriteString(fmt.Sprintf("  平均下载速度: %s/s\n", humanSize(int64(avgSpeed))))
	}

	if verboseLevel >= 1 {
		sb.WriteString("\n## 详细信息\n\n")
		sb.WriteString(fmt.Sprintf("  Worker 数: %d\n", cfg.Workers))
		sb.WriteString(fmt.Sprintf("  Gap Tolerance: %d KB\n", cfg.GapTolerance/1024))
		sb.WriteString(fmt.Sprintf("  Max Ranges: %d\n", cfg.MaxRangesPerReq))
		sb.WriteString(fmt.Sprintf("  Full Bundle Threshold: %.2f\n", cfg.FullBundleThreshold))
	}

	if len(stats.Failed) > 0 {
		sb.WriteString(fmt.Sprintf("\n## %s详情 (%d 个)\n\n", failedLabel, len(stats.Failed)))
		for _, p := range stats.Failed {
			sb.WriteString(fmt.Sprintf("  ✗ %s\n", p))
		}
	} else {
		sb.WriteString(fmt.Sprintf("\n## %s详情\n\n  (无)\n", failedLabel))
	}

	if len(dl.errors) > 0 {
		sb.WriteString(fmt.Sprintf("\n## Bundle 错误详情 (%d 个)\n\n", len(dl.errors)))
		for _, e := range dl.errors {
			sb.WriteString(fmt.Sprintf("  ✗ %s\n", e))
		}
	}

	return os.WriteFile(path, []byte(sb.String()), 0644)
}

// ---- 列表显示 ----

func printFileList(files []rman.FileEntry, limit int) {
	var totalSize uint64
	totalChunks := 0
	uniqueBundles := make(map[uint64]struct{})
	for _, f := range files {
		totalSize += f.FileSize
		totalChunks += len(f.Chunks)
		for _, c := range f.Chunks {
			uniqueBundles[c.BundleID] = struct{}{}
		}
	}

	fmt.Printf("\n%-70s %12s %6s %s\n", "路径", "大小", "Chunks", "Flags")
	fmt.Println(strings.Repeat("─", 110))

	showCount := len(files)
	if limit >= 0 && showCount > limit {
		showCount = limit
	}

	for i := 0; i < showCount; i++ {
		f := files[i]
		flagStr := ""
		if len(f.Flags) > 0 {
			flagStr = strings.Join(f.Flags, ",")
		}
		path := f.Path
		if len(path) > 68 {
			path = "..." + path[len(path)-65:]
		}
		fmt.Printf("%-70s %12s %6d %s\n", path, humanSize(int64(f.FileSize)), len(f.Chunks), flagStr)
	}

	if showCount < len(files) {
		fmt.Printf("... 还有 %d 个文件（使用 -n -1 显示全部）\n", len(files)-showCount)
	}

	fmt.Println(strings.Repeat("─", 110))
	fmt.Printf("合计: %d 个文件, %s, %d Chunks, %d Bundles\n",
		len(files), humanSize(int64(totalSize)), totalChunks, len(uniqueBundles))
}

// ---- 下载计划统计 ----

// planSummary 是 -plan-only 输出的下载计划统计口径。
type planSummary struct {
	Jobs           int   // 作业总数
	FullBundleJobs int   // 整包 GET 作业数
	Segments       int   // 非整包作业的 Range 段数合计（整包请求不带 Range 头，不计段）
	UsefulBytes    int64 // Σ Chunk CompressedSize：写盘所需的有效压缩字节
	FetchedBytes   int64 // 实际将请求的网络字节：整包作业取 BundleSize，Range 作业取段宽合计
	P50, P90, Max  int64 // 每作业 Fetched 字节分位（升序取 idx=len*50/100、len*90/100，Max=末位）
}

// summarizePlan 把调度产出的 BundleJob 列表汇总为 planSummary。
// FetchedBytes 与 UsefulBytes 的差值即 Range 间隙合并与整包下载多拉取的浪费字节。
func summarizePlan(jobs []core.BundleJob) planSummary {
	var s planSummary
	s.Jobs = len(jobs)
	perJob := make([]int64, 0, len(jobs))
	for _, job := range jobs {
		var fetched int64
		if job.FullBundle {
			s.FullBundleJobs++
			fetched = int64(job.BundleSize)
		} else {
			s.Segments += len(job.Ranges)
			for _, r := range job.Ranges {
				fetched += int64(r.End) - int64(r.Start) + 1
			}
		}
		for _, r := range job.Ranges {
			for _, c := range r.Chunks {
				s.UsefulBytes += int64(c.CompressedSize)
			}
		}
		s.FetchedBytes += fetched
		perJob = append(perJob, fetched)
	}
	if len(perJob) > 0 {
		sort.Slice(perJob, func(i, j int) bool { return perJob[i] < perJob[j] })
		s.P50 = perJob[len(perJob)*50/100]
		s.P90 = perJob[len(perJob)*90/100]
		s.Max = perJob[len(perJob)-1]
	}
	return s
}

// printPlanSummary 打印 plan-only 计划统计，多下占比以实际请求字节为基数。
func printPlanSummary(printer *outputPrinter, s planSummary) {
	printer.info("📋 下载计划统计（零网络流量）\n")
	printer.info("   作业数: %d（整包 %d）\n", s.Jobs, s.FullBundleJobs)
	printer.info("   Range 段数: %d\n", s.Segments)
	printer.info("   有效字节: %s\n", humanSize(s.UsefulBytes))
	printer.info("   实际请求字节: %s\n", humanSize(s.FetchedBytes))
	wasted := s.FetchedBytes - s.UsefulBytes
	if wasted < 0 {
		wasted = 0
	}
	pct := 0.0
	if s.FetchedBytes > 0 {
		pct = float64(wasted) / float64(s.FetchedBytes) * 100
	}
	printer.info("   合并/整包多下: %s（占实际请求 %.1f%%）\n", humanSize(wasted), pct)
	printer.info("   作业尺寸分位: P50 %s / P90 %s / Max %s\n",
		humanSize(s.P50), humanSize(s.P90), humanSize(s.Max))
}

// ---- 边缘优选（-edge） ----

// probeBundleMinSize 是 pickProbeBundle 挑选探测目标的尺寸门槛，与
// edge.Config.ProbeBytes 的默认值（1MiB）一致：选够 1MiB 的 Bundle 才能让探测
// 请求的 Range 落在真实数据区间内，避免探测体裁得比请求还短。
const probeBundleMinSize = 1 << 20

// pickProbeBundle 从 BundleSizes 选探测目标：>=1MB 的最小者，全部 <1MB 则取最大者；
// 同尺寸按 BundleID 小者优先（map 遍历无序，保证确定性）；空表返回 false。
func pickProbeBundle(sizes map[uint64]uint32) (uint64, bool) {
	if len(sizes) == 0 {
		return 0, false
	}

	var haveGE bool
	var geID uint64
	var geSize uint32
	var haveMax bool
	var maxID uint64
	var maxSize uint32

	for id, size := range sizes {
		if !haveMax || size > maxSize || (size == maxSize && id < maxID) {
			maxID, maxSize, haveMax = id, size, true
		}
		if size >= probeBundleMinSize {
			if !haveGE || size < geSize || (size == geSize && id < geID) {
				geID, geSize, haveGE = id, size, true
			}
		}
	}

	if haveGE {
		return geID, true
	}
	return maxID, true
}

// ---- 工具函数 ----

// parseCDNURLs 解析 -u 的逗号分隔多域名值：按逗号切分、去首尾空白、丢弃空项。
// 全部为空时返回 nil。
func parseCDNURLs(s string) []string {
	var urls []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			urls = append(urls, p)
		}
	}
	return urls
}

// humanCount 返回带千位分隔符的十进制计数（如 787480 -> "787,480"）。
func humanCount(n int) string {
	s := strconv.Itoa(n)
	if n < 0 || len(s) <= 3 {
		return s
	}
	var sb strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		sb.WriteString(s[:pre])
	}
	for i := pre; i < len(s); i += 3 {
		if sb.Len() > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(s[i : i+3])
	}
	return sb.String()
}

func humanSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func extractManifestArg(args []string) (manifest string, remaining []string) {
	flagsWithValue := map[string]bool{
		"-o": true, "-u": true, "-p": true, "-f": true,
		"-w": true, "-n": true, "-log": true, "-retry": true, "-v": true,
		"-update": true, "-retry-wait": true,
		"-gap-tolerance": true, "-max-ranges": true, "-full-bundle-threshold": true,
		"-edge-winners": true,
	}

	remaining = make([]string, 0, len(args))
	skipNext := false

	for _, arg := range args {
		if skipNext {
			skipNext = false
			remaining = append(remaining, arg)
			continue
		}

		if strings.HasPrefix(arg, "-") {
			remaining = append(remaining, arg)
			if flagsWithValue[arg] {
				skipNext = true
			}
			continue
		}

		if manifest == "" {
			manifest = arg
		}
	}

	return manifest, remaining
}
