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
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Virace/RiotManifestGo/pkg/core"
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
	cdnURL := flag.String("u", defaultCDNURL, "CDN Bundle 基础 URL")
	pattern := flag.String("p", "", "路径筛选（子串或正则）")
	flags := flag.String("f", "", "Flag 过滤，逗号代表与，管道符代表或（如 de_DE,windows 或 ja_JP|ko_KR）")
	workers := flag.Int("w", 16, "并发下载 Worker 数")
	listOnly := flag.Bool("list", false, "仅列出文件，不下载")
	listLimit := flag.Int("n", 20, "列表模式下最大显示数量（-1=全部）")
	logFile := flag.String("log", "", "下载完毕保存完整日志到指定文件")
	maxRetries := flag.Int("retry", 3, "单个 Bundle 下载失败最大重试次数")
	silent := flag.Bool("s", false, "静默模式，仅输出错误")
	verbose := flag.Int("v", 0, "详细输出等级（0=默认进度条, 1=基础滚屏, 2=详细, 3=调试）")
	showVersion := flag.Bool("version", false, "显示程序版本信息并退出")
	updatePath := flag.String("update", "", "旧清单路径，用于增量更新比对（缺省时自动从本地存档发现）")
	repair := flag.Bool("repair", false, "修复模式：逐文件重新校验并补齐本地缺失/损坏的内容，不做文件级跳过")
	verifyOnly := flag.Bool("verify-only", false, "仅校验本地文件完整性，不下载、不写盘")
	noVerify := flag.Bool("no-verify", false, "跳过校验，将全部匹配文件当作全新内容整体下载")
	keepRemoved := flag.Bool("keep-removed", false, "保留旧清单中已不存在的文件，不清理磁盘（默认清理）")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "RiotManifestGo CLI (%s, commit %s) — Riot 游戏资源清单解析与下载\n\n", version, commit)
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli <manifest文件或URL> [选项]\n\n")
		fmt.Fprintf(os.Stderr, "示例:\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -list\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli https://...releases/XXX.manifest -list -p Aatrox -f ja_JP|ko_KR\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -p \"\\.dll\" -o ./output -log download.log\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli new.manifest -o ./output                 # 增量更新（自动发现旧版本）\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli new.manifest -o ./output -update old.manifest\n")
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

	if manifestSource == "" {
		flag.Usage()
		os.Exit(1)
	}

	printer := &outputPrinter{silent: *silent, verbose: *verbose}

	// 1. 解析 Manifest
	printer.info("📄 解析 manifest: %s\n", manifestSource)
	manifest, manifestRaw, err := loadManifest(manifestSource)
	if err != nil {
		log.Fatalf("❌ 解析失败: %v", err)
	}
	printer.info("✅ ManifestID: %016X | 文件总数: %d\n\n", manifest.ManifestID, len(manifest.Files))

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

	// 4. 更新计划预览。具体待下载的 Chunk 集合由 Sync 按新旧清单差异动态决定
	// （可能远小于 files 全集），因此这里不再预先计算 Map/Schedule——那只是
	// Sync 内部下载执行阶段自己的事，重复算一遍并不会让预览更准确。
	var totalDeclaredSize uint64
	for _, f := range files {
		totalDeclaredSize += f.FileSize
	}
	printer.info("📦 更新计划:\n")
	printer.info("   文件数: %d\n", len(files))
	printer.info("   声明总大小: %s\n", humanSize(int64(totalDeclaredSize)))
	printer.info("   模式: %s\n", modeDescription(updateMode))
	printer.info("   CDN: %s\n", *cdnURL)
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

	dl := core.NewDownloader(core.DownloadConfig{
		CDNBaseURL:      *cdnURL,
		OutputDir:       *outputDir,
		Workers:         *workers,
		MaxFileHandles:  500,
		MaxRangesPerReq: 30,
		GapTolerance:    core.DefaultGapTolerance,
		MaxRetries:      *maxRetries,
	})

	dlLog := &downloadLog{startTime: time.Now(), files: files}
	go consumeEvents(dl.Events(), dlLog, printer)

	if updateMode == update.ModeVerifyOnly {
		printer.info("🔍 开始验证...\n")
	} else {
		printer.info("🚀 开始更新...\n")
	}

	// 心跳 ticker：每 2 秒刷新一次，即使没有新 Chunk 完成也显示 elapsed；
	// ModeVerifyOnly 零下载、零写盘，进度以逐文件验证事件呈现，不需要心跳。
	stopHeartbeat := make(chan struct{})
	if !*silent && *verbose == 0 && updateMode != update.ModeVerifyOnly {
		go heartbeat(dlLog, stopHeartbeat)
	}

	opts := buildSyncOptions(updateMode, *updatePath, *keepRemoved)
	stats, syncErr := update.Sync(ctx, manifest, manifestRaw, manifestSource, *outputDir, files, dl, opts)
	close(stopHeartbeat)

	printer.info("\n")

	elapsed := time.Since(dlLog.startTime)
	if syncErr != nil {
		if ctx.Err() != nil {
			printer.info("⏹️  更新已取消\n")
		} else {
			fmt.Fprintf(os.Stderr, "❌ 更新失败: %v\n", syncErr)
		}
	} else {
		printSyncSummary(printer, updateMode, stats, elapsed)
	}

	// 保存日志
	if *logFile != "" {
		dlLog.elapsed = elapsed
		if writeErr := saveLog(*logFile, dlLog, stats, updateMode, files, *verbose); writeErr != nil {
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
				printer.info("  [BUNDLE] %s 开始 (%d Range, %d Chunk)\n",
					e.BundleFilename, e.RangeCount, e.ChunkCount)
			}

		case core.EventBundleDone:
			dl.mu.Lock()
			dl.bundlesProcessed++
			dl.mu.Unlock()
			if printer.verbose >= 1 {
				printer.info("  [BUNDLE] %016X 完成\n", e.BundleID)
			}

		case core.EventRetry:
			dl.mu.Lock()
			dl.retries++
			dl.mu.Unlock()
			fmt.Fprintf(os.Stderr, "  🔄 重试 Bundle %s (%d/%d): %v\n",
				e.BundleFilename, e.Attempt, e.MaxRetries, e.Err)

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

// loadManifest 解析 manifest，同时返回其原始字节，供调用方在需要时存档
// （写入 update.Archive）。
func loadManifest(source string) (*rman.Manifest, []byte, error) {
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		return loadManifestFromURL(source)
	}
	return loadManifestFromFile(source)
}

func loadManifestFromFile(path string) (*rman.Manifest, []byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("读取 manifest 文件失败: %w", err)
	}
	manifest, err := rman.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	return manifest, data, nil
}

func loadManifestFromURL(url string) (*rman.Manifest, []byte, error) {
	client := &http.Client{
		Timeout: 60 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, nil, fmt.Errorf("下载 manifest 失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("下载 manifest 失败: HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("读取 manifest 数据失败: %w", err)
	}

	manifest, err := rman.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	return manifest, data, nil
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

// buildSyncOptions 将 -update/-keep-removed 映射为 update.Options；keepRemoved
// 语义上与 RemoveDeleted 相反（未指定 -keep-removed 时默认清理旧内容）。
func buildSyncOptions(mode update.Mode, updatePath string, keepRemoved bool) update.Options {
	return update.Options{
		Mode:            mode,
		OldManifestPath: updatePath,
		RemoveDeleted:   !keepRemoved,
	}
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
		return "自动增量"
	}
}

// printSyncSummary 输出一次 Sync 的汇总结果。ModeVerifyOnly 下 Stats.Failed
// 表示"校验发现待修复"而非下载失败，因此单列逐文件校验结果、用不同措辞呈现；
// 其余模式按跳过/修复/新建/移动/删除/失败分类汇总文件数与复用/下载字节数。
func printSyncSummary(printer *outputPrinter, mode update.Mode, stats update.Stats, elapsed time.Duration) {
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

	printer.info("✅ 更新完成！耗时 %s\n", elapsed.Round(time.Millisecond))
	printer.info("📊 更新统计:\n")
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

func saveLog(path string, dl *downloadLog, stats update.Stats, mode update.Mode, files []rman.FileEntry, verboseLevel int) error {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		os.MkdirAll(dir, 0755)
	}

	var sb strings.Builder

	sb.WriteString("═══════════════════════════════════════\n")
	sb.WriteString("  RiotManifestGo 更新日志\n")
	sb.WriteString(fmt.Sprintf("  时间: %s\n", dl.startTime.Format("2006-01-02 15:04:05")))
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

	sb.WriteString("\n## 更新统计\n\n")
	sb.WriteString(fmt.Sprintf("  耗时: %s\n", dl.elapsed.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("  本地复用: %s\n", humanSize(stats.ReusedBytes)))
	sb.WriteString(fmt.Sprintf("  网络下载: %s\n", humanSize(stats.DownloadedBytes)))
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
		sb.WriteString("  Worker 数: 默认\n")
		sb.WriteString(fmt.Sprintf("  Gap Tolerance: %d KB\n", core.DefaultGapTolerance/1024))
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

// ---- 工具函数 ----

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
		"-update": true,
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
