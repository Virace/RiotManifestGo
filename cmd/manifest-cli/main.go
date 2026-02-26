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
	flags := flag.String("f", "", "Flag 过滤，逗号分隔（如 de_DE,windows）")
	workers := flag.Int("w", 16, "并发下载 Worker 数")
	listOnly := flag.Bool("list", false, "仅列出文件，不下载")
	listLimit := flag.Int("n", 20, "列表模式下最大显示数量（-1=全部）")
	logFile := flag.String("log", "", "下载完毕保存完整日志到指定文件")
	maxRetries := flag.Int("retry", 3, "单个 Bundle 下载失败最大重试次数")
	silent := flag.Bool("s", false, "静默模式，仅输出错误")
	verbose := flag.Int("v", 0, "详细输出等级（0=默认进度条, 1=基础滚屏, 2=详细, 3=调试）")
	showVersion := flag.Bool("version", false, "显示程序版本信息并退出")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "RiotManifestGo CLI (v%s, commit %s) — Riot 游戏资源清单解析与下载\n\n", version, commit)
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli <manifest文件或URL> [选项]\n\n")
		fmt.Fprintf(os.Stderr, "示例:\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -list\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli https://...releases/XXX.manifest -list -p Aatrox -f de_DE\n")
		fmt.Fprintf(os.Stderr, "  manifest-cli game.manifest -p \"\\.dll\" -o ./output -log download.log\n\n")
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

	if manifestSource == "" {
		flag.Usage()
		os.Exit(1)
	}

	printer := &outputPrinter{silent: *silent, verbose: *verbose}

	// 1. 解析 Manifest
	printer.info("📄 解析 manifest: %s\n", manifestSource)
	manifest, err := loadManifest(manifestSource)
	if err != nil {
		log.Fatalf("❌ 解析失败: %v", err)
	}
	printer.info("✅ ManifestID: %016X | 文件总数: %d\n\n", manifest.ManifestID, len(manifest.Files))

	// 2. 过滤
	files := manifest.Files
	if *pattern != "" || *flags != "" {
		var opts []core.FilterOption
		opt := core.FilterOption{Pattern: *pattern}
		if *flags != "" {
			opt.Flags = strings.Split(*flags, ",")
		}
		opts = append(opts, opt)
		files = core.Filter(files, opts...)
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

	// 4. 下载模式
	taskMap := core.Map(files)
	var totalChunks int
	var totalCompBytes int64
	for _, tasks := range taskMap {
		for _, task := range tasks {
			totalChunks++
			totalCompBytes += int64(task.CompressedSize)
		}
	}
	bundleJobs := core.Schedule(taskMap, core.ScheduleConfig{
		MaxRangesPerReq: 30,
		GapTolerance:    core.DefaultGapTolerance,
	})

	printer.info("📦 下载计划:\n")
	printer.info("   文件数: %d\n", len(files))
	printer.info("   唯一 Chunk 数: %d\n", totalChunks)
	printer.info("   Bundle Job 数: %d\n", len(bundleJobs))
	printer.info("   待下载压缩数据: %s\n", humanSize(totalCompBytes))
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

	printer.info("🚀 开始下载...\n")

	// 心跳 ticker：每 2 秒刷新一次，即使没有新 Chunk 完成也显示 elapsed
	stopHeartbeat := make(chan struct{})
	if !*silent && *verbose == 0 {
		go heartbeat(dlLog, stopHeartbeat)
	}

	err = dl.Download(ctx, files)
	close(stopHeartbeat)

	printer.info("\n")

	if err != nil {
		if ctx.Err() != nil {
			printer.info("⏹️  下载已取消\n")
		} else {
			fmt.Fprintf(os.Stderr, "❌ 下载失败: %v\n", err)
		}
	} else {
		printer.info("✅ 下载完成！耗时 %s\n", time.Since(dlLog.startTime).Round(time.Millisecond))
	}

	// 保存日志
	if *logFile != "" {
		dlLog.elapsed = time.Since(dlLog.startTime)
		if writeErr := saveLog(*logFile, dlLog, files, *verbose); writeErr != nil {
			fmt.Fprintf(os.Stderr, "⚠️  保存日志失败: %v\n", writeErr)
		} else {
			printer.info("📝 日志已保存: %s\n", *logFile)
		}
	}

	if err != nil {
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

		case core.EventComplete:
			dl.mu.Lock()
			speedStr := ""
			if e.Elapsed.Seconds() > 0 {
				avgSpeed := float64(dl.bytesDownloaded) / e.Elapsed.Seconds()
				speedStr = fmt.Sprintf(" | 均速: %s/s", humanSize(int64(avgSpeed)))
			}
			dl.mu.Unlock()
			if printer.verbose == 0 && !printer.silent {
				fmt.Printf("\r   📥 %s | Chunks: %d | Bundles: %d%s    ",
					humanSize(dl.bytesDownloaded), dl.chunksProcessed, dl.bundlesProcessed, speedStr)
				fmt.Println()
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
	fmt.Printf("\r   📥 %s | Chunks: %d | Bundles: %d%s | %s    ",
		humanSize(dl.bytesDownloaded), dl.chunksProcessed, dl.bundlesProcessed,
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

func loadManifest(source string) (*rman.Manifest, error) {
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		return loadManifestFromURL(source)
	}
	return rman.ParseFile(source)
}

func loadManifestFromURL(url string) (*rman.Manifest, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("下载 manifest 失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("下载 manifest 失败: HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 manifest 数据失败: %w", err)
	}

	return rman.Parse(bytes.NewReader(data))
}

// ---- 日志保存 ----

func saveLog(path string, dl *downloadLog, files []rman.FileEntry, verboseLevel int) error {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		os.MkdirAll(dir, 0755)
	}

	var sb strings.Builder

	sb.WriteString("═══════════════════════════════════════\n")
	sb.WriteString("  RiotManifestGo 下载日志\n")
	sb.WriteString(fmt.Sprintf("  时间: %s\n", dl.startTime.Format("2006-01-02 15:04:05")))
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

	sb.WriteString("\n## 下载统计\n\n")
	sb.WriteString(fmt.Sprintf("  耗时: %s\n", dl.elapsed.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("  已下载压缩数据: %s\n", humanSize(dl.bytesDownloaded)))
	sb.WriteString(fmt.Sprintf("  Chunk 处理数: %d\n", dl.chunksProcessed))
	sb.WriteString(fmt.Sprintf("  Bundle 完成数: %d\n", dl.bundlesProcessed))
	sb.WriteString(fmt.Sprintf("  Bundle 失败数: %d\n", dl.bundlesFailed))
	sb.WriteString(fmt.Sprintf("  重试次数: %d\n", dl.retries))
	if dl.elapsed.Seconds() > 0 {
		avgSpeed := float64(dl.bytesDownloaded) / dl.elapsed.Seconds()
		sb.WriteString(fmt.Sprintf("  平均下载速度: %s/s\n", humanSize(int64(avgSpeed))))
	}

	if verboseLevel >= 1 {
		sb.WriteString("\n## 详细信息\n\n")
		sb.WriteString("  Worker 数: 默认\n")
		sb.WriteString(fmt.Sprintf("  Gap Tolerance: %d KB\n", core.DefaultGapTolerance/1024))
	}

	if len(dl.errors) > 0 {
		sb.WriteString(fmt.Sprintf("\n## 错误详情 (%d 个)\n\n", len(dl.errors)))
		for _, e := range dl.errors {
			sb.WriteString(fmt.Sprintf("  ✗ %s\n", e))
		}
	} else {
		sb.WriteString("\n## 错误详情\n\n  (无错误)\n")
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
