package core

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Virace/RiotManifestGo/internal/fswriter"
	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/klauspost/compress/zstd"
)

// ---- Mock BundleFetcher ----

// mockFetcher 是 BundleFetcher 的测试 mock 实现。
type mockFetcher struct {
	mu              sync.Mutex
	callCount       int
	failUntil       int                                                               // 前 failUntil 次调用返回 error
	responses       map[string][]rangeResp                                            // bundleFilename → Range 级别响应（按调用顺序返回整批，不关心具体 Range 边界）
	fetchError      error                                                             // 始终返回的 error（优先于 failUntil）
	rangeFunc       func(bundleFilename string, ranges []ByteRange) ([][]byte, error) // 优先于 responses：按实际请求的 Range 精确应答，用于断言"只请求了预期的字节区间"
	requestedRanges []ByteRange                                                       // 记录所有调用中实际收到的 ranges，用于断言子集下载未越界请求
	requestedOpts   []FetchOptions                                                    // 记录每次调用收到的 FetchOptions，用于断言 URLHint 轮转与整包透传
}

type rangeResp struct {
	data []byte
}

func newMockFetcher() *mockFetcher {
	return &mockFetcher{
		responses: make(map[string][]rangeResp),
	}
}

func (m *mockFetcher) FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange, opts FetchOptions) ([][]byte, error) {
	m.mu.Lock()
	m.callCount++
	count := m.callCount
	m.requestedRanges = append(m.requestedRanges, ranges...)
	m.requestedOpts = append(m.requestedOpts, opts)
	rangeFunc := m.rangeFunc
	m.mu.Unlock()

	if m.fetchError != nil {
		return nil, m.fetchError
	}

	if count <= m.failUntil {
		return nil, fmt.Errorf("mock 第 %d 次调用失败（模拟网络错误）", count)
	}

	if rangeFunc != nil {
		return rangeFunc(bundleFilename, ranges)
	}

	resps, ok := m.responses[bundleFilename]
	if !ok {
		return nil, fmt.Errorf("mock: 未配置的 Bundle 文件 %s", bundleFilename)
	}

	result := make([][]byte, len(resps))
	for i, r := range resps {
		result[i] = r.data
	}
	return result, nil
}

func (m *mockFetcher) Close() {}

// ---- 压缩辅助 ----

// zstdCompress 将数据用 ZSTD 压缩。
func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("创建 ZSTD 编码器失败: %v", err)
	}
	defer enc.Close()
	return enc.EncodeAll(data, nil)
}

// buildRangeData 为一组 Chunk 构造连续的 Range 数据（各 Chunk 压缩字节拼接）。
func buildRangeData(t *testing.T, chunks ...[]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, chunk := range chunks {
		compressed := zstdCompress(t, chunk)
		buf.Write(compressed)
	}
	return buf.Bytes()
}

// makeTestConfig 创建测试用 DownloadConfig。
func makeTestConfig(t *testing.T) (DownloadConfig, string) {
	t.Helper()
	dir := t.TempDir()
	return DownloadConfig{
		OutputDir:       dir,
		Workers:         2,
		MaxFileHandles:  10,
		MaxRangesPerReq: 30,
		GapTolerance:    0,
		MaxRetries:      3,
	}, dir
}

// ---- 辅助：构造 FileEntry + Chunk 数据的一致性对 ----

// testChunkData 存储测试使用的 Chunk 原始数据和压缩后数据及元信息。
type testChunkData struct {
	raw          []byte
	compressed   []byte
	chunkID      uint64
	compSize     uint32
	uncompSize   uint32
	bundleID     uint64
	bundleOffset uint32
	hashType     rman.HashType
}

// makeTestChunk 构造一个测试 Chunk，自动计算 ChunkID（SHA256 哈希）。
func makeTestChunk(t *testing.T, data []byte, bundleID uint64, bundleOffset uint32) testChunkData {
	t.Helper()
	compressed := zstdCompress(t, data)
	chunkID := zstream.ComputeHash(data, rman.HashTypeSHA256)
	return testChunkData{
		raw:          data,
		compressed:   compressed,
		chunkID:      chunkID,
		compSize:     uint32(len(compressed)),
		uncompSize:   uint32(len(data)),
		bundleID:     bundleID,
		bundleOffset: bundleOffset,
		hashType:     rman.HashTypeSHA256,
	}
}

// makeFileEntry 从 testChunkData 构造 rman.FileEntry。
func makeTestFileEntry(path string, chunks ...testChunkData) rman.FileEntry {
	var fileSize uint64
	rmanChunks := make([]rman.ChunkInfo, len(chunks))
	for i, c := range chunks {
		rmanChunks[i] = rman.ChunkInfo{
			ChunkID:          c.chunkID,
			BundleID:         c.bundleID,
			BundleOffset:     c.bundleOffset,
			CompressedSize:   c.compSize,
			UncompressedSize: c.uncompSize,
			HashType:         c.hashType,
		}
		fileSize += uint64(c.uncompSize)
	}
	return rman.FileEntry{
		Path:     path,
		FileSize: fileSize,
		Chunks:   rmanChunks,
	}
}

// setupMockResponse 根据 testChunkData 为 mockFetcher 配置响应。
// 假设所有 Chunk 属于同一 Bundle、连续排列，生成一段 Range 数据。
func setupMockResponse(t *testing.T, mock *mockFetcher, bundleID uint64, chunks ...testChunkData) {
	t.Helper()
	var buf bytes.Buffer
	for _, c := range chunks {
		buf.Write(c.compressed)
	}
	filename := BundleFilename(bundleID)
	mock.responses[filename] = []rangeResp{{data: buf.Bytes()}}
}

// ---- 测试用例 ----

// TestDownload_BasicPipeline 验证单文件 2 个 Chunk 的完整管线：
// Map → Schedule → processBundle → 验证写盘正确。
func TestDownload_BasicPipeline(t *testing.T) {
	config, dir := makeTestConfig(t)

	const bundleID uint64 = 0xAA00000000000001

	chunk0Data := []byte("Hello, this is chunk zero data!!")
	chunk1Data := []byte("And this is chunk one data!!!!!!")

	chunk0 := makeTestChunk(t, chunk0Data, bundleID, 0)
	chunk1 := makeTestChunk(t, chunk1Data, bundleID, chunk0.compSize)

	file := makeTestFileEntry("test/output.bin", chunk0, chunk1)

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, chunk0, chunk1)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	// 消费事件（避免 channel 阻塞）
	var eventList []DownloadEvent
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for ev := range events {
			eventList = append(eventList, ev)
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 失败: %v", err)
	}

	// 验证文件内容
	outputPath := filepath.Join(dir, "test", "output.bin")
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}

	expected := append(chunk0Data, chunk1Data...)
	if !bytes.Equal(content, expected) {
		t.Errorf("文件内容不匹配:\n  实际长度: %d\n  期望长度: %d", len(content), len(expected))
	}
}

// TestDownload_FanoutWrite 验证两个文件共享同一 ChunkID 时扇出写入正确。
func TestDownload_FanoutWrite(t *testing.T) {
	config, dir := makeTestConfig(t)

	const bundleID uint64 = 0xBB00000000000001

	sharedData := []byte("shared chunk data for fanout test")
	shared := makeTestChunk(t, sharedData, bundleID, 0)

	// 两个文件都引用同一个 Chunk
	file1 := makeTestFileEntry("out/file_a.bin", shared)
	file2 := makeTestFileEntry("out/file_b.bin", shared)

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, shared)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file1, file2})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 失败: %v", err)
	}

	// 验证两个文件都有正确内容
	for _, name := range []string{"out/file_a.bin", "out/file_b.bin"} {
		content, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("读取 %s 失败: %v", name, err)
		}
		if !bytes.Equal(content, sharedData) {
			t.Errorf("%s 内容不匹配: 实际长度=%d, 期望长度=%d", name, len(content), len(sharedData))
		}
	}
}

// TestDownload_RetryOnFailure 验证前 N 次失败后重试成功的行为。
func TestDownload_RetryOnFailure(t *testing.T) {
	config, dir := makeTestConfig(t)
	config.MaxRetries = 3
	config.RetryWait = 10 * time.Millisecond

	const bundleID uint64 = 0xCC00000000000001
	chunkData := []byte("retry test data")
	chunk := makeTestChunk(t, chunkData, bundleID, 0)
	file := makeTestFileEntry("retry/output.bin", chunk)

	mock := newMockFetcher()
	mock.failUntil = 2 // 前2次失败，第3次成功
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var waits []time.Duration
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for ev := range events {
			if r, ok := ev.(EventRetry); ok {
				waits = append(waits, r.Wait)
			}
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 应在重试后成功，但失败: %v", err)
	}

	// 退避等待应按 base×2^(N-1) 指数增长
	wantWaits := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	if len(waits) != len(wantWaits) {
		t.Fatalf("期望 %d 次 EventRetry，实际 %d 次", len(wantWaits), len(waits))
	}
	for i, want := range wantWaits {
		if waits[i] != want {
			t.Errorf("第 %d 次重试 Wait = %v，期望 %v", i+1, waits[i], want)
		}
	}

	// 验证文件内容
	content, err := os.ReadFile(filepath.Join(dir, "retry", "output.bin"))
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}
	if !bytes.Equal(content, chunkData) {
		t.Error("重试后的文件内容不匹配")
	}
}

// TestDownload_AllRetriesFail 验证耗尽重试次数后的错误行为。
func TestDownload_AllRetriesFail(t *testing.T) {
	config, _ := makeTestConfig(t)
	config.MaxRetries = 2

	const bundleID uint64 = 0xDD00000000000001
	chunkData := []byte("always fail")
	chunk := makeTestChunk(t, chunkData, bundleID, 0)
	file := makeTestFileEntry("fail/output.bin", chunk)

	mock := newMockFetcher()
	mock.fetchError = fmt.Errorf("永久性网络错误")

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var hasErrorEvent bool
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for ev := range events {
			if _, ok := ev.(EventError); ok {
				hasErrorEvent = true
			}
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err == nil {
		t.Fatal("所有重试都失败后 Download 应返回 error")
	}

	if !hasErrorEvent {
		t.Error("应发出 EventError 事件")
	}
}

// TestDownload_ContextCancel 验证 context 取消后快速退出。
func TestDownload_ContextCancel(t *testing.T) {
	config, _ := makeTestConfig(t)

	const bundleID uint64 = 0xEE00000000000001
	chunkData := []byte("cancel test")
	chunk := makeTestChunk(t, chunkData, bundleID, 0)
	file := makeTestFileEntry("cancel/output.bin", chunk)

	// Mock 设置延迟（模拟慢网络），但通过 sleep 被 cancel 打断
	mock := newMockFetcher()
	mock.fetchError = fmt.Errorf("should not reach")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- dl.Download(ctx, []rman.FileEntry{file})
	}()

	select {
	case err := <-done:
		eventWg.Wait()
		if err == nil {
			// 取消后可能是 context error 或 bundle 错误，不强制要求 error
			t.Log("Download 在 cancel 后完成（可能预分配阶段就停了）")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Download 在 context cancel 后 5秒内未退出，可能存在协程泄漏")
	}
}

// TestDownload_Events 验证完整事件序列。
func TestDownload_Events(t *testing.T) {
	config, _ := makeTestConfig(t)

	const bundleID uint64 = 0xFF00000000000001
	chunkData := []byte("event test data for sequence validation")
	chunk := makeTestChunk(t, chunkData, bundleID, 0)
	file := makeTestFileEntry("events/test.bin", chunk)

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var eventList []DownloadEvent
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for ev := range events {
			eventList = append(eventList, ev)
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 失败: %v", err)
	}

	// 检查事件序列中包含关键事件
	var hasPrealloc, hasBundleStart, hasChunkDone, hasBundleDone, hasComplete bool
	for _, ev := range eventList {
		switch ev.(type) {
		case EventFilePreallocated:
			hasPrealloc = true
		case EventBundleStart:
			hasBundleStart = true
		case EventChunkDone:
			hasChunkDone = true
		case EventBundleDone:
			hasBundleDone = true
		case EventComplete:
			hasComplete = true
		}
	}

	if !hasPrealloc {
		t.Error("缺少 EventFilePreallocated 事件")
	}
	if !hasBundleStart {
		t.Error("缺少 EventBundleStart 事件")
	}
	if !hasChunkDone {
		t.Error("缺少 EventChunkDone 事件")
	}
	if !hasBundleDone {
		t.Error("缺少 EventBundleDone 事件")
	}
	if !hasComplete {
		t.Error("缺少 EventComplete 事件")
	}

	// EventComplete 应该是最后一个事件
	if len(eventList) > 0 {
		if _, ok := eventList[len(eventList)-1].(EventComplete); !ok {
			t.Errorf("最后一个事件不是 EventComplete，而是 %T", eventList[len(eventList)-1])
		}
	}

	// 验证 EventComplete 字段
	for _, ev := range eventList {
		if complete, ok := ev.(EventComplete); ok {
			if complete.TotalFiles != 1 {
				t.Errorf("EventComplete.TotalFiles = %d, 期望 1", complete.TotalFiles)
			}
			if complete.TotalChunks != 1 {
				t.Errorf("EventComplete.TotalChunks = %d, 期望 1", complete.TotalChunks)
			}
			if complete.FailedFiles != 0 {
				t.Errorf("EventComplete.FailedFiles = %d, 期望 0", complete.FailedFiles)
			}
		}
	}
}

func TestDownloadRejectsPathTraversal(t *testing.T) {
	config, dir := makeTestConfig(t)
	config.MaxRetries = -1

	const bundleID uint64 = 0xDD00000000000001
	chunk := makeTestChunk(t, []byte("path traversal payload"), bundleID, 0)
	file := makeTestFileEntry("../escape.bin", chunk)

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	err := dl.Download(context.Background(), []rman.FileEntry{file})
	if err == nil {
		t.Fatal("Download succeeded for path traversal manifest path, want error")
	}

	if _, statErr := os.Stat(filepath.Join(filepath.Dir(dir), "escape.bin")); !os.IsNotExist(statErr) {
		t.Fatalf("path traversal created file outside output dir: %v", statErr)
	}
}

func TestDownloadRejectsMismatchedRangeResponseCount(t *testing.T) {
	config, _ := makeTestConfig(t)
	config.MaxRetries = -1

	const bundleID uint64 = 0xEE00000000000001
	chunk0 := makeTestChunk(t, []byte("first range data"), bundleID, 0)
	chunk1 := makeTestChunk(t, []byte("second range data"), bundleID, chunk0.compSize+100)
	file := makeTestFileEntry("safe/output.bin", chunk0, chunk1)

	mock := newMockFetcher()
	mock.responses[BundleFilename(bundleID)] = []rangeResp{{data: chunk0.compressed}}

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range events {
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	<-done
	if err == nil {
		t.Fatal("Download succeeded with mismatched range response count, want error")
	}
}

func TestDownloadDoesNotBlockWithoutEventConsumer(t *testing.T) {
	config, dir := makeTestConfig(t)
	config.Workers = 1

	const bundleID uint64 = 0xAB00000000000001
	chunk := makeTestChunk(t, []byte("event consumer optional payload"), bundleID, 0)

	files := make([]rman.FileEntry, 0, 8)
	for i := 0; i < 8; i++ {
		files = append(files, makeTestFileEntry(fmt.Sprintf("noevents/file_%02d.bin", i), chunk))
	}

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	done := make(chan error, 1)
	go func() {
		done <- dl.Download(context.Background(), files)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Download without event consumer returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Download blocked when Events was not consumed")
	}

	content, err := os.ReadFile(filepath.Join(dir, "noevents", "file_00.bin"))
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}
	if !bytes.Equal(content, chunk.raw) {
		t.Fatal("未消费事件时写盘内容不正确")
	}
}

func TestDownloadDefaultMaxRetriesUsesThreeRetries(t *testing.T) {
	config, dir := makeTestConfig(t)
	config.MaxRetries = 0

	const bundleID uint64 = 0xAC00000000000001
	chunkData := []byte("zero value retry default data")
	chunk := makeTestChunk(t, chunkData, bundleID, 0)
	file := makeTestFileEntry("retry/default.bin", chunk)

	mock := newMockFetcher()
	mock.failUntil = 2
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range events {
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	<-done
	if err != nil {
		t.Fatalf("zero-value MaxRetries should use default retries, got error: %v", err)
	}

	mock.mu.Lock()
	callCount := mock.callCount
	mock.mu.Unlock()
	if callCount != 3 {
		t.Fatalf("FetchRanges call count = %d, want 3", callCount)
	}

	content, err := os.ReadFile(filepath.Join(dir, "retry", "default.bin"))
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}
	if !bytes.Equal(content, chunkData) {
		t.Fatal("默认重试后的文件内容不匹配")
	}
}

// ---- DownloadTasks / staging 测试 ----

// TestDownloadTasksSubset 验证 DownloadTasks 接受部分任务集时，只会向 BundleFetcher
// 请求这部分任务对应的字节区间：Map 出 10 个 Chunk 后只把其中 3 个（非相邻）灌入
// DownloadTasks，mock 只为这 3 个 Chunk 自身的字节跨度配置了响应，若实现意外请求了
// 其余 7 个 Chunk 的区间会直接报错，从而证明子集语义。
func TestDownloadTasksSubset(t *testing.T) {
	config, dir := makeTestConfig(t)

	const bundleID uint64 = 0x1000000000000001

	// 10 个 Chunk 连续排列在同一 Bundle 内，全部属于同一个文件。
	chunks := make([]testChunkData, 0, 10)
	var bundleOffset uint32
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("chunk-%02d-payload-content-data!!", i))
		c := makeTestChunk(t, data, bundleID, bundleOffset)
		chunks = append(chunks, c)
		bundleOffset += c.compSize
	}
	file := makeTestFileEntry("subset/full.bin", chunks...)

	fullTaskMap := Map([]rman.FileEntry{file})
	fullTasks := fullTaskMap[bundleID]
	if len(fullTasks) != 10 {
		t.Fatalf("Map 产出的任务数 = %d, 期望 10", len(fullTasks))
	}

	// 只取索引 1、4、7 三个非相邻任务组成子集，其余 7 个不应被下载。
	subsetIdx := []int{1, 4, 7}
	subset := make([]GlobalChunkTask, 0, len(subsetIdx))
	for _, idx := range subsetIdx {
		subset = append(subset, fullTasks[idx])
	}
	subsetTaskMap := map[uint64][]GlobalChunkTask{bundleID: subset}

	// 按精确字节区间应答：只为子集中 3 个 Chunk 自身的跨度配置响应。
	spanData := make(map[[2]int64][]byte, len(subsetIdx))
	for _, idx := range subsetIdx {
		c := chunks[idx]
		start := int64(c.bundleOffset)
		end := start + int64(c.compSize) - 1
		spanData[[2]int64{start, end}] = c.compressed
	}

	mock := newMockFetcher()
	mock.rangeFunc = func(bundleFilename string, ranges []ByteRange) ([][]byte, error) {
		result := make([][]byte, len(ranges))
		for i, r := range ranges {
			data, ok := spanData[[2]int64{r.Start, r.End}]
			if !ok {
				return nil, fmt.Errorf("mock: 请求了子集之外的区间 start=%d end=%d", r.Start, r.End)
			}
			result[i] = data
		}
		return result, nil
	}

	dl := NewDownloaderWithFetcher(config, mock)
	defer dl.filePool.Close()
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	ctx := context.Background()
	if err := dl.preallocateFiles(ctx, []rman.FileEntry{file}); err != nil {
		t.Fatalf("preallocateFiles 失败: %v", err)
	}

	failed, err := dl.DownloadTasks(ctx, subsetTaskMap, TaskOptions{ManageStaging: true})
	close(dl.events)
	eventWg.Wait()

	if err != nil {
		t.Fatalf("DownloadTasks 失败: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("failed 集合应为空，实际: %v", failed)
	}

	mock.mu.Lock()
	requested := len(mock.requestedRanges)
	mock.mu.Unlock()
	if requested != len(subsetIdx) {
		t.Errorf("请求的 Range 数 = %d, 期望 %d（子集大小）", requested, len(subsetIdx))
	}

	content, err := os.ReadFile(filepath.Join(dir, "subset", "full.bin"))
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}

	var fileOffset int64
	for i, c := range chunks {
		inSubset := false
		for _, idx := range subsetIdx {
			if idx == i {
				inSubset = true
				break
			}
		}

		want := make([]byte, c.uncompSize)
		if inSubset {
			copy(want, c.raw)
		}
		// 不在子集内的 Chunk 从未被写入，应保持 PreallocateFile 截断产生的零值填充。

		got := content[fileOffset : fileOffset+int64(c.uncompSize)]
		if !bytes.Equal(got, want) {
			t.Errorf("Chunk[%d] 内容不符 (inSubset=%v): got=%q want=%q", i, inSubset, got, want)
		}
		fileOffset += int64(c.uncompSize)
	}
}

// TestDownloadTasksNoStagingManagement 验证 ManageStaging=false 时 DownloadTasks
// 既不会自行创建/预分配 staging 文件，也不会在完成后提交或丢弃 staging：staging 的
// 创建与提交/丢弃完全归调用方负责，DownloadTasks 只对调用方已经准备好的
// StagingPath(FilePath) 执行写入。
func TestDownloadTasksNoStagingManagement(t *testing.T) {
	config, dir := makeTestConfig(t)
	config.MaxRetries = -1 // 不重试，加速失败路径

	const bundleID1 uint64 = 0x4000000000000001
	const bundleID2 uint64 = 0x4000000000000002

	readyData := []byte("chunk data for a caller-managed staging file!!!")
	readyChunk := makeTestChunk(t, readyData, bundleID1, 0)
	readyFile := makeTestFileEntry("nostage/ready.bin", readyChunk)

	missingData := []byte("chunk data for a file whose staging was never made")
	missingChunk := makeTestChunk(t, missingData, bundleID2, 0)
	missingFile := makeTestFileEntry("nostage/missing.bin", missingChunk)

	taskMap := Map([]rman.FileEntry{readyFile, missingFile})

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID1, readyChunk)
	setupMockResponse(t, mock, bundleID2, missingChunk)

	dl := NewDownloaderWithFetcher(config, mock)
	defer dl.filePool.Close()
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	readyPath := filepath.Join(dir, "nostage", "ready.bin")
	oldReadyContent := []byte("old content, must remain untouched by a non-managed staging write")
	if err := os.MkdirAll(filepath.Dir(readyPath), 0755); err != nil {
		t.Fatalf("创建目录失败: %v", err)
	}
	if err := os.WriteFile(readyPath, oldReadyContent, 0644); err != nil {
		t.Fatalf("写入旧目标文件失败: %v", err)
	}
	// 调用方（模拟增量更新编排器）自行预分配 ready.bin 的 staging，DownloadTasks 只写入。
	if err := dl.filePool.PreallocateFile(fswriter.StagingPath(readyPath), int64(len(readyData))); err != nil {
		t.Fatalf("PreallocateFile 失败: %v", err)
	}

	// missing.bin 的 staging 故意不预分配，验证 DownloadTasks 不会替调用方创建它。
	missingPath := filepath.Join(dir, "nostage", "missing.bin")

	failed, err := dl.DownloadTasks(context.Background(), taskMap, TaskOptions{ManageStaging: false})
	close(dl.events)
	eventWg.Wait()

	if err == nil {
		t.Fatal("missing.bin 的 staging 未预分配，DownloadTasks 应返回 error")
	}
	if !failed["nostage/missing.bin"] {
		t.Errorf("failed 集合应包含 nostage/missing.bin, 实际: %v", failed)
	}
	if failed["nostage/ready.bin"] {
		t.Errorf("failed 集合不应包含 nostage/ready.bin, 实际: %v", failed)
	}

	// ready.bin: 写入应落在 staging，目标文件本身保持旧内容（未提交）。
	readyContent, err := os.ReadFile(readyPath)
	if err != nil {
		t.Fatalf("读取 ready.bin 失败: %v", err)
	}
	if !bytes.Equal(readyContent, oldReadyContent) {
		t.Errorf("ManageStaging=false 不应提交 staging: got %q, want %q", readyContent, oldReadyContent)
	}

	stagingContent, err := os.ReadFile(fswriter.StagingPath(readyPath))
	if err != nil {
		t.Fatalf("读取 ready.bin 暂存文件失败: %v", err)
	}
	if !bytes.Equal(stagingContent, readyData) {
		t.Errorf("staging 内容 = %q, 期望 %q", stagingContent, readyData)
	}

	// missing.bin: 目标文件与 staging 均不应被创建——DownloadTasks 没有替调用方建任何东西。
	if _, statErr := os.Stat(missingPath); !os.IsNotExist(statErr) {
		t.Errorf("missing.bin 不应被创建")
	}
	if _, statErr := os.Stat(fswriter.StagingPath(missingPath)); !os.IsNotExist(statErr) {
		t.Errorf("missing.bin 的暂存文件不应被创建")
	}
}

// TestDownloadTasksNoStagingManagementReleasesHandlesForCallerRename 验证
// ManageStaging=false 时，DownloadTasks 返回前会释放它在处理过程中于 FilePool
// 打开的 staging 句柄：调用方（增量更新编排器）紧随其后对同一 staging 路径执行
// os.Rename 必须成功——Windows 下对仍被打开的文件 rename 会失败，这是本测试
// 要固化的跨平台行为。
func TestDownloadTasksNoStagingManagementReleasesHandlesForCallerRename(t *testing.T) {
	config, dir := makeTestConfig(t)

	const bundleID uint64 = 0x5000000000000001
	data := []byte("handle must be released before DownloadTasks(false) returns")
	chunk := makeTestChunk(t, data, bundleID, 0)
	file := makeTestFileEntry("release/target.bin", chunk)

	taskMap := Map([]rman.FileEntry{file})

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	defer dl.filePool.Close()
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	targetPath := filepath.Join(dir, "release", "target.bin")
	stagingPath := fswriter.StagingPath(targetPath)
	if err := dl.filePool.PreallocateFile(stagingPath, int64(len(data))); err != nil {
		t.Fatalf("PreallocateFile 失败: %v", err)
	}

	failed, err := dl.DownloadTasks(context.Background(), taskMap, TaskOptions{ManageStaging: false})
	close(dl.events)
	eventWg.Wait()

	if err != nil {
		t.Fatalf("DownloadTasks 失败: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("failed 集合应为空，实际: %v", failed)
	}

	// 调用方在此处自行提交 staging（模拟 pkg/update.Sync 的第 6 步）：
	// 若 DownloadTasks 没有释放句柄，Windows 下这一步会失败。
	if err := os.Rename(stagingPath, targetPath); err != nil {
		t.Fatalf("DownloadTasks(false) 返回后调用方 rename staging 失败（句柄未释放）: %v", err)
	}

	content, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("读取提交后的目标文件失败: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("提交后内容不匹配: got %q, want %q", content, data)
	}
}

// TestDownloadWritesStagingAndCommits 验证既有 Download 路径下 staging 的提交时机：
// 目标旧文件在全部作业完成之前字节必须保持不变（写入只落 staging），只有全部作业
// 成功后才会被原子替换为新内容。
func TestDownloadWritesStagingAndCommits(t *testing.T) {
	config, dir := makeTestConfig(t)

	const bundleID uint64 = 0x2000000000000001
	oldContent := []byte("old file content, should remain until commit!!")
	newData := []byte("brand new committed content data!!")
	chunk := makeTestChunk(t, newData, bundleID, 0)
	file := makeTestFileEntry("commit/target.bin", chunk)

	targetPath := filepath.Join(dir, "commit", "target.bin")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatalf("创建目录失败: %v", err)
	}
	if err := os.WriteFile(targetPath, oldContent, 0644); err != nil {
		t.Fatalf("写入旧目标文件失败: %v", err)
	}

	block := make(chan struct{})
	proceed := make(chan struct{})
	mock := newMockFetcher()
	mock.rangeFunc = func(bundleFilename string, ranges []ByteRange) ([][]byte, error) {
		close(block) // 通知主线程：下载即将写入，可以检查目标文件当前状态
		<-proceed    // 等待主线程放行，模拟下载仍在进行中
		return [][]byte{chunk.compressed}, nil
	}

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	done := make(chan error, 1)
	go func() {
		done <- dl.Download(context.Background(), []rman.FileEntry{file})
	}()

	<-block
	// 下载仍阻塞在 FetchRanges 内部，尚未写入任何 staging 数据：目标文件应保持旧内容。
	mid, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("读取目标文件失败: %v", err)
	}
	if !bytes.Equal(mid, oldContent) {
		t.Errorf("下载完成前目标文件已被改动: got %q, want %q", mid, oldContent)
	}
	close(proceed)

	if err := <-done; err != nil {
		t.Fatalf("Download 失败: %v", err)
	}
	eventWg.Wait()

	final, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("读取目标文件失败: %v", err)
	}
	if !bytes.Equal(final, newData) {
		t.Errorf("Download 完成后目标文件未被替换为新内容: got %q, want %q", final, newData)
	}

	if _, statErr := os.Stat(fswriter.StagingPath(targetPath)); !os.IsNotExist(statErr) {
		t.Errorf("提交后暂存文件应已消失, stat err=%v", statErr)
	}
}

// TestFailedFileKeepsOldAndDiscardsStaging 验证一个文件关联的 Bundle 作业最终失败
// （重试耗尽）时：旧文件保持不变，半成品 staging 被丢弃。
func TestFailedFileKeepsOldAndDiscardsStaging(t *testing.T) {
	config, dir := makeTestConfig(t)
	config.MaxRetries = -1 // 不重试，加速失败路径

	const bundleID uint64 = 0x3000000000000001
	oldContent := []byte("old content that must survive a failed download")
	chunk := makeTestChunk(t, []byte("irrelevant new data, never lands on disk"), bundleID, 0)
	file := makeTestFileEntry("fail/keep.bin", chunk)

	targetPath := filepath.Join(dir, "fail", "keep.bin")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		t.Fatalf("创建目录失败: %v", err)
	}
	if err := os.WriteFile(targetPath, oldContent, 0644); err != nil {
		t.Fatalf("写入旧目标文件失败: %v", err)
	}

	mock := newMockFetcher()
	mock.fetchError = fmt.Errorf("永久性网络错误")

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err == nil {
		t.Fatal("Download 应因 Bundle 失败返回 error")
	}

	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("读取目标文件失败: %v", err)
	}
	if !bytes.Equal(got, oldContent) {
		t.Errorf("失败后旧文件应保持不变: got %q, want %q", got, oldContent)
	}

	if _, statErr := os.Stat(fswriter.StagingPath(targetPath)); !os.IsNotExist(statErr) {
		t.Errorf("失败后暂存文件应已被丢弃, stat err=%v", statErr)
	}
}

// ---- retryWait 指数退避计算 ----

func TestRetryWaitExponentialGrowthWithCap(t *testing.T) {
	cases := []struct {
		base    time.Duration
		attempt int
		want    time.Duration
	}{
		{time.Second, 1, time.Second},
		{time.Second, 2, 2 * time.Second},
		{time.Second, 3, 4 * time.Second},
		{4 * time.Second, 4, 32 * time.Second},
		{4 * time.Second, 5, 60 * time.Second}, // 4s×2^4=64s，封顶 maxRetryWait
		{2 * time.Minute, 1, 2 * time.Minute},  // base 大于默认上限时以 base 为上限
		{2 * time.Minute, 3, 2 * time.Minute},
		{0, 1, time.Second},                  // 非法 base 兜底 1s
		{time.Second, 100, 60 * time.Second}, // 大 attempt 不溢出，稳定封顶
	}
	for _, c := range cases {
		if got := retryWait(c.base, c.attempt); got != c.want {
			t.Errorf("retryWait(%v, %d) = %v，期望 %v", c.base, c.attempt, got, c.want)
		}
	}
}

func TestNewDownloaderDefaultRetryWait(t *testing.T) {
	dl := NewDownloaderWithFetcher(DownloadConfig{}, newMockFetcher())
	if dl.config.RetryWait != time.Second {
		t.Errorf("RetryWait 零值应默认 1s，实际 %v", dl.config.RetryWait)
	}
}

// ---- FetchOptions 接线：整包判定、域名轮转、实际字节口径 ----

// TestFullBundleEndToEnd 验证 DownloadConfig 携带 BundleSizes+FullBundleThreshold
// 且覆盖率达标时：BundleFetcher 收到的 FetchOptions.FullBundleSize>0，请求的
// Range 是 Schedule 合并后的结果（跨 Gap 的单段，而非按 Chunk 拆分的多段），
// 且最终落盘数据仍然正确（gap 部分的垃圾字节不会被误写入文件）。
func TestFullBundleEndToEnd(t *testing.T) {
	config, dir := makeTestConfig(t)

	const bundleID uint64 = 0x7000000000000001
	const gapFillerSize = 5

	chunk0Data := []byte("full bundle chunk zero data!!!!")
	chunk1Data := []byte("full bundle chunk one data!!!!!")

	chunk0 := makeTestChunk(t, chunk0Data, bundleID, 0)
	chunk1 := makeTestChunk(t, chunk1Data, bundleID, chunk0.compSize+gapFillerSize)

	file := makeTestFileEntry("fullbundle/output.bin", chunk0, chunk1)

	mergedSpan := int64(chunk1.bundleOffset) + int64(chunk1.compSize)

	mock := newMockFetcher()
	mock.rangeFunc = func(bundleFilename string, ranges []ByteRange) ([][]byte, error) {
		if len(ranges) != 1 {
			return nil, fmt.Errorf("mock: 期望合并为 1 段 Range，实际 %d 段", len(ranges))
		}
		if ranges[0].Start != 0 || ranges[0].End != mergedSpan-1 {
			return nil, fmt.Errorf("mock: 期望合并区间 [0,%d]，实际 [%d,%d]", mergedSpan-1, ranges[0].Start, ranges[0].End)
		}
		var buf bytes.Buffer
		buf.Write(chunk0.compressed)
		buf.Write(bytes.Repeat([]byte{0xFF}, gapFillerSize)) // gap 内的垃圾字节，不应落盘
		buf.Write(chunk1.compressed)
		return [][]byte{buf.Bytes()}, nil
	}

	config.GapTolerance = gapFillerSize
	config.FullBundleThreshold = SuggestedFullBundleThreshold
	config.BundleSizes = map[uint64]uint32{bundleID: uint32(mergedSpan)}

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var eventList []DownloadEvent
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for ev := range events {
			eventList = append(eventList, ev)
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 失败: %v", err)
	}

	mock.mu.Lock()
	opts := append([]FetchOptions(nil), mock.requestedOpts...)
	mock.mu.Unlock()
	if len(opts) != 1 {
		t.Fatalf("期望 1 次 FetchRanges 调用，实际 %d 次", len(opts))
	}
	if opts[0].FullBundleSize != mergedSpan {
		t.Errorf("FetchOptions.FullBundleSize = %d, 期望 %d", opts[0].FullBundleSize, mergedSpan)
	}

	var hasBundleStart bool
	for _, ev := range eventList {
		if start, ok := ev.(EventBundleStart); ok {
			hasBundleStart = true
			if !start.FullBundle {
				t.Error("EventBundleStart.FullBundle 应为 true")
			}
		}
	}
	if !hasBundleStart {
		t.Fatal("缺少 EventBundleStart 事件")
	}

	content, err := os.ReadFile(filepath.Join(dir, "fullbundle", "output.bin"))
	if err != nil {
		t.Fatalf("读取输出文件失败: %v", err)
	}
	expected := append(append([]byte{}, chunk0Data...), chunk1Data...)
	if !bytes.Equal(content, expected) {
		t.Errorf("文件内容不匹配:\n  实际: %q\n  期望: %q", content, expected)
	}
}

// TestRetryRotatesURLHint 验证重试时 attempt 递增使相邻两次 FetchOptions.URLHint
// 相差 1（统一公式 URLHint = BundleID + attempt）。
func TestRetryRotatesURLHint(t *testing.T) {
	config, _ := makeTestConfig(t)
	config.RetryWait = 10 * time.Millisecond

	const bundleID uint64 = 0x7100000000000001
	chunk := makeTestChunk(t, []byte("retry url hint rotation data!!!"), bundleID, 0)
	file := makeTestFileEntry("urlhint/retry.bin", chunk)

	mock := newMockFetcher()
	mock.failUntil = 1 // 第 1 次（attempt=0）失败，第 2 次（attempt=1）成功
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 应在重试后成功，但失败: %v", err)
	}

	mock.mu.Lock()
	opts := append([]FetchOptions(nil), mock.requestedOpts...)
	mock.mu.Unlock()
	if len(opts) != 2 {
		t.Fatalf("期望 2 次 FetchRanges 调用，实际 %d 次", len(opts))
	}
	if opts[1].URLHint-opts[0].URLHint != 1 {
		t.Errorf("两次 URLHint 应相差 1: 第1次=%d 第2次=%d", opts[0].URLHint, opts[1].URLHint)
	}
	if opts[0].URLHint != bundleID {
		t.Errorf("首次 URLHint 应等于 BundleID: 实际=%d 期望=%d", opts[0].URLHint, bundleID)
	}
}

// TestURLHintIsBundleID 验证无重试（首次即成功）时 URLHint == BundleID。
func TestURLHintIsBundleID(t *testing.T) {
	config, _ := makeTestConfig(t)

	const bundleID uint64 = 0x7200000000000001
	chunk := makeTestChunk(t, []byte("no retry url hint data!!!!!!!!!!"), bundleID, 0)
	file := makeTestFileEntry("urlhint/noretry.bin", chunk)

	mock := newMockFetcher()
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for range events {
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 失败: %v", err)
	}

	mock.mu.Lock()
	opts := append([]FetchOptions(nil), mock.requestedOpts...)
	mock.mu.Unlock()
	if len(opts) != 1 {
		t.Fatalf("期望 1 次 FetchRanges 调用，实际 %d 次", len(opts))
	}
	if opts[0].URLHint != bundleID {
		t.Errorf("URLHint = %d, 期望等于 BundleID %d", opts[0].URLHint, bundleID)
	}
}

// TestEventFetchedBytes 验证 EventBundleDone.FetchedBytes 的两种口径：
// Range 作业等于 Σ(End-Start+1)（合并后的 Range 段宽度之和，而非解压后的
// Chunk 原始大小），整包作业等于配置的 BundleSize。
func TestEventFetchedBytes(t *testing.T) {
	t.Run("Range作业", func(t *testing.T) {
		config, _ := makeTestConfig(t)

		const bundleID uint64 = 0x7300000000000001
		chunk0 := makeTestChunk(t, []byte("fetched bytes chunk zero data!!"), bundleID, 0)
		chunk1 := makeTestChunk(t, []byte("fetched bytes chunk one data!!!"), bundleID, chunk0.compSize)
		file := makeTestFileEntry("fetchedbytes/range.bin", chunk0, chunk1)

		mock := newMockFetcher()
		setupMockResponse(t, mock, bundleID, chunk0, chunk1)

		dl := NewDownloaderWithFetcher(config, mock)
		events := dl.Events()

		var eventList []DownloadEvent
		var eventWg sync.WaitGroup
		eventWg.Add(1)
		go func() {
			defer eventWg.Done()
			for ev := range events {
				eventList = append(eventList, ev)
			}
		}()

		err := dl.Download(context.Background(), []rman.FileEntry{file})
		eventWg.Wait()
		if err != nil {
			t.Fatalf("Download 失败: %v", err)
		}

		wantBytes := int64(chunk0.compSize) + int64(chunk1.compSize)
		var found bool
		for _, ev := range eventList {
			if done, ok := ev.(EventBundleDone); ok {
				found = true
				if done.FetchedBytes != wantBytes {
					t.Errorf("EventBundleDone.FetchedBytes = %d, 期望 %d", done.FetchedBytes, wantBytes)
				}
			}
		}
		if !found {
			t.Fatal("缺少 EventBundleDone 事件")
		}
	})

	t.Run("整包作业", func(t *testing.T) {
		config, _ := makeTestConfig(t)

		const bundleID uint64 = 0x7400000000000001
		chunk := makeTestChunk(t, []byte("fetched bytes full bundle data!"), bundleID, 0)
		file := makeTestFileEntry("fetchedbytes/full.bin", chunk)

		const bundleSize = uint32(4096) // 假定 Bundle 实际总大小大于本次覆盖的字节

		mock := newMockFetcher()
		setupMockResponse(t, mock, bundleID, chunk)

		config.FullBundleThreshold = 0.01 // 极低阈值，确保判定为整包
		config.BundleSizes = map[uint64]uint32{bundleID: bundleSize}

		dl := NewDownloaderWithFetcher(config, mock)
		events := dl.Events()

		var eventList []DownloadEvent
		var eventWg sync.WaitGroup
		eventWg.Add(1)
		go func() {
			defer eventWg.Done()
			for ev := range events {
				eventList = append(eventList, ev)
			}
		}()

		err := dl.Download(context.Background(), []rman.FileEntry{file})
		eventWg.Wait()
		if err != nil {
			t.Fatalf("Download 失败: %v", err)
		}

		var found bool
		for _, ev := range eventList {
			if done, ok := ev.(EventBundleDone); ok {
				found = true
				if done.FetchedBytes != int64(bundleSize) {
					t.Errorf("EventBundleDone.FetchedBytes = %d, 期望等于 BundleSize %d", done.FetchedBytes, bundleSize)
				}
			}
		}
		if !found {
			t.Fatal("缺少 EventBundleDone 事件")
		}
	})
}
