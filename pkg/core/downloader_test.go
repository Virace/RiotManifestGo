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

	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/klauspost/compress/zstd"
)

// ---- Mock BundleFetcher ----

// mockFetcher 是 BundleFetcher 的测试 mock 实现。
type mockFetcher struct {
	mu         sync.Mutex
	callCount  int
	failUntil  int                    // 前 failUntil 次调用返回 error
	responses  map[string][]rangeResp // bundleFilename → Range 级别响应
	fetchError error                  // 始终返回的 error（优先于 failUntil）
}

type rangeResp struct {
	data []byte
}

func newMockFetcher() *mockFetcher {
	return &mockFetcher{
		responses: make(map[string][]rangeResp),
	}
}

func (m *mockFetcher) FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange) ([][]byte, error) {
	m.mu.Lock()
	m.callCount++
	count := m.callCount
	m.mu.Unlock()

	if m.fetchError != nil {
		return nil, m.fetchError
	}

	if count <= m.failUntil {
		return nil, fmt.Errorf("mock 第 %d 次调用失败（模拟网络错误）", count)
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

	const bundleID uint64 = 0xCC00000000000001
	chunkData := []byte("retry test data")
	chunk := makeTestChunk(t, chunkData, bundleID, 0)
	file := makeTestFileEntry("retry/output.bin", chunk)

	mock := newMockFetcher()
	mock.failUntil = 2 // 前2次失败，第3次成功
	setupMockResponse(t, mock, bundleID, chunk)

	dl := NewDownloaderWithFetcher(config, mock)
	events := dl.Events()

	var retryCount int
	var eventWg sync.WaitGroup
	eventWg.Add(1)
	go func() {
		defer eventWg.Done()
		for ev := range events {
			if _, ok := ev.(EventRetry); ok {
				retryCount++
			}
		}
	}()

	err := dl.Download(context.Background(), []rman.FileEntry{file})
	eventWg.Wait()

	if err != nil {
		t.Fatalf("Download 应在重试后成功，但失败: %v", err)
	}

	if retryCount != 2 {
		t.Errorf("期望 2 次 EventRetry，实际 %d 次", retryCount)
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
			if complete.FailedBundles != 0 {
				t.Errorf("EventComplete.FailedBundles = %d, 期望 0", complete.FailedBundles)
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
