package core

import (
	"testing"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// ---- 测试数据辅助 ----

// makeChunk 构造一个 ChunkInfo，用于 Mapper 测试。
func makeChunk(chunkID, bundleID uint64, bundleOffset, compSize, uncompSize uint32) rman.ChunkInfo {
	return rman.ChunkInfo{
		ChunkID:          chunkID,
		BundleID:         bundleID,
		BundleOffset:     bundleOffset,
		CompressedSize:   compSize,
		UncompressedSize: uncompSize,
		HashType:         rman.HashTypeSHA256,
	}
}

// makeFileEntry 构造一个带有 chunks 的 FileEntry，用于 Mapper 测试。
func makeFileEntry(path string, chunks ...rman.ChunkInfo) rman.FileEntry {
	return rman.FileEntry{
		Path:   path,
		Flags:  nil,
		Chunks: chunks,
	}
}

// ---- 测试用例 ----

// TestMap_EmptyInput 验证空输入返回空 map（非 nil）。
func TestMap_EmptyInput(t *testing.T) {
	result := Map(nil)
	if result == nil {
		t.Error("Map(nil) 不应返回 nil，期望返回空 map")
	}
	if len(result) != 0 {
		t.Errorf("Map(nil) 期望空 map，但得到 %d 个 key", len(result))
	}

	result2 := Map([]rman.FileEntry{})
	if len(result2) != 0 {
		t.Errorf("Map([]) 期望空 map，但得到 %d 个 key", len(result2))
	}
}

// TestMap_SingleFile 验证单文件的 Chunk 被正确分组到对应 BundleID。
func TestMap_SingleFile(t *testing.T) {
	const (
		bundleID uint64 = 0xDEADBEEF00000001
		chunkA   uint64 = 0xAAAA0001
		chunkB   uint64 = 0xAAAA0002
	)

	files := []rman.FileEntry{
		makeFileEntry("file_a.bin",
			makeChunk(chunkA, bundleID, 0, 100, 200),
			makeChunk(chunkB, bundleID, 100, 150, 300),
		),
	}

	result := Map(files)

	// 应只有 1 个 BundleID
	if len(result) != 1 {
		t.Fatalf("期望 1 个 BundleID，得到 %d 个", len(result))
	}

	tasks, ok := result[bundleID]
	if !ok {
		t.Fatalf("BundleID=%X 不在结果中", bundleID)
	}

	// 应有 2 个 GlobalChunkTask
	if len(tasks) != 2 {
		t.Fatalf("期望 2 个 GlobalChunkTask，得到 %d 个", len(tasks))
	}

	// 每个 Task 应有 1 个 WriteTarget
	for _, task := range tasks {
		if len(task.Targets) != 1 {
			t.Errorf("ChunkID=%X 期望 1 个 WriteTarget，得到 %d 个",
				task.Targets[0].ChunkID, len(task.Targets))
		}
	}
}

// TestMap_ChunkDedup 验证两个文件共享同一 ChunkID 时，Targets 被正确合并。
//
// DoD 核心检测：相同 ChunkID 只出现一次，WriteTarget 数量=引用该 Chunk 的文件数。
func TestMap_ChunkDedup(t *testing.T) {
	const (
		bundleID    uint64 = 0x1111111111111111
		sharedChunk uint64 = 0x5AABED // 被两个文件共享的 ChunkID
	)

	// 两个文件都引用同一个 Chunk
	files := []rman.FileEntry{
		makeFileEntry("fileA.bin",
			makeChunk(sharedChunk, bundleID, 0, 500, 1000),
		),
		makeFileEntry("fileB.bin",
			makeChunk(sharedChunk, bundleID, 0, 500, 1000),
		),
	}

	result := Map(files)

	tasks, ok := result[bundleID]
	if !ok {
		t.Fatalf("BundleID=%X 不在结果中", bundleID)
	}

	// 关键：ChunkID 去重后只有 1 个 GlobalChunkTask
	if len(tasks) != 1 {
		t.Fatalf("共享 ChunkID 去重后期望 1 个 GlobalChunkTask，得到 %d 个", len(tasks))
	}

	task := tasks[0]
	// Targets 应有 2 个（两个文件各一个写入目标）
	if len(task.Targets) != 2 {
		t.Fatalf("共享 ChunkID 期望 2 个 WriteTarget（扇出），得到 %d 个", len(task.Targets))
	}

	// 验证两个 WriteTarget 对应不同文件
	paths := map[string]bool{}
	for _, target := range task.Targets {
		paths[target.FilePath] = true
	}
	if !paths["fileA.bin"] || !paths["fileB.bin"] {
		t.Errorf("WriteTarget 中缺少预期文件路径，得到: %v", paths)
	}
}

// TestMap_BundleCount 验证 DoD：输出 Bundle 数量 ≤ 输入文件中不同 BundleID 的数量。
func TestMap_BundleCount(t *testing.T) {
	const (
		bundle1 uint64 = 0xBBBB000000000001
		bundle2 uint64 = 0xBBBB000000000002
	)

	files := []rman.FileEntry{
		makeFileEntry("fileA.bin",
			makeChunk(0xA001, bundle1, 0, 100, 200),
			makeChunk(0xA002, bundle1, 100, 100, 200),
			makeChunk(0xB001, bundle2, 0, 100, 200),
		),
		makeFileEntry("fileB.bin",
			makeChunk(0xA001, bundle1, 0, 100, 200), // 与 fileA 共享，应去重
			makeChunk(0xC001, bundle2, 100, 100, 200),
		),
	}

	// 输入中涉及的不同 BundleID 数量
	expectedBundleCount := 2

	result := Map(files)

	if len(result) > expectedBundleCount {
		t.Errorf("输出 Bundle 数 %d > 输入不同 Bundle 数 %d，违反 DoD",
			len(result), expectedBundleCount)
	}
	if len(result) != expectedBundleCount {
		t.Errorf("期望 %d 个 Bundle，得到 %d 个", expectedBundleCount, len(result))
	}

	// ChunkID=0xA001 应只出现一次（去重）
	totalA001 := 0
	for _, tasks := range result {
		for _, task := range tasks {
			if task.Targets[0].ChunkID == 0xA001 {
				totalA001++
			}
		}
	}
	if totalA001 != 1 {
		t.Errorf("ChunkID=0xA001 期望出现 1 次（去重后），实际出现 %d 次", totalA001)
	}
}

// TestMap_SortedByOffset 验证同一 Bundle 内的 GlobalChunkTask 按 BundleOffset 升序排列。
func TestMap_SortedByOffset(t *testing.T) {
	const bundleID uint64 = 0xCCCC000000000001

	// 故意以乱序方式构造（BundleOffset: 300, 0, 150）
	files := []rman.FileEntry{
		makeFileEntry("file.bin",
			makeChunk(0xD003, bundleID, 300, 100, 200),
			makeChunk(0xD001, bundleID, 0, 100, 200),
			makeChunk(0xD002, bundleID, 150, 100, 200),
		),
	}

	result := Map(files)

	tasks, ok := result[bundleID]
	if !ok {
		t.Fatal("BundleID 不在结果中")
	}
	if len(tasks) != 3 {
		t.Fatalf("期望 3 个 Task，得到 %d", len(tasks))
	}

	// 验证按 BundleOffset 升序
	for i := 1; i < len(tasks); i++ {
		if tasks[i].BundleOffset < tasks[i-1].BundleOffset {
			t.Errorf("Task[%d].BundleOffset=%d < Task[%d].BundleOffset=%d，未按升序排列",
				i, tasks[i].BundleOffset, i-1, tasks[i-1].BundleOffset)
		}
	}

	// 具体验证排列顺序：0, 150, 300
	wantOffsets := []uint32{0, 150, 300}
	for i, want := range wantOffsets {
		if tasks[i].BundleOffset != want {
			t.Errorf("Task[%d].BundleOffset=%d，期望 %d", i, tasks[i].BundleOffset, want)
		}
	}
}

// TestMap_MultipleChunksPerBundle 验证多文件、多 Bundle 的综合分组正确性。
func TestMap_MultipleChunksPerBundle(t *testing.T) {
	const (
		bundleX uint64 = 0xEEEE000000000001
		bundleY uint64 = 0xEEEE000000000002
		bundleZ uint64 = 0xEEEE000000000003
	)

	files := []rman.FileEntry{
		makeFileEntry("alpha.bin",
			makeChunk(0xE001, bundleX, 0, 100, 200),
			makeChunk(0xE002, bundleY, 0, 100, 200),
		),
		makeFileEntry("beta.bin",
			makeChunk(0xE003, bundleX, 100, 100, 200),
			makeChunk(0xE004, bundleZ, 0, 100, 200),
		),
		makeFileEntry("gamma.bin",
			makeChunk(0xE001, bundleX, 0, 100, 200), // 与 alpha 共享
			makeChunk(0xE005, bundleZ, 100, 100, 200),
		),
	}

	result := Map(files)

	// 应有 3 个 Bundle（X, Y, Z）
	if len(result) != 3 {
		t.Fatalf("期望 3 个 Bundle，得到 %d", len(result))
	}

	// bundleX 应有 2 个 Task（E001 被去重，E003 独立）
	tasksX := result[bundleX]
	if len(tasksX) != 2 {
		t.Errorf("bundleX 期望 2 个 Task，得到 %d", len(tasksX))
	}

	// E001 应有 2 个 WriteTarget（alpha 和 gamma 都引用了它）
	for _, task := range tasksX {
		if task.BundleOffset == 0 { // E001 的 offset
			if len(task.Targets) != 2 {
				t.Errorf("ChunkID=E001 期望 2 个 WriteTarget，得到 %d", len(task.Targets))
			}
		}
	}

	// bundleY, bundleZ 各有各自的 Task
	if len(result[bundleY]) != 1 {
		t.Errorf("bundleY 期望 1 个 Task，得到 %d", len(result[bundleY]))
	}
	if len(result[bundleZ]) != 2 {
		t.Errorf("bundleZ 期望 2 个 Task，得到 %d", len(result[bundleZ]))
	}
}

// TestFileOffset_Accumulation 验证 WriteTarget.FileOffset 正确按 UncompressedSize 累加。
//
// 该值由 Mapper 根据 Chunk 在文件内的顺序和 UncompressedSize 计算，
// 供后续 Streamer 层的 WriteAt 使用。
func TestFileOffset_Accumulation(t *testing.T) {
	const bundleID uint64 = 0xFF00FF00FF00FF00

	// 单文件，3 个 chunk，UncompressedSize: 200, 300, 400
	files := []rman.FileEntry{
		makeFileEntry("target.bin",
			makeChunk(0xF001, bundleID, 0, 100, 200),
			makeChunk(0xF002, bundleID, 100, 100, 300),
			makeChunk(0xF003, bundleID, 200, 100, 400),
		),
	}

	result := Map(files)
	tasks := result[bundleID]

	// 按 BundleOffset 重建顺序后检查 FileOffset
	type wantEntry struct {
		bundleOffset uint32
		fileOffset   int64
	}
	wants := []wantEntry{
		{0, 0},
		{100, 200},  // 第 2 个 chunk，FileOffset = 前 chunk 解压大小 200
		{200, 500},  // 第 3 个 chunk，FileOffset = 200 + 300
	}

	taskByOffset := make(map[uint32]GlobalChunkTask, len(tasks))
	for _, task := range tasks {
		taskByOffset[task.BundleOffset] = task
	}

	for _, w := range wants {
		task, ok := taskByOffset[w.bundleOffset]
		if !ok {
			t.Errorf("BundleOffset=%d 对应的 Task 不存在", w.bundleOffset)
			continue
		}
		if len(task.Targets) == 0 {
			t.Errorf("BundleOffset=%d 的 Task 没有 WriteTarget", w.bundleOffset)
			continue
		}
		if task.Targets[0].FileOffset != w.fileOffset {
			t.Errorf("BundleOffset=%d 的 FileOffset=%d，期望 %d",
				w.bundleOffset, task.Targets[0].FileOffset, w.fileOffset)
		}
	}
}
