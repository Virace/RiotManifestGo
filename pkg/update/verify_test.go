package update

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// ---- 测试数据辅助 ----

// randBytes 生成 n 字节真实随机数据，用于构造 chunk 内容（非 mock）。
func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("生成随机数据失败: %v", err)
	}
	return buf
}

// concatBytes 依次拼接多段字节切片。
func concatBytes(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// chunkFromData 按给定数据与哈希算法构造 ChunkInfo，ChunkID 用 zstream.ComputeHash 真实算出，
// 与生产代码路径一致（非硬编码常量）。
func chunkFromData(data []byte, ht rman.HashType) rman.ChunkInfo {
	return rman.ChunkInfo{
		ChunkID:          zstream.ComputeHash(data, ht),
		UncompressedSize: uint32(len(data)),
		HashType:         ht,
	}
}

// chunkNoneFromData 构造一个 HashType=HashTypeNone 的 ChunkInfo，但 ChunkID 用 guessHT
// 算出，模拟清单中该文件无 params 条目、数据本身完好、需靠穷举猜测才能命中的场景。
func chunkNoneFromData(data []byte, guessHT rman.HashType) rman.ChunkInfo {
	return rman.ChunkInfo{
		ChunkID:          zstream.ComputeHash(data, guessHT),
		UncompressedSize: uint32(len(data)),
		HashType:         rman.HashTypeNone,
	}
}

// writeFile 将 data 写入 dir 下的 name 文件，返回完整路径。
func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("写入测试文件失败: %v", err)
	}
	return path
}

// ---- 测试用例 ----

// TestVerifyFileChunks_AllHits 验证本地文件内容与 manifest 完全一致时全部 Chunk 命中，
// 且 FileOffset 按 Chunks 顺序累加 UncompressedSize 得出。
func TestVerifyFileChunks_AllHits(t *testing.T) {
	dir := t.TempDir()
	d0 := randBytes(t, 37)
	d1 := randBytes(t, 128)
	d2 := randBytes(t, 5)

	c0 := chunkFromData(d0, rman.HashTypeSHA256)
	c1 := chunkFromData(d1, rman.HashTypeBlake3)
	c2 := chunkFromData(d2, rman.HashTypeHKDF)

	path := writeFile(t, dir, "f.bin", concatBytes(d0, d1, d2))
	entry := rman.FileEntry{Path: "f.bin", Chunks: []rman.ChunkInfo{c0, c1, c2}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if !result.Exists {
		t.Error("Exists 期望 true")
	}
	if len(result.Misses) != 0 {
		t.Errorf("Misses 期望空，got %d", len(result.Misses))
	}
	if len(result.Hits) != 3 {
		t.Fatalf("Hits 期望 3 个，got %d", len(result.Hits))
	}

	wantOffsets := []int64{0, int64(len(d0)), int64(len(d0) + len(d1))}
	for i, want := range wantOffsets {
		if result.Hits[i].FileOffset != want {
			t.Errorf("Hits[%d].FileOffset = %d, want %d", i, result.Hits[i].FileOffset, want)
		}
	}
}

// TestVerifyFileChunks_PartialHit 验证中间某个 Chunk 磁盘内容损坏时，
// 只有该 Chunk 落入 Misses，其余仍然命中。
func TestVerifyFileChunks_PartialHit(t *testing.T) {
	dir := t.TempDir()
	d0 := randBytes(t, 20)
	d1 := randBytes(t, 20)
	d2 := randBytes(t, 20)
	bad1 := randBytes(t, 20) // 磁盘上第二段实际写入的是这份数据，而非 d1

	c0 := chunkFromData(d0, rman.HashTypeSHA256)
	c1 := chunkFromData(d1, rman.HashTypeSHA256)
	c2 := chunkFromData(d2, rman.HashTypeSHA256)

	path := writeFile(t, dir, "f.bin", concatBytes(d0, bad1, d2))
	entry := rman.FileEntry{Path: "f.bin", Chunks: []rman.ChunkInfo{c0, c1, c2}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if len(result.Hits) != 2 {
		t.Fatalf("Hits 期望 2 个，got %d", len(result.Hits))
	}
	if len(result.Misses) != 1 {
		t.Fatalf("Misses 期望 1 个，got %d", len(result.Misses))
	}
	if result.Misses[0].FileOffset != int64(len(d0)) {
		t.Errorf("Misses[0].FileOffset = %d, want %d", result.Misses[0].FileOffset, len(d0))
	}
	if result.Misses[0].Chunk.ChunkID != c1.ChunkID {
		t.Errorf("Misses[0].Chunk.ChunkID = %X, want %X", result.Misses[0].Chunk.ChunkID, c1.ChunkID)
	}
}

// TestVerifyFileChunks_FileNotExist 验证本地文件不存在时 Exists=false，且全部 Chunk 归入 Misses。
func TestVerifyFileChunks_FileNotExist(t *testing.T) {
	dir := t.TempDir()
	d0 := randBytes(t, 10)
	d1 := randBytes(t, 15)
	c0 := chunkFromData(d0, rman.HashTypeSHA256)
	c1 := chunkFromData(d1, rman.HashTypeSHA512)

	path := filepath.Join(dir, "missing.bin") // 不写入该文件
	entry := rman.FileEntry{Path: "missing.bin", Chunks: []rman.ChunkInfo{c0, c1}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if result.Exists {
		t.Error("Exists 期望 false")
	}
	if len(result.Hits) != 0 {
		t.Errorf("Hits 期望空，got %d", len(result.Hits))
	}
	if len(result.Misses) != 2 {
		t.Fatalf("Misses 期望 2 个，got %d", len(result.Misses))
	}
	if result.Misses[0].FileOffset != 0 || result.Misses[1].FileOffset != int64(len(d0)) {
		t.Errorf("Misses 偏移不符: got [%d %d], want [0 %d]",
			result.Misses[0].FileOffset, result.Misses[1].FileOffset, len(d0))
	}
}

// TestVerifyFileChunks_ShortFile 验证文件偏短、尾部 Chunk 越界时该 Chunk 判 miss，
// 前面完整的 Chunk 仍然命中。
func TestVerifyFileChunks_ShortFile(t *testing.T) {
	dir := t.TempDir()
	d0 := randBytes(t, 30)
	d1 := randBytes(t, 30) // 声明的第二个 chunk，但磁盘文件会被截断，读不到完整内容

	c0 := chunkFromData(d0, rman.HashTypeSHA256)
	c1 := chunkFromData(d1, rman.HashTypeSHA256)

	// 磁盘文件只写入 d0 + d1 的前 10 字节，模拟文件被截断/偏短。
	path := writeFile(t, dir, "f.bin", concatBytes(d0, d1[:10]))
	entry := rman.FileEntry{Path: "f.bin", Chunks: []rman.ChunkInfo{c0, c1}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if !result.Exists {
		t.Error("Exists 期望 true")
	}
	if len(result.Hits) != 1 {
		t.Fatalf("Hits 期望 1 个，got %d", len(result.Hits))
	}
	if result.Hits[0].FileOffset != 0 {
		t.Errorf("Hits[0].FileOffset = %d, want 0", result.Hits[0].FileOffset)
	}
	if len(result.Misses) != 1 {
		t.Fatalf("Misses 期望 1 个，got %d", len(result.Misses))
	}
	if result.Misses[0].FileOffset != int64(len(d0)) {
		t.Errorf("Misses[0].FileOffset = %d, want %d", result.Misses[0].FileOffset, len(d0))
	}
}

// TestVerifyFileChunks_HashTypeNoneHit 验证 HashType=HashTypeNone 且磁盘数据完好时，
// 穷举猜测（HKDF → Blake3 → SHA256 → SHA512）能够命中。此处 ChunkID 用 SHA256（穷举顺序
// 中的第三个，非首个）算出，证明猜测循环确实会遍历后续算法而非只试第一个。
func TestVerifyFileChunks_HashTypeNoneHit(t *testing.T) {
	dir := t.TempDir()
	d0 := randBytes(t, 40)
	c0 := chunkNoneFromData(d0, rman.HashTypeSHA256)

	path := writeFile(t, dir, "f.bin", d0)
	entry := rman.FileEntry{Path: "f.bin", Chunks: []rman.ChunkInfo{c0}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if len(result.Hits) != 1 || len(result.Misses) != 0 {
		t.Fatalf("HashTypeNone 数据完好时期望穷举猜测命中，got Hits=%d Misses=%d", len(result.Hits), len(result.Misses))
	}
}

// TestVerifyFileChunks_HashTypeNoneMiss 验证 HashType=HashTypeNone 且磁盘数据与
// ChunkID 不符时，穷举所有算法均不命中，最终判 miss。
func TestVerifyFileChunks_HashTypeNoneMiss(t *testing.T) {
	dir := t.TempDir()
	d0 := randBytes(t, 40)
	bad := randBytes(t, 40)
	c0 := chunkNoneFromData(d0, rman.HashTypeBlake3)

	path := writeFile(t, dir, "f.bin", bad) // 磁盘内容与算 ChunkID 所用的数据不同
	entry := rman.FileEntry{Path: "f.bin", Chunks: []rman.ChunkInfo{c0}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if len(result.Misses) != 1 || len(result.Hits) != 0 {
		t.Fatalf("HashTypeNone 数据不符时期望 miss，got Hits=%d Misses=%d", len(result.Hits), len(result.Misses))
	}
}

// TestVerifyFileChunks_SameChunkIDTwoPositionsIndependent 验证同一个 ChunkID 在文件中
// 出现两次（两个 Chunk 描述引用同一份内容）时，命中判定按各自的固定位置独立进行：
// 第一处磁盘数据完好应为 hit，第二处被破坏应为 miss，而不是被 ChunkID 相同"传染"。
func TestVerifyFileChunks_SameChunkIDTwoPositionsIndependent(t *testing.T) {
	dir := t.TempDir()
	d := randBytes(t, 25)
	bad := randBytes(t, 25)
	c := chunkFromData(d, rman.HashTypeSHA256)

	path := writeFile(t, dir, "f.bin", concatBytes(d, bad))
	entry := rman.FileEntry{Path: "f.bin", Chunks: []rman.ChunkInfo{c, c}}

	result, err := VerifyFileChunks(entry, path)
	if err != nil {
		t.Fatalf("VerifyFileChunks 返回 error: %v", err)
	}
	if len(result.Hits) != 1 || len(result.Misses) != 1 {
		t.Fatalf("同 ChunkID 双位置期望按位置独立判定（一 hit 一 miss），got Hits=%d Misses=%d", len(result.Hits), len(result.Misses))
	}
	if result.Hits[0].FileOffset != 0 {
		t.Errorf("Hits[0].FileOffset = %d, want 0", result.Hits[0].FileOffset)
	}
	if result.Misses[0].FileOffset != 25 {
		t.Errorf("Misses[0].FileOffset = %d, want 25", result.Misses[0].FileOffset)
	}
}
