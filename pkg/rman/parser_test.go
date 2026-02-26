package rman

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// ---- 辅助函数 ----

// le 是 Little Endian 的快捷引用
var le = binary.LittleEndian

// buildTestHeader 构造一个合法的 28 字节 RMAN 文件头。
// manifestID = testManifestID，内容偏移固定为 28（头之后立即是正文）。
func buildTestHeader(manifestID uint64, contentOffset, compressedSize, uncompressedSize uint32, major, minor byte) []byte {
	buf := make([]byte, 28)
	copy(buf[0:4], []byte("RMAN"))
	buf[4] = major
	buf[5] = minor
	// [6:8] 保留
	le.PutUint32(buf[8:12], contentOffset)
	le.PutUint32(buf[12:16], compressedSize)
	le.PutUint64(buf[16:24], manifestID)
	le.PutUint32(buf[24:28], uncompressedSize)
	return buf
}

// ---- 文件头解析测试 ----

// TestParseHeader_Valid 验证合法文件头的全部字段能被正确解析。
func TestParseHeader_Valid(t *testing.T) {
	const (
		wantManifestID     = uint64(0xDEADBEEFCAFEBABE)
		wantContentOffset  = uint32(28)
		wantCompressedSize = uint32(1234)
		wantUncompressed   = uint32(5678)
	)
	data := buildTestHeader(wantManifestID, wantContentOffset, wantCompressedSize, wantUncompressed, 2, 0)

	hdr, err := parseHeader(data)
	if err != nil {
		t.Fatalf("parseHeader 返回意外错误: %v", err)
	}

	if hdr.MajorVersion != 2 {
		t.Errorf("MajorVersion = %d, 期望 2", hdr.MajorVersion)
	}
	if hdr.MinorVersion != 0 {
		t.Errorf("MinorVersion = %d, 期望 0", hdr.MinorVersion)
	}
	if hdr.ContentOffset != wantContentOffset {
		t.Errorf("ContentOffset = %d, 期望 %d", hdr.ContentOffset, wantContentOffset)
	}
	if hdr.CompressedSize != wantCompressedSize {
		t.Errorf("CompressedSize = %d, 期望 %d", hdr.CompressedSize, wantCompressedSize)
	}
	if hdr.ManifestID != wantManifestID {
		t.Errorf("ManifestID = %X, 期望 %X", hdr.ManifestID, wantManifestID)
	}
	if hdr.UncompressedSize != wantUncompressed {
		t.Errorf("UncompressedSize = %d, 期望 %d", hdr.UncompressedSize, wantUncompressed)
	}
}

// TestParseHeader_BadMagic 验证魔数不为 "RMAN" 时返回错误。
func TestParseHeader_BadMagic(t *testing.T) {
	data := buildTestHeader(0, 28, 0, 0, 2, 0)
	data[0] = 'X' // 破坏魔数

	_, err := parseHeader(data)
	if err == nil {
		t.Fatal("期望返回错误，但 parseHeader 返回 nil")
	}
}

// TestParseHeader_BadVersion 验证主版本号不为 2 时返回错误。
func TestParseHeader_BadVersion(t *testing.T) {
	data := buildTestHeader(0, 28, 0, 0, 3, 0) // MajorVersion = 3

	_, err := parseHeader(data)
	if err == nil {
		t.Fatal("期望返回错误（MajorVersion=3），但 parseHeader 返回 nil")
	}
}

// TestParseHeader_TooShort 验证数据不足 28 字节时返回错误。
func TestParseHeader_TooShort(t *testing.T) {
	data := make([]byte, 10) // 只有 10 字节
	_, err := parseHeader(data)
	if err == nil {
		t.Fatal("期望返回错误（数据不足），但 parseHeader 返回 nil")
	}
}

// TestParseHeader_MinorVersionWarning 验证次版本号非 0/1 时能继续解析（仅输出警告）。
func TestParseHeader_MinorVersionWarning(t *testing.T) {
	data := buildTestHeader(0, 28, 0, 0, 2, 99) // MinorVersion = 99
	// 预期不返回错误，只输出 log 警告
	hdr, err := parseHeader(data)
	if err != nil {
		t.Fatalf("次版本号 99 不应返回错误，但得到: %v", err)
	}
	if hdr.MinorVersion != 99 {
		t.Errorf("MinorVersion = %d, 期望 99", hdr.MinorVersion)
	}
}

// ---- bundle_offset 累加逻辑测试 ----

// TestBundleOffsetAccumulation 验证 bundle_offset 由 Parser 累加 compressed_size 正确推算。
//
// BundleOffset 由 Parser 层通过累加 compressed_size 推算：
// bundle_offset 不存储在 FlatBuffers 中，由 Parser 在遍历同一 Bundle 的 Chunk 时
// 累加 compressed_size 自行推算。
//
// 本测试构造一个包含 3 个 Chunk 的最小合法 FlatBuffers body，
// 验证解析后 BundleOffset 依次为 0、chunk0.CompressedSize、chunk0+chunk1。
func TestBundleOffsetAccumulation(t *testing.T) {
	// 构造一个最小化的 FlatBuffers body，包含：
	// - 1 个 Bundle，含 3 个 Chunk
	// - 无 Language、Directory、FileEntry（避免关联复杂性）
	body := buildMinimalFlatBuffersBody(t)

	chunkMap, err := parseBundles(body, fbRootOffset(body))
	if err != nil {
		t.Fatalf("parseBundles 失败: %v", err)
	}

	// 期望 chunk0 BundleOffset=0, chunk1=100, chunk2=250（对应构造时的 compressed_size）
	type wantChunk struct {
		id           uint64
		wantOffset   uint32
		wantCompSize uint32
	}
	wants := []wantChunk{
		{id: 0xAAAA, wantOffset: 0, wantCompSize: 100},
		{id: 0xBBBB, wantOffset: 100, wantCompSize: 150},
		{id: 0xCCCC, wantOffset: 250, wantCompSize: 200},
	}

	for _, w := range wants {
		ci, ok := chunkMap[w.id]
		if !ok {
			t.Errorf("chunkMap 中找不到 chunkID=%X", w.id)
			continue
		}
		if ci.BundleOffset != w.wantOffset {
			t.Errorf("chunk %X BundleOffset=%d, 期望 %d", w.id, ci.BundleOffset, w.wantOffset)
		}
		if ci.CompressedSize != w.wantCompSize {
			t.Errorf("chunk %X CompressedSize=%d, 期望 %d", w.id, ci.CompressedSize, w.wantCompSize)
		}
	}
}

// ---- 路径重建测试 ----

// TestPathReconstruction 验证三级目录树的路径重建结果。
//
// 路径重建通过目录树向上递归：
// 最终路径 = "grandparent_dir/parent_dir/filename"
func TestPathReconstruction(t *testing.T) {
	dirMap := map[uint64]dirEntry{
		// root -> "a" -> "b" -> "c" 三级目录
		1: {Name: "a", ParentID: 0}, // 顶层目录
		2: {Name: "b", ParentID: 1},
		3: {Name: "c", ParentID: 2},
	}

	tests := []struct {
		name     string
		dirID    uint64
		filename string
		wantPath string
	}{
		{
			name:     "根目录文件",
			dirID:    0,
			filename: "root.txt",
			wantPath: "root.txt",
		},
		{
			name:     "一级目录",
			dirID:    1,
			filename: "file.txt",
			wantPath: "a/file.txt",
		},
		{
			name:     "三级目录",
			dirID:    3,
			filename: "deep.bin",
			wantPath: "a/b/c/deep.bin",
		},
		{
			name:     "不存在的目录ID",
			dirID:    999,
			filename: "orphan.txt",
			wantPath: "orphan.txt", // dirID 不在 map 中，退化为仅文件名
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildPath(dirMap, tc.dirID, tc.filename)
			if got != tc.wantPath {
				t.Errorf("buildPath(dirID=%d, name=%q) = %q, 期望 %q",
					tc.dirID, tc.filename, got, tc.wantPath)
			}
		})
	}
}

// TestPathReconstruction_CycleDetection 验证目录树存在循环引用时不会无限循环。
func TestPathReconstruction_CycleDetection(t *testing.T) {
	// 构造循环引用：1 -> 2 -> 1（环）
	dirMap := map[uint64]dirEntry{
		1: {Name: "x", ParentID: 2},
		2: {Name: "y", ParentID: 1},
	}
	// 只要不进入死循环，任何结果都可以接受
	got := buildPath(dirMap, 1, "file.txt")
	if got == "" {
		t.Error("循环引用时 buildPath 返回空字符串")
	}
	t.Logf("循环引用下的结果（允许任意非死循环结果）: %q", got)
}

// ---- 真实文件集成测试 ----

// TestParseManifest_RealFile 对真实的 .manifest 文件进行端到端解析验证。
//
// 如果测试文件不存在，自动跳过（不影响 CI）。
// 使用文件：.temp/BA80B75282F55531.manifest（规格文档指定的测试文件）
func TestParseManifest_RealFile(t *testing.T) {
	// 获取测试文件路径（相对于本测试文件所在的 pkg/rman/ 目录向上两级）
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法获取测试文件路径")
	}
	manifestPath := filepath.Join(filepath.Dir(testFile), "..", "..", ".temp", "BA80B75282F55531.manifest")
	manifestPath = filepath.Clean(manifestPath)

	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		t.Skipf("测试文件不存在，跳过：%s", manifestPath)
	}

	manifest, err := ParseFile(manifestPath)
	if err != nil {
		t.Fatalf("ParseFile 失败: %v", err)
	}

	// 基础断言
	if manifest.ManifestID == 0 {
		t.Error("ManifestID 为 0，可能解析错误")
	}
	if len(manifest.Files) == 0 {
		t.Fatal("文件列表为空，期望至少有 1 个 FileEntry")
	}

	t.Logf("✓ ManifestID: %016X", manifest.ManifestID)
	t.Logf("✓ 文件总数: %d", len(manifest.Files))

	// 打印前 5 个文件的详情
	limit := 5
	if len(manifest.Files) < limit {
		limit = len(manifest.Files)
	}
	t.Log("── 前 5 个 FileEntry ──")
	for i := 0; i < limit; i++ {
		f := manifest.Files[i]
		t.Logf("  [%d] 路径: %s", i, f.Path)
		t.Logf("      文件大小: %d 字节", f.FileSize)
		t.Logf("      Flags: %v", f.Flags)
		t.Logf("      Chunk 数量: %d", len(f.Chunks))
		if len(f.Chunks) > 0 {
			c := f.Chunks[0]
			t.Logf("      首个 Chunk: ID=%016X BundleID=%016X BundleOffset=%d CompSize=%d Hash=%s",
				c.ChunkID, c.BundleID, c.BundleOffset, c.CompressedSize, c.HashType)

			// 关键断言：首个 chunk 的 BundleID 和 CompressedSize 应非零
			if c.BundleID == 0 {
				t.Errorf("文件 %q 的首个 Chunk BundleID 为 0，可能解析错误", f.Path)
			}
			if c.CompressedSize == 0 {
				t.Errorf("文件 %q 的首个 Chunk CompressedSize 为 0，可能解析错误", f.Path)
			}
		}
	}

	// 统计信息
	totalChunks := 0
	uniqueBundles := make(map[uint64]struct{})
	for _, f := range manifest.Files {
		totalChunks += len(f.Chunks)
		for _, c := range f.Chunks {
			uniqueBundles[c.BundleID] = struct{}{}
		}
	}
	t.Logf("✓ 总 Chunk 引用数: %d", totalChunks)
	t.Logf("✓ 涉及不同 Bundle 数: %d", len(uniqueBundles))
}

// ---- FlatBuffers 测试辅助构造器 ----

// buildMinimalFlatBuffersBody 构造一个最小化的 FlatBuffers body，
// 仅包含 1 个 Bundle（含 3 个 Chunk），用于测试 bundle_offset 累加逻辑。
//
// 构造的 Chunk 数据：
//
//	ChunkID=0xAAAA, CompressedSize=100, UncompressedSize=200
//	ChunkID=0xBBBB, CompressedSize=150, UncompressedSize=300
//	ChunkID=0xCCCC, CompressedSize=200, UncompressedSize=400
//
// 期望的 BundleOffset：0, 100, 250
func buildMinimalFlatBuffersBody(t *testing.T) []byte {
	t.Helper()
	return buildFBBodyUsingRawBytes()
}

// buildFBBodyUsingRawBytes 精确构造测试用 FlatBuffers body。
//
// 布局策略：预分配 buf[0:4] 作为 root offset 占位符，正向写入所有数据，
// 最后回填 root offset（从 buf[0] 起算的绝对偏移）。
//
// 包含 1 个 Bundle（ID=0x1234），含 3 个 Chunk：
//
//	Chunk0: ID=0xAAAA, CompSize=100, UncompSize=200
//	Chunk1: ID=0xBBBB, CompSize=150, UncompSize=300
//	Chunk2: ID=0xCCCC, CompSize=200, UncompSize=400
//
// FlatBuffers Table 内存格式：
//
//	tablePos:        int32 soffset（= vtablePos - tablePos，负数时指向 vtable）
//	tablePos+fields: 字段数据（按 vtable 中的 offset 寻址）
//	vtablePos:       uint16 vtable_len, uint16 obj_size, uint16 field_offset...
func buildFBBodyUsingRawBytes() []byte {
	buf := make([]byte, 0, 512)

	wr16 := func(v uint16) int {
		pos := len(buf)
		buf = append(buf, byte(v), byte(v>>8))
		return pos
	}
	wr32 := func(v uint32) int {
		pos := len(buf)
		buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
		return pos
	}
	wr64 := func(v uint64) int {
		pos := len(buf)
		buf = append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
			byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
		return pos
	}
	patch32 := func(pos int, v uint32) {
		buf[pos] = byte(v)
		buf[pos+1] = byte(v >> 8)
		buf[pos+2] = byte(v >> 16)
		buf[pos+3] = byte(v >> 24)
	}

	// ── 预分配 root offset 占位符（4 字节，最后回填）──
	rootOffPlaceholder := wr32(0)

	// ── 辅助：构造 Chunk Table ──
	// Chunk Table vtable 布局：
	//   vtable: [vtable_len=10][obj_size=24][f0_off=8][f1_off=16][f2_off=20]
	//   table:  [soffset(4)][padding(4)][chunk_id(8)][comp_size(4)][uncomp_size(4)]
	//
	// 字段偏移（相对 tablePos）：
	//   field0 (chunk_id/uint64)  @ tablePos+8
	//   field1 (comp_size/uint32) @ tablePos+16
	//   field2 (uncomp/uint32)    @ tablePos+20
	buildChunk := func(chunkID uint64, compSize, uncompSize uint32) int {
		vtablePos := len(buf)
		wr16(10) // vtable_len
		wr16(24) // obj_size
		wr16(8)  // field0 offset
		wr16(16) // field1 offset
		wr16(20) // field2 offset

		tablePos := len(buf)
		soffsetPos := wr32(0) // soffset placeholder
		// FlatBuffers 规范：soffset = tablePos - vtablePos（负数，vtable 在 table 之前）
		// 读取时：vtableOffset = tableOffset - soffset = tablePos - (tablePos - vtablePos) = vtablePos ✓
		patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
		wr32(0)          // padding (保证 uint64 8字节对齐)
		wr64(chunkID)    // field0: chunk_id
		wr32(compSize)   // field1: compressed_size
		wr32(uncompSize) // field2: uncompressed_size
		return tablePos
	}

	c0 := buildChunk(0xAAAA, 100, 200)
	c1 := buildChunk(0xBBBB, 150, 300)
	c2 := buildChunk(0xCCCC, 200, 400)

	// ── Chunk Vector（3 个元素，每个是 4 字节 referenceOffset）──
	// FlatBuffers vector: [count(uint32)][ref0(uint32)][ref1][ref2]
	// referenceOffset = target_abs_pos - self_pos
	chunkVecPos := len(buf)
	wr32(3) // count
	r0p := wr32(0)
	r1p := wr32(0)
	r2p := wr32(0)
	patch32(r0p, uint32(int32(c0)-int32(r0p)))
	patch32(r1p, uint32(int32(c1)-int32(r1p)))
	patch32(r2p, uint32(int32(c2)-int32(r2p)))

	// ── Bundle Table ──
	// vtable: [vtable_len=8][obj_size=20][f0=8][f1=16]
	// table:  [soffset(4)][padding(4)][bundle_id(8)][chunks_ref(4)]
	//
	// 字段偏移（相对 tablePos）：
	//   field0 (bundle_id/uint64) @ tablePos+8
	//   field1 (chunks/vector)    @ tablePos+16
	bundleVtablePos := len(buf)
	wr16(8)  // vtable_len
	wr16(20) // obj_size
	wr16(8)  // field0 (bundle_id)
	wr16(16) // field1 (chunks vector)

	bundleTablePos := len(buf)
	bSoffset := wr32(0)
	// soffset = tablePos - vtablePos（负数）
	patch32(bSoffset, uint32(int32(bundleTablePos)-int32(bundleVtablePos)))
	wr32(0)      // padding
	wr64(0x1234) // bundle_id
	chunkRefPos := wr32(0)
	patch32(chunkRefPos, uint32(int32(chunkVecPos)-int32(chunkRefPos)))

	// ── Bundle Vector（1 个元素）──
	bundleVecPos := len(buf)
	wr32(1) // count
	br0p := wr32(0)
	patch32(br0p, uint32(int32(bundleTablePos)-int32(br0p)))

	// ── Root Table ──
	// vtable: [vtable_len=16][obj_size=8][f0=4][f1=0][f2=0][f3=0][f4=0][f5=0]
	// table:  [soffset(4)][bundles_ref(4)]
	//
	// 字段偏移（相对 tablePos）：
	//   field0 (bundles/vector) @ tablePos+4
	rootVtablePos := len(buf)
	wr16(16) // vtable_len = 4 + 6*2
	wr16(8)  // obj_size
	wr16(4)  // field0 (bundles)
	wr16(0)  // field1 (languages) 不存在
	wr16(0)  // field2 (fileEntries) 不存在
	wr16(0)  // field3 (directories) 不存在
	wr16(0)  // field4 跳过
	wr16(0)  // field5 (parameters) 不存在

	rootTablePos := len(buf)
	rSoffset := wr32(0)
	// soffset = tablePos - vtablePos（负数）
	patch32(rSoffset, uint32(int32(rootTablePos)-int32(rootVtablePos)))
	bundleVecRefPos := wr32(0)
	patch32(bundleVecRefPos, uint32(int32(bundleVecPos)-int32(bundleVecRefPos)))

	// ── 回填 root offset（buf[0:4] = rootTablePos）──
	// FlatBuffers 规范：buf[0:4] 存储根 Table 相对 buf[0] 的绝对偏移
	patch32(rootOffPlaceholder, uint32(rootTablePos))

	return buf
}
