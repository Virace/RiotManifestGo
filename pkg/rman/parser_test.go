package rman

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
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

	chunkMap, sizes, err := parseBundles(body, fbRootOffset(body))
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

	// bundle 总大小 = 全部 chunk 的 CompressedSize 之和（100+150+200=450）
	const wantBundleID = uint64(0x1234)
	const wantTotalSize = uint32(100 + 150 + 200)
	if len(sizes) != 1 {
		t.Errorf("sizes 数量 = %d, 期望 1", len(sizes))
	}
	if got := sizes[wantBundleID]; got != wantTotalSize {
		t.Errorf("sizes[%X] = %d, 期望 %d", wantBundleID, got, wantTotalSize)
	}
}

// TestParseBundlesMultipleBundlesAndEmptyChunks 验证 parseBundles 对多个 Bundle
// 各自独立累加总大小，且 chunkCount==0 的 Bundle 不在 sizes 中生成条目。
func TestParseBundlesMultipleBundlesAndEmptyChunks(t *testing.T) {
	specs := []bundleSpec{
		{
			bundleID: 0x1001,
			chunks: []chunkSpec{
				{chunkID: 0x1, compressedSize: 10, uncompressedSize: 20},
				{chunkID: 0x2, compressedSize: 20, uncompressedSize: 40},
			},
		},
		{
			bundleID: 0x1002,
			chunks: []chunkSpec{
				{chunkID: 0x3, compressedSize: 5, uncompressedSize: 10},
			},
		},
		{
			bundleID: 0x1003,
			chunks:   nil, // 空 chunks，不应产生 sizes 条目
		},
	}
	body := buildFBBodyWithBundles(specs)

	chunkMap, sizes, err := parseBundles(body, fbRootOffset(body))
	if err != nil {
		t.Fatalf("parseBundles 失败: %v", err)
	}

	if len(sizes) != 2 {
		t.Fatalf("sizes 数量 = %d, 期望 2（空 chunks 的 bundle 不应入表）", len(sizes))
	}
	if got, want := sizes[0x1001], uint32(30); got != want {
		t.Errorf("sizes[0x1001] = %d, 期望 %d", got, want)
	}
	if got, want := sizes[0x1002], uint32(5); got != want {
		t.Errorf("sizes[0x1002] = %d, 期望 %d", got, want)
	}
	if _, ok := sizes[0x1003]; ok {
		t.Error("sizes 不应包含空 chunks 的 bundle 0x1003")
	}
	if _, ok := chunkMap[0x3]; !ok {
		t.Error("chunkMap 中找不到 bundle 0x1002 的 chunk 0x3")
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

func TestParseBodyMalformedFlatBuffersReturnsError(t *testing.T) {
	body := []byte{0x04, 0x00, 0x00, 0x00, 0xff}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("parseBody should return error instead of panic, got panic: %v", r)
		}
	}()

	if _, _, _, _, err := parseBody(body); err == nil {
		t.Fatal("parseBody should reject malformed FlatBuffers body")
	}
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

// chunkSpec 描述 buildFBBodyWithBundles 构造中一个 Chunk 的字段取值。
type chunkSpec struct {
	chunkID          uint64
	compressedSize   uint32
	uncompressedSize uint32
}

// bundleSpec 描述 buildFBBodyWithBundles 构造中一个 Bundle 及其 Chunk 列表。
// chunks 为空切片时，构造出的 Bundle 对象不含 chunks 字段（field1 offset=0），
// 对应 chunkCount==0 的场景。
type bundleSpec struct {
	bundleID uint64
	chunks   []chunkSpec
}

// buildFBBodyWithBundles 构造仅含 Bundle 列表（无 Language/FileEntry/Parameters）的
// 最小化 FlatBuffers body，用于测试 parseBundles 对多个 Bundle 的独立累加，
// 以及空 chunks 的 Bundle 不产生 sizes 条目的行为。
//
// Table 布局与 buildFBBodyUsingRawBytes 一致，此处泛化为支持任意数量的 Bundle。
func buildFBBodyWithBundles(specs []bundleSpec) []byte {
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

	rootOffPlaceholder := wr32(0)

	// Chunk Table：同 buildFBBodyUsingRawBytes 的布局
	buildChunk := func(c chunkSpec) int {
		vtablePos := len(buf)
		wr16(10) // vtable_len
		wr16(24) // obj_size
		wr16(8)  // field0 chunk_id
		wr16(16) // field1 compressed_size
		wr16(20) // field2 uncompressed_size
		tablePos := len(buf)
		soffsetPos := wr32(0)
		patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
		wr32(0) // padding
		wr64(c.chunkID)
		wr32(c.compressedSize)
		wr32(c.uncompressedSize)
		return tablePos
	}

	writeTableVector := func(tablePositions []int) int {
		pos := wr32(uint32(len(tablePositions)))
		refPositions := make([]int, len(tablePositions))
		for i := range tablePositions {
			refPositions[i] = wr32(0)
		}
		for i, tp := range tablePositions {
			patch32(refPositions[i], uint32(int32(tp)-int32(refPositions[i])))
		}
		return pos
	}

	bundleTablePositions := make([]int, 0, len(specs))
	for _, spec := range specs {
		hasChunks := len(spec.chunks) > 0
		var chunkVecPos int
		if hasChunks {
			chunkPositions := make([]int, 0, len(spec.chunks))
			for _, c := range spec.chunks {
				chunkPositions = append(chunkPositions, buildChunk(c))
			}
			chunkVecPos = writeTableVector(chunkPositions)
		}

		// Bundle Table：field0=bundle_id, field1=chunks(vector)
		// 无 chunks 时 field1 offset=0（字段不存在），对应 chunkCount==0
		bundleVtablePos := len(buf)
		wr16(8) // vtable_len
		if hasChunks {
			wr16(20) // obj_size
		} else {
			wr16(16) // obj_size（无 chunks_ref 字段）
		}
		wr16(8) // field0 bundle_id
		if hasChunks {
			wr16(16) // field1 chunks
		} else {
			wr16(0) // field1 不存在
		}

		bundleTablePos := len(buf)
		bSoffset := wr32(0)
		patch32(bSoffset, uint32(int32(bundleTablePos)-int32(bundleVtablePos)))
		wr32(0) // padding
		wr64(spec.bundleID)
		if hasChunks {
			chunkRefPos := wr32(0)
			patch32(chunkRefPos, uint32(int32(chunkVecPos)-int32(chunkRefPos)))
		}
		bundleTablePositions = append(bundleTablePositions, bundleTablePos)
	}

	bundleVecPos := writeTableVector(bundleTablePositions)

	// Root Table：field0=bundles，其余字段不存在
	rootVtablePos := len(buf)
	wr16(16) // vtable_len = 4 + 6*2
	wr16(8)  // obj_size
	wr16(4)  // field0 bundles
	wr16(0)  // field1 languages 不存在
	wr16(0)  // field2 fileEntries 不存在
	wr16(0)  // field3 directories 不存在
	wr16(0)  // field4 跳过
	wr16(0)  // field5 parameters 不存在

	rootTablePos := len(buf)
	rSoffset := wr32(0)
	patch32(rSoffset, uint32(int32(rootTablePos)-int32(rootVtablePos)))
	bundleVecRefPos := wr32(0)
	patch32(bundleVecRefPos, uint32(int32(bundleVecPos)-int32(bundleVecRefPos)))

	patch32(rootOffPlaceholder, uint32(rootTablePos))
	return buf
}

// buildFBBodyWithMetadata 构造一个带清单级元信息的 FlatBuffers body，
// 用于验证 Languages/Parameters 提取与 param_index 哈希类型关联：
//
//   - 1 个 Bundle（ID=0x1234），含 2 个 Chunk（0xAAAA/100B、0xBBBB/150B）
//   - 2 个 Language：flag_id=1 "en_US"、flag_id=2 "zh_CN"
//   - 1 个 Parameters 条目：HashType=HKDF, min=4096, max=16384, maxUncomp=16384
//   - 1 个 FileEntry："root.wad"，500B，language_mask=0b10（zh_CN），
//     chunk_ids=[0xAAAA,0xBBBB]，param_index=0
func buildFBBodyWithMetadata() []byte {
	buf := make([]byte, 0, 1024)

	wr8 := func(v uint8) int {
		pos := len(buf)
		buf = append(buf, v)
		return pos
	}
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
	// FlatBuffers 字符串：[u32 长度][UTF-8 字节][NUL]，返回绝对位置
	writeString := func(s string) int {
		pos := wr32(uint32(len(s)))
		buf = append(buf, s...)
		buf = append(buf, 0)
		return pos
	}
	// Table 型 vector：[u32 count][referenceOffset...]，元素指向已写入的子 Table
	writeTableVector := func(tablePositions []int) int {
		pos := wr32(uint32(len(tablePositions)))
		for _, tp := range tablePositions {
			slot := wr32(0)
			patch32(slot, uint32(int32(tp)-int32(slot)))
		}
		return pos
	}

	rootOffPlaceholder := wr32(0)

	// ── Chunk Table（同 buildFBBodyUsingRawBytes 的布局）──
	buildChunk := func(chunkID uint64, compSize, uncompSize uint32) int {
		vtablePos := len(buf)
		wr16(10) // vtable_len
		wr16(24) // obj_size
		wr16(8)  // field0 chunk_id
		wr16(16) // field1 compressed_size
		wr16(20) // field2 uncompressed_size
		tablePos := len(buf)
		soffsetPos := wr32(0)
		patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
		wr32(0) // padding
		wr64(chunkID)
		wr32(compSize)
		wr32(uncompSize)
		return tablePos
	}
	c0 := buildChunk(0xAAAA, 100, 200)
	c1 := buildChunk(0xBBBB, 150, 300)
	chunkVecPos := writeTableVector([]int{c0, c1})

	// ── Bundle Table：field0=bundle_id, field1=chunks(vector) ──
	bundleVtablePos := len(buf)
	wr16(8)  // vtable_len
	wr16(20) // obj_size
	wr16(8)  // field0 bundle_id
	wr16(16) // field1 chunks
	bundleTablePos := len(buf)
	bSoffset := wr32(0)
	patch32(bSoffset, uint32(int32(bundleTablePos)-int32(bundleVtablePos)))
	wr32(0) // padding
	wr64(0x1234)
	chunkRefPos := wr32(0)
	patch32(chunkRefPos, uint32(int32(chunkVecPos)-int32(chunkRefPos)))
	bundleVecPos := writeTableVector([]int{bundleTablePos})

	// ── Language Table：field0=flag_id(uint8), field1=name(string) ──
	buildLanguage := func(flagID uint8, name string) int {
		strPos := writeString(name)
		vtablePos := len(buf)
		wr16(8)  // vtable_len
		wr16(12) // obj_size
		wr16(4)  // field0 flag_id
		wr16(8)  // field1 name
		tablePos := len(buf)
		soffsetPos := wr32(0)
		patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
		wr8(flagID)
		wr8(0) // padding ×3，对齐 name ref 到 tableStart+8
		wr8(0)
		wr8(0)
		nameRefPos := wr32(0)
		patch32(nameRefPos, uint32(int32(strPos)-int32(nameRefPos)))
		return tablePos
	}
	lang0 := buildLanguage(1, "en_US")
	lang1 := buildLanguage(2, "zh_CN")
	langVecPos := writeTableVector([]int{lang0, lang1})

	// ── Parameters Table：field1=hash_type(uint8), field2=min, field3=max,
	// field4=max_uncompressed（field0 未使用）──
	paramVtablePos := len(buf)
	wr16(14) // vtable_len = 4 + 5*2
	wr16(20) // obj_size
	wr16(0)  // field0（未使用）
	wr16(4)  // field1 hash_type
	wr16(8)  // field2 min_chunk_size
	wr16(12) // field3 max_chunk_size
	wr16(16) // field4 max_uncompressed
	paramTablePos := len(buf)
	pSoffset := wr32(0)
	patch32(pSoffset, uint32(int32(paramTablePos)-int32(paramVtablePos)))
	wr8(uint8(HashTypeHKDF))
	wr8(0) // padding ×3
	wr8(0)
	wr8(0)
	wr32(4096)  // min_chunk_size
	wr32(16384) // max_chunk_size
	wr32(16384) // max_uncompressed
	paramVecPos := writeTableVector([]int{paramTablePos})

	// ── FileEntry Table：field2=file_size, field3=name, field4=language_mask,
	// field7=chunk_ids(vector), field11=param_index ──
	fileNamePos := writeString("root.wad")
	chunkIDsVecPos := len(buf)
	wr32(2) // count
	wr64(0xAAAA)
	wr64(0xBBBB)

	fileVtablePos := len(buf)
	wr16(28) // vtable_len = 4 + 12*2（field0..field11）
	wr16(32) // obj_size
	wr16(0)  // field0 file_entry_id（缺省）
	wr16(0)  // field1 directory_id（缺省 -> 根目录）
	wr16(4)  // field2 file_size
	wr16(12) // field3 name
	wr16(16) // field4 language_mask
	wr16(0)  // field5（未使用）
	wr16(0)  // field6（未使用）
	wr16(24) // field7 chunk_ids
	wr16(0)  // field8（未使用）
	wr16(0)  // field9 link（缺省）
	wr16(0)  // field10（未使用）
	wr16(28) // field11 param_index
	fileTablePos := len(buf)
	fSoffset := wr32(0)
	patch32(fSoffset, uint32(int32(fileTablePos)-int32(fileVtablePos)))
	wr64(500) // file_size @ +4
	fileNameRefPos := wr32(0)
	patch32(fileNameRefPos, uint32(int32(fileNamePos)-int32(fileNameRefPos)))
	wr64(0b10) // language_mask @ +16（bit1 = flag_id 2 = zh_CN）
	chunkIDsRefPos := wr32(0)
	patch32(chunkIDsRefPos, uint32(int32(chunkIDsVecPos)-int32(chunkIDsRefPos)))
	wr8(0) // param_index @ +28
	wr8(0) // padding ×3
	wr8(0)
	wr8(0)
	fileVecPos := writeTableVector([]int{fileTablePos})

	// ── Root Table：field0=bundles, field1=languages, field2=fileEntries,
	// field5=parameters ──
	rootVtablePos := len(buf)
	wr16(16) // vtable_len = 4 + 6*2
	wr16(20) // obj_size
	wr16(4)  // field0 bundles
	wr16(8)  // field1 languages
	wr16(12) // field2 fileEntries
	wr16(0)  // field3 directories（缺省）
	wr16(0)  // field4（未使用）
	wr16(16) // field5 parameters
	rootTablePos := len(buf)
	rSoffset := wr32(0)
	patch32(rSoffset, uint32(int32(rootTablePos)-int32(rootVtablePos)))
	bundlesRefPos := wr32(0)
	patch32(bundlesRefPos, uint32(int32(bundleVecPos)-int32(bundlesRefPos)))
	langsRefPos := wr32(0)
	patch32(langsRefPos, uint32(int32(langVecPos)-int32(langsRefPos)))
	filesRefPos := wr32(0)
	patch32(filesRefPos, uint32(int32(fileVecPos)-int32(filesRefPos)))
	paramsRefPos := wr32(0)
	patch32(paramsRefPos, uint32(int32(paramVecPos)-int32(paramsRefPos)))

	patch32(rootOffPlaceholder, uint32(rootTablePos))
	return buf
}

// TestParseBytesPopulatesManifestMetadata 验证 parseBytes 将文件头版本号与
// Body 中的 Languages/Parameters 元信息透传到 Manifest，
// 且 FileEntry 按 param_index 关联到正确的哈希类型。
func TestParseBytesPopulatesManifestMetadata(t *testing.T) {
	const wantManifestID = uint64(0x1122334455667788)

	body := buildFBBodyWithMetadata()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("创建 ZSTD 编码器失败: %v", err)
	}
	compressed := enc.EncodeAll(body, nil)
	enc.Close()

	data := buildTestHeader(wantManifestID, 28, uint32(len(compressed)), uint32(len(body)), 2, 1)
	data = append(data, compressed...)

	m, err := parseBytes(data)
	if err != nil {
		t.Fatalf("parseBytes 失败: %v", err)
	}

	if m.ManifestID != wantManifestID {
		t.Errorf("ManifestID = %016X, 期望 %016X", m.ManifestID, wantManifestID)
	}
	if m.MajorVersion != 2 || m.MinorVersion != 1 {
		t.Errorf("版本 = %d.%d, 期望 2.1", m.MajorVersion, m.MinorVersion)
	}

	wantFlags := []string{"en_US", "zh_CN"}
	if len(m.Flags) != len(wantFlags) {
		t.Fatalf("Flags = %v, 期望 %v", m.Flags, wantFlags)
	}
	for i, want := range wantFlags {
		if m.Flags[i] != want {
			t.Errorf("Flags[%d] = %q, 期望 %q", i, m.Flags[i], want)
		}
	}

	if len(m.Params) != 1 {
		t.Fatalf("Params 数量 = %d, 期望 1", len(m.Params))
	}
	wantParams := Params{HashType: HashTypeHKDF, MinChunkSize: 4096, MaxChunkSize: 16384, MaxUncompressed: 16384}
	if m.Params[0] != wantParams {
		t.Errorf("Params[0] = %+v, 期望 %+v", m.Params[0], wantParams)
	}

	if len(m.Files) != 1 {
		t.Fatalf("Files 数量 = %d, 期望 1", len(m.Files))
	}
	f := m.Files[0]
	if f.Path != "root.wad" {
		t.Errorf("Path = %q, 期望 %q", f.Path, "root.wad")
	}
	if f.FileSize != 500 {
		t.Errorf("FileSize = %d, 期望 500", f.FileSize)
	}
	if len(f.Flags) != 1 || f.Flags[0] != "zh_CN" {
		t.Errorf("文件 Flags = %v, 期望 [zh_CN]", f.Flags)
	}
	if len(f.Chunks) != 2 {
		t.Fatalf("Chunks 数量 = %d, 期望 2", len(f.Chunks))
	}
	if f.Chunks[0].HashType != HashTypeHKDF {
		t.Errorf("Chunks[0].HashType = %s, 期望 HKDF（应由 param_index=0 关联）", f.Chunks[0].HashType)
	}

	// BundleSizes 应非空，且与 Files[].Chunks 反推一致：
	// 对每个出现的 BundleID，其 size 应不小于该 bundle 任意 chunk 的 BundleOffset+CompressedSize。
	if len(m.BundleSizes) == 0 {
		t.Fatal("BundleSizes 为空，期望至少 1 条")
	}
	for _, chunk := range f.Chunks {
		size, ok := m.BundleSizes[chunk.BundleID]
		if !ok {
			t.Errorf("BundleSizes 中找不到 BundleID=%X", chunk.BundleID)
			continue
		}
		if got, want := size, chunk.BundleOffset+chunk.CompressedSize; got < want {
			t.Errorf("BundleSizes[%X] = %d, 期望 >= %d（chunk %X 的 BundleOffset+CompressedSize）",
				chunk.BundleID, got, want, chunk.ChunkID)
		}
	}
}
