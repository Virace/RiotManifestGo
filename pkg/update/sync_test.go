package update

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/core"
	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/klauspost/compress/zstd"
)

// ---- 最小 RMAN FlatBuffers 编码器（仅测试使用） ----
//
// Sync 对"旧清单"的消费只经由 diff.Diff(old.Files, newManifest.Files)，只依赖
// 每个 FileEntry 的 Path 与 Chunks[].ChunkID 序列；旧内容的实际验证读取走磁盘上
// 现存的文件，而不是旧清单本身。因此这里的编码器只需要保证 Path 与 ChunkID
// 序列正确，其余字段（bundle_id、compressed_size、directory 等）用占位值即可，
// 不需要还原 pkg/rman 解析器所支持的全部特性（Language/Directory/Parameters）。
//
// 字段布局与 pkg/rman/flatbuffers.go、parser.go 描述的手写 FlatBuffers 格式
// 一一对应，构造顺序遵循"子对象先写、父对象后写并以相对偏移回填引用"的规则
// （与 pkg/rman/parser_test.go 的 buildFBBodyUsingRawBytes 同一手法）。

type fbBuf struct{ b []byte }

func (w *fbBuf) pos() uint32 { return uint32(len(w.b)) }

func (w *fbBuf) u16(v uint16) uint32 {
	p := w.pos()
	w.b = append(w.b, byte(v), byte(v>>8))
	return p
}

func (w *fbBuf) u32(v uint32) uint32 {
	p := w.pos()
	w.b = append(w.b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	return p
}

func (w *fbBuf) u64(v uint64) uint32 {
	p := w.pos()
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
	return p
}

func (w *fbBuf) raw(b []byte) uint32 {
	p := w.pos()
	w.b = append(w.b, b...)
	return p
}

func (w *fbBuf) patch32(pos uint32, v uint32) {
	w.b[pos] = byte(v)
	w.b[pos+1] = byte(v >> 8)
	w.b[pos+2] = byte(v >> 16)
	w.b[pos+3] = byte(v >> 24)
}

// writeString 写入一个 FlatBuffers 字符串对象（uint32 长度 + UTF-8 字节 + NUL），
// 返回其绝对位置。
func (w *fbBuf) writeString(s string) uint32 {
	pos := w.pos()
	w.u32(uint32(len(s)))
	w.raw([]byte(s))
	w.raw([]byte{0})
	return pos
}

// writeU64Vector 写入一个标量 uint64 vector（FileEntry.chunk_ids 用这种内联存储）。
func (w *fbBuf) writeU64Vector(vals []uint64) uint32 {
	pos := w.pos()
	w.u32(uint32(len(vals)))
	for _, v := range vals {
		w.u64(v)
	}
	return pos
}

// writeTableVector 写入一个 Table 型 vector（每元素是 referenceOffset），
// tablePositions 必须是已经写入完毕的子 Table 的绝对位置。
func (w *fbBuf) writeTableVector(tablePositions []uint32) uint32 {
	pos := w.pos()
	w.u32(uint32(len(tablePositions)))
	for _, tp := range tablePositions {
		slot := w.u32(0)
		w.patch32(slot, uint32(int32(tp)-int32(slot)))
	}
	return pos
}

// writeChunkTable 写入一个最小 Chunk Table，只含 field0=chunk_id。
// compressed_size/uncompressed_size 对旧清单的 diff 消费无意义，故不写（读取时
// 走 fbReadUint32 的 defaultVal=0 兜底，不影响 chunkMap 按 ChunkID 建索引）。
func (w *fbBuf) writeChunkTable(chunkID uint64) uint32 {
	vtablePos := w.pos()
	w.u16(6)  // vtable_len = 4 + 1*2（仅 field0）
	w.u16(12) // obj_size（未被读取端使用，仅占位）
	w.u16(4)  // field0(chunk_id) 相对 tableStart 的偏移
	tablePos := w.pos()
	soffsetPos := w.u32(0)
	w.patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
	w.u64(chunkID) // field0 @ tableStart+4
	return tablePos
}

// writeBundleTable 写入一个 Bundle Table：field0=bundle_id, field1=chunks(vector)。
func (w *fbBuf) writeBundleTable(bundleID uint64, chunksVecPos uint32) uint32 {
	vtablePos := w.pos()
	w.u16(8)  // vtable_len = 4 + 2*2
	w.u16(20) // obj_size（占位）
	w.u16(8)  // field0(bundle_id)
	w.u16(16) // field1(chunks)
	tablePos := w.pos()
	soffsetPos := w.u32(0)
	w.patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
	w.u32(0)                   // padding，对齐 bundle_id 到 tableStart+8
	w.u64(bundleID)            // field0 @ +8
	chunksFieldPos := w.u32(0) // field1 @ +16
	w.patch32(chunksFieldPos, uint32(int32(chunksVecPos)-int32(chunksFieldPos)))
	return tablePos
}

// writeFileEntryTable 写入一个 FileEntry Table：field2=file_size, field3=name,
// field7=chunk_ids(vector)。field1(directory_id) 缺省为 0，buildPath 遇 dirID=0
// 直接返回 name（等价"根目录平铺文件"），因此无需构造任何 Directory 对象。
func (w *fbBuf) writeFileEntryTable(fileSize uint64, nameRefPos, chunkIDsVecPos uint32) uint32 {
	vtablePos := w.pos()
	w.u16(20) // vtable_len = 4 + 8*2（field0..field7）
	w.u16(20) // obj_size（占位）
	w.u16(0)  // field0 file_entry_id（缺省）
	w.u16(0)  // field1 directory_id（缺省 -> 根目录）
	w.u16(4)  // field2 file_size
	w.u16(12) // field3 name
	w.u16(0)  // field4 language_mask（缺省）
	w.u16(0)  // field5（未使用）
	w.u16(0)  // field6（未使用）
	w.u16(16) // field7 chunk_ids
	tablePos := w.pos()
	soffsetPos := w.u32(0)
	w.patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
	w.u64(fileSize)          // field2 @ +4
	nameFieldPos := w.u32(0) // field3 @ +12
	w.patch32(nameFieldPos, uint32(int32(nameRefPos)-int32(nameFieldPos)))
	chunkIDsFieldPos := w.u32(0) // field7 @ +16
	w.patch32(chunkIDsFieldPos, uint32(int32(chunkIDsVecPos)-int32(chunkIDsFieldPos)))
	return tablePos
}

// writeRootTable 写入根 Table：field0=bundles(vector), field2=fileEntries(vector)。
// field1(languages)/field3(directories)/field5(parameters) 均缺省为空。
func (w *fbBuf) writeRootTable(bundlesVecPos, fileEntriesVecPos uint32) uint32 {
	vtablePos := w.pos()
	w.u16(16) // vtable_len = 4 + 6*2（field0..field5）
	w.u16(12) // obj_size（占位）
	w.u16(4)  // field0 bundles
	w.u16(0)  // field1 languages（缺省）
	w.u16(8)  // field2 fileEntries
	w.u16(0)  // field3 directories（缺省）
	w.u16(0)  // field4（未使用）
	w.u16(0)  // field5 parameters（缺省）
	tablePos := w.pos()
	soffsetPos := w.u32(0)
	w.patch32(soffsetPos, uint32(int32(tablePos)-int32(vtablePos)))
	bundlesFieldPos := w.u32(0) // field0 @ +4
	w.patch32(bundlesFieldPos, uint32(int32(bundlesVecPos)-int32(bundlesFieldPos)))
	fileEntriesFieldPos := w.u32(0) // field2 @ +8
	w.patch32(fileEntriesFieldPos, uint32(int32(fileEntriesVecPos)-int32(fileEntriesFieldPos)))
	return tablePos
}

// oldFileSpec 描述一份旧清单里的一个文件：只需要 Path 与 ChunkID 序列。
type oldFileSpec struct {
	path     string
	chunkIDs []uint64
}

// buildOldManifestBody 构造旧清单的 FlatBuffers Body 字节。
func buildOldManifestBody(specs []oldFileSpec) []byte {
	w := &fbBuf{}
	rootOffsetPos := w.u32(0) // 占位，最后回填

	seen := make(map[uint64]bool)
	var allChunkIDs []uint64
	for _, spec := range specs {
		for _, id := range spec.chunkIDs {
			if !seen[id] {
				seen[id] = true
				allChunkIDs = append(allChunkIDs, id)
			}
		}
	}

	chunkTablePositions := make([]uint32, len(allChunkIDs))
	for i, id := range allChunkIDs {
		chunkTablePositions[i] = w.writeChunkTable(id)
	}
	chunksVecPos := w.writeTableVector(chunkTablePositions)
	bundleTablePos := w.writeBundleTable(0x1, chunksVecPos)
	bundlesVecPos := w.writeTableVector([]uint32{bundleTablePos})

	fileEntryTablePositions := make([]uint32, len(specs))
	for i, spec := range specs {
		nameRefPos := w.writeString(spec.path)
		chunkIDsVecPos := w.writeU64Vector(spec.chunkIDs)
		fileEntryTablePositions[i] = w.writeFileEntryTable(0, nameRefPos, chunkIDsVecPos)
	}
	fileEntriesVecPos := w.writeTableVector(fileEntryTablePositions)

	rootTablePos := w.writeRootTable(bundlesVecPos, fileEntriesVecPos)
	w.patch32(rootOffsetPos, rootTablePos)

	return w.b
}

// zstdCompressBytes 用 ZSTD 压缩数据（与生产 rman.ParseFile 解压路径对应的编码器）。
func zstdCompressBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("创建 ZSTD 编码器失败: %v", err)
	}
	defer enc.Close()
	return enc.EncodeAll(data, nil)
}

// writeOldManifestFile 构造一份完整可被 rman.ParseFile 解析的旧清单文件并写入
// dir，返回其路径。
func writeOldManifestFile(t *testing.T, dir string, manifestID uint64, specs []oldFileSpec) string {
	t.Helper()
	body := buildOldManifestBody(specs)
	compressed := zstdCompressBytes(t, body)

	header := make([]byte, 28)
	copy(header[0:4], []byte("RMAN"))
	header[4] = 2
	header[5] = 0
	binary.LittleEndian.PutUint32(header[8:12], 28)
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(compressed)))
	binary.LittleEndian.PutUint64(header[16:24], manifestID)
	binary.LittleEndian.PutUint32(header[24:28], uint32(len(body)))

	full := append(header, compressed...)
	path := filepath.Join(dir, fmt.Sprintf("%016X.manifest", manifestID))
	if err := os.WriteFile(path, full, 0644); err != nil {
		t.Fatalf("写入旧清单文件失败: %v", err)
	}
	return path
}

// managedAutoOptions 建立一份与旧清单匹配的 schema 2 覆盖，并返回受管理 AUTO
// 选项。只有调用此辅助的测试才拥有 SKIP/MOVE/REMOVE 权限。
func managedAutoOptions(t *testing.T, dir, oldManifestPath string, manifestID uint64,
	specs []oldFileSpec, removeDeleted bool) Options {
	t.Helper()
	raw, err := os.ReadFile(oldManifestPath)
	if err != nil {
		t.Fatalf("读取旧清单失败: %v", err)
	}
	files := make([]string, 0, len(specs))
	for _, spec := range specs {
		files = append(files, spec.path)
	}
	if err := NewArchive(dir).Save(manifestID, raw, "test://old-manifest", files); err != nil {
		t.Fatalf("建立旧安装状态失败: %v", err)
	}
	return Options{
		Operation:       OperationInstall,
		Mode:            ModeAuto,
		OldManifestPath: oldManifestPath,
		RemoveDeleted:   removeDeleted,
	}
}

// TestOldManifestFixtureRoundTrips 验证测试专用编码器产出的旧清单能被生产
// rman.ParseFile 正确解析回 Path + ChunkID 序列，是后续全部 Sync 测试的地基。
func TestOldManifestFixtureRoundTrips(t *testing.T) {
	dir := t.TempDir()
	specs := []oldFileSpec{
		{path: "a/one.bin", chunkIDs: []uint64{0x1111, 0x2222, 0x3333}},
		{path: "two.bin", chunkIDs: []uint64{0x4444}},
		{path: "empty.bin", chunkIDs: nil},
	}
	path := writeOldManifestFile(t, dir, 0xABCDEF0123456789, specs)

	manifest, err := rman.ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile 失败: %v", err)
	}
	if manifest.ManifestID != 0xABCDEF0123456789 {
		t.Errorf("ManifestID = %X, want %X", manifest.ManifestID, uint64(0xABCDEF0123456789))
	}
	if len(manifest.Files) != len(specs) {
		t.Fatalf("Files 数量 = %d, want %d", len(manifest.Files), len(specs))
	}
	for i, spec := range specs {
		got := manifest.Files[i]
		if got.Path != spec.path {
			t.Errorf("Files[%d].Path = %q, want %q", i, got.Path, spec.path)
		}
		if len(got.Chunks) != len(spec.chunkIDs) {
			t.Fatalf("Files[%d].Chunks 数量 = %d, want %d", i, len(got.Chunks), len(spec.chunkIDs))
		}
		for j, id := range spec.chunkIDs {
			if got.Chunks[j].ChunkID != id {
				t.Errorf("Files[%d].Chunks[%d].ChunkID = %X, want %X", i, j, got.Chunks[j].ChunkID, id)
			}
		}
	}
}

// ---- mock core.BundleFetcher ----

// mockChunkSource 描述一个已注册进 mockFetcher 的 Chunk：所属 Bundle 文件名、
// Bundle 内偏移、压缩字节，用于按请求的字节区间精确拼出响应。
type mockChunkSource struct {
	bundleFilename string
	offset         uint32
	compressed     []byte
}

// mockFetcher 是 core.BundleFetcher 的测试替身：按注册的 Chunk 精确拼出任意
// 请求区间的响应（支持 Gap Tolerance 合并产生的多 Chunk 连续 Range），并可以让
// 指定 Bundle 文件名恒定失败，用于构造部分下载失败场景。
type mockFetcher struct {
	mu              sync.Mutex
	chunks          map[uint64]mockChunkSource
	byBundle        map[string][]uint64
	failBundles     map[string]bool
	requestedRanges []core.ByteRange
	callCount       int
}

func newMockFetcher() *mockFetcher {
	return &mockFetcher{
		chunks:      make(map[uint64]mockChunkSource),
		byBundle:    make(map[string][]uint64),
		failBundles: make(map[string]bool),
	}
}

// addChunk 注册一个 Chunk 的数据来源，chunkID 必须与 rman.ChunkInfo.ChunkID 一致。
func (m *mockFetcher) addChunk(chunkID, bundleID uint64, offset uint32, compressed []byte) {
	filename := core.BundleFilename(bundleID)
	m.chunks[chunkID] = mockChunkSource{bundleFilename: filename, offset: offset, compressed: compressed}
	m.byBundle[filename] = append(m.byBundle[filename], chunkID)
}

func (m *mockFetcher) FetchRanges(_ context.Context, bundleFilename string, ranges []core.ByteRange, _ core.FetchOptions) ([][]byte, error) {
	m.mu.Lock()
	m.callCount++
	m.requestedRanges = append(m.requestedRanges, ranges...)
	fail := m.failBundles[bundleFilename]
	m.mu.Unlock()

	if fail {
		return nil, fmt.Errorf("mock: 模拟 bundle %s 下载失败", bundleFilename)
	}

	result := make([][]byte, len(ranges))
	for i, r := range ranges {
		data, err := m.buildRangeData(bundleFilename, r)
		if err != nil {
			return nil, err
		}
		result[i] = data
	}
	return result, nil
}

func (m *mockFetcher) buildRangeData(bundleFilename string, r core.ByteRange) ([]byte, error) {
	m.mu.Lock()
	ids := append([]uint64(nil), m.byBundle[bundleFilename]...)
	m.mu.Unlock()

	type piece struct {
		offset uint32
		data   []byte
	}
	var pieces []piece
	for _, id := range ids {
		c := m.chunks[id]
		if int64(c.offset) >= r.Start && int64(c.offset) <= r.End {
			pieces = append(pieces, piece{c.offset, c.compressed})
		}
	}
	sort.Slice(pieces, func(i, j int) bool { return pieces[i].offset < pieces[j].offset })

	var buf []byte
	expected := uint32(r.Start)
	for _, p := range pieces {
		if p.offset != expected {
			return nil, fmt.Errorf("mock: range [%d,%d] 内部不连续，chunk offset=%d 期望=%d", r.Start, r.End, p.offset, expected)
		}
		buf = append(buf, p.data...)
		expected += uint32(len(p.data))
	}
	if len(buf) == 0 {
		return nil, fmt.Errorf("mock: 未配置的区间 bundle=%s start=%d end=%d", bundleFilename, r.Start, r.End)
	}
	return buf, nil
}

func (m *mockFetcher) Close() {}

// ---- Chunk/FileEntry 测试构造辅助 ----

// syncChunk 打包一个测试用 Chunk 的原始数据、压缩数据与 rman.ChunkInfo。
type syncChunk struct {
	raw        []byte
	compressed []byte
	info       rman.ChunkInfo
}

// makeSyncChunk 构造一个测试 Chunk：ChunkID 用真实 SHA256 算出（与生产路径一致）。
func makeSyncChunk(t *testing.T, data []byte, bundleID uint64, offset uint32) syncChunk {
	t.Helper()
	compressed := zstdCompressBytes(t, data)
	return syncChunk{
		raw:        data,
		compressed: compressed,
		info: rman.ChunkInfo{
			ChunkID:          zstream.ComputeHash(data, rman.HashTypeSHA256),
			BundleID:         bundleID,
			BundleOffset:     offset,
			CompressedSize:   uint32(len(compressed)),
			UncompressedSize: uint32(len(data)),
			HashType:         rman.HashTypeSHA256,
		},
	}
}

// makeSyncFileEntry 从一组 syncChunk 构造 rman.FileEntry。
func makeSyncFileEntry(path string, chunks ...syncChunk) rman.FileEntry {
	var size uint64
	infos := make([]rman.ChunkInfo, len(chunks))
	for i, c := range chunks {
		infos[i] = c.info
		size += uint64(c.info.UncompressedSize)
	}
	return rman.FileEntry{Path: path, FileSize: size, Chunks: infos}
}

// newSyncTestDownloader 创建一个注入 mockFetcher 的 core.Downloader，outputDir 为
// 临时目录，GapTolerance=0（关闭间隙合并，简化 mock 区间断言）、MaxRetries=-1
// （不重试，重试策略已由 pkg/core 自己的测试覆盖，这里只关心 Sync 的编排逻辑，
// 避免指数退避拖慢测试）。
func newSyncTestDownloader(t *testing.T, mock *mockFetcher) (*core.Downloader, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := core.DownloadConfig{
		OutputDir:       dir,
		Workers:         2,
		MaxFileHandles:  50,
		MaxRangesPerReq: 30,
		GapTolerance:    0,
		MaxRetries:      -1,
	}
	return core.NewDownloaderWithFetcher(cfg, mock), dir
}

// 说明：本文件的测试均不消费 d.Events()。emit 是非阻塞的（channel 满则丢弃事件，
// 见 pkg/core/downloader.go 的 emit 实现），且 Sync 不拥有 Downloader 的事件
// channel 生命周期（不会 close 它，交由调用方决定，与 DownloadTasks 的既有约定
// 一致），因此不消费事件既不会阻塞生产者，也不需要额外的收尾同步。

// snapshotDir 递归读取 dir 下全部普通文件的相对路径与内容，用于比较 Sync 前后
// 目录状态是否完全一致（ModeVerifyOnly 的零写盘断言）。
func snapshotDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	snap := make(map[string][]byte)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(dir, path)
		if relErr != nil {
			return relErr
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		snap[rel] = data
		return nil
	})
	if err != nil {
		t.Fatalf("快照目录失败: %v", err)
	}
	return snap
}

// dirSnapshotsEqual 比较两份 snapshotDir 结果是否完全一致（文件集合与内容均相同）。
func dirSnapshotsEqual(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		other, ok := b[k]
		if !ok || !bytes.Equal(v, other) {
			return false
		}
	}
	return true
}

// ---- Sync 端到端测试 ----

// TestSync_Auto_IncrementalPatch_PartialChunkMiss 验证 ModeAuto 的核心增量路径：
// 3 个文件在位，新清单只改动 1 个文件的 5 个 chunk 中的 2 个——fetcher 只应该
// 收到这 2 个 chunk 对应的字节区间，Reused/Downloaded 字节数精确匹配，另外两个
// Unchanged 文件被文件级跳过、mtime 与字节都不变。
func TestSync_Auto_IncrementalPatch_PartialChunkMiss(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	faData := []byte("file-a-unchanged-content-payload!!!")
	fcData := []byte("file-c-unchanged-content-payload!!!")
	c0 := makeSyncChunk(t, []byte("AAAAAAAAAAAAAAAAAAAA"), 0, 0)
	c1 := makeSyncChunk(t, []byte("BBBBBBBBBBBBBBBBBBBB"), 0, 0)
	c2 := makeSyncChunk(t, []byte("CCCCCCCCCCCCCCCCCCCC"), 0, 0)
	oldC3 := makeSyncChunk(t, []byte("DDDDDDDDDDDDDDDDDDDD"), 0, 0)
	oldC4 := makeSyncChunk(t, []byte("EEEEEEEEEEEEEEEEEEEE"), 0, 0)

	const bundleID uint64 = 0x9000000000000001
	newC3 := makeSyncChunk(t, []byte("XXXXXXXXXXXXXXXXXXXX"), bundleID, 0)
	newC4 := makeSyncChunk(t, []byte("YYYYYYYYYYYYYYYYYYYY"), bundleID, newC3.info.CompressedSize)

	mock.addChunk(newC3.info.ChunkID, bundleID, newC3.info.BundleOffset, newC3.compressed)
	mock.addChunk(newC4.info.ChunkID, bundleID, newC4.info.BundleOffset, newC4.compressed)

	oldFileB := concatBytes(c0.raw, c1.raw, c2.raw, oldC3.raw, oldC4.raw)
	writeFile(t, dir, "fileA.bin", faData)
	writeFile(t, dir, "fileB.bin", oldFileB)
	writeFile(t, dir, "fileC.bin", fcData)

	faChunk := makeSyncChunk(t, faData, 0, 0)
	fcChunk := makeSyncChunk(t, fcData, 0, 0)

	oldSpecs := []oldFileSpec{
		{path: "fileA.bin", chunkIDs: []uint64{faChunk.info.ChunkID}},
		{path: "fileB.bin", chunkIDs: []uint64{c0.info.ChunkID, c1.info.ChunkID, c2.info.ChunkID, oldC3.info.ChunkID, oldC4.info.ChunkID}},
		{path: "fileC.bin", chunkIDs: []uint64{fcChunk.info.ChunkID}},
	}
	oldManifestPath := writeOldManifestFile(t, dir, 0x1111111111111111, oldSpecs)

	newFiles := []rman.FileEntry{
		makeSyncFileEntry("fileA.bin", faChunk),
		makeSyncFileEntry("fileB.bin", c0, c1, c2, newC3, newC4),
		makeSyncFileEntry("fileC.bin", fcChunk),
	}

	pathA := filepath.Join(dir, "fileA.bin")
	pathC := filepath.Join(dir, "fileC.bin")
	statA, err := os.Stat(pathA)
	if err != nil {
		t.Fatalf("stat fileA 失败: %v", err)
	}
	statC, err := os.Stat(pathC)
	if err != nil {
		t.Fatalf("stat fileC 失败: %v", err)
	}
	mtimeA, mtimeC := statA.ModTime(), statC.ModTime()

	newManifest := &rman.Manifest{ManifestID: 0x2222222222222222, Files: newFiles}
	opts := managedAutoOptions(t, dir, oldManifestPath, 0x1111111111111111, oldSpecs, true)

	stats, err := Sync(context.Background(), newManifest, []byte("raw-new-manifest-bytes"), "https://example.com/new.manifest", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Failed) != 0 {
		t.Fatalf("Failed 应为空: %v", stats.Failed)
	}

	wantReused := int64(len(c0.raw) + len(c1.raw) + len(c2.raw))
	wantDownloaded := int64(len(newC3.raw) + len(newC4.raw))
	if stats.ReusedBytes != wantReused {
		t.Errorf("ReusedBytes = %d, want %d", stats.ReusedBytes, wantReused)
	}
	if stats.DownloadedBytes != wantDownloaded {
		t.Errorf("DownloadedBytes = %d, want %d", stats.DownloadedBytes, wantDownloaded)
	}

	if len(mock.requestedRanges) != 1 {
		t.Fatalf("期望恰好 1 次 Range 请求（newC3+newC4 连续合并），got %d: %v", len(mock.requestedRanges), mock.requestedRanges)
	}
	wantEnd := int64(newC3.info.CompressedSize) + int64(newC4.info.CompressedSize) - 1
	if mock.requestedRanges[0].Start != 0 || mock.requestedRanges[0].End != wantEnd {
		t.Errorf("请求区间 = [%d,%d], want [0,%d]", mock.requestedRanges[0].Start, mock.requestedRanges[0].End, wantEnd)
	}

	if got := stats.Skipped; len(got) != 2 {
		t.Fatalf("Skipped 期望 2 个 (fileA, fileC)，got %v", got)
	}
	if got := stats.Patched; len(got) != 1 || got[0] != "fileB.bin" {
		t.Fatalf("Patched 期望 [fileB.bin]，got %v", got)
	}

	gotB, err := os.ReadFile(filepath.Join(dir, "fileB.bin"))
	if err != nil {
		t.Fatalf("读取 fileB 失败: %v", err)
	}
	wantB := concatBytes(c0.raw, c1.raw, c2.raw, newC3.raw, newC4.raw)
	if !bytes.Equal(gotB, wantB) {
		t.Errorf("fileB 内容不匹配:\n got=%q\nwant=%q", gotB, wantB)
	}

	newStatA, err := os.Stat(pathA)
	if err != nil {
		t.Fatalf("stat fileA 失败: %v", err)
	}
	newStatC, err := os.Stat(pathC)
	if err != nil {
		t.Fatalf("stat fileC 失败: %v", err)
	}
	if !newStatA.ModTime().Equal(mtimeA) {
		t.Errorf("fileA mtime 被改动: got %v, want %v", newStatA.ModTime(), mtimeA)
	}
	if !newStatC.ModTime().Equal(mtimeC) {
		t.Errorf("fileC mtime 被改动: got %v, want %v", newStatC.ModTime(), mtimeC)
	}
	gotA, err := os.ReadFile(pathA)
	if err != nil {
		t.Fatalf("读取 fileA 失败: %v", err)
	}
	if !bytes.Equal(gotA, faData) {
		t.Error("fileA 字节被改动")
	}

	archive := NewArchive(dir)
	state, err := archive.LoadInstalled()
	if err != nil || state == nil {
		t.Fatalf("installed.json 未正确写入: err=%v state=%v", err, state)
	}
	wantID := fmt.Sprintf("%016X", newManifest.ManifestID)
	if state.ManifestID != wantID {
		t.Errorf("installed.json ManifestID = %s, want %s", state.ManifestID, wantID)
	}
	if wantFiles := []string{"fileA.bin", "fileB.bin", "fileC.bin"}; !slices.Equal(state.Files, wantFiles) {
		t.Errorf("跨版本覆盖未正确携带/确认: Files=%v want=%v", state.Files, wantFiles)
	}
}

// TestSync_Auto_Moved_ZeroDownload 验证 Moved 文件整文件本地复制、零网络请求。
func TestSync_Auto_Moved_ZeroDownload(t *testing.T) {
	mock := newMockFetcher() // 未注册任何 chunk：一旦被请求会直接报错。
	d, dir := newSyncTestDownloader(t, mock)

	data := []byte("moved-file-content-should-be-copied-not-downloaded!")
	chunk := makeSyncChunk(t, data, 0, 0)

	writeFile(t, dir, "old-name.bin", data)

	oldSpecs := []oldFileSpec{{path: "old-name.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, 0x3333333333333333, oldSpecs)

	newFiles := []rman.FileEntry{makeSyncFileEntry("new-name.bin", chunk)}
	newManifest := &rman.Manifest{ManifestID: 0x4444444444444444, Files: newFiles}

	opts := managedAutoOptions(t, dir, oldManifestPath, 0x3333333333333333, oldSpecs, true)
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Failed) != 0 {
		t.Fatalf("Failed 应为空: %v", stats.Failed)
	}
	if len(stats.Moved) != 1 || stats.Moved[0] != "new-name.bin" {
		t.Fatalf("Moved 期望 [new-name.bin]，got %v", stats.Moved)
	}
	if mock.callCount != 0 {
		t.Errorf("Moved 不应触发任何网络请求，实际调用次数 %d", mock.callCount)
	}
	if stats.DownloadedBytes != 0 {
		t.Errorf("DownloadedBytes 期望 0，got %d", stats.DownloadedBytes)
	}
	if stats.ReusedBytes != int64(len(data)) {
		t.Errorf("ReusedBytes = %d, want %d", stats.ReusedBytes, len(data))
	}

	gotNew, err := os.ReadFile(filepath.Join(dir, "new-name.bin"))
	if err != nil {
		t.Fatalf("读取新路径失败: %v", err)
	}
	if !bytes.Equal(gotNew, data) {
		t.Errorf("新路径内容不匹配: got %q want %q", gotNew, data)
	}
}

// TestSync_Auto_Removed_RespectsKeepFlag 验证 Removed 处理只删除 diff 判定的旧
// 路径，且 RemoveDeleted=false 时保留文件不动。
func TestSync_Auto_Removed_RespectsKeepFlag(t *testing.T) {
	for _, removeDeleted := range []bool{true, false} {
		t.Run(fmt.Sprintf("RemoveDeleted=%v", removeDeleted), func(t *testing.T) {
			mock := newMockFetcher()
			d, dir := newSyncTestDownloader(t, mock)

			keptData := []byte("kept-file-content-unchanged!!!!!")
			removedData := []byte("removed-file-content-should-maybe-go")
			keptChunk := makeSyncChunk(t, keptData, 0, 0)
			removedChunk := makeSyncChunk(t, removedData, 0, 0)

			writeFile(t, dir, "kept.bin", keptData)
			writeFile(t, dir, "removed.bin", removedData)

			oldSpecs := []oldFileSpec{
				{path: "kept.bin", chunkIDs: []uint64{keptChunk.info.ChunkID}},
				{path: "removed.bin", chunkIDs: []uint64{removedChunk.info.ChunkID}},
			}
			oldManifestPath := writeOldManifestFile(t, dir, 0x5555555555555555, oldSpecs)

			newFiles := []rman.FileEntry{makeSyncFileEntry("kept.bin", keptChunk)}
			newManifest := &rman.Manifest{ManifestID: 0x6666666666666666, Files: newFiles}

			opts := managedAutoOptions(t, dir, oldManifestPath, 0x5555555555555555, oldSpecs, removeDeleted)
			stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
			if err != nil {
				t.Fatalf("Sync 失败: %v", err)
			}

			removedPath := filepath.Join(dir, "removed.bin")
			_, statErr := os.Stat(removedPath)
			if removeDeleted {
				if !os.IsNotExist(statErr) {
					t.Errorf("RemoveDeleted=true 时 removed.bin 应被删除, stat err=%v", statErr)
				}
				if len(stats.Removed) != 1 || stats.Removed[0] != "removed.bin" {
					t.Errorf("Stats.Removed 期望 [removed.bin]，got %v", stats.Removed)
				}
			} else {
				if statErr != nil {
					t.Errorf("RemoveDeleted=false 时 removed.bin 应保留, stat err=%v", statErr)
				}
				if len(stats.Removed) != 0 {
					t.Errorf("RemoveDeleted=false 时 Stats.Removed 期望空，got %v", stats.Removed)
				}
			}

			gotKept, err := os.ReadFile(filepath.Join(dir, "kept.bin"))
			if err != nil {
				t.Fatalf("读取 kept.bin 失败: %v", err)
			}
			if !bytes.Equal(gotKept, keptData) {
				t.Error("kept.bin 内容被意外改动")
			}
		})
	}
}

// TestSync_ModeVerifyOnly_NoSideEffects 验证 ModeVerifyOnly 零写盘：Sync 前后
// 目录快照必须完全一致，且不触发任何网络请求、不创建 installed.json。
func TestSync_ModeVerifyOnly_NoSideEffects(t *testing.T) {
	mock := newMockFetcher() // 未注册任何 chunk：一旦被请求会直接报错。
	d, dir := newSyncTestDownloader(t, mock)

	goodData := []byte("intact-file-content-matches-manifest!!!")
	badData := []byte("corrupted-file-content-does-not-match!!")
	declaredData := []byte("declared-file-content-differs-from-disk")
	goodChunk := makeSyncChunk(t, goodData, 0, 0)
	declaredChunk := makeSyncChunk(t, declaredData, 0, 0)

	writeFile(t, dir, "good.bin", goodData)
	writeFile(t, dir, "bad.bin", badData)

	newFiles := []rman.FileEntry{
		makeSyncFileEntry("good.bin", goodChunk),
		makeSyncFileEntry("bad.bin", declaredChunk),
	}
	newManifest := &rman.Manifest{ManifestID: 0x7777777777777777, Files: newFiles}

	before := snapshotDir(t, dir)

	opts := Options{Mode: ModeVerifyOnly}
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	after := snapshotDir(t, dir)
	if !dirSnapshotsEqual(before, after) {
		t.Errorf("ModeVerifyOnly 应零写盘，快照前后不一致\nbefore=%v\nafter=%v", before, after)
	}
	if mock.callCount != 0 {
		t.Errorf("ModeVerifyOnly 应零下载，实际网络调用次数 %d", mock.callCount)
	}
	if len(stats.Skipped) != 1 || stats.Skipped[0] != "good.bin" {
		t.Errorf("Skipped 期望 [good.bin]，got %v", stats.Skipped)
	}
	if len(stats.Failed) != 1 || stats.Failed[0] != "bad.bin" {
		t.Errorf("Failed 期望 [bad.bin]（校验发现问题），got %v", stats.Failed)
	}

	archive := NewArchive(dir)
	if _, ok := archive.InstalledManifestPath(); ok {
		t.Error("ModeVerifyOnly 不应创建 installed.json")
	}
}

// TestSync_ModeForceFull_AlwaysDownloadsEverything 验证 ForceFull 跳过验证、
// 即使本地内容已经与新清单完全一致也整体走网络。
func TestSync_ModeForceFull_AlwaysDownloadsEverything(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	data := []byte("content-already-correct-on-disk-before-sync-runs")
	const bundleID uint64 = 0x8800000000000001
	chunk := makeSyncChunk(t, data, bundleID, 0)
	mock.addChunk(chunk.info.ChunkID, bundleID, chunk.info.BundleOffset, chunk.compressed)

	writeFile(t, dir, "file.bin", data)

	newFiles := []rman.FileEntry{makeSyncFileEntry("file.bin", chunk)}
	newManifest := &rman.Manifest{ManifestID: 0x9999999999999999, Files: newFiles}

	opts := Options{Mode: ModeForceFull}
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if mock.callCount == 0 {
		t.Error("ModeForceFull 应该发起网络请求，即使本地内容已经正确")
	}
	if len(stats.Created) != 1 || stats.Created[0] != "file.bin" {
		t.Errorf("Created 期望 [file.bin]，got %v", stats.Created)
	}
	if stats.DownloadedBytes != int64(len(data)) {
		t.Errorf("DownloadedBytes = %d, want %d", stats.DownloadedBytes, len(data))
	}
	if stats.ReusedBytes != 0 {
		t.Errorf("ForceFull 不应有任何本地复用，ReusedBytes = %d", stats.ReusedBytes)
	}

	got, err := os.ReadFile(filepath.Join(dir, "file.bin"))
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("file.bin 内容不匹配")
	}
}

// TestSync_Auto_NoOldManifest_FullVerifyZeroDownloadWhenIntact 验证无 -update 且
// 无 installed.json 时退化为全量验证：本地内容已经完好则零下载。
func TestSync_Auto_NoOldManifest_FullVerifyZeroDownloadWhenIntact(t *testing.T) {
	mock := newMockFetcher() // 未注册任何 chunk：一旦被请求会直接报错。
	d, dir := newSyncTestDownloader(t, mock)

	data := []byte("no-old-manifest-file-already-correct-on-disk!!!")
	chunk := makeSyncChunk(t, data, 0, 0)
	writeFile(t, dir, "file.bin", data)

	newFiles := []rman.FileEntry{makeSyncFileEntry("file.bin", chunk)}
	newManifest := &rman.Manifest{ManifestID: 0xAAAAAAAAAAAAAAAA, Files: newFiles}

	opts := Options{Mode: ModeAuto, RemoveDeleted: true}
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if mock.callCount != 0 {
		t.Errorf("本地完好时应零下载，实际网络调用次数 %d", mock.callCount)
	}
	if len(stats.Skipped) != 1 || stats.Skipped[0] != "file.bin" {
		t.Errorf("Skipped 期望 [file.bin]，got %v", stats.Skipped)
	}
	if len(stats.Failed) != 0 {
		t.Errorf("Failed 应为空: %v", stats.Failed)
	}
}

// TestSync_Auto_PartialDownloadFailure_KeepsOldFileAndDoesNotAdvanceInstalled 验证
// 一个文件下载失败时：其旧文件保持无损、进 Failed 且 staging 已丢弃；其余文件
// 正常提交；installed.json 不推进。
func TestSync_Auto_PartialDownloadFailure_KeepsOldFileAndDoesNotAdvanceInstalled(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	oldAData := []byte("file-a-old-content-must-survive-a-failed-patch!")
	oldAChunk := makeSyncChunk(t, oldAData, 0, 0)
	const bundleA uint64 = 0xBB00000000000001
	newAData := []byte("file-a-new-content-download-will-fail-for-this!")
	newAChunk := makeSyncChunk(t, newAData, bundleA, 0)
	mock.failBundles[core.BundleFilename(bundleA)] = true

	oldBData := []byte("file-b-old-content")
	oldBChunk := makeSyncChunk(t, oldBData, 0, 0)
	const bundleB uint64 = 0xBB00000000000002
	newBData := []byte("file-b-new-content!")
	newBChunk := makeSyncChunk(t, newBData, bundleB, 0)
	mock.addChunk(newBChunk.info.ChunkID, bundleB, newBChunk.info.BundleOffset, newBChunk.compressed)

	writeFile(t, dir, "fileA.bin", oldAData)
	writeFile(t, dir, "fileB.bin", oldBData)

	oldSpecs := []oldFileSpec{
		{path: "fileA.bin", chunkIDs: []uint64{oldAChunk.info.ChunkID}},
		{path: "fileB.bin", chunkIDs: []uint64{oldBChunk.info.ChunkID}},
	}
	oldManifestPath := writeOldManifestFile(t, dir, 0xCCCCCCCCCCCCCCCC, oldSpecs)

	newFiles := []rman.FileEntry{
		makeSyncFileEntry("fileA.bin", newAChunk),
		makeSyncFileEntry("fileB.bin", newBChunk),
	}
	newManifest := &rman.Manifest{ManifestID: 0xDDDDDDDDDDDDDDDD, Files: newFiles}

	opts := Options{
		Operation:       OperationInstall,
		Mode:            ModeAuto,
		OldManifestPath: oldManifestPath,
		RemoveDeleted:   true,
	}
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 不应返回顶层 error（部分失败走 Stats.Failed）: %v", err)
	}

	if len(stats.Failed) != 1 || stats.Failed[0] != "fileA.bin" {
		t.Fatalf("Failed 期望 [fileA.bin]，got %v", stats.Failed)
	}
	if len(stats.Patched) != 1 || stats.Patched[0] != "fileB.bin" {
		t.Fatalf("Patched 期望 [fileB.bin]，got %v", stats.Patched)
	}

	gotA, err := os.ReadFile(filepath.Join(dir, "fileA.bin"))
	if err != nil {
		t.Fatalf("读取 fileA 失败: %v", err)
	}
	if !bytes.Equal(gotA, oldAData) {
		t.Errorf("失败文件的旧内容应保持不变: got %q want %q", gotA, oldAData)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "fileA.bin.rman-tmp")); !os.IsNotExist(statErr) {
		t.Errorf("失败文件的 staging 应已被丢弃, stat err=%v", statErr)
	}

	gotB, err := os.ReadFile(filepath.Join(dir, "fileB.bin"))
	if err != nil {
		t.Fatalf("读取 fileB 失败: %v", err)
	}
	if !bytes.Equal(gotB, newBData) {
		t.Errorf("成功文件应提交为新内容: got %q want %q", gotB, newBData)
	}

	archive := NewArchive(dir)
	if _, ok := archive.InstalledManifestPath(); ok {
		t.Error("存在 Failed 文件时不应推进 installed.json")
	}
}

// TestSync_Auto_SkipsCorruptedUnchanged_RepairFixes 验证既定权衡：
// ModeAuto 下与旧清单一致的（Unchanged）文件即使本地已损坏也不会被修复
// （文件级跳过、不验证）；同一目录换 ModeRepair 重跑则能验证补洞、修复损坏。
func TestSync_Auto_SkipsCorruptedUnchanged_RepairFixes(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	goodData := []byte("this-is-the-correct-file-content-per-manifest!!")
	chunk := makeSyncChunk(t, goodData, 0, 0)

	corrupted := make([]byte, len(goodData))
	copy(corrupted, goodData)
	corrupted[0] ^= 0xFF

	writeFile(t, dir, "file.bin", corrupted)

	oldSpecs := []oldFileSpec{{path: "file.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, 0xEEEEEEEEEEEEEEEE, oldSpecs)

	newFiles := []rman.FileEntry{makeSyncFileEntry("file.bin", chunk)}
	newManifest := &rman.Manifest{ManifestID: 0xFFFFFFFFFFFFFFFF, Files: newFiles}

	// 第一次：ModeAuto，diff 判 Unchanged，文件级跳过、不验证——损坏未被修复。
	autoOpts := managedAutoOptions(t, dir, oldManifestPath, 0xEEEEEEEEEEEEEEEE, oldSpecs, true)
	autoStats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, autoOpts)
	if err != nil {
		t.Fatalf("ModeAuto Sync 失败: %v", err)
	}
	if len(autoStats.Skipped) != 1 || autoStats.Skipped[0] != "file.bin" {
		t.Fatalf("ModeAuto 期望 Skipped=[file.bin]，got %+v", autoStats)
	}
	afterAuto, err := os.ReadFile(filepath.Join(dir, "file.bin"))
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if !bytes.Equal(afterAuto, corrupted) {
		t.Fatalf("ModeAuto 不应修复被跳过的文件，但内容已改变: got %q want(仍损坏) %q", afterAuto, corrupted)
	}
	if bytes.Equal(afterAuto, goodData) {
		t.Fatal("前提假设失败：损坏文件不应该恰好等于正确内容")
	}

	// 第二次：ModeRepair，逐 chunk 验证补洞，修复损坏。
	const bundleID uint64 = 0xEF00000000000001
	repairChunk := makeSyncChunk(t, goodData, bundleID, 0)
	mock.addChunk(repairChunk.info.ChunkID, bundleID, repairChunk.info.BundleOffset, repairChunk.compressed)
	newFilesForRepair := []rman.FileEntry{makeSyncFileEntry("file.bin", repairChunk)}

	repairOpts := Options{Mode: ModeRepair}
	repairStats, err := Sync(context.Background(), newManifest, []byte("raw2"), "src", dir, newFilesForRepair, d, repairOpts)
	if err != nil {
		t.Fatalf("ModeRepair Sync 失败: %v", err)
	}
	if len(repairStats.Failed) != 0 {
		t.Fatalf("ModeRepair Failed 应为空: %v", repairStats.Failed)
	}
	if len(repairStats.Patched) != 1 || repairStats.Patched[0] != "file.bin" {
		t.Fatalf("ModeRepair 期望 Patched=[file.bin]，got %+v", repairStats)
	}

	afterRepair, err := os.ReadFile(filepath.Join(dir, "file.bin"))
	if err != nil {
		t.Fatalf("读取失败: %v", err)
	}
	if !bytes.Equal(afterRepair, goodData) {
		t.Errorf("ModeRepair 后内容应修复为正确内容: got %q want %q", afterRepair, goodData)
	}
}

// TestSync_Auto_Moved_SourceMissing_DiscardsStagingKeepsOthers 验证缺失的受管理 MOVE
// 源会在规划阶段降级为 PATCH，下载失败后不残留 staging；同批次成功提交的 MOVE
// 目标可以落盘，但因为整批未成功，旧源不清理且 installed.json 保持旧状态。
func TestSync_Auto_Moved_SourceMissing_DiscardsStagingKeepsOthers(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	goodData := []byte("moved-source-exists-should-copy-and-source-removed")
	goodChunk := makeSyncChunk(t, goodData, 0, 0)
	writeFile(t, dir, "good-old.bin", goodData)

	missingChunk := makeSyncChunk(t, []byte("moved-source-missing-content-declaration-only!!"), 0, 0)
	// 故意不写 missing-old.bin：旧清单声明它存在，但 Sync 运行时磁盘上已经缺失。

	oldSpecs := []oldFileSpec{
		{path: "good-old.bin", chunkIDs: []uint64{goodChunk.info.ChunkID}},
		{path: "missing-old.bin", chunkIDs: []uint64{missingChunk.info.ChunkID}},
	}
	oldManifestPath := writeOldManifestFile(t, dir, 0x3131313131313131, oldSpecs)

	newFiles := []rman.FileEntry{
		makeSyncFileEntry("good-new.bin", goodChunk),
		makeSyncFileEntry("missing-new.bin", missingChunk),
	}
	newManifest := &rman.Manifest{ManifestID: 0x4141414141414141, Files: newFiles}

	opts := managedAutoOptions(t, dir, oldManifestPath, 0x3131313131313131, oldSpecs, true)
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 不应返回顶层 error（部分失败走 Stats.Failed）: %v", err)
	}

	if len(stats.Failed) != 1 || stats.Failed[0] != "missing-new.bin" {
		t.Fatalf("Failed 期望 [missing-new.bin]，got %v", stats.Failed)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "missing-new.bin.rman-tmp")); !os.IsNotExist(statErr) {
		t.Errorf("失败 Moved 文件的 staging 应已被丢弃, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "missing-new.bin")); !os.IsNotExist(statErr) {
		t.Errorf("失败 Moved 文件不应生成最终文件, stat err=%v", statErr)
	}
	// 源路径本来就不存在，Sync 也不应该凭空创建它。
	if _, statErr := os.Stat(filepath.Join(dir, "missing-old.bin")); !os.IsNotExist(statErr) {
		t.Errorf("失败 Moved 文件的（本就缺失的）源路径不应被创建, stat err=%v", statErr)
	}

	if len(stats.Moved) != 1 || stats.Moved[0] != "good-new.bin" {
		t.Fatalf("Moved 期望 [good-new.bin]，got %v", stats.Moved)
	}
	gotGood, err := os.ReadFile(filepath.Join(dir, "good-new.bin"))
	if err != nil {
		t.Fatalf("读取 good-new.bin 失败: %v", err)
	}
	if !bytes.Equal(gotGood, goodData) {
		t.Errorf("good-new.bin 内容不匹配: got %q want %q", gotGood, goodData)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "good-old.bin")); statErr != nil {
		t.Errorf("整批存在失败时不应清理成功 Moved 的源路径, stat err=%v", statErr)
	}

	archive := NewArchive(dir)
	state, loadErr := archive.LoadInstalled()
	if loadErr != nil || state == nil {
		t.Fatalf("旧 installed.json 应保留: state=%+v err=%v", state, loadErr)
	}
	if state.ManifestID != "3131313131313131" {
		t.Errorf("存在 Failed 文件时不应推进 installed.json，got %s", state.ManifestID)
	}
}

// TestSync_Auto_Moved_RemoveDeletedTrue_DeletesSource 验证 RemoveDeleted=true
// 时，成功提交的 Moved 配对的旧路径应被删除，新路径持有内容。
func TestSync_Auto_Moved_RemoveDeletedTrue_DeletesSource(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	data := []byte("moved-file-content-old-path-should-be-deleted!!")
	chunk := makeSyncChunk(t, data, 0, 0)
	writeFile(t, dir, "old-name.bin", data)

	oldSpecs := []oldFileSpec{{path: "old-name.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, 0x5151515151515151, oldSpecs)

	newFiles := []rman.FileEntry{makeSyncFileEntry("new-name.bin", chunk)}
	newManifest := &rman.Manifest{ManifestID: 0x6161616161616161, Files: newFiles}

	opts := managedAutoOptions(t, dir, oldManifestPath, 0x5151515151515151, oldSpecs, true)
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Moved) != 1 || stats.Moved[0] != "new-name.bin" {
		t.Fatalf("Moved 期望 [new-name.bin]，got %v", stats.Moved)
	}
	if len(stats.Removed) != 0 {
		t.Errorf("Moved 源清理不应计入 Stats.Removed，got %v", stats.Removed)
	}

	gotNew, err := os.ReadFile(filepath.Join(dir, "new-name.bin"))
	if err != nil {
		t.Fatalf("读取新路径失败: %v", err)
	}
	if !bytes.Equal(gotNew, data) {
		t.Errorf("新路径内容不匹配: got %q want %q", gotNew, data)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "old-name.bin")); !os.IsNotExist(statErr) {
		t.Errorf("RemoveDeleted=true 时旧路径应被删除, stat err=%v", statErr)
	}
}

// TestSync_Auto_Moved_RemoveDeletedFalse_KeepsSource 验证 RemoveDeleted=false
// 时，成功提交的 Moved 配对的旧路径应原样保留。
func TestSync_Auto_Moved_RemoveDeletedFalse_KeepsSource(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	data := []byte("moved-file-content-old-path-should-be-kept!!!!!")
	chunk := makeSyncChunk(t, data, 0, 0)
	writeFile(t, dir, "old-name.bin", data)

	oldSpecs := []oldFileSpec{{path: "old-name.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, 0x7171717171717171, oldSpecs)

	newFiles := []rman.FileEntry{makeSyncFileEntry("new-name.bin", chunk)}
	newManifest := &rman.Manifest{ManifestID: 0x8181818181818181, Files: newFiles}

	opts := managedAutoOptions(t, dir, oldManifestPath, 0x7171717171717171, oldSpecs, false)
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Moved) != 1 || stats.Moved[0] != "new-name.bin" {
		t.Fatalf("Moved 期望 [new-name.bin]，got %v", stats.Moved)
	}

	gotOld, err := os.ReadFile(filepath.Join(dir, "old-name.bin"))
	if err != nil {
		t.Fatalf("RemoveDeleted=false 时旧路径应保留: %v", err)
	}
	if !bytes.Equal(gotOld, data) {
		t.Errorf("旧路径内容被意外改动: got %q want %q", gotOld, data)
	}
}

// TestSync_Auto_MissesAcrossTwoBundlesOneFails_WholeFileFailedStagingDiscarded 验证
// 单个文件的 Miss 分别落在两个不同 Bundle，其中一个 Bundle 下载失败、
// 另一个成功——DownloadTasks 按文件聚合失败（一个 Job 失败即整份文件不可信），
// 因此整个文件都应计入 Stats.Failed，其 staging 已丢弃，旧内容保持不变。
func TestSync_Auto_MissesAcrossTwoBundlesOneFails_WholeFileFailedStagingDiscarded(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	oldData := []byte("old-content-differs-from-both-new-chunks-must-survive")
	oldChunk := makeSyncChunk(t, oldData, 0, 0)
	writeFile(t, dir, "file.bin", oldData)

	const bundleFail uint64 = 0x9100000000000001
	const bundleOK uint64 = 0x9100000000000002
	newC0 := makeSyncChunk(t, []byte("chunk-zero-lives-in-the-bundle-that-fails!!!!!!"), bundleFail, 0)
	newC1 := makeSyncChunk(t, []byte("chunk-one-lives-in-the-bundle-that-succeeds!!!!"), bundleOK, 0)
	mock.failBundles[core.BundleFilename(bundleFail)] = true
	mock.addChunk(newC1.info.ChunkID, bundleOK, newC1.info.BundleOffset, newC1.compressed)

	oldSpecs := []oldFileSpec{{path: "file.bin", chunkIDs: []uint64{oldChunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, 0xA1A1A1A1A1A1A1A1, oldSpecs)

	newFiles := []rman.FileEntry{makeSyncFileEntry("file.bin", newC0, newC1)}
	newManifest := &rman.Manifest{ManifestID: 0xB1B1B1B1B1B1B1B1, Files: newFiles}

	opts := Options{
		Operation:       OperationInstall,
		Mode:            ModeAuto,
		OldManifestPath: oldManifestPath,
		RemoveDeleted:   true,
	}
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, newFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 不应返回顶层 error（部分失败走 Stats.Failed）: %v", err)
	}

	if len(stats.Failed) != 1 || stats.Failed[0] != "file.bin" {
		t.Fatalf("Failed 期望 [file.bin]，got %v", stats.Failed)
	}
	if len(stats.Patched) != 0 || len(stats.Created) != 0 {
		t.Errorf("file.bin 不应出现在 Patched/Created，got Patched=%v Created=%v", stats.Patched, stats.Created)
	}

	gotOld, err := os.ReadFile(filepath.Join(dir, "file.bin"))
	if err != nil {
		t.Fatalf("读取 file.bin 失败: %v", err)
	}
	if !bytes.Equal(gotOld, oldData) {
		t.Errorf("旧内容应保持不变: got %q want %q", gotOld, oldData)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "file.bin.rman-tmp")); !os.IsNotExist(statErr) {
		t.Errorf("staging 应已被丢弃, stat err=%v", statErr)
	}

	archive := NewArchive(dir)
	if _, ok := archive.InstalledManifestPath(); ok {
		t.Error("存在 Failed 文件时不应推进 installed.json")
	}
}

// ---- 过滤运行：差分必须基于完整新旧清单，不能拿过滤子集当"新清单" ----
//
// files 是 CLI -p/-f 过滤后的处理子集。若用它充当新清单一侧参与 diff，旧清单里
// 过滤范围外的路径将天然找不到匹配，会被判为 Removed，或被误配对成某个过滤内
// Added 条目的 Moved 源；RemoveDeleted=true 时二者都会删掉完整新清单仍然声明的
// 文件。因此差分固定用完整新旧清单计算，再按 files 收窄处理范围。

// TestSync_Auto_FilteredRun_RemovedFromCompleteDiff_KeepsOutOfFilterFiles 验证
// Removed 判定必须来自完整新旧清单的 diff：过滤范围外但完整新清单仍然声明的
// ahri.bin 不应被删除；真正被完整新清单移除的 legacy-old.bin 仍应被删除；
// 过滤范围内的 aatrox.bin 正常按 chunk 级补洞更新。
func TestSync_Auto_FilteredRun_RemovedFromCompleteDiff_KeepsOutOfFilterFiles(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	aatroxOldData := []byte("aatrox-old-content-should-be-patched-by-filtered-run!")
	aatroxOldChunk := makeSyncChunk(t, aatroxOldData, 0, 0)
	const bundleID uint64 = 0xC100000000000001
	aatroxNewData := []byte("aatrox-new-content-after-filtered-run-completes!!!!!!")
	aatroxNewChunk := makeSyncChunk(t, aatroxNewData, bundleID, 0)
	mock.addChunk(aatroxNewChunk.info.ChunkID, bundleID, aatroxNewChunk.info.BundleOffset, aatroxNewChunk.compressed)

	ahriData := []byte("ahri-unchanged-out-of-filter-content-must-not-be-touched")
	ahriChunk := makeSyncChunk(t, ahriData, 0, 0)

	legacyData := []byte("legacy-content-genuinely-removed-from-new-manifest!!!")
	legacyChunk := makeSyncChunk(t, legacyData, 0, 0)

	writeFile(t, dir, "aatrox.bin", aatroxOldData)
	writeFile(t, dir, "ahri.bin", ahriData)
	writeFile(t, dir, "legacy-old.bin", legacyData)

	oldSpecs := []oldFileSpec{
		{path: "aatrox.bin", chunkIDs: []uint64{aatroxOldChunk.info.ChunkID}},
		{path: "ahri.bin", chunkIDs: []uint64{ahriChunk.info.ChunkID}},
		{path: "legacy-old.bin", chunkIDs: []uint64{legacyChunk.info.ChunkID}},
	}
	oldManifestPath := writeOldManifestFile(t, dir, 0xC1C1C1C1C1C1C1C1, oldSpecs)

	// 完整新清单：legacy-old.bin 已不再声明（真正的 Removed），ahri.bin 原样保留。
	allNewFiles := []rman.FileEntry{
		makeSyncFileEntry("aatrox.bin", aatroxNewChunk),
		makeSyncFileEntry("ahri.bin", ahriChunk),
	}
	newManifest := &rman.Manifest{ManifestID: 0xC2C2C2C2C2C2C2C2, Files: allNewFiles}

	// 模拟 CLI "-p aatrox" 过滤后只剩 aatrox.bin 需要处理。
	filteredFiles := []rman.FileEntry{allNewFiles[0]}

	opts := managedAutoOptions(t, dir, oldManifestPath, 0xC1C1C1C1C1C1C1C1, oldSpecs, true)
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, filteredFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Failed) != 0 {
		t.Fatalf("Failed 应为空: %v", stats.Failed)
	}

	gotAhri, err := os.ReadFile(filepath.Join(dir, "ahri.bin"))
	if err != nil {
		t.Fatalf("过滤外但完整新清单仍声明的 ahri.bin 被误删: %v", err)
	}
	if !bytes.Equal(gotAhri, ahriData) {
		t.Errorf("ahri.bin 内容被意外改动: got %q want %q", gotAhri, ahriData)
	}

	if _, statErr := os.Stat(filepath.Join(dir, "legacy-old.bin")); !os.IsNotExist(statErr) {
		t.Errorf("legacy-old.bin 应被删除（完整新清单已不声明）, stat err=%v", statErr)
	}

	gotAatrox, err := os.ReadFile(filepath.Join(dir, "aatrox.bin"))
	if err != nil {
		t.Fatalf("读取 aatrox.bin 失败: %v", err)
	}
	if !bytes.Equal(gotAatrox, aatroxNewData) {
		t.Errorf("aatrox.bin 未按新清单更新: got %q want %q", gotAatrox, aatroxNewData)
	}

	if len(stats.Removed) != 1 || stats.Removed[0] != "legacy-old.bin" {
		t.Errorf("Stats.Removed 期望仅 [legacy-old.bin]，got %v", stats.Removed)
	}
}

// TestSync_Auto_FilteredRun_MovedMispairingGuard_KeepsDeclaredOldPath 验证 Moved
// 配对必须基于完整新旧清单：shared-data.bin 在完整新清单里原路径原样保留，
// 不应该因为过滤子集里恰好只有 new-champ.bin（与它 ChunkID 序列相同）而被误判
// 为 new-champ.bin 的 Moved 源——否则 RemoveDeleted=true 时 shared-data.bin 会被
// 当作"迁移后的重复副本"删除，而它其实是完整新清单仍需要的正式内容。
func TestSync_Auto_FilteredRun_MovedMispairingGuard_KeepsDeclaredOldPath(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xC300000000000001
	sharedData := []byte("shared-content-declared-at-same-path-in-both-manifests!")
	sharedChunk := makeSyncChunk(t, sharedData, bundleID, 0)
	mock.addChunk(sharedChunk.info.ChunkID, bundleID, sharedChunk.info.BundleOffset, sharedChunk.compressed)

	writeFile(t, dir, "shared-data.bin", sharedData)
	// 故意不预先写 new-champ.bin：它是过滤范围内的新增文件，内容靠校验命中或
	// 下载补齐都可以，测试不关心具体走哪条路径。

	oldSpecs := []oldFileSpec{
		{path: "shared-data.bin", chunkIDs: []uint64{sharedChunk.info.ChunkID}},
	}
	oldManifestPath := writeOldManifestFile(t, dir, 0xC4C4C4C4C4C4C4C4, oldSpecs)

	allNewFiles := []rman.FileEntry{
		makeSyncFileEntry("shared-data.bin", sharedChunk), // 完整新清单在原路径原样保留，未变化
		makeSyncFileEntry("new-champ.bin", sharedChunk),   // 过滤内新增，ChunkID 序列与 shared-data.bin 相同
	}
	newManifest := &rman.Manifest{ManifestID: 0xC5C5C5C5C5C5C5C5, Files: allNewFiles}

	// 模拟 CLI 过滤后只剩 new-champ.bin 需要处理。
	filteredFiles := []rman.FileEntry{allNewFiles[1]}

	opts := managedAutoOptions(t, dir, oldManifestPath, 0xC4C4C4C4C4C4C4C4, oldSpecs, true)
	stats, err := Sync(context.Background(), newManifest, []byte("raw"), "src", dir, filteredFiles, d, opts)
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Failed) != 0 {
		t.Fatalf("Failed 应为空: %v", stats.Failed)
	}

	gotShared, err := os.ReadFile(filepath.Join(dir, "shared-data.bin"))
	if err != nil {
		t.Fatalf("shared-data.bin 被误删（过滤外旧路径被误配对成 Moved 源）: %v", err)
	}
	if !bytes.Equal(gotShared, sharedData) {
		t.Errorf("shared-data.bin 内容被意外改动: got %q want %q", gotShared, sharedData)
	}

	gotNewChamp, err := os.ReadFile(filepath.Join(dir, "new-champ.bin"))
	if err != nil {
		t.Fatalf("读取 new-champ.bin 失败: %v", err)
	}
	if !bytes.Equal(gotNewChamp, sharedData) {
		t.Errorf("new-champ.bin 内容不匹配: got %q want %q", gotNewChamp, sharedData)
	}
}

func TestSync_StandaloneRepeatedSelectionsRemainIndependent(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xD100000000000001
	firstChunk := makeSyncChunk(t, []byte("standalone-first-file"), bundleID, 0)
	secondChunk := makeSyncChunk(t, []byte("standalone-second-file"), bundleID, firstChunk.info.CompressedSize)
	mock.addChunk(firstChunk.info.ChunkID, bundleID, firstChunk.info.BundleOffset, firstChunk.compressed)
	mock.addChunk(secondChunk.info.ChunkID, bundleID, secondChunk.info.BundleOffset, secondChunk.compressed)

	allFiles := []rman.FileEntry{
		makeSyncFileEntry("description.json", firstChunk),
		makeSyncFileEntry("game.wad.client", secondChunk),
	}
	manifest := &rman.Manifest{ManifestID: 0xD1D1D1D1D1D1D1D1, Files: allFiles}

	for _, target := range allFiles {
		stats, err := Sync(context.Background(), manifest, []byte("unused"), "src", dir,
			[]rman.FileEntry{target}, d, Options{Mode: ModeAuto})
		if err != nil {
			t.Fatalf("standalone Sync(%s) 失败: %v", target.Path, err)
		}
		if len(stats.Failed) != 0 {
			t.Fatalf("standalone Sync(%s) Failed=%v", target.Path, stats.Failed)
		}
	}

	for _, target := range allFiles {
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(target.Path))); err != nil {
			t.Errorf("standalone 目标 %s 未落盘: %v", target.Path, err)
		}
	}
	if exists, err := NewArchive(dir).HasInstalledState(); err != nil || exists {
		t.Errorf("standalone 不应创建 installed.json: exists=%v err=%v", exists, err)
	}
}

func TestSync_ManagedPartialInstallAccumulatesAndRepairsMissingMember(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xD200000000000001
	firstChunk := makeSyncChunk(t, []byte("managed-description-file"), bundleID, 0)
	secondChunk := makeSyncChunk(t, []byte("managed-game-wad-file"), bundleID, firstChunk.info.CompressedSize)
	mock.addChunk(firstChunk.info.ChunkID, bundleID, firstChunk.info.BundleOffset, firstChunk.compressed)
	mock.addChunk(secondChunk.info.ChunkID, bundleID, secondChunk.info.BundleOffset, secondChunk.compressed)

	allFiles := []rman.FileEntry{
		makeSyncFileEntry("description.json", firstChunk),
		makeSyncFileEntry("game.wad.client", secondChunk),
	}
	specs := []oldFileSpec{
		{path: allFiles[0].Path, chunkIDs: []uint64{firstChunk.info.ChunkID}},
		{path: allFiles[1].Path, chunkIDs: []uint64{secondChunk.info.ChunkID}},
	}
	manifestID := uint64(0xD2D2D2D2D2D2D2D2)
	manifestPath := writeOldManifestFile(t, dir, manifestID, specs)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("读取测试 manifest 失败: %v", err)
	}
	manifest := &rman.Manifest{ManifestID: manifestID, Files: allFiles}
	opts := Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true}

	if _, err := Sync(context.Background(), manifest, raw, "src", dir,
		[]rman.FileEntry{allFiles[0]}, d, opts); err != nil {
		t.Fatalf("第一次部分安装失败: %v", err)
	}
	state, err := NewArchive(dir).LoadInstalled()
	if err != nil || state == nil {
		t.Fatalf("读取第一次安装状态失败: state=%+v err=%v", state, err)
	}
	if len(state.Files) != 1 || state.Files[0] != "description.json" {
		t.Fatalf("第一次 Files=%v, want [description.json]", state.Files)
	}

	if _, err := Sync(context.Background(), manifest, raw, "src", dir,
		[]rman.FileEntry{allFiles[1]}, d, opts); err != nil {
		t.Fatalf("第二次部分安装失败: %v", err)
	}
	state, err = NewArchive(dir).LoadInstalled()
	if err != nil || state == nil {
		t.Fatalf("读取第二次安装状态失败: state=%+v err=%v", state, err)
	}
	wantFiles := []string{"description.json", "game.wad.client"}
	if !slices.Equal(state.Files, wantFiles) {
		t.Fatalf("累积 Files=%v, want %v", state.Files, wantFiles)
	}

	if err := os.Remove(filepath.Join(dir, "game.wad.client")); err != nil {
		t.Fatalf("删除受管理目标失败: %v", err)
	}
	beforeCalls := mock.callCount
	stats, err := Sync(context.Background(), manifest, raw, "src", dir,
		[]rman.FileEntry{allFiles[1]}, d, opts)
	if err != nil {
		t.Fatalf("修复缺失成员失败: %v", err)
	}
	if mock.callCount <= beforeCalls {
		t.Error("state-listed 但磁盘缺失的文件必须重新下载")
	}
	if len(stats.Created) != 1 || stats.Created[0] != "game.wad.client" {
		t.Errorf("缺失成员应重新创建，stats=%+v", stats)
	}
}

func TestSync_ManagedCleanupOnlyDeletesOwnedPaths(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	managedData := []byte("owned-obsolete-file")
	unmanagedData := []byte("unmanaged-obsolete-file")
	managedChunk := makeSyncChunk(t, managedData, 0, 0)
	unmanagedChunk := makeSyncChunk(t, unmanagedData, 0, 0)
	writeFile(t, dir, "managed-old.bin", managedData)
	writeFile(t, dir, "unmanaged-old.bin", unmanagedData)

	oldID := uint64(0xD3D3D3D3D3D3D3D3)
	oldSpecs := []oldFileSpec{
		{path: "managed-old.bin", chunkIDs: []uint64{managedChunk.info.ChunkID}},
		{path: "unmanaged-old.bin", chunkIDs: []uint64{unmanagedChunk.info.ChunkID}},
	}
	oldManifestPath := writeOldManifestFile(t, dir, oldID, oldSpecs)
	raw, err := os.ReadFile(oldManifestPath)
	if err != nil {
		t.Fatalf("读取旧清单失败: %v", err)
	}
	if err := NewArchive(dir).Save(oldID, raw, "src", []string{"managed-old.bin"}); err != nil {
		t.Fatalf("保存旧状态失败: %v", err)
	}

	newManifest := &rman.Manifest{ManifestID: 0xD4D4D4D4D4D4D4D4}
	opts := Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true}
	stats, err := Sync(context.Background(), newManifest, []byte("new"), "src", dir, nil, d, opts)
	if err != nil {
		t.Fatalf("受管理清理失败: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "managed-old.bin")); !os.IsNotExist(err) {
		t.Errorf("已管理 obsolete 路径应删除: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "unmanaged-old.bin")); err != nil {
		t.Errorf("未管理 obsolete 路径不得删除: %v", err)
	}
	if !slices.Equal(stats.Removed, []string{"managed-old.bin"}) {
		t.Errorf("Removed=%v, want [managed-old.bin]", stats.Removed)
	}
}

func TestSync_LegacySchema1UsesManifestHintWithoutSkipAuthority(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xD500000000000001
	goodData := []byte("legacy-state-must-reverify-this-file")
	chunk := makeSyncChunk(t, goodData, bundleID, 0)
	mock.addChunk(chunk.info.ChunkID, bundleID, chunk.info.BundleOffset, chunk.compressed)

	corrupted := append([]byte(nil), goodData...)
	corrupted[0] ^= 0xFF
	writeFile(t, dir, "file.bin", corrupted)

	manifestID := uint64(0xD5D5D5D5D5D5D5D5)
	specs := []oldFileSpec{{path: "file.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	manifestPath := writeOldManifestFile(t, dir, manifestID, specs)
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("读取 manifest 失败: %v", err)
	}
	archive := NewArchive(dir)
	if err := archive.Save(manifestID, raw, "src", []string{"file.bin"}); err != nil {
		t.Fatalf("建立测试 archive 失败: %v", err)
	}

	legacyPayload := fmt.Sprintf(`{
  "schema": 1,
  "manifest_id": "%016X",
  "manifest_file": "manifests/%016X.manifest",
  "source": "src",
  "updated_at": "2026-01-01T00:00:00Z"
}`, manifestID, manifestID)
	if err := os.WriteFile(filepath.Join(dir, ".rman", "installed.json"), []byte(legacyPayload), 0644); err != nil {
		t.Fatalf("写入 legacy state 失败: %v", err)
	}

	entry := makeSyncFileEntry("file.bin", chunk)
	manifest := &rman.Manifest{ManifestID: manifestID, Files: []rman.FileEntry{entry}}
	stats, err := Sync(context.Background(), manifest, raw, "src", dir,
		[]rman.FileEntry{entry}, d,
		Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true})
	if err != nil {
		t.Fatalf("legacy 升级 Sync 失败: %v", err)
	}
	if len(stats.Patched) != 1 || stats.Patched[0] != "file.bin" {
		t.Fatalf("schema 1 不得授权 skip，stats=%+v", stats)
	}

	state, err := archive.LoadInstalled()
	if err != nil || state == nil {
		t.Fatalf("读取升级后状态失败: state=%+v err=%v", state, err)
	}
	if state.Schema != 2 || !slices.Equal(state.Files, []string{"file.bin"}) {
		t.Errorf("升级后 state=%+v, want schema2 Files=[file.bin]", state)
	}
}

func TestSync_UnmanagedMoveSourceIsDownloadedAndPreserved(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xD600000000000001
	data := []byte("unmanaged-move-source-content")
	chunk := makeSyncChunk(t, data, bundleID, 0)
	mock.addChunk(chunk.info.ChunkID, bundleID, chunk.info.BundleOffset, chunk.compressed)
	writeFile(t, dir, "old-name.bin", data)

	oldID := uint64(0xD6D6D6D6D6D6D6D6)
	oldSpecs := []oldFileSpec{{path: "old-name.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, oldID, oldSpecs)
	raw, err := os.ReadFile(oldManifestPath)
	if err != nil {
		t.Fatalf("读取旧清单失败: %v", err)
	}
	if err := NewArchive(dir).Save(oldID, raw, "src", nil); err != nil {
		t.Fatalf("保存空覆盖旧状态失败: %v", err)
	}

	entry := makeSyncFileEntry("new-name.bin", chunk)
	newManifest := &rman.Manifest{ManifestID: 0xD7D7D7D7D7D7D7D7, Files: []rman.FileEntry{entry}}
	stats, err := Sync(context.Background(), newManifest, []byte("new"), "src", dir,
		[]rman.FileEntry{entry}, d,
		Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true})
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Moved) != 0 || len(stats.Created) != 1 {
		t.Fatalf("未管理 MOVE 源必须退化为下载，stats=%+v", stats)
	}
	if mock.callCount == 0 {
		t.Error("未管理 MOVE 源不得零网络复用")
	}
	if _, err := os.Stat(filepath.Join(dir, "old-name.bin")); err != nil {
		t.Errorf("未管理 MOVE 源不得清理: %v", err)
	}
}

func TestSync_CleanupErrorKeepsPreviousState(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	oldPath := filepath.Join(dir, "old.bin")
	if err := os.Mkdir(oldPath, 0755); err != nil {
		t.Fatalf("建立非普通旧路径失败: %v", err)
	}

	oldID := uint64(0xD8D8D8D8D8D8D8D8)
	oldSpecs := []oldFileSpec{{path: "old.bin"}}
	oldManifestPath := writeOldManifestFile(t, dir, oldID, oldSpecs)
	raw, err := os.ReadFile(oldManifestPath)
	if err != nil {
		t.Fatalf("读取旧清单失败: %v", err)
	}
	archive := NewArchive(dir)
	if err := archive.Save(oldID, raw, "src", []string{"old.bin"}); err != nil {
		t.Fatalf("保存旧状态失败: %v", err)
	}

	newManifest := &rman.Manifest{ManifestID: 0xD9D9D9D9D9D9D9D9}
	_, err = Sync(context.Background(), newManifest, []byte("new"), "src", dir, nil, d,
		Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true})
	if err == nil {
		t.Fatal("非普通受管理清理目标应使状态转换失败")
	}

	state, loadErr := archive.LoadInstalled()
	if loadErr != nil || state == nil {
		t.Fatalf("读取旧状态失败: state=%+v err=%v", state, loadErr)
	}
	if state.ManifestID != fmt.Sprintf("%016X", oldID) {
		t.Errorf("清理失败后状态被推进: %+v", state)
	}
}

func TestSync_AutoMoveGateFailurePatchesThenCleansOwnedSource(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xDA00000000000001
	data := []byte("correct-moved-content")
	chunk := makeSyncChunk(t, data, bundleID, 0)
	mock.addChunk(chunk.info.ChunkID, bundleID, chunk.info.BundleOffset, chunk.compressed)
	writeFile(t, dir, "old-name.bin", []byte("wrong-size"))

	oldID := uint64(0xDADADADADADADADA)
	oldSpecs := []oldFileSpec{{path: "old-name.bin", chunkIDs: []uint64{chunk.info.ChunkID}}}
	oldManifestPath := writeOldManifestFile(t, dir, oldID, oldSpecs)
	raw, err := os.ReadFile(oldManifestPath)
	if err != nil {
		t.Fatalf("读取旧清单失败: %v", err)
	}
	if err := NewArchive(dir).Save(oldID, raw, "src", []string{"old-name.bin"}); err != nil {
		t.Fatalf("保存旧状态失败: %v", err)
	}

	entry := makeSyncFileEntry("new-name.bin", chunk)
	stats, err := Sync(context.Background(),
		&rman.Manifest{ManifestID: 0xDBDBDBDBDBDBDBDB, Files: []rman.FileEntry{entry}},
		[]byte("new"), "src", dir, []rman.FileEntry{entry}, d,
		Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true})
	if err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}
	if len(stats.Moved) != 0 || len(stats.Created) != 1 {
		t.Fatalf("MOVE size gate 失败后应 PATCH/CREATE，stats=%+v", stats)
	}
	if _, err := os.Stat(filepath.Join(dir, "old-name.bin")); !os.IsNotExist(err) {
		t.Errorf("PATCH 成功后应清理受管理 Moved 旧源: %v", err)
	}
}

func TestSync_ManagedNonAutoStrategiesKeepCleanupAuthority(t *testing.T) {
	for _, mode := range []Mode{ModeRepair, ModeForceFull} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			mock := newMockFetcher()
			d, dir := newSyncTestDownloader(t, mock)

			removedData := []byte("owned-removed-content")
			removedChunk := makeSyncChunk(t, removedData, 0, 0)
			movedData := []byte("owned-moved-content")
			const bundleID uint64 = 0xDC00000000000001
			movedChunk := makeSyncChunk(t, movedData, bundleID, 0)
			mock.addChunk(movedChunk.info.ChunkID, bundleID, movedChunk.info.BundleOffset, movedChunk.compressed)

			writeFile(t, dir, "removed.bin", removedData)
			writeFile(t, dir, "old-name.bin", movedData)

			oldID := uint64(0xDCDCDCDCDCDCDCDC)
			oldSpecs := []oldFileSpec{
				{path: "removed.bin", chunkIDs: []uint64{removedChunk.info.ChunkID}},
				{path: "old-name.bin", chunkIDs: []uint64{movedChunk.info.ChunkID}},
			}
			oldManifestPath := writeOldManifestFile(t, dir, oldID, oldSpecs)
			raw, err := os.ReadFile(oldManifestPath)
			if err != nil {
				t.Fatalf("读取旧清单失败: %v", err)
			}
			if err := NewArchive(dir).Save(oldID, raw, "src",
				[]string{"removed.bin", "old-name.bin"}); err != nil {
				t.Fatalf("保存旧状态失败: %v", err)
			}

			entry := makeSyncFileEntry("new-name.bin", movedChunk)
			stats, err := Sync(context.Background(),
				&rman.Manifest{ManifestID: 0xDDDDDDDDDDDDDDDD, Files: []rman.FileEntry{entry}},
				[]byte("new"), "src", dir, []rman.FileEntry{entry}, d,
				Options{Operation: OperationInstall, Mode: mode, RemoveDeleted: true})
			if err != nil {
				t.Fatalf("Sync 失败: %v", err)
			}
			if len(stats.Moved) != 0 || len(stats.Created) != 1 {
				t.Fatalf("非 AUTO 不应走 MOVE 复用，stats=%+v", stats)
			}
			if !slices.Equal(stats.Removed, []string{"removed.bin"}) {
				t.Errorf("真实 Removed 未清理: %v", stats.Removed)
			}
			for _, oldPath := range []string{"removed.bin", "old-name.bin"} {
				if _, err := os.Stat(filepath.Join(dir, oldPath)); !os.IsNotExist(err) {
					t.Errorf("%s 应在成功状态转换后清理: %v", oldPath, err)
				}
			}
		})
	}
}

func TestSync_MatchingManagedStatePrecedesStaleExplicitHint(t *testing.T) {
	mock := newMockFetcher()
	d, dir := newSyncTestDownloader(t, mock)

	const bundleID uint64 = 0xDE00000000000001
	firstChunk := makeSyncChunk(t, []byte("first-managed-file"), bundleID, 0)
	secondChunk := makeSyncChunk(t, []byte("second-managed-file"), bundleID, firstChunk.info.CompressedSize)
	mock.addChunk(firstChunk.info.ChunkID, bundleID, firstChunk.info.BundleOffset, firstChunk.compressed)
	mock.addChunk(secondChunk.info.ChunkID, bundleID, secondChunk.info.BundleOffset, secondChunk.compressed)

	allFiles := []rman.FileEntry{
		makeSyncFileEntry("first.bin", firstChunk),
		makeSyncFileEntry("second.bin", secondChunk),
	}
	manifestID := uint64(0xDEDEDEDEDEDEDEDE)
	currentPath := writeOldManifestFile(t, dir, manifestID, []oldFileSpec{
		{path: "first.bin", chunkIDs: []uint64{firstChunk.info.ChunkID}},
		{path: "second.bin", chunkIDs: []uint64{secondChunk.info.ChunkID}},
	})
	currentRaw, err := os.ReadFile(currentPath)
	if err != nil {
		t.Fatalf("读取当前清单失败: %v", err)
	}
	manifest := &rman.Manifest{ManifestID: manifestID, Files: allFiles}
	baseOpts := Options{Operation: OperationInstall, Mode: ModeAuto, RemoveDeleted: true}
	if _, err := Sync(context.Background(), manifest, currentRaw, "src", dir,
		[]rman.FileEntry{allFiles[0]}, d, baseOpts); err != nil {
		t.Fatalf("首次安装失败: %v", err)
	}

	stalePath := writeOldManifestFile(t, dir, 0xDFDFDFDFDFDFDFDF, nil)
	secondOpts := baseOpts
	secondOpts.OldManifestPath = stalePath
	if _, err := Sync(context.Background(), manifest, currentRaw, "src", dir,
		[]rman.FileEntry{allFiles[1]}, d, secondOpts); err != nil {
		t.Fatalf("带 stale hint 的第二次安装失败: %v", err)
	}

	state, err := NewArchive(dir).LoadInstalled()
	if err != nil || state == nil {
		t.Fatalf("读取最终状态失败: state=%+v err=%v", state, err)
	}
	if want := []string{"first.bin", "second.bin"}; !slices.Equal(state.Files, want) {
		t.Errorf("stale explicit hint 不应覆盖 managed state: Files=%v want=%v", state.Files, want)
	}
}
