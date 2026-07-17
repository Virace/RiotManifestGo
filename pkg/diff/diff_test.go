package diff

import (
	"testing"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// ---- 测试数据辅助 ----

// chunk 构造一个只带 ChunkID 的 ChunkInfo（Diff 只关心 ChunkID，其余字段测试不需要）。
func chunk(id uint64) rman.ChunkInfo {
	return rman.ChunkInfo{ChunkID: id}
}

// entry 构造一个指定路径与 ChunkID 序列的 FileEntry。
func entry(path string, chunkIDs ...uint64) rman.FileEntry {
	chunks := make([]rman.ChunkInfo, len(chunkIDs))
	for i, id := range chunkIDs {
		chunks[i] = chunk(id)
	}
	return rman.FileEntry{Path: path, Chunks: chunks}
}

// paths 提取 FileEntry 切片中的 Path 列表，便于断言。
func paths(entries []rman.FileEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Path
	}
	return out
}

// ---- 测试用例 ----

// TestDiff_Unchanged 验证 Path 与 ChunkID 序列均相同的文件归入 Unchanged。
func TestDiff_Unchanged(t *testing.T) {
	old := []rman.FileEntry{entry("a.bin", 1, 2, 3)}
	newFiles := []rman.FileEntry{entry("a.bin", 1, 2, 3)}

	got := Diff(old, newFiles)

	if len(got.Unchanged) != 1 || got.Unchanged[0].Path != "a.bin" {
		t.Fatalf("Unchanged = %v, 期望包含 a.bin", paths(got.Unchanged))
	}
	if len(got.Changed) != 0 || len(got.Added) != 0 || len(got.Removed) != 0 || len(got.Moved) != 0 {
		t.Errorf("其余分类应为空，got Changed=%d Added=%d Removed=%d Moved=%d",
			len(got.Changed), len(got.Added), len(got.Removed), len(got.Moved))
	}
}

// TestDiff_Changed 验证 Path 相同但 ChunkID 序列不同的文件归入 Changed。
func TestDiff_Changed(t *testing.T) {
	old := []rman.FileEntry{entry("a.bin", 1, 2, 3)}
	newFiles := []rman.FileEntry{entry("a.bin", 1, 2, 9)}

	got := Diff(old, newFiles)

	if len(got.Changed) != 1 || got.Changed[0].Path != "a.bin" {
		t.Fatalf("Changed = %v, 期望包含 a.bin", paths(got.Changed))
	}
	if len(got.Unchanged) != 0 || len(got.Added) != 0 || len(got.Removed) != 0 || len(got.Moved) != 0 {
		t.Errorf("其余分类应为空，got Unchanged=%d Added=%d Removed=%d Moved=%d",
			len(got.Unchanged), len(got.Added), len(got.Removed), len(got.Moved))
	}
}

// TestDiff_AddedWithEmptyOld 验证空旧清单时，新清单全部文件归入 Added。
func TestDiff_AddedWithEmptyOld(t *testing.T) {
	newFiles := []rman.FileEntry{entry("a.bin", 1), entry("b.bin", 2)}

	got := Diff(nil, newFiles)

	if len(got.Added) != 2 {
		t.Fatalf("Added = %v, 期望 2 个", paths(got.Added))
	}
	if got.Added[0].Path != "a.bin" || got.Added[1].Path != "b.bin" {
		t.Errorf("Added 顺序应与 new 一致，got %v", paths(got.Added))
	}
	if len(got.Unchanged) != 0 || len(got.Changed) != 0 || len(got.Removed) != 0 || len(got.Moved) != 0 {
		t.Errorf("其余分类应为空，got Unchanged=%d Changed=%d Removed=%d Moved=%d",
			len(got.Unchanged), len(got.Changed), len(got.Removed), len(got.Moved))
	}
}

// TestDiff_Removed 验证新清单中缺失、且未被 Moved 配对的旧路径归入 Removed。
func TestDiff_Removed(t *testing.T) {
	old := []rman.FileEntry{entry("a.bin", 1)}

	got := Diff(old, nil)

	if len(got.Removed) != 1 || got.Removed[0] != "a.bin" {
		t.Fatalf("Removed = %v, 期望 [a.bin]", got.Removed)
	}
	if len(got.Unchanged) != 0 || len(got.Changed) != 0 || len(got.Added) != 0 || len(got.Moved) != 0 {
		t.Errorf("其余分类应为空，got Unchanged=%d Changed=%d Added=%d Moved=%d",
			len(got.Unchanged), len(got.Changed), len(got.Added), len(got.Moved))
	}
}

// TestDiff_Moved 验证 ChunkID 序列一致但 Path 变化的文件被一对一配对为 Moved。
func TestDiff_Moved(t *testing.T) {
	old := []rman.FileEntry{entry("old/a.bin", 1, 2, 3)}
	newFiles := []rman.FileEntry{entry("new/a.bin", 1, 2, 3)}

	got := Diff(old, newFiles)

	if len(got.Moved) != 1 {
		t.Fatalf("Moved 数量 = %d, 期望 1", len(got.Moved))
	}
	if got.Moved[0].From != "old/a.bin" || got.Moved[0].Entry.Path != "new/a.bin" {
		t.Errorf("Moved[0] = %+v, 期望 From=old/a.bin Entry.Path=new/a.bin", got.Moved[0])
	}
	if len(got.Unchanged) != 0 || len(got.Changed) != 0 || len(got.Added) != 0 || len(got.Removed) != 0 {
		t.Errorf("其余分类应为空，got Unchanged=%d Changed=%d Added=%d Removed=%d",
			len(got.Unchanged), len(got.Changed), len(got.Added), len(got.Removed))
	}
}

// TestDiff_LinkFieldIgnored 验证 Link 字段按普通条目对待，不参与判同逻辑
// （即便 Link 不同，只要 Path 与 ChunkID 序列相同就仍是 Unchanged）。
func TestDiff_LinkFieldIgnored(t *testing.T) {
	old := []rman.FileEntry{{Path: "a.bin", Link: "linkA", Chunks: []rman.ChunkInfo{chunk(1), chunk(2)}}}
	newFiles := []rman.FileEntry{{Path: "a.bin", Link: "linkB", Chunks: []rman.ChunkInfo{chunk(1), chunk(2)}}}

	got := Diff(old, newFiles)

	if len(got.Unchanged) != 1 {
		t.Fatalf("Unchanged 数量 = %d, 期望 1（Link 差异不应影响判同）", len(got.Unchanged))
	}
	if len(got.Changed) != 0 {
		t.Errorf("Changed 数量 = %d, 期望 0", len(got.Changed))
	}
}

// TestDiff_FingerprintCollisionNotPaired 验证指纹碰撞但 ChunkID 序列不同的两个文件
// 不会被误配对为 Moved。
//
// a、b 是通过暴力搜索找到的一对真实 fnv32a 碰撞值：单独编码为 8 字节小端后，
// fingerprint([]ChunkInfo{{ChunkID:a}}) == fingerprint([]ChunkInfo{{ChunkID:b}})，
// 但 a != b，因此 sameChunks 必须判定二者不同。
func TestDiff_FingerprintCollisionNotPaired(t *testing.T) {
	const a uint64 = 1662899713301785713
	const b uint64 = 17025389778295797564

	// 前置断言：确认这两个值确实是构造好的指纹碰撞，否则测试意图不成立。
	fpA := fingerprint([]rman.ChunkInfo{chunk(a)})
	fpB := fingerprint([]rman.ChunkInfo{chunk(b)})
	if fpA != fpB {
		t.Fatalf("测试前提不成立：fingerprint(a)=%d fingerprint(b)=%d 应相等", fpA, fpB)
	}

	old := []rman.FileEntry{entry("old/x.bin", a)}
	newFiles := []rman.FileEntry{entry("new/y.bin", b)}

	got := Diff(old, newFiles)

	if len(got.Moved) != 0 {
		t.Fatalf("Moved 数量 = %d, 期望 0（指纹碰撞不应导致误配对）", len(got.Moved))
	}
	if len(got.Removed) != 1 || got.Removed[0] != "old/x.bin" {
		t.Errorf("Removed = %v, 期望 [old/x.bin]", got.Removed)
	}
	if len(got.Added) != 1 || got.Added[0].Path != "new/y.bin" {
		t.Errorf("Added = %v, 期望 [new/y.bin]", paths(got.Added))
	}
}

// TestDiff_MovedOneToOne 验证一个旧路径对应多个候选新路径（ChunkID 序列相同）时，
// 只配对第一个（按 new 原始顺序），其余归入 Added。
func TestDiff_MovedOneToOne(t *testing.T) {
	old := []rman.FileEntry{entry("old/a.bin", 5, 6, 7)}
	newFiles := []rman.FileEntry{
		entry("new/a1.bin", 5, 6, 7),
		entry("new/a2.bin", 5, 6, 7),
	}

	got := Diff(old, newFiles)

	if len(got.Moved) != 1 {
		t.Fatalf("Moved 数量 = %d, 期望 1", len(got.Moved))
	}
	if got.Moved[0].From != "old/a.bin" || got.Moved[0].Entry.Path != "new/a1.bin" {
		t.Errorf("Moved[0] = %+v, 期望 From=old/a.bin Entry.Path=new/a1.bin（先到先得）", got.Moved[0])
	}
	if len(got.Added) != 1 || got.Added[0].Path != "new/a2.bin" {
		t.Errorf("Added = %v, 期望 [new/a2.bin]", paths(got.Added))
	}
	if len(got.Removed) != 0 {
		t.Errorf("Removed 数量 = %d, 期望 0（旧路径已被配对）", len(got.Removed))
	}
}
