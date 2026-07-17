package update

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTempArchive 创建一个基于临时目录的 Archive，供测试使用。
func newTempArchive(t *testing.T) (*Archive, string) {
	t.Helper()
	dir := t.TempDir()
	return NewArchive(dir), dir
}

// TestSaveLoadRoundTrip 验证 Save 写入后 LoadInstalled 能读回一致的状态，
// 且落盘的 manifest 原始字节与传入内容一致，InstalledManifestPath 也能定位到该文件。
func TestSaveLoadRoundTrip(t *testing.T) {
	archive, outputDir := newTempArchive(t)

	manifestID := uint64(0x037EC59D5BD7C5D3)
	raw := []byte("fake manifest bytes")
	source := "https://lol.secure.dyn.riotcdn.net/channels/public/releases/037EC59D5BD7C5D3.manifest"

	if err := archive.Save(manifestID, raw, source); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}

	state, err := archive.LoadInstalled()
	if err != nil {
		t.Fatalf("LoadInstalled 返回 error: %v", err)
	}
	if state == nil {
		t.Fatal("LoadInstalled 返回 nil，期望有效状态")
	}

	if state.Schema != 1 {
		t.Errorf("Schema = %d, want 1", state.Schema)
	}
	wantManifestID := "037EC59D5BD7C5D3"
	if state.ManifestID != wantManifestID {
		t.Errorf("ManifestID = %q, want %q", state.ManifestID, wantManifestID)
	}
	wantManifestFile := "manifests/037EC59D5BD7C5D3.manifest"
	if state.ManifestFile != wantManifestFile {
		t.Errorf("ManifestFile = %q, want %q", state.ManifestFile, wantManifestFile)
	}
	if state.Source != source {
		t.Errorf("Source = %q, want %q", state.Source, source)
	}
	if _, err := time.Parse(time.RFC3339, state.UpdatedAt); err != nil {
		t.Errorf("UpdatedAt 不是合法 RFC3339: %q (%v)", state.UpdatedAt, err)
	}
	if !strings.HasSuffix(state.UpdatedAt, "Z") {
		t.Errorf("UpdatedAt 期望为 UTC（以 Z 结尾）: %q", state.UpdatedAt)
	}

	// 校验落盘的 manifest 原始字节内容
	manifestPath := filepath.Join(outputDir, ".rman", "manifests", wantManifestID+".manifest")
	gotRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("读取存档 manifest 失败: %v", err)
	}
	if string(gotRaw) != string(raw) {
		t.Errorf("存档 manifest 内容不匹配: got %q want %q", gotRaw, raw)
	}

	// InstalledManifestPath 应指向该文件且文件存在
	path, ok := archive.InstalledManifestPath()
	if !ok {
		t.Fatal("InstalledManifestPath 返回 ok=false，期望 true")
	}
	if path != manifestPath {
		t.Errorf("InstalledManifestPath = %q, want %q", path, manifestPath)
	}
}

// TestLoadInstalled_Missing 验证 installed.json 不存在时返回 (nil, nil)。
func TestLoadInstalled_Missing(t *testing.T) {
	archive, _ := newTempArchive(t)

	state, err := archive.LoadInstalled()
	if err != nil {
		t.Fatalf("LoadInstalled 返回 error: %v", err)
	}
	if state != nil {
		t.Errorf("installed.json 缺失时期望 nil，got %+v", state)
	}

	if _, ok := archive.InstalledManifestPath(); ok {
		t.Error("installed.json 缺失时 InstalledManifestPath 期望 ok=false")
	}
}

// TestLoadInstalled_CorruptJSON 验证 installed.json 内容不是合法 JSON 时返回 (nil, nil)。
func TestLoadInstalled_CorruptJSON(t *testing.T) {
	archive, outputDir := newTempArchive(t)
	rmanDir := filepath.Join(outputDir, ".rman")
	if err := os.MkdirAll(rmanDir, 0755); err != nil {
		t.Fatalf("创建 .rman 目录失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rmanDir, "installed.json"), []byte("{this is not json"), 0644); err != nil {
		t.Fatalf("写入损坏 JSON 失败: %v", err)
	}

	state, err := archive.LoadInstalled()
	if err != nil {
		t.Fatalf("LoadInstalled 返回 error: %v", err)
	}
	if state != nil {
		t.Errorf("损坏 JSON 期望 nil，got %+v", state)
	}
}

// TestLoadInstalled_UnknownSchema 验证 schema 字段不为 1 时视为无状态，返回 (nil, nil)。
func TestLoadInstalled_UnknownSchema(t *testing.T) {
	archive, outputDir := newTempArchive(t)
	rmanDir := filepath.Join(outputDir, ".rman")
	if err := os.MkdirAll(rmanDir, 0755); err != nil {
		t.Fatalf("创建 .rman 目录失败: %v", err)
	}

	payload, err := json.Marshal(map[string]any{
		"schema":        2,
		"manifest_id":   "037EC59D5BD7C5D3",
		"manifest_file": "manifests/037EC59D5BD7C5D3.manifest",
		"source":        "https://example.com/x.manifest",
		"updated_at":    time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("构造测试数据失败: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rmanDir, "installed.json"), payload, 0644); err != nil {
		t.Fatalf("写入 installed.json 失败: %v", err)
	}

	state, err := archive.LoadInstalled()
	if err != nil {
		t.Fatalf("LoadInstalled 返回 error: %v", err)
	}
	if state != nil {
		t.Errorf("未识别 schema 期望 nil，got %+v", state)
	}
}

// TestSaveRetainsOnlyLatestTwoManifests 验证连续 Save 三个版本后，
// manifests/ 目录只保留最近两份（本次与上一次），且 installed.json 指向最新版本。
func TestSaveRetainsOnlyLatestTwoManifests(t *testing.T) {
	archive, outputDir := newTempArchive(t)

	ids := []uint64{0x1111111111111111, 0x2222222222222222, 0x3333333333333333}
	for i, id := range ids {
		if err := archive.Save(id, []byte(fmt.Sprintf("manifest-%d", i)), "source"); err != nil {
			t.Fatalf("第 %d 次 Save 失败: %v", i+1, err)
		}
	}

	manifestsDir := filepath.Join(outputDir, ".rman", "manifests")
	entries, err := os.ReadDir(manifestsDir)
	if err != nil {
		t.Fatalf("读取 manifests 目录失败: %v", err)
	}
	if len(entries) != 2 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("manifests 目录期望剩 2 个文件，got %d: %v", len(entries), names)
	}

	wantNames := map[string]bool{
		fmt.Sprintf("%016X.manifest", ids[1]): true,
		fmt.Sprintf("%016X.manifest", ids[2]): true,
	}
	for _, e := range entries {
		if !wantNames[e.Name()] {
			t.Errorf("不应保留的文件: %s", e.Name())
		}
	}

	state, err := archive.LoadInstalled()
	if err != nil || state == nil {
		t.Fatalf("LoadInstalled 失败: err=%v state=%v", err, state)
	}
	wantLatestID := fmt.Sprintf("%016X", ids[2])
	if state.ManifestID != wantLatestID {
		t.Errorf("installed.json 指向 %s，want %s", state.ManifestID, wantLatestID)
	}
}

// TestSaveAtomicity_TmpWriteFailureLeavesInstalledIntact 验证 installed.json 写入采用
// tmp+rename 原子流程：预置一份合法 installed.json，注入错误使临时文件写入（rename 之前）
// 必然失败，Save 应返回 error 且旧 installed.json 内容必须保持字节级不变。
func TestSaveAtomicity_TmpWriteFailureLeavesInstalledIntact(t *testing.T) {
	archive, outputDir := newTempArchive(t)
	rmanDir := filepath.Join(outputDir, ".rman")
	if err := os.MkdirAll(rmanDir, 0755); err != nil {
		t.Fatalf("创建 .rman 目录失败: %v", err)
	}

	original := []byte(`{
  "schema": 1,
  "manifest_id": "AAAAAAAAAAAAAAAA",
  "manifest_file": "manifests/AAAAAAAAAAAAAAAA.manifest",
  "source": "https://example.com/old.manifest",
  "updated_at": "2026-01-01T00:00:00Z"
}`)
	installedPath := filepath.Join(rmanDir, "installed.json")
	if err := os.WriteFile(installedPath, original, 0644); err != nil {
		t.Fatalf("预置 installed.json 失败: %v", err)
	}

	// 注入错误：让 installed.json 对应的临时文件路径被一个同名目录占用，
	// 使得写临时文件这一步（发生在 rename 之前）必然失败，从而验证 rename 之前
	// 出错不会损坏旧文件。
	tmpPath := installedPath + ".tmp"
	if err := os.Mkdir(tmpPath, 0755); err != nil {
		t.Fatalf("预置冲突目录失败: %v", err)
	}

	err := archive.Save(0xBBBBBBBBBBBBBBBB, []byte("new raw"), "https://example.com/new.manifest")
	if err == nil {
		t.Fatal("期望 Save 返回 error（临时文件写入应失败），实际为 nil")
	}

	got, err := os.ReadFile(installedPath)
	if err != nil {
		t.Fatalf("读取 installed.json 失败: %v", err)
	}
	if string(got) != string(original) {
		t.Errorf("installed.json 被破坏：\ngot:\n%s\nwant:\n%s", got, original)
	}
}
