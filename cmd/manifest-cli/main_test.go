package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Virace/RiotManifestGo/pkg/core"
	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/Virace/RiotManifestGo/pkg/update"
)

func TestApplyFiltersReturnsInvalidRegexError(t *testing.T) {
	files := []rman.FileEntry{{Path: "DATA/Aatrox.bin"}}

	_, err := applyFilters(files, "[invalid", "")
	if err == nil {
		t.Fatal("applyFilters should return invalid regex error")
	}
}

func TestApplyFiltersKeepsValidFilterBehavior(t *testing.T) {
	files := []rman.FileEntry{
		{Path: "DATA/Aatrox.bin", Flags: []string{"zh_CN"}},
		{Path: "DATA/Ahri.bin", Flags: []string{"de_DE"}},
	}

	got, err := applyFilters(files, "Aatrox", "zh_CN")
	if err != nil {
		t.Fatalf("applyFilters returned error for valid filter: %v", err)
	}
	if len(got) != 1 || got[0].Path != "DATA/Aatrox.bin" {
		t.Fatalf("applyFilters returned %#v, want Aatrox only", got)
	}
}

// ---- resolveUpdateMode：-repair/-verify-only/-no-verify 互斥与映射 ----

func TestResolveUpdateModeDefaultsToAuto(t *testing.T) {
	mode, err := resolveUpdateMode(false, false, false)
	if err != nil {
		t.Fatalf("resolveUpdateMode 不应返回 error: %v", err)
	}
	if mode != update.ModeAuto {
		t.Errorf("mode = %v, want ModeAuto", mode)
	}
}

func TestResolveUpdateModeSingleFlagMapping(t *testing.T) {
	cases := []struct {
		name                         string
		repair, verifyOnly, noVerify bool
		want                         update.Mode
	}{
		{"repair", true, false, false, update.ModeRepair},
		{"verify-only", false, true, false, update.ModeVerifyOnly},
		{"no-verify", false, false, true, update.ModeForceFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mode, err := resolveUpdateMode(tc.repair, tc.verifyOnly, tc.noVerify)
			if err != nil {
				t.Fatalf("resolveUpdateMode 不应返回 error: %v", err)
			}
			if mode != tc.want {
				t.Errorf("mode = %v, want %v", mode, tc.want)
			}
		})
	}
}

func TestResolveUpdateModeRejectsMutuallyExclusiveCombinations(t *testing.T) {
	cases := []struct {
		name                         string
		repair, verifyOnly, noVerify bool
	}{
		{"repair+verify-only", true, true, false},
		{"repair+no-verify", true, false, true},
		{"verify-only+no-verify", false, true, true},
		{"all-three", true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resolveUpdateMode(tc.repair, tc.verifyOnly, tc.noVerify)
			if err == nil {
				t.Fatal("resolveUpdateMode 期望互斥组合返回 error")
			}
		})
	}
}

// ---- buildSyncOptions：flag → update.Options 映射 ----

func TestBuildSyncOptionsDefaultsToStandaloneWithoutCleanup(t *testing.T) {
	opts := buildSyncOptions(update.ModeAuto, false, "", false)
	if opts.Operation != update.OperationDownload {
		t.Errorf("Operation = %v, want OperationDownload", opts.Operation)
	}
	if opts.RemoveDeleted {
		t.Error("默认 standalone 不得拥有清理权限")
	}
	if opts.OldManifestPath != "" {
		t.Errorf("OldManifestPath = %q, want empty", opts.OldManifestPath)
	}
	if opts.Mode != update.ModeAuto {
		t.Errorf("Mode = %v, want ModeAuto", opts.Mode)
	}
}

func TestBuildSyncOptionsKeepRemovedDisablesManagedCleanup(t *testing.T) {
	opts := buildSyncOptions(update.ModeAuto, true, "", true)
	if opts.Operation != update.OperationInstall {
		t.Errorf("Operation = %v, want OperationInstall", opts.Operation)
	}
	if opts.RemoveDeleted {
		t.Error("keepRemoved=true 时 RemoveDeleted 期望 false（保留旧文件）")
	}
}

func TestBuildSyncOptionsManagedInstallPassesUpdateAndEnablesCleanup(t *testing.T) {
	opts := buildSyncOptions(update.ModeRepair, true, "/tmp/old.manifest", false)
	if opts.OldManifestPath != "/tmp/old.manifest" {
		t.Errorf("OldManifestPath = %q, want /tmp/old.manifest", opts.OldManifestPath)
	}
	if opts.Mode != update.ModeRepair {
		t.Errorf("Mode = %v, want ModeRepair", opts.Mode)
	}
	if !opts.RemoveDeleted {
		t.Error("受管理安装默认应启用清理")
	}
}

func TestValidateInstallOnlyFlags(t *testing.T) {
	if err := validateInstallOnlyFlags(false, "old.manifest", false); err == nil {
		t.Error("standalone -update 应报错")
	}
	if err := validateInstallOnlyFlags(false, "", true); err == nil {
		t.Error("standalone -keep-removed 应报错")
	}
	if err := validateInstallOnlyFlags(true, "old.manifest", true); err != nil {
		t.Errorf("-install 应允许 install-only flags: %v", err)
	}
}

func TestGuardStandaloneManagedRoot(t *testing.T) {
	dir := t.TempDir()
	rmanDir := filepath.Join(dir, ".rman")
	if err := os.MkdirAll(rmanDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rmanDir, "installed.json"), []byte("{future-or-corrupt}"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := guardStandaloneManagedRoot(dir, update.OperationDownload, update.ModeAuto); err == nil {
		t.Error("standalone 写入受管理根应报错")
	}
	if err := guardStandaloneManagedRoot(dir, update.OperationDownload, update.ModeVerifyOnly); err != nil {
		t.Errorf("standalone VERIFY_ONLY 应允许: %v", err)
	}
	if err := guardStandaloneManagedRoot(dir, update.OperationInstall, update.ModeAuto); err != nil {
		t.Errorf("managed install 应允许: %v", err)
	}
}

// ---- extractManifestArg：-update 需要消费其后的值 ----

func TestExtractManifestArgConsumesUpdateFlagValue(t *testing.T) {
	args := []string{"game.manifest", "-update", "old.manifest", "-o", "./output", "-retry-wait", "4s"}
	manifest, remaining := extractManifestArg(args)
	if manifest != "game.manifest" {
		t.Errorf("manifest = %q, want game.manifest", manifest)
	}
	want := []string{"-update", "old.manifest", "-o", "./output", "-retry-wait", "4s"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Errorf("remaining[%d] = %q, want %q", i, remaining[i], want[i])
		}
	}
}

func TestExtractManifestArgConsumesGranularityFlagValues(t *testing.T) {
	args := []string{
		"game.manifest",
		"-gap-tolerance", "65536",
		"-max-ranges", "20",
		"-full-bundle-threshold", "0.7",
		"-plan-only",
	}
	manifest, remaining := extractManifestArg(args)
	if manifest != "game.manifest" {
		t.Errorf("manifest = %q, want game.manifest", manifest)
	}
	want := []string{
		"-gap-tolerance", "65536",
		"-max-ranges", "20",
		"-full-bundle-threshold", "0.7",
		"-plan-only",
	}
	if len(remaining) != len(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Errorf("remaining[%d] = %q, want %q", i, remaining[i], want[i])
		}
	}
}

func TestExtractManifestArgTreatsOperationAndModeFlagsAsBoolFlags(t *testing.T) {
	args := []string{"-install", "-repair", "-verify-only", "-no-verify", "-keep-removed", "game.manifest"}
	manifest, remaining := extractManifestArg(args)
	if manifest != "game.manifest" {
		t.Errorf("manifest = %q, want game.manifest", manifest)
	}
	want := []string{"-install", "-repair", "-verify-only", "-no-verify", "-keep-removed"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
}

// ---- manifestInfoLines：启动横幅元信息 ----

// infoTestManifest 构造横幅测试用清单：2 文件共 3 个唯一 Chunk（其中 0xB1 被
// 两个文件共享，验证压缩总量按唯一 ChunkID 去重）、2 个 Bundle、1 条参数。
func infoTestManifest() *rman.Manifest {
	shared := rman.ChunkInfo{ChunkID: 0xB1, BundleID: 0x2, CompressedSize: 300, HashType: rman.HashTypeHKDF}
	return &rman.Manifest{
		ManifestID:   0x1122334455667788,
		MajorVersion: 2,
		MinorVersion: 1,
		Flags:        []string{"en_US", "zh_CN"},
		Params: []rman.Params{
			{HashType: rman.HashTypeHKDF, MinChunkSize: 4096, MaxChunkSize: 16384, MaxUncompressed: 16384},
		},
		Files: []rman.FileEntry{
			{Path: "a.wad", FileSize: 1000, Chunks: []rman.ChunkInfo{
				{ChunkID: 0xA1, BundleID: 0x1, CompressedSize: 100, HashType: rman.HashTypeHKDF},
				shared,
			}},
			{Path: "b.wad", FileSize: 2000, Chunks: []rman.ChunkInfo{
				shared,
				{ChunkID: 0xC1, BundleID: 0x2, CompressedSize: 200, HashType: rman.HashTypeHKDF},
			}},
		},
	}
}

func linesContain(lines []string, sub string) bool {
	for _, l := range lines {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

func TestManifestInfoLinesShowsManifestFacts(t *testing.T) {
	lines := manifestInfoLines(infoTestManifest(), manifestSourceMeta{size: 12345})

	for _, want := range []string{
		"ManifestID: 1122334455667788",
		"RMAN v2.1",
		"文件: 2",
		"Chunk: 3",  // 唯一 ChunkID 去重后 3 个
		"Bundle: 2", // 0x1 与 0x2
		"600 B",     // 压缩总量 100+300+200，共享 Chunk 只计一次
		"哈希算法: HKDF",
		"4.0 KB ~ 16.0 KB",
		"en_US, zh_CN（共 2 个）",
	} {
		if !linesContain(lines, want) {
			t.Errorf("横幅缺少 %q，实际输出:\n%s", want, strings.Join(lines, "\n"))
		}
	}
}

func TestManifestInfoLinesMarksURLTimeAsGuessed(t *testing.T) {
	meta := manifestSourceMeta{
		size:    100,
		modTime: time.Date(2023, 8, 3, 8, 38, 0, 0, time.UTC),
		fromURL: true,
	}
	lines := manifestInfoLines(infoTestManifest(), meta)

	if !linesContain(lines, "推测") {
		t.Errorf("URL 来源的清单时间必须标注推测，实际输出:\n%s", strings.Join(lines, "\n"))
	}
	if !linesContain(lines, "Last-Modified") {
		t.Errorf("URL 来源的清单时间应说明来自 Last-Modified 响应头，实际输出:\n%s", strings.Join(lines, "\n"))
	}
}

func TestManifestInfoLinesMarksLocalMtimeAsGuessed(t *testing.T) {
	meta := manifestSourceMeta{
		size:    100,
		modTime: time.Date(2026, 7, 18, 21, 0, 0, 0, time.Local),
	}
	lines := manifestInfoLines(infoTestManifest(), meta)

	if !linesContain(lines, "推测") {
		t.Errorf("本地文件时间必须标注推测，实际输出:\n%s", strings.Join(lines, "\n"))
	}
	if !linesContain(lines, "本地文件修改时间") {
		t.Errorf("本地来源应说明时间取自文件系统 mtime，实际输出:\n%s", strings.Join(lines, "\n"))
	}
}

func TestManifestInfoLinesUnknownTime(t *testing.T) {
	lines := manifestInfoLines(infoTestManifest(), manifestSourceMeta{size: 100})
	if !linesContain(lines, "清单时间: 未知") {
		t.Errorf("时间缺失时应显示未知，实际输出:\n%s", strings.Join(lines, "\n"))
	}
}

func TestManifestInfoLinesNotesFilesWithoutDeclaredHash(t *testing.T) {
	m := infoTestManifest()
	m.Files = append(m.Files, rman.FileEntry{Path: "c.wad", FileSize: 10, Chunks: []rman.ChunkInfo{
		{ChunkID: 0xD1, BundleID: 0x3, CompressedSize: 10, HashType: rman.HashTypeNone},
	}})
	lines := manifestInfoLines(m, manifestSourceMeta{size: 100})

	if !linesContain(lines, "1 个文件的 Chunk 哈希算法未声明") {
		t.Errorf("存在 HashTypeNone 文件时应输出提示，实际输出:\n%s", strings.Join(lines, "\n"))
	}
}

func TestManifestInfoLinesNoParamsTable(t *testing.T) {
	m := infoTestManifest()
	m.Params = nil
	lines := manifestInfoLines(m, manifestSourceMeta{size: 100})

	if !linesContain(lines, "哈希算法: 未声明") {
		t.Errorf("无 Parameters 表时应显示未声明，实际输出:\n%s", strings.Join(lines, "\n"))
	}
}

// ---- parseCDNURLs：-u 逗号分隔多域名解析 ----

func TestParseCDNURLs(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"single", "https://a.example.com", []string{"https://a.example.com"}},
		{"two with spaces", "https://a.example.com, https://b.example.com",
			[]string{"https://a.example.com", "https://b.example.com"}},
		{"trailing comma", "https://a.example.com,https://b.example.com,",
			[]string{"https://a.example.com", "https://b.example.com"}},
		{"all empty", " , ,\t", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCDNURLs(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("parseCDNURLs(%q) = %v, want %v", tc.in, got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("parseCDNURLs(%q)[%d] = %q, want %q", tc.in, i, got[i], tc.want[i])
				}
			}
		})
	}
}

// ---- summarizePlan：plan-only 计划统计 ----

func TestSummarizePlan(t *testing.T) {
	jobs := []core.BundleJob{
		// Job0：整包作业。BundleSize=1000，内部 Chunk 有效字节合计 700 → 该作业多下 300。
		{
			BundleID:   1,
			FullBundle: true,
			BundleSize: 1000,
			Ranges: []core.ChunkRange{
				{Start: 0, End: 299, Chunks: []core.GlobalChunkTask{{CompressedSize: 300}}},
				{Start: 300, End: 699, Chunks: []core.GlobalChunkTask{{CompressedSize: 400}}},
			},
		},
		// Job1：两段 Range 作业，段宽与 Chunk 大小完全一致，无多下浪费。
		{
			BundleID: 2,
			Ranges: []core.ChunkRange{
				{Start: 0, End: 99, Chunks: []core.GlobalChunkTask{{CompressedSize: 100}}},
				{Start: 150, End: 249, Chunks: []core.GlobalChunkTask{{CompressedSize: 100}}},
			},
		},
		// Job2：单段 Range 作业，Gap Tolerance 合并产生 40 字节多下（段宽 190 vs Chunk 合计 150）。
		{
			BundleID: 3,
			Ranges: []core.ChunkRange{
				{Start: 0, End: 189, Chunks: []core.GlobalChunkTask{{CompressedSize: 100}, {CompressedSize: 50}}},
			},
		},
	}

	summary := summarizePlan(jobs)

	if summary.Jobs != 3 {
		t.Errorf("Jobs = %d, want 3", summary.Jobs)
	}
	if summary.FullBundleJobs != 1 {
		t.Errorf("FullBundleJobs = %d, want 1", summary.FullBundleJobs)
	}
	// Segments 只统计非整包作业的 Range 段数：Job1 两段 + Job2 一段 = 3。
	if summary.Segments != 3 {
		t.Errorf("Segments = %d, want 3", summary.Segments)
	}
	// UsefulBytes = 全部 Chunk CompressedSize 之和：(300+400)+(100+100)+(100+50) = 1050。
	if summary.UsefulBytes != 1050 {
		t.Errorf("UsefulBytes = %d, want 1050", summary.UsefulBytes)
	}
	// FetchedBytes：整包用 BundleSize=1000；Job1 段宽 100+100=200；Job2 段宽 190 → 合计 1390。
	if summary.FetchedBytes != 1390 {
		t.Errorf("FetchedBytes = %d, want 1390", summary.FetchedBytes)
	}
	// 每作业 Fetched 字节 [1000, 200, 190] 升序为 [190, 200, 1000]：
	// P50 idx=3*50/100=1 → 200；P90 idx=3*90/100=2 → 1000；Max=1000。
	if summary.P50 != 200 {
		t.Errorf("P50 = %d, want 200", summary.P50)
	}
	if summary.P90 != 1000 {
		t.Errorf("P90 = %d, want 1000", summary.P90)
	}
	if summary.Max != 1000 {
		t.Errorf("Max = %d, want 1000", summary.Max)
	}
}

func TestSummarizePlanEmptyJobs(t *testing.T) {
	summary := summarizePlan(nil)
	if summary.Jobs != 0 || summary.FullBundleJobs != 0 || summary.Segments != 0 {
		t.Errorf("summarizePlan(nil) 计数字段应全为 0，实际 %+v", summary)
	}
	if summary.UsefulBytes != 0 || summary.FetchedBytes != 0 {
		t.Errorf("summarizePlan(nil) 字节字段应全为 0，实际 %+v", summary)
	}
	if summary.P50 != 0 || summary.P90 != 0 || summary.Max != 0 {
		t.Errorf("summarizePlan(nil) 分位字段应全为 0，实际 %+v", summary)
	}
}

// ---- humanCount ----

func TestHumanCount(t *testing.T) {
	cases := map[int]string{
		0:       "0",
		999:     "999",
		1000:    "1,000",
		787480:  "787,480",
		1000000: "1,000,000",
	}
	for n, want := range cases {
		if got := humanCount(n); got != want {
			t.Errorf("humanCount(%d) = %q, want %q", n, got, want)
		}
	}
}
