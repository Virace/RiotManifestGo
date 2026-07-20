package main

import (
	"testing"

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

func TestBuildSyncOptionsDefaultRemovesDeleted(t *testing.T) {
	opts := buildSyncOptions(update.ModeAuto, "", false)
	if !opts.RemoveDeleted {
		t.Error("keepRemoved=false 时 RemoveDeleted 期望 true（默认清理）")
	}
	if opts.OldManifestPath != "" {
		t.Errorf("OldManifestPath = %q, want empty", opts.OldManifestPath)
	}
	if opts.Mode != update.ModeAuto {
		t.Errorf("Mode = %v, want ModeAuto", opts.Mode)
	}
}

func TestBuildSyncOptionsKeepRemovedDisablesCleanup(t *testing.T) {
	opts := buildSyncOptions(update.ModeAuto, "", true)
	if opts.RemoveDeleted {
		t.Error("keepRemoved=true 时 RemoveDeleted 期望 false（保留旧文件）")
	}
}

func TestBuildSyncOptionsPassesThroughUpdatePath(t *testing.T) {
	opts := buildSyncOptions(update.ModeRepair, "/tmp/old.manifest", false)
	if opts.OldManifestPath != "/tmp/old.manifest" {
		t.Errorf("OldManifestPath = %q, want /tmp/old.manifest", opts.OldManifestPath)
	}
	if opts.Mode != update.ModeRepair {
		t.Errorf("Mode = %v, want ModeRepair", opts.Mode)
	}
}

// ---- extractManifestArg：-update 需要消费其后的值 ----

func TestExtractManifestArgConsumesUpdateFlagValue(t *testing.T) {
	args := []string{"game.manifest", "-update", "old.manifest", "-o", "./output"}
	manifest, remaining := extractManifestArg(args)
	if manifest != "game.manifest" {
		t.Errorf("manifest = %q, want game.manifest", manifest)
	}
	want := []string{"-update", "old.manifest", "-o", "./output"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
	for i := range want {
		if remaining[i] != want[i] {
			t.Errorf("remaining[%d] = %q, want %q", i, remaining[i], want[i])
		}
	}
}

func TestExtractManifestArgTreatsRepairVerifyOnlyNoVerifyAsBoolFlags(t *testing.T) {
	args := []string{"-repair", "-verify-only", "-no-verify", "-keep-removed", "game.manifest"}
	manifest, remaining := extractManifestArg(args)
	if manifest != "game.manifest" {
		t.Errorf("manifest = %q, want game.manifest", manifest)
	}
	want := []string{"-repair", "-verify-only", "-no-verify", "-keep-removed"}
	if len(remaining) != len(want) {
		t.Fatalf("remaining = %v, want %v", remaining, want)
	}
}
