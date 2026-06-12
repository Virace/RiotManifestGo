package main

import (
	"testing"

	"github.com/Virace/RiotManifestGo/pkg/rman"
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
