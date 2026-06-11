package core

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestOutputPathAcceptsSafeManifestPath(t *testing.T) {
	root := t.TempDir()
	got, err := outputPath(root, "RADS/solutions/file.bin")
	if err != nil {
		t.Fatalf("outputPath returned error for safe path: %v", err)
	}

	want := filepath.Join(root, "RADS", "solutions", "file.bin")
	if got != want {
		t.Fatalf("outputPath = %q, want %q", got, want)
	}
}

func TestOutputPathRejectsTraversalAndPlatformSpecificEscapes(t *testing.T) {
	root := t.TempDir()
	unsafePaths := []string{
		"../escape.bin",
		"safe/../../escape.bin",
		"/absolute.bin",
		"safe\\..\\escape.bin",
		"C:/Windows/system32.dll",
		"file.bin:stream",
		"safe//file.bin",
	}

	for _, p := range unsafePaths {
		t.Run(strings.ReplaceAll(p, "/", "_"), func(t *testing.T) {
			if got, err := outputPath(root, p); err == nil {
				t.Fatalf("outputPath(%q) = %q, want error", p, got)
			}
		})
	}
}
