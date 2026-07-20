package core

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestOutputPathAcceptsSafeManifestPath(t *testing.T) {
	root := t.TempDir()
	got, err := OutputPath(root, "RADS/solutions/file.bin")
	if err != nil {
		t.Fatalf("OutputPath returned error for safe path: %v", err)
	}

	want := filepath.Join(root, "RADS", "solutions", "file.bin")
	if got != want {
		t.Fatalf("OutputPath = %q, want %q", got, want)
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
			if got, err := OutputPath(root, p); err == nil {
				t.Fatalf("OutputPath(%q) = %q, want error", p, got)
			}
		})
	}
}
