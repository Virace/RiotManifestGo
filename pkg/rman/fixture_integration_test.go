//go:build fixtures

package rman

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type fixtureMetadata struct {
	CacheDir string              `json:"cache_dir"`
	Fixtures []fixtureDefinition `json:"fixtures"`
}

type fixtureDefinition struct {
	Name              string `json:"name"`
	Slot              string `json:"slot"`
	Platform          string `json:"platform"`
	Product           string `json:"product"`
	Version           string `json:"version"`
	UpstreamIndexPath string `json:"upstream_index_path"`
	ManifestID        string `json:"manifest_id"`
	CDNURL            string `json:"cdn_url"`
	LatestSelector    string `json:"latest_selector"`
	Size              int64  `json:"size"`
	SHA256            string `json:"sha256"`
}

type resolvedFixtureDocument struct {
	Fixtures []resolvedFixture `json:"fixtures"`
}

type resolvedFixture struct {
	Name              string `json:"name"`
	Slot              string `json:"slot"`
	Platform          string `json:"platform"`
	Product           string `json:"product"`
	Version           string `json:"version"`
	UpstreamIndexPath string `json:"upstream_index_path"`
	ManifestID        string `json:"manifest_id"`
	CDNURL            string `json:"cdn_url"`
	LocalPath         string `json:"local_path"`
	Size              int64  `json:"size"`
	SHA256            string `json:"sha256"`
}

type expectedFixtureSummary struct {
	ManifestID        string               `json:"manifest_id"`
	FileCount         int                  `json:"file_count"`
	TotalChunkRefs    int                  `json:"total_chunk_refs"`
	UniqueBundleCount int                  `json:"unique_bundle_count"`
	RequiredFiles     []expectedMarkerFile `json:"required_files"`
}

type expectedMarkerFile struct {
	Path      string `json:"path"`
	Flag      string `json:"flag"`
	MinChunks int    `json:"min_chunks"`
}

func TestManifestFixtureMetadata(t *testing.T) {
	metadata := readFixtureMetadata(t)
	if len(metadata.Fixtures) != 3 {
		t.Fatalf("fixture metadata count = %d, want 3", len(metadata.Fixtures))
	}

	seenSlots := make(map[string]bool, len(metadata.Fixtures))
	for _, fixture := range metadata.Fixtures {
		if fixture.Name == "" {
			t.Fatal("fixture name must not be empty")
		}
		if seenSlots[fixture.Slot] {
			t.Fatalf("duplicate fixture slot %q", fixture.Slot)
		}
		seenSlots[fixture.Slot] = true

		if fixture.Slot == "latest-release-check" {
			if fixture.LatestSelector == "" {
				t.Fatalf("latest fixture %q must define latest_selector", fixture.Name)
			}
			continue
		}

		if fixture.CDNURL == "" || fixture.ManifestID == "" || fixture.SHA256 == "" || fixture.Size <= 0 {
			t.Fatalf("fixed fixture %q must pin cdn_url, manifest_id, sha256, and size", fixture.Name)
		}
	}
}

func TestManifestFixtureCacheReadable(t *testing.T) {
	for _, fixture := range readResolvedFixtures(t) {
		t.Run(fixture.Name, func(t *testing.T) {
			data, err := os.ReadFile(repoPath(t, fixture.LocalPath))
			if err != nil {
				t.Fatalf("read fixture cache: %v", err)
			}
			if int64(len(data)) != fixture.Size {
				t.Fatalf("fixture size = %d, want %d", len(data), fixture.Size)
			}
			if len(data) < 4 || string(data[:4]) != "RMAN" {
				t.Fatalf("fixture does not start with RMAN magic")
			}

			sum := sha256.Sum256(data)
			got := strings.ToUpper(hex.EncodeToString(sum[:]))
			if got != strings.ToUpper(fixture.SHA256) {
				t.Fatalf("fixture sha256 = %s, want %s", got, fixture.SHA256)
			}
		})
	}
}

func TestParseManifestFixtures(t *testing.T) {
	expected := readExpectedSummaries(t)
	for _, fixture := range readResolvedFixtures(t) {
		t.Run(fixture.Name, func(t *testing.T) {
			manifest, err := ParseFile(repoPath(t, fixture.LocalPath))
			if err != nil {
				t.Fatalf("ParseFile failed: %v", err)
			}

			gotManifestID := fmt.Sprintf("%016X", manifest.ManifestID)
			if gotManifestID != fixture.ManifestID {
				t.Fatalf("manifest id = %s, want %s", gotManifestID, fixture.ManifestID)
			}

			totalChunkRefs, uniqueBundleCount := manifestCounts(manifest)
			if len(manifest.Files) == 0 || totalChunkRefs == 0 || uniqueBundleCount == 0 {
				t.Fatalf("parsed manifest is empty: files=%d chunks=%d bundles=%d", len(manifest.Files), totalChunkRefs, uniqueBundleCount)
			}

			summary, ok := expected[fixture.Name]
			if !ok {
				return
			}

			if gotManifestID != summary.ManifestID {
				t.Fatalf("summary manifest id = %s, want %s", gotManifestID, summary.ManifestID)
			}
			if len(manifest.Files) != summary.FileCount {
				t.Fatalf("file count = %d, want %d", len(manifest.Files), summary.FileCount)
			}
			if totalChunkRefs != summary.TotalChunkRefs {
				t.Fatalf("total chunk refs = %d, want %d", totalChunkRefs, summary.TotalChunkRefs)
			}
			if uniqueBundleCount != summary.UniqueBundleCount {
				t.Fatalf("unique bundle count = %d, want %d", uniqueBundleCount, summary.UniqueBundleCount)
			}

			filesByPath := make(map[string]FileEntry, len(manifest.Files))
			for _, file := range manifest.Files {
				filesByPath[file.Path] = file
			}
			for _, marker := range summary.RequiredFiles {
				file, ok := filesByPath[marker.Path]
				if !ok {
					t.Fatalf("required file %q not found", marker.Path)
				}
				if len(file.Chunks) < marker.MinChunks {
					t.Fatalf("required file %q chunks = %d, want at least %d", marker.Path, len(file.Chunks), marker.MinChunks)
				}
				if marker.Flag != "" && !containsFixtureFlag(file.Flags, marker.Flag) {
					t.Fatalf("required file %q flags = %v, want %q", marker.Path, file.Flags, marker.Flag)
				}
			}
		})
	}
}

func readFixtureMetadata(t *testing.T) fixtureMetadata {
	t.Helper()

	var metadata fixtureMetadata
	readFixtureJSON(t, "pkg/rman/testdata/fixtures/riot-manifests/fixtures.json", &metadata)
	return metadata
}

func readResolvedFixtures(t *testing.T) []resolvedFixture {
	t.Helper()

	var resolved resolvedFixtureDocument
	readFixtureJSON(t, "pkg/rman/testdata/.cache/riot-manifests/resolved-fixtures.json", &resolved)
	if len(resolved.Fixtures) == 0 {
		t.Fatal("resolved fixture cache is empty; run .\\scripts\\init-fixtures.ps1")
	}
	if len(resolved.Fixtures) > 3 {
		t.Fatalf("resolved fixture count = %d, want at most 3", len(resolved.Fixtures))
	}
	return resolved.Fixtures
}

func readExpectedSummaries(t *testing.T) map[string]expectedFixtureSummary {
	t.Helper()

	var expected map[string]expectedFixtureSummary
	readFixtureJSON(t, "pkg/rman/testdata/fixtures/riot-manifests/expected_summaries.json", &expected)
	return expected
}

func readFixtureJSON(t *testing.T, relativePath string, target any) {
	t.Helper()

	data, err := os.ReadFile(repoPath(t, relativePath))
	if os.IsNotExist(err) && strings.Contains(relativePath, ".cache") {
		t.Fatalf("fixture cache metadata missing at %s; run .\\scripts\\init-fixtures.ps1", relativePath)
	}
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("parse %s: %v", relativePath, err)
	}
}

func repoPath(t *testing.T, relativePath string) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", filepath.FromSlash(relativePath))
}

func manifestCounts(manifest *Manifest) (int, int) {
	totalChunkRefs := 0
	uniqueBundles := make(map[uint64]struct{})
	for _, file := range manifest.Files {
		totalChunkRefs += len(file.Chunks)
		for _, chunk := range file.Chunks {
			uniqueBundles[chunk.BundleID] = struct{}{}
		}
	}
	return totalChunkRefs, len(uniqueBundles)
}

func containsFixtureFlag(flags []string, want string) bool {
	for _, flag := range flags {
		if flag == want {
			return true
		}
	}
	return false
}
