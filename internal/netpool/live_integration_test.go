//go:build integration

package netpool_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Virace/RiotManifestGo/internal/netpool"
	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/core"
	"github.com/Virace/RiotManifestGo/pkg/rman"
)

const (
	rangeFallbackManifestURL = "https://lol.secure.dyn.riotcdn.net/channels/public/releases/5E3CF394695BDD5E.manifest"
	rangeFallbackBundleURL   = "https://lol.dyn.riotcdn.net/channels/public/bundles"
	rangeFallbackTargetPath  = "Plugins/rcp-be-lol-game-data/default-assets.wad"

	maxManifestBytes   = 16 << 20
	maxIntegrationData = 64 << 20
)

// TestRiotCDNDefaultAssetsMultiRange exercises the production path that
// previously failed when Riot CDN returned multipart/byteranges with an
// unquoted CloudFront boundary containing a colon. The standard MIME parser
// rejected that Content-Type, so the response was misclassified as one
// single-part 206:
//
// manifest parse -> exact file selection -> Map/Schedule -> live CDN Range
// fetch -> per-chunk ZSTD decompression and hash validation.
//
// The fixed manifest is intentionally release-only. Normal go test ./...
// remains offline; release gates opt in with -tags=integration.
func TestRiotCDNDefaultAssetsMultiRange(t *testing.T) {
	manifestCtx, cancelManifest := context.WithTimeout(context.Background(), time.Minute)
	defer cancelManifest()

	req, err := http.NewRequestWithContext(manifestCtx, http.MethodGet, rangeFallbackManifestURL, nil)
	if err != nil {
		t.Fatalf("create manifest request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("download manifest: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download manifest: HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > maxManifestBytes {
		t.Fatalf("manifest size %d exceeds release-test limit %d", resp.ContentLength, maxManifestBytes)
	}

	manifest, err := rman.Parse(io.LimitReader(resp.Body, maxManifestBytes))
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if got := fmt.Sprintf("%016X", manifest.ManifestID); got != "5E3CF394695BDD5E" {
		t.Fatalf("manifest ID = %s, want 5E3CF394695BDD5E", got)
	}

	var target *rman.FileEntry
	for i := range manifest.Files {
		if manifest.Files[i].Path == rangeFallbackTargetPath {
			target = &manifest.Files[i]
			break
		}
	}
	if target == nil {
		t.Fatalf("target file not found: %s", rangeFallbackTargetPath)
	}

	jobs := core.Schedule(
		core.Map([]rman.FileEntry{*target}),
		core.ScheduleConfig{
			MaxRangesPerReq: 30,
			GapTolerance:    core.DefaultGapTolerance,
		},
	)

	// Pick the smallest job with at least four disjoint ranges. This matches
	// the reported failure shape while keeping every release run bounded.
	var candidate core.BundleJob
	var candidateBytes int64
	for _, job := range jobs {
		if len(job.Ranges) < 4 {
			continue
		}
		var total int64
		for _, chunkRange := range job.Ranges {
			total += int64(chunkRange.End) - int64(chunkRange.Start) + 1
		}
		if candidateBytes == 0 ||
			total < candidateBytes ||
			(total == candidateBytes && job.BundleID < candidate.BundleID) {
			candidate = job
			candidateBytes = total
		}
	}
	if candidateBytes == 0 {
		t.Fatal("manifest no longer contains a multi-Range job with at least four ranges")
	}
	if candidateBytes > maxIntegrationData {
		t.Fatalf(
			"smallest candidate downloads %d bytes, exceeds release-test limit %d",
			candidateBytes,
			maxIntegrationData,
		)
	}

	ranges := make([]netpool.ByteRange, len(candidate.Ranges))
	for i, chunkRange := range candidate.Ranges {
		ranges[i] = netpool.ByteRange{
			Start: int64(chunkRange.Start),
			End:   int64(chunkRange.End),
		}
	}

	bundleClient := netpool.NewBundleClient(rangeFallbackBundleURL, 1)
	defer bundleClient.Close()

	t.Logf(
		"selected bundle=%s ranges=%v compressed_bytes=%d",
		candidate.BundleFilename,
		ranges,
		candidateBytes,
	)

	fetchCtx, cancelFetch := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelFetch()
	rangeData, err := bundleClient.FetchRanges(fetchCtx, candidate.BundleFilename, ranges)
	if err != nil {
		t.Fatalf(
			"fetch %s (%d ranges, %d bytes): %v",
			candidate.BundleFilename,
			len(ranges),
			candidateBytes,
			err,
		)
	}
	if len(rangeData) != len(candidate.Ranges) {
		t.Fatalf("range count = %d, want %d", len(rangeData), len(candidate.Ranges))
	}

	decoder := zstream.NewDecoder()
	validatedChunks := 0
	for i, data := range rangeData {
		chunkRange := candidate.Ranges[i]
		expectedLength := int(chunkRange.End - chunkRange.Start + 1)
		if len(data) != expectedLength {
			t.Fatalf("range[%d] length = %d, want %d", i, len(data), expectedLength)
		}

		for _, task := range chunkRange.Chunks {
			if len(task.Targets) == 0 {
				t.Fatalf("bundle task %016X has no write target", task.BundleID)
			}
			if task.BundleOffset < chunkRange.Start {
				t.Fatalf(
					"chunk %016X starts before range[%d]: chunk=%d range=%d",
					task.Targets[0].ChunkID,
					i,
					task.BundleOffset,
					chunkRange.Start,
				)
			}
			start := int(task.BundleOffset - chunkRange.Start)
			end := start + int(task.CompressedSize)
			if start < 0 || end > len(data) {
				t.Fatalf(
					"chunk %016X outside range[%d]: start=%d end=%d data=%d",
					task.Targets[0].ChunkID,
					i,
					start,
					end,
					len(data),
				)
			}

			target := task.Targets[0]
			if _, err := decoder.DecompressAndValidate(
				data[start:end],
				target.ExpectedLen,
				target.ChunkID,
				target.HashType,
			); err != nil {
				t.Fatalf("validate chunk %016X: %v", target.ChunkID, err)
			}
			validatedChunks++
		}
	}

	t.Logf(
		"validated manifest=%016X bundle=%s ranges=%d compressed_bytes=%d chunks=%d",
		manifest.ManifestID,
		candidate.BundleFilename,
		len(candidate.Ranges),
		candidateBytes,
		validatedChunks,
	)
}
