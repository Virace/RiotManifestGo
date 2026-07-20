package core

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// OutputPath resolves a manifest file path under outputDir and rejects
// absolute paths, parent-directory traversal, Windows drive/ADS syntax, and
// empty path elements that could escape or alias the output root on different
// platforms.
//
// Exported so pkg/update.Sync can resolve the same outputDir-relative target
// paths as the downloader (staging/verify/moved/removed all need the exact
// same path-safety rules); keeping one implementation avoids two independently
// maintained copies of security-relevant path validation.
func OutputPath(outputDir, manifestPath string) (string, error) {
	if manifestPath == "" {
		return "", fmt.Errorf("manifest path is empty")
	}

	normalized := strings.ReplaceAll(manifestPath, "\\", "/")
	if path.IsAbs(normalized) {
		return "", fmt.Errorf("manifest path %q is absolute", manifestPath)
	}

	for _, part := range strings.Split(normalized, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("manifest path %q contains unsafe segment %q", manifestPath, part)
		}
		if strings.Contains(part, ":") {
			return "", fmt.Errorf("manifest path %q contains unsafe ':' in segment %q", manifestPath, part)
		}
	}

	cleaned := path.Clean(normalized)
	if cleaned == "." || strings.HasPrefix(cleaned, "../") || cleaned == ".." {
		return "", fmt.Errorf("manifest path %q escapes output directory", manifestPath)
	}

	return filepath.Join(outputDir, filepath.FromSlash(cleaned)), nil
}
