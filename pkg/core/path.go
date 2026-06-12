package core

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// outputPath resolves a manifest file path under outputDir and rejects
// absolute paths, parent-directory traversal, Windows drive/ADS syntax, and
// empty path elements that could escape or alias the output root on different
// platforms.
func outputPath(outputDir, manifestPath string) (string, error) {
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
