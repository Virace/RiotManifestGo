package core

import (
	"fmt"
	"sort"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// Map 执行全局 Bundle 聚合：将 []FileEntry 转换为按 BundleID 分组的 GlobalChunkTask 映射。
//
// 核心特性：
//   - ChunkID 全局去重：相同 ChunkID 只保留一个 Task，多个 WriteTarget 合并
//   - 按 BundleID 分组
//   - 每组内按 BundleOffset 升序排列
//
// 与逐文件分组不同，本函数对所有 FileEntry 处理完毕后一次性全局聚合，
// 实现跨文件的 Chunk 去重与合并。
func Map(files []rman.FileEntry) map[uint64][]GlobalChunkTask {
	// ChunkID → GlobalChunkTask（全局唯一）
	chunkIndex := make(map[uint64]*GlobalChunkTask)

	for _, file := range files {
		fileOffset := int64(0)
		for _, chunk := range file.Chunks {
			target := WriteTarget{
				FilePath:    file.Path,
				FileOffset:  fileOffset,
				ExpectedLen: chunk.UncompressedSize,
				ChunkID:     chunk.ChunkID,
				HashType:    chunk.HashType,
			}

			if existing, ok := chunkIndex[chunk.ChunkID]; ok {
				existing.Targets = append(existing.Targets, target)
			} else {
				chunkIndex[chunk.ChunkID] = &GlobalChunkTask{
					BundleID:       chunk.BundleID,
					BundleOffset:   chunk.BundleOffset,
					CompressedSize: chunk.CompressedSize,
					Targets:        []WriteTarget{target},
				}
			}

			fileOffset += int64(chunk.UncompressedSize)
		}
	}

	// 按 BundleID 分组
	bundleMap := make(map[uint64][]GlobalChunkTask)
	for _, task := range chunkIndex {
		bundleMap[task.BundleID] = append(bundleMap[task.BundleID], *task)
	}

	// 每组内按 BundleOffset 升序排列
	for bundleID := range bundleMap {
		sort.Slice(bundleMap[bundleID], func(i, j int) bool {
			return bundleMap[bundleID][i].BundleOffset < bundleMap[bundleID][j].BundleOffset
		})
	}

	return bundleMap
}

// BundleFilename 返回 Bundle ID 对应的文件名（%016X.bundle）。
func BundleFilename(bundleID uint64) string {
	return fmt.Sprintf("%016X.bundle", bundleID)
}
