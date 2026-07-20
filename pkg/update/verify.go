package update

import (
	"fmt"
	"os"

	"github.com/Virace/RiotManifestGo/internal/zstream"
	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// ChunkRef 引用 rman.FileEntry.Chunks 中的一个 Chunk 及其在文件解压域中的固定偏移。
type ChunkRef struct {
	Chunk rman.ChunkInfo
	// FileOffset 文件内解压域偏移：按 Chunks 顺序累加各 Chunk 的 UncompressedSize 得出，
	// 与 pkg/core.Map 中 fileOffset 的推算语义一致。
	FileOffset int64
}

// FileVerifyResult 是 VerifyFileChunks 对单个文件本地 chunk 级验证的结果。
type FileVerifyResult struct {
	Entry rman.FileEntry
	// Exists 表示 VerifyFileChunks 传入的本地路径是否存在。
	// 为 false 时 Hits 必为空，Misses 覆盖 Entry 的全部 Chunk。
	Exists bool
	// Hits 是固定位置校验命中（本地数据完好、可直接复用）的 Chunk。
	Hits []ChunkRef
	// Misses 是越界或哈希不匹配（需要重新下载）的 Chunk。
	Misses []ChunkRef
}

// VerifyFileChunks 按固定位置校验本地文件 path 相对于 manifest 中 entry 的每个 Chunk
// 是否完好：偏移量按 entry.Chunks 顺序累加 UncompressedSize 推算（与 core.Map 同语义），
// 依次比较 [FileOffset, FileOffset+UncompressedSize) 区间的磁盘数据与 Chunk 的哈希是否匹配。
// 越界或哈希不匹配的 Chunk 归入 Misses，供上层编排逻辑仅下载缺失部分、其余直接本地复用。
//
// path 以只读方式打开（os.Open），不经 FilePool：验证读取与下载写盘生命周期无关，
// 无需共享句柄池管理。path 不存在时不视为 error，而是返回 Exists=false、全部 Chunk 落入
// Misses（本地文件缺失，全部需要下载）。
func VerifyFileChunks(entry rman.FileEntry, path string) (FileVerifyResult, error) {
	result := FileVerifyResult{Entry: entry}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			result.Misses = allMisses(entry)
			return result, nil
		}
		return FileVerifyResult{}, fmt.Errorf("打开本地文件失败: %w", err)
	}
	defer f.Close()

	result.Exists = true

	info, err := f.Stat()
	if err != nil {
		return FileVerifyResult{}, fmt.Errorf("获取本地文件信息失败: %w", err)
	}
	fileSize := info.Size()

	var off int64
	for _, c := range entry.Chunks {
		ref := ChunkRef{Chunk: c, FileOffset: off}
		off += int64(c.UncompressedSize)

		if ref.FileOffset+int64(c.UncompressedSize) > fileSize {
			result.Misses = append(result.Misses, ref)
			continue
		}

		buf := make([]byte, c.UncompressedSize)
		if _, err := f.ReadAt(buf, ref.FileOffset); err != nil {
			result.Misses = append(result.Misses, ref)
			continue
		}

		if matchChunk(buf, c) {
			result.Hits = append(result.Hits, ref)
		} else {
			result.Misses = append(result.Misses, ref)
		}
	}

	return result, nil
}

// allMisses 返回 entry 全部 Chunk 对应的 ChunkRef 列表（本地文件不存在时使用），
// 偏移推算方式与 VerifyFileChunks 主循环一致。
func allMisses(entry rman.FileEntry) []ChunkRef {
	if len(entry.Chunks) == 0 {
		return nil
	}
	misses := make([]ChunkRef, 0, len(entry.Chunks))
	var off int64
	for _, c := range entry.Chunks {
		misses = append(misses, ChunkRef{Chunk: c, FileOffset: off})
		off += int64(c.UncompressedSize)
	}
	return misses
}

// matchChunk 判断 data 是否与 c 描述的 Chunk 匹配：已知哈希类型直接比对；
// HashTypeNone（param_index 越界或对应 Parameters 条目 hash_type 为 0/缺省）时
// 穷举猜测 HKDF → Blake3 → SHA256 → SHA512，任一算法命中即视为匹配，
// 对齐 rman RChunk::hash_type 的猜测策略。
//
// 无 params 条目的 Chunk 其 ChunkID 仍是内容哈希；若一律判 miss，本地完好
// 数据会被无谓地重新下载。
func matchChunk(data []byte, c rman.ChunkInfo) bool {
	if c.HashType != rman.HashTypeNone {
		return zstream.ComputeHash(data, c.HashType) == c.ChunkID
	}
	for _, ht := range []rman.HashType{rman.HashTypeHKDF, rman.HashTypeBlake3, rman.HashTypeSHA256, rman.HashTypeSHA512} {
		if zstream.ComputeHash(data, ht) == c.ChunkID {
			return true
		}
	}
	return false
}
