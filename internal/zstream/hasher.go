package zstream

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/Virace/RiotManifestGo/pkg/rman"
	"lukechampine.com/blake3"
)

// ValidateChunk 校验解压后的 Chunk 数据哈希是否匹配预期的 ChunkID。
//
// ChunkID 实质上是数据哈希值的前 8 字节（uint64 小端）。
// 返回 true 表示校验通过。
//
// HashTypeNone（该文件无有效 params 条目，实测 16.3 清单约 81% chunk 如此）
// 直接放行不做哈希校验：下载路径的完整性由 ZSTD 解压成功 + 解压大小精确匹配兜底。
// 该契约与 PyManifest validate_chunk_hash（hash_type=0 → 跳过）一致，已经其
// 16.3→16.4 真实网络 E2E 验证；本地复用验证（pkg/update.VerifyFileChunks）
// 则相反地对 HashTypeNone 做穷举猜测，两侧分工见 update spec §3 经验 3。
func ValidateChunk(data []byte, chunkID uint64, hashType rman.HashType) bool {
	if hashType == rman.HashTypeNone {
		return true
	}
	return ComputeHash(data, hashType) == chunkID
}

// ComputeHash 根据指定的 hashType 计算数据哈希值（uint64）。
//
// 所有算法的最终产物都是 uint64：取摘要的前 8 字节按小端解释。
func ComputeHash(data []byte, hashType rman.HashType) uint64 {
	switch hashType {
	case rman.HashTypeSHA256:
		return hashSHA256(data)
	case rman.HashTypeSHA512:
		return hashSHA512(data)
	case rman.HashTypeHKDF:
		return HKDFHash(data)
	case rman.HashTypeBlake3:
		return hashBlake3(data)
	default:
		return 0
	}
}

// hashSHA256 计算 SHA-256 哈希，取前 8 字节作为 uint64 LE。
func hashSHA256(data []byte) uint64 {
	h := sha256.Sum256(data)
	return binary.LittleEndian.Uint64(h[:8])
}

// hashSHA512 计算 SHA-512 哈希，取前 8 字节作为 uint64 LE。
func hashSHA512(data []byte) uint64 {
	h := sha512Compute(data)
	return binary.LittleEndian.Uint64(h[:8])
}

// hashBlake3 计算 Blake3 哈希，取前 8 字节作为 uint64 LE。
func hashBlake3(data []byte) uint64 {
	h := blake3.Sum256(data)
	return binary.LittleEndian.Uint64(h[:8])
}

// ValidateChunkError 与 ValidateChunk 相同，但返回详细错误信息。
// HashTypeNone 同样直接放行（见 ValidateChunk 注释）。
func ValidateChunkError(data []byte, chunkID uint64, hashType rman.HashType) error {
	if hashType == rman.HashTypeNone {
		return nil
	}
	computed := ComputeHash(data, hashType)
	if computed != chunkID {
		return fmt.Errorf("哈希校验失败 [%s]: 计算值=%016X, 期望值=%016X",
			hashType.String(), computed, chunkID)
	}
	return nil
}
