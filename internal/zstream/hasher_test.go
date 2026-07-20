package zstream

import (
	"testing"

	"github.com/Virace/RiotManifestGo/pkg/rman"
)

// ---- HKDF 测试 ----

// TestHKDFHash_Deterministic 验证 HKDF 算法对同一输入产出稳定结果。
func TestHKDFHash_Deterministic(t *testing.T) {
	data := []byte("hello world")
	h1 := HKDFHash(data)
	h2 := HKDFHash(data)
	if h1 != h2 {
		t.Errorf("HKDF 不确定：%016X != %016X", h1, h2)
	}
	if h1 == 0 {
		t.Error("HKDF 对非空输入不应返回 0")
	}
	t.Logf("HKDFHash(\"hello world\") = %016X", h1)
}

// TestHKDFHash_DifferentInputs 验证不同输入产出不同哈希。
func TestHKDFHash_DifferentInputs(t *testing.T) {
	h1 := HKDFHash([]byte("aaa"))
	h2 := HKDFHash([]byte("bbb"))
	if h1 == h2 {
		t.Errorf("不同输入不应产出相同哈希：%016X", h1)
	}
}

// TestHKDFHash_EmptyInput 验证空切片不 panic 并返回固定值。
func TestHKDFHash_EmptyInput(t *testing.T) {
	h := HKDFHash([]byte{})
	t.Logf("HKDFHash(\"\") = %016X", h)
	// 只要不 panic 即可
}

// ---- Hasher 测试 ----

// TestComputeHash_SHA256 验证 SHA256 哈希计算与 ValidateChunk 的一致性。
func TestComputeHash_SHA256(t *testing.T) {
	data := []byte("test data for sha256")
	hash := ComputeHash(data, rman.HashTypeSHA256)
	if hash == 0 {
		t.Error("SHA256 对非空输入不应返回 0")
	}

	// 使用计算出的 hash 作为 chunkID，ValidateChunk 应返回 true
	if !ValidateChunk(data, hash, rman.HashTypeSHA256) {
		t.Error("ValidateChunk 应返回 true（自校验）")
	}

	// 篡改数据后应返回 false
	tampered := make([]byte, len(data))
	copy(tampered, data)
	tampered[0] ^= 0xFF
	if ValidateChunk(tampered, hash, rman.HashTypeSHA256) {
		t.Error("篡改数据后 ValidateChunk 应返回 false")
	}
}

// TestComputeHash_Blake3 验证 Blake3 哈希计算。
func TestComputeHash_Blake3(t *testing.T) {
	data := []byte("test data for blake3")
	hash := ComputeHash(data, rman.HashTypeBlake3)
	if hash == 0 {
		t.Error("Blake3 对非空输入不应返回 0")
	}

	if !ValidateChunk(data, hash, rman.HashTypeBlake3) {
		t.Error("ValidateChunk(Blake3) 应返回 true（自校验）")
	}
}

// TestComputeHash_HKDF 验证 HKDF 哈希计算与校验。
func TestComputeHash_HKDF(t *testing.T) {
	data := []byte("test data for hkdf")
	hash := ComputeHash(data, rman.HashTypeHKDF)
	if hash == 0 {
		t.Error("HKDF 对非空输入不应返回 0")
	}

	if !ValidateChunk(data, hash, rman.HashTypeHKDF) {
		t.Error("ValidateChunk(HKDF) 应返回 true（自校验）")
	}
}

// TestValidateChunk_HashTypeNoneSkips 验证 HashTypeNone 时跳过哈希校验直接放行：
// 无 params 条目的 chunk 无法确定哈希算法，不做哈希校验，
// 完整性由 ZSTD 解压成功 + 解压大小精确匹配兜底。
func TestValidateChunk_HashTypeNoneSkips(t *testing.T) {
	data := []byte("chunk without params entry")
	arbitraryID := uint64(0x1234567890ABCDEF) // 与任何算法结果都不匹配的任意 ID

	if !ValidateChunk(data, arbitraryID, rman.HashTypeNone) {
		t.Error("HashTypeNone 时 ValidateChunk 应跳过校验返回 true")
	}
	if err := ValidateChunkError(data, arbitraryID, rman.HashTypeNone); err != nil {
		t.Errorf("HashTypeNone 时 ValidateChunkError 应返回 nil，got %v", err)
	}
}

// TestValidateChunkError 验证 ValidateChunkError 返回详细错误信息。
func TestValidateChunkError(t *testing.T) {
	data := []byte("test")
	wrongID := uint64(0xDEADDEADDEADDEAD)

	err := ValidateChunkError(data, wrongID, rman.HashTypeSHA256)
	if err == nil {
		t.Fatal("错误的 ChunkID 应返回 error")
	}
	t.Logf("期望的错误信息: %s", err)
}
