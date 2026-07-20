package zstream

import (
	"fmt"
	"testing"

	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/klauspost/compress/zstd"
)

// TestDecoder_DecompressChunk 验证基本 ZSTD 解压功能。
func TestDecoder_DecompressChunk(t *testing.T) {
	// 准备：用 ZSTD 压缩一段已知数据
	original := []byte("Hello, this is test data for ZSTD decompression. 测试中文数据。")
	compressed := compressTestData(t, original)

	d := NewDecoder()
	result, err := d.DecompressChunk(compressed)
	if err != nil {
		t.Fatalf("DecompressChunk 失败: %v", err)
	}

	if string(result) != string(original) {
		t.Errorf("解压结果不匹配: got %q, want %q", result, original)
	}
}

// TestDecoder_DecompressAndValidate 验证解压+哈希校验一体化。
func TestDecoder_DecompressAndValidate(t *testing.T) {
	original := []byte("chunk data for hash validation test")
	compressed := compressTestData(t, original)

	d := NewDecoder()

	// 计算正确的哈希值
	correctHash := ComputeHash(original, rman.HashTypeBlake3)

	result, err := d.DecompressAndValidate(
		compressed,
		uint32(len(original)),
		correctHash,
		rman.HashTypeBlake3,
	)
	if err != nil {
		t.Fatalf("DecompressAndValidate 失败: %v", err)
	}

	if string(result) != string(original) {
		t.Errorf("解压结果不匹配")
	}
}

// TestDecoder_DecompressAndValidate_WrongHash 验证哈希不匹配时返回错误。
func TestDecoder_DecompressAndValidate_WrongHash(t *testing.T) {
	original := []byte("data with wrong hash")
	compressed := compressTestData(t, original)

	d := NewDecoder()

	_, err := d.DecompressAndValidate(
		compressed,
		uint32(len(original)),
		0xDEADBEEF, // 错误的 hash
		rman.HashTypeBlake3,
	)
	if err == nil {
		t.Fatal("哈希不匹配时应返回 error")
	}
	t.Logf("期望的错误: %s", err)
}

// TestDecoder_DecompressAndValidate_WrongSize 验证解压大小不匹配时返回错误。
func TestDecoder_DecompressAndValidate_WrongSize(t *testing.T) {
	original := []byte("size mismatch test")
	compressed := compressTestData(t, original)

	d := NewDecoder()
	correctHash := ComputeHash(original, rman.HashTypeSHA256)

	_, err := d.DecompressAndValidate(
		compressed,
		uint32(len(original)+999), // 故意传入错误的大小
		correctHash,
		rman.HashTypeSHA256,
	)
	if err == nil {
		t.Fatal("解压大小不匹配时应返回 error")
	}
}

// TestDecoder_DecompressAndValidate_HashTypeNone 验证 HashTypeNone 的 chunk
// 解压 + 大小校验通过后即接受（跳过哈希校验），但解压大小不匹配仍然报错。
func TestDecoder_DecompressAndValidate_HashTypeNone(t *testing.T) {
	original := []byte("chunk data without params entry")
	compressed := compressTestData(t, original)

	d := NewDecoder()

	result, err := d.DecompressAndValidate(
		compressed,
		uint32(len(original)),
		0xDEADBEEF, // 任意 ChunkID：HashTypeNone 下不参与校验
		rman.HashTypeNone,
	)
	if err != nil {
		t.Fatalf("HashTypeNone 时 DecompressAndValidate 应通过: %v", err)
	}
	if string(result) != string(original) {
		t.Errorf("解压结果不匹配")
	}

	// 大小校验不因 HashTypeNone 而放松
	if _, err := d.DecompressAndValidate(
		compressed,
		uint32(len(original)+1),
		0xDEADBEEF,
		rman.HashTypeNone,
	); err == nil {
		t.Fatal("HashTypeNone 下解压大小不匹配仍应返回 error")
	}
}

// TestDecoder_ConcurrentSafe 验证 Decoder 的并发安全性（sync.Pool）。
func TestDecoder_ConcurrentSafe(t *testing.T) {
	d := NewDecoder()
	data := []byte("concurrent test data")
	compressed := compressTestData(t, data)

	const goroutines = 16
	errCh := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			result, err := d.DecompressChunk(compressed)
			if err != nil {
				errCh <- err
				return
			}
			if string(result) != string(data) {
				errCh <- fmt.Errorf("解压结果不匹配")
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

// compressTestData 使用 ZSTD 压缩测试数据。
func compressTestData(t *testing.T, data []byte) []byte {
	t.Helper()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("创建 ZSTD 编码器失败: %v", err)
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil)
}
