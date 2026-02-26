package zstream

import (
	"fmt"
	"sync"

	"github.com/Virace/RiotManifestGo/pkg/rman"
	"github.com/klauspost/compress/zstd"
)

// Decoder 是可复用的 ZSTD 解码器，支持逐 Chunk 解压与哈希校验。
//
// 内部使用 sync.Pool 管理多个 decoder 实例，复用内部缓冲，并发安全。
type Decoder struct {
	pool sync.Pool
}

// NewDecoder 创建一个新的可复用 ZSTD 解码器。
func NewDecoder() *Decoder {
	return &Decoder{
		pool: sync.Pool{
			New: func() interface{} {
				d, _ := zstd.NewReader(nil,
					zstd.WithDecoderConcurrency(1),
					zstd.WithDecoderMaxMemory(64<<20),
				)
				return d
			},
		},
	}
}

// DecompressChunk 解压一个 ZSTD 压缩的 Chunk 数据块。
func (d *Decoder) DecompressChunk(compressed []byte) ([]byte, error) {
	dec := d.pool.Get().(*zstd.Decoder)
	defer d.pool.Put(dec)

	decompressed, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("ZSTD 解压失败: %w", err)
	}
	return decompressed, nil
}

// DecompressAndValidate 执行 ZSTD 解压 + 哈希校验一体化操作。
//
// 流水线：ZSTD 解压 → 大小校验 → Hash 校验。
func (d *Decoder) DecompressAndValidate(
	compressed []byte,
	expectedSize uint32,
	chunkID uint64,
	hashType rman.HashType,
) ([]byte, error) {
	decompressed, err := d.DecompressChunk(compressed)
	if err != nil {
		return nil, err
	}

	if uint32(len(decompressed)) != expectedSize {
		return nil, fmt.Errorf("解压大小不匹配: 实际=%d, 期望=%d", len(decompressed), expectedSize)
	}

	if err := ValidateChunkError(decompressed, chunkID, hashType); err != nil {
		return nil, err
	}

	return decompressed, nil
}
