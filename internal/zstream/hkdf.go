// Package zstream 提供 ZSTD 流式解压与哈希校验管线。
//
// 支持的哈希算法：SHA256、HKDF（32轮 HMAC-SHA256 PRF 展开）、Blake3。
//
// 参考实现: https://github.com/Morilli/ManifestDownloader/blob/master/rman.c
package zstream

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// HKDFHash 实现 RMAN 清单中使用的 HKDF 哈希算法。
//
// 算法逻辑：
//  1. PRK = SHA256(data)
//  2. 将 PRK 作为 HMAC-SHA256 的密钥
//  3. buffer = HMAC-SHA256(PRK, "\x00\x00\x00\x01")
//  4. result = LE.Uint64(buffer[:8])
//  5. 循环31次：buffer = HMAC-SHA256(PRK, buffer); result ^= LE.Uint64(buffer[:8])
//  6. return result
//
// 本质上是 HMAC-PRF 32轮展开，取每轮前8字节 XOR 累积。
func HKDFHash(data []byte) uint64 {
	prk := sha256.Sum256(data)
	mac := hmac.New(sha256.New, prk[:])

	mac.Write([]byte{0x00, 0x00, 0x00, 0x01})
	buffer := mac.Sum(nil)
	result := binary.LittleEndian.Uint64(buffer[:8])

	for i := 0; i < 31; i++ {
		mac.Reset()
		mac.Write(buffer)
		buffer = mac.Sum(nil)
		result ^= binary.LittleEndian.Uint64(buffer[:8])
	}

	return result
}
