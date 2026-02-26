package zstream

import (
	"crypto/sha512"
)

// sha512Compute 计算 SHA-512 摘要。
// 独立文件以保持 hasher.go 的 import 干净。
func sha512Compute(data []byte) [64]byte {
	return sha512.Sum512(data)
}
