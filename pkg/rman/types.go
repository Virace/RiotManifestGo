// Package rman 实现了拳头游戏 RMAN（.manifest）文件的解析器。
//
// 参考实现: https://github.com/Morilli/ManifestDownloader/blob/master/rman.c
package rman

// HashType 表示 Chunk 哈希校验算法类型。
type HashType uint8

const (
	// HashTypeNone 无哈希类型（HashType 字段零值）。
	// 出现于该文件的 param_index 越界（未指向有效 Parameters 条目），或对应
	// Parameters 条目自身的 hash_type 字段为 0/缺省时（见 Parser.parseFileEntries
	// 与 parseParameters），此时无法直接判断 ChunkID 对应的哈希算法，
	// 需要由消费方穷举猜测（参考 rman RChunk::hash_type 的猜测策略）。
	HashTypeNone HashType = 0
	// HashTypeSHA512 SHA-512 哈希（极少使用）
	HashTypeSHA512 HashType = 1
	// HashTypeSHA256 SHA-256 哈希
	HashTypeSHA256 HashType = 2
	// HashTypeHKDF HMAC-PRF 32轮展开，取前 8 字节
	HashTypeHKDF HashType = 3
	// HashTypeBlake3 Blake3 哈希
	HashTypeBlake3 HashType = 4
)

// String 返回 HashType 的可读字符串。
func (h HashType) String() string {
	switch h {
	case HashTypeNone:
		return "None"
	case HashTypeSHA512:
		return "SHA512"
	case HashTypeSHA256:
		return "SHA256"
	case HashTypeHKDF:
		return "HKDF"
	case HashTypeBlake3:
		return "Blake3"
	default:
		return "Unknown"
	}
}

// ChunkInfo 是单个 Chunk 的完整信息，经解析层还原后的产物。
//
// 注意：BundleOffset 不存储在 FlatBuffers 中，由 Parser 在遍历同一 Bundle 的
// Chunk 时累加 CompressedSize 自行推算。
type ChunkInfo struct {
	ChunkID          uint64
	BundleID         uint64
	BundleOffset     uint32 // 由 Parser 累加 CompressedSize 推算，非 FlatBuffers 原生字段
	CompressedSize   uint32
	UncompressedSize uint32
	HashType         HashType // 来自对应 FileEntry.param_index 所指向的 Parameters 对象
}

// FileEntry 表示 Manifest 中的一个游戏文件。
type FileEntry struct {
	// Path 是经目录树重建后的完整相对路径（如 "RADS/solutions/..."）
	Path string
	// Link 可选，RMAN v2.1 后已废弃
	Link string
	// FileSize 文件字节大小
	FileSize uint64
	// Flags 该文件的标记列表（由 flag_mask 位掩码解码）。
	// 多数情况下为语言/区域代码（如 "de_DE"、"zh_CN"），
	// 也可能是平台标识（如 "windows"、"macos"）或其他自定义标记。
	// 对应 CDTB PatcherFile.flags 字段。
	Flags []string
	// Chunks 按文件内偏移顺序排列的 Chunk 列表
	Chunks []ChunkInfo
}

// Manifest 是 RMAN 文件解析的根输出对象。
type Manifest struct {
	// ManifestID 来自文件头第 16-24 字节
	ManifestID uint64
	// Files 该 Manifest 包含的所有游戏文件列表
	Files []FileEntry
}

// Header 是 RMAN 文件头的解析结果，供内部使用。
type Header struct {
	MajorVersion     uint8
	MinorVersion     uint8
	ContentOffset    uint32
	CompressedSize   uint32
	ManifestID       uint64
	UncompressedSize uint32
}

// rmanMagic RMAN 文件的魔数（ASCII "RMAN"）
var rmanMagic = [4]byte{'R', 'M', 'A', 'N'}

// parameters 是 FlatBuffers Body 中 Parameters 对象的内部表示。
type parameters struct {
	HashType        HashType
	MinChunkSize    uint32
	MaxChunkSize    uint32
	MaxUncompressed uint32
}

// dirEntry 是目录对象的内部表示，用于路径树重建。
type dirEntry struct {
	Name     string
	ParentID uint64
}
