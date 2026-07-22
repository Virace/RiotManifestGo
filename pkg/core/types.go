// Package core 是 RiotManifestGo 的调度核心层（Mapper + Scheduler + Downloader）。
//
//	[]rman.FileEntry → Filter → []rman.FileEntry → Map → Schedule → Download
package core

import "github.com/Virace/RiotManifestGo/pkg/rman"

// WriteTarget 描述一次 Chunk 数据写入操作的目标。
// 同一个 Chunk 的解压数据可能需要写入多个目标（扇出写入）。
type WriteTarget struct {
	FilePath    string
	FileOffset  int64
	ExpectedLen uint32
	ChunkID     uint64
	HashType    rman.HashType
}

// GlobalChunkTask 是全局调度池的最小原子任务。
//
// 核心不变式：同一 ChunkID 在全局中只出现一次。
// 相同 ChunkID 被多个文件引用时，多个 WriteTarget 合并到同一个 Task 的 Targets 中。
type GlobalChunkTask struct {
	BundleID       uint64
	BundleOffset   uint32
	CompressedSize uint32
	Targets        []WriteTarget
}

// ChunkRange 是一次 HTTP Range 请求的覆盖段，包含多个连续（或已合并）的 Chunk。
type ChunkRange struct {
	Start  uint32 // Bundle 内字节起始（inclusive）
	End    uint32 // Bundle 内字节结束（inclusive）
	Chunks []GlobalChunkTask
}

// BundleJob 是派发给下载 Worker 的一个 Bundle 下载任务。
type BundleJob struct {
	BundleID       uint64
	BundleFilename string
	Ranges         []ChunkRange
	FullBundle     bool   // true=整包作业：不发 Range 头的普通 GET，响应走 200 全体切片
	BundleSize     uint32 // FullBundle 作业的整包字节数（动态超时与实际流量统计用）；非整包为 0
}
