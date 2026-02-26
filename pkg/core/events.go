package core

import "time"

// DownloadEvent 是下载管线发出的事件接口。
//
// pkg/core 内只通过 chan DownloadEvent 抛出事件，
// 严禁任何 fmt.Printf 或终端操作，保证与 CLI / GUI 完全解耦。
type DownloadEvent interface {
	eventMarker()
}

type eventBase struct{}

func (eventBase) eventMarker() {}

// EventFilePreallocated 在文件预分配完成时发出。
type EventFilePreallocated struct {
	eventBase
	Path string
	Size int64
}

// EventBundleStart 在开始下载一个 Bundle 时发出。
type EventBundleStart struct {
	eventBase
	BundleID       uint64
	BundleFilename string
	RangeCount     int
	ChunkCount     int
}

// EventChunkDone 在一个 Chunk 成功解压、校验并写盘后发出。
type EventChunkDone struct {
	eventBase
	ChunkID        uint64
	BundleID       uint64
	CompressedSize uint32
	TargetCount    int
}

// EventBundleDone 在一个 Bundle 的所有 Chunk 处理完成时发出。
type EventBundleDone struct {
	eventBase
	BundleID uint64
}

// EventRetry 在 BundleJob 失败后重试时发出。
type EventRetry struct {
	eventBase
	BundleID       uint64
	BundleFilename string
	Attempt        int
	MaxRetries     int
	Err            error
}

// EventError 在处理 Bundle 出错时发出（已耗尽重试次数）。
type EventError struct {
	eventBase
	BundleID       uint64
	BundleFilename string
	Err            error
}

// EventComplete 在所有 BundleJob 处理完毕时发出。
type EventComplete struct {
	eventBase
	TotalFiles    int
	TotalChunks   int
	TotalBytes    int64
	FailedBundles int
	Elapsed       time.Duration
}
