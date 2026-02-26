package core

import "context"

// BundleFetcher 是下载层的 HTTP 获取抽象接口。
//
// Downloader 通过此接口与网络层解耦，便于测试时注入 mock 实现。
// 生产环境由 netpool.BundleClient 实现。
type BundleFetcher interface {
	// FetchRanges 从 CDN 获取指定 Bundle 文件的多个字节范围。
	// 返回的 [][]byte 与 ranges 一一对应。
	FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange) ([][]byte, error)

	// Close 关闭底层连接资源。
	Close()
}

// ByteRange 表示一个 HTTP Range 的字节范围（inclusive）。
type ByteRange struct {
	Start int64
	End   int64
}
