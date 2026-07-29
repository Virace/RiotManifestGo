package core

import "context"

// FetchOptions 控制单次 FetchRanges 调用的域名选取与整包模式。
//
// core 层的镜像类型：BundleFetcher 的实现（如 netBundleFetcherAdapter）负责把它
// 转换为具体网络层（netpool）的等价类型，netpool 的实现细节不通过本接口外泄。
type FetchOptions struct {
	// URLHint 统一取 BundleID + attempt（attempt 从 0 起，uint64 溢出回绕可接受），
	// 实现方按 URLHint % len(候选域名) 选取本次请求使用的域名；重试时 attempt
	// 递增即可让相邻两次尝试落在不同域名上。
	URLHint uint64

	// FullBundleSize 大于 0 时启用整包 GET 模式（不发 Range 头，请求 Bundle 全体
	// 内容后按 ranges 从响应体切片）；等于 0 时走既有 Range 请求路径。
	FullBundleSize int64
}

// BundleFetcher 是下载层的 HTTP 获取抽象接口。
//
// Downloader 通过此接口与网络层解耦，便于测试时注入 mock 实现。
// 生产环境由 netpool.BundleClient 实现。
type BundleFetcher interface {
	// FetchRanges 从 CDN 获取指定 Bundle 文件的多个字节范围。
	// 返回的 [][]byte 与 ranges 一一对应。
	FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange, opts FetchOptions) ([][]byte, error)

	// Close 关闭底层连接资源。
	Close()
}

// ByteRange 表示一个 HTTP Range 的字节范围（inclusive）。
type ByteRange struct {
	Start int64
	End   int64
}
