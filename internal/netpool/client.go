// Package netpool 提供面向 Riot CDN 的 HTTP 客户端，支持 Range 请求与 multipart 响应解析。
//
// 参考实现: https://github.com/Morilli/ManifestDownloader/blob/master/download.c
package netpool

import (
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"strings"
	"time"
)

// BundleClient 是面向 Riot CDN Bundle 文件的 HTTP 客户端。
//
// 特性：
//   - 带 Keep-Alive 的长连接池
//   - 支持单段和多段 Range 请求
//   - 自动解析 multipart/byteranges 响应
//   - 动态超时（基于请求数据量）
type BundleClient struct {
	httpClient *http.Client
	baseURL    string
}

// NewBundleClient 创建一个新的 Bundle HTTP 客户端。
//
// baseURL 格式示例："https://lol.dyn.riotcdn.net/channels/public/bundles"
// workers 指定并发 Worker 数量，用于配置连接池大小。
func NewBundleClient(baseURL string, workers int) *BundleClient {
	baseURL = strings.TrimRight(baseURL, "/")

	transport := &http.Transport{
		MaxIdleConns:        workers * 2,
		MaxIdleConnsPerHost: workers * 2,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		// Bundle 数据已是 ZSTD 压缩，禁用 HTTP 层压缩避免双重压缩
		DisableCompression: true,
	}

	return &BundleClient{
		httpClient: &http.Client{
			Transport: transport,
			// 不设全局超时，改用 per-request context 动态超时
		},
		baseURL: baseURL,
	}
}

// ByteRange 表示一个 HTTP Range 的字节范围（inclusive）。
type ByteRange struct {
	Start int64
	End   int64
}

// FetchRanges 从 CDN 获取指定 Bundle 的多个字节范围。
//
// 动态超时策略：
//   - 基础超时 30s（连接建立 + TLS 握手）
//   - 数据量因子：totalBytes / 50KB（假设最低 50KB/s）
//   - 上限 10 分钟
//
// 返回的 [][]byte 与 ranges 一一对应。
func (c *BundleClient) FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange) ([][]byte, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	url := c.baseURL + "/" + bundleFilename
	rangeHeader := buildRangeHeader(ranges)

	// 计算动态超时
	var totalBytes int64
	for _, r := range ranges {
		totalBytes += r.End - r.Start + 1
	}
	timeout := dynamicTimeout(totalBytes)

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}
	req.Header.Set("Range", rangeHeader)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP 请求失败 (%s): %w", bundleFilename, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusPartialContent: // 206
		return c.parsePartialContent(resp, len(ranges))
	case resp.StatusCode == http.StatusOK: // 200
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("读取完整响应失败: %w", err)
		}
		return extractFromFullBody(body, ranges)
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable: // 416
		return nil, fmt.Errorf("Range 不满足 (%s): 服务器返回 416", bundleFilename)
	default:
		return nil, fmt.Errorf("HTTP 响应异常 (%s): status=%d", bundleFilename, resp.StatusCode)
	}
}

// Close 关闭 HTTP 客户端的空闲连接。
func (c *BundleClient) Close() {
	c.httpClient.CloseIdleConnections()
}

// dynamicTimeout 根据请求数据量计算动态超时。
func dynamicTimeout(totalBytes int64) time.Duration {
	const (
		baseTimeout = 30 * time.Second
		minSpeed    = 50 * 1024 // 50 KB/s 最低速度假设
		maxTimeout  = 10 * time.Minute
	)
	sizeFactor := time.Duration(totalBytes/int64(minSpeed)) * time.Second
	timeout := baseTimeout + sizeFactor
	if timeout > maxTimeout {
		timeout = maxTimeout
	}
	return timeout
}

func buildRangeHeader(ranges []ByteRange) string {
	parts := make([]string, len(ranges))
	for i, r := range ranges {
		parts[i] = fmt.Sprintf("%d-%d", r.Start, r.End)
	}
	return "bytes=" + strings.Join(parts, ", ")
}

func (c *BundleClient) parsePartialContent(resp *http.Response, expectedCount int) ([][]byte, error) {
	contentType := resp.Header.Get("Content-Type")

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err == nil && strings.HasPrefix(mediaType, "multipart/") {
		return parseMultipartResponse(resp.Body, params["boundary"], expectedCount)
	}

	if expectedCount != 1 {
		return nil, fmt.Errorf("单段 Range 响应数量不匹配: 收到=1, 期望=%d", expectedCount)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取单段响应失败: %w", err)
	}
	return [][]byte{data}, nil
}

// parseMultipartResponse 解析 multipart/byteranges 响应。
func parseMultipartResponse(body io.Reader, boundary string, expectedCount int) ([][]byte, error) {
	reader := multipart.NewReader(body, boundary)
	results := make([][]byte, 0, expectedCount)

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取 multipart 段失败: %w", err)
		}

		data, err := io.ReadAll(part)
		if err != nil {
			return nil, fmt.Errorf("读取 multipart 段数据失败: %w", err)
		}
		results = append(results, data)
	}

	if len(results) != expectedCount {
		return nil, fmt.Errorf("multipart 段数不匹配: 收到=%d, 期望=%d", len(results), expectedCount)
	}
	return results, nil
}

func extractFromFullBody(body []byte, ranges []ByteRange) ([][]byte, error) {
	results := make([][]byte, len(ranges))
	for i, r := range ranges {
		if int(r.End) >= len(body) || r.Start < 0 {
			return nil, fmt.Errorf("Range [%d-%d] 超出文件大小 %d", r.Start, r.End, len(body))
		}
		results[i] = body[r.Start : r.End+1]
	}
	return results, nil
}
