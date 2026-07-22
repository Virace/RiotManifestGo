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
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// BundleClient 是面向 Riot CDN Bundle 文件的 HTTP 客户端。
//
// 特性：
//   - 带 Keep-Alive 的长连接池
//   - 支持单段和多段 Range 请求
//   - 自动解析 multipart/byteranges 响应
//   - 兼容 CloudFront 未加引号的 multipart boundary
//   - 多 Range 只返回部分数据时顺序补齐缺失段
//   - 动态超时（基于请求数据量）
//   - 多 BaseURL，按请求携带的 URLHint 取模选取
type BundleClient struct {
	httpClient        *http.Client
	baseURLs          []string
	preferSingleRange atomic.Bool
}

// ClientConfig 是 NewBundleClient 的构造配置。
type ClientConfig struct {
	// BaseURLs 是候选的 CDN 基础 URL 列表（至少一个），格式示例：
	// "https://lol.dyn.riotcdn.net/channels/public/bundles"。
	// 构造时逐个 TrimRight "/"；多个时由 FetchOptions.URLHint 取模选取。
	BaseURLs []string

	// Workers 指定并发 Worker 数量，用于配置连接池大小。
	Workers int

	// DialContext 自定义拨号函数，用于替换连接建立逻辑（如 edge 层接管连接）。
	// 为 nil 时使用默认 net.Dialer{Timeout: 30s, KeepAlive: 30s}。
	DialContext func(ctx context.Context, network, addr string) (net.Conn, error)
}

// NewBundleClient 创建一个新的 Bundle HTTP 客户端。
//
// cfg.BaseURLs 不能为空：FetchRanges 按 URLHint % len(BaseURLs) 选取域名，
// 空列表会导致运行时对 0 取模 panic。这是一个编程错误前置条件，调用方必须
// 在归一化阶段保证至少提供一个 BaseURL（core.NewDownloader 即如此处理）。
func NewBundleClient(cfg ClientConfig) *BundleClient {
	if len(cfg.BaseURLs) == 0 {
		panic("netpool: BaseURLs 不能为空")
	}

	baseURLs := make([]string, len(cfg.BaseURLs))
	for i, u := range cfg.BaseURLs {
		baseURLs[i] = strings.TrimRight(u, "/")
	}

	dialContext := cfg.DialContext
	if dialContext == nil {
		dialContext = (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
	}

	transport := &http.Transport{
		MaxIdleConns:        cfg.Workers * 2,
		MaxIdleConnsPerHost: cfg.Workers * 2,
		IdleConnTimeout:     90 * time.Second,
		DialContext:         dialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		// Bundle 数据已是 ZSTD 压缩，禁用 HTTP 层压缩避免双重压缩
		DisableCompression: true,
	}

	return &BundleClient{
		httpClient: &http.Client{
			Transport: transport,
			// 不设全局超时，改用 per-request context 动态超时
		},
		baseURLs: baseURLs,
	}
}

// ByteRange 表示一个 HTTP Range 的字节范围（inclusive）。
type ByteRange struct {
	Start int64
	End   int64
}

// FetchOptions 控制单次 FetchRanges 请求的域名选取与整包模式。
type FetchOptions struct {
	// URLHint 决定本次请求选用的 BaseURL：BaseURLs[URLHint % len(BaseURLs)]。
	URLHint uint64

	// FullBundleSize 大于 0 时启用整包 GET 模式：不发 Range 头，请求 Bundle 全体
	// 内容，动态超时按该字节数计算；等于 0 时走既有 Range 请求分支。
	FullBundleSize int64
}

// FetchRanges 从 CDN 获取指定 Bundle 的多个字节范围。
//
// 动态超时策略：
//   - 基础超时 30s（连接建立 + TLS 握手）
//   - 数据量因子：totalBytes / 50KB（假设最低 50KB/s）
//   - 上限 10 分钟
//
// 返回的 [][]byte 与 ranges 一一对应。
func (c *BundleClient) FetchRanges(ctx context.Context, bundleFilename string, ranges []ByteRange, opts FetchOptions) ([][]byte, error) {
	if len(ranges) == 0 {
		return nil, nil
	}

	baseURL := c.baseURLs[opts.URLHint%uint64(len(c.baseURLs))]
	url := baseURL + "/" + bundleFilename

	if opts.FullBundleSize > 0 {
		return c.fetchFullBundle(ctx, url, bundleFilename, ranges, opts.FullBundleSize)
	}

	rangeHeader := buildRangeHeader(ranges)

	// 计算动态超时
	var totalBytes int64
	for _, r := range ranges {
		totalBytes += r.End - r.Start + 1
	}
	timeout := dynamicTimeout(totalBytes)

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if len(ranges) > 1 && c.preferSingleRange.Load() {
		return c.fetchSingleRanges(reqCtx, url, bundleFilename, ranges, nil)
	}

	resp, err := c.doRangeRequest(reqCtx, url, bundleFilename, rangeHeader)
	if err != nil {
		return nil, err
	}

	switch {
	case resp.StatusCode == http.StatusPartialContent: // 206
		results, parseErr := parsePartialContent(resp, ranges)
		closeErr := resp.Body.Close()
		if parseErr != nil {
			return nil, parseErr
		}
		if closeErr != nil {
			return nil, fmt.Errorf("关闭 Partial Content 响应失败: %w", closeErr)
		}
		if allRangesPresent(results) {
			return results, nil
		}
		if len(ranges) == 1 {
			return nil, fmt.Errorf("单段 Range 响应未完整覆盖请求范围 [%d-%d]", ranges[0].Start, ranges[0].End)
		}

		// RFC 9110 允许服务器只满足多 Range 请求的一部分。检测到这种响应后，
		// 当前客户端会话后续直接使用单 Range 请求，避免每个 Bundle 都先探测失败。
		c.preferSingleRange.Store(true)
		return c.fetchSingleRanges(reqCtx, url, bundleFilename, ranges, results)
	case resp.StatusCode == http.StatusOK: // 200
		body, err := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("读取完整响应失败: %w", err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("关闭完整响应失败: %w", closeErr)
		}
		return extractFromFullBody(body, ranges)
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable: // 416
		resp.Body.Close()
		return nil, fmt.Errorf("Range 不满足 (%s): 服务器返回 416", bundleFilename)
	default:
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP 响应异常 (%s): status=%d", bundleFilename, resp.StatusCode)
	}
}

func (c *BundleClient) doRangeRequest(
	ctx context.Context,
	url string,
	bundleFilename string,
	rangeHeader string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}
	req.Header.Set("Range", rangeHeader)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP 请求失败 (%s): %w", bundleFilename, err)
	}
	return resp, nil
}

// fetchSingleRanges 在现有 worker 内顺序补齐缺失 Range，不额外创建 goroutine。
// initial 中已由首个 206 响应完整覆盖的 Range 会被直接复用。
func (c *BundleClient) fetchSingleRanges(
	ctx context.Context,
	url string,
	bundleFilename string,
	ranges []ByteRange,
	initial [][]byte,
) ([][]byte, error) {
	results := initial
	if results == nil {
		results = make([][]byte, len(ranges))
	}

	for i, requested := range ranges {
		if results[i] != nil {
			continue
		}

		resp, err := c.doRangeRequest(ctx, url, bundleFilename, buildRangeHeader([]ByteRange{requested}))
		if err != nil {
			return nil, err
		}

		switch resp.StatusCode {
		case http.StatusPartialContent:
			singleResult, parseErr := parsePartialContent(resp, []ByteRange{requested})
			closeErr := resp.Body.Close()
			if parseErr != nil {
				return nil, parseErr
			}
			if closeErr != nil {
				return nil, fmt.Errorf("关闭单 Range 响应失败: %w", closeErr)
			}
			if singleResult[0] == nil {
				return nil, fmt.Errorf(
					"单段 Range 响应未完整覆盖请求范围 [%d-%d]",
					requested.Start,
					requested.End,
				)
			}
			results[i] = singleResult[0]

		case http.StatusOK:
			// 服务器忽略 Range 时，完整响应一次即可覆盖全部请求；不要为后续每段
			// 重复下载整个 Bundle。
			body, readErr := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("读取完整响应失败: %w", readErr)
			}
			if closeErr != nil {
				return nil, fmt.Errorf("关闭完整响应失败: %w", closeErr)
			}
			return extractFromFullBody(body, ranges)

		case http.StatusRequestedRangeNotSatisfiable:
			resp.Body.Close()
			return nil, fmt.Errorf(
				"Range 不满足 (%s): [%d-%d] 服务器返回 416",
				bundleFilename,
				requested.Start,
				requested.End,
			)

		default:
			statusCode := resp.StatusCode
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP 响应异常 (%s): status=%d", bundleFilename, statusCode)
		}
	}

	return results, nil
}

// fetchFullBundle 以整包 GET 方式获取 Bundle 全部内容（不发 Range 头），
// 再从响应体中切出 ranges 对应的片段。仅接受 200：不发 Range 头的请求不应
// 收到 206，收到即视为响应异常。
func (c *BundleClient) fetchFullBundle(ctx context.Context, url, bundleFilename string, ranges []ByteRange, fullBundleSize int64) ([][]byte, error) {
	timeout := dynamicTimeout(fullBundleSize)

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("创建 HTTP 请求失败: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP 请求失败 (%s): %w", bundleFilename, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("整包 GET 响应异常 (%s): status=%d", bundleFilename, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取完整响应失败: %w", err)
	}
	return extractFromFullBody(body, ranges)
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

func parsePartialContent(resp *http.Response, requested []ByteRange) ([][]byte, error) {
	contentType := resp.Header.Get("Content-Type")

	boundary, isMultipart, err := parseMultipartBoundary(contentType)
	if err != nil {
		return nil, err
	}
	if isMultipart {
		return parseMultipartResponse(resp.Body, boundary, requested)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取单段响应失败: %w", err)
	}
	contentRange, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil {
		return nil, err
	}

	results := make([][]byte, len(requested))
	if err := mapRangeData(results, requested, contentRange, data); err != nil {
		return nil, err
	}
	return results, nil
}

// parseMultipartBoundary 优先使用标准 MIME 解析器；当 CloudFront 返回未加引号、
// 含冒号的 boundary（例如 boundary=CloudFront:ABC）时，按参数原值兼容提取。
// 冒号在 quoted-string 中合法，但部分 Riot CDN 节点没有按标准添加引号。
func parseMultipartBoundary(contentType string) (string, bool, error) {
	mediaType, params, parseErr := mime.ParseMediaType(contentType)
	if parseErr == nil {
		if !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
			return "", false, nil
		}
		boundary := params["boundary"]
		if boundary == "" {
			return "", true, fmt.Errorf("multipart 响应缺少 boundary")
		}
		return boundary, true, nil
	}

	parts := strings.Split(contentType, ";")
	if len(parts) == 0 ||
		!strings.HasPrefix(strings.ToLower(strings.TrimSpace(parts[0])), "multipart/") {
		return "", false, nil
	}

	for _, parameter := range parts[1:] {
		key, value, ok := strings.Cut(parameter, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "boundary") {
			continue
		}
		boundary := strings.TrimSpace(value)
		if len(boundary) >= 2 && boundary[0] == '"' && boundary[len(boundary)-1] == '"' {
			unquoted, err := strconv.Unquote(boundary)
			if err != nil {
				return "", true, fmt.Errorf(
					"解析 multipart Content-Type %q 的 boundary 失败: %w",
					contentType,
					err,
				)
			}
			boundary = unquoted
		}
		if boundary == "" || strings.ContainsAny(boundary, "\r\n") {
			return "", true, fmt.Errorf(
				"multipart Content-Type %q 包含无效 boundary",
				contentType,
			)
		}
		return boundary, true, nil
	}

	return "", true, fmt.Errorf(
		"解析 multipart Content-Type %q 失败: %w",
		contentType,
		parseErr,
	)
}

// parseMultipartResponse 解析 multipart/byteranges 响应。
func parseMultipartResponse(body io.Reader, boundary string, requested []ByteRange) ([][]byte, error) {
	reader := multipart.NewReader(body, boundary)
	results := make([][]byte, len(requested))
	partCount := 0

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("读取 multipart 段失败: %w", err)
		}
		partCount++
		if partCount > len(requested) {
			return nil, fmt.Errorf("multipart 段数超过请求数量: 收到>%d", len(requested))
		}

		data, err := io.ReadAll(part)
		if err != nil {
			return nil, fmt.Errorf("读取 multipart 段数据失败: %w", err)
		}
		contentRange, err := parseContentRange(part.Header.Get("Content-Range"))
		if err != nil {
			return nil, fmt.Errorf("multipart 第 %d 段 Content-Range 无效: %w", partCount, err)
		}
		if err := mapRangeData(results, requested, contentRange, data); err != nil {
			return nil, fmt.Errorf("multipart 第 %d 段无效: %w", partCount, err)
		}
	}

	if partCount == 0 {
		return nil, fmt.Errorf("multipart 响应不包含任何数据段")
	}
	return results, nil
}

func parseContentRange(value string) (ByteRange, error) {
	fields := strings.Fields(value)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bytes") {
		return ByteRange{}, fmt.Errorf("Content-Range 格式无效: %q", value)
	}

	rangeAndLength := strings.Split(fields[1], "/")
	if len(rangeAndLength) != 2 {
		return ByteRange{}, fmt.Errorf("Content-Range 格式无效: %q", value)
	}
	bounds := strings.Split(rangeAndLength[0], "-")
	if len(bounds) != 2 {
		return ByteRange{}, fmt.Errorf("Content-Range 范围无效: %q", value)
	}

	start, err := strconv.ParseInt(bounds[0], 10, 64)
	if err != nil {
		return ByteRange{}, fmt.Errorf("Content-Range 起点无效: %q", value)
	}
	end, err := strconv.ParseInt(bounds[1], 10, 64)
	if err != nil || start < 0 || end < start {
		return ByteRange{}, fmt.Errorf("Content-Range 终点无效: %q", value)
	}

	if rangeAndLength[1] != "*" {
		completeLength, err := strconv.ParseInt(rangeAndLength[1], 10, 64)
		if err != nil || completeLength <= end {
			return ByteRange{}, fmt.Errorf("Content-Range 总长度无效: %q", value)
		}
	}

	return ByteRange{Start: start, End: end}, nil
}

// mapRangeData 按 Content-Range 将响应数据映射到请求顺序。服务端合并相邻
// Range 时，一个响应段可覆盖多个请求；只覆盖请求一部分的响应保留为空，
// 交给单 Range 回退补齐。
func mapRangeData(results [][]byte, requested []ByteRange, contentRange ByteRange, data []byte) error {
	expectedLength := contentRange.End - contentRange.Start + 1
	if int64(len(data)) != expectedLength {
		return fmt.Errorf(
			"Content-Range 数据长度不匹配: range=[%d-%d], data=%d",
			contentRange.Start,
			contentRange.End,
			len(data),
		)
	}

	mapped := false
	for i, want := range requested {
		if contentRange.Start > want.Start || contentRange.End < want.End {
			continue
		}
		if results[i] != nil {
			return fmt.Errorf("Range [%d-%d] 收到重复响应", want.Start, want.End)
		}

		start := want.Start - contentRange.Start
		end := start + want.End - want.Start + 1
		results[i] = data[start:end]
		mapped = true
	}
	if mapped {
		return nil
	}

	// 206 可以只返回请求范围的一个子集；这种数据不足以形成调用方需要的完整
	// Range，但仍属于合法响应，后续会用精确单 Range 请求补齐。
	for _, want := range requested {
		if contentRange.Start >= want.Start && contentRange.End <= want.End {
			return nil
		}
	}

	return fmt.Errorf(
		"Content-Range [%d-%d] 不属于任何请求范围",
		contentRange.Start,
		contentRange.End,
	)
}

func allRangesPresent(results [][]byte) bool {
	for _, result := range results {
		if result == nil {
			return false
		}
	}
	return true
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
