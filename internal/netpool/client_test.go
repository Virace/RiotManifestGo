package netpool

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- httptest CDN 模拟 ----

// mockCDNHandler 模拟 Riot CDN 的 Bundle 文件服务。
type mockCDNHandler struct {
	// bundleData 存储 bundleFilename → 完整文件字节
	bundleData map[string][]byte
	// forceStatus 强制返回指定状态码（0 表示正常逻辑）
	forceStatus int
}

func newMockCDN() *mockCDNHandler {
	return &mockCDNHandler{
		bundleData: make(map[string][]byte),
	}
}

func (m *mockCDNHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.forceStatus != 0 {
		w.WriteHeader(m.forceStatus)
		return
	}

	// 路径格式: /XXXX.bundle
	path := strings.TrimPrefix(r.URL.Path, "/")
	data, ok := m.bundleData[path]
	if !ok {
		http.NotFound(w, r)
		return
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		// 无 Range，返回完整文件
		w.WriteHeader(http.StatusOK)
		w.Write(data)
		return
	}

	// 解析 Range: bytes=A-B, C-D, ...
	ranges := parseRangeHeader(rangeHeader, int64(len(data)))
	if ranges == nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if len(ranges) == 1 {
		// 单段：206 + 直接返回切片
		r := ranges[0]
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", r.start, r.end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[r.start : r.end+1])
		return
	}

	// 多段：206 + multipart/byteranges
	boundary := "TestBoundary12345"
	w.Header().Set("Content-Type", fmt.Sprintf("multipart/byteranges; boundary=%s", boundary))
	w.WriteHeader(http.StatusPartialContent)

	mw := multipart.NewWriter(w)
	mw.SetBoundary(boundary)

	for _, rng := range ranges {
		partHeader := make(textproto.MIMEHeader)
		partHeader.Set("Content-Type", "application/octet-stream")
		partHeader.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, len(data)))

		part, err := mw.CreatePart(partHeader)
		if err != nil {
			return
		}
		part.Write(data[rng.start : rng.end+1])
	}
	mw.Close()
}

type byteRange struct {
	start, end int64
}

// parseRangeHeader 解析 "bytes=A-B, C-D" 格式，返回合法的 Range 列表。
func parseRangeHeader(header string, fileSize int64) []byteRange {
	if !strings.HasPrefix(header, "bytes=") {
		return nil
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.Split(spec, ",")
	var ranges []byteRange
	for _, p := range parts {
		p = strings.TrimSpace(p)
		dash := strings.IndexByte(p, '-')
		if dash < 0 {
			return nil
		}
		startStr := p[:dash]
		endStr := p[dash+1:]

		start, err1 := strconv.ParseInt(startStr, 10, 64)
		end, err2 := strconv.ParseInt(endStr, 10, 64)
		if err1 != nil || err2 != nil {
			return nil
		}
		if start < 0 || end >= fileSize || start > end {
			return nil
		}
		ranges = append(ranges, byteRange{start, end})
	}
	return ranges
}

// ---- 测试用例 ----

// TestFetchRanges_SingleRange 验证单段 Range → 206 单段响应。
func TestFetchRanges_SingleRange(t *testing.T) {
	cdn := newMockCDN()
	cdn.bundleData["test.bundle"] = []byte("0123456789ABCDEF")

	srv := httptest.NewServer(cdn)
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	result, err := client.FetchRanges(context.Background(), "test.bundle", []ByteRange{
		{Start: 0, End: 3}, // "0123"
	})
	if err != nil {
		t.Fatalf("FetchRanges 失败: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("期望 1 段结果，得到 %d", len(result))
	}
	if string(result[0]) != "0123" {
		t.Errorf("数据不匹配: %q, 期望 %q", result[0], "0123")
	}
}

// TestFetchRanges_MultiRange 验证多段 Range → 206 multipart 响应。
func TestFetchRanges_MultiRange(t *testing.T) {
	cdn := newMockCDN()
	cdn.bundleData["multi.bundle"] = []byte("0123456789ABCDEF")

	srv := httptest.NewServer(cdn)
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	result, err := client.FetchRanges(context.Background(), "multi.bundle", []ByteRange{
		{Start: 0, End: 3},   // "0123"
		{Start: 10, End: 13}, // "ABCD"
	})
	if err != nil {
		t.Fatalf("FetchRanges 失败: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("期望 2 段结果，得到 %d", len(result))
	}

	if string(result[0]) != "0123" {
		t.Errorf("Range[0] 数据不匹配: %q, 期望 %q", result[0], "0123")
	}
	if string(result[1]) != "ABCD" {
		t.Errorf("Range[1] 数据不匹配: %q, 期望 %q", result[1], "ABCD")
	}
}

// TestFetchRanges_MultiRangeCloudFrontBoundary 验证 Riot CDN 使用未加引号且
// 含冒号的 CloudFront boundary 时，仍能按 multipart 正确解析。
func TestFetchRanges_MultiRangeCloudFrontBoundary(t *testing.T) {
	const boundary = "CloudFront:1943018D7AC7E2AE58471E3C2FCCA87B"
	body := fmt.Sprintf(
		"--%s\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-3/16\r\n\r\n0123\r\n"+
			"--%s\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 10-13/16\r\n\r\nABCD\r\n"+
			"--%s--\r\n",
		boundary,
		boundary,
		boundary,
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "multipart/byteranges; boundary="+boundary)
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte(body))
	}))
	defer srv.Close()

	client := NewBundleClient(srv.URL, 1)
	defer client.Close()

	result, err := client.FetchRanges(context.Background(), "cloudfront.bundle", []ByteRange{
		{Start: 0, End: 3},
		{Start: 10, End: 13},
	})
	if err != nil {
		t.Fatalf("FetchRanges 失败: %v", err)
	}
	if string(result[0]) != "0123" || string(result[1]) != "ABCD" {
		t.Fatalf("结果不匹配: %q, %q", result[0], result[1])
	}
}

// TestFetchRangesFallsBackFromSinglePartForMultiRange 验证多 Range 请求只收到
// 一个 206 单段响应时，会复用已返回的段并顺序补齐剩余段；同一客户端后续请求
// 直接使用单 Range，避免每个 Bundle 都重复探测。
func TestFetchRangesFallsBackFromSinglePartForMultiRange(t *testing.T) {
	data := []byte("0123456789ABCDEF")
	var mu sync.Mutex
	var rangeHeaders []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		mu.Lock()
		rangeHeaders = append(rangeHeaders, rangeHeader)
		mu.Unlock()

		ranges := parseRangeHeader(rangeHeader, int64(len(data)))
		if len(ranges) == 0 {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		// 多 Range 请求只返回其中第二段，模拟真实 CDN 行为。
		selected := ranges[0]
		if len(ranges) > 1 {
			selected = ranges[1]
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set(
			"Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", selected.start, selected.end, len(data)),
		)
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[selected.start : selected.end+1])
	}))
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	ranges := []ByteRange{
		{Start: 0, End: 3},
		{Start: 10, End: 13},
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := client.FetchRanges(context.Background(), "fallback.bundle", ranges)
		if err != nil {
			t.Fatalf("第 %d 次 FetchRanges 失败: %v", attempt+1, err)
		}
		if string(result[0]) != "0123" || string(result[1]) != "ABCD" {
			t.Fatalf("第 %d 次结果不匹配: %q, %q", attempt+1, result[0], result[1])
		}
	}

	mu.Lock()
	gotHeaders := append([]string(nil), rangeHeaders...)
	mu.Unlock()
	wantHeaders := []string{
		"bytes=0-3, 10-13", // 首次多 Range 探测
		"bytes=0-3",        // 补齐缺失的第一段
		"bytes=0-3",        // 后续调用直接逐段请求
		"bytes=10-13",
	}
	if len(gotHeaders) != len(wantHeaders) {
		t.Fatalf("Range 请求数 = %d, want %d: %v", len(gotHeaders), len(wantHeaders), gotHeaders)
	}
	for i, want := range wantHeaders {
		if gotHeaders[i] != want {
			t.Errorf("Range 请求[%d] = %q, want %q", i, gotHeaders[i], want)
		}
	}
}

// TestFetchRangesFallbackUsesFullBodyOnce 验证补请求收到 200 完整体时，
// 客户端一次切出全部 Range，不会为后续段重复下载整个 Bundle。
func TestFetchRangesFallbackUsesFullBodyOnce(t *testing.T) {
	data := []byte("0123456789ABCDEF")
	var requestCount int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		ranges := parseRangeHeader(r.Header.Get("Range"), int64(len(data)))
		if requestCount == 1 {
			selected := ranges[0]
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set(
				"Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", selected.start, selected.end, len(data)),
			)
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data[selected.start : selected.end+1])
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	result, err := client.FetchRanges(context.Background(), "full-fallback.bundle", []ByteRange{
		{Start: 0, End: 3},
		{Start: 10, End: 13},
	})
	if err != nil {
		t.Fatalf("FetchRanges 失败: %v", err)
	}
	if string(result[0]) != "0123" || string(result[1]) != "ABCD" {
		t.Fatalf("结果不匹配: %q, %q", result[0], result[1])
	}
	if requestCount != 2 {
		t.Fatalf("请求数 = %d, want 2", requestCount)
	}
}

func TestFetchRangesRejectsInvalidSingleRangeMetadata(t *testing.T) {
	tests := []struct {
		name         string
		contentRange string
		body         string
	}{
		{name: "missing Content-Range", body: "0123"},
		{name: "unexpected Content-Range", contentRange: "bytes 1-4/16", body: "1234"},
		{name: "truncated body", contentRange: "bytes 0-3/16", body: "012"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/octet-stream")
				if tt.contentRange != "" {
					w.Header().Set("Content-Range", tt.contentRange)
				}
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			client := NewBundleClient(srv.URL, 1)
			defer client.Close()

			_, err := client.FetchRanges(
				context.Background(),
				"invalid.bundle",
				[]ByteRange{{Start: 0, End: 3}},
			)
			if err == nil {
				t.Fatal("无效单 Range 元数据应返回 error")
			}
		})
	}
}

// TestFetchRanges_FullBody200 验证 CDN 返回 200（忽略 Range）时的处理。
func TestFetchRanges_FullBody200(t *testing.T) {
	// 模拟 CDN 始终返回 200 + 完整文件体
	data := []byte("0123456789ABCDEF")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}))
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	result, err := client.FetchRanges(context.Background(), "full.bundle", []ByteRange{
		{Start: 0, End: 3},   // "0123"
		{Start: 10, End: 13}, // "ABCD"
	})
	if err != nil {
		t.Fatalf("FetchRanges 失败: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("期望 2 段结果，得到 %d", len(result))
	}
	if string(result[0]) != "0123" {
		t.Errorf("Range[0] 数据不匹配: %q", result[0])
	}
	if string(result[1]) != "ABCD" {
		t.Errorf("Range[1] 数据不匹配: %q", result[1])
	}
}

// TestFetchRanges_416Error 验证 CDN 返回 416 时的错误处理。
func TestFetchRanges_416Error(t *testing.T) {
	cdn := newMockCDN()
	cdn.forceStatus = http.StatusRequestedRangeNotSatisfiable

	srv := httptest.NewServer(cdn)
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	_, err := client.FetchRanges(context.Background(), "any.bundle", []ByteRange{
		{Start: 0, End: 999},
	})
	if err == nil {
		t.Fatal("CDN 返回 416 时应返回 error")
	}
	if !strings.Contains(err.Error(), "416") {
		t.Errorf("错误信息应提及 416: %v", err)
	}
}

// TestFetchRanges_ServerError 验证 CDN 返回 500 时的错误处理。
func TestFetchRanges_ServerError(t *testing.T) {
	cdn := newMockCDN()
	cdn.forceStatus = http.StatusInternalServerError

	srv := httptest.NewServer(cdn)
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	_, err := client.FetchRanges(context.Background(), "error.bundle", []ByteRange{
		{Start: 0, End: 10},
	})
	if err == nil {
		t.Fatal("CDN 返回 500 时应返回 error")
	}
}

// TestFetchRanges_Empty 验证空 ranges 返回 nil。
func TestFetchRanges_Empty(t *testing.T) {
	client := NewBundleClient("http://unused", 2)
	defer client.Close()

	result, err := client.FetchRanges(context.Background(), "any.bundle", nil)
	if err != nil {
		t.Fatalf("空 ranges 不应返回 error: %v", err)
	}
	if result != nil {
		t.Errorf("空 ranges 应返回 nil，得到 %v", result)
	}
}

// TestBuildRangeHeader 验证 Range 头的格式正确性。
func TestBuildRangeHeader(t *testing.T) {
	tests := []struct {
		name   string
		ranges []ByteRange
		want   string
	}{
		{
			name:   "单段",
			ranges: []ByteRange{{Start: 0, End: 99}},
			want:   "bytes=0-99",
		},
		{
			name:   "多段",
			ranges: []ByteRange{{Start: 0, End: 99}, {Start: 200, End: 399}},
			want:   "bytes=0-99, 200-399",
		},
		{
			name:   "三段",
			ranges: []ByteRange{{Start: 100, End: 199}, {Start: 500, End: 599}, {Start: 1000, End: 1099}},
			want:   "bytes=100-199, 500-599, 1000-1099",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildRangeHeader(tt.ranges)
			if got != tt.want {
				t.Errorf("buildRangeHeader() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestDynamicTimeout 验证动态超时计算逻辑。
func TestDynamicTimeout(t *testing.T) {
	tests := []struct {
		name       string
		totalBytes int64
		wantMin    time.Duration
		wantMax    time.Duration
	}{
		{
			name:       "零字节（仅基础超时）",
			totalBytes: 0,
			wantMin:    30 * time.Second,
			wantMax:    30 * time.Second,
		},
		{
			name:       "小数据（50KB → 基础+1s）",
			totalBytes: 50 * 1024,
			wantMin:    31 * time.Second,
			wantMax:    31 * time.Second,
		},
		{
			name:       "大数据（超上限 10 分钟）",
			totalBytes: 100 * 1024 * 1024, // 100MB
			wantMin:    10 * time.Minute,
			wantMax:    10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dynamicTimeout(tt.totalBytes)
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("dynamicTimeout(%d) = %v, 期望 [%v, %v]",
					tt.totalBytes, got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

// TestFetchRanges_ContextCancel 验证 context 取消时请求立即中止。
func TestFetchRanges_ContextCancel(t *testing.T) {
	// 创建一个慢响应服务器
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Second) // 故意很慢
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewBundleClient(srv.URL, 2)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := client.FetchRanges(ctx, "slow.bundle", []ByteRange{{Start: 0, End: 99}})
		done <- err
	}()

	// 100ms 后取消
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Log("context 取消后无 error（服务器可能已响应）")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("FetchRanges 在 context cancel 后 3 秒内未退出")
	}
}

// ---- 内部解析函数测试 ----

// TestParseMultipartResponse 验证 multipart 解析边界情况。
func TestParseMultipartResponse(t *testing.T) {
	// 手动构造乱序 multipart body，验证结果按 Content-Range 而非响应顺序映射。
	boundary := "testboundary"
	body := fmt.Sprintf(
		"--%s\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 10-13/16\r\n\r\nABCD\r\n"+
			"--%s\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-3/16\r\n\r\n0123\r\n"+
			"--%s--\r\n",
		boundary, boundary, boundary,
	)

	result, err := parseMultipartResponse(strings.NewReader(body), boundary, []ByteRange{
		{Start: 0, End: 3},
		{Start: 10, End: 13},
	})
	if err != nil {
		t.Fatalf("parseMultipartResponse 失败: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("期望 2 段，得到 %d", len(result))
	}
	if string(result[0]) != "0123" {
		t.Errorf("Part[0] = %q, 期望 %q", result[0], "0123")
	}
	if string(result[1]) != "ABCD" {
		t.Errorf("Part[1] = %q, 期望 %q", result[1], "ABCD")
	}
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		value string
		want  ByteRange
		ok    bool
	}{
		{value: "bytes 0-3/16", want: ByteRange{Start: 0, End: 3}, ok: true},
		{value: "Bytes 10-13/*", want: ByteRange{Start: 10, End: 13}, ok: true},
		{value: "", ok: false},
		{value: "bytes 4-3/16", ok: false},
		{value: "bytes 0-16/16", ok: false},
		{value: "items 0-3/16", ok: false},
	}

	for _, tt := range tests {
		got, err := parseContentRange(tt.value)
		if tt.ok {
			if err != nil {
				t.Errorf("parseContentRange(%q) unexpected error: %v", tt.value, err)
				continue
			}
			if got != tt.want {
				t.Errorf("parseContentRange(%q) = %+v, want %+v", tt.value, got, tt.want)
			}
			continue
		}
		if err == nil {
			t.Errorf("parseContentRange(%q) 应返回 error", tt.value)
		}
	}
}

// TestExtractFromFullBody 验证从完整体中按 Range 提取数据。
func TestExtractFromFullBody(t *testing.T) {
	body := []byte("0123456789ABCDEF")

	result, err := extractFromFullBody(body, []ByteRange{
		{Start: 0, End: 3},
		{Start: 10, End: 15},
	})
	if err != nil {
		t.Fatalf("extractFromFullBody 失败: %v", err)
	}
	if string(result[0]) != "0123" {
		t.Errorf("Range[0] = %q, 期望 %q", result[0], "0123")
	}
	if string(result[1]) != "ABCDEF" {
		t.Errorf("Range[1] = %q, 期望 %q", result[1], "ABCDEF")
	}
}

// TestExtractFromFullBody_OutOfRange 验证 Range 越界时返回错误。
func TestExtractFromFullBody_OutOfRange(t *testing.T) {
	body := []byte("short")

	_, err := extractFromFullBody(body, []ByteRange{
		{Start: 0, End: 99}, // 越界
	})
	if err == nil {
		t.Fatal("Range 越界时应返回 error")
	}
}

// 确保 io 包的导入不被优化掉（用于 multipart 测试的补充）
var _ = io.EOF
