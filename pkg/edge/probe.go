package edge

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"
)

// probeDialTimeout 是探测请求建连阶段的拨号超时。
const probeDialTimeout = 10 * time.Second

// probeTotalTimeout 是单次探测请求（含建连、发送、响应头、读取限量体）的总超时。
const probeTotalTimeout = 15 * time.Second

// httpRangeProbe 对候选 IP 发起一次性 HTTP Range 探测：为本次探测单独构造
// Transport，将 DialContext 固定拨到 ip:port，而请求 URL 的 host 仍保持
// probeURL 原有域名——这样 TLS SNI 与证书校验走的是真实域名，天然正确，探测
// 到的只是"走哪条网络路径"而非伪造身份。
//
// 打分口径：TTFB 记录从发起请求到响应头到达的耗时（含 TCP+TLS 建连）；错误
// 或响应状态非 200/206、限量体读取失败，均视为本次探测失败（调用方据此将该
// 候选逐出赢家表）。
func httpRangeProbe(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
	if err != nil {
		return probeResult{}, fmt.Errorf("edge: 构造探测请求失败: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", limit-1))

	dialer := &net.Dialer{Timeout: probeDialTimeout}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			_, port, splitErr := net.SplitHostPort(addr)
			if splitErr != nil {
				return nil, splitErr
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		},
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   probeTotalTimeout,
	}
	defer client.CloseIdleConnections()

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return probeResult{}, fmt.Errorf("edge: 探测请求失败 ip=%s: %w", ip, err)
	}
	defer resp.Body.Close()
	ttfb := time.Since(start)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return probeResult{}, fmt.Errorf("edge: 探测响应异常 ip=%s status=%d", ip, resp.StatusCode)
	}

	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, limit)); err != nil {
		return probeResult{}, fmt.Errorf("edge: 读取探测响应体失败 ip=%s: %w", ip, err)
	}

	return probeResult{TTFB: ttfb, Total: time.Since(start)}, nil
}
