package edge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"

	"github.com/miekg/dns"
)

// udpServers 是 UDP 直查使用的公共 DNS 服务器：阿里云、腾讯 DNSPod、
// Cloudflare。每个服务器对应一个独立的发现源，互不影响。
var udpServers = []string{"223.5.5.5", "119.29.29.29", "1.1.1.1"}

// aliDoHEndpoint 是阿里云 DoH（DNS over HTTPS，RFC 8484）查询端点。
const aliDoHEndpoint = "https://223.5.5.5/dns-query"

// udpQueryTimeout 是 UDP 直查单次交换的超时。
const udpQueryTimeout = 3 * time.Second

// dohQueryTimeout 是 DoH 查询单次 HTTP 请求的超时。
const dohQueryTimeout = 10 * time.Second

// defaultDiscoverers 组装 Selector 默认使用的候选发现源集合：系统 DNS 一个
// + UDP 直查（每服务器一个，共 len(udpServers) 个）+ 阿里 DoH（携带出口 IP
// 的 ECS）一个。任意源查询失败只影响自身返回，由调用方并行收集、逐个吞掉
// error，不影响其余源。
func defaultDiscoverers() []discoverFunc {
	discoverers := make([]discoverFunc, 0, 2+len(udpServers))
	discoverers = append(discoverers, systemDNSDiscover)
	for _, server := range udpServers {
		discoverers = append(discoverers, udpDiscoverer(server))
	}
	discoverers = append(discoverers, aliDoHDiscover)
	return discoverers
}

// systemDNSDiscover 使用进程所在系统的默认解析器（遵循操作系统 hosts 文件与
// DNS 配置）解析 host 的 IPv4 地址。
func systemDNSDiscover(ctx context.Context, host string) ([]netip.Addr, error) {
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return nil, fmt.Errorf("edge: 系统 DNS 解析 %s 失败: %w", host, err)
	}
	return addrs, nil
}

// udpDiscoverer 返回一个通过指定 UDP DNS 服务器直查 A 记录的发现源。
func udpDiscoverer(server string) discoverFunc {
	return func(ctx context.Context, host string) ([]netip.Addr, error) {
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn(host), dns.TypeA)

		client := &dns.Client{Timeout: udpQueryTimeout}
		in, _, err := client.ExchangeContext(ctx, m, server+":53")
		if err != nil {
			return nil, fmt.Errorf("edge: UDP 直查 %s 解析 %s 失败: %w", server, host, err)
		}
		return extractA(in), nil
	}
}

// aliDoHDiscover 通过阿里云 DoH（RFC 8484 wireformat，POST application/dns-message）
// 解析 host。携带调用方出口 IP 的 EDNS Client Subnet（ECS，/24 掩码）以获取更
// 贴近调用方网络位置的解析结果；拿不到出口 IP 时降级为不带 ECS 的普通查询，
// 而不是直接失败。
func aliDoHDiscover(ctx context.Context, host string) ([]netip.Addr, error) {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(host), dns.TypeA)
	m.SetEdns0(1232, false)

	if ecs := egressIP(ctx); ecs != nil {
		opt := m.IsEdns0()
		opt.Option = append(opt.Option, &dns.EDNS0_SUBNET{
			Code:          dns.EDNS0SUBNET,
			Family:        1,
			SourceNetmask: 24,
			Address:       ecs.Mask(net.CIDRMask(24, 32)),
		})
	}

	raw, err := m.Pack()
	if err != nil {
		return nil, fmt.Errorf("edge: 打包 DoH 请求失败: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, aliDoHEndpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("edge: 构造 DoH 请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/dns-message")
	req.Header.Set("Accept", "application/dns-message")

	client := &http.Client{Timeout: dohQueryTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("edge: DoH 请求 %s 失败: %w", host, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("edge: DoH 响应异常 %s status=%d", host, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("edge: 读取 DoH 响应失败: %w", err)
	}

	in := new(dns.Msg)
	if err := in.Unpack(body); err != nil {
		return nil, fmt.Errorf("edge: 解析 DoH 响应失败: %w", err)
	}
	return extractA(in), nil
}

// extractA 从 DNS 响应的 Answer 段提取全部 A 记录地址，忽略非 A 记录与解析
// 失败的条目。
func extractA(msg *dns.Msg) []netip.Addr {
	if msg == nil {
		return nil
	}
	addrs := make([]netip.Addr, 0, len(msg.Answer))
	for _, rr := range msg.Answer {
		a, ok := rr.(*dns.A)
		if !ok || a.A == nil {
			continue
		}
		ip, ok := netip.AddrFromSlice(a.A.To4())
		if !ok {
			continue
		}
		addrs = append(addrs, ip)
	}
	return addrs
}

// egressIP 探测调用方的出口 IPv4 地址，用于 DoH 查询携带 ECS 信息，使解析结果
// 更贴近调用方的网络位置。优先使用 Cloudflare 的 whoami.cloudflare CHAOS TXT
// 查询，失败则降级到 Google 的 o-o.myaddr.l.google.com TXT 查询；两者均失败
// 返回 nil，调用方据此跳过 ECS、退化为普通查询。
func egressIP(ctx context.Context) net.IP {
	if ip := queryTXTIP(ctx, "1.1.1.1:53", "whoami.cloudflare", dns.ClassCHAOS); ip != nil {
		return ip
	}
	return queryTXTIP(ctx, "8.8.8.8:53", "o-o.myaddr.l.google.com", dns.ClassINET)
}

// queryTXTIP 向指定 DNS 服务器查询给定 qname/qclass 的 TXT 记录，取首条结果
// 尝试解析为 IPv4 地址；查询失败、无结果或结果不是合法 IPv4 时返回 nil。
func queryTXTIP(ctx context.Context, server, qname string, qclass uint16) net.IP {
	m := new(dns.Msg)
	m.Id = dns.Id()
	m.RecursionDesired = true
	m.Question = []dns.Question{{Name: dns.Fqdn(qname), Qtype: dns.TypeTXT, Qclass: qclass}}

	client := &dns.Client{Timeout: udpQueryTimeout}
	in, _, err := client.ExchangeContext(ctx, m, server)
	if err != nil || in == nil {
		return nil
	}

	for _, rr := range in.Answer {
		txt, ok := rr.(*dns.TXT)
		if !ok || len(txt.Txt) == 0 {
			continue
		}
		ip := net.ParseIP(txt.Txt[0])
		if ip == nil {
			continue
		}
		if ip4 := ip.To4(); ip4 != nil {
			return ip4
		}
	}
	return nil
}
