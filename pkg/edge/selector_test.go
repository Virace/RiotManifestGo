package edge

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// baseConfig 返回一个通过 NewSelector 校验的最小配置：单个注册域名
// cdn.example.com，供各用例在此基础上替换 discoverers/probe/dialFn。
func baseConfig() Config {
	return Config{
		ProbeURLs: []string{"https://cdn.example.com/probe"},
	}
}

// newTestSelector 构造 Selector 并断言 NewSelector 未返回 error；后续用例通过
// 直接替换同包可见字段（discoverers/probe/dialFn/winners）注入 mock，全程零
// 真实网络。
func newTestSelector(t *testing.T, cfg Config) *Selector {
	t.Helper()
	s, err := NewSelector(cfg)
	if err != nil {
		t.Fatalf("NewSelector 失败: %v", err)
	}
	return s
}

// TestRefreshUnionDedup 验证两个发现源返回重叠 IP 时，探测阶段收到的是去重
// 并集，每个候选恰好被探测一次。
func TestRefreshUnionDedup(t *testing.T) {
	s := newTestSelector(t, baseConfig())

	ipA := netip.MustParseAddr("1.1.1.1")
	ipB := netip.MustParseAddr("2.2.2.2")

	s.discoverers = []discoverFunc{
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{ipA, ipB}, nil
		},
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{ipB, ipA}, nil // 与第一个源重叠
		},
	}

	var mu sync.Mutex
	seen := make(map[netip.Addr]int)
	s.probe = func(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error) {
		mu.Lock()
		seen[ip]++
		mu.Unlock()
		return probeResult{TTFB: 10 * time.Millisecond}, nil
	}

	s.refresh(context.Background())

	if len(seen) != 2 {
		t.Fatalf("期望去重后 2 个候选进入探测，实际=%d (%v)", len(seen), seen)
	}
	for ip, count := range seen {
		if count != 1 {
			t.Errorf("候选 %s 被探测 %d 次，期望恰好 1 次", ip, count)
		}
	}
}

// TestDiscoverFailureIgnored 验证一个发现源报错时，其余发现源的候选照常进入
// 探测，不受影响。
func TestDiscoverFailureIgnored(t *testing.T) {
	s := newTestSelector(t, baseConfig())

	ipA := netip.MustParseAddr("1.1.1.1")
	s.discoverers = []discoverFunc{
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			return nil, errors.New("发现源故障")
		},
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{ipA}, nil
		},
	}

	var probed int32
	s.probe = func(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error) {
		atomic.AddInt32(&probed, 1)
		return probeResult{TTFB: 5 * time.Millisecond}, nil
	}

	s.refresh(context.Background())

	if atomic.LoadInt32(&probed) != 1 {
		t.Fatalf("期望其余候选照常进入探测，实际探测次数=%d", probed)
	}
	winners := s.Winners()
	if len(winners) != 1 || winners[0] != ipA {
		t.Fatalf("期望赢家=[%s]，实际=%v", ipA, winners)
	}
}

// TestProbeFailureCulled 验证探测报错的候选不会进入赢家表。
func TestProbeFailureCulled(t *testing.T) {
	s := newTestSelector(t, baseConfig())

	ipGood := netip.MustParseAddr("1.1.1.1")
	ipBad := netip.MustParseAddr("2.2.2.2")
	s.discoverers = []discoverFunc{
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{ipGood, ipBad}, nil
		},
	}
	s.probe = func(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error) {
		if ip == ipBad {
			return probeResult{}, errors.New("探测失败")
		}
		return probeResult{TTFB: 10 * time.Millisecond}, nil
	}

	s.refresh(context.Background())

	winners := s.Winners()
	if len(winners) != 1 || winners[0] != ipGood {
		t.Fatalf("期望赢家仅含探测成功的 %s，实际=%v", ipGood, winners)
	}
}

// TestWinnersTopNByTTFB 验证 5 个 TTFB 各异的候选恰好按升序取出最快的
// Winners(3) 个。
func TestWinnersTopNByTTFB(t *testing.T) {
	cfg := baseConfig()
	cfg.Winners = 3
	s := newTestSelector(t, cfg)

	ttfbByIP := map[netip.Addr]time.Duration{
		netip.MustParseAddr("1.1.1.1"): 50 * time.Millisecond,
		netip.MustParseAddr("2.2.2.2"): 10 * time.Millisecond,
		netip.MustParseAddr("3.3.3.3"): 30 * time.Millisecond,
		netip.MustParseAddr("4.4.4.4"): 20 * time.Millisecond,
		netip.MustParseAddr("5.5.5.5"): 40 * time.Millisecond,
	}
	ips := make([]netip.Addr, 0, len(ttfbByIP))
	for ip := range ttfbByIP {
		ips = append(ips, ip)
	}

	s.discoverers = []discoverFunc{
		func(ctx context.Context, host string) ([]netip.Addr, error) { return ips, nil },
	}
	s.probe = func(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error) {
		return probeResult{TTFB: ttfbByIP[ip]}, nil
	}

	s.refresh(context.Background())

	got := s.Winners()
	want := []netip.Addr{
		netip.MustParseAddr("2.2.2.2"), // 10ms
		netip.MustParseAddr("4.4.4.4"), // 20ms
		netip.MustParseAddr("3.3.3.3"), // 30ms
	}
	if len(got) != len(want) {
		t.Fatalf("期望 %d 个赢家，实际=%d (%v)", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("赢家[%d]=%s，期望=%s", i, got[i], w)
		}
	}
}

// TestDialRotation 验证 3 个赢家下，连续拨号的 addr 按轮转依次变化。
func TestDialRotation(t *testing.T) {
	s := newTestSelector(t, baseConfig())
	s.winners = []netip.Addr{
		netip.MustParseAddr("1.1.1.1"),
		netip.MustParseAddr("2.2.2.2"),
		netip.MustParseAddr("3.3.3.3"),
	}

	var mu sync.Mutex
	var dialed []string
	s.dialFn = func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		return nil, errors.New("测试桩：不建立真实连接")
	}

	for i := 0; i < 6; i++ {
		_, _ = s.DialContext(context.Background(), "tcp", "cdn.example.com:443")
	}

	want := []string{
		"1.1.1.1:443", "2.2.2.2:443", "3.3.3.3:443",
		"1.1.1.1:443", "2.2.2.2:443", "3.3.3.3:443",
	}
	if len(dialed) != len(want) {
		t.Fatalf("期望拨号 %d 次，实际=%d (%v)", len(want), len(dialed), dialed)
	}
	for i, w := range want {
		if dialed[i] != w {
			t.Errorf("第 %d 次拨号 addr=%s，期望=%s", i, dialed[i], w)
		}
	}
}

// TestDialFallbackEmptyPool 验证探测全灭（赢家池为空）时 DialContext 透传原
// addr 给 dialFn，而不是拨向一个不存在的赢家。
func TestDialFallbackEmptyPool(t *testing.T) {
	s := newTestSelector(t, baseConfig()) // s.winners 为零值空切片

	var gotAddr string
	s.dialFn = func(ctx context.Context, network, addr string) (net.Conn, error) {
		gotAddr = addr
		return nil, errors.New("测试桩")
	}

	_, _ = s.DialContext(context.Background(), "tcp", "cdn.example.com:443")

	if gotAddr != "cdn.example.com:443" {
		t.Fatalf("赢家池为空时应原样透传 addr，实际=%s", gotAddr)
	}
}

// TestDialPassthroughUnknownHost 验证非注册域名的连接请求原样透传，不受赢家
// 表影响（即便赢家池非空）。
func TestDialPassthroughUnknownHost(t *testing.T) {
	s := newTestSelector(t, baseConfig())
	s.winners = []netip.Addr{netip.MustParseAddr("1.1.1.1")}

	var gotAddr string
	s.dialFn = func(ctx context.Context, network, addr string) (net.Conn, error) {
		gotAddr = addr
		return nil, errors.New("测试桩")
	}

	_, _ = s.DialContext(context.Background(), "tcp", "other.example.com:443")

	if gotAddr != "other.example.com:443" {
		t.Fatalf("非注册域名应原样透传，实际=%s", gotAddr)
	}
}

// TestRefreshSwapsWinners 验证第二轮探测结果与第一轮不同时，Winners() 反映
// 最新一轮的结果（赢家表整体替换而非累加）。
func TestRefreshSwapsWinners(t *testing.T) {
	s := newTestSelector(t, baseConfig())

	ipA := netip.MustParseAddr("1.1.1.1")
	ipB := netip.MustParseAddr("2.2.2.2")

	s.discoverers = []discoverFunc{
		func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{ipA, ipB}, nil
		},
	}

	var round int32
	s.probe = func(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error) {
		fast, slow := ipA, ipB
		if atomic.LoadInt32(&round) != 0 {
			fast, slow = ipB, ipA
		}
		if ip == fast {
			return probeResult{TTFB: 10 * time.Millisecond}, nil
		}
		_ = slow
		return probeResult{TTFB: 50 * time.Millisecond}, nil
	}

	s.refresh(context.Background())
	first := s.Winners()
	if len(first) == 0 || first[0] != ipA {
		t.Fatalf("第一轮期望赢家[0]=%s，实际=%v", ipA, first)
	}

	atomic.StoreInt32(&round, 1)
	s.refresh(context.Background())
	second := s.Winners()
	if len(second) == 0 || second[0] != ipB {
		t.Fatalf("第二轮期望赢家[0]=%s，实际=%v", ipB, second)
	}
}

// TestNewSelectorValidation 验证 ProbeURLs 为空、或其中某个 URL 无法解析出
// host 时，NewSelector 返回 error。
func TestNewSelectorValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "空 ProbeURLs",
			cfg:  Config{},
		},
		{
			name: "URL 语法非法",
			cfg:  Config{ProbeURLs: []string{"://not-a-url"}},
		},
		{
			name: "URL 无 host",
			cfg:  Config{ProbeURLs: []string{"/just/a/path"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSelector(tt.cfg); err == nil {
				t.Fatal("期望 NewSelector 返回 error")
			}
		})
	}
}
