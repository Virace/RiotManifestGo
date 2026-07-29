// Package edge 提供 Riot CDN 边缘节点的候选发现、探测打分与拨号接管：多源
// DNS 发现候选 IP → HTTP Range 探测打分 → 取最快的若干个作为"赢家" →
// DialContext 拦截对已注册域名的连接，改拨到赢家 IP（域名本身保持不变，
// SNI/证书校验不受影响）。
//
// 兜底语义贯穿全链路：任意环节失败都不劣于"不启用"——发现全灭、探测全灭、
// 单个域名未注册，DialContext 均原样透传给默认拨号，行为等价于完全不启用
// edge 选路。
package edge

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// 探测并行度上限：单轮 refresh 中同时进行的 HTTP 探测请求数。
const probeConcurrency = 8

// probeResult 记录单次探测的关键耗时指标。
type probeResult struct {
	TTFB  time.Duration // 发起请求到响应头到达的耗时（含 TCP+TLS 建连），打分主口径
	Total time.Duration // 发起请求到探测体读取完成的总耗时
}

// discoverFunc 是候选 IP 发现源的统一签名：给定被接管域名 host，返回该域名下
// 可用的候选 IPv4 地址；查询失败返回 error，调用方按"单源失败静默跳过"处理，
// 不影响其余源与其余域名。
type discoverFunc func(ctx context.Context, host string) ([]netip.Addr, error)

// probeFunc 是候选 IP 打分探测的统一签名：对 probeURL 对应域名发起一次性 HTTP
// Range 探测，固定拨号到 ip，读取 limit 字节，返回耗时指标；探测失败（超时、
// 连接失败、非 200/206 响应、读取失败）返回 error。
type probeFunc func(ctx context.Context, probeURL string, ip netip.Addr, limit int64) (probeResult, error)

// Config 是 NewSelector 的构造配置。
type Config struct {
	// ProbeURLs 为每个待接管 CDN 域名提供一个完整探测 URL（例如
	// "https://cdn.example.com/probe-object"）；该 URL 的 host 即为被接管的
	// 域名——发现阶段解析这个域名的候选 IP，DialContext 只拦截对这些域名的
	// 连接。不能为空，且每个 URL 都必须能解析出非空 host。
	ProbeURLs []string

	// ProbeBytes 是每次探测请求的 Range 字节数，<=0 时取默认值 1<<20（1MiB）。
	ProbeBytes int64

	// Winners 是每轮刷新后保留的赢家数量，<=0 时取默认值 3。
	Winners int

	// RefreshEvery 是后台刷新间隔，<=0 时取默认值 60s。
	RefreshEvery time.Duration

	// Logf 是可选的日志回调，用于观测每轮候选数/赢家 IP 与 TTFB；nil 表示
	// 静默——edge 包自身不做任何打印，全部输出经由此回调。
	Logf func(format string, args ...any)
}

// Selector 维护一组被接管 CDN 域名的候选发现、探测打分与赢家轮转拨号状态。
type Selector struct {
	cfg Config

	// hosts 是按 ProbeURLs 顺序去重后的注册域名列表，决定 refresh 中并行发现
	// 任务的展开顺序。
	hosts []string
	// hostProbeURL 记录每个注册域名对应的探测 URL。
	hostProbeURL map[string]string
	// hostSet 用于 DialContext 快速判断目标 host 是否命中注册域名。
	hostSet map[string]struct{}

	// discoverers 是候选 IP 发现源集合；同包测试可整体替换以注入 mock。
	discoverers []discoverFunc
	// probe 是候选 IP 打分函数；同包测试可替换以注入 mock。
	probe probeFunc
	// dialFn 是实际建立连接的函数，默认 net.Dialer.DialContext；同包测试可
	// 替换以观测/拦截拨号目标，避免真实网络访问。
	dialFn func(ctx context.Context, network, addr string) (net.Conn, error)

	// mu 保护 winners 的整体替换（refresh 写、Winners/DialContext 读）。
	mu      sync.RWMutex
	winners []netip.Addr

	// rotate 是赢家轮转的原子计数器，支持并发 DialContext 调用下的轮转分布。
	rotate atomic.Uint64
}

// NewSelector 校验配置、填充默认值并装配默认发现源与探测函数。ProbeURLs 为
// 空，或其中任一 URL 无法解析出非空 host，均返回 error。此阶段不做任何网络
// 访问——真正的 DNS 发现与 HTTP 探测发生在 Start 触发的 refresh 中。
func NewSelector(cfg Config) (*Selector, error) {
	if len(cfg.ProbeURLs) == 0 {
		return nil, fmt.Errorf("edge: ProbeURLs 不能为空")
	}
	if cfg.ProbeBytes <= 0 {
		cfg.ProbeBytes = 1 << 20
	}
	if cfg.Winners <= 0 {
		cfg.Winners = 3
	}
	if cfg.RefreshEvery <= 0 {
		cfg.RefreshEvery = 60 * time.Second
	}

	hosts := make([]string, 0, len(cfg.ProbeURLs))
	hostProbeURL := make(map[string]string, len(cfg.ProbeURLs))
	hostSet := make(map[string]struct{}, len(cfg.ProbeURLs))
	for _, raw := range cfg.ProbeURLs {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("edge: 无法解析探测 URL %q: %w", raw, err)
		}
		host := u.Hostname()
		if host == "" {
			return nil, fmt.Errorf("edge: 探测 URL %q 无法解析出 host", raw)
		}
		if _, exists := hostSet[host]; !exists {
			hosts = append(hosts, host)
			hostSet[host] = struct{}{}
		}
		hostProbeURL[host] = raw
	}

	return &Selector{
		cfg:          cfg,
		hosts:        hosts,
		hostProbeURL: hostProbeURL,
		hostSet:      hostSet,
		discoverers:  defaultDiscoverers(),
		probe:        httpRangeProbe,
		dialFn:       (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}, nil
}

// Start 同步执行首轮 refresh（发现/探测失败不返回 error，赢家池为空即回退到
// 透传默认拨号），随后启动后台 goroutine 按 RefreshEvery 周期重复 refresh；
// 该 goroutine 随 ctx 取消而退出，不留常驻任务。
func (s *Selector) Start(ctx context.Context) {
	s.refresh(ctx)

	go func() {
		ticker := time.NewTicker(s.cfg.RefreshEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refresh(ctx)
			}
		}
	}()
}

// Winners 返回当前赢家 IP 列表的快照（按上一轮探测 TTFB 升序），赢家池为空
// 时返回长度为 0 的切片。返回值为拷贝，调用方修改不影响内部状态。
func (s *Selector) Winners() []netip.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]netip.Addr, len(s.winners))
	copy(out, s.winners)
	return out
}

// DialContext 拦截对注册域名的连接请求：目标 host 命中注册域名集且赢家池非空
// 时，按原子计数器轮转选取一个赢家 IP，替换 addr 的 host 部分后交给 dialFn；
// 否则（addr 格式不含端口、host 非注册域名、或赢家池为空）原样透传 addr 给
// dialFn，退化为默认拨号行为——这是发现/探测全灭或域名未接管时的兜底路径。
func (s *Selector) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return s.dialFn(ctx, network, addr)
	}

	if _, ok := s.hostSet[host]; !ok {
		return s.dialFn(ctx, network, addr)
	}

	s.mu.RLock()
	winners := s.winners
	s.mu.RUnlock()

	if len(winners) == 0 {
		return s.dialFn(ctx, network, addr)
	}

	idx := s.rotate.Add(1) - 1
	winner := winners[idx%uint64(len(winners))]
	return s.dialFn(ctx, network, net.JoinHostPort(winner.String(), port))
}

// refresh 执行一整轮候选发现 → 探测打分 → 赢家表更新：对每个注册域名并行跑
// 全部发现源，候选按 IPv4 地址去重后并行探测（限流 probeConcurrency），最终
// 按 TTFB 升序取前 Winners 个整体替换赢家表。候选为零或探测全部失败都会让
// 赢家表变为空，DialContext 随之回退到透传默认拨号。
func (s *Selector) refresh(ctx context.Context) {
	candidates := s.discoverCandidates(ctx)
	ranked := s.probeAndRank(ctx, candidates)

	n := s.cfg.Winners
	if n > len(ranked) {
		n = len(ranked)
	}
	winners := make([]netip.Addr, n)
	for i := 0; i < n; i++ {
		winners[i] = ranked[i].addr
	}

	s.mu.Lock()
	s.winners = winners
	s.mu.Unlock()

	s.logf("edge: 本轮候选=%d 赢家=%d", len(candidates), len(winners))
	for i := 0; i < n; i++ {
		s.logf("edge: 赢家[%d]=%s ttfb=%s", i, ranked[i].addr, ranked[i].TTFB)
	}
}

// discoverJob 是单个(域名, 发现源)组合的产出：该域名下发现的候选 IP，以及
// 这些候选后续探测时应使用的探测 URL。
type discoverJob struct {
	addrs    []netip.Addr
	probeURL string
}

// discoverCandidates 对每个注册域名并行运行全部发现源，将结果按 IPv4 地址去重
// 合并为候选集合；一个候选被多个域名/发现源发现时，探测 URL 取首个到达的结果
// （任取其一，语义上等价，因为赢家池不区分来源域名）。单个发现源失败只记录
// 日志，不影响其余源与其余域名的候选。
func (s *Selector) discoverCandidates(ctx context.Context) map[netip.Addr]string {
	var wg sync.WaitGroup
	jobs := make(chan discoverJob, len(s.hosts)*len(s.discoverers))

	for _, host := range s.hosts {
		probeURL := s.hostProbeURL[host]
		for _, d := range s.discoverers {
			wg.Add(1)
			go func(host string, d discoverFunc) {
				defer wg.Done()
				addrs, err := d(ctx, host)
				if err != nil {
					s.logf("edge: 发现源失败 host=%s err=%v", host, err)
					return
				}
				jobs <- discoverJob{addrs: addrs, probeURL: probeURL}
			}(host, d)
		}
	}
	go func() {
		wg.Wait()
		close(jobs)
	}()

	merged := make(map[netip.Addr]string)
	for job := range jobs {
		for _, addr := range job.addrs {
			if !addr.Is4() && !addr.Is4In6() {
				continue // 仅接管 IPv4：候选与后续探测/拨号统一 IPv4
			}
			addr = addr.Unmap() // 归一化 4-in-6 表示，避免同一 IP 因表示不同而重复入池
			if _, exists := merged[addr]; !exists {
				merged[addr] = job.probeURL
			}
		}
	}
	return merged
}

// winnerCandidate 是探测成功的候选，携带其耗时指标以便按 TTFB 排序。
type winnerCandidate struct {
	addr netip.Addr
	probeResult
}

// probeAndRank 并行探测候选集合（信号量限流 probeConcurrency），失败者
// （超时/连接失败/非 200|206/读取失败）静默出池并记录日志，成功者按 TTFB
// 升序返回。
func (s *Selector) probeAndRank(ctx context.Context, candidates map[netip.Addr]string) []winnerCandidate {
	type probed struct {
		addr netip.Addr
		res  probeResult
		err  error
	}

	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	resCh := make(chan probed, len(candidates))

	for addr, probeURL := range candidates {
		wg.Add(1)
		go func(addr netip.Addr, probeURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res, err := s.probe(ctx, probeURL, addr, s.cfg.ProbeBytes)
			resCh <- probed{addr: addr, res: res, err: err}
		}(addr, probeURL)
	}
	go func() {
		wg.Wait()
		close(resCh)
	}()

	winners := make([]winnerCandidate, 0, len(candidates))
	for p := range resCh {
		if p.err != nil {
			s.logf("edge: 探测失败 addr=%s err=%v", p.addr, p.err)
			continue
		}
		winners = append(winners, winnerCandidate{addr: p.addr, probeResult: p.res})
	}

	sort.Slice(winners, func(i, j int) bool {
		return winners[i].TTFB < winners[j].TTFB
	})
	return winners
}

// logf 是 Config.Logf 的零值安全包装：Logf 为 nil 时静默丢弃，保证 edge 包
// 自身不产生任何打印。
func (s *Selector) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf(format, args...)
	}
}
