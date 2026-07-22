// Package security 负责出站投递的目标校验与 SSRF 防护。
//
// D3: 投递目标只能来自 destinations 注册表——worker 投递的 URL 是入队时从注册表快照来的，
// 系统里没有“调用方传任意 URL”的代码路径。本包在此基础上再加一层网络级兜底：即便是已登记的
// 目标，也不允许其解析到内网/回环地址。
//
// SSRF: 关键在于校验“实际要连接的 IP”，而不是 URL 里的 host 字符串——否则域名可被解析到内网
// （DNS rebinding）。因此真正的把关放在 NewClient 里 net.Dialer.Control：它在建连前拿到解析
// 后的具体 IP，逐个校验。ValidateTarget 是一层“早失败”的预校验，不替代连接时校验。
package security

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"
)

// ErrBlockedIP 表示目标命中内网/回环黑名单（SSRF）。worker 用 errors.Is 识别它并判为不可重试。
var ErrBlockedIP = errors.New("security: blocked internal/loopback address")

// blockedNets 是 SSRF 黑名单网段：内网、回环、链路本地等，绝不允许投递目标落在这些网段。
// SSRF: 覆盖 CLAUDE.md/README §6 点名的网段 + 明显该拦的 IPv6 对应网段。
var blockedNets = mustParseCIDRs(
	"10.0.0.0/8",     // 私有网 A
	"172.16.0.0/12",  // 私有网 B
	"192.168.0.0/16", // 私有网 C
	"127.0.0.0/8",    // IPv4 回环
	"169.254.0.0/16", // 链路本地（含云元数据 169.254.169.254）
	"0.0.0.0/8",      // “这台主机”本机地址
	"::1/128",        // IPv6 回环
	"fc00::/7",       // IPv6 唯一本地地址 (ULA)
	"fe80::/10",      // IPv6 链路本地
)

func mustParseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic(fmt.Sprintf("security: bad CIDR %q: %v", c, err))
		}
		out = append(out, n)
	}
	return out
}

// isBlockedIP 判断一个已解析出的 IP 是否落在黑名单网段。
// SSRF: 必须对“实际要连接的 IP”调用它，而不是对 host 字符串。
func isBlockedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}
	for _, n := range blockedNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// ValidateTarget 在投递前对目标 URL 做预校验：scheme 合法、host 非空、预解析出的 IP 不在黑名单。
// 这是一层“早失败 + 清晰原因”的防御；真正防 DNS rebinding 的是 NewClient 里连接时的 IP 校验。
// SSRF: 命中黑名单时返回包裹 ErrBlockedIP 的错误。
func ValidateTarget(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("security: 目标 URL 无法解析: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("security: 只允许 http/https，收到 %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("security: 目标 URL 缺少 host")
	}

	// host 本身是 IP：直接校验。
	if ip := net.ParseIP(host); ip != nil {
		if isBlockedIP(ip) {
			return fmt.Errorf("security: 目标 %s 命中内网/回环黑名单: %w", ip, ErrBlockedIP)
		}
		return nil
	}

	// 域名：解析所有 IP，任一命中黑名单即拒绝。
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return fmt.Errorf("security: 解析 host %q 失败: %w", host, err)
	}
	for _, ip := range ips {
		if isBlockedIP(ip) {
			return fmt.Errorf("security: host %q 解析到内网/回环地址 %s: %w", host, ip, ErrBlockedIP)
		}
	}
	return nil
}

// ClientConfig 配置投递用 HTTP 客户端的超时。两个超时都必须设，别用默认（默认无超时会让 worker 卡死）。
type ClientConfig struct {
	DialTimeout  time.Duration // 建连超时（含 TLS 握手）
	TotalTimeout time.Duration // 整体请求超时（含读取响应）
}

// NewClient 构造一个 SSRF 安全、带超时的 *http.Client：
//   - net.Dialer.Control 在建连前校验解析出的实际 IP（防 DNS rebinding）——真正的 SSRF 把关点；
//   - 禁止跟随重定向（3xx 可能把请求引向内网，也是 SSRF 向量）；
//   - 连接超时 + 整体超时都显式设置（README §4：别用默认无超时）。
func NewClient(cfg ClientConfig) *http.Client {
	dialer := &net.Dialer{
		Timeout: cfg.DialTimeout,
		// SSRF: Control 在建连前拿到解析后的具体地址，逐个 IP 校验，防住 DNS rebinding。
		Control: func(network, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("security: 无法解析连接地址 %q: %w", address, err)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("security: 连接地址 %q 不是合法 IP", host)
			}
			if isBlockedIP(ip) {
				return fmt.Errorf("security: 拒绝连接内网/回环地址 %s: %w", ip, ErrBlockedIP)
			}
			return nil
		},
	}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: cfg.DialTimeout,
	}
	return &http.Client{
		Timeout:   cfg.TotalTimeout,
		Transport: transport,
		// SSRF: 不跟随重定向，避免被 3xx 引向内网目标。
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
