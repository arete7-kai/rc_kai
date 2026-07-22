package security

import (
	"net"
	"testing"
)

// TestIsBlockedIP 验证 SSRF 黑名单判断：内网/回环/链路本地 → true，公网 → false。
func TestIsBlockedIP(t *testing.T) {
	tests := []struct {
		ip   string
		want bool
	}{
		// 内网 / 回环 / 链路本地：必须拦。
		{"10.0.0.1", true},
		{"10.255.255.255", true},
		{"172.16.0.1", true},
		{"172.31.255.255", true},
		{"192.168.1.1", true},
		{"127.0.0.1", true},
		{"169.254.169.254", true}, // 云元数据地址
		{"0.0.0.0", true},
		{"::1", true},
		{"fe80::1", true}, // IPv6 链路本地
		{"fc00::1", true}, // IPv6 ULA
		// 公网：必须放行。
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"93.184.216.34", false},                       // example.com
		{"2606:2800:220:1:248:1893:25c8:1946", false},  // 公网 IPv6
		{"172.32.0.1", false},                          // 刚好在 172.16/12 之外
		{"172.15.255.255", false},                      // 刚好在 172.16/12 之外（下界）
	}
	for _, tt := range tests {
		ip := net.ParseIP(tt.ip)
		if ip == nil {
			t.Fatalf("测试用例 IP 非法: %q", tt.ip)
		}
		if got := isBlockedIP(ip); got != tt.want {
			t.Errorf("isBlockedIP(%s) = %v, want %v", tt.ip, got, tt.want)
		}
	}
}
