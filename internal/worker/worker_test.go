package worker

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"rc_kai/internal/security"
)

// TestBackoff 验证退避计算：结果落在 [cap/2, cap] 区间、随 attempt 递增（触顶前）、
// 不超过 backoff_cap、且确有抖动（同一 attempt 的多次采样不恒定）。
func TestBackoff(t *testing.T) {
	const base = 1 * time.Second
	const capDur = 30 * time.Second
	const samples = 3000

	p := &Pool{cfg: Config{BaseBackoff: base}}

	var prevAvg time.Duration
	for attempt := 0; attempt <= 8; attempt++ {
		expected := time.Duration(float64(base) * math.Pow(2, float64(attempt)))
		if expected <= 0 || expected > capDur {
			expected = capDur // 触顶
		}
		lo, hi := expected/2, expected

		minObs, maxObs, sum := time.Duration(math.MaxInt64), time.Duration(0), time.Duration(0)
		for i := 0; i < samples; i++ {
			d := p.backoff(attempt, capDur)
			if d < lo || d > hi {
				t.Fatalf("attempt %d: %v 越出预期区间 [%v, %v]", attempt, d, lo, hi)
			}
			if d > capDur {
				t.Fatalf("attempt %d: %v 超过 cap %v", attempt, d, capDur)
			}
			if d < minObs {
				minObs = d
			}
			if d > maxObs {
				maxObs = d
			}
			sum += d
		}

		// 有抖动：区间够宽时应观测到变化，而不是常数。
		if minObs == maxObs {
			t.Errorf("attempt %d: 未观测到抖动 (min==max==%v)", attempt, minObs)
		}

		// 随 attempt 递增：未触顶前每级均值应显著上升。
		avg := sum / samples
		if attempt > 0 && expected < capDur && avg <= prevAvg {
			t.Errorf("attempt %d: 均值 %v 未随 attempt 递增（上一级 %v）", attempt, avg, prevAvg)
		}
		prevAvg = avg
	}
}

// TestClassify 表驱动覆盖各类结果的归类（README §4.5）。重点：429 与其它 4xx 相反。
func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		code int
		err  error
		want outcome
	}{
		{"200 成功", 200, nil, outcomeDelivered},
		{"204 成功", 204, nil, outcomeDelivered},
		{"400 不可重试→死信", 400, nil, outcomeDead},
		{"401 不可重试→死信", 401, nil, outcomeDead},
		{"404 不可重试→死信", 404, nil, outcomeDead},
		{"429 可重试（与其它 4xx 相反）", 429, nil, outcomeRetry},
		{"500 可重试", 500, nil, outcomeRetry},
		{"503 可重试", 503, nil, outcomeRetry},
		{"超时/连接错误 可重试", 0, errors.New("dial tcp: i/o timeout"), outcomeRetry},
		{"SSRF 命中 不可重试→死信", 0, fmt.Errorf("blocked: %w", security.ErrBlockedIP), outcomeDead},
		{"302 重定向（不跟随）→死信", 302, nil, outcomeDead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classify(tt.code, tt.err); got != tt.want {
				t.Errorf("classify(%d, %v) = %d, want %d", tt.code, tt.err, got, tt.want)
			}
		})
	}
}
