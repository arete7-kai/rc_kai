// Package worker 是投递引擎：一个 goroutine 池循环从 store 领取到期任务，带超时地发起 HTTP
// 投递，并按结果做错误分类 → 成功 / 退避重排 / 进死信（对应 README §4.5 与 DECISIONS D4）。
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"rc_kai/internal/security"
	"rc_kai/internal/store"
)

// statusWriteGrace 是投递上下文在 HTTP 整体超时之外，为写回投递结果额外预留的余量，
// 避免 HTTP 恰好用满超时预算后写状态的 ctx 已过期。
const statusWriteGrace = 5 * time.Second

// Config 是投递引擎的可调参数。
type Config struct {
	Workers      int           // worker goroutine 数量
	BatchSize    int           // 每次 ClaimDue 领取的批量上限
	PollInterval time.Duration // 空闲时的轮询间隔
	BaseBackoff  time.Duration // 指数退避基数（base）
	DialTimeout  time.Duration // 建连超时
	TotalTimeout time.Duration // 整体请求超时
}

// Pool 是投递 worker 池。
type Pool struct {
	store  *store.Store
	client *http.Client
	cfg    Config
	log    *slog.Logger
}

// New 构造 worker 池，内部用 security.NewClient 装配一个 SSRF 安全、带超时的 HTTP 客户端。
func New(st *store.Store, cfg Config, log *slog.Logger) *Pool {
	client := security.NewClient(security.ClientConfig{
		DialTimeout:  cfg.DialTimeout,
		TotalTimeout: cfg.TotalTimeout,
	})
	return &Pool{store: st, client: client, cfg: cfg, log: log}
}

// Run 启动 worker 池并阻塞，直到 ctx 取消。多个 worker 并发调用 ClaimDue，
// 靠 FOR UPDATE SKIP LOCKED 互不抢占（D2）。
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.loop(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id int) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.drain(ctx, id)
		}
	}
}

// drain 反复领取并投递，直到某次领不满一批（说明当前没有更多到期任务）。
func (p *Pool) drain(ctx context.Context, id int) {
	for {
		if ctx.Err() != nil {
			return
		}
		tasks, err := p.store.ClaimDue(ctx, p.cfg.BatchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return // shutdown 中，正常停止领新活
			}
			p.log.Error("claim due failed", "worker", id, "err", err)
			return
		}
		for i := range tasks {
			if ctx.Err() != nil {
				return // shutdown：停止启动新任务；已在途的由 deliver 独立 ctx 自行收尾
			}
			p.deliver(&tasks[i])
		}
		if len(tasks) < p.cfg.BatchSize {
			return
		}
	}
}

// deliver 投递单条通知，并按结果更新状态。
//
// 投递用独立于 shutdown 的上下文：收到关闭信号时在途投递不被立即取消，让它自然收尾；
// 总收尾上限由 main 的 SHUTDOWN_GRACE 统一兜底，到点仍未完成的靠租约回收(§4.7)重投。
func (p *Pool) deliver(t *store.Notification) {
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.TotalTimeout+statusWriteGrace)
	defer cancel()

	// D3: 目标 URL 来自入队时从注册表快照的 t.URL，worker 从不接受调用方任意 URL。
	// SSRF: 先做预校验（连接时的 IP 校验在 security.NewClient 的 Control 里再做一次，防 DNS rebinding）。
	if err := security.ValidateTarget(ctx, t.URL); err != nil {
		p.log.Warn("target rejected", "id", t.ID, "err", err)
		p.markDead(ctx, t, 0, "target rejected: "+err.Error())
		return
	}

	req, err := http.NewRequestWithContext(ctx, t.Method, t.URL, bytes.NewReader(t.Body))
	if err != nil {
		p.markDead(ctx, t, 0, "build request failed: "+err.Error())
		return
	}
	applyHeaders(req, t.Headers)
	// D1: 透传幂等键给上游，供其去重（at-least-once 下重复投递的兜底之一）。
	if t.IdempotencyKey.Valid {
		req.Header.Set("Idempotency-Key", t.IdempotencyKey.String)
	}

	// 带超时的 HTTP 调用（超时在 security.NewClient 里设，连接超时 + 整体超时都有）。
	resp, err := p.client.Do(req)
	if err != nil {
		if errors.Is(err, security.ErrBlockedIP) {
			// SSRF: 连接时命中内网（DNS rebinding）→ 不可重试，直接死信。
			p.log.Warn("blocked at dial", "id", t.ID, "err", err)
			p.markDead(ctx, t, 0, "ssrf blocked: "+err.Error())
			return
		}
		// 连接错误 / 超时 → 可重试（README §4.5）。
		p.retryOrDead(ctx, t, 0, "transport error: "+err.Error())
		return
	}
	defer resp.Body.Close()
	// 读掉少量响应体以便连接复用；业务方不关心返回值，故不解析内容。
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32<<10))

	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		// 2xx → 成功（README §4.5）。
		if err := p.store.MarkDelivered(ctx, t.ID, code); err != nil {
			p.log.Error("mark delivered failed", "id", t.ID, "err", err)
			return
		}
		p.log.Info("delivered", "id", t.ID, "dest", t.DestinationID, "code", code)
	case code == 429 || (code >= 500 && code <= 599):
		// 429 / 5xx → 可重试（README §4.5）。
		p.retryOrDead(ctx, t, code, fmt.Sprintf("upstream status %d", code))
	case code >= 400 && code < 500:
		// 其余 4xx → 不可重试，直接死信（README §4.5）。
		p.markDead(ctx, t, code, fmt.Sprintf("non-retryable status %d", code))
	default:
		// 1xx/3xx（已禁止跟随重定向）视为不可投递，进死信等人工排查。
		p.markDead(ctx, t, code, fmt.Sprintf("unexpected status %d", code))
	}
}

// retryOrDead 对可重试失败做决策：未到上限则退避重排，到上限则进死信（README §4.5, D4）。
// 重试上限 / 退避上限均取自 notification 上的“入队快照”（t.MaxAttempts / t.BackoffCapSecs），
// worker 不再回查注册表——在途任务行为可预测优先于配置即时生效。
func (p *Pool) retryOrDead(ctx context.Context, t *store.Notification, code int, reason string) {
	nextAttempts := t.Attempts + 1
	if nextAttempts >= t.MaxAttempts {
		p.markDead(ctx, t, code, fmt.Sprintf("max attempts (%d) reached: %s", t.MaxAttempts, reason))
		return
	}
	delay := p.backoff(t.Attempts, time.Duration(t.BackoffCapSecs)*time.Second)
	next := time.Now().Add(delay)
	if err := p.store.MarkRetry(ctx, t.ID, next, code, reason); err != nil {
		p.log.Error("mark retry failed", "id", t.ID, "err", err)
		return
	}
	p.log.Info("scheduled retry", "id", t.ID, "dest", t.DestinationID,
		"attempt", nextAttempts, "max", t.MaxAttempts, "in", delay.String(), "reason", reason)
}

// markDead 把通知打入死信并记原因（D4: 死信是可查询、可重投的结构化记录，不是只写日志）。
func (p *Pool) markDead(ctx context.Context, t *store.Notification, code int, reason string) {
	if err := p.store.MarkDead(ctx, t.ID, code, reason); err != nil {
		p.log.Error("mark dead failed", "id", t.ID, "err", err)
		return
	}
	// D4: 是否告警由注册表的 alert_on_dead 决定；v1 先落到 WARN 日志，真正的告警通道后续接。
	p.log.Warn("dead-lettered", "id", t.ID, "dest", t.DestinationID, "code", code, "reason", reason)
}

// backoff: 指数退避 + 抖动（README §4.5, D4）。min(base*2^attempts, cap) 后叠加抖动，
// 取 [50%,100%) 区间，避免供应商恢复瞬间的重试风暴（thundering herd）。
func (p *Pool) backoff(attempts int, capDur time.Duration) time.Duration {
	d := float64(p.cfg.BaseBackoff) * math.Pow(2, float64(attempts))
	capped := time.Duration(d)
	if capped <= 0 || capped > capDur {
		capped = capDur
	}
	half := capped / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// applyHeaders 把注册表里存的 jsonb Header（string→string 对象）应用到请求上。
// 注册表应保证 Header 合法；解析失败则跳过（不阻断投递）。
func applyHeaders(req *http.Request, raw []byte) {
	if len(raw) == 0 {
		return
	}
	var h map[string]string
	if err := json.Unmarshal(raw, &h); err != nil {
		return
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
}
