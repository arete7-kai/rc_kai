// Command relay 是通知中继服务的进程入口：单进程内装配 HTTP API + worker 池 + 租约回收循环，
// 共享一个 Postgres（DB 即队列，D2）。配置全部走环境变量，不硬编码，不含密钥。
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	// pgx 走标准 database/sql 接口（匿名 import 注册 "pgx" 驱动，SQL 写法不变）。
	_ "github.com/jackc/pgx/v5/stdlib"

	"rc_kai/internal/api"
	"rc_kai/internal/store"
	"rc_kai/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// 数据库连接串是唯一必填项，缺失即快速失败（绝不硬编码默认连接串/密钥）。
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	workerCount := getenvInt("WORKER_COUNT", 4, log)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Error("open db failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	// 连接池上限略高于 worker 数，给 api 与回收循环留余量。
	db.SetMaxOpenConns(workerCount + 4)

	// 启动即 ping，连不上就别假装健康。
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(pingCtx); err != nil {
		cancelPing()
		log.Error("db ping failed", "err", err)
		os.Exit(1)
	}
	cancelPing()

	st := store.New(db)

	// 信号驱动的根上下文：SIGINT/SIGTERM 触发优雅关闭。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool := worker.New(st, worker.Config{
		Workers:      workerCount,
		BatchSize:    getenvInt("WORKER_BATCH_SIZE", 10, log),
		PollInterval: getenvDuration("WORKER_POLL_INTERVAL", time.Second, log),
		BaseBackoff:  getenvDuration("WORKER_BASE_BACKOFF", time.Second, log),
		DialTimeout:  getenvDuration("HTTP_DIAL_TIMEOUT", 5*time.Second, log),
		TotalTimeout: getenvDuration("HTTP_TOTAL_TIMEOUT", 10*time.Second, log),
	}, log)

	leaseTimeout := getenvDuration("LEASE_TIMEOUT", 60*time.Second, log)
	reclaimInterval := getenvDuration("RECLAIM_INTERVAL", 30*time.Second, log)
	shutdownGrace := getenvDuration("SHUTDOWN_GRACE", 30*time.Second, log)
	addr := getenv("RELAY_ADDR", ":8080")

	srv := &http.Server{Addr: addr, Handler: api.New(st, log).Routes()}

	var wg sync.WaitGroup

	// 1) HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("http server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
			stop() // 监听失败也触发整体关闭
		}
	}()

	// 2) worker 池：ctx 取消后停止领新活，在途投递自行收尾后返回。
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Run(ctx)
	}()

	// 3) 租约回收循环（§4.7）：定期把卡在 DELIVERING 的行重置回 PENDING。
	wg.Add(1)
	go func() {
		defer wg.Done()
		reclaimLoop(ctx, st, reclaimInterval, leaseTimeout, log)
	}()

	log.Info("relay started", "workers", workerCount, "poll", "see config")

	// 等待关闭信号。
	<-ctx.Done()
	log.Info("shutdown signal received, draining", "grace", shutdownGrace.String())

	// 整体收尾上限：到点仍未收尾就强制退出，剩余在途交给 ReclaimExpired 重投。
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancelShutdown()

	// 先停 ingress 并 drain 在处理的 HTTP handler。
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown incomplete", "err", err)
	}

	// 等待 worker 池 + 回收循环收尾，受 shutdownGrace 上限约束。
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		log.Info("all workers drained, exiting cleanly")
	case <-shutdownCtx.Done():
		log.Warn("shutdown grace exceeded, forcing exit; in-flight tasks will be reclaimed",
			"grace", shutdownGrace.String())
	}
}

// reclaimLoop 周期性回收租约超时的在途任务（§4.7 崩溃恢复）。
func reclaimLoop(ctx context.Context, st *store.Store, interval, leaseTimeout time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := st.ReclaimExpired(ctx, leaseTimeout)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Error("reclaim expired failed", "err", err)
				continue
			}
			if n > 0 {
				log.Info("reclaimed stuck deliveries", "count", n)
			}
		}
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int, log *slog.Logger) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Warn("invalid int env, using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

func getenvDuration(key string, def time.Duration, log *slog.Logger) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Warn("invalid duration env, using default", "key", key, "value", v, "default", def.String())
		return def
	}
	return d
}
