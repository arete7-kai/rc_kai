// Command mock 是一个可控失败的 mock upstream，供 demo/测试用。
// 用 query 参数控制行为：
//
//	?behavior=ok             稳定 200
//	?behavior=fail           稳定 500
//	?behavior=reject         稳定 400（不可重试，演示直接进死信）
//	?behavior=flaky&n=2      前 n 次 500、之后 200（演示重试后成功）；按 Idempotency-Key/key 分组计数
//	?behavior=timeout&ms=15000  先 sleep 再 200（演示超时可重试；ms 应 > relay 的整体超时）
//
// 每次请求都打一条日志，便于在 demo 里肉眼看到重试过程。
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	mu       sync.Mutex
	attempts = map[string]int{} // flaky 计数，按 key 分组
)

func main() {
	addr := getenv("MOCK_ADDR", ":9090")
	http.HandleFunc("/", handle)
	log.Printf("mock upstream listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("mock upstream failed: %v", err)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	behavior := q.Get("behavior")
	if behavior == "" {
		behavior = "ok"
	}
	// flaky 按逻辑消息分组计数：优先用透传的 Idempotency-Key（也顺带证明了 D1 的透传），
	// 退而用 ?key=，再退到全局。
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = q.Get("key")
	}
	if key == "" {
		key = "default"
	}

	switch behavior {
	case "ok":
		respond(w, r, behavior, key, http.StatusOK, "ok")
	case "fail":
		respond(w, r, behavior, key, http.StatusInternalServerError, "always fail")
	case "reject":
		respond(w, r, behavior, key, http.StatusBadRequest, "rejected (non-retryable)")
	case "flaky":
		n := atoiDefault(q.Get("n"), 2)
		mu.Lock()
		attempts[key]++
		c := attempts[key]
		mu.Unlock()
		if c <= n {
			respond(w, r, behavior, key, http.StatusInternalServerError, "flaky fail "+strconv.Itoa(c)+"/"+strconv.Itoa(n))
		} else {
			respond(w, r, behavior, key, http.StatusOK, "flaky ok on attempt "+strconv.Itoa(c))
		}
	case "timeout":
		ms := atoiDefault(q.Get("ms"), 15000)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		respond(w, r, behavior, key, http.StatusOK, "late ok")
	default:
		respond(w, r, behavior, key, http.StatusOK, "ok (unknown behavior)")
	}
}

func respond(w http.ResponseWriter, r *http.Request, behavior, key string, code int, msg string) {
	log.Printf("[mock] %s %s behavior=%s key=%s -> %d (%s)", r.Method, r.URL.Path, behavior, key, code, msg)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// mock 是测试工具，响应写失败无关紧要，但仍显式处理不吞。
	if err := json.NewEncoder(w).Encode(map[string]any{
		"status": code, "behavior": behavior, "message": msg,
	}); err != nil {
		log.Printf("[mock] write response failed: %v", err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func atoiDefault(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
