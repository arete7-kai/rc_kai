// Package api 是入站 HTTP 接口层：提交通知、查状态、重投死信。用标准库 net/http，不引框架。
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"rc_kai/internal/store"
)

// Server 持有依赖，暴露路由。
type Server struct {
	store *store.Store
	log   *slog.Logger
}

// New 构造 api Server。
func New(st *store.Store, log *slog.Logger) *Server {
	return &Server{store: st, log: log}
}

// Routes 返回配置好的多路复用器（MVP 三个端点，用 Go 1.22+ 的方法+通配路由，无需框架）。
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notifications", s.handleCreate)
	mux.HandleFunc("GET /notifications/{id}", s.handleGet)
	mux.HandleFunc("POST /notifications/{id}/retry", s.handleRetry)
	return mux
}

// createRequest 是 POST /notifications 的请求体。
type createRequest struct {
	DestinationID string `json:"destination_id"`
	// Body 是发往上游的最终 payload。v1 假设 payload 为 JSON，原样存储并透传；
	// 非 JSON（form / XML 等）属已知限制，留待演进（见 README §2.2）。
	Body json.RawMessage `json:"body"`
}

type createResponse struct {
	ID string `json:"id"`
}

type retryResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// statusResponse 是对外暴露的“安全视图”，绝不含 body / headers / secret_ref。
type statusResponse struct {
	ID             string    `json:"id"`
	DestinationID  string    `json:"destination_id"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	NextAttemptAt  time.Time `json:"next_attempt_at"`
	LastStatusCode *int      `json:"last_status_code,omitempty"`
	LastError      *string   `json:"last_error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// handleCreate 提交通知：校验 → 按 destination_id 取注册表配置并快照落盘 PENDING → 202 + id。
func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.DestinationID == "" {
		s.writeError(w, http.StatusBadRequest, "destination_id is required")
		return
	}
	if len(req.Body) == 0 {
		s.writeError(w, http.StatusBadRequest, "body is required")
		return
	}

	// D3: 目标必须是注册表里登记的 destination，不接受任意 URL；未登记 → 400。
	dest, err := s.store.GetDestination(r.Context(), req.DestinationID)
	if errors.Is(err, store.ErrNotFound) {
		s.writeError(w, http.StatusBadRequest, "unknown destination: "+req.DestinationID)
		return
	}
	if err != nil {
		s.log.Error("get destination failed", "dest", req.DestinationID, "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// 入队时把注册表配置快照进通知记录（D3/D4）；worker 全程只读快照，不回查注册表。
	id, existed, err := s.store.Enqueue(r.Context(), store.NewNotification{
		IdempotencyKey: r.Header.Get("Idempotency-Key"), // 缺失即空串，不做去重
		DestinationID:  dest.ID,
		URL:            dest.URL,
		Method:         dest.Method,
		Headers:        dest.Headers,
		SecretRef:      dest.SecretRef.String,
		Body:           req.Body,
		MaxAttempts:    dest.MaxAttempts,
		BackoffCapSecs: dest.BackoffCapSecs,
	})
	if err != nil {
		s.log.Error("enqueue failed", "dest", req.DestinationID, "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// D1: 先落盘再返回 202；带幂等键的重复提交返回同一 id（idempotent_hit=true）。
	s.log.Info("accepted", "notification_id", id, "destination", dest.ID, "idempotent_hit", existed)
	s.writeJSON(w, http.StatusAccepted, createResponse{ID: id})
}

// handleGet 查询单条通知状态。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n, err := s.store.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		s.writeError(w, http.StatusNotFound, "notification not found")
		return
	}
	if err != nil {
		s.log.Error("get notification failed", "notification_id", id, "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeJSON(w, http.StatusOK, toStatusResponse(n))
}

// handleRetry 人工重投死信（运维）：仅对 DEAD 态生效。
func (s *Server) handleRetry(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ok, err := s.store.RequeueDead(r.Context(), id)
	if err != nil {
		s.log.Error("requeue dead failed", "notification_id", id, "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if ok {
		s.log.Info("dead-letter requeued", "notification_id", id)
		s.writeJSON(w, http.StatusAccepted, retryResponse{ID: id, Status: string(store.StatusPending)})
		return
	}

	// 未重投：区分“不存在(404)”与“存在但不在 DEAD 态(409)”。
	if _, err := s.store.Get(r.Context(), id); errors.Is(err, store.ErrNotFound) {
		s.writeError(w, http.StatusNotFound, "notification not found")
		return
	} else if err != nil {
		s.log.Error("get notification failed", "notification_id", id, "err", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	s.writeError(w, http.StatusConflict, "notification is not in DEAD state")
}

func toStatusResponse(n *store.Notification) statusResponse {
	resp := statusResponse{
		ID:            n.ID,
		DestinationID: n.DestinationID,
		Status:        string(n.Status),
		Attempts:      n.Attempts,
		MaxAttempts:   n.MaxAttempts,
		NextAttemptAt: n.NextAttemptAt,
		CreatedAt:     n.CreatedAt,
		UpdatedAt:     n.UpdatedAt,
	}
	if n.LastStatusCode.Valid {
		c := int(n.LastStatusCode.Int32)
		resp.LastStatusCode = &c
	}
	if n.LastError.Valid {
		e := n.LastError.String
		resp.LastError = &e
	}
	return resp
}

func (s *Server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.log.Error("write response failed", "err", err)
	}
}

func (s *Server) writeError(w http.ResponseWriter, code int, msg string) {
	s.writeJSON(w, code, errorResponse{Error: msg})
}
