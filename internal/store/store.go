// Package store 是 notifications 的持久层，也是任务队列的存取层。
//
// D2: DB 即队列——本包用原生 SQL（不用 ORM）把 `FOR UPDATE SKIP LOCKED` 的领取查询
// 和状态流转显式暴露出来，让并发领取“怎么发生”一眼可见（这正是 TECH_CHOICES §4 的取舍）。
//
// 注意：本包只依赖标准库 database/sql，不导入任何具体驱动。驱动（lib/pq 或 pgx）
// 在 cmd/relay 装配 *sql.DB 时以匿名 import 引入——放到后续步骤，并会先与作者确认。
package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrNotFound 表示按 id 未找到通知（api 层据此返回 404）。
var ErrNotFound = errors.New("notification not found")

// Status 是投递状态机的取值，与 migrations 里的 CHECK 约束一致（README §4.3）。
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusDelivering Status = "DELIVERING"
	StatusDelivered  Status = "DELIVERED"
	StatusDead       Status = "DEAD"
)

// Notification 映射 notifications 表的一行（字段见 README §4.1）。
type Notification struct {
	ID             string
	IdempotencyKey sql.NullString
	DestinationID  string
	URL            string
	Method         string
	Headers        []byte // jsonb 原始字节
	SecretRef      sql.NullString
	Body           []byte
	Status         Status
	Attempts       int
	MaxAttempts    int
	BackoffCapSecs int
	NextAttemptAt  time.Time
	LockedAt       sql.NullTime
	LastStatusCode sql.NullInt32
	LastError      sql.NullString
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewNotification 是入队时由 api 层组装的入参。api 先按 destination_id 从注册表取配置，
// 把 url/method/headers/secret_ref + max_attempts/backoff_cap 一并“快照”进这里(D3/D4)，
// body 是调用方最终 payload。worker 全程只读这些快照值，不再回查注册表。
type NewNotification struct {
	IdempotencyKey string // "" 表示不做入口去重
	DestinationID  string
	URL            string
	Method         string
	Headers        []byte
	SecretRef      string // "" 表示无
	Body           []byte
	MaxAttempts    int
	BackoffCapSecs int
}

// Destination 映射 destinations 注册表的一行。注册表是系统的“策略控制面”：
// 每个目标在这里携带自己的重试策略(D4)、告警与顺序标注(D5)，worker 投递时按此取策略。
type Destination struct {
	ID               string
	URL              string
	Method           string
	Headers          []byte // jsonb 原始字节
	SecretRef        sql.NullString
	Severity         string // 'high' / 'low' (D4)
	MaxAttempts      int    // D4: 重试上限
	BackoffCapSecs   int    // D4: 退避上限（秒）
	AlertOnDead      bool   // D4: 进死信是否告警
	RequiresOrdering bool   // D5: 是否需顺序（v1 只标注）
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Store 封装对 notifications 表的所有存取。
type Store struct {
	db *sql.DB
}

// New 用一个已装配好驱动的 *sql.DB 构造 Store。
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// notifCols 是所有查询共享的列顺序，与 scanNotification 一一对应。
const notifCols = `id, idempotency_key, destination_id, url, method, headers, secret_ref, body,
	status, attempts, max_attempts, backoff_cap_secs, next_attempt_at, locked_at,
	last_status_code, last_error, created_at, updated_at`

// rowScanner 兼容 *sql.Row 与 *sql.Rows。
type rowScanner interface {
	Scan(dest ...any) error
}

func scanNotification(sc rowScanner) (Notification, error) {
	var n Notification
	var status string
	err := sc.Scan(
		&n.ID, &n.IdempotencyKey, &n.DestinationID, &n.URL, &n.Method, &n.Headers, &n.SecretRef, &n.Body,
		&status, &n.Attempts, &n.MaxAttempts, &n.BackoffCapSecs, &n.NextAttemptAt, &n.LockedAt,
		&n.LastStatusCode, &n.LastError, &n.CreatedAt, &n.UpdatedAt,
	)
	n.Status = Status(status)
	return n, err
}

// Enqueue 落盘一条 PENDING 通知（“先落盘再返回 202”的落盘动作，D1 的地基）。
//
// D1: 入口幂等——带 idempotency_key 时，重复提交只入队一次，返回原有 id 且 existed=true。
func (s *Store) Enqueue(ctx context.Context, n NewNotification) (id string, existed bool, err error) {
	headers := n.Headers
	if headers == nil {
		headers = []byte("{}")
	}

	if n.IdempotencyKey == "" {
		err = s.db.QueryRowContext(ctx, `
			INSERT INTO notifications (destination_id, url, method, headers, secret_ref, body, max_attempts, backoff_cap_secs)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)
			RETURNING id`,
			n.DestinationID, n.URL, n.Method, string(headers), nullString(n.SecretRef), n.Body, n.MaxAttempts, n.BackoffCapSecs,
		).Scan(&id)
		return id, false, err
	}

	// 带幂等键：冲突则不插入，随后取回已存在的 id。
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO notifications (idempotency_key, destination_id, url, method, headers, secret_ref, body, max_attempts, backoff_cap_secs)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id`,
		n.IdempotencyKey, n.DestinationID, n.URL, n.Method, string(headers), nullString(n.SecretRef), n.Body, n.MaxAttempts, n.BackoffCapSecs,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		// 已存在同键记录：取回原 id，视为幂等命中。
		err = s.db.QueryRowContext(ctx,
			`SELECT id FROM notifications WHERE idempotency_key = $1`, n.IdempotencyKey,
		).Scan(&id)
		return id, true, err
	}
	return id, false, err
}

// ClaimDue 原子领取一批到期任务并置为 DELIVERING，返回被领走的行。
//
// D2: DB 即队列——FOR UPDATE SKIP LOCKED 让多个 worker/进程副本并发领取而互不抢占，
// 无需外部分布式锁（README §4.6）。attempts 不在此处 +1，留到记录投递结果时再计。
func (s *Store) ClaimDue(ctx context.Context, limit int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		UPDATE notifications
		SET status = 'DELIVERING', locked_at = now(), updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE status = 'PENDING' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING `+notifCols, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// MarkDelivered 记录一次成功投递：DELIVERING -> DELIVERED（README §4.3）。
func (s *Store) MarkDelivered(ctx context.Context, id string, statusCode int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'DELIVERED', attempts = attempts + 1,
		    last_status_code = $2, last_error = NULL,
		    locked_at = NULL, updated_at = now()
		WHERE id = $1`,
		id, nullInt(statusCode))
	return err
}

// MarkRetry 记录一次“可重试失败”并按退避重排：DELIVERING -> PENDING（D4）。
// nextAttemptAt 由 worker 用指数退避 + 抖动算出；statusCode 为 0 时（连接错误/超时）存 NULL。
func (s *Store) MarkRetry(ctx context.Context, id string, nextAttemptAt time.Time, statusCode int, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'PENDING', attempts = attempts + 1,
		    next_attempt_at = $2, last_status_code = $3, last_error = $4,
		    locked_at = NULL, updated_at = now()
		WHERE id = $1`,
		id, nextAttemptAt, nullInt(statusCode), nullString(errMsg))
	return err
}

// MarkDead 把通知打入死信：DELIVERING -> DEAD（不可重试的 4xx，或超过 max_attempts）。
//
// D4: 死信 ≠ 日志——它是结构化、可查询、可一键重投的记录（见 ListDead / RequeueDead）。
func (s *Store) MarkDead(ctx context.Context, id string, statusCode int, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'DEAD', attempts = attempts + 1,
		    last_status_code = $2, last_error = $3,
		    locked_at = NULL, updated_at = now()
		WHERE id = $1`,
		id, nullInt(statusCode), nullString(errMsg))
	return err
}

// GetDestination 按 id 从注册表取一个目标（worker 投递时用它取重试策略）。未找到返回 ErrNotFound。
// D3: 目标只能来自注册表——worker 从不接受调用方传入的任意 URL。
func (s *Store) GetDestination(ctx context.Context, id string) (*Destination, error) {
	var d Destination
	err := s.db.QueryRowContext(ctx, `
		SELECT id, url, method, headers, secret_ref, severity, max_attempts,
		       backoff_cap_secs, alert_on_dead, requires_ordering, created_at, updated_at
		FROM destinations WHERE id = $1`, id).Scan(
		&d.ID, &d.URL, &d.Method, &d.Headers, &d.SecretRef, &d.Severity, &d.MaxAttempts,
		&d.BackoffCapSecs, &d.AlertOnDead, &d.RequiresOrdering, &d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// Get 按 id 查询单条通知（服务于 GET /notifications/:id）。未找到返回 ErrNotFound。
func (s *Store) Get(ctx context.Context, id string) (*Notification, error) {
	n, err := scanNotification(s.db.QueryRowContext(ctx,
		`SELECT `+notifCols+` FROM notifications WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &n, nil
}

// ListDead 列出死信记录，按最近更新倒序分页（服务于死信排查）。D4: 死信可查询。
func (s *Store) ListDead(ctx context.Context, limit, offset int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+notifCols+`
		FROM notifications
		WHERE status = 'DEAD'
		ORDER BY updated_at DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// RequeueDead 人工重投一条死信：DEAD -> PENDING，重置尝试次数与下次时间（POST /:id/retry）。
// 只对当前处于 DEAD 的行生效；返回是否确有一行被重投（否则 api 层可返回 404/409）。D4。
func (s *Store) RequeueDead(ctx context.Context, id string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'PENDING', attempts = 0, next_attempt_at = now(),
		    locked_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'DEAD'`, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// ReclaimExpired 回收崩溃遗留的任务：把租约超时的 DELIVERING 行重置回 PENDING 以便重新领取。
//
// §4.7: 崩溃恢复——worker 领取后中途崩溃会把行卡在 DELIVERING，用 locked_at 做可见性超时。
// 此处不给 attempts +1（这次崩溃并未记录为一次投递尝试），返回被回收的行数。
//
// 已知取舍 (D6)：崩溃回收不计入 attempts，接受理论上的重复投递风险（由接收方幂等兜底）；
// 若未来观测到某任务反复回收，再引入独立的 reclaim_count 上限。
func (s *Store) ReclaimExpired(ctx context.Context, leaseTimeout time.Duration) (int, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'PENDING', locked_at = NULL, next_attempt_at = now(), updated_at = now()
		WHERE status = 'DELIVERING'
		  AND locked_at < now() - ($1 * interval '1 second')`,
		leaseTimeout.Seconds())
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// nullInt 把 HTTP 状态码转成可空整数：0 表示“无状态码”（连接错误/超时）→ 存 NULL。
func nullInt(code int) sql.NullInt32 {
	if code == 0 {
		return sql.NullInt32{}
	}
	return sql.NullInt32{Int32: int32(code), Valid: true}
}

// nullString 把空串转成 NULL。
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
