-- 0001_init.sql — 建表：destinations 注册表 + notifications 队列/持久层
-- 关键决策对应见 docs/DECISIONS.md 与 README.md 的小节标注。

-- =====================================================================
-- destinations：供应商注册表，同时是系统的“策略控制面”
--   D3: 仅注册表寻址，不开放任意 URL 透传；密钥集中管理（此处只放引用，不存明文）。
--   每个目标在这里携带自己的重试等级(D4)、是否告警(D4)、是否需顺序(D5)。
-- =====================================================================
CREATE TABLE destinations (
    id                TEXT PRIMARY KEY,                                          -- slug，例如 'ad-system' / 'crm' / 'inventory'
    url               TEXT NOT NULL,
    method            TEXT NOT NULL DEFAULT 'POST',
    headers           JSONB NOT NULL DEFAULT '{}'::jsonb,                         -- 各供应商 Header 各异，用 JSONB 存任意结构
    secret_ref        TEXT,                                                      -- D3: 密钥“占位”——指向密钥存放处的引用，绝不在库里存明文
    severity          TEXT NOT NULL DEFAULT 'low' CHECK (severity IN ('high','low')), -- D4: 重试等级（严重程度）
    max_attempts      INT  NOT NULL DEFAULT 5,                                    -- D4: 该目标的重试上限
    backoff_cap_secs  INT  NOT NULL DEFAULT 3600,                                 -- D4: 退避上限（秒）
    alert_on_dead     BOOLEAN NOT NULL DEFAULT FALSE,                             -- D4: 进死信是否告警
    requires_ordering BOOLEAN NOT NULL DEFAULT FALSE,                            -- D5: 是否需顺序——v1 只标注、不实现 FIFO
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =====================================================================
-- notifications：核心表，既是持久层也是任务队列（D2: DB 即队列）
--   字段参照 README §4.1。
--   url/method/headers 在入队时从注册表“快照”而来（调用方不能传任意 URL, D3）；
--   body 是调用方发来的最终 payload——本服务是哑管道，不解析它。
-- =====================================================================
CREATE TABLE notifications (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),                 -- 主键，也是返回给业务方的追踪 ID（PG13+ 内置 gen_random_uuid）
    idempotency_key  TEXT,                                                       -- D1: 入口幂等键，业务方可选传入；NULL 表示不参与去重
    destination_id   TEXT NOT NULL REFERENCES destinations(id),
    url              TEXT NOT NULL,                                              -- 下面三列为入队时从 destinations 快照的目标请求描述
    method           TEXT NOT NULL,
    headers          JSONB NOT NULL DEFAULT '{}'::jsonb,
    body             BYTEA NOT NULL,                                             -- 最终 payload，格式不透明（哑管道）
    status           TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING','DELIVERING','DELIVERED','DEAD')), -- 状态机见 README §4.3
    attempts         INT  NOT NULL DEFAULT 0,                                    -- 已尝试次数（记录结果时 +1）
    max_attempts     INT  NOT NULL,                                             -- 上限，来自注册表 severity 策略 (D4)
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),                        -- D4: 退避的落地方式——下次可被领取的时间
    locked_at        TIMESTAMPTZ,                                               -- §4.7: 领取租约时间戳，崩溃恢复用
    last_status_code INT,                                                       -- 最近一次 HTTP 状态码（连接错误/超时时为 NULL），排障用
    last_error       TEXT,                                                       -- 最近一次错误信息，排障用
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 入口幂等：同一 idempotency_key 只入队一次；NULL 不参与去重（部分唯一索引）(D1)
CREATE UNIQUE INDEX uq_notifications_idem
    ON notifications (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- 队列领取的部分索引：worker 只需扫“到期的 PENDING 行”，避免全表扫 (D2: DB 即队列)
CREATE INDEX idx_notifications_due
    ON notifications (status, next_attempt_at)
    WHERE status = 'PENDING';

-- 租约回收查询用：只扫 DELIVERING 行，回收卡住的任务 (§4.7 崩溃恢复)
CREATE INDEX idx_notifications_lease
    ON notifications (locked_at)
    WHERE status = 'DELIVERING';
