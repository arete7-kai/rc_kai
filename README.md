# 可靠 API 通知中继服务（Reliable Notification Relay）

> 一个内部服务：接收业务系统提交的"要发往外部供应商 HTTP API 的一次通知"，并**尽最大努力可靠地送达**。业务方不关心返回值，只要求送达稳定可靠。

**技术栈**：Go（`net/http` + `database/sql`）· PostgreSQL（同时充当持久层与任务队列）· 无外部消息中间件（v1 刻意不引入，理由见 §9）。

---

## 0. 一句话结论（TL;DR）

核心动作是 **收下 → 落盘 → 立即返回 `202` → 异步投递 → 失败退避重试 → 到上限进死信**。

一个刻意的判断贯穿全文：**这是第一版系统，我优先保证"讲得清、扛得住、运维几乎零成本"，而不是"功能齐全"。** 因此我用"数据库当队列"支撑 v1，并明确写出何时才会升级到消息中间件、熔断、限流等更重的方案（§7、§8）。

---

## 1. 我对问题的理解

多个业务系统在关键事件发生时（注册引流回传、订阅付款后改 CRM 状态、下单后扣库存等）需要调用**不同供应商**的外部 HTTP API。这些 API 的地址、Header、Body 各不相同。业务方有两个明确特征：

1. **不关心返回值** —— 它只想"把这次通知交出去，然后不用管了"。
2. **要求可靠送达** —— 外部系统可能超时、抖动、甚至长时间宕机，通知不能因此丢失。

这实际上是一个**异步、可靠的出站中继（outbound relay）**问题。它的难点不在"发一个 HTTP 请求"，而在**在业务方 fire-and-forget 的前提下，替它承担起投递的可靠性责任**。识别出这一点，整个设计的地基就是一句话：**在给业务方确认之前，先把请求持久化。** 这条 durability 边界一旦成立，投递语义、重试、死信全是自然推论。

---

## 2. 系统边界

清晰的边界比功能数量更重要。以下是我**主动选择**的取舍。

### 2.1 系统内解决的问题

- **可靠接收与持久化**：先落盘再确认，确认后本服务对送达负责。
- **异步投递**：对业务方毫秒级返回 `202`，投递在后台进行。
- **失败重试**：区分可重试/不可重试错误，指数退避 + 抖动。
- **死信兜底**：重试到上限的请求进入死信，可查询、可重投，绝不静默丢弃。
- **入口幂等**：业务方重复提交（如没收到我的 `202`）不会产生重复投递任务。
- **状态可查**：提供按 ID 查询投递状态的接口，便于排障。

### 2.2 明确**不**解决的问题（及原因）

| 不做 | 原因 |
|---|---|
| 解析/处理外部 API 的业务返回 | 业务方明确不关心返回值；本服务只依据 HTTP 状态码判断成败。 |
| **Exactly-once（恰好一次）** | 跨第三方无法实现——需要接收方全部支持幂等键，这不受我们控制。诚实地做 at-least-once，把幂等责任交给接收方（并透传幂等键帮它去重）。 |
| **全局投递顺序** | v1 不承诺顺序。绝大多数回调场景（付款回传、库存变更）本身可乱序 + 幂等。真需要顺序是少数场景，留作演进（§8 的 per-key FIFO）。 |
| "何时该发通知"的业务判断 | 这是业务系统的职责。本服务是一根**可靠的哑管道**，不介入业务语义。 |
| Payload 的模板化/字段转换 | v1 由业务方发送最终 payload。引入转换引擎是典型的过度设计（§7）。 |
| 非 JSON 的 payload（form / XML 等） | v1 假设 payload 为 JSON，`POST /notifications` 的 `body` 以 `json.RawMessage` 原样存储并透传给上游；其他格式属已知限制，留待演进。 |
| 自建消息中间件 | 用现成基础设施；v1 甚至用数据库即可（§9）。 |
| 多活/跨区域高可用 | v1 单实例或少量 worker 即可；HA 是演进项，不是第一版该背的复杂度。 |

> **一个边界外但值得点明的问题**：如果业务方需要"业务事务真正提交了才发通知"的强一致保证，正确做法是**业务方自己用事务性 Outbox**（在同一个 DB 事务里写业务数据和待发消息），而不是把这层复杂度塞进本服务。本服务提供的是最简单的 HTTP 接入；更强的保证应在业务方一侧解决。守住这条边界，本身就是一个工程判断。

---

## 3. 整体架构

单进程内四个部件，共享一个 PostgreSQL：

```mermaid
flowchart LR
    BIZ[业务系统] -->|POST /notifications| API[Ingress API]
    API -->|先落盘 PENDING| DB[(PostgreSQL<br/>notifications 表)]
    API -->|202 Accepted + id| BIZ

    subgraph SVC[通知中继服务 单进程]
      API
      W1[Worker]
      W2[Worker]
    end

    W1 -->|SKIP LOCKED 领取到期任务| DB
    W2 -->|SKIP LOCKED 领取到期任务| DB
    W1 -->|带超时的 HTTP 调用| EXT[外部供应商 API]
    W2 --> EXT
    W1 -->|更新状态/退避/死信| DB

    OPS[运维/排障] -->|GET /notifications/:id| API
    OPS -->|查看&重投死信| API
```

- **Ingress API**：校验 → 落盘为 `PENDING` → 返回 `202 + id`。
- **PostgreSQL**：既是持久层，也是任务队列（`FOR UPDATE SKIP LOCKED`，见 §4.6）。
- **Worker 池**：Go 的 goroutine，轮询领取到期任务、发起投递、按结果更新状态。
- **运维接口**：查询单条状态、查看/重投死信。

---

## 4. 核心设计

### 4.1 数据模型

一张核心表 `notifications`：

| 字段 | 说明 |
|---|---|
| `id` (uuid) | 主键，也是返回给业务方的追踪 ID |
| `idempotency_key` | 业务方可选传入，做入口去重（唯一索引） |
| `destination_id` | 指向供应商配置（推荐），或直接存下面三列 |
| `url` / `method` / `headers` / `body` | 目标请求描述 |
| `status` | `PENDING` / `DELIVERING` / `DELIVERED` / `DEAD` |
| `attempts` / `max_attempts` | 已尝试次数 / 上限 |
| `next_attempt_at` | 下次可被领取的时间（退避的落地方式） |
| `locked_at` | 领取租约时间戳（崩溃恢复用，见 §4.7） |
| `last_status_code` / `last_error` | 最近一次结果，排障用 |
| `created_at` / `updated_at` | 时间戳 |

### 4.2 API（MVP 三个端点）

- `POST /notifications` —— 提交通知，返回 `202 { id }`。支持 `Idempotency-Key` 请求头。
- `GET /notifications/{id}` —— 查询投递状态。
- `POST /notifications/{id}/retry` —— 手动重投死信（运维）。

### 4.3 投递状态机

```mermaid
stateDiagram-v2
    [*] --> PENDING: 落盘
    PENDING --> DELIVERING: worker 领取
    DELIVERING --> DELIVERED: 2xx
    DELIVERING --> PENDING: 可重试失败<br/>(退避后重排)
    DELIVERING --> DEAD: 不可重试 或 超过 max_attempts
    DEAD --> PENDING: 人工重投
    DELIVERED --> [*]
```

### 4.4 投递语义：at-least-once

因为"先落盘再确认"，本服务保证**至少一次**送达。可能重复的情形：投递成功但我方在写回 `DELIVERED` 前崩溃 → 恢复后会再投一次。这是 at-least-once 的固有代价，我用两点应对：

1. **透传幂等键**：业务方可携带幂等键，我在投递时原样带给供应商，供应商据此去重。
2. **文档明确告知**：接收方应做成幂等。这比假装 exactly-once 更诚实、更可靠。

### 4.5 重试与退避

- **可重试**：连接错误、超时、`5xx`、`429`。
- **不可重试**：多数 `4xx`（`400/401/403/404` 等，重试无意义）→ 直接进死信并记录，等人工介入。
- **退避**：指数退避 + 抖动（如 `min(base * 2^attempts, cap)` 再叠加随机抖动），落地为写 `next_attempt_at`。抖动是为了避免供应商恢复瞬间的重试风暴（thundering herd）。
- **上限**：`max_attempts` 到达后进 `DEAD`。

### 4.6 数据库即队列：并发安全领取

Worker 领取任务用 PostgreSQL 的行级锁 + 跳过已锁行：

```sql
UPDATE notifications
SET status='DELIVERING', locked_at=now()
WHERE id IN (
  SELECT id FROM notifications
  WHERE status='PENDING' AND next_attempt_at <= now()
  ORDER BY next_attempt_at
  FOR UPDATE SKIP LOCKED
  LIMIT $1
)
RETURNING *;
```

`FOR UPDATE SKIP LOCKED` 让多个 worker（乃至多个进程副本）并发领取而互不抢占、无需额外分布式锁。这是 v1 不上消息队列却仍能安全并发的关键。

### 4.7 崩溃恢复（租约）

Worker 领取后若在投递中途崩溃，任务会卡在 `DELIVERING`。用 `locked_at` 做**可见性超时（租约）**：一个回收任务（或领取时的条件）把"`DELIVERING` 且 `locked_at` 超过阈值"的行重置为 `PENDING`，从而被重新领取。这保证了单点崩溃不丢消息。

---

## 5. 可靠性与失败处理（作业必答）

- **投递语义**：**at-least-once**，理由与代价见 §4.4。
- **外部短时抖动/超时**：可重试错误走指数退避 + 抖动，自动恢复。
- **外部长期不可用**：持续退避重试至 `max_attempts` → 进死信，保留全部上下文供人工排查与重投；**不会丢，也不会无限空转打爆对方**。
- **本服务自身崩溃**：先落盘保证不丢；`DELIVERING` 租约超时机制保证卡住的任务被重新领取（§4.7）。
- **业务方重复提交**：入口幂等键去重（§4.1）。

---

## 6. 安全：出站中继天然的 SSRF 风险

一个"替业务方向任意 URL 发请求"的内部服务，本质上是一个**可被利用的 SSRF 代理**——恶意或出错的调用方可能让它去打内网地址（元数据服务、内部管理端口等）。v1 就必须处理：

- **目标白名单 / 供应商注册表**：优先让业务方引用预先登记的 `destination_id`，而非任意 URL；凭证也集中管理，避免密钥散落在每个请求和日志里。
- **禁止内网目标**：解析目标**实际连接的 IP**（而非只看 URL 里的 host 字符串，防 DNS rebinding），拒绝私有网段与回环地址（`10/8`、`172.16/12`、`192.168/16`、`127/8`、`169.254/16`、`::1` 等）。校验点在连接前的 `net.Dialer.Control`，并顺带禁止跟随 3xx 重定向（也是 SSRF 向量）。

这一点 AI 生成的方案里经常缺失，但它是真实生产事故的常见来源，所以放进 v1。

> **`SSRF_ALLOW_HOSTS`（仅 dev/demo）**：因为 demo 的 mock upstream 必然跑在回环/私网地址上，会被上面的拦截挡掉，故提供一个环境变量白名单 `SSRF_ALLOW_HOSTS`（逗号分隔的 host）。它遵循"默认拒绝 + 显式白名单"：**默认空 = 严格拦截**；只有命中的 host 才跳过"私网/回环 IP 检查"这一条，而 **scheme 仅限 http/https、以及"目标必须来自注册表（D3）"两条仍然照走**，白名单不是绕过一切的后门。⚠️ 命中的 host 会跳过内网拦截，**绝不能填入不可信地址**；**生产环境必须留空**。

---

## 7. 关键工程决策与取舍（作业必答：哪些 AI 建议我不采纳）

在方案讨论中，以下"看起来很专业"的设计我判断为**对第一版过度**，明确不采纳：

| 被砍掉的设计 | 不采纳的理由 |
|---|---|
| 引入 Kafka / RabbitMQ / BullMQ | v1 预期吞吐下"数据库即队列"完全够用，且运维成本几乎为零。过早引入 broker 是纯粹的复杂度负债（何时才引入见 §8）。 |
| 拆成多微服务 | 单进程内 API + Worker 足够，拆分只会带来部署与调试成本，收益为零。 |
| 每目标熔断器 + 自适应限流 | 是好东西，但属于"某供应商开始拖累全局"之后才需要的隔离手段，v1 用退避 + 死信已能兜底。 |
| 插件化 payload 转换引擎 | 业务方直接发最终 payload 即可，转换引擎是典型的臆想需求。 |
| 分布式追踪 / Service Mesh | v1 结构化日志 + 单条状态查询已足够排障。 |

**判断依据**：每一项额外复杂度都要问"它现在解决的是真问题还是想象中的问题"。上表里的东西解决的都是"更大规模/更多供应商"时才出现的问题，而不是 v1 的问题。**能识别并拒绝过度设计，本身就是这份作业要考的核心能力。**

---

## 8. 演进路线（作业必答：规模/复杂度增长后如何演进）

不追求覆盖所有情况，只列出**触发信号 → 应对手段**，说明判断依据：

1. **吞吐增长，DB 轮询成为瓶颈** → 引入消息中间件（Kafka / RabbitMQ / Redis Streams）承接投递流，DB 退回纯持久化角色。信号：领取延迟上升、DB 连接/CPU 吃紧。
2. **某供应商变慢/宕机拖累全局（队头阻塞、noisy neighbor）** → 按 `destination` 分片队列 + 独立并发上限 + 熔断器，让坏邻居不饿死健康供应商。这是最先会真实出现的痛点。
3. **出现顺序要求** → 对需要顺序的键做 per-key FIFO（同 key 串行投递）。
4. **供应商有速率限制** → 每目标限流 / 令牌桶。
5. **可观测性需求** → 投递延迟、重试率、死信积压等指标 + 告警。
6. **凭证安全升级** → 用 Vault 等集中管理供应商密钥。
7. **水平扩展** → 因为领取用 `SKIP LOCKED`，多实例可直接横向加，几乎零改造。

---

## 9. 中间件选型说明（作业要求）

**为什么 v1 用"数据库即队列"而不用消息中间件？**

- **足够**：中小吞吐下，Postgres + `SKIP LOCKED` 提供了可靠的并发领取，且天然带持久化。
- **简单**：少一个需要部署、监控、调优、会宕机的组件；持久化与队列共用一次事务，语义更直观。
- **可演进**：接口对 worker 屏蔽了"队列从哪来"，未来换成真正的 broker 是局部改造。

**不使用中间件的替代方案（也就是我现在采用的）**：即上文的 DB-as-queue。反过来说，**引入 broker 的条件**已在 §8 第 1 条写明——当且仅当出现明确的吞吐/解耦信号时才上。

---

## 10. 目录结构与运行

```
├── README.md            # 本设计文档
├── docs/                # DECISIONS / TECH_CHOICES / AI_USAGE
├── cmd/relay/main.go    # 服务入口（装配 API + worker 池 + 租约回收 + 优雅关闭）
├── internal/
│   ├── api/             # Ingress + 查询/重投接口
│   ├── store/           # notifications 表、SKIP LOCKED 领取、死信、租约回收
│   ├── worker/          # 投递、退避、错误分类
│   └── security/        # 目标校验、SSRF 防护
├── migrations/          # 建表 SQL（docker compose 首次起库时自动执行）
├── mock/                # 可控失败的 mock upstream（demo/测试用）
├── seed/                # 注册表种子（三个 demo destination）
├── demo/                # demo 脚本
└── docker-compose.yml   # 起 Postgres
```

### 一步步跑通 demo 全景

启动依赖仅 Postgres。以下命令假定在仓库根目录；Windows 用户在 Git Bash 里跑脚本。

```bash
# 1) 起 Postgres（migrations/ 挂到 initdb.d，首次起库自动建表）
docker compose up -d

# 2) seed 注册表：预置 ad-system / crm / inventory 三个 destination
docker compose exec -T db psql -U relay -d relay < seed/seed.sql

# 3) 起 mock upstream（监听 :9090，用 query 控制成功/失败/flaky）
go run ./mock &

# 4) 起 relay。SSRF_ALLOW_HOSTS 放行本地 mock（仅 dev/demo，生产留空）；
#    退避基数调小让 demo 几秒内就能看到重试→成功/死信。
DATABASE_URL='postgres://relay:relay@localhost:5432/relay?sslmode=disable' \
SSRF_ALLOW_HOSTS='127.0.0.1' \
WORKER_BASE_BACKOFF='1s' \
go run ./cmd/relay &

# 5) 跑 demo：提交一批通知，等几秒，查状态/死信
bash demo/demo.sh
```

**预期结果**：`ad-system` → `DELIVERED`（一次成功）；`crm` → `DELIVERED`（flaky，重试约 3 次后成功）；`inventory` → `DEAD`（稳定 500，重试到 `max_attempts` 进死信，可用 `POST /notifications/{id}/retry` 重投）。relay 与 mock 的结构化日志里能看到每次投递尝试 / 重试 / 进死信的全过程。
## 实际结果

### 1. 提交一批通知

```text
ad-system  -> 1f44000b-7ea6-45d6-a4d6-1bf2a0e8c453
crm        -> b3e20c0f-525e-4c12-a162-561bf5d87a5b
inventory  -> 88a2e8b3-54f0-44d6-8d5f-405593a8b7bd
```

### 2. 入口幂等验证

使用同一个 `Idempotency-Key` 重复提交 `ad-system`，应返回相同的通知 ID。

```text
重复提交 -> 1f44000b-7ea6-45d6-a4d6-1bf2a0e8c453
```

验证结果：

```text
OK：ID 相同，未重复入队
```

### 3. 等待 Worker 投递和退避重试

等待约 10 秒，让 Worker 完成消息投递及退避重试。

### 4. 查询通知状态

#### ad-system

通知 ID：

```text
1f44000b-7ea6-45d6-a4d6-1bf2a0e8c453
```

```json
{
  "id": "1f44000b-7ea6-45d6-a4d6-1bf2a0e8c453",
  "destination_id": "ad-system",
  "status": "DELIVERED",
  "attempts": 1,
  "max_attempts": 5,
  "next_attempt_at": "2026-07-22T17:52:31.05912-04:00",
  "last_status_code": 200,
  "created_at": "2026-07-22T17:52:31.05912-04:00",
  "updated_at": "2026-07-22T17:52:31.287755-04:00"
}
```

#### crm

通知 ID：

```text
b3e20c0f-525e-4c12-a162-561bf5d87a5b
```

```json
{
  "id": "b3e20c0f-525e-4c12-a162-561bf5d87a5b",
  "destination_id": "crm",
  "status": "DELIVERED",
  "attempts": 3,
  "max_attempts": 10,
  "next_attempt_at": "2026-07-22T17:52:33.545941-04:00",
  "last_status_code": 200,
  "created_at": "2026-07-22T17:52:31.130339-04:00",
  "updated_at": "2026-07-22T17:52:34.287651-04:00"
}
```

#### inventory

通知 ID：

```text
88a2e8b3-54f0-44d6-8d5f-405593a8b7bd
```

```json
{
  "id": "88a2e8b3-54f0-44d6-8d5f-405593a8b7bd",
  "destination_id": "inventory",
  "status": "DEAD",
  "attempts": 3,
  "max_attempts": 3,
  "next_attempt_at": "2026-07-22T17:52:33.402905-04:00",
  "last_status_code": 500,
  "last_error": "max attempts (3) reached: upstream status 500",
  "created_at": "2026-07-22T17:52:31.201031-04:00",
  "updated_at": "2026-07-22T17:52:34.292567-04:00"
}
```

### 预期结果

| Destination | 预期状态        |   实际尝试次数 |
| ----------- | ----------- | -------: |
| `ad-system` | `DELIVERED` |        1 |
| `crm`       | `DELIVERED` |    约 3 次 |
| `inventory` | `DEAD`      | 达到上限 3 次 |

实际结果符合预期。

### 5. 手动重投死信消息

手动重投 `inventory` 的死信消息。由于上游仍然返回失败，该消息后续会再次重试，并在达到最大重试次数后重新进入死信状态。

重投响应：

```json
{
  "id": "88a2e8b3-54f0-44d6-8d5f-405593a8b7bd",
  "status": "PENDING"
}
```

重投后，消息状态恢复为 `PENDING`。随后 Worker 会重新执行投递，并按照重试策略处理，直到投递成功或再次达到最大重试次数。

> 重跑一遍（清空数据重新建表 + seed）：`docker compose down -v && docker compose up -d`，再从第 2 步开始。

---

## 11. AI 使用说明（占位，最终提交前补全）

按作业要求，最终提交需包含 `docs/AI_USAGE.md`，记录：AI 在哪些地方提供了帮助、给过哪些**我没有采纳**的建议（重点：AI 倾向于建议上 MQ/熔断/微服务，被我以 §7 的理由砍掉）、哪些关键决策是我自己做的及原因（如：坚持 at-least-once 而非假装 exactly-once、v1 用 DB-as-queue、把 SSRF 防护纳入 v1）。建议从现在起边做边记。
