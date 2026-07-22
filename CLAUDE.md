# CLAUDE.md — 给 Claude Code 的项目约束

本项目是一个**可靠的出站 HTTP 通知中继服务（MVP）**。设计已定稿，见 `README.md` 与 `docs/DECISIONS.md`。**动手前先读这两份文件，严格遵守下面的约束。**

## 最高原则

这是**第一版（MVP）**。目标是"讲得清、扛得住、运维零成本"，**不是功能齐全**。
**克制优先于完备。** 任何"以防万一"的功能都不要加。如果你觉得某个功能有用但设计文档没提到，**先在对话里问我，不要自作主张写进代码。**

## 技术栈（不要替换）

- 语言：Go（标准库优先：`net/http`、`database/sql`、`log/slog`）
- 存储：PostgreSQL，**同时充当持久层和任务队列**
- Postgres 驱动：`github.com/jackc/pgx/v5/stdlib`，走标准 `database/sql` 接口（SQL 写法不变）
- **不引入**消息中间件（Kafka/RabbitMQ/Redis）、ORM 框架、Web 框架。标准库能做的就用标准库。
- 依赖越少越好。加任何第三方库前先问我。

## 绝对不要做的事（这些是我刻意砍掉的，别加回来）

- ❌ 不要引入 Kafka / RabbitMQ / Redis / 任何消息队列
- ❌ 不要拆微服务，就一个进程（API + Worker 池）
- ❌ 不要加熔断器、自适应限流（v2 才考虑）
- ❌ 不要做 payload 模板/转换引擎
- ❌ 不要开放"任意 URL 透传"——**只允许注册表里的 destination**
- ❌ 不要引入分布式追踪 / service mesh
- ❌ 不要为了"生产级"擅自加配置项、抽象层、插件机制

## 必须坚守的核心设计（来自 DECISIONS）

- **投递语义 = at-least-once**：先落盘再返回 202，再异步投递。接收方负责幂等。
- **DB 即队列**：worker 用 `SELECT ... FOR UPDATE SKIP LOCKED` 领取到期任务。
- **重试**：指数退避 + 抖动；4xx 不重试（直接进死信），5xx/429/超时/连接错误才重试。
- **死信 ≠ 日志**：死信是可查询、可重投的结构化记录（DB 里），不是只写日志。
- **崩溃恢复**：`DELIVERING` 行用 `locked_at` 租约超时机制回收；回收不计入 attempts（见 DECISIONS D6）。
- **注册表是策略控制面**：每个 destination 携带 url/method/headers/密钥引用 + 重试等级 + 是否告警 + 是否需顺序。
- **SSRF 防护纳入 v1**：解析到实际 IP 后再校验，拒绝内网/回环网段（10/8、172.16/12、192.168/16、127/8、169.254/16、::1）。
- **顺序**：v1 不实现 FIFO，只在注册表标注需求；用 payload 版本号 / last-write-wins 兜底。

## 工程规范（写代码时必须遵守）

- **配置与密钥走环境变量**：数据库连接串、worker 数量、各类超时、租约阈值等一律从环境变量读，**绝不硬编码**，更不许把密钥写进代码或提交进 git（配合 `.gitignore` 挡掉的 `.env`）。
- **不许吞 error**：每个可能失败的调用都要处理 error，禁止 `_ =` 静默忽略或空 catch。对本可靠性系统而言，被吞掉的投递错误可能把该重试的任务误判成成功。
- **关键路径打结构化日志**：用 `log/slog`，每次投递尝试 / 重试 / 进死信都打一条带 `notification_id`、`destination`、`attempt`、状态码或错误的结构化日志。这是 MVP 验收（演示重试与死信全景）的证据来源。指标/dashboard 属于 v2，不要做。
- **HTTP 客户端必须设超时**：连接超时 + 整体超时都要显式设置，禁止使用无超时的默认客户端（一个挂起的上游会永久占住 worker）。

## 目录结构（按此组织）

```
├── CLAUDE.md
├── README.md
├── go.mod
├── .gitignore
├── docs/                 # 设计文档、决策记录、AI 使用说明
├── cmd/relay/main.go     # 服务入口
├── internal/
│   ├── api/              # POST /notifications、GET /:id、POST /:id/retry
│   ├── store/            # notifications 表、SKIP LOCKED 领取、死信
│   ├── worker/           # 投递、退避、错误分类
│   └── security/         # 目标校验、SSRF 防护
├── migrations/           # 建表 SQL
├── mock/                 # 可控失败的 mock upstream（测试用）
└── docker-compose.yml    # 起 Postgres
```

## 测试要跑得出结果（MVP 的验收标准）

必须能演示这条全景：**提交通知 → 成功的成功了、抖动的重试几次后成了、一直失败的重试到上限进死信**。为此需要：

- 一个**可控的 mock upstream**（`mock/`），用 query 参数控制行为：稳定 200、稳定 5xx、前 N 次失败后成功（flaky）、超时、400 拒绝。
- **seed 数据**：注册表预置三个 destination——`ad-system`→ok、`crm`→flaky（顺序敏感/高危/告警）、`inventory`→fail（高危，演示死信）。
- 一个 **demo 脚本**：往 `POST /notifications` 打一批命中不同 destination 的请求，再查状态/死信。

## 工作方式

- **小步走**：一次只做一层（先 migrations + store，再 worker，再 api，最后 mock+demo）。每步做完停下来让我看，不要一口气生成整个项目。
- **跨层接口先对齐再写实现**：每进入新一层，先说清这层如何调用上一层（方法签名、传入/返回的数据结构），等我确认接口对得上，再写实现。层与层之间的约定错了最难返工。
- 每个关键决策如果代码里体现了，在注释里用一行标注它对应 DECISIONS 的哪条（如 `// D1: at-least-once`）。
- 不确定就问，不要猜。
- **每完成一步并停下时，在 `docs/AI_USAGE.md` 追加记录**：本步里你（Claude Code）提议过但被本文件约束拦下的建议，写进该文件"第二节"；本步中起关键作用的帮助，写进"第一节"。**只追加、不改写已有内容**。若本步没有可记的取舍，就明确写"本步无新增取舍"，不要编造。
- **每步停下时，提醒我做一次 git commit**：给出建议的 commit message 和本步应纳入提交的文件清单，供我参考。**不要自己执行 git add / git commit** —— 提交由我手动完成，提交粒度和信息我自己把控。
