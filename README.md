# Pipeline Run Observability

基于 FastAPI 的流水线运行状态上报与可观测查询服务。在**重复上报、乱序到达、并发写入**
三类现实条件下，对外提供：

1. 唯一且可解释的运行状态收敛结论；
2. 按任务各自期望周期的新鲜度/滞后判定；
3. 去重、可恢复的告警，以及归一化的错误类型。

## 1. 运行与验证

```bash
uv sync                       # 安装依赖（含 pytest）
uv run uvicorn app.main:app --reload   # 启动服务
uv run pytest                 # 全部单测（日志会打印事件输入与判定依据）
uv run pytest -q              # 精简输出
```

测试用例共 26 个，分布在：

| 文件 | 覆盖内容 |
| --- | --- |
| `tests/test_store.py` | 乱序收敛、重复幂等、终态吸收、拒绝原因可区分、并发去重/收敛 |
| `tests/test_freshness.py` | 首次无数据、宽限、超期、错过、按任务独立周期、时钟回拨、结论稳定性、边界 |
| `tests/test_alerts.py` | 持续故障只告警一次、恢复后重新触发、故障类型区分、并发只开一条告警 |
| `tests/test_api.py` | HTTP 全链路、错误归一化、生命周期、并发 POST 去重 |

单测日志（`log_cli` 默认开启）形如：

```
INFO observability.tests [乱序] 输入事件 run_id=r1 ... type=SUCCEEDED occurred_at=30 ...
  => 判定 accepted=True state=SUCCEEDED reason=ACCEPTED | 事件已接收，收敛后状态=SUCCEEDED
INFO observability.events event_ingest ... -> accepted=False state=SUCCEEDED
  reason=TERMINAL_STALE detail=运行已处于终态 SUCCEEDED，事件 PROGRESS 被拒绝
```

## 2. 事件模型

`POST /runs/events` 的请求体（`app/models.py::RunEvent`）：

| 字段 | 说明 |
| --- | --- |
| `run_id` / `task_id` | 运行与任务标识，非空 |
| `event_type` | `STARTED` / `PROGRESS` / `SUCCEEDED` / `FAILED` / `CANCELED` |
| `occurred_at` | 事件发生时间（秒，客户端时间戳，`>= 0`） |
| `seq` | 同一次运行内的单调序号，默认 0；与 `occurred_at` 共同决定先后 |
| `progress` | 可选，`0~100`，仅 `PROGRESS` 语义使用 |
| `client_id` | 上报方标识，非空 |

事件到运行状态的映射：

```
STARTED, PROGRESS -> RUNNING
SUCCEEDED         -> SUCCEEDED（终态）
FAILED            -> FAILED（终态）
CANCELED          -> CANCELED（终态）
```

运行状态机：`PENDING -> RUNNING -> {SUCCEEDED | FAILED | CANCELED}`。

## 3. 状态收敛规则

结论只依赖“已接收事件集合”，与到达顺序无关。实现见 `app/store.py`。

- **顺序键**：`(occurred_at, seq, 事件优先级)`。
  - 同一 `(occurred_at, seq)` 并列时，用确定性优先级裁决：
    `STARTED(0) < PROGRESS(1) < SUCCEEDED/FAILED/CANCELED(2)`，
    保证任何输入下获胜者唯一，不依赖字典序/线程调度。
- **事件身份键**：`(event_type, occurred_at, seq)`，用于幂等去重。
- **当前状态** = 顺序键最大的已接收事件所映射的状态；进度取该事件
  （或携带进度的最新事件）的 `progress`。
- **拒绝原因（`reject_reason`）**，互相可区分：
  - `DUPLICATE_EVENT`：身份键完全相同的事件重复投递；
  - `OUT_OF_ORDER`：运行尚未终结，但新事件顺序键不新于已接收最新事件
    （晚到/并列冲突），拒绝且不覆盖任何字段；
  - `TERMINAL_STALE`：运行已处于终态，终态是吸收态，任何事件都不得使其回退；
  - `CLOCK_SKEW`：事件时间领先服务器单调时钟超过容差（默认 60s）。

被拒绝的事件统一返回 `200`，响应体 `accepted=false` 并带原因与判定依据，
因为这是“已做出的业务判定”而非请求格式错误；格式/时钟类问题返回 `422`。

正序与逆序投递同一批事件，终态结论完全一致（见
`test_out_of_order_events_converge_uniquely`）。所有读写在同一把锁内完成，
并发下保证：同一身份最多接收一次、全局最大键事件必然获胜、终态不被覆盖。

## 4. 新鲜度与滞后判定

任务通过 `POST /tasks` 注册各自的期望周期（`TaskConfig`）：

| 配置 | 含义 |
| --- | --- |
| `expected_period_seconds` | 期望上报周期（>0），判定按任务独立 |
| `grace_seconds` | 宽限时间，抖动容忍 |
| `miss_after_periods` | 多少个周期无数据判定为“错过”，默认 1.0 |

判定为纯函数（`app/freshness.py::evaluate_freshness`）。
设 `elapsed = max(0, evaluated_at - last_seen_at)`：

| 条件 | 结论 |
| --- | --- |
| 从未上报（`last_seen_at is None`） | `NO_DATA`（滞后为 `null`，不告警不误报） |
| `elapsed <= grace` | `FRESH` |
| `grace < elapsed < period * miss_after_periods` | `OVERDUE`，滞后 `elapsed - grace` |
| `elapsed >= period * miss_after_periods` | `MISSED` |

稳定性边界处理：

- **抗抖动**：结论是同一对 `(last_seen_at, evaluated_at)` 的纯函数，反复评估不翻转；
  `elapsed` 恰好等于宽限仍为 `FRESH`。
- **时钟回拨**：`evaluated_at < last_seen_at` 时滞后钳为 0，结论为 `FRESH`，
  绝不凭空产生滞后。
- **时间源**：新鲜度使用服务端单调时钟（`time.monotonic`，见 `app/clock.py`）
  记录“接收时刻”，不受客户端时钟漂移影响；客户端时间异常过大则以
  `CLOCK_SKEW` 拒绝。
- 只有**被接收**的事件才推进任务的 `last_seen_at`，被拒绝的重复/乱序/过期
  事件不会影响新鲜度。

查询：`GET /tasks/{task_id}/freshness`，返回结论、滞后秒数与文字依据 `reason`。

## 5. 告警去重与恢复

`app/alerts.py` 按 `(task_id, 故障类型)` 维度维护状态机：

- `OVERDUE` 对应 `STALE_DATA`，`MISSED` 对应 `MISSED_RUN`（两种可区分类型）；
  `FRESH` / `NO_DATA` 视为健康；
- 故障且无活动告警 → 新开一条 `FIRING`；
- 持续故障 → 幂等，**不重复产生告警**；
- 恢复健康 → 关闭为 `RESOLVED` 并记录 `resolved_at`；
- 恢复后再次故障 → **新开一条**告警（新的 `incident_id`）。

“评估 + 转态”在锁内原子完成，并发查询/上报只会打开恰好一条活动告警
（见 `test_concurrent_evaluation_opens_exactly_one_incident`）。
查询：`GET /incidents?task_id=...`，返回含历史（已恢复）在内的告警轨迹。

## 6. 错误归一化

所有对外错误都是稳定、可区分的领域类型，不泄露底层堆栈
（`app/errors.py`）：

| `error` | HTTP 状态码 | 场景 |
| --- | --- | --- |
| `INVALID_PAYLOAD` | 422 | 请求体字段/类型/取值不合法 |
| `CLOCK_SKEW` | 422 | 事件时间相对服务器异常超前 |
| `RUN_NOT_FOUND` | 404 | 查询不存在的运行 |
| `TASK_NOT_FOUND` | 404 | 查询未注册任务的新鲜度 |
| `INTERNAL_ERROR` | 500 | 兜底：未预期异常只记日志，不透出 |

业务判定类拒绝（重复/乱序/终态过期）走 `200 + accepted=false`，见第 3 节。

## 7. HTTP 接口一览

```
POST /tasks                         注册任务周期配置
POST /runs/events                   上报运行事件
GET  /runs/{run_id}                 查询运行收敛快照
GET  /tasks/{task_id}/freshness     查询新鲜度（同时推进告警状态机）
GET  /incidents?task_id=...         查询告警（含历史）
```

### 快速体验

```bash
curl -s -X POST localhost:8000/tasks -H 'Content-Type: application/json' \
  -d '{"task_id":"t1","expected_period_seconds":10,"grace_seconds":2}'

curl -s -X POST localhost:8000/runs/events -H 'Content-Type: application/json' \
  -d '{"run_id":"r1","task_id":"t1","event_type":"STARTED",
       "occurred_at":1000.0,"client_id":"c1"}'
```

## 8. 模块结构

```
app/
  models.py    枚举、事件/快照/配置/告警等领域模型
  clock.py     单调时钟抽象（SystemClock / 测试用 ManualClock）
  errors.py    领域错误与统一归一化处理器
  store.py     加锁的事件存储与状态收敛（去重/乱序/终态吸收）
  freshness.py 新鲜度纯函数判定
  alerts.py    告警状态机（去重、恢复、重触发）
  service.py   编排：时钟校验、入库、最近可见时间、新鲜度+告警联动
  api.py       HTTP 路由
  main.py      create_app() 应用装配
tests/         单测（事件输入与判定依据打印到日志）
```

> 说明：当前为内存存储（进程内、加锁），适用于单实例与测试；
> 持久化/多实例可在 `RunStore`/`AlertManager` 接口后替换实现，收敛与
> 幂等规则保持不变。
