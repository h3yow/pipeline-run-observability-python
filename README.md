# Pipeline Run Observability

基于 FastAPI 的流水线运行状态上报与可观测查询服务。在**重复上报、乱序到达、并发写入**
三类条件下，接收方都能得到唯一、稳定、可解释的结论。

## 1. 快速开始

```bash
uv sync
uv run pytest                       # 运行全部测试（日志会打印事件输入与判定依据）
uv run uvicorn main:app --reload    # 启动服务
```

服务为内存实现（单进程、线程安全），重启后数据清空。

## 2. 事件模型

`POST /runs/events`

| 字段 | 说明 |
| --- | --- |
| `event_id` | 事件全局唯一 ID，用于**幂等去重** |
| `run_id` | 一次运行的 ID；同一 run 的事件共享状态机 |
| `task_id` | 任务 ID，需先通过 `POST /tasks` 注册期望周期 |
| `event_type` | `START` / `PROGRESS` / `SUCCEEDED` / `FAILED` / `CANCELLED` |
| `seq` | 生产者维护的**单调递增序号**，乱序排序与终态保护的唯一依据 |
| `occurred_at` | 生产者时钟下的事件发生时间（epoch 秒） |
| `progress` | 仅 `PROGRESS` 必填（0–100） |
| `error` | 仅 `FAILED` 必填 |

状态机：`PENDING → RUNNING → {SUCCEEDED | FAILED | CANCELLED}`，
后三者为终态；`PENDING` 表示事件已保留但尚缺可应用的 `START`（存在序号缺口或孤立 `PROGRESS`）。

### 2.1 状态收敛规则（与到达顺序无关）

1. 事件一旦被接受即**不可变保留**到按 `seq` 索引的日志中；
2. 最终状态由“按 `seq` 升序重放整份日志”得出，因此无论事件按什么顺序到达，
   结论都唯一确定；
3. `event_id` 重复 → 拒绝为 `DUPLICATE_EVENT`（幂等，重复上报不改变任何状态）；
4. 同一 `seq` 槽位被不同 `event_id` 占用 → 拒绝为 `EVENT_STALE`（槽位以先到者为准）；
5. 重放中遇到终态后，其后序号更大的任何事件（如迟到的 `PROGRESS`）都不能回退终态，
   对外拒绝为 `RUN_ALREADY_TERMINAL`，响应体给出 `terminal_status / terminal_seq / event_seq`；
6. 多个终态事件并存时，以 **`seq` 最小的终态事件**作为终因（迟到终态不能改写更早的结局）；
7. 非法跃迁（运行中重复 `START`、`START` 之前的 `PROGRESS`）事件保留留痕但不应用，
   响应 `detail.ignored_by_convergence=true` 可查。

> 设计要点：高水位（high-watermark）只用来标注“见过的最大 seq”，**不会**把晚到但
> 序号更小的合理事件丢弃——它们仍进入日志并参与重放，这是“乱序到达结论唯一”的关键。

### 2.2 归一化错误类型

所有错误响应统一为 `{ "error_code", "message", "detail" }`，不回显底层堆栈：

| HTTP | error_code | 触发场景 |
| --- | --- | --- |
| 404 | `UNKNOWN_TASK` / `UNKNOWN_RUN` | 任务未注册 / 运行不存在 |
| 409 | `DUPLICATE_EVENT` | `event_id` 重复上报 |
| 409 | `EVENT_STALE` | 同 `seq` 冲突（乱序覆盖） |
| 409 | `RUN_ALREADY_TERMINAL` | 终态后迟到事件试图回退 |
| 422 | `INVALID_EVENT` | 字段/协议校验失败、`occurred_at` 超前服务端 60s 以上 |
| 500 | `INTERNAL_ERROR` | 未预期错误（详情仅写服务端日志） |

## 3. 新鲜度与滞后判定

任务配置 `POST /tasks`：

- `expected_period_seconds`：该任务的**期望事件周期**（每个任务独立口径）；
- `lag_tolerance_seconds`：破限后的**宽限/防抖时间**；
- `recovery_grace_seconds`：恢复迟滞带宽度。

查询：`GET /tasks/{task_id}/freshness`、`GET /freshness`。

判定口径：

- 滞后 `lag = now - last_received_at`，使用**服务端接收时间**，避免生产者时钟抖动污染结论；
- **首次上报前**：`NO_DATA`（`lag=null`），不判超期、不产生告警；
- 首次事件只建立基线，永远不会立刻判超期；
- `lag > expected_period` 视为破限，但需**持续 `lag_tolerance_seconds`** 才转为
  `OVERDUE`；破限不足宽限停留在 `GRACE_WINDOW`（对外仍是 `FRESH`），
  因此一次延迟尖刺不会反复误报；
- 恢复使用迟滞：`OVERDUE` 后只有 `lag ≤ expected_period + recovery_grace` 才回 `FRESH`
  （通常由新事件到达触发，`reason` 会标记 `RECOVERED`）；
- **时钟回拨**：当 `occurred_at` 相对上一事件回退超过 1s，仅置 `clock_skew=true`，
  不更新生产者时钟基线，也不改变基于服务端时间的滞后判定；
- 每次结论都带可读 `reason`（如 `OVERDUE_DETECTED: lag 14.000s beyond expected
  period 10.000s for 4.000s (tolerance 2.000s)`），可直接作为判定依据。

## 4. 告警去重与恢复

- 告警键：`task_id + fault_type`（当前故障类型为 `STALLED`）；
- 同一故障在 `FIRING` 持续期内，无论上报多少次，**只有一条告警**，重复观察只累加
  `count` 与刷新 `last_seen_at`；
- 新鲜度恢复后告警置 `RESOLVED`；**恢复后再次故障会重新触发**为新的 `FIRING`
  （同一键位复用记录，`count` 重置为 1，`first_seen_at` 更新）；
- 摄入路径与查询路径都会联动告警，因此“仅时间流逝导致的超期”也能被发现；
- 摄入、收敛、告警联动在同一把锁内串行化，`AlertManager` 内部再加锁，
  并发上报不会产生重复告警；
- 查询：`GET /alerts?state=FIRING|RESOLVED`。

## 5. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 注册/覆盖任务期望周期配置 |
| POST | `/runs/events` | 上报一个运行事件 |
| GET | `/runs/{run_id}` | 查询单次运行收敛后的状态 |
| GET | `/runs?task_id=` | 列出运行 |
| GET | `/tasks/{task_id}/freshness` | 单任务新鲜度与滞后 |
| GET | `/freshness` | 全部任务新鲜度 |
| GET | `/alerts?state=` | 告警列表 |

## 6. 验证方法

```bash
uv run pytest -q                    # 23 个用例
uv run pytest tests/test_concurrency.py   # 重复/乱序/并发专项
```

测试覆盖：

- `test_convergence.py`：重复上报幂等、乱序收敛、正逆序结论一致、终态不回退、
  同 seq 冲突、非法负载归一、孤立 PROGRESS；
- `test_freshness.py`：NO_DATA、首次不超期、宽限防抖、迟滞恢复、恢复后重新触发、
  时钟回拨标记；
- `test_concurrency.py`：24 路乱序并发投递（每事件重复 8 次）结论唯一、
  30 路相同事件仅 1 次 ACCEPTED、100 路并发故障仅 1 条告警；
- `test_api.py`：查询端点与统一错误结构。

测试日志会打印每个事件输入（`INPUT ...`）与最终判定依据（`DECISION ...` /
`FINAL STATE ...`）；服务端摄入日志同样输出事件字段、决策原因与收敛状态。

### 6.1 手动验证示例

```bash
curl -sXPOST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"task_id":"t1","expected_period_seconds":10,"lag_tolerance_seconds":2}'

# 先报结束、再报开始（乱序），最终仍唯一收敛为 SUCCEEDED
curl -sXPOST localhost:8000/runs/events -H 'content-type: application/json' \
  -d '{"event_id":"e3","run_id":"r1","task_id":"t1","event_type":"SUCCEEDED","seq":2,"occurred_at":1002}'
curl -sXPOST localhost:8000/runs/events -H 'content-type: application/json' \
  -d '{"event_id":"e1","run_id":"r1","task_id":"t1","event_type":"START","seq":0,"occurred_at":1000}'

curl -s localhost:8000/runs/r1
# -> "status":"SUCCEEDED","high_watermark":2
```
