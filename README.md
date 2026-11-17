# 电表日结 SQL 核对 API

数仓工程师改写电表日结的关联 SQL 后，用本服务确认读数**没有漏计或重复计入**。
提交两条只读 `SELECT`，服务在同一已提交快照上并行读取两侧结果，按
**行内容 + 出现次数**做多重集核对（忽略行序，`NULL` 与空串视为不同），
返回一致结论或最多 20 种差异行及两侧次数。

## 1. 快速启动

```bash
docker compose up --build
# API:   http://localhost:8000
# 文档:  http://localhost:8000/docs   （Swagger UI，可直接发请求演示）
```

启动后 PostgreSQL 自动执行 `sql/init.sql`（源表 + 示例数据 + 示例查询）和
`sql/audit.sql`（日核查报告表与过期检测触发器）；应用启动时也会再幂等执行
一次 `sql/audit.sql`，已有数据卷无需手动迁移。示例数据包含：

- `2026-11-15`：三条 `SELECT` 核对演示用的正常读数；
- `2026-11-16`：日核查演示日，故意包含**读数回退**（1002）、**间隔 3 小时**
  （2001）以及**当天无读数的表**（3001），因此核查结论为不可用。

可直接在 `/docs` 里演示：`POST /daily-audits?date=2026-11-16`，再
`GET /daily-reports?date=2026-11-16`。

停止并清理：

```bash
docker compose down -v
```

## 2. 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 存活探针，不触碰数据库 |
| `POST` | `/jobs` | 提交核对任务（同时只允许一组运行，占用中返回 `409`） |
| `GET`  | `/jobs` | 列出近期任务（含终态），状态查询始终可用 |
| `GET`  | `/jobs/{job_id}` | 查询单个任务状态与结论 |
| `POST` | `/jobs/{job_id}/cancel` | 请求取消，及时受理 |
| `POST` | `/daily-audits?date=YYYY-MM-DD` | 发起某 UTC 日期的读数核查（同一时刻只允许一个日核查） |
| `GET`  | `/daily-audits/{audit_id}` | 查询日核查任务状态与结果 |
| `POST` | `/daily-audits/{audit_id}/cancel` | 请求取消日核查 |
| `GET`  | `/daily-reports?date=YYYY-MM-DD` | 按日期查询最近一次成功的核查报告（含实时过期标记） |
| `GET`  | `/daily-reports/latest?date=YYYY-MM-DD` | 按日期查询最新一次尝试（含 running/failed/aborted） |
| `GET`  | `/daily-reports/history?date=YYYY-MM-DD` | 按日期查看历次尝试 |
| `GET`  | `/daily-reports/{report_id}` | 按报告 ID 查询（历史报告可查，过期状态实时计算） |

任务状态：`pending → running →（cancelling）→ succeeded | failed | cancelled | timeout`。
失败、取消、超时一律 **不发布比较结论**（`result` 为 `null`），仅给出原因。
日核查任务状态另有 `aborted`（扫描途中源表被更正或报告保存失败）。

### 请求示例

```bash
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "left_sql":  "SELECT meter_id::bigint, wh::bigint FROM readings",
  "right_sql": "SELECT meter_id::bigint, wh::bigint FROM readings",
  "name": "daily-settlement-v2"
}'
```

### 结论结构（成功时）

```json
{
  "status": "succeeded",
  "result": {
    "identical": false,
    "columns": ["meter_id", "wh"],
    "column_types": ["bigint", "bigint"],
    "left_row_total": 504013,
    "right_row_total": 504012,
    "matched_group_count": 504011,
    "diff_group_total": 1,
    "diffs_shown": 1,
    "diffs": [
      {"values": [1001, 1150], "left_count": 2, "right_count": 1}
    ]
  }
}
```

## 3. 对提交 SQL 的约束

- 必须是**单条只读 SELECT**（允许 `WITH` / `UNION` / `INTERSECT` / `EXCEPT`），
  静态解析拒绝多语句与任何写操作 / DDL / `SELECT INTO` / `LOCK` / `CALL` 等；
- 结果列只允许 `bigint` 与 `text`，两侧**列数相同、对应位置类型相同**，
  否则本轮失败且不给出比较结论；
- 数据库连接再叠加一层纵深防御：`READ ONLY` 事务、`statement_timeout`、
  `lock_timeout`，即使静态校验被绕过也无法写入或长时间持锁。

## 4. 关键设计

### 同一已提交快照，源表仍可更正

每条任务开三条独立连接：快照连接执行
`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` + `pg_export_snapshot()`，
两条读取连接分别 `SET TRANSACTION SNAPSHOT` 绑定**同一个快照**。
核对期间其他会话对 `readings` / `meters` 的更正照常提交，但对本轮不可见。

### 内存有界的外部归并（不全量载入、不靠哈希判等）

1. 命名服务端游标按 `itersize` 分批拉取，客户端从不缓存整表；
2. 每 `SORT_CHUNK_ROWS`（默认 2 万）行在进程内排序，逐行流式 pickle 落盘为一个 run；
3. 归并阶段对两侧 run 做单轮 k 路堆归并（堆中每流只驻留一行），
   键随行走，按全序键聚合等值组后比较两侧次数；
4. 差异明细最多保留 20 种，但总行数、差异组总数等计数覆盖全量数据；
5. 全程不使用整表哈希作为判等依据。

### 单名额、30 秒时限与终止语义

- 进程内单名额信号量：全局同时只运行一组，冲突返回 `409`；
- 看门狗按 `JOB_TIMEOUT_SECONDS`（默认 30）判定超时；
- 取消/超时/任一侧失败后：`conn.cancel()` 请求中止本轮全部 SQL，
  必要时 `pg_terminate_backend` 兜底；
- 严格按顺序收尾：**中止 SQL → 关闭连接 → 清空暂存目录 → 释放名额 → 才落终态**，
  因此状态查询一旦看到终态，名额必然已经释放、暂存必然已经清空。

## 5. 按 UTC 日期核查日结读数

### 判定口径

对 `date` 当天的 UTC 区间 `[00:00:00Z, 次日 00:00:00Z)`：

1. 每块表按 `read_time` 升序，逐对比较**相邻两条累计 Wh**；
2. 后一条 `<` 前一条 → 记“读数回退”；
3. 相邻间隔 > `MAX_GAP_MINUTES`（默认 120 分钟）→ 记“间隔超过 2 小时”；
4. 同一条读数两种条件都满足时只记一条异常，`reasons` 同时给出；
5. 每块表当天的**首条读数不接前一天**，不参与跨日比较；
6. `meters` 表中任何一块表当天没有读数 → **不能判定为可用**；
7. 当天完全没有读数、或表计档案为空 → 同样不可用；
8. 报告给出全量异常总数，并最多展示排序后的前 20 条
   （表号、前后时刻、前后读数、间隔秒数、原因），按 `(表号, 当前时刻)` 排序。

### 接口

```bash
# 发起核查（返回 audit_id，异步执行）
curl -X POST 'http://localhost:8000/daily-audits?date=2026-11-16'

# 轮询任务；完成后 report 即为已持久化的报告
curl 'http://localhost:8000/daily-audits/{audit_id}'

# 按日期查询最近一次成功报告（stale 为实时计算的过期标记）
curl 'http://localhost:8000/daily-reports?date=2026-11-16'

# 最新一次尝试（可能是 running / failed / aborted）、历次尝试、按 ID 查询
curl 'http://localhost:8000/daily-reports/latest?date=2026-11-16'
curl 'http://localhost:8000/daily-reports/history?date=2026-11-16'
curl 'http://localhost:8000/daily-reports/{report_id}'
```

报告核心字段：

```json
{
  "audit_date": "2026-11-16",
  "status": "succeeded",
  "usable": false,
  "usable_reason": "发现 2 条相邻读数异常；7 块表当天无读数，不能判定为可用",
  "stale": false,
  "settlement_usable": false,
  "anomaly_total": 2,
  "meters_total": 12,
  "meters_with_readings": 5,
  "meters_without_readings": [{"meter_id": 1001, "line_name": "A线"}],
  "anomalies": [
    {
      "seq": 1, "meter_id": 4002,
      "previous_time": "2026-11-16T09:00:00Z", "previous_wh": 1000,
      "current_time": "2026-11-16T10:00:00Z", "current_wh": 850,
      "gap_seconds": 3600, "reasons": ["rollback"],
      "reason_text": "读数回退"
    },
    {
      "seq": 2, "meter_id": 4003,
      "previous_time": "2026-11-16T08:00:00Z", "previous_wh": 2300,
      "current_time": "2026-11-16T11:00:00Z", "current_wh": 2600,
      "gap_seconds": 10800, "reasons": ["gap"],
      "reason_text": "间隔超过 120 分钟"
    }
  ]
}
```

`settlement_usable` 是“能否直接拿去做结算”的唯一判断：需要
`status=succeeded` 且 `usable=true`、`stale=false`，并且该日期**最新一次
尝试**也是成功。任何一次失败/中止的尝试都会阻断旧报告的可用标记，
直到重新核查成功后才恢复。

### 同一已提交快照

- 核查在单个 `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` 事务内完成，
  事务开头先取当天版本号，因此扫描到的数据与记录的 `source_version` 来自同一快照；
- 扫描期间其他会话的更正照常提交，但对本轮不可见。

### 过期：任何已提交改动（含直接 SQL）

`sql/audit.sql` 在 `readings` 上按**语句级 transition table** 建触发器，
把受影响 UTC 日期的版本号 +1，不按行放大：

| 操作 | 过期日期 |
| --- | --- |
| `INSERT` | 新读了所在的 UTC 日 |
| `DELETE` | 被删读数所在的 UTC 日 |
| `UPDATE` | OLD 与 NEW 的 UTC 日**都**标记（读数跨日移动时两天都过期） |
| `TRUNCATE` | 清空前仍存在的全部日期 |

触发器和报告表由应用启动时幂等执行 `sql/audit.sql` 建立，因此**绕过应用、
直接连库执行 SQL** 同样会让旧报告失效。查询时把报告的 `source_version`
与当前版本比较，不同即返回 `stale=true` 并给出原因；历史报告仍可查看，
但 `settlement_usable=false`。

### 扫描途中更正 / 报告保存失败

- 报告先以 `running` 落库，只有全部校验通过才在同一个事务里改成 `succeeded`；
- 保存前对当天版本行 `FOR SHARE` 并复核版本：若扫描开始后有更正提交，
  版本不一致，整个尝试记为 `aborted`，**不发布任何结论**；
- 报告写入失败同样回滚，尝试记为 `aborted`，旧报告因“最新一次尝试失败”
  而 `settlement_usable=false`，直到重查成功才恢复；
- 取消/超时：任务终态为 `cancelled`/`timeout`，尝试置为 `failed`，
  同样不发布结论。

## 6. 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PG_DSN` | `postgresql://recon:recon@postgres:5432/recon` | 数据库连接串 |
| `JOB_TIMEOUT_SECONDS` | `30` | 单轮硬时限 |
| `PG_STATEMENT_TIMEOUT_MS` | `28000` | 服务端单语句超时（留 2s 收尾余量） |
| `SORT_CHUNK_ROWS` | `20000` | 单个排序块行数 |
| `MAX_DIFF_TYPES` | `20` | 差异展示上限 |
| `WORK_DIR` | `/tmp/recon-work` | 外部排序暂存目录（独立卷） |
| `MAX_GAP_MINUTES` | `120` | 相邻读数视为异常的最大间隔（分钟） |
| `MAX_ANOMALIES_SHOWN` | `20` | 日核查报告展示的异常条数上限 |
| `DAILY_READ_CHUNK_ROWS` | `50000` | 日核查流式读取每批行数 |

## 7. 本地自测

```bash
# 单元 + 集成测试（集成测试需要一个 PostgreSQL；可用 PG_DSN 指定）
python -m pytest tests/ -q

# 50 万行级端到端压测（内存峰值、暂存占用、结论正确性、清理与名额释放）
python scripts/load_test.py

# 日核查专项测试（按 UTC 日、异常明细、过期、扫描中更正、保存失败）
python -m pytest tests/test_daily_audit.py -q
```

实测（受限沙箱环境，容器内通常更快）：两侧各 504,012 行、
含一次重复计入差异，端到端约 16 秒，进程峰值 RSS 约 68 MiB，
暂存峰值约 44 MiB，结束后暂存目录清空、名额释放。

## 8. 目录结构

```
app/
  config.py          运行期配置
  sqlvalidation.py   只读 SELECT 静态校验
  db.py              快照会话、只读连接、命名游标流式拉取、列类型闸门
  external_merge.py  流式 run、k 路归并、多重集核对
  manager.py         单名额编排、看门狗、取消/超时/失败的统一收尾
  daily_audit.py     按 UTC 日核查、报告读写、过期判定
  daily_manager.py   日核查单名额编排、取消/超时/失败语义
  models.py          API 模型
  main.py            FastAPI 路由
sql/init.sql         源表、示例数据、示例查询
sql/audit.sql        报告/版本表结构、过期检测触发器（启动时幂等执行）
scripts/             压测数据生成与负载脚本
tests/               单元与集成测试
docker-compose.yml   应用（128MiB 上限）+ PostgreSQL
Dockerfile
```
