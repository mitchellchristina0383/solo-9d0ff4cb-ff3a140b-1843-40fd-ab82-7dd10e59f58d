# 只读查询 API（带守护的 PostgreSQL 连接池）

数据库值班场景：多个应用共用有限的 PostgreSQL 连接。客户端等慢 SQL 超时后，
查询可能仍占着会话；未清理的事务也可能影响下一个请求。本服务用
FastAPI + 自研异步连接池解决这些问题：

- 每个请求可**分别设置**“等待连接的超时”（`acquire_timeout`）和
  “执行查询的超时”（`query_timeout`）；
- 排队中的请求被取消后**立即释放队列名额**（排队不占用连接）；
- 查询超时、客户端断开、SQL 报错后，连接标记为“脏”，**在确认会话
  状态已清理之前绝不再借出**：先做服务端取消（`pg_cancel_backend` 等价的
  CancelRequest）、回滚遗留事务、再用 `SELECT 1` 验证；无法确认的一律
  **关闭并更换新连接**；
- 连接数有硬上限：每条连接始终属于一个槽位，更换时先关旧再开新，
  取消与归还交错也不会越限；
- 会话级只读（`default_transaction_read_only=on`），本 API 只暴露
  固定的 SELECT 查询，不执行任意 SQL。

此外，服务内置 **pg_stat_statements 巡检榜单**（见下文“巡检”一节）：
值班员在 `/docs` 手动采样，即可得到相邻两次巡检之间按执行耗时增量
排名的最耗时 SQL，并自动识别单条重置 / 淘汰重现等不可比条目。

## 快速开始

```bash
docker compose up --build
```

- API 文档（Swagger UI）：http://localhost:8000/docs
- 连接池指标：http://localhost:8000/pool/stats
- 巡检接口前缀：`/inspect`（同样在 `/docs` 内可点击）
- PostgreSQL：`localhost:5432`，用户/密码/库均为 `app`/`app`/`demo`

连接池大小用环境变量 `POOL_SIZE` 控制（默认 4，见 `docker-compose.yml`）。
数据库镜像为 **PostgreSQL 17**：巡检依赖按条目的 `stats_since`（起算时间）
与单条重置 `pg_stat_statements_reset(userid, dbid, queryid)`，这两者都是
PG17 的 pg_stat_statements 才具备的能力；扩展在 `db/init.sql` 中自动创建。


## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/query/normal` | 普通查询（`SELECT ... FROM items LIMIT n`） |
| POST | `/query/slow` | 延时查询（`SELECT pg_sleep(n)`），模拟慢 SQL |
| POST | `/query/error` | 报错查询：`autocommit` 单语句报错；`transaction` 在 `BEGIN` 后报错，故意留下失败事务 |
| GET | `/pool/stats` | 连接池指标：`in_use`（在用数）、`waiting`（等待数）、`idle`、`cleaning`、`opened_connections` 及各类计数器 |
| POST | `/demo/burst` | 后台并发发起 n 个慢查询，便于在 `/pool/stats` 观察排队 |

查询类接口的请求体都支持 `acquire_timeout` / `query_timeout`（秒，`null` 表示不限）。

响应码约定：`200` 成功；`503` 等连接超时；`504` 查询执行超时；
`400` SQL 错误；`502` 连接级故障（如后端被杀）；`499` 客户端中途断开。

## 在 /docs 里演示

1. **排队**：先 `POST /demo/burst`（默认 8 个 3 秒慢查询，池只有 4 条
   连接），立刻反复 `GET /pool/stats`，可看到 `in_use=4, waiting=4`。
2. **等连接超时**：池被 burst 占满时，`POST /query/slow` 设
   `acquire_timeout=1`，约 1 秒后返回 `503`。
3. **查询超时**：`POST /query/slow` 设 `delay_seconds=10, query_timeout=1`，
   约 1 秒后返回 `504`；`/pool/stats` 里 `cleaning` 短暂为 1（服务端
   取消 + 验证会话），随后恢复，`counters.recycled` 或 `replaced` 加一。
4. **SQL 报错**：`POST /query/error`，`mode=transaction` 会留下失败
   事务；连接池回滚并验证后才复用（`counters.recycled` 加一）。
5. **客户端取消**：用 `curl` 发一个慢查询后按 `Ctrl-C` 中断，
   `/pool/stats` 的 `waiting`/`in_use` 会立刻回落，
   `counters.queue_cancelled` 或 `client_disconnects` 加一。

## 巡检：两次巡检之间的最耗时 SQL

数据库变慢时，值班员想知道“上一次巡检到现在，哪些 SQL 最耗时”。本服务
在 `/docs` 下提供一组手动巡检接口（实现见 `app/pg_inspect.py` 与
`app/pg_inspect_api.py`），数据源是 PostgreSQL 的 `pg_stat_statements`。

### 配对规则

统计条目以四元组 `(userid, dbid, toplevel, queryid)` 标识。一条 SQL 只有
**同时满足**下面两个条件才进入榜单：

1. 在相邻两份**有效样本**中都出现；
2. 两份样本里该条目的 `stats_since`（条目起算时间）相同。

不满足的条目标为**不可比**并给出原因，其余条目照常排名：

| 情形 | 判定 | 不可比原因 |
| --- | --- | --- |
| 单条 `pg_stat_statements_reset(...)` 重置 | 条目保留但计数清零、`stats_since` 推进 | `stats_since_changed` |
| 条目被淘汰（超过 `pg_stat_statements.max`）后重现 | 以新的 `stats_since` 重新累计 | `stats_since_changed`（若中间样本缺失则为 `only_in_*`） |
| 只在新样本出现（新条目 / 重现） | 无旧基线可减 | `only_in_new_sample` |
| 只在旧样本出现（被淘汰 / 全量重置后消失） | 无新值可减 | `only_in_previous_sample` |

入榜条目输出调用数增量（`calls_delta`）、执行耗时增量
（`exec_time_delta_ms`，毫秒）等，并**按执行耗时增量降序排名**。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/inspect/samples` | 手动采集一份样本。首次只建基线，之后每次与上一份有效样本配对生成榜单 |
| GET | `/inspect/ranking` | 最近一次榜单；`?limit=N` 只看前 N 名 |
| GET | `/inspect/samples` | 已保存样本列表与最近样本摘要 |
| GET | `/inspect/samples/{sample_id}` | 某份样本的原始条目快照 |
| POST | `/inspect/ranking/recompute` | 用任意两份已保存样本重算榜单（不改状态，会校验时间方向） |
| POST | `/inspect/reset` | 重置单个条目（演示不可比场景），入参为条目四元组 |

样本与榜单以 JSON 原子落盘到 `INSPECT_DATA_DIR`（容器内 `/data/inspect`，
Compose 中挂载为命名卷 `inspect-data`，重启不丢）。

### 并发与失败语义

- **并发采样**：多个值班员同时点“采样”时，存储层用进程内 `asyncio` 锁 +
  跨进程 `fcntl` 文件锁串行化，并在锁内完成“分配单调序号 → 采集 → 比对
  基线 → 落盘”。基线防回退：新样本的 `(observed_at, 单调序号)` 必须严格
  新于已保存基线，否则返回 `stale_observation` 拒绝，**较早的观测不会
  覆盖较新的基线**。
- **失败保留**：采集失败（数据库报错）或保存失败（磁盘错误）时不写任何
  文件，接口返回 502，磁盘上的上次有效样本与榜单原样保留；故障恢复后
  采样可继续，配对不受影响。

### 在 /docs 里演示巡检

1. 先造一些流量：连发若干次 `POST /query/normal` 和 `POST /query/slow`。
2. `POST /inspect/samples` 两次（第一次建基线，第二次出榜单），
   `GET /inspect/ranking` 即可看到 `pg_sleep` 一类慢 SQL 排在最前，
   并带 `calls_delta` 与 `exec_time_delta_ms`。
3. **单条重置**：从某份样本里复制目标条目的四元组，`POST /inspect/reset`
   后再采样一次，该条目会以 `stats_since_changed` 出现在 `incomparable`
   列表，其他条目照常排名。
4. **并发采样**：同时发起多个 `POST /inspect/samples`，所有请求成功且
   样本序号 / 观测时刻严格递增，无覆盖。

## 自测

```bash
pip install -r tests/requirements.txt
# 可选：提供 psql 连接串后，会额外做服务端核对
# （连接数不超过上限、无残留活跃查询、无失败事务）
export PSQL="postgresql://app:app@127.0.0.1:5432/demo"
python tests/selftest.py

# 巡检模块的单元测试 + 端到端测试（端到端需要可连接的 PG17）
export PG_DSN="postgresql://app@127.0.0.1:5432/demo"
python -m pytest tests/test_inspect.py -v
```

巡检测试覆盖：榜单配对与排名规则、单条重置 / 淘汰重现 / 仅一次出现的
不可比判定、stale 基线防回退、采集与保存失败时的状态保留、并发采样
串行化（进程内与跨进程）、以及对真实 PostgreSQL 17 的端到端链路
（含单条重置、10 路并发采样、全部 `/inspect` 路由的 HTTP 语义）。


覆盖：普通查询、排队与并发上限、等连接超时、排队取消释放名额、
查询超时后的连接清理/更换、两类 SQL 报错后的复用、客户端断开、
以及取消与归还交错的压力测试（全程校验
`idle + in_use + cleaning == size` 且 `opened_connections <= size`）。

## 关键设计

- **槽位模型**：池大小固定，每条连接处于 `idle / in_use / cleaning`
  三态之一；更换连接时先关闭旧连接再创建新连接，因此打开的连接数
  永远不会超过上限。
- **排队不持名额**：`acquire()` 的等待者只挂在条件变量上，被取消
  （如客户端断开）时从队列移除即可，不会泄漏连接。
- **脏连接不出借**：归还时若连接可能仍在执行查询（超时/断开）或
  刚发生 SQL 错误，则转入后台清理：先发服务端取消并等待查询任务
  收尾，再回滚遗留事务，最后 `SELECT 1` 验证会话空闲；任何一步失败
  即关闭并新建连接顶替。清理期间该槽位不可借出。
- **借出前把关**：借出时先做本地状态检查；空闲超过
  `checkout_verify_after` 秒（默认 10）的连接额外做一次 `SELECT 1`
  活性验证，防止数据库重启后把已死的空闲连接借给请求。

## 已知限制

- 连接关闭失败（如对端无响应且超时）时，客户端按已关闭记账，服务端
  可能短暂残留连接，依赖 PostgreSQL 的 TCP keepalive 最终回收；
  `/pool/stats` 的 `opened_connections` 是客户端视角。
- 排队唤醒不保证严格 FIFO（`asyncio.Condition` 语义），极端情况下
  可能有请求等待偏久，但都会在自己的 `acquire_timeout` 内得到确定结果。
- 单进程单事件循环；多副本部署时每个进程各有自己的连接池，
  总连接数 = 副本数 × `POOL_SIZE`，需要值班员按此规划。
- 活性验证只在“借出时”进行；验证通过与实际执行之间的极短窗口内
  连接仍可能失效（此时该请求报错、连接被更换，属正常兜底路径）。

### 巡检功能

- 样本文件按次保存且不做自动清理，长期高频采样会持续占用
  `INSPECT_DATA_DIR` 空间；当前需要运维定期归档/清理 `samples/` 目录
  （`latest.json` 与 `ranking-latest.json` 不要手动删除）。
- 防回退依赖服务端 `now()` 与存储层单调序号；若 PostgreSQL 时钟被
  大幅回拨，对应采样会被标为 `stale_observation` 拒绝，需时钟恢复后
  再采样（已有样本与榜单不受影响）。
- 跨进程互斥依赖 `fcntl`（Linux/macOS 可用）；在不支持 `fcntl` 的
  平台上退化为仅进程内锁，单 worker 部署仍然正确，多 worker 需借助
  共享存储上的其他锁机制。
- 巡检只读取/重置 `pg_stat_statements`，不做查询计划级分析；要定位
  具体慢因仍需结合 `EXPLAIN` / `pg_stat_activity` 等手段。

