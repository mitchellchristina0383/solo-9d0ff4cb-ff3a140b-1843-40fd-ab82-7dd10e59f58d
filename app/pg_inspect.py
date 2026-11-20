"""pg_stat_statements 巡检：相邻两次采样的最耗时 SQL 榜单。

值班场景：数据库变慢时，值班员在 API 的 /docs（Swagger UI）上手动点
“采样”，相邻两份*有效*样本之间按执行耗时增量排名，找出两次巡检之间
最耗时的 SQL。

配对规则（PostgreSQL 统计条目以 (userid, dbid, toplevel, queryid) 标识）：

- 条目必须在相邻两份有效样本中**都出现**，且两边的 stats_since（条目
  起算时间）**相同**，才计入榜单；
- 单条重置（pg_stat_statements_reset(userid, dbid, queryid)）会把该条目
  计数清零并把 stats_since 推进到重置时刻 -> stats_since 不一致 -> 不可比；
- 条目被淘汰（超过 pg_stat_statements.max）后再次出现时，会以新的
  stats_since 重新累计 -> 同样不可比；
- 只在其中一份样本出现的条目（新出现 / 被淘汰 / 全量重置后消失）->
  不可比。

并发与失败语义：

- 同一时刻可能有多个值班员点采样。存储层用文件锁串行化，并以样本的
  服务端观测时刻（now，带单调回退保护）为基线：较早的观测绝不会覆盖
  较新的基线（rejected_stale），因此相邻有效样本始终沿时间递增；
- 采集失败或保存失败时，磁盘上上次有效样本与已生成的榜单原样保留，
  接口返回错误而不是破坏现有状态。

本模块不依赖 FastAPI，纯函数部分可直接用 pytest 做确定性单测。
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------- 常量与 SQL

# 统计条目的标识：PG17 的 pg_stat_statements 中同一 queryid 在不同
# user / db / 是否顶层调用下各占一行，因此四元组（见 EntryKey）才是
# 完整标识。

# 采样时落盘的计数列（榜单用到 calls / total_exec_time，其余保留以便
# 日后扩展，且让样本本身可独立审计）。
METRIC_FIELDS = (
    "calls",
    "total_exec_time",
    "total_plan_time",
    "rows",
    "shared_blks_hit",
    "shared_blks_read",
    "shared_blks_dirtied",
    "shared_blks_written",
    "temp_blks_read",
    "temp_blks_written",
)

SAMPLE_SQL = """
SELECT
    userid,
    dbid,
    toplevel,
    queryid,
    query,
    stats_since,
    {metrics}
FROM pg_stat_statements
ORDER BY userid, dbid, toplevel, queryid
""".format(metrics=", ".join(METRIC_FIELDS))

RESET_ENTRY_SQL = """
SELECT pg_stat_statements_reset(userid, dbid, queryid)
FROM pg_stat_statements
WHERE userid = %s AND dbid = %s AND toplevel = %s AND queryid = %s
LIMIT 1
"""


class InspectError(Exception):
    """巡检采集 / 保存过程中的可预期错误（接口层映射为 502）。"""


# ---------------------------------------------------------------- 数据结构

@dataclasses.dataclass(frozen=True)
class EntryKey:
    """一条 pg_stat_statements 统计条目的标识。"""

    userid: int
    dbid: int
    toplevel: bool
    queryid: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "userid": self.userid,
            "dbid": self.dbid,
            "toplevel": self.toplevel,
            "queryid": self.queryid,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "EntryKey":
        return cls(
            userid=int(row["userid"]),
            dbid=int(row["dbid"]),
            toplevel=bool(row["toplevel"]),
            queryid=int(row["queryid"]),
        )


def _iso(ts: datetime | None) -> str | None:
    """timestamptz -> 带偏移的 ISO 字符串；stats_since 理论上非空，空值
    （如备库异常）保留 None，比较时按不可比处理。"""
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.isoformat()


@dataclasses.dataclass
class Sample:
    """一份有效样本：观测时刻 + 全部条目快照。"""

    sample_id: str
    observed_at: str          # 服务端 now()，ISO8601 带时区
    monotonic_seq: int        # 存储层分配的严格递增序号（防同刻并列）
    entry_count: int
    entries: dict[str, dict[str, Any]]  # key 字符串 -> 条目数据

    @staticmethod
    def key_of(k: EntryKey) -> str:
        return json.dumps(k.as_dict(), separators=(",", ":"), sort_keys=True)

    def to_json(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_json(cls, doc: dict[str, Any]) -> "Sample":
        return cls(
            sample_id=str(doc["sample_id"]),
            observed_at=str(doc["observed_at"]),
            monotonic_seq=int(doc["monotonic_seq"]),
            entry_count=int(doc.get("entry_count", len(doc.get("entries", {})))),
            entries=dict(doc["entries"]),
        )


# ---------------------------------------------------------------- 采集

async def collect_sample(conn, monotonic_seq: int) -> Sample:
    """在给定连接上查询 pg_stat_statements，组装成一份 Sample。

    连接需要普通（非只读）会话即可——本函数只执行 SELECT。任何数据库
    错误向上抛 InspectError，由调用方保证不触碰已保存的样本/榜单。
    """
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT now()")
            (now,) = await cur.fetchone()
            await cur.execute(SAMPLE_SQL)
            cols = [d.name for d in cur.description]
            rows = await cur.fetchall()
    except Exception as exc:  # psycopg.Error 等
        raise InspectError(f"采集 pg_stat_statements 失败: {exc}") from exc

    entries: dict[str, dict[str, Any]] = {}
    for row in rows:
        rec = dict(zip(cols, row))
        key = EntryKey.from_row(rec)
        item: dict[str, Any] = {
            "key": key.as_dict(),
            "query": rec["query"],
            "stats_since": _iso(rec["stats_since"]),
        }
        for f in METRIC_FIELDS:
            value = rec[f]
            if isinstance(value, Decimal):
                value = float(value)
            item[f] = value
        entries[Sample.key_of(key)] = item

    observed = _iso(now)
    # sample_id 只用文件名 / URL 路径安全字符
    safe_ts = (observed.replace(":", "").replace("-", "")
               .replace("+", "p").replace(".", ""))
    return Sample(
        sample_id=f"{safe_ts}-{monotonic_seq}",
        observed_at=observed,
        monotonic_seq=monotonic_seq,
        entry_count=len(entries),
        entries=entries,
    )


# ---------------------------------------------------------------- 榜单计算（纯函数）

# 不可比原因代码 -> 说明
REASON_ONLY_NEW = "only_in_new_sample"        # 仅新样本出现（新条目/重现）
REASON_ONLY_OLD = "only_in_previous_sample"   # 仅旧样本出现（被淘汰/重置消失）
REASON_STATS_SINCE = "stats_since_changed"    # 两边都在但起算时间不同
REASON_STATS_SINCE_NULL = "stats_since_missing"


def _entry_stats_since(entry: dict[str, Any]) -> str | None:
    return entry.get("stats_since")


def build_ranking(
    previous: Sample,
    current: Sample,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """对相邻两份有效样本计算增量榜单（纯函数，无 IO）。

    返回：
    - comparable: 入榜条目，按 total_exec_time 增量降序；
    - incomparable: 不可比条目及原因（单条重置 / 淘汰重现 / 只出现一次）；
    - 汇总计数。调用数增量 = current.calls - previous.calls。
    """
    ranked: list[dict[str, Any]] = []
    incomparable: list[dict[str, Any]] = []

    old_keys = set(previous.entries)
    new_keys = set(current.entries)

    # --- 只在一份样本出现的条目 ---
    for k in sorted(new_keys - old_keys):
        incomparable.append(_incomparable(k, current.entries[k], REASON_ONLY_NEW))
    for k in sorted(old_keys - new_keys):
        incomparable.append(
            _incomparable(k, previous.entries[k], REASON_ONLY_OLD))

    # --- 两份都出现的条目：检查起算时间 ---
    for k in sorted(old_keys & new_keys):
        old = previous.entries[k]
        new = current.entries[k]
        old_since = _entry_stats_since(old)
        new_since = _entry_stats_since(new)
        if old_since is None or new_since is None:
            incomparable.append(
                _incomparable(k, new, REASON_STATS_SINCE_NULL))
            continue
        if old_since != new_since:
            # 单条重置或淘汰后重现：起算时间被推进，累计值不连续
            incomparable.append(_incomparable(
                k, new, REASON_STATS_SINCE,
                detail={"previous_stats_since": old_since,
                        "current_stats_since": new_since}))
            continue
        calls_delta = int(new["calls"]) - int(old["calls"])
        exec_delta = float(new["total_exec_time"]) - float(old["total_exec_time"])
        plan_delta = float(new["total_plan_time"]) - float(old["total_plan_time"])
        ranked.append({
            "key": new["key"],
            "query": new["query"],
            "stats_since": new_since,
            "calls_previous": int(old["calls"]),
            "calls_current": int(new["calls"]),
            "calls_delta": calls_delta,
            "total_exec_time_ms_previous": round(float(old["total_exec_time"]), 3),
            "total_exec_time_ms_current": round(float(new["total_exec_time"]), 3),
            "exec_time_delta_ms": round(exec_delta, 3),
            "plan_time_delta_ms": round(plan_delta, 3),
            "rows_delta": int(new["rows"]) - int(old["rows"]),
        })

    # 执行耗时增量降序；并列时调用数增量多的在前，再并列用 key 保证确定性
    ranked.sort(key=lambda r: (
        -r["exec_time_delta_ms"], -r["calls_delta"], json.dumps(
            r["key"], sort_keys=True)))
    incomparable.sort(key=lambda x: (
        x["reason"], json.dumps(x["entry_key"], sort_keys=True)))

    comparable_total = len(ranked)
    if limit is not None and limit >= 0:
        ranked = ranked[:limit]

    return {
        "previous_sample_id": previous.sample_id,
        "current_sample_id": current.sample_id,
        "previous_observed_at": previous.observed_at,
        "current_observed_at": current.observed_at,
        "comparable_count": comparable_total,
        "incomparable_count": len(incomparable),
        "ranking": ranked,
        "incomparable": incomparable,
    }


def _incomparable(
    key_str: str,
    entry: dict[str, Any],
    reason: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = {
        "entry_key": json.loads(key_str),
        "reason": reason,
        "query": entry.get("query"),
        "stats_since": entry.get("stats_since"),
        "calls": entry.get("calls"),
        "total_exec_time_ms": (
            round(float(entry["total_exec_time"]), 3)
            if entry.get("total_exec_time") is not None else None),
    }
    if detail:
        out["detail"] = detail
    return out


# ---------------------------------------------------------------- 存储

class SampleStore:
    """样本与榜单的持久化存储。

    - 目录布局：samples/<sample_id>.json 单样本文件、latest.json 最近样本、
      ranking-latest.json 最近榜单，全部“临时文件 + os.replace”原子落盘；
    - 进程内锁 + 文件锁（fcntl，Linux）双重串行化，兼容多 worker / 多进程；
    - 基线防回退：新样本的 (observed_at, monotonic_seq) 必须严格新于
      已保存的最近样本，否则拒绝（stale），较早观测不会覆盖较新基线；
    - 保存失败抛异常，已有的 latest / 榜单不受影响。
    """

    LATEST_NAME = "latest.json"
    RANKING_NAME = "ranking-latest.json"
    SEQ_NAME = "monotonic.seq"
    LOCK_NAME = ".inspect.lock"

    def __init__(self, base_dir: str | os.PathLike[str]):
        self.base_dir = Path(base_dir)
        self.samples_dir = self.base_dir / "samples"
        self.samples_dir.mkdir(parents=True, exist_ok=True)
        # 进程内串行化（同一 uvicorn 进程内的并发采样请求）
        self._async_lock = asyncio.Lock()
        self._lock_path = self.base_dir / self.LOCK_NAME
        self._lock_path.touch(exist_ok=True)

    # -- 锁 --
    @contextlib.asynccontextmanager
    async def _locked(self):
        """进程内 asyncio 锁 + 跨进程 fcntl 排他锁。

        fcntl 仅在 Linux 可用；缺失时退化为仅进程内锁（单 worker 部署
        依然正确）。锁内允许 await 数据库采集，因此不会阻塞事件循环。
        """
        fcntl = None
        with contextlib.suppress(ImportError):
            import fcntl as _fcntl
            fcntl = _fcntl
        async with self._async_lock:
            fh = None
            if fcntl is not None:
                fh = open(self._lock_path, "a+")
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if fh is not None:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                    fh.close()

    # -- 序号 --
    def _next_seq(self) -> int:
        path = self.base_dir / self.SEQ_NAME
        try:
            seq = int(path.read_text().strip())
        except (FileNotFoundError, ValueError):
            seq = 0
        seq += 1
        self._atomic_write(path, str(seq).encode())
        return seq

    def _rollback_seq(self, seq: int) -> None:
        """采集失败时回收刚分配的序号。仅在持锁状态调用，因此当前序号
        一定就是传入的 seq；文件异常时静默忽略（空号不影响正确性）。"""
        path = self.base_dir / self.SEQ_NAME
        try:
            current = int(path.read_text().strip())
            if current == seq:
                self._atomic_write(path, str(seq - 1).encode())
        except (FileNotFoundError, ValueError, OSError):
            pass

    def _atomic_write(self, path: Path, payload: bytes) -> None:
        tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}")
        try:
            with open(tmp, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                tmp.unlink()

    def _save_json(self, path: Path, doc: dict[str, Any]) -> None:
        self._atomic_write(
            path,
            (json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=False)
             + "\n").encode("utf-8"),
        )

    # -- 读取 --
    def load_latest(self) -> Sample | None:
        path = self.base_dir / self.LATEST_NAME
        if not path.exists():
            return None
        return Sample.from_json(json.loads(path.read_text("utf-8")))

    def load_sample(self, sample_id: str) -> Sample | None:
        path = self.samples_dir / f"{sample_id}.json"
        if not path.exists():
            return None
        return Sample.from_json(json.loads(path.read_text("utf-8")))

    def list_sample_ids(self) -> list[str]:
        return sorted(p.stem for p in self.samples_dir.glob("*.json"))

    def load_latest_ranking(self) -> dict[str, Any] | None:
        path = self.base_dir / self.RANKING_NAME
        if not path.exists():
            return None
        return json.loads(path.read_text("utf-8"))

    # -- 写入 --
    @staticmethod
    def _is_newer(latest: Sample, candidate: Sample) -> bool:
        """候选样本是否严格新于已保存基线。

        先比服务端观测时刻；同一时刻再比存储层分配的单调序号，
        序号在锁内分配，因此后到的采样必然序号更大，不会误伤。
        """
        if candidate.observed_at != latest.observed_at:
            return candidate.observed_at > latest.observed_at
        return candidate.monotonic_seq > latest.monotonic_seq

    # -- 供服务层组合使用的一站式入口 --
    async def persist_observed_sample(
        self,
        build_sample,
        *,
        ranking_limit: int | None = None,
    ) -> dict[str, Any]:
        """在同一把锁内完成“分配序号 -> 采集 -> 比对基线 -> 落盘”。

        build_sample(monotonic_seq) 是返回 Sample 的 awaitable 工厂（通常
        闭包一次数据库采集）。把采集放进锁内意味着并发采样被串行化：
        后一个采样一定带着更晚的服务端 now()，因此只会出现“后到者
        更新”，较早观测不可能覆盖较新基线。采集抛错时本函数不写任何
        文件，上次有效样本与榜单原样保留。
        """
        async with self._locked():
            seq = self._next_seq()
            try:
                sample = await build_sample(seq)
            except Exception:
                # 采集失败：回滚序号，避免失败尝试在序号上留空号；
                # 此时还没有写任何样本/榜单文件
                self._rollback_seq(seq)
                raise
            latest = self.load_latest()
            # 正常路径下 seq 已保证更新；observed_at 比对作为第二道防线，
            # 覆盖时钟异常 / 外部写入样本等情况。
            if latest is not None and not self._is_newer(latest, sample):
                return {
                    "accepted": False,
                    "reason": "stale_observation",
                    "latest_sample_id": latest.sample_id,
                    "rejected_sample_id": sample.sample_id,
                    "ranking_updated": False,
                    "entry_count": sample.entry_count,
                    "observed_at": sample.observed_at,
                }
            ranking = (
                build_ranking(latest, sample, limit=ranking_limit)
                if latest is not None else None)
            self._save_json(
                self.samples_dir / f"{sample.sample_id}.json",
                sample.to_json())
            self._save_json(self.base_dir / self.LATEST_NAME, sample.to_json())
            if ranking is not None:
                self._save_json(self.base_dir / self.RANKING_NAME, ranking)
            return {
                "accepted": True,
                "reason": None,
                "sample_id": sample.sample_id,
                "ranking_updated": ranking is not None,
                "previous_sample_id": latest.sample_id if latest else None,
                "entry_count": sample.entry_count,
                "observed_at": sample.observed_at,
                "comparable_count": (
                    ranking["comparable_count"] if ranking else 0),
                "incomparable_count": (
                    ranking["incomparable_count"] if ranking else 0),
            }
