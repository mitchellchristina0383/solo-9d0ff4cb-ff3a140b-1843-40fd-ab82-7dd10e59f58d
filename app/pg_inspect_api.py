"""巡检接口：在 /docs（Swagger UI）上手动采集 pg_stat_statements 并查看榜单。

路由都挂在 /inspect 前缀下，与业务只读接口分离。巡检连接独立于业务
连接池单独建立（autocommit、非只读），原因：

- pg_stat_statements_reset() 不能在只读事务中执行；
- 巡检是低频值班操作，不应占用业务查询的池名额。

失败语义：采集或保存失败时返回 502，磁盘上的上次有效样本与榜单保持
原样（见 pg_inspect.SampleStore 的原子写与 stale 保护）。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import psycopg
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from pg_inspect import (
    RESET_ENTRY_SQL,
    InspectError,
    SampleStore,
    build_ranking,
    collect_sample,
)

router = APIRouter(prefix="/inspect", tags=["巡检：pg_stat_statements 采样与榜单"])

# 样本/榜单落盘目录（容器内）。挂载卷或调整 INSPECT_DATA_DIR 可持久化。
DATA_DIR = os.environ.get("INSPECT_DATA_DIR", "/data/inspect")
RANKING_LIMIT = int(os.environ.get("INSPECT_RANKING_LIMIT", "20"))


def _dsn() -> dict[str, Any]:
    return dict(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "demo"),
        user=os.environ.get("PGUSER", "app"),
        password=os.environ.get("PGPASSWORD", "app"),
    )


@asynccontextmanager
async def inspect_connection():
    """巡检专用短连接：autocommit + 非只读（reset 函数要求可写事务）。"""
    try:
        conn = await psycopg.AsyncConnection.connect(
            **_dsn(), autocommit=True, application_name="inspect-sampler",
            connect_timeout=int(os.environ.get("INSPECT_CONNECT_TIMEOUT", "5")))
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={"error": "connect_failed", "message": str(exc)}) from exc
    try:
        yield conn
    finally:
        await conn.close()


def _store() -> SampleStore:
    """进程内复用同一个存储句柄：asyncio 锁必须共享才能在单进程内
    串行化并发采样；fcntl 文件锁则负责跨进程（多 worker）互斥。"""
    global _STORE_SINGLETON
    if _STORE_SINGLETON is None or _STORE_SINGLETON.base_dir != Path(DATA_DIR):
        _STORE_SINGLETON = SampleStore(DATA_DIR)
    return _STORE_SINGLETON


_STORE_SINGLETON: SampleStore | None = None


# ---------------------------------------------------------------- 请求/响应模型

class ResetRequest(BaseModel):
    userid: int = Field(..., description="条目所属用户 OID")
    dbid: int = Field(..., description="条目所属数据库 OID")
    toplevel: bool = Field(True, description="是否顶层调用条目")
    queryid: int = Field(..., description="pg_stat_statements 的 queryid")


class SampleSummary(BaseModel):
    sample_id: str
    observed_at: str
    entry_count: int


# ---------------------------------------------------------------- 端点

@router.post("/samples", summary="手动采集一份 pg_stat_statements 样本")
async def take_sample() -> dict[str, Any]:
    """采集当前统计视图并落盘。

    - 第一次采集只建立基线，不产生榜单；
    - 之后每次采集都会与上一份*有效*样本配对生成榜单；
    - 并发点击时采样在存储层串行化，较早观测不会覆盖较新基线；
    - 采集/保存失败时，已有样本和榜单不受影响。
    """
    store = _store()
    async with inspect_connection() as conn:
        try:
            return await store.persist_observed_sample(
                lambda seq: collect_sample(conn, seq),
                ranking_limit=RANKING_LIMIT)
        except InspectError as exc:
            raise HTTPException(
                status_code=502,
                detail={"error": "collect_failed", "message": str(exc)}) from exc
        except OSError as exc:
            raise HTTPException(
                status_code=502,
                detail={"error": "persist_failed", "message": str(exc)}) from exc


@router.get("/ranking", summary="最近一次榜单（相邻有效样本的耗时增量排名）")
async def get_ranking(
    limit: int | None = Query(
        None, ge=1, le=500,
        description="只返回前 N 条；默认返回生成榜单时保存的完整可比值"),
) -> dict[str, Any]:
    ranking = _store().load_latest_ranking()
    if ranking is None:
        raise HTTPException(
            status_code=409,
            detail={"error": "ranking_unavailable",
                    "message": "有效样本不足两份，尚未生成榜单；"
                               "请先连续调用两次 POST /inspect/samples"})
    if limit is not None:
        ranking = dict(ranking)
        ranking["ranking"] = ranking["ranking"][:limit]
        ranking["truncated"] = True
    return ranking


@router.get("/samples", summary="列出已保存的有效样本")
async def list_samples() -> dict[str, Any]:
    store = _store()
    ids = store.list_sample_ids()
    latest = store.load_latest()
    return {
        "sample_count": len(ids),
        "sample_ids": ids,
        "latest": (
            SampleSummary(
                sample_id=latest.sample_id,
                observed_at=latest.observed_at,
                entry_count=latest.entry_count).model_dump()
            if latest else None),
    }


@router.get("/samples/{sample_id}", summary="查看某份样本的原始条目快照")
async def get_sample(sample_id: str) -> dict[str, Any]:
    sample = _store().load_sample(sample_id)
    if sample is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "sample_not_found", "sample_id": sample_id})
    return sample.to_json()


@router.post("/reset", summary="重置单条统计条目（演示不可比场景）")
async def reset_entry(req: ResetRequest) -> dict[str, Any]:
    """对单个 (userid, dbid, toplevel, queryid) 条目执行
    pg_stat_statements_reset：计数清零、stats_since 推进到当前时刻，
    条目本身保留。下一次榜单中该条目会因 stats_since 变化被标为不可比。
    """
    async with inspect_connection() as conn:
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    RESET_ENTRY_SQL,
                    (req.userid, req.dbid, req.toplevel, req.queryid))
                row = await cur.fetchone()
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail={"error": "reset_failed", "message": str(exc)}) from exc
    if row is None or row[0] is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "entry_not_found",
                    "message": "给定四元组在 pg_stat_statements 中不存在",
                    "entry_key": req.model_dump()})
    return {
        "reset": req.model_dump(),
        "new_stats_since": row[0].isoformat(),
        "note": "条目计数已清零，stats_since 已推进；下次采样配对时该条目"
                "将以 stats_since_changed 进入 incomparable 列表",
    }


@router.post("/ranking/recompute",
             summary="用已保存的两份样本重新计算榜单（不采集）")
async def recompute(
    previous_id: str = Query(..., description="较早样本的 sample_id"),
    current_id: str = Query(..., description="较晚样本的 sample_id"),
    limit: int | None = Query(None, ge=1, le=500),
) -> dict[str, Any]:
    """供值班员核对：任意两份已保存样本之间重算榜单。不修改任何状态。"""
    store = _store()
    previous = store.load_sample(previous_id)
    current = store.load_sample(current_id)
    if previous is None or current is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "sample_not_found",
                    "previous_found": previous is not None,
                    "current_found": current is not None})
    if not SampleStore._is_newer(previous, current):
        raise HTTPException(
            status_code=400,
            detail={"error": "sample_order_invalid",
                    "message": "current_id 必须是晚于 previous_id 的样本",
                    "previous_observed_at": previous.observed_at,
                    "current_observed_at": current.observed_at})
    return build_ranking(previous, current, limit=limit)
