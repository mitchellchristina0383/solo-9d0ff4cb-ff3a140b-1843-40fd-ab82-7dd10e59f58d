"""只读查询 API：多个应用共享有限的 PostgreSQL 连接。

- 等待连接与执行查询的超时相互独立，可按请求设置；
- 排队中的请求被取消后立即释放队列名额；
- 查询超时、客户端断开或 SQL 报错后，连接标记为“脏”并交还连接池，
  只有确认会话状态已清理的连接才会被复用，否则更换新连接。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from typing import Any, Awaitable, Callable, Literal

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from pool import AcquireTimeoutError, AsyncPool, PoolClosedError, PoolConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("api")


# ---------------------------------------------------------------- 异常类型

class ClientGone(Exception):
    """客户端在排队或查询期间断开连接。"""


class QueryTimeout(Exception):
    """查询未在请求给定的超时时间内完成。"""


class SqlFailed(Exception):
    """SQL 执行报错。status_code 区分客户端 SQL 错误(400)与连接故障(502)。"""

    def __init__(self, detail: str, status_code: int = 400):
        super().__init__(detail)
        self.status_code = status_code


# ---------------------------------------------------------------- 请求模型

class QueryOpts(BaseModel):
    acquire_timeout: float | None = Field(
        10.0, description="等待连接的最长时间（秒），null 表示不限")
    query_timeout: float | None = Field(
        30.0, description="查询执行超时（秒），null 表示不限")


class NormalQuery(QueryOpts):
    limit: int = Field(5, ge=1, le=100, description="返回行数")


class SlowQuery(QueryOpts):
    delay_seconds: float = Field(
        5.0, ge=0, le=120, description="pg_sleep 延时秒数")


class ErrorQuery(QueryOpts):
    mode: Literal["autocommit", "transaction"] = Field(
        "autocommit",
        description="autocommit=单语句报错；transaction=BEGIN 后报错，留下失败事务")


class Burst(BaseModel):
    n: int = Field(8, ge=1, le=64, description="并发慢查询数量")
    delay_seconds: float = Field(3.0, ge=0, le=120)
    acquire_timeout: float | None = Field(30.0, description="每个请求的等连接超时")


# ---------------------------------------------------------------- 应用装配

@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = AsyncPool(PoolConfig(
        size=int(os.environ.get("POOL_SIZE", "4")),
        dsn=dict(
            host=os.environ.get("PGHOST", "localhost"),
            port=int(os.environ.get("PGPORT", "5432")),
            dbname=os.environ.get("PGDATABASE", "demo"),
            user=os.environ.get("PGUSER", "app"),
            password=os.environ.get("PGPASSWORD", "app"),
        ),
    ))
    await pool.start()
    app.state.pool = pool
    logger.info("pool ready: %s", await pool.stats())
    yield
    await pool.close()


app = FastAPI(
    title="只读查询 API（带守护的连接池）",
    description=(
        "多个应用共享有限的 PostgreSQL 连接。支持普通 / 延时 / 报错查询，"
        "可在 /pool/stats 查看连接池在用数、等待数等指标。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(AcquireTimeoutError)
async def _on_acquire_timeout(req: Request, exc: AcquireTimeoutError):
    return JSONResponse(
        {"error": "acquire_timeout", "detail": str(exc)}, status_code=503)


@app.exception_handler(QueryTimeout)
async def _on_query_timeout(req: Request, exc: QueryTimeout):
    return JSONResponse(
        {"error": "query_timeout", "detail": str(exc)}, status_code=504)


@app.exception_handler(SqlFailed)
async def _on_sql_failed(req: Request, exc: SqlFailed):
    return JSONResponse(
        {"error": "sql_error", "detail": str(exc)}, status_code=exc.status_code)


@app.exception_handler(ClientGone)
async def _on_client_gone(req: Request, exc: ClientGone):
    # 客户端已经断开，响应大概率发不出去，状态码仅作记录用途。
    return JSONResponse({"error": "client_disconnected"}, status_code=499)


# ---------------------------------------------------------------- 核心执行逻辑

async def _watch_disconnect(request: Request) -> None:
    while not await request.is_disconnected():
        await asyncio.sleep(0.1)


async def _run_query(
    request: Request,
    opts: QueryOpts,
    fn: Callable[[psycopg.AsyncConnection], Awaitable[dict]],
) -> Any:
    """借一条池化连接并执行 fn(conn)，带独立的两段超时。

    - 等连接与执行查询的超时互不影响；
    - 排队时客户端断开：队列名额立即释放；
    - 查询超时 / 客户端断开 / SQL 报错：连接按“脏”归还，由连接池
      清理验证后再复用或更换。
    """
    pool: AsyncPool = request.app.state.pool
    disconnect = asyncio.create_task(_watch_disconnect(request))
    conn = None
    query: asyncio.Task | None = None
    started = time.monotonic()
    try:
        # ---- 阶段 1：借连接（受 acquire_timeout 约束）
        acquire = asyncio.create_task(pool.acquire(opts.acquire_timeout))
        done, _ = await asyncio.wait(
            {acquire, disconnect}, return_when=asyncio.FIRST_COMPLETED)
        if disconnect in done:
            if not acquire.done():
                acquire.cancel()
                with suppress(asyncio.CancelledError, AcquireTimeoutError,
                              PoolClosedError):
                    await acquire
                pool.counters["queue_cancelled"] += 1
            elif acquire.exception() is None:
                # 竞态：刚拿到连接但客户端已离开，干净归还
                await pool.release(acquire.result(), dirty=False)
                pool.counters["queue_cancelled"] += 1
            raise ClientGone()
        conn = acquire.result()  # 可能抛 AcquireTimeoutError -> 503

        # ---- 阶段 2：执行查询（受 query_timeout 约束）
        query = asyncio.create_task(fn(conn))
        done, _ = await asyncio.wait(
            {query, disconnect}, timeout=opts.query_timeout,
            return_when=asyncio.FIRST_COMPLETED)
        if disconnect in done:
            pool.counters["client_disconnects"] += 1
            await pool.release(conn, dirty=True, pending=query)
            conn = None
            raise ClientGone()
        if query not in done:
            pool.counters["query_timeouts"] += 1
            await pool.release(conn, dirty=True, pending=query)
            conn = None
            raise QueryTimeout(
                f"query did not finish within {opts.query_timeout}s")
        try:
            result = query.result()
        except psycopg.Error as exc:
            pool.counters["sql_errors"] += 1
            await pool.release(conn, dirty=True)
            conn = None
            status = 502 if isinstance(exc, psycopg.OperationalError) else 400
            raise SqlFailed(f"{type(exc).__name__}: {exc}", status) from exc
        await pool.release(conn, dirty=False)
        conn = None
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return result
    finally:
        disconnect.cancel()
        with suppress(asyncio.CancelledError):
            await disconnect
        if conn is not None:  # 意外退出（如服务关停）时按脏连接归还
            await pool.release(conn, dirty=True, pending=query)


# ---------------------------------------------------------------- 接口

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


@app.post("/query/normal", summary="普通查询")
async def query_normal(request: Request, opts: NormalQuery | None = None):
    opts = opts or NormalQuery()

    async def run(conn):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, name, price FROM items ORDER BY id LIMIT %s",
                (opts.limit,))
            rows = await cur.fetchall()
        return {"rows": [{"id": r[0], "name": r[1], "price": float(r[2])}
                         for r in rows]}

    return await _run_query(request, opts, run)


@app.post("/query/slow", summary="延时查询（pg_sleep）")
async def query_slow(request: Request, opts: SlowQuery | None = None):
    opts = opts or SlowQuery()

    async def run(conn):
        async with conn.cursor() as cur:
            await cur.execute("SELECT pg_sleep(%s)", (opts.delay_seconds,))
            await cur.fetchall()
        return {"slept_seconds": opts.delay_seconds}

    return await _run_query(request, opts, run)


@app.post("/query/error", summary="报错查询")
async def query_error(request: Request, opts: ErrorQuery | None = None):
    opts = opts or ErrorQuery()

    async def run(conn):
        async with conn.cursor() as cur:
            if opts.mode == "transaction":
                # 故意留下一个失败的事务，考验连接池的清理
                await cur.execute("BEGIN")
            await cur.execute("SELECT 1/0")
        return {}  # 不会到达

    return await _run_query(request, opts, run)


@app.get("/pool/stats", summary="连接池指标（在用数、等待数等）")
async def pool_stats(request: Request):
    return await request.app.state.pool.stats()


@app.post("/demo/burst", summary="演示：并发打满连接池，观察排队")
async def demo_burst(request: Request, opts: Burst | None = None):
    """后台并发发起 n 个慢查询，配合 GET /pool/stats 观察 waiting/in_use。"""
    opts = opts or Burst()
    pool: AsyncPool = request.app.state.pool
    for _ in range(opts.n):
        pool.spawn(_burst_one(pool, opts))
    return {"launched": opts.n,
            "hint": "马上反复查询 GET /pool/stats 观察 in_use / waiting"}


async def _burst_one(pool: AsyncPool, opts: Burst) -> None:
    try:
        conn = await pool.acquire(opts.acquire_timeout)
    except Exception:
        return
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT pg_sleep(%s)", (opts.delay_seconds,))
            await cur.fetchall()
    except Exception:
        await pool.release(conn, dirty=True)
        return
    await pool.release(conn, dirty=False)
