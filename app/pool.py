"""一个语义显式的异步 PostgreSQL 连接池。

设计目标（详见 README）：

- 固定数量的“槽位”：每条连接始终属于且只属于一个槽位
  （idle / in_use / cleaning 三态之一），因此打开的连接数永远不会
  超过池上限；
- 可能仍在执行查询的连接（客户端超时、断开）或发生过 SQL 错误的
  连接，在确认会话状态已清理之前绝不会被再次借出；无法确认的一律
  关闭并更换新连接；
- 排队中的请求不占用连接名额，被取消后立即离开等待队列。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass

import psycopg
from psycopg.pq import TransactionStatus

logger = logging.getLogger("pool")


class AcquireTimeoutError(Exception):
    """在请求的超时时间内没有可用连接。"""


class PoolClosedError(Exception):
    """连接池已关闭。"""


def _consume_task_exception(task: asyncio.Task) -> None:
    """取回被我们有意放弃的任务的异常，避免事件循环告警。"""
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:
        pass


@dataclass
class PoolConfig:
    dsn: dict
    size: int = 4
    connect_timeout: float = 5.0
    startup_retries: int = 60      # 启动时等待数据库就绪的重试次数
    retry_interval: float = 1.0
    cleanup_timeout: float = 10.0  # 清理单条连接的总预算
    drain_grace: float = 5.0       # 等待被取消的查询收尾的时间
    close_timeout: float = 3.0
    # 空闲超过该秒数的连接，借出前先用 SELECT 1 验证活性
    # （防止数据库重启后把已死的空闲连接借给请求）；None 表示从不验证
    checkout_verify_after: float | None = 10.0
    checkout_verify_timeout: float = 3.0


class AsyncPool:
    def __init__(self, config: PoolConfig):
        self.cfg = config
        self._cond = asyncio.Condition()
        # 空闲连接及其进入空闲的时刻（monotonic），用于借出前活性验证
        self._idle: list[tuple[psycopg.AsyncConnection, float]] = []
        self._in_use = 0    # 已借给请求
        self._cleaning = 0  # 正在清理/更换（不可借出）
        self._waiting = 0   # 正在 acquire() 中排队的请求数
        self._opened = 0    # 当前打开的连接数（<= size）
        self._closed = False
        self._bg: set[asyncio.Task] = set()
        self.counters: dict[str, int] = {
            "acquired": 0,           # 成功借出次数
            "acquire_timeouts": 0,   # 等连接超时次数
            "queue_cancelled": 0,    # 排队中客户端取消次数
            "released_clean": 0,     # 干净归还次数
            "recycled": 0,           # 脏连接清理后复用次数
            "replaced": 0,           # 脏连接废弃并更换次数
            "server_cancels": 0,     # 发送的服务端取消次数
            "query_timeouts": 0,     # 查询执行超时次数
            "client_disconnects": 0,  # 查询期间客户端断开次数
            "sql_errors": 0,         # SQL 报错次数
        }

    # ------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        for _ in range(self.cfg.size):
            conn = await self._connect_with_retry(self.cfg.startup_retries)
            self._idle.append((conn, time.monotonic()))
        logger.info("pool started with %d connections", len(self._idle))

    async def close(self) -> None:
        self._closed = True
        for task in list(self._bg):
            task.cancel()
        async with self._cond:
            idle, self._idle = self._idle, []
            self._cond.notify_all()
        for conn, _ in idle:
            await self._close_quietly(conn)

    # -------------------------------------------------------- 借出 / 归还

    async def acquire(self, timeout: float | None) -> psycopg.AsyncConnection:
        """从池中取一条连接。

        在 `timeout` 秒内没有可用连接则抛出 AcquireTimeoutError
        （None 表示无限等待）。排队中的协程被取消时，其队列名额立即
        释放——排队请求从不持有连接名额。

        借出前如发现连接已死，或空闲时间过长且 SELECT 1 验证失败，
        则将其转入清理/更换并继续等下一条，不会把坏连接交给调用方。
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            conn, idle_since = await self._take_idle(deadline)
            if await self._verify_checkout(conn, idle_since):
                return conn
            # 验证失败：连接已按脏连接送清理/更换，在剩余预算内重试
            if deadline is not None and time.monotonic() >= deadline:
                self.counters["acquire_timeouts"] += 1
                raise AcquireTimeoutError(
                    f"no connection available within {timeout}s")

    async def _take_idle(
        self, deadline: float | None
    ) -> tuple[psycopg.AsyncConnection, float]:
        async with self._cond:
            self._waiting += 1
            try:
                while True:
                    if self._closed:
                        raise PoolClosedError("pool is closed")
                    if self._idle:
                        conn, idle_since = self._idle.pop()
                        self._in_use += 1
                        self.counters["acquired"] += 1
                        return conn, idle_since
                    if deadline is None:
                        await self._cond.wait()
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.counters["acquire_timeouts"] += 1
                        raise AcquireTimeoutError("no connection available")
                    try:
                        await asyncio.wait_for(self._cond.wait(), remaining)
                    except asyncio.TimeoutError:
                        self.counters["acquire_timeouts"] += 1
                        raise AcquireTimeoutError(
                            "no connection available") from None
            finally:
                self._waiting -= 1

    async def _verify_checkout(self, conn, idle_since: float) -> bool:
        """借出前的最后把关。本地状态免费检查总是做；空闲太久的连接
        额外做一次 SELECT 1 活性验证。"""
        if not self._looks_clean(conn):
            await self.release(conn, dirty=True)
            return False
        after = self.cfg.checkout_verify_after
        if after is None or time.monotonic() - idle_since < after:
            return True
        try:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute("SELECT 1"), self.cfg.checkout_verify_timeout)
        except Exception:
            logger.info("checkout verify failed; recycling connection")
            await self.release(conn, dirty=True)
            return False
        if self._looks_clean(conn):
            return True
        await self.release(conn, dirty=True)
        return False

    async def release(
        self,
        conn: psycopg.AsyncConnection,
        *,
        dirty: bool,
        pending: asyncio.Task | None = None,
    ) -> None:
        """归还 acquire() 借出的连接。

        干净的连接直接回到空闲队列；脏连接（查询超时 / 客户端断开 /
        SQL 报错）转入后台清理，只有在会话状态被验证干净后才可再次
        借出，无法验证的关闭并更换。本函数不会在清理上阻塞调用方。
        """
        if pending is not None:
            pending.add_done_callback(_consume_task_exception)
        if not dirty and self._looks_clean(conn):
            async with self._cond:
                self._in_use -= 1
                self._idle.append((conn, time.monotonic()))
                self.counters["released_clean"] += 1
                self._cond.notify(1)
            return
        async with self._cond:
            self._in_use -= 1
            self._cleaning += 1
        self.spawn(self._recycle(conn, pending))

    # ------------------------------------------------------------ 内部实现

    def _looks_clean(self, conn: psycopg.AsyncConnection) -> bool:
        try:
            return (
                not conn.closed
                and not getattr(conn, "broken", False)
                and conn.info.transaction_status == TransactionStatus.IDLE
            )
        except Exception:
            return False

    def spawn(self, coro) -> asyncio.Task:
        """在池的后台任务集中运行一个协程（供清理与演示任务使用）。"""
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        task.add_done_callback(_consume_task_exception)
        return task

    async def _recycle(self, conn, pending) -> None:
        try:
            ok = await self._sanitize(conn, pending)
        except Exception:
            logger.exception("sanitize crashed")
            ok = False
        if ok:
            reusable = conn
            self.counters["recycled"] += 1
            logger.info("dirty connection cleaned and reused")
        else:
            await self._close_quietly(conn)
            self.counters["replaced"] += 1
            if self._closed:
                reusable = None
            else:
                logger.info("connection discarded; opening a replacement")
                reusable = await self._connect_with_retry(0)  # 0 = 一直重试
        async with self._cond:
            self._cleaning -= 1
            if reusable is not None:
                self._idle.append((reusable, time.monotonic()))
            self._cond.notify(1)

    async def _sanitize(self, conn, pending) -> bool:
        """确认连接的会话状态已清理才返回 True。"""
        if conn.closed or getattr(conn, "broken", False):
            return False
        try:
            return await asyncio.wait_for(
                self._sanitize_inner(conn, pending), self.cfg.cleanup_timeout
            )
        except Exception:
            logger.warning("sanitize failed", exc_info=True)
            return False

    async def _sanitize_inner(self, conn, pending) -> bool:
        # 1) 查询可能仍在服务端执行：发送服务端取消，并等待客户端侧的
        #    查询任务观察到取消结果。
        still_running = pending is not None and not pending.done()
        if still_running or conn.info.transaction_status == TransactionStatus.ACTIVE:
            await self._server_cancel(conn)
        if still_running:
            try:
                await asyncio.wait_for(asyncio.shield(pending), self.cfg.drain_grace)
            except Exception:
                pass
            if not pending.done():
                return False  # 查询不肯结束 -> 无法信任该会话
        # 2) 回滚任何遗留 / 失败的事务。
        status = conn.info.transaction_status
        if status in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
            await conn.rollback()
        # 3) 验证会话可用且处于空闲。
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            row = await cur.fetchone()
        return (
            row is not None
            and tuple(row) == (1,)
            and conn.info.transaction_status == TransactionStatus.IDLE
        )

    async def _server_cancel(self, conn) -> None:
        try:
            result = conn.cancel()
            if inspect.isawaitable(result):  # 兼容不同 psycopg 版本
                await result
            self.counters["server_cancels"] += 1
        except Exception:
            logger.warning("server-side cancel failed", exc_info=True)

    async def _close_quietly(self, conn) -> None:
        try:
            await asyncio.wait_for(conn.close(), self.cfg.close_timeout)
        except Exception:
            logger.warning("error while closing connection", exc_info=True)
        finally:
            self._opened -= 1

    async def _connect(self) -> psycopg.AsyncConnection:
        conn = await psycopg.AsyncConnection.connect(
            **self.cfg.dsn,
            autocommit=True,
            application_name="ro-api",
            connect_timeout=int(self.cfg.connect_timeout),
            # 会话级只读：本 API 是只读查询服务
            options="-c default_transaction_read_only=on",
        )
        self._opened += 1
        return conn

    async def _connect_with_retry(self, retries: int) -> psycopg.AsyncConnection:
        """retries <= 0 表示一直重试。"""
        attempt = 0
        while True:
            try:
                return await self._connect()
            except Exception:
                attempt += 1
                if 0 < retries <= attempt:
                    raise
                logger.warning("connect failed (attempt %d), retrying", attempt)
                await asyncio.sleep(self.cfg.retry_interval)

    # ------------------------------------------------------------ 指标

    async def stats(self) -> dict:
        async with self._cond:
            return {
                "size": self.cfg.size,
                "idle": len(self._idle),
                "in_use": self._in_use,
                "cleaning": self._cleaning,
                "waiting": self._waiting,
                "opened_connections": self._opened,
                "counters": dict(self.counters),
            }
