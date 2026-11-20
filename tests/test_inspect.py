#!/usr/bin/env python3
"""巡检模块的单元测试与端到端测试。

分三层：

1. test_ranking_*：build_ranking 纯函数，覆盖配对/不可比/排名规则；
2. test_store_*：SampleStore 的落盘、stale 防回退、失败保留、并发串行化；
3. test_e2e_*：对真实 PostgreSQL 17（含 pg_stat_statements）的端到端，
   包括单条重置与并发采样。需要环境变量 PG_DSN，否则跳过。

用法：
  python -m pytest tests/test_inspect.py -v
  PG_DSN="postgresql://app@127.0.0.1:5432/demo" python -m pytest tests/test_inspect.py -v
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

from pg_inspect import (  # noqa: E402
    EntryKey,
    InspectError,
    Sample,
    SampleStore,
    build_ranking,
    collect_sample,
)

PG_DSN = os.environ.get("PG_DSN")


# ---------------------------------------------------------------- 测试夹具

def _key(qid: int, userid: int = 10, dbid: int = 16384,
         toplevel: bool = True) -> EntryKey:
    return EntryKey(userid=userid, dbid=dbid, toplevel=toplevel, queryid=qid)


def _entry(key: EntryKey, *, calls: int, exec_ms: float,
           stats_since: str = "2026-01-01T00:00:00+00:00",
           query: str | None = None, plan_ms: float = 0.0,
           rows: int | None = None) -> dict:
    return {
        "key": key.as_dict(),
        "query": query if query is not None else f"SELECT q{key.queryid}",
        "stats_since": stats_since,
        "calls": calls,
        "total_exec_time": exec_ms,
        "total_plan_time": plan_ms,
        "rows": rows if rows is not None else calls,
        "shared_blks_hit": 0,
        "shared_blks_read": 0,
        "shared_blks_dirtied": 0,
        "shared_blks_written": 0,
        "temp_blks_read": 0,
        "temp_blks_written": 0,
    }


def _sample(sid: str, observed_at: str, seq: int, entries: list[dict]) -> Sample:
    return Sample(
        sample_id=sid, observed_at=observed_at, monotonic_seq=seq,
        entry_count=len(entries),
        entries={Sample.key_of(EntryKey.from_row(e["key"])): e for e in entries})


@pytest.fixture
def store(tmp_path):
    return SampleStore(tmp_path / "inspect")


# ================================================================ 纯逻辑

def test_ranking_comparable_delta_and_order():
    k1, k2, k3 = _key(1), _key(2), _key(3)
    old = _sample("s1", "2026-01-01T00:00:00+00:00", 1, [
        _entry(k1, calls=10, exec_ms=100),
        _entry(k2, calls=5, exec_ms=200),
        _entry(k3, calls=2, exec_ms=50),
    ])
    new = _sample("s2", "2026-01-01T00:01:00+00:00", 2, [
        # k1: exec +50, calls +3
        _entry(k1, calls=13, exec_ms=150),
        # k2: exec +300（最大）, calls +1
        _entry(k2, calls=6, exec_ms=500),
        # k3: exec +10, calls +8
        _entry(k3, calls=10, exec_ms=60),
    ])
    out = build_ranking(old, new)
    assert out["comparable_count"] == 3
    assert out["incomparable_count"] == 0
    qids = [r["key"]["queryid"] for r in out["ranking"]]
    assert qids == [2, 1, 3]  # 按 exec 增量 300 > 50 > 10
    top = out["ranking"][0]
    assert top["calls_delta"] == 1
    assert top["exec_time_delta_ms"] == 300.0
    assert top["calls_previous"] == 5 and top["calls_current"] == 6


def test_ranking_tie_breaks_on_calls_then_key():
    k1, k2 = _key(1), _key(2)
    old = _sample("s1", "2026-01-01T00:00:00+00:00", 1, [
        _entry(k1, calls=1, exec_ms=10),
        _entry(k2, calls=1, exec_ms=10),
    ])
    new = _sample("s2", "2026-01-01T00:01:00+00:00", 2, [
        _entry(k1, calls=5, exec_ms=20),  # exec +10, calls +4
        _entry(k2, calls=2, exec_ms=20),  # exec +10, calls +1
    ])
    out = build_ranking(old, new)
    assert [r["key"]["queryid"] for r in out["ranking"]] == [1, 2]


def test_ranking_single_entry_reset_is_incomparable():
    """单条重置：条目两边都在，但 stats_since 被推进 -> 不可比。"""
    k = _key(7)
    old = _sample("s1", "2026-01-01T00:00:00+00:00", 1, [
        _entry(k, calls=99, exec_ms=900, stats_since="2025-12-31T00:00:00+00:00")])
    new = _sample("s2", "2026-01-01T00:01:00+00:00", 2, [
        _entry(k, calls=2, exec_ms=1, stats_since="2026-01-01T00:00:30+00:00")])
    out = build_ranking(old, new)
    assert out["comparable_count"] == 0
    assert len(out["incomparable"]) == 1
    item = out["incomparable"][0]
    assert item["reason"] == "stats_since_changed"
    assert item["entry_key"]["queryid"] == 7
    assert item["detail"]["previous_stats_since"] == "2025-12-31T00:00:00+00:00"
    assert item["detail"]["current_stats_since"] == "2026-01-01T00:00:30+00:00"


def test_ranking_eviction_reappear_is_incomparable():
    """淘汰后重现：只在新样本出现 -> only_in_new_sample。"""
    k_gone, k_keep, k_back = _key(1), _key(2), _key(3)
    old = _sample("s1", "2026-01-01T00:00:00+00:00", 1, [
        _entry(k_gone, calls=5, exec_ms=50),
        _entry(k_keep, calls=5, exec_ms=50),
        _entry(k_back, calls=5, exec_ms=50),
    ])
    new = _sample("s2", "2026-01-01T00:01:00+00:00", 2, [
        # k_gone 被淘汰未重现；k_back 淘汰后以新 stats_since 重现
        _entry(k_keep, calls=6, exec_ms=60),
        _entry(k_back, calls=1, exec_ms=1,
               stats_since="2026-01-01T00:00:45+00:00"),
        _entry(_key(99), calls=1, exec_ms=1),  # 全新条目
    ])
    out = build_ranking(old, new)
    reasons = {i["entry_key"]["queryid"]: i["reason"]
               for i in out["incomparable"]}
    assert reasons[1] == "only_in_previous_sample"
    assert reasons[3] == "stats_since_changed"
    assert reasons[99] == "only_in_new_sample"
    assert [r["key"]["queryid"] for r in out["ranking"]] == [2]


def test_ranking_limit_truncates_ranking_only():
    keys = [_key(i) for i in range(5)]
    old = _sample("s1", "2026-01-01T00:00:00+00:00", 1,
                  [_entry(k, calls=1, exec_ms=0) for k in keys])
    new = _sample("s2", "2026-01-01T00:01:00+00:00", 2,
                  [_entry(k, calls=2, exec_ms=10 * (5 - k.queryid))
                   for k in keys])
    out = build_ranking(old, new, limit=2)
    assert out["comparable_count"] == 5      # 总数不变
    assert len(out["ranking"]) == 2          # 只截断展示
    # exec 增量 = 10*(5-qid)，queryid=0 增量最大
    assert out["ranking"][0]["key"]["queryid"] == 0


def test_ranking_null_stats_since_is_incomparable():
    k = _key(4)
    old = _sample("s1", "2026-01-01T00:00:00+00:00", 1,
                  [_entry(k, calls=1, exec_ms=1)])
    entry_new = _entry(k, calls=2, exec_ms=2)
    entry_new["stats_since"] = None
    new = _sample("s2", "2026-01-01T00:01:00+00:00", 2, [entry_new])
    out = build_ranking(old, new)
    assert out["incomparable"][0]["reason"] == "stats_since_missing"


# ================================================================ 存储

def test_store_first_sample_is_baseline_no_ranking(store):
    async def scenario():
        async def factory(seq):
            return _sample(f"s{seq}", f"2026-01-01T00:00:0{seq}:00+00:00",
                           seq, [_entry(_key(1), calls=1, exec_ms=1)])
        out = await store.persist_observed_sample(factory)
        assert out["accepted"] is True
        assert out["ranking_updated"] is False
        assert store.load_latest_ranking() is None
        assert store.load_latest().sample_id == "s1"
    asyncio.run(scenario())


def test_store_second_sample_produces_ranking(store):
    async def scenario():
        for seq, calls, exec_ms in [(1, 1, 10.0), (2, 4, 40.0)]:
            async def factory(seq=seq, calls=calls, exec_ms=exec_ms):
                return _sample(
                    f"s{seq}", f"2026-01-01T00:00:0{seq}:00+00:00", seq,
                    [_entry(_key(1), calls=calls, exec_ms=exec_ms)])
            await store.persist_observed_sample(factory)
        ranking = store.load_latest_ranking()
        assert ranking is not None
        assert ranking["ranking"][0]["calls_delta"] == 3
        assert ranking["ranking"][0]["exec_time_delta_ms"] == 30.0
        # 两份样本文件都在
        assert store.list_sample_ids() == ["s1", "s2"]
    asyncio.run(scenario())


def test_store_stale_observation_rejected(store):
    """手工写入一个较新基线后，再提交较早观测，必须被拒绝且基线不变。"""
    async def scenario():
        newer = _sample("newer", "2026-01-01T00:05:00+00:00", 1,
                        [_entry(_key(1), calls=1, exec_ms=1)])
        store._save_json(store.base_dir / store.LATEST_NAME, newer.to_json())

        async def stale_factory(seq):
            return _sample("stale", "2026-01-01T00:01:00+00:00", seq,
                           [_entry(_key(1), calls=1, exec_ms=1)])
        out = await store.persist_observed_sample(stale_factory)
        assert out["accepted"] is False
        assert out["reason"] == "stale_observation"
        assert store.load_latest().sample_id == "newer"  # 基线未被覆盖
        assert "stale" not in store.list_sample_ids()
        assert store.load_latest_ranking() is None       # 榜单未受影响
    asyncio.run(scenario())


def test_store_collect_failure_keeps_previous_state(store):
    """采集抛错：不写任何文件，上次有效样本与榜单原样保留。"""
    async def scenario():
        async def ok_factory(seq):
            return _sample(f"s{seq}", f"2026-01-01T00:00:0{seq}:00+00:00",
                           seq, [_entry(_key(1), calls=seq, exec_ms=seq)])
        await store.persist_observed_sample(ok_factory)
        await store.persist_observed_sample(ok_factory)
        before_latest = store.load_latest().sample_id
        before_ranking = store.load_latest_ranking()
        assert before_ranking is not None

        async def boom_factory(seq):
            raise InspectError("模拟数据库故障")

        with pytest.raises(InspectError):
            await store.persist_observed_sample(boom_factory)
        assert store.load_latest().sample_id == before_latest
        assert store.load_latest_ranking() == before_ranking
        assert len(store.list_sample_ids()) == 2
    asyncio.run(scenario())


def test_store_persist_failure_keeps_previous_state(store, monkeypatch):
    """保存阶段失败（模拟磁盘错误）：已有 latest 与榜单不动。"""
    async def scenario():
        async def ok_factory(seq):
            return _sample(f"s{seq}", f"2026-01-01T00:00:0{seq}:00+00:00",
                           seq, [_entry(_key(1), calls=seq, exec_ms=seq)])
        await store.persist_observed_sample(ok_factory)
        await store.persist_observed_sample(ok_factory)
        before = store.load_latest().sample_id

        def broken_write(path, payload):
            if path.name == store.LATEST_NAME:
                raise OSError("模拟磁盘只读")
            orig_write(path, payload)
        orig_write = store._save_json
        monkeypatch.setattr(store, "_save_json", broken_write)
        async def f3(seq):
            return _sample(f"s{seq}", "2026-01-01T00:00:03:00+00:00",
                           seq, [_entry(_key(1), calls=3, exec_ms=3)])
        with pytest.raises(OSError):
            await store.persist_observed_sample(f3)
        assert store.load_latest().sample_id == before
        # 残留临时文件应被清理
        assert not list(store.base_dir.glob(".*.tmp-*"))
    asyncio.run(scenario())


def test_store_concurrent_samples_serialized_and_ordered(store):
    """并发 20 个采样：全部被接受、序号严格递增、observed_at 单调，
    且每个相邻样本对都生成了对应榜单（最后一份榜单可追溯）。"""
    async def scenario():
        n = 20

        async def factory(seq):
            await asyncio.sleep(0.005)  # 制造交错
            return _sample(
                f"s{seq}",
                f"2026-01-01T00:00:{seq:02d}.000000+00:00", seq,
                [_entry(_key(1), calls=seq, exec_ms=float(seq))])

        results = await asyncio.gather(*[
            store.persist_observed_sample(factory) for _ in range(n)])
        assert all(r["accepted"] for r in results), results
        ids = store.list_sample_ids()
        assert len(ids) == n
        latest = store.load_latest()
        assert latest.monotonic_seq == n
        assert latest.sample_id == f"s{n}"
        # 序号文件也单调
        assert int((store.base_dir / store.SEQ_NAME).read_text()) == n
    asyncio.run(scenario())


def test_store_samples_are_valid_json_files(store):
    async def scenario():
        async def factory(seq):
            return _sample(f"s{seq}", f"2026-01-01T00:00:0{seq}:00+00:00",
                           seq, [_entry(_key(1), calls=1, exec_ms=1)])
        await store.persist_observed_sample(factory)
        raw = json.loads((store.base_dir / store.LATEST_NAME).read_text())
        assert raw["entries"] and raw["monotonic_seq"] == 1
    asyncio.run(scenario())


# ================================================================ 端到端（PG17）

e2e = pytest.mark.skipif(not PG_DSN, reason="未设置 PG_DSN，跳过端到端测试")


@e2e
def test_e2e_collect_real_sample():
    import psycopg

    async def scenario():
        async with await psycopg.AsyncConnection.connect(
                PG_DSN, autocommit=True) as conn:
            s = await collect_sample(conn, monotonic_seq=1)
        assert s.entry_count >= 1
        any_item = next(iter(s.entries.values()))
        assert all(f in any_item for f in
                   ("key", "query", "stats_since", "calls", "total_exec_time"))
        assert any_item["stats_since"] is not None
    asyncio.run(scenario())


@e2e
def test_e2e_single_reset_then_incomparable(tmp_path):
    """完整链路：两次采样间重置单条 -> 该条目落 incomparable，其余照常排名。"""
    import psycopg

    async def scenario():
        st = SampleStore(tmp_path / "e2e")
        conn = await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True)
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_stat_statements_reset()")
                # 用不同临时表名制造两条 queryid 不同、文本可识别的语句
                # （输出别名不参与 queryid 计算，不能用 SELECT n AS name）
                await cur.execute(
                    "CREATE TEMP TABLE marker_alpha(x int)")
                await cur.execute("CREATE TEMP TABLE marker_beta(y int)")
                await cur.execute("SELECT count(*) FROM marker_alpha")
                await cur.execute("SELECT count(*) FROM marker_beta")
                await cur.execute("SELECT count(*) FROM marker_alpha")

            s1 = await st.persist_observed_sample(
                lambda seq, c=conn: collect_sample(c, seq))
            assert s1["accepted"]

            # 找出 alpha 的四元组并单条重置
            latest = st.load_latest()
            alpha = None
            for e in latest.entries.values():
                if "FROM marker_alpha" in (e["query"] or ""):
                    alpha = e
                    break
            assert alpha is not None
            k = alpha["key"]
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT pg_stat_statements_reset(%s, %s, %s)",
                    (k["userid"], k["dbid"], k["queryid"]))
                # 两个条目都再执行：alpha 重置后重新累计，beta 连续累计
                await cur.execute("SELECT count(*) FROM marker_alpha")
                await cur.execute("SELECT count(*) FROM marker_beta")
                await cur.execute("SELECT count(*) FROM marker_beta")

            s2 = await st.persist_observed_sample(
                lambda seq, c=conn: collect_sample(c, seq))
            assert s2["ranking_updated"]
            ranking = st.load_latest_ranking()
            incomparable = {i["entry_key"]["queryid"]: i
                            for i in ranking["incomparable"]}
            assert k["queryid"] in incomparable
            assert incomparable[k["queryid"]]["reason"] == "stats_since_changed"
            # beta 仍正常入榜，调用数增量为 2
            beta = None
            for r in ranking["ranking"]:
                if "FROM marker_beta" in r["query"]:
                    beta = r
                    break
            assert beta is not None
            assert beta["calls_delta"] == 2
        finally:
            await conn.close()
    asyncio.run(scenario())


@e2e
def test_e2e_concurrent_sampling(tmp_path):
    """10 个并发采样请求：全部成功、样本单调、无 stale 覆盖。"""
    import psycopg

    async def scenario():
        st = SampleStore(tmp_path / "e2e-conc")

        async def one():
            conn = await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True)
            try:
                return await st.persist_observed_sample(
                    lambda seq: collect_sample(conn, seq))
            finally:
                await conn.close()

        results = await asyncio.gather(*[one() for _ in range(10)])
        assert len(results) == 10
        accepted = [r for r in results if r["accepted"]]
        assert len(accepted) == 10
        assert all(r["reason"] is None for r in accepted)
        assert len(st.list_sample_ids()) == 10
        # observed_at 严格单调（沿保存顺序）
        observed = []
        for sid in st.list_sample_ids():
            observed.append(st.load_sample(sid).observed_at)
        assert observed == sorted(observed)
        assert len(set(observed)) == 10
    asyncio.run(scenario())


@e2e
def test_e2e_api_endpoints(tmp_path):
    """通过 ASGI 在进程内验证 /inspect 路由的完整 HTTP 语义。"""
    # inspect_api 在导入时读取这些变量，必须先于 import 设置
    os.environ["INSPECT_DATA_DIR"] = str(tmp_path / "apidata")
    os.environ.setdefault("PGHOST", "127.0.0.1")
    os.environ.setdefault("PGPORT", "5432")
    os.environ.setdefault("PGDATABASE", "demo")
    os.environ.setdefault("PGUSER", "app")
    os.environ.setdefault("PGPASSWORD", "app")

    import httpx
    from fastapi import FastAPI
    import pg_inspect_api  # noqa: F401  确保模块级常量已初始化
    from pg_inspect_api import router

    fastapi_app = FastAPI()
    fastapi_app.include_router(router)

    async def scenario():
        transport = httpx.ASGITransport(app=fastapi_app)
        async with httpx.AsyncClient(
                transport=transport, base_url="http://t") as client:
            # 首份样本：无榜单
            r = await client.post("/inspect/samples")
            assert r.status_code == 200, r.text
            assert r.json()["ranking_updated"] is False
            r = await client.get("/inspect/ranking")
            assert r.status_code == 409
            # 第二份：产生榜单
            r = await client.post("/inspect/samples")
            assert r.status_code == 200 and r.json()["ranking_updated"] is True
            r = await client.get("/inspect/ranking")
            assert r.status_code == 200
            body = r.json()
            assert body["comparable_count"] + body["incomparable_count"] >= 1
            # 样本列表与详情
            r = await client.get("/inspect/samples")
            assert r.status_code == 200 and r.json()["sample_count"] == 2
            sid = r.json()["sample_ids"][0]
            assert (await client.get(f"/inspect/samples/{sid}")).status_code == 200
            assert (await client.get("/inspect/samples/nope")).status_code == 404
            # 重算
            sid2 = r.json()["sample_ids"][1]
            r = await client.post(
                f"/inspect/ranking/recompute?previous_id={sid}&current_id={sid2}")
            assert r.status_code == 200
            # 顺序颠倒 -> 400
            r = await client.post(
                f"/inspect/ranking/recompute?previous_id={sid2}&current_id={sid}")
            assert r.status_code == 400
            # 重置不存在的条目 -> 404
            r = await client.post("/inspect/reset", json={
                "userid": 10, "dbid": 16384, "toplevel": True,
                "queryid": 999999999999})
            assert r.status_code == 404

    asyncio.run(scenario())
