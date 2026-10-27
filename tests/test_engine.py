"""Engine tests against a real Postgres (docker compose up -d)."""

import asyncio
import contextlib
import time
import uuid

import pytest
from psycopg import sql

from pgreplay_flow import (
    NO_RETRY,
    NonDeterminismError,
    RetryPolicy,
    RunNotFound,
    StepFailed,
    WorkflowCancelled,
    WorkflowFailed,
    step,
    step_info,
)


async def test_basic_workflow_completes(flow, start_worker):
    calls = []

    async def add(a, b):
        calls.append((a, b))
        return a + b

    @flow.workflow(name="basic")
    async def basic(ctx, x):
        y = await ctx.step(add, x, 1)
        return await ctx.step(add, y, 10)

    start_worker()
    assert await flow.run(basic, 5, timeout=10) == 16
    assert calls == [(5, 1), (6, 10)]


async def test_sync_steps_and_json_normalization(flow, start_worker):
    def pair():  # sync step runs in a thread; tuple normalizes to list
        return (1, 2)

    @flow.workflow(name="norm")
    async def norm(ctx):
        v = await ctx.step(pair)
        return {"is_list": isinstance(v, list), "v": v}

    start_worker()
    assert await flow.run(norm, timeout=10) == {"is_list": True, "v": [1, 2]}


async def test_retry_then_success_with_stable_idempotency_key(flow, start_worker):
    seen = []

    @step(retry=RetryPolicy(max_attempts=3, initial_delay=0.05, jitter=0))
    async def flaky():
        info = step_info()
        seen.append((info.attempt, info.idempotency_key))
        if info.attempt < 3:
            raise RuntimeError("boom")
        return "ok"

    @flow.workflow(name="retry")
    async def wf(ctx):
        return await ctx.step(flaky)

    start_worker()
    assert await flow.run(wf, timeout=10) == "ok"
    assert [a for a, _ in seen] == [1, 2, 3]
    assert len({k for _, k in seen}) == 1


async def test_step_failure_is_catchable_and_run_fails_otherwise(flow, start_worker):
    async def bad():
        raise ValueError("nope")

    @flow.workflow(name="catch", retry=NO_RETRY)
    async def catch(ctx):
        try:
            await ctx.step(bad)
        except StepFailed as e:
            return e.error_type

    @flow.workflow(name="fail", retry=NO_RETRY)
    async def fail(ctx):
        await ctx.step(bad)

    start_worker()
    assert await flow.run(catch, timeout=10) == "ValueError"
    with pytest.raises(WorkflowFailed, match="nope"):
        await flow.run(fail, timeout=10)


async def test_durable_backoff_suspends_run(flow, start_worker):
    """Backoff longer than inline_wait_max releases the worker; another claim
    resumes the retry without re-running completed steps."""
    calls = {"before": 0, "flaky": 0}

    async def before():
        calls["before"] += 1

    @step(retry=RetryPolicy(max_attempts=2, initial_delay=1.5, jitter=0))
    async def flaky():
        calls["flaky"] += 1
        if calls["flaky"] == 1:
            raise RuntimeError("transient")
        return "done"

    @flow.workflow(name="backoff")
    async def wf(ctx):
        await ctx.step(before)
        return await ctx.step(flaky)

    start_worker(inline_wait_max=0.2)
    run_id = await flow.start(wf)
    assert await flow.result(run_id, timeout=15) == "done"
    assert calls == {"before": 1, "flaky": 2}
    assert (await flow.get_run(run_id))["claims"] == 2


async def test_durable_sleep_releases_worker(flow, start_worker):
    @flow.workflow(name="sleeper")
    async def wf(ctx):
        t0 = await ctx.now()
        await ctx.sleep(1.5)
        t1 = await ctx.now()
        return (t1 - t0).total_seconds()

    start_worker(inline_wait_max=0.1)
    run_id = await flow.start(wf)
    await asyncio.sleep(0.7)
    assert (await flow.get_run(run_id))["status"] == "sleeping"
    elapsed = await flow.result(run_id, timeout=10)
    assert elapsed >= 1.4


async def test_gather_wakes_at_earliest_deadline_and_keys_are_deterministic(flow, start_worker):
    order = []

    async def mark(tag):
        order.append(tag)
        return tag

    @flow.workflow(name="fan")
    async def wf(ctx):
        async def branch(tag, delay):
            await ctx.sleep(delay)
            return await ctx.step(mark, tag)

        return await asyncio.gather(branch("slow", 2.5), branch("fast", 1.2))

    start_worker(inline_wait_max=0.1)
    run_id = await flow.start(wf)
    t0 = time.monotonic()
    assert await flow.result(run_id, timeout=15) == ["slow", "fast"]
    assert time.monotonic() - t0 >= 2.4
    assert order == ["fast", "slow"]  # each step ran once, at its own wake-up
    keys = sorted(e["key"] for e in await flow.get_journal(run_id))
    assert keys == ["t1/1", "t1/2", "t2/1", "t2/2"]
    report = await flow.replay(run_id)
    assert report.outcome == "completed" and report.matches_recorded


async def test_signals_buffered_and_timeout(flow, start_worker):
    @flow.workflow(name="approval")
    async def wf(ctx):
        a = await ctx.wait_for_signal("approve")
        b = await ctx.wait_for_signal("second", timeout=1.5)
        return {"a": a, "b": b}

    start_worker(inline_wait_max=0.1)
    early = await flow.start(wf)
    await flow.send_signal(early, "approve", {"by": "alice"})  # before it's waited on
    assert await flow.result(early, timeout=10) == {"a": {"by": "alice"}, "b": None}

    late = await flow.start(wf)
    await asyncio.sleep(0.8)
    assert (await flow.get_run(late))["status"] == "waiting"
    await flow.send_signal(late, "approve", 1)
    await flow.send_signal(late, "second", 2)
    assert await flow.result(late, timeout=10) == {"a": 1, "b": 2}


async def test_idempotent_start(flow, start_worker):
    @flow.workflow(name="idem")
    async def wf(ctx, x):
        return x

    a = await flow.start(wf, 1, idempotency_key="order-42")
    b = await flow.start(wf, 2, idempotency_key="order-42")
    assert a == b
    start_worker()
    assert await flow.result(a, timeout=10) == 1


async def test_transactional_enqueue_rolls_back(flow):
    @flow.workflow(name="txenq")
    async def wf(ctx):
        return 1

    run_id: uuid.UUID | None = None
    async with flow.pool.connection() as conn:
        with pytest.raises(RuntimeError):
            async with conn.transaction():
                run_id = await flow.start(wf, conn=conn)
                raise RuntimeError("rollback")
    assert run_id is not None
    with pytest.raises(RunNotFound):
        await flow.get_run(run_id)


async def test_tx_step_is_exactly_once(flow, start_worker, schema):
    async def setup(conn):
        await conn.execute(sql.SQL("CREATE TABLE {}.ledger (k text PRIMARY KEY, n int)").format(sql.Identifier(schema)))

    async def credit(conn, k, n):
        await conn.execute(sql.SQL("INSERT INTO {}.ledger VALUES (%s, %s)").format(sql.Identifier(schema)), (k, n))
        return n

    @flow.workflow(name="ledger")
    async def wf(ctx):
        await ctx.tx_step(setup)
        return await ctx.tx_step(credit, "acct", 100)

    start_worker()
    run_id = await flow.start(wf)
    assert await flow.result(run_id, timeout=10) == 100
    # A forked run reuses the journal: neither tx_step runs again (the INSERT
    # would violate the primary key if it did).
    fork_id = await flow.fork(run_id)
    assert await flow.result(fork_id, timeout=10) == 100
    async with flow.pool.connection() as conn:
        cur = await conn.execute(sql.SQL("SELECT count(*) FROM {}.ledger").format(sql.Identifier(schema)))
        assert (await cur.fetchone())[0] == 1


async def test_tx_step_rolls_back_on_error(flow, start_worker, schema):
    async def setup(conn):
        await conn.execute(sql.SQL("CREATE TABLE {}.t (v int)").format(sql.Identifier(schema)))

    async def insert_then_fail(conn):
        await conn.execute(sql.SQL("INSERT INTO {}.t VALUES (1)").format(sql.Identifier(schema)))
        raise RuntimeError("fail after write")

    @flow.workflow(name="txfail", retry=NO_RETRY)
    async def wf(ctx):
        await ctx.tx_step(setup)
        with contextlib.suppress(StepFailed):
            await ctx.tx_step(insert_then_fail)

    start_worker()
    await flow.run(wf, timeout=10)
    async with flow.pool.connection() as conn:
        cur = await conn.execute(sql.SQL("SELECT count(*) FROM {}.t").format(sql.Identifier(schema)))
        assert (await cur.fetchone())[0] == 0


async def test_cancel_stops_running_workflow(flow, start_worker):
    @flow.workflow(name="long")
    async def wf(ctx):
        await ctx.sleep(60)

    start_worker(inline_wait_max=0.1)
    run_id = await flow.start(wf)
    await asyncio.sleep(0.5)
    assert await flow.cancel(run_id)
    with pytest.raises(WorkflowCancelled):
        await flow.result(run_id, timeout=5)
    assert not await flow.cancel(run_id)


async def test_replay_detects_nondeterminism(flow, start_worker):
    async def a():
        return "a"

    async def b():
        return "b"

    @flow.workflow(name="evolve")
    async def v1(ctx):
        return await ctx.step(a)

    start_worker()
    run_id = await flow.start(v1)
    assert await flow.result(run_id, timeout=10) == "a"

    ok = await flow.replay(run_id)
    assert ok.outcome == "completed" and ok.matches_recorded and ok.ops_replayed == 1

    # Simulate deploying incompatible code under the same workflow name.
    async def v2(ctx):
        return await ctx.step(b)

    flow.workflows["evolve"] = flow.workflows["evolve"].__class__("evolve", v2, "default",
                                                                   flow.default_retry)
    bad = await flow.replay(run_id)
    assert bad.outcome == "nondeterministic"
    assert NonDeterminismError.__name__ in bad.error["type"]


async def test_replay_incomplete_run(flow):
    def one():
        return 1

    @flow.workflow(name="pending")
    async def wf(ctx):
        return await ctx.step(one)

    run_id = await flow.start(wf)  # no worker: nothing journaled yet
    report = await flow.replay(run_id)
    assert report.outcome == "incomplete" and report.stopped_at == "1"


async def test_fork_resumes_after_fix(flow, start_worker):
    counts = {"expensive": 0}
    fixed = {"v": False}

    async def expensive():
        counts["expensive"] += 1
        return 41

    async def buggy(x):
        if not fixed["v"]:
            raise ValueError("bug")
        return x + 1

    @flow.workflow(name="forkme", retry=NO_RETRY)
    async def wf(ctx):
        x = await ctx.step(expensive)
        return await ctx.step(buggy, x)

    start_worker()
    run_id = await flow.start(wf)
    with pytest.raises(WorkflowFailed):
        await flow.result(run_id, timeout=10)

    fixed["v"] = True
    fork_id = await flow.fork(run_id, keep=1)  # keep `expensive`, redo `buggy`
    assert await flow.result(fork_id, timeout=10) == 42
    assert counts["expensive"] == 1
    assert (await flow.get_run(fork_id))["forked_from"] == run_id


async def test_many_workers_each_run_executes_once(flow, start_worker):
    executions = []

    async def work(i):
        executions.append(i)
        await asyncio.sleep(0.01)
        return i

    @flow.workflow(name="bulk")
    async def wf(ctx, i):
        return await ctx.step(work, i)

    for _ in range(4):
        start_worker(concurrency=5)
    ids = [await flow.start(wf, i) for i in range(60)]
    results = await asyncio.gather(*(flow.result(r, timeout=30) for r in ids))
    assert results == list(range(60))
    assert sorted(executions) == list(range(60))


async def test_stale_worker_cannot_commit_after_lease_takeover(flow, start_worker, schema):
    """Fencing: once another worker claims the run (new lease token), the old
    owner's step result is rejected, so exactly one result is ever recorded."""
    attempts = []
    release_first = asyncio.Event()

    @step(retry=RetryPolicy(max_attempts=5))
    async def slow():
        n = len(attempts) + 1
        attempts.append(n)
        if n == 1:
            await release_first.wait()  # stale owner, stuck mid-step
        return f"attempt-{n}"

    @flow.workflow(name="fenced")
    async def wf(ctx):
        return await ctx.step(slow)

    # Long lease so worker 1 doesn't notice via heartbeat before we act.
    start_worker(lease_seconds=60)
    run_id = await flow.start(wf)
    while not attempts:
        await asyncio.sleep(0.02)

    # Simulate a GC pause / partition: lease expires while worker 1 is stuck.
    async with flow.pool.connection() as conn:
        await conn.execute(
            sql.SQL("UPDATE {}.runs SET lease_expires_at = now() - interval '1s' WHERE id = %s").format(sql.Identifier(schema)),
            (run_id,),
        )
    start_worker(lease_seconds=60)
    assert await flow.result(run_id, timeout=10) == "attempt-2"

    release_first.set()  # stale owner now tries to commit attempt-1
    await asyncio.sleep(0.3)
    run = await flow.get_run(run_id)
    assert run["result"] == "attempt-2" and run["recoveries"] == 1
    [entry] = await flow.get_journal(run_id)
    assert entry["output"] == "attempt-2"


async def test_non_retryable_error_fails_immediately(flow, start_worker):
    calls = []

    @step(retry=RetryPolicy(max_attempts=5, initial_delay=0.01, non_retryable=(KeyError,)))
    async def fatal():
        calls.append(1)
        raise KeyError("missing")

    @flow.workflow(name="fatal")
    async def wf(ctx):
        await ctx.step(fatal)

    start_worker()
    with pytest.raises(WorkflowFailed, match="StepFailed"):
        await flow.run(wf, timeout=10)
    assert len(calls) == 1


async def test_busy_branch_is_not_suspended_while_sibling_sleeps(flow, start_worker):
    """Regression: a gather() branch doing a plain await (not a ctx op) must keep
    the run alive; previously the run suspended and cancelled the branch."""
    t0 = time.time()

    async def stamp():
        return time.time()

    @flow.workflow(name="mixed_branches")
    async def wf(ctx):
        async def long_branch():
            await ctx.sleep(3)

        async def busy_branch():
            await asyncio.sleep(0.3)  # not a ctx op
            return await ctx.step(stamp)

        _, stamped = await asyncio.gather(long_branch(), busy_branch())
        return stamped

    start_worker(inline_wait_max=0.05)
    stamped = await flow.run(wf, timeout=15)
    assert stamped - t0 < 2.0  # ran right away, not after the 3s sleep


async def test_busy_branch_can_signal_its_waiting_sibling(flow, start_worker):
    """Regression: with a signal wait (no wake-up time), premature suspension
    stranded the run forever."""
    holder = {}

    async def send():
        await flow.send_signal(holder["id"], "go", "hello")

    @flow.workflow(name="self_signal")
    async def wf(ctx):
        async def waiter():
            return await ctx.wait_for_signal("go")

        async def sender():
            await asyncio.sleep(0.3)
            await ctx.step(send)

        got, _ = await asyncio.gather(waiter(), sender())
        return got

    start_worker(inline_wait_max=0.05)
    holder["id"] = await flow.start(wf)
    assert await flow.result(holder["id"], timeout=10) == "hello"


async def test_concurrent_result_calls_share_one_listener(flow, start_worker):
    """Regression: each result() used to open its own LISTEN connection."""
    import psycopg

    from tests.conftest import DSN

    async def done_listeners() -> int:
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as c:
            cur = await c.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE query = %s",
                (f'LISTEN "{flow.done_channel}"',),
            )
            row = await cur.fetchone()
            assert row is not None
            return row[0]

    @flow.workflow(name="gated")
    async def gated(ctx, i):
        return [i, await ctx.wait_for_signal("go")]

    ids = [await flow.start(gated, i) for i in range(40)]
    waiters = [asyncio.create_task(flow.result(r, timeout=20)) for r in ids]
    await asyncio.sleep(0.5)
    assert await done_listeners() == 1  # one shared listener, not 40

    start_worker(inline_wait_max=0.05)
    for r in ids:
        await flow.send_signal(r, "go", "ok")
    assert await asyncio.gather(*waiters) == [[i, "ok"] for i in range(40)]
