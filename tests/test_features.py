"""Child workflows, retention purge and metrics."""

import asyncio
import contextlib
import uuid

import pytest
from psycopg import sql

from pgreplay_flow import (
    NO_RETRY,
    ChildFailed,
    StepFailed,
    WorkflowCancelled,
    WorkflowFailed,
)
from pgreplay_flow.metrics import collect, serve_metrics

# -- child workflows -------------------------------------------------------------


async def test_child_result_flows_to_parent_and_parent_is_released(flow, start_worker):
    @flow.workflow(name="square")
    async def square(ctx, x):
        await ctx.sleep(0.2)
        return x * x

    @flow.workflow(name="sum_squares")
    async def parent(ctx, xs):
        results = await asyncio.gather(*(ctx.child(square, x) for x in xs))
        return sum(results)

    start_worker(inline_wait_max=0.05)
    run_id = await flow.start(parent, [1, 2, 3])
    assert await flow.result(run_id, timeout=15) == 14

    children = await flow.list_runs(parent_id=run_id)
    assert len(children) == 3
    assert {c["status"] for c in children} == {"completed"}
    assert all(c["attached"] for c in children)
    journal = await flow.get_journal(run_id)
    # ctx.child() allocates its key at call time, and the generator is consumed
    # in the root task before gather() wraps the coroutines: keys are root-level.
    assert sorted(e["key"] for e in journal) == ["1", "2", "3"]
    assert {e["kind"] for e in journal} == {"child"}
    # The parent was parked while children ran, not holding a worker slot.
    assert (await flow.get_run(run_id))["claims"] >= 2

    report = await flow.replay(run_id)
    assert report.outcome == "completed" and report.matches_recorded


async def test_child_ids_are_deterministic_so_recovery_never_duplicates(flow, start_worker):
    starts = []

    async def note(x):
        starts.append(x)
        return x

    @flow.workflow(name="kid")
    async def kid(ctx, x):
        return await ctx.step(note, x)

    @flow.workflow(name="dad")
    async def dad(ctx):
        return await ctx.child(kid, 7)

    run_id = await flow.start(dad)
    expected = uuid.uuid5(uuid.UUID("6f1c6b4e-5f0e-4d8a-9a51-0c9b1f3e2d77"), f"{run_id}:1")
    start_worker()
    assert await flow.result(run_id, timeout=10) == 7
    [child] = await flow.list_runs(parent_id=run_id)
    assert child["id"] == expected
    assert starts == [7]


async def test_child_failure_raises_child_failed_in_parent(flow, start_worker):
    async def boom():
        raise ValueError("child broke")

    @flow.workflow(name="bad_kid", retry=NO_RETRY)
    async def bad_kid(ctx):
        await ctx.step(boom)

    @flow.workflow(name="careful_parent")
    async def careful(ctx):
        try:
            await ctx.child(bad_kid)
        except ChildFailed as e:
            return {"caught": e.error_type, "is_step_failed": isinstance(e, StepFailed)}

    @flow.workflow(name="careless_parent")
    async def careless(ctx):
        await ctx.child(bad_kid)

    start_worker()
    assert await flow.run(careful, timeout=10) == {"caught": "StepFailed", "is_step_failed": True}
    with pytest.raises(WorkflowFailed, match="ChildFailed"):
        await flow.run(careless, timeout=10)


async def test_start_child_is_detached(flow, start_worker):
    @flow.workflow(name="bg")
    async def bg(ctx, x):
        await ctx.sleep(1.5)
        return x

    @flow.workflow(name="fire_and_forget")
    async def parent(ctx):
        child_id = await ctx.start_child(bg, "later")
        return str(child_id)

    start_worker(inline_wait_max=0.05)
    run_id = await flow.start(parent)
    child_id = uuid.UUID(await flow.result(run_id, timeout=10))  # parent didn't wait
    child = await flow.get_run(child_id)
    assert child["status"] != "completed" and not child["attached"]
    assert child["parent_id"] == run_id
    assert await flow.result(child_id, timeout=10) == "later"


async def test_cancel_cascades_to_attached_children_only(flow, start_worker):
    @flow.workflow(name="slow_kid")
    async def slow_kid(ctx):
        await ctx.sleep(60)

    @flow.workflow(name="family")
    async def family(ctx):
        detached = await ctx.start_child(slow_kid)
        await ctx.child(slow_kid)
        return str(detached)

    start_worker(inline_wait_max=0.05)
    run_id = await flow.start(family)
    for _ in range(100):
        kids = await flow.list_runs(parent_id=run_id)
        if len(kids) == 2 and all(k["status"] == "sleeping" for k in kids):
            break
        await asyncio.sleep(0.05)
    assert await flow.cancel(run_id)
    with pytest.raises(WorkflowCancelled):
        await flow.result(run_id, timeout=5)
    by_kind = {k["attached"]: k["status"] for k in await flow.list_runs(parent_id=run_id)}
    assert by_kind == {True: "cancelled", False: "sleeping"}


async def test_cancelling_child_wakes_waiting_parent(flow, start_worker):
    @flow.workflow(name="stuck_kid")
    async def stuck(ctx):
        await ctx.wait_for_signal("never")

    @flow.workflow(name="watcher")
    async def watcher(ctx):
        try:
            await ctx.child(stuck)
        except ChildFailed as e:
            return e.error_type

    start_worker(inline_wait_max=0.05)
    run_id = await flow.start(watcher)
    kids: list = []
    for _ in range(100):
        kids = await flow.list_runs(parent_id=run_id)
        if kids and kids[0]["status"] == "waiting":
            break
        await asyncio.sleep(0.05)
    assert kids, "child never started"
    assert await flow.cancel(kids[0]["id"])
    assert await flow.result(run_id, timeout=10) == "WorkflowCancelled"


async def test_nested_children(flow, start_worker):
    @flow.workflow(name="leaf")
    async def leaf(ctx, n):
        return n

    @flow.workflow(name="mid")
    async def mid(ctx, n):
        return await ctx.child(leaf, n) + await ctx.child(leaf, n * 10)

    @flow.workflow(name="root")
    async def root(ctx):
        return await ctx.child(mid, 1) + await ctx.child(mid, 2)

    start_worker(concurrency=3)
    assert await flow.run(root, timeout=20) == 33


# -- retention ---------------------------------------------------------------------


async def _age_runs(flow, schema, seconds):
    async with flow.pool.connection() as conn:
        await conn.execute(
            sql.SQL("""
                UPDATE {}.runs SET finished_at = finished_at - make_interval(secs => %s)
                WHERE finished_at IS NOT NULL
            """).format(sql.Identifier(schema)),
            (seconds,),
        )


async def test_purge_deletes_only_old_terminal_runs(flow, start_worker, schema):
    @flow.workflow(name="quick")
    async def quick(ctx, x):
        return await ctx.step(lambda: x)

    @flow.workflow(name="parked")
    async def parked(ctx):
        await ctx.wait_for_signal("go")

    start_worker(inline_wait_max=0.05)
    done = [await flow.start(quick, i) for i in range(5)]
    for r in done:
        await flow.result(r, timeout=10)
    waiting = await flow.start(parked)
    await flow.send_signal(waiting, "unrelated")

    assert await flow.purge(3600) == 0  # nothing is old yet
    await _age_runs(flow, schema, 7200)
    fresh = await flow.start(quick, 99)
    await flow.result(fresh, timeout=10)

    assert await flow.count_purgeable(3600) == 5
    assert await flow.purge(3600, batch_size=2) == 5  # several batches
    assert await flow.count_purgeable(3600) == 0
    remaining = {r["id"] for r in await flow.list_runs(limit=100)}
    assert remaining == {waiting, fresh}
    async with flow.pool.connection() as conn:
        for table in ("steps", "signals"):
            cur = await conn.execute(
                sql.SQL("SELECT count(DISTINCT run_id) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
            n = (await cur.fetchone())[0]
            assert n <= 2, table  # journals of purged runs went with them


async def test_purge_keeps_child_of_unfinished_parent(flow, start_worker, schema):
    @flow.workflow(name="tiny")
    async def tiny(ctx):
        return 1

    @flow.workflow(name="slow_parent")
    async def slow_parent(ctx):
        await ctx.child(tiny)
        await ctx.wait_for_signal("finish")

    start_worker(inline_wait_max=0.05)
    run_id = await flow.start(slow_parent)
    for _ in range(100):
        if (await flow.get_run(run_id))["status"] == "waiting" and await flow.list_runs(
            parent_id=run_id, status="completed"
        ):
            break
        await asyncio.sleep(0.05)
    await _age_runs(flow, schema, 7200)
    # Regression: the dry-run count must apply the same rule as purge().
    assert await flow.count_purgeable(60) == 0
    assert await flow.purge(60) == 0
    with pytest.raises(ValueError):
        await flow.purge(60, statuses=("running",))


async def test_worker_background_purge(flow, start_worker, schema):
    @flow.workflow(name="old")
    async def old(ctx):
        return 1

    start_worker()
    run_id = await flow.start(old)
    await flow.result(run_id, timeout=10)
    await _age_runs(flow, schema, 100)
    w = start_worker(purge_after=50, purge_interval=0.2)
    for _ in range(50):
        if not await flow.list_runs():
            break
        await asyncio.sleep(0.1)
    assert await flow.list_runs() == []
    assert w.metrics.get("runs_purged_total") == 1


# -- metrics -------------------------------------------------------------------------


async def test_worker_metrics_and_stats(flow, start_worker):
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("once")
        return "ok"

    @flow.workflow(name="metered")
    async def metered(ctx):
        await ctx.sleep(0.3)
        return await ctx.step(flaky)

    w = start_worker(inline_wait_max=0.05)
    from pgreplay_flow import RetryPolicy

    flow.workflows["metered"] = flow.workflows["metered"].__class__(
        "metered", metered, "default", RetryPolicy(max_attempts=3, initial_delay=0.01)
    )
    run_id = await flow.start(metered)
    assert await flow.result(run_id, timeout=10) == "ok"

    m = w.metrics
    assert m.get("runs_finished_total", workflow="metered", status="completed") == 1
    assert m.get("runs_suspended_total", workflow="metered", status="sleeping") >= 1
    assert m.get("step_attempts_total", workflow="metered", step="test_worker_metrics_and_stats"
                 ".<locals>.flaky", outcome="retrying") == 1
    assert m.get("step_attempts_total", workflow="metered", step="test_worker_metrics_and_stats"
                 ".<locals>.flaky", outcome="completed") == 1
    assert m.get("runs_claimed_total", workflow="metered") >= 2
    assert m.get("runs_in_flight") == 0

    stats = await flow.stats()
    assert {"queue": "default", "workflow": "metered", "status": "completed", "count": 1} in stats["runs"]
    assert stats["expired_leases"] == 0

    text = await collect(flow, w)
    assert '# TYPE pgreplay_flow_runs gauge' in text
    assert 'pgreplay_flow_runs{queue="default",status="completed",workflow="metered"} 1' in text
    assert 'pgreplay_flow_runs_finished_total{status="completed",workflow="metered"} 1' in text
    assert "pgreplay_flow_step_attempt_seconds_count" in text


async def test_metrics_http_endpoint(flow, start_worker):
    w = start_worker()
    server = await serve_metrics(flow, w, port=0)
    port = server.sockets[0].getsockname()[1]
    try:
        async def get(path):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            await writer.drain()
            data = await reader.read()
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            return data.decode()

        ok = await get("/metrics")
        assert ok.startswith("HTTP/1.1 200 OK")
        assert "pgreplay_flow_expired_leases 0" in ok
        assert "pgreplay_flow_runs_in_flight 0" in ok
        assert (await get("/nope")).startswith("HTTP/1.1 404")
    finally:
        server.close()
        await server.wait_closed()


async def test_parent_recovering_after_crash_reuses_child(flow, start_worker, monkeypatch):
    """The parent's worker dies right after the child run was created (the
    start_child transaction committed, nothing after it did). Recovery must
    reattach to that same child, not start a second one."""
    from pgreplay_flow.errors import StoreUnavailable
    from pgreplay_flow.store import RunStore

    kid_runs = []

    async def work():
        kid_runs.append(1)
        return "kid-done"

    @flow.workflow(name="kid2")
    async def kid2(ctx):
        return await ctx.step(work)

    @flow.workflow(name="dad2")
    async def dad2(ctx):
        return await ctx.child(kid2)

    crashed = []
    real_child_outcome = RunStore.child_outcome

    async def crash_once(self, child_id):
        if not crashed:
            crashed.append(child_id)
            raise StoreUnavailable("simulated crash after start_child committed")
        return await real_child_outcome(self, child_id)

    monkeypatch.setattr(RunStore, "child_outcome", crash_once)
    start_worker(lease_seconds=1.0)
    run_id = await flow.start(dad2)
    assert await flow.result(run_id, timeout=15) == "kid-done"

    [child] = await flow.list_runs(parent_id=run_id)
    assert crashed == [child["id"]]
    assert (await flow.get_run(run_id))["recoveries"] == 1
    assert kid_runs == [1]
