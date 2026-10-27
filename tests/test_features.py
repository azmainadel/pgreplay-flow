"""Child workflows."""

import asyncio
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


async def test_parent_recovering_after_lease_loss_reuses_child(flow, start_worker, schema):
    """Worker dies right after the child was created: recovery must attach to
    the same child, not start a second one."""
    kid_runs = []

    async def work():
        kid_runs.append(1)
        return "kid-done"

    @flow.workflow(name="kid2")
    async def kid2(ctx):
        await ctx.sleep(0.5)
        return await ctx.step(work)

    @flow.workflow(name="dad2")
    async def dad2(ctx):
        return await ctx.child(kid2)

    run_id = await flow.start(dad2)
    w1 = start_worker(lease_seconds=60, inline_wait_max=2.0, queues=("default",))
    # Wait until the child exists, then make w1 "vanish": stop it without
    # releasing, and expire the parent's lease as if its process had died.
    for _ in range(100):
        if await flow.list_runs(parent_id=run_id):
            break
        await asyncio.sleep(0.02)
    w1._stopping.set()
    async with flow.pool.connection() as conn:
        await conn.execute(
            sql.SQL("""
                UPDATE {}.runs SET lease_expires_at = now() - interval '1s'
                WHERE id = %s AND status = 'running'
            """).format(sql.Identifier(schema)),
            (run_id,),
        )
    start_worker(inline_wait_max=0.05)
    assert await flow.result(run_id, timeout=15) == "kid-done"
    assert len(await flow.list_runs(parent_id=run_id)) == 1
    assert kid_runs == [1]
