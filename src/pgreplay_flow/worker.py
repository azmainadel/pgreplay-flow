"""Worker: claims runnable runs and executes them."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import sql

from .context import WorkflowContext
from .errors import LeaseLost, NonDeterminismError, StoreUnavailable
from .flow import Flow
from .listener import ChannelListener
from .metrics import Metrics
from .serde import error_info, normalize
from .store import RunStore

log = logging.getLogger("pgreplay_flow")


@dataclass
class _Claim:
    run_id: uuid.UUID
    workflow: str
    args: dict[str, Any]
    token: uuid.UUID
    db_now: float
    recovered: bool = False


@dataclass
class Worker:
    """Claims runnable runs and executes them.

    Run as many workers (processes, hosts) as you like against the same
    database; they coordinate only through Postgres.
    """

    flow: Flow
    queues: tuple[str, ...] = ("default",)
    concurrency: int = 10
    lease_seconds: float = 30.0
    poll_interval: float = 5.0
    inline_wait_max: float = 1.0
    # Retention: if set, this worker periodically deletes runs that finished
    # more than purge_after seconds ago (see Flow.purge).
    purge_after: float | None = None
    purge_interval: float = 3600.0
    worker_id: str = field(
        default_factory=lambda: f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    )

    def __post_init__(self) -> None:
        self._wake = asyncio.Event()
        self._stopping = asyncio.Event()
        self._running: dict[uuid.UUID, asyncio.Task] = {}
        self.metrics = Metrics()
        self.metrics.set("runs_in_flight", 0)

    # -- public ---------------------------------------------------------------

    async def run(self) -> None:
        """Run until stop() is called (or the task is cancelled)."""
        # Any wake notification (or a reconnect, which may have missed some) just
        # nudges the claim loop; the claim query is the source of truth.
        listener = ChannelListener(
            self.flow.dsn, self.flow.wake_channel,
            lambda _payload: self._wake.set(), on_connect=self._wake.set,
        )
        listener.start()
        purger = (
            asyncio.create_task(self._purge_loop(), name="pgreplay-flow-purge")
            if self.purge_after is not None else None
        )
        try:
            while not self._stopping.is_set():
                self._wake.clear()
                claimed = 0
                free = self._free_slots()
                if free:
                    try:
                        claims = await self._claim(free)
                    except psycopg.OperationalError as e:
                        log.warning("claim failed: %s", e)
                        claims = []
                    claimed = len(claims)
                    for c in claims:
                        t = asyncio.create_task(self._execute(c), name=f"run-{c.run_id}")
                        self._running[c.run_id] = t
                        self.metrics.inc("runs_claimed_total", workflow=c.workflow)
                        if c.recovered:
                            self.metrics.inc("runs_recovered_total", workflow=c.workflow)
                        self.metrics.set("runs_in_flight", len(self._running))
                        t.add_done_callback(lambda _t, rid=c.run_id: self._done(rid))
                if claimed and claimed == free:
                    continue  # maybe more work queued
                await self._idle(free_slots=bool(self._free_slots()))
        finally:
            if purger is not None:
                purger.cancel()
            await self._shutdown(grace=0)
            await listener.close()
            if purger is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await purger

    async def stop(self, *, grace: float = 10.0) -> None:
        """Stop claiming; let in-flight runs finish for up to `grace` seconds,
        then hand the rest back to the queue."""
        self._stopping.set()
        self._wake.set()
        await self._shutdown(grace)

    # -- loop internals -------------------------------------------------------

    def _free_slots(self) -> int:
        return self.concurrency - len(self._running)

    def _done(self, run_id: uuid.UUID) -> None:
        self._running.pop(run_id, None)
        self.metrics.set("runs_in_flight", len(self._running))
        self._wake.set()

    async def _shutdown(self, grace: float) -> None:
        tasks = list(self._running.values())
        if not tasks:
            return
        if grace > 0:
            await asyncio.wait(tasks, timeout=grace)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _idle(self, *, free_slots: bool) -> None:
        timeout = self.poll_interval
        if free_slots:
            with contextlib.suppress(psycopg.OperationalError):
                nxt = await self._next_due()
                if nxt is not None:
                    timeout = max(0.0, min(timeout, nxt))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout)

    async def _purge_loop(self) -> None:
        assert self.purge_after is not None
        while True:
            try:
                n = await self.flow.purge(self.purge_after)
                if n:
                    self.metrics.inc("runs_purged_total", n)
                    log.info("purged %d finished runs", n)
            except psycopg.Error as e:
                log.warning("purge failed: %s", e)
            await asyncio.sleep(self.purge_interval)

    async def _next_due(self) -> float | None:
        names = list(self.flow.workflows)
        async with self.flow.pool.connection() as conn:
            cur = await conn.execute(
                sql.SQL("""
                SELECT extract(epoch FROM min(t) - now())::float8 FROM (
                    SELECT min(ready_at) AS t FROM {0}.runs
                     WHERE status IN ('pending','sleeping','waiting') AND queue = ANY(%s)
                       AND workflow = ANY(%s)
                    UNION ALL
                    SELECT min(lease_expires_at) FROM {0}.runs
                     WHERE status = 'running' AND queue = ANY(%s) AND workflow = ANY(%s)
                ) x
                """).format(sql.Identifier(self.flow.schema)),
                (list(self.queues), names, list(self.queues), names),
            )
            row = await cur.fetchone()
            return None if row is None or row[0] is None else row[0]

    async def _claim(self, limit: int) -> list[_Claim]:
        """Claim up to `limit` runs: due pending/sleeping/waiting runs, plus
        running runs whose lease expired (crash recovery). SKIP LOCKED lets any
        number of workers claim concurrently without blocking each other."""
        async with self.flow.pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                sql.SQL("""
                WITH c AS (
                    SELECT id, status AS prev_status FROM {0}.runs
                    WHERE queue = ANY(%s) AND workflow = ANY(%s) AND (
                          (status IN ('pending','sleeping','waiting') AND ready_at <= now())
                       OR (status = 'running' AND lease_expires_at < now()))
                    ORDER BY coalesce(ready_at, lease_expires_at)
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {0}.runs r
                SET recoveries = r.recoveries + (r.status = 'running')::int,
                    status = 'running',
                    lease_token = gen_random_uuid(),
                    lease_owner = %s,
                    lease_expires_at = now() + make_interval(secs => %s),
                    claims = r.claims + 1,
                    updated_at = now()
                FROM c WHERE r.id = c.id
                RETURNING r.id, r.workflow, r.args, r.lease_token,
                          extract(epoch FROM now())::float8, c.prev_status = 'running'
                """).format(sql.Identifier(self.flow.schema)),
                (list(self.queues), list(self.flow.workflows), limit, self.worker_id,
                 self.lease_seconds),
            )
            rows = await cur.fetchall()
        return [_Claim(*r) for r in rows]

    # -- executing one run ----------------------------------------------------

    async def _execute(self, claim: _Claim) -> None:
        store = RunStore(self.flow.pool, self.flow.schema, claim.run_id, claim.token)
        m, wf = self.metrics, claim.workflow
        t0 = time.perf_counter()
        try:
            kind, status = await self._execute_inner(claim, store)
            m.inc("runs_finished_total" if kind == "finished" else "runs_suspended_total",
                  workflow=wf, status=status)
        except asyncio.CancelledError:
            m.inc("runs_abandoned_total", workflow=wf, reason="shutdown")
            # Worker shutdown: hand the run back right away instead of waiting
            # for the lease to expire.
            with contextlib.suppress(Exception, LeaseLost, StoreUnavailable):
                await asyncio.shield(store.release())
            raise
        except (LeaseLost, StoreUnavailable) as e:
            reason = "lease_lost" if isinstance(e, LeaseLost) else "store_unavailable"
            m.inc("runs_abandoned_total", workflow=wf, reason=reason)
            log.info("run %s abandoned: %r", claim.run_id, e)
        except Exception:
            m.inc("runs_abandoned_total", workflow=wf, reason="internal_error")
            log.exception("internal error executing run %s", claim.run_id)
        finally:
            m.observe("run_execution_seconds", time.perf_counter() - t0, workflow=wf)

    async def _execute_inner(self, claim: _Claim, store: RunStore) -> tuple[str, str]:
        wf = self.flow.workflows[claim.workflow]
        journal = await self.flow._load_journal(claim.run_id)
        # Durable deadlines are compared against the database clock (ready_at
        # <= now()), so use DB time inside the workflow too.
        offset = claim.db_now - time.time()
        ctx = WorkflowContext(
            run_id=claim.run_id, workflow=wf.name, journal=journal, store=store,
            default_retry=wf.retry, inline_wait_max=self.inline_wait_max,
            clock=lambda: time.time() + offset,
            resolve=self.flow._child_ref, metrics=self.metrics,
        )
        main = asyncio.get_running_loop().create_task(
            ctx._main(wf.fn, claim.args["args"], claim.args["kwargs"]),
            context=contextvars.Context(),
        )
        lost = asyncio.Event()
        hb = asyncio.create_task(self._heartbeat(store, lost))
        quiet = asyncio.create_task(ctx.quiescent.wait())
        lost_w = asyncio.create_task(lost.wait())
        try:
            await asyncio.wait({main, quiet, lost_w}, return_when=asyncio.FIRST_COMPLETED)
            if main.done():
                return "finished", await self._finish(ctx, main, store)
            if lost.is_set():
                raise LeaseLost()
            ctx.close()
            main.cancel()
            with contextlib.suppress(BaseException):
                await main
            status, until = ctx.suspension()
            if not await store.suspend(status, until):
                raise LeaseLost()
            return "suspended", status
        finally:
            for t in (hb, quiet, lost_w):
                t.cancel()
            if not main.done():
                main.cancel()
                with contextlib.suppress(BaseException):
                    await main

    async def _finish(self, ctx: WorkflowContext, main: asyncio.Task, store: RunStore) -> str:
        exc = main.exception() if not main.cancelled() else asyncio.CancelledError()
        if isinstance(exc, (LeaseLost, StoreUnavailable)):
            raise exc
        if exc is None:
            try:
                ctx.assert_journal_consumed()
                result = normalize(main.result())
            except (NonDeterminismError, TypeError) as e:
                status, ok = "failed", await store.finish("failed", error=error_info(e))
            else:
                status, ok = "completed", await store.finish("completed", result=result)
        else:
            status, ok = "failed", await store.finish("failed", error=error_info(exc))
        if not ok:
            raise LeaseLost()
        return status

    async def _heartbeat(self, store: RunStore, lost: asyncio.Event) -> None:
        interval = self.lease_seconds / 3
        while True:
            await asyncio.sleep(interval)
            try:
                if not await store.heartbeat(self.lease_seconds):
                    lost.set()  # cancelled, or another worker took over
                    return
            except StoreUnavailable as e:
                log.warning("heartbeat failed for %s: %s", store.run_id, e)
