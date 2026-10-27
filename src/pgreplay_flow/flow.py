"""Flow (registry + client API) and Worker (claims and executes runs)."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
import os
import socket
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .context import WorkflowContext
from .errors import (
    JournalExhausted,
    LeaseLost,
    NonDeterminismError,
    RunNotFound,
    StoreUnavailable,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowNotFound,
)
from .listener import ChannelListener
from .metrics import Metrics
from .retry import RetryPolicy
from .schema import ddl, validate_schema_name
from .serde import error_info, normalize
from .store import RunStore

log = logging.getLogger("pgreplay_flow")

TERMINAL = ("completed", "failed", "cancelled")

WorkflowFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class WorkflowDef:
    name: str
    fn: WorkflowFn
    queue: str
    retry: RetryPolicy


@dataclass(frozen=True)
class ReplayReport:
    """Outcome of re-executing a run's workflow code against its journal, offline."""

    run_id: uuid.UUID
    outcome: str  # completed | failed | incomplete | nondeterministic
    result: Any = None
    error: dict[str, Any] | None = None
    recorded_status: str = ""
    matches_recorded: bool = False
    ops_replayed: int = 0
    stopped_at: str | None = None  # key of the first op with no recorded outcome


class Flow:
    """Workflow registry and client. One per process; share it with Workers.

    Usage::

        flow = Flow("postgresql://...")

        @flow.workflow()
        async def onboard(ctx, user_id): ...

        async with flow:
            run_id = await flow.start(onboard, 42)
            print(await flow.result(run_id))
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "pgreplay_flow",
        default_retry: RetryPolicy | None = None,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
    ):
        self.dsn = dsn
        self.schema = validate_schema_name(schema)
        self.default_retry = default_retry or RetryPolicy()
        self.pool = AsyncConnectionPool(
            dsn, min_size=pool_min_size, max_size=pool_max_size, open=False,
            kwargs={"autocommit": True},
        )
        self.workflows: dict[str, WorkflowDef] = {}
        self.wake_channel = f"{self.schema}_wake"
        self.done_channel = f"{self.schema}_done"
        # result() waiters share one LISTEN connection, opened on first use.
        self._done_listener: ChannelListener | None = None
        self._done_waiters: dict[str, set[asyncio.Event]] = {}

    # -- lifecycle ------------------------------------------------------------

    async def open(self, *, migrate: bool = True) -> None:
        await self.pool.open(wait=True)
        if migrate:
            await self.migrate()

    async def close(self) -> None:
        if self._done_listener is not None:
            await self._done_listener.close()
            self._done_listener = None
        await self.pool.close()

    async def __aenter__(self) -> Flow:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def migrate(self) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            # Serialize concurrent migrations (CREATE ... IF NOT EXISTS can race).
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.schema,))
            await conn.execute(ddl(self.schema))  # type: ignore[arg-type]

    # -- registration ---------------------------------------------------------

    def workflow(
        self, *, name: str | None = None, queue: str = "default", retry: RetryPolicy | None = None
    ) -> Callable[[WorkflowFn], WorkflowFn]:
        """Register `async def wf(ctx, *args, **kwargs)` as a durable workflow.

        `name` is the durable identity stored in the database; set it explicitly
        if you may rename or move the function. `retry` is the default step
        retry policy inside this workflow.
        """

        def deco(fn: WorkflowFn) -> WorkflowFn:
            if not asyncio.iscoroutinefunction(fn):
                raise TypeError("workflows must be async functions")
            wf_name = name or f"{fn.__module__}.{fn.__qualname__}"
            if wf_name in self.workflows and self.workflows[wf_name].fn is not fn:
                raise ValueError(f"workflow {wf_name!r} already registered")
            self.workflows[wf_name] = WorkflowDef(wf_name, fn, queue, retry or self.default_retry)
            fn.__pgreplay_workflow__ = wf_name  # type: ignore[attr-defined]
            return fn

        return deco

    def _child_ref(self, workflow: WorkflowFn | str) -> tuple[str, str]:
        """(name, queue) for a child workflow. A string name that is not
        registered in this process is allowed (another service's workers may
        run it); it goes to the 'default' queue."""
        if isinstance(workflow, str) and workflow not in self.workflows:
            return workflow, "default"
        wf = self._resolve(workflow)
        return wf.name, wf.queue

    def _resolve(self, workflow: WorkflowFn | str) -> WorkflowDef:
        name = workflow if isinstance(workflow, str) else getattr(
            workflow, "__pgreplay_workflow__", None
        )
        if name is None or name not in self.workflows:
            raise WorkflowNotFound(f"workflow not registered: {workflow!r}")
        return self.workflows[name]

    # -- client API -----------------------------------------------------------

    async def start(
        self,
        workflow: WorkflowFn | str,
        *args: Any,
        idempotency_key: str | None = None,
        queue: str | None = None,
        conn: AsyncConnection | None = None,
        **kwargs: Any,
    ) -> uuid.UUID:
        """Enqueue a run and return its id.

        With `idempotency_key`, starting the same key twice returns the first
        run's id. Pass `conn` to enqueue inside your own transaction: the run
        becomes visible to workers only if that transaction commits.
        """
        wf = self._resolve(workflow)
        payload = normalize({"args": list(args), "kwargs": kwargs})
        run_id = uuid.uuid4()
        params = (run_id, wf.name, queue or wf.queue, Jsonb(payload), idempotency_key)
        insert = sql.SQL("""
            INSERT INTO {0}.runs (id, workflow, queue, args, idempotency_key)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING RETURNING id
        """).format(sql.Identifier(self.schema))

        async def run(c: AsyncConnection) -> uuid.UUID:
            cur = await c.execute(insert, params)
            row = await cur.fetchone()
            if row is None:
                cur = await c.execute(
                    sql.SQL("SELECT id FROM {0}.runs WHERE idempotency_key = %s").format(sql.Identifier(self.schema)),
                    (idempotency_key,),
                )
                row = await cur.fetchone()
                assert row is not None
                return row[0]
            # NOTIFY is transactional: delivered only if the enqueue commits.
            await c.execute("SELECT pg_notify(%s, '')", (self.wake_channel,))
            return row[0]

        if conn is not None:
            return await run(conn)
        async with self.pool.connection() as c, c.transaction():
            return await run(c)

    async def get_run(self, run_id: uuid.UUID) -> dict[str, Any]:
        async with self.pool.connection() as conn:
            cur = conn.cursor(row_factory=dict_row)
            await cur.execute(sql.SQL("SELECT * FROM {0}.runs WHERE id = %s").format(sql.Identifier(self.schema)), (run_id,))
            row = await cur.fetchone()
        if row is None:
            raise RunNotFound(str(run_id))
        return row

    async def get_journal(self, run_id: uuid.UUID) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cur = conn.cursor(row_factory=dict_row)
            await cur.execute(
                sql.SQL("SELECT * FROM {0}.steps WHERE run_id = %s ORDER BY ord").format(sql.Identifier(self.schema)), (run_id,)
            )
            return await cur.fetchall()

    def _notify_done(self, payload: str) -> None:
        for ev in self._done_waiters.get(payload, ()):
            ev.set()

    def _wake_all_waiters(self) -> None:
        # After a (re)connect, notifications may have been missed: re-check all.
        for evs in self._done_waiters.values():
            for ev in evs:
                ev.set()

    async def _ensure_done_listener(self) -> ChannelListener:
        if self._done_listener is None:
            self._done_listener = ChannelListener(
                self.dsn, self.done_channel, self._notify_done, on_connect=self._wake_all_waiters
            )
        self._done_listener.start()
        await self._done_listener.wait_started()
        return self._done_listener

    async def result(self, run_id: uuid.UUID, *, timeout: float | None = None) -> Any:
        """Wait for a run to finish. Returns its result, or raises WorkflowFailed /
        WorkflowCancelled / TimeoutError.

        All concurrent waiters share one LISTEN connection per Flow. If it is
        down, waiting degrades to polling once a second.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        key = str(run_id)
        event = asyncio.Event()
        # Register before the first status check: a notification that arrives
        # between the check and the wait still sets the event (no lost wakeup).
        self._done_waiters.setdefault(key, set()).add(event)
        try:
            listener = await self._ensure_done_listener()
            while True:
                event.clear()
                run = await self.get_run(run_id)
                if run["status"] == "completed":
                    return run["result"]
                if run["status"] == "failed":
                    raise WorkflowFailed(run_id, run["error"])
                if run["status"] == "cancelled":
                    raise WorkflowCancelled(run_id)
                # With a live listener this is only a safety net.
                wait = 30.0 if listener.connected else 1.0
                if deadline is not None:
                    wait = min(wait, deadline - time.monotonic())
                    if wait <= 0:
                        raise TimeoutError(f"run {run_id} still {run['status']}")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(event.wait(), wait)
        finally:
            waiters = self._done_waiters.get(key)
            if waiters is not None:
                waiters.discard(event)
                if not waiters:
                    del self._done_waiters[key]

    async def run(self, workflow: WorkflowFn | str, *args: Any, timeout: float | None = None,
                  **kwargs: Any) -> Any:
        """start() + result(). Needs a Worker running somewhere."""
        return await self.result(await self.start(workflow, *args, **kwargs), timeout=timeout)

    async def send_signal(self, run_id: uuid.UUID, name: str, payload: Any = None) -> None:
        """Deliver a signal. Buffered until the workflow calls wait_for_signal(name)."""
        payload = normalize(payload)
        async with self.pool.connection() as conn, conn.transaction():
            # Lock the run row first; pairs with RunStore.suspend (see there).
            cur = await conn.execute(
                sql.SQL("SELECT status FROM {0}.runs WHERE id = %s FOR UPDATE").format(sql.Identifier(self.schema)), (run_id,)
            )
            row = await cur.fetchone()
            if row is None:
                raise RunNotFound(str(run_id))
            if row[0] in TERMINAL:
                return
            await conn.execute(
                sql.SQL("INSERT INTO {0}.signals (run_id, name, payload) VALUES (%s, %s, %s)").format(sql.Identifier(self.schema)),
                (run_id, name, Jsonb(payload)),
            )
            if row[0] == "waiting":
                await conn.execute(
                    sql.SQL("UPDATE {0}.runs SET ready_at = now(), updated_at = now() WHERE id = %s").format(sql.Identifier(self.schema)),
                    (run_id,),
                )
                await conn.execute("SELECT pg_notify(%s, '')", (self.wake_channel,))

    async def cancel(self, run_id: uuid.UUID) -> bool:
        """Cancel a non-terminal run and, recursively, the attached children
        (ctx.child) it is waiting on. A worker executing a cancelled run loses its
        lease and stops at its next journal write or heartbeat. A parent waiting
        on a cancelled child is woken and sees ChildFailed.
        Returns False if the run had already finished."""
        s = sql.Identifier(self.schema)
        async with self.pool.connection() as conn, conn.transaction():
            # Lock order is parent -> child everywhere (see RunStore.finish).
            cur = await conn.execute(
                sql.SQL("SELECT parent_id, attached FROM {0}.runs WHERE id = %s").format(s),
                (run_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise RunNotFound(str(run_id))
            parent_id = row[0] if row[1] else None
            if parent_id is not None:
                await conn.execute(
                    sql.SQL("SELECT 1 FROM {0}.runs WHERE id = %s FOR UPDATE").format(s),
                    (parent_id,),
                )
            cancelled: list[uuid.UUID] = []
            level = [run_id]
            while level:
                cur = await conn.execute(
                    sql.SQL("""
                    UPDATE {0}.runs
                    SET status = 'cancelled', finished_at = now(), updated_at = now(),
                        lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                        ready_at = NULL
                    WHERE id = ANY(%s) AND status NOT IN ('completed','failed','cancelled')
                    RETURNING id
                    """).format(s),
                    (level,),
                )
                done = [r[0] for r in await cur.fetchall()]
                cancelled.extend(done)
                if not done:
                    break
                cur = await conn.execute(
                    sql.SQL("""
                    SELECT id FROM {0}.runs
                    WHERE parent_id = ANY(%s) AND attached
                      AND status NOT IN ('completed','failed','cancelled')
                    """).format(s),
                    (done,),
                )
                level = [r[0] for r in await cur.fetchall()]
            if not cancelled:
                return False
            if parent_id is not None:
                await conn.execute(
                    sql.SQL("""
                    UPDATE {0}.runs SET ready_at = now(), updated_at = now()
                    WHERE id = %s AND status IN ('waiting','sleeping')
                    """).format(s),
                    (parent_id,),
                )
                await conn.execute("SELECT pg_notify(%s, '')", (self.wake_channel,))
            for rid in cancelled:
                await conn.execute("SELECT pg_notify(%s, %s)", (self.done_channel, str(rid)))
            return True

    # -- inspection -------------------------------------------------------------

    async def list_runs(
        self,
        *,
        status: str | None = None,
        workflow: str | None = None,
        parent_id: uuid.UUID | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Most recent runs first (without args/result payloads)."""
        async with self.pool.connection() as conn:
            cur = conn.cursor(row_factory=dict_row)
            await cur.execute(
                sql.SQL("""
                SELECT id, workflow, queue, status, parent_id, attached, claims, recoveries,
                       created_at, updated_at, finished_at, ready_at, lease_owner
                FROM {0}.runs
                WHERE (%s::text IS NULL OR status = %s)
                  AND (%s::text IS NULL OR workflow = %s)
                  AND (%s::uuid IS NULL OR parent_id = %s)
                ORDER BY created_at DESC LIMIT %s
                """).format(sql.Identifier(self.schema)),
                (status, status, workflow, workflow, parent_id, parent_id, limit),
            )
            return await cur.fetchall()

    async def stats(self) -> dict[str, Any]:
        """Database-wide gauges: run counts by (queue, workflow, status), age of
        the oldest unclaimed runnable run per queue, and expired leases."""
        s = sql.Identifier(self.schema)
        async with self.pool.connection() as conn:
            cur = conn.cursor(row_factory=dict_row)
            await cur.execute(
                sql.SQL("""
                SELECT queue, workflow, status, count(*)::int AS count
                FROM {0}.runs GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
                """).format(s)
            )
            runs = await cur.fetchall()
            await cur.execute(
                sql.SQL("""
                SELECT queue, extract(epoch FROM now() - min(ready_at))::float8 AS lag
                FROM {0}.runs
                WHERE status IN ('pending','sleeping','waiting') AND ready_at <= now()
                GROUP BY queue
                """).format(s)
            )
            lag = {r["queue"]: r["lag"] for r in await cur.fetchall()}
            for q in {r["queue"] for r in runs}:
                lag.setdefault(q, 0.0)
            await cur.execute(
                sql.SQL("""
                SELECT count(*)::int AS n FROM {0}.runs
                WHERE status = 'running' AND lease_expires_at < now()
                """).format(s)
            )
            row = await cur.fetchone()
        return {"runs": runs, "queue_lag_seconds": lag,
                "expired_leases": row["n"] if row else 0}

    # -- retention ----------------------------------------------------------------

    async def purge(
        self,
        older_than: float,
        *,
        workflow: str | None = None,
        statuses: tuple[str, ...] = TERMINAL,
        batch_size: int = 1000,
    ) -> int:
        """Delete finished runs (with their journals and signals) whose
        finished_at is more than `older_than` seconds ago. Returns the count.

        Runs in small batches with SKIP LOCKED, so it is safe to call from many
        workers at once. An attached child is kept while its parent is still
        running, because the parent has not copied its result yet.
        """
        bad = set(statuses) - set(TERMINAL)
        if bad:
            raise ValueError(f"can only purge terminal statuses, not {sorted(bad)}")
        if older_than < 0:
            raise ValueError("older_than must be >= 0")
        total = 0
        while True:
            async with self.pool.connection() as conn, conn.transaction():
                cur = await conn.execute(
                    sql.SQL("""
                    DELETE FROM {0}.runs WHERE id IN (
                        SELECT r.id FROM {0}.runs r
                        WHERE r.status = ANY(%s)
                          AND r.finished_at < now() - make_interval(secs => %s)
                          AND (%s::text IS NULL OR r.workflow = %s)
                          AND NOT (r.attached AND EXISTS (
                                SELECT 1 FROM {0}.runs p
                                WHERE p.id = r.parent_id
                                  AND p.status NOT IN ('completed','failed','cancelled')))
                        ORDER BY r.finished_at
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED)
                    """).format(sql.Identifier(self.schema)),
                    (list(statuses), older_than, workflow, workflow, batch_size),
                )
                n = cur.rowcount
            total += n
            if n < batch_size:
                return total

    # -- replay and fork --------------------------------------------------------

    async def _load_journal(self, run_id: uuid.UUID) -> dict[str, dict[str, Any]]:
        return {e["key"]: e for e in await self.get_journal(run_id)}

    async def replay(self, run_id: uuid.UUID) -> ReplayReport:
        """Re-execute the run's workflow code against its journal with no side
        effects (steps, sleeps and signals return their recorded outcomes).

        Use it to debug a run, or to check that a code change is replay-safe
        for runs that are still in flight.
        """
        run = await self.get_run(run_id)
        wf = self._resolve(run["workflow"])
        journal = await self._load_journal(run_id)
        ctx = WorkflowContext(
            run_id=run_id, workflow=wf.name, journal=journal, store=None, default_retry=wf.retry,
            resolve=self._child_ref,
        )
        status: str = run["status"]

        def report(**kw: Any) -> ReplayReport:
            return ReplayReport(run_id=run_id, recorded_status=status, **kw)

        task = asyncio.get_running_loop().create_task(
            ctx._main(wf.fn, run["args"]["args"], run["args"]["kwargs"]),
            context=contextvars.Context(),
        )
        try:
            value = await task
        except JournalExhausted as e:
            return report(outcome="incomplete", stopped_at=e.key,
                                ops_replayed=len(ctx._seen) - 1)
        except NonDeterminismError as e:
            return report(outcome="nondeterministic", error=error_info(e),
                                ops_replayed=len(ctx._seen))
        except Exception as e:  # noqa: BLE001
            err = error_info(e)
            matches = run["status"] == "failed" and (run["error"] or {}).get("type") == err["type"]
            return report(outcome="failed", error=err, matches_recorded=matches,
                                ops_replayed=len(ctx._seen))
        try:
            ctx.assert_journal_consumed()
        except NonDeterminismError as e:
            return report(outcome="nondeterministic", error=error_info(e),
                                ops_replayed=len(ctx._seen))
        value = normalize(value)
        return report(outcome="completed", result=value,
                            matches_recorded=run["status"] == "completed" and run["result"] == value,
                            ops_replayed=len(ctx._seen))

    async def fork(self, run_id: uuid.UUID, *, keep: int | None = None) -> uuid.UUID:
        """Start a new run of the same workflow and arguments that reuses the old
        run's completed journal entries, in write order, up to the first entry
        that did not complete (failed, retrying, waiting) and at most `keep`
        entries. Everything after that runs live, e.g. with fixed code.

        keep=None reuses as much as possible; keep=0 reruns from scratch.
        """
        if keep is not None and keep < 0:
            raise ValueError("keep must be >= 0")
        run = await self.get_run(run_id)
        new_id = uuid.uuid4()
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL("""
                INSERT INTO {0}.runs (id, workflow, queue, args, forked_from)
                SELECT %s, workflow, queue, args, id FROM {0}.runs WHERE id = %s
                """).format(sql.Identifier(self.schema)),
                (new_id, run_id),
            )
            await conn.execute(
                sql.SQL("""
                INSERT INTO {0}.steps (run_id, key, kind, name, status, output, error, attempts)
                SELECT %s, key, kind, name, status, output, error, attempts FROM (
                    SELECT *,
                           -- true while every entry so far (by ord) completed
                           bool_and(status = 'completed') OVER (ORDER BY ord) AS clean,
                           row_number() OVER (ORDER BY ord) AS n
                    FROM {0}.steps WHERE run_id = %s
                ) x
                WHERE clean AND (%s::int IS NULL OR n <= %s)
                ORDER BY ord
                """).format(sql.Identifier(self.schema)),
                (new_id, run["id"], keep, keep),
            )
            await conn.execute("SELECT pg_notify(%s, '')", (self.wake_channel,))
        return new_id


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
        self._slots = asyncio.Semaphore(self.concurrency)
        self.metrics = Metrics()
        self.metrics.set("runs_in_flight", 0)

    # -- public ---------------------------------------------------------------

    async def run(self) -> None:
        """Run until stop() is called (or the task is cancelled)."""
        listener = asyncio.create_task(self._listen(), name="pgreplay-flow-listener")
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
                        await self._slots.acquire()
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
            listener.cancel()
            if purger is not None:
                purger.cancel()
            await self._shutdown(grace=0)
            for t in (listener, purger):
                if t is not None:
                    with contextlib.suppress(asyncio.CancelledError):
                        await t

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
        self._slots.release()
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

    async def _listen(self) -> None:
        backoff = 0.5
        while True:
            try:
                async with await AsyncConnection.connect(self.flow.dsn, autocommit=True) as conn:
                    await conn.execute(
                        sql.SQL("LISTEN {}").format(sql.Identifier(self.flow.wake_channel))
                    )
                    backoff = 0.5
                    self._wake.set()  # may have missed notifications while disconnected
                    async for _ in conn.notifies():
                        self._wake.set()
            except psycopg.OperationalError as e:
                log.warning("listener disconnected: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)

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
