"""WorkflowContext: the deterministic API surface handed to workflow code.

Execution model
---------------
A run is re-executed from the top every time a worker claims it. Each ctx
operation gets a *key* synchronously at call time. If the journal already
holds an outcome for that key it is returned with no side effects; otherwise
the operation runs live and its outcome is committed under the run's lease.

Keys are hierarchical per asyncio task: ops issued by the root workflow
coroutine are "1", "2", ...; child tasks get the prefix "t1/", "t2/", ... in
*creation* order (assigned by a task factory, see install_task_factory), and
ops inside a task are numbered in program order. Keys are therefore
deterministic even when gather()ed branches interleave differently live vs.
on replay.

Suspension
----------
A wait that cannot finish inline (long sleep, retry backoff, signal, child)
*parks*: it records its deadline and blocks. The run is quiescent when no ctx
op is in flight, something is parked, and every live workflow task is idle:
parked itself, or blocked only on other workflow tasks (gather, TaskGroup).
A task in any other await (asyncio.sleep, a socket, a thread) is busy and
keeps the run alive. On quiescence the worker cancels the coroutine and
suspends the run until the earliest parked deadline, so
`gather(sleep(10), sleep(5))` wakes at 5, not 10.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import random as _random
import time
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeVar

from .errors import (
    ChildFailed,
    JournalExhausted,
    LeaseLost,
    NonDeterminismError,
    StepFailed,
    StoreUnavailable,
)
from .retry import RetryPolicy
from .serde import error_info, normalize

if TYPE_CHECKING:
    from .metrics import Metrics
    from .store import RunStore

T = TypeVar("T")

_STEP_INFO: contextvars.ContextVar[StepInfo | None] = contextvars.ContextVar(
    "pgreplay_flow_step_info", default=None
)
_SCOPE: contextvars.ContextVar[_Scope | None] = contextvars.ContextVar(
    "pgreplay_flow_scope", default=None
)

_PASSTHROUGH = (asyncio.CancelledError, LeaseLost, StoreUnavailable, JournalExhausted)

# Child run ids are derived from (parent run id, op key), so re-executing the
# parent after a crash finds the same child instead of starting a second one.
_CHILD_NS = uuid.UUID("6f1c6b4e-5f0e-4d8a-9a51-0c9b1f3e2d77")
_TERMINAL = ("completed", "failed", "cancelled")


@dataclass(frozen=True)
class StepInfo:
    run_id: uuid.UUID
    key: str
    name: str
    attempt: int

    @property
    def idempotency_key(self) -> str:
        """Stable across retries and crash recovery. Pass it to external APIs."""
        return f"{self.run_id}:{self.key}"


def step_info() -> StepInfo:
    """Info about the currently executing step (call from inside a step body)."""
    info = _STEP_INFO.get()
    if info is None:
        raise RuntimeError("step_info() called outside of a step")
    return info


@dataclass(frozen=True)
class StepOptions:
    name: str | None = None
    retry: RetryPolicy | None = None


def step(*, name: str | None = None, retry: RetryPolicy | None = None):
    """Attach step options (stable journal name, retry policy) to a function."""

    def deco(fn):
        fn.__pgreplay_step__ = StepOptions(name=name, retry=retry)
        return fn

    return deco


def _unwrap(fn: Any) -> Any:
    while isinstance(fn, functools.partial):
        fn = fn.func
    return fn


def _step_options(fn: Callable[..., Any]) -> StepOptions:
    f = fn
    while True:
        opts = getattr(f, "__pgreplay_step__", None)
        if opts is not None:
            return opts
        if isinstance(f, functools.partial):
            f = f.func
            continue
        return StepOptions()


def _fn_name(fn: Callable[..., Any]) -> str:
    f = _unwrap(fn)
    return getattr(f, "__qualname__", None) or type(f).__qualname__


async def _invoke(fn: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
    if inspect.iscoroutinefunction(_unwrap(fn)):
        return await fn(*args, **kwargs)
    # Sync callables run in a thread so they don't block the event loop (and the
    # lease heartbeat). to_thread copies contextvars, so step_info() works there.
    res = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(res):
        res = await res
    return res


class _Scope:
    __slots__ = ("children", "counter", "owner", "path")

    def __init__(self, path: str, owner: WorkflowContext):
        self.path = path
        self.owner = owner
        self.counter = 0
        self.children = 0


def install_task_factory(loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Give tasks created inside a workflow a deterministic key scope.

    Idempotent; chains to any task factory already installed on the loop.
    Tasks created outside workflows are untouched.
    """
    loop = loop or asyncio.get_running_loop()
    prev = loop.get_task_factory()
    if getattr(prev, "__pgreplay_flow__", False):
        return

    def factory(loop: asyncio.AbstractEventLoop, coro: Any, **kw: Any) -> asyncio.Future:
        # Use the scope of the context the task will run in: create_task(...,
        # context=contextvars.Context()) opts an internal task out of the workflow.
        given = kw.get("context")
        parent = given.get(_SCOPE) if given is not None else _SCOPE.get()
        if parent is not None:
            ctx = given if given is not None else contextvars.copy_context()
            parent.children += 1
            ctx.run(_SCOPE.set, _Scope(f"{parent.path}t{parent.children}/", parent.owner))
            kw["context"] = ctx
        task = prev(loop, coro, **kw) if prev is not None else asyncio.Task(coro, loop=loop, **kw)
        if parent is not None and isinstance(task, asyncio.Task):
            parent.owner._register_task(task, asyncio.current_task(loop))
        return task

    factory.__pgreplay_flow__ = True  # type: ignore[attr-defined]
    loop.set_task_factory(factory)


class WorkflowContext:
    def __init__(
        self,
        *,
        run_id: uuid.UUID,
        workflow: str,
        journal: dict[str, dict[str, Any]],
        store: RunStore | None,
        default_retry: RetryPolicy,
        inline_wait_max: float = 1.0,
        clock: Callable[[], float] = time.time,
        resolve: Callable[[Any], tuple[str, str]] | None = None,
        metrics: Metrics | None = None,
    ):
        self.run_id = run_id
        self.workflow = workflow
        self._journal = journal
        self._store = store
        self._default_retry = default_retry
        self._inline_wait_max = inline_wait_max
        self._clock = clock
        self._resolve = resolve
        self._metrics = metrics
        self._root: _Scope | None = None
        self._seen: set[str] = set()
        # Quiescence tracking (live mode only).
        self._pending = 0
        self._parked: dict[str, tuple[str, float | None]] = {}
        self._park_futures: set[asyncio.Future] = set()
        # Live workflow tasks -> the task that created them.
        self._tasks: dict[asyncio.Task, asyncio.Task | None] = {}
        self._confirm_task: asyncio.Task | None = None
        self._closed = False
        self.quiescent = asyncio.Event()

    # -- driving ------------------------------------------------------------

    async def _main(self, fn: Callable[..., Awaitable[Any]], args: list, kwargs: dict) -> Any:
        install_task_factory()
        self._root = _Scope("", self)
        _SCOPE.set(self._root)
        root = asyncio.current_task()
        if root is not None:
            self._register_task(root, None)
        return await fn(self, *args, **kwargs)

    def _register_task(self, task: asyncio.Task, parent: asyncio.Task | None) -> None:
        self._tasks[task] = parent
        task.add_done_callback(self._tasks.pop)  # type: ignore[arg-type]

    def close(self) -> None:
        """Called before the workflow coroutine is cancelled for suspension: any
        ctx op issued afterwards (e.g. from a finally: block) must not run live."""
        self._closed = True

    def suspension(self) -> tuple[str, float | None]:
        """(status, wake_at) for a quiescent run. wake_at None = signal only."""
        kinds = {k for k, _ in self._parked.values()}
        deadlines = [u for _, u in self._parked.values() if u is not None]
        status = "waiting" if kinds & {"signal", "child"} else "sleeping"
        return status, (min(deadlines) if deadlines else None)

    def assert_journal_consumed(self) -> None:
        extra = sorted(set(self._journal) - self._seen)
        if extra:
            raise NonDeterminismError(
                f"run {self.run_id}: workflow returned but journal entries {extra} "
                "were never reached by the current code"
            )

    @property
    def is_replaying(self) -> bool:
        """True while re-executing recorded history (e.g. to suppress duplicate logs)."""
        return self._store is None or len(self._seen) < len(self._journal)

    # -- key allocation and op tracking --------------------------------------

    def _alloc(self) -> str:
        if self._closed:
            raise asyncio.CancelledError("workflow is suspending")
        scope = _SCOPE.get()
        if scope is None or self._root is None:
            raise RuntimeError("ctx operations must be called from within the workflow")
        scope.counter += 1
        key = f"{scope.path}{scope.counter}"
        self._seen.add(key)
        self._pending += 1
        return key

    def _track(self, key: str, coro: Coroutine[Any, Any, T]) -> Coroutine[Any, Any, T]:
        async def run() -> T:
            # While the op runs (step body, store I/O) its task is blocked, so no
            # workflow-level ops can be issued from it. Clear the scope so tasks
            # spawned by step bodies or the driver don't consume child-scope
            # numbers -- they don't exist on replay.
            scope_token = _SCOPE.set(None)
            try:
                return await coro
            finally:
                _SCOPE.reset(scope_token)
                if key not in self._parked:
                    self._op_done()

        return run()

    def _op_done(self) -> None:
        self._pending -= 1
        if self._pending == 0 and self._parked and self._confirm_task is None:
            # Empty context: this internal task must not consume a child scope.
            self._confirm_task = asyncio.get_running_loop().create_task(
                self._confirm(), context=contextvars.Context()
            )

    async def _confirm(self) -> None:
        # Let freshly scheduled tasks (e.g. gather children) start and allocate,
        # then keep re-checking while a branch is busy outside ctx: a busy task
        # may later become idle without issuing an op (e.g. it starts awaiting
        # siblings that are already parked), so polling is the only trigger.
        try:
            for _ in range(3):
                await asyncio.sleep(0)
            while self._pending == 0 and self._parked and not self._closed:
                if self._all_tasks_idle():
                    self.quiescent.set()
                    return
                await asyncio.sleep(0.05)
        finally:
            self._confirm_task = None

    def _all_tasks_idle(self) -> bool:
        return all(self._task_idle(t) for t in list(self._tasks) if not t.done())

    def _task_idle(self, task: asyncio.Task) -> bool:
        """Is this workflow task blocked only on ctx waits or on other workflow
        tasks? `_fut_waiter` is a CPython implementation detail (present in
        3.11-3.14); without it we fall back to counting ctx ops only."""
        if not hasattr(task, "_fut_waiter"):
            return True
        waiter = task._fut_waiter  # type: ignore[attr-defined]
        if waiter is None:
            return False  # runnable
        if waiter in self._park_futures or waiter in self._tasks:
            return True
        children = getattr(waiter, "_children", None)  # asyncio.gather()
        if children is not None:
            return all(c.done() or c in self._tasks for c in children)
        # A plain future. TaskGroup.__aexit__ waits on one of these until its
        # children finish: idle iff the task has live workflow children.
        return any(p is task and not c.done() for c, p in self._tasks.items())

    async def _park(self, key: str, kind: str, until: float | None) -> Any:
        self._parked[key] = (kind, until)
        fut = asyncio.get_running_loop().create_future()
        self._park_futures.add(fut)
        self._op_done()
        await fut  # resolved only by cancellation

    def _entry(self, key: str, kind: str, name: str) -> dict[str, Any] | None:
        entry = self._journal.get(key)
        if entry is not None and (entry["kind"] != kind or entry["name"] != name):
            raise NonDeterminismError(
                f"run {self.run_id} op {key}: code issued {kind} {name!r} "
                f"but the journal recorded {entry['kind']} {entry['name']!r}"
            )
        return entry

    def _live(self, key: str) -> RunStore:
        if self._store is None:
            raise JournalExhausted(key)
        return self._store

    async def _wait_until(self, key: str, kind: str, until: float | None) -> None:
        if self._store is None:  # offline replay: recorded waits have already fired
            return
        if until is not None:
            remaining = until - self._clock()
            if remaining <= 0:
                return
            if remaining <= self._inline_wait_max:
                await asyncio.sleep(remaining)
                return
        await self._park(key, kind, until)

    # -- steps --------------------------------------------------------------

    def step(self, fn: Callable[..., Awaitable[T] | T], /, *args: Any, **kwargs: Any) -> Awaitable[T]:
        """Run `fn(*args, **kwargs)` as a durable step; its JSON result is journaled.

        The body runs at-least-once (it re-runs if the worker dies before the
        result commits); the result is recorded exactly once. Use
        step_info().idempotency_key for external side effects.
        """
        key = self._alloc()
        opts = _step_options(fn)
        name = opts.name or _fn_name(fn)
        retry = opts.retry or self._default_retry

        async def call() -> Any:
            return normalize(await _invoke(fn, args, kwargs))

        return self._track(key, self._run_step(key, "step", name, retry, call, tx=None))

    def tx_step(
        self, fn: Callable[..., Awaitable[T]], /, *args: Any, **kwargs: Any
    ) -> Awaitable[T]:
        """Run `await fn(conn, *args, **kwargs)` in the same Postgres transaction
        that records the step result: database effects are exactly-once."""
        key = self._alloc()
        opts = _step_options(fn)
        name = opts.name or _fn_name(fn)
        retry = opts.retry or self._default_retry
        return self._track(key, self._run_step(key, "tx_step", name, retry, None, (fn, args, kwargs)))

    async def _run_step(
        self,
        key: str,
        kind: str,
        name: str,
        retry: RetryPolicy,
        call: Callable[[], Awaitable[Any]] | None,
        tx: tuple[Callable[..., Any], tuple, dict] | None,
    ) -> Any:
        entry = self._entry(key, kind, name)
        attempts = 0
        if entry is not None:
            if entry["status"] == "completed":
                return entry["output"]
            if entry["status"] == "failed":
                raise StepFailed(name, entry["error"] or {})
            attempts = entry["attempts"]
        store = self._live(key)

        if entry is not None and entry["status"] == "pending":
            # A previous owner started attempt N and vanished before recording an
            # outcome. Count it as a failed attempt so a step that crashes the
            # process (OOM, segfault) cannot loop forever.
            crash = {"type": "WorkerCrashed", "message": f"worker lost during attempt {attempts}"}
            if attempts >= retry.max_attempts:
                await store.put_step(key, kind, name, "failed", attempts, error=crash)
                raise StepFailed(name, crash)
        elif entry is not None and entry["status"] == "retrying":
            # Woken before this step's backoff elapsed (e.g. by a sibling branch).
            await self._wait_until(key, "retry", (entry["output"] or {}).get("retry_at"))

        while True:
            attempts += 1
            token = _STEP_INFO.set(StepInfo(self.run_id, key, name, attempts))
            exc: Exception | None = None
            t0 = time.perf_counter()
            try:
                if tx is not None:
                    fn, args, kwargs = tx
                    out, exc = await store.run_tx_step(key, kind, name, attempts, fn, args, kwargs)
                    if exc is None:
                        self._attempt_done(name, "completed", t0)
                        return out
                else:
                    assert call is not None
                    await store.put_step(key, kind, name, "pending", attempts)
                    try:
                        out = await call()
                    except _PASSTHROUGH:
                        raise
                    except Exception as e:  # noqa: BLE001 - step bodies may raise anything
                        exc = e
                    else:
                        await store.put_step(key, kind, name, "completed", attempts, output=out)
                        self._attempt_done(name, "completed", t0)
                        return out
            finally:
                _STEP_INFO.reset(token)

            err = error_info(exc)
            if retry.should_retry(attempts, exc):
                self._attempt_done(name, "retrying", t0)
                retry_at = self._clock() + retry.delay(attempts)
                await store.put_step(
                    key, kind, name, "retrying", attempts, error=err, output={"retry_at": retry_at}
                )
                await self._wait_until(key, "retry", retry_at)
                continue
            self._attempt_done(name, "failed", t0)
            await store.put_step(key, kind, name, "failed", attempts, error=err)
            raise StepFailed(name, err) from exc

    def _attempt_done(self, name: str, outcome: str, t0: float) -> None:
        if self._metrics is not None:
            labels = {"workflow": self.workflow, "step": name}
            self._metrics.inc("step_attempts_total", outcome=outcome, **labels)
            self._metrics.observe("step_attempt_seconds", time.perf_counter() - t0, **labels)

    # -- child workflows ----------------------------------------------------

    def _resolve_child(self, workflow: Any) -> tuple[str, str]:
        if self._resolve is None:
            raise RuntimeError("child workflows need a Flow-backed context")
        return self._resolve(workflow)

    def child(self, workflow: Any, /, *args: Any, **kwargs: Any) -> Awaitable[Any]:
        """Run a registered workflow as a child run and wait for its result.

        The child is an ordinary run (any worker may execute it, it has its own
        journal and retries). The parent is released while it waits. If the
        child fails or is cancelled, ChildFailed (a StepFailed) is raised here.
        Cancelling the parent cancels children it is waiting on.
        """
        name, queue = self._resolve_child(workflow)
        key = self._alloc()
        return self._track(key, self._child(key, name, queue, args, kwargs, wait=True))

    def start_child(self, workflow: Any, /, *args: Any, **kwargs: Any) -> Awaitable[uuid.UUID]:
        """Start a registered workflow as a detached child and return its run id
        without waiting. Detached children outlive the parent's cancellation."""
        name, queue = self._resolve_child(workflow)
        key = self._alloc()
        return self._track(key, self._child(key, name, queue, args, kwargs, wait=False))

    async def _child(
        self, key: str, name: str, queue: str, args: tuple, kwargs: dict, *, wait: bool
    ) -> Any:
        kind = "child" if wait else "child_start"
        entry = self._entry(key, kind, name)
        if entry is not None and entry["status"] == "completed":
            out = entry["output"]
            return out["result"] if wait else uuid.UUID(out["child_id"])
        if entry is not None and entry["status"] == "failed":
            child_id = uuid.UUID((entry["output"] or {})["child_id"])
            raise ChildFailed(name, child_id, entry["error"] or {})
        store = self._live(key)

        if entry is None:
            child_id = uuid.uuid5(_CHILD_NS, f"{self.run_id}:{key}")
            payload = normalize({"args": list(args), "kwargs": kwargs})
            await store.start_child(key, kind, name, queue, child_id, payload)
            if not wait:
                return child_id
        else:
            child_id = uuid.UUID(entry["output"]["child_id"])

        outcome = await store.child_outcome(child_id)
        if outcome is None or outcome[0] in _TERMINAL:
            status, result, error = outcome or ("failed", None, None)
            ref = {"child_id": str(child_id)}
            if status == "completed":
                await store.put_step(key, kind, name, "completed", 1,
                                     output={**ref, "result": result})
                return result
            if outcome is None:
                error = {"type": "ChildMissing", "message": f"child run {child_id} not found"}
            elif status == "cancelled":
                error = {"type": "WorkflowCancelled", "message": f"child run {child_id} was cancelled"}
            await store.put_step(key, kind, name, "failed", 1, output=ref, error=error)
            raise ChildFailed(name, child_id, error or {})

        # Not finished: park. The child's completion wakes this run (see
        # RunStore.finish), and re-execution lands back here.
        await self._park(key, "child", None)

    # -- durable timers -----------------------------------------------------

    def sleep(self, duration: float | timedelta) -> Awaitable[None]:
        """Durable sleep. Long sleeps release the worker; the run resumes later."""
        secs = duration.total_seconds() if isinstance(duration, timedelta) else float(duration)
        key = self._alloc()
        return self._track(key, self._sleep(key, lambda: self._clock() + secs))

    def sleep_until(self, when: datetime) -> Awaitable[None]:
        if when.tzinfo is None:
            raise ValueError("sleep_until requires a timezone-aware datetime")
        key = self._alloc()
        return self._track(key, self._sleep(key, when.timestamp))

    async def _sleep(self, key: str, until_fn: Callable[[], float]) -> None:
        entry = self._entry(key, "sleep", "sleep")
        if entry is None:
            store = self._live(key)
            until = until_fn()
            await store.put_step(key, "sleep", "sleep", "completed", 1, output={"until": until})
        else:
            until = entry["output"]["until"]
        await self._wait_until(key, "sleep", until)

    # -- signals ------------------------------------------------------------

    def wait_for_signal(
        self, name: str, *, timeout: float | timedelta | None = None
    ) -> Awaitable[Any]:
        """Wait for Flow.send_signal(run_id, name, payload). Returns the payload,
        or None on timeout. Signals are buffered, so early sends are not lost."""
        secs = timeout.total_seconds() if isinstance(timeout, timedelta) else timeout
        key = self._alloc()
        return self._track(key, self._wait_signal(key, name, secs))

    async def _wait_signal(self, key: str, name: str, timeout: float | None) -> Any:
        entry = self._entry(key, "signal", name)
        if entry is not None and entry["status"] == "completed":
            return entry["output"]["payload"]
        store = self._live(key)
        if entry is None:
            deadline = self._clock() + timeout if timeout is not None else None
            await store.put_step(key, "signal", name, "pending", 1, output={"deadline": deadline})
        else:
            deadline = entry["output"]["deadline"]

        found, payload = await store.consume_signal(key, name)
        if found:
            return payload
        if deadline is None or self._clock() < deadline:
            await self._park(key, "signal", deadline)
        await store.put_step(
            key, "signal", name, "completed", 1, output={"payload": None, "timed_out": True}
        )
        return None

    # -- recorded non-determinism --------------------------------------------

    def _record(self, kind: str, produce: Callable[[], Any]) -> Awaitable[Any]:
        key = self._alloc()

        async def run() -> Any:
            entry = self._entry(key, kind, kind)
            if entry is not None:
                return entry["output"]
            value = normalize(produce())
            await self._live(key).put_step(key, kind, kind, "completed", 1, output=value)
            return value

        return self._track(key, run())

    async def now(self) -> datetime:
        """Current UTC time, recorded so replay sees the same value."""
        return datetime.fromtimestamp(await self._record("now", self._clock), tz=UTC)

    async def uuid4(self) -> uuid.UUID:
        return uuid.UUID(await self._record("uuid", lambda: str(uuid.uuid4())))

    async def random(self) -> float:
        return await self._record("random", _random.random)


__all__ = ["StepInfo", "WorkflowContext", "install_task_factory", "step", "step_info"]
