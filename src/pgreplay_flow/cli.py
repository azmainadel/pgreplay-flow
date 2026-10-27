"""Command-line interface: `pgreplay-flow <command>`.

Most commands only need the database. `worker`, `replay` and `start` need your
workflow code too: pass `--app module:attr` pointing at your Flow object
(e.g. `--app myapp.flows:flow`). The current directory is on the import path.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import json
import logging
import os
import signal
import sys
import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from .errors import FlowError, RunNotFound, WorkflowCancelled, WorkflowFailed
from .flow import Flow, Worker
from .metrics import render_stats, serve_metrics

DEFAULT_SCHEMA = "pgreplay_flow"


class CliError(Exception):
    """User-facing error: printed without a traceback, exit code 2."""


# -- helpers ------------------------------------------------------------------


def _parse_duration(text: str) -> float:
    """'90', '90s', '15m', '12h', '30d' -> seconds."""
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    t = text.strip().lower()
    try:
        value = float(t[:-1]) * units[t[-1]] if t and t[-1] in units else float(t)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid duration {text!r} (use e.g. 30s, 15m, 7d)") from None
    if value < 0:
        raise argparse.ArgumentTypeError("duration must be >= 0")
    return value


def _parse_uuid(text: str) -> uuid.UUID:
    try:
        return uuid.UUID(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a run id: {text!r}") from None


def _parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"invalid JSON: {e}") from None


def _load_app(spec: str) -> Flow:
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise CliError(f"--app must look like 'module:attr', got {spec!r}")
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise CliError(f"cannot import {module_name!r}: {e}") from e
    obj: Any = module
    for part in attr.split("."):
        obj = getattr(obj, part, None)
        if obj is None:
            raise CliError(f"{module_name!r} has no attribute {attr!r}")
    if not isinstance(obj, Flow):
        raise CliError(f"{spec!r} is a {type(obj).__name__}, not a pgreplay_flow.Flow")
    return obj


def _make_flow(args: argparse.Namespace, *, need_app: bool = False) -> Flow:
    if args.app:
        # Never reuse the app's own Flow: its pool may already be open (or
        # closed) in this process. Build a fresh one sharing the registry.
        return _rebind(_load_app(args.app), args)
    if need_app:
        raise CliError(f"'{args.command}' needs --app module:attr (or PGREPLAY_FLOW_APP)")
    if not args.dsn:
        raise CliError("no database: pass --dsn or set DATABASE_URL")
    return Flow(args.dsn, schema=args.schema)


def _rebind(app: Flow, args: argparse.Namespace) -> Flow:
    # Explicit CLI connection settings override what the app was built with.
    flow = Flow(args.dsn if args.dsn_explicit else app.dsn,
                schema=args.schema if args.schema_explicit else app.schema,
                default_retry=app.default_retry)
    flow.workflows.update(app.workflows)
    return flow


def _fmt_time(v: datetime | None) -> str:
    return "-" if v is None else v.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [max(len(str(c)) for c in col) for col in zip(headers, *rows, strict=False)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=False))
    body = ["  ".join(str(c).ljust(w) for c, w in zip(r, widths, strict=False)) for r in rows]
    return "\n".join([line, *body])


def _jsonable(v: Any) -> Any:
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def _print_json(v: Any) -> None:
    print(json.dumps(_jsonable(v), indent=2, sort_keys=True))


def _short(v: Any, n: int = 60) -> str:
    text = json.dumps(_jsonable(v)) if v is not None else "-"
    return text if len(text) <= n else text[: n - 1] + "…"


# -- commands -----------------------------------------------------------------


async def cmd_migrate(flow: Flow, args: argparse.Namespace) -> int:
    await flow.migrate()
    print(f"schema {flow.schema!r} is up to date")
    return 0


async def cmd_list(flow: Flow, args: argparse.Namespace) -> int:
    runs = await flow.list_runs(status=args.status, workflow=args.workflow,
                                parent_id=args.parent, limit=args.limit)
    if args.json:
        _print_json(runs)
        return 0
    if not runs:
        print("no runs")
        return 0
    rows = [[str(r["id"]), r["workflow"], r["status"], str(r["claims"]), str(r["recoveries"]),
             _fmt_time(r["created_at"]), _fmt_time(r["finished_at"])] for r in runs]
    print(_table(rows, ["ID", "WORKFLOW", "STATUS", "CLAIMS", "RECOVERED", "CREATED", "FINISHED"]))
    return 0


async def cmd_status(flow: Flow, args: argparse.Namespace) -> int:
    run = await flow.get_run(args.run_id)
    journal = await flow.get_journal(args.run_id)
    children = await flow.list_runs(parent_id=args.run_id, limit=1000)
    if args.json:
        _print_json({"run": run, "journal": journal, "children": children})
        return 0
    fields = [
        ("id", run["id"]), ("workflow", run["workflow"]), ("queue", run["queue"]),
        ("status", run["status"]), ("created", _fmt_time(run["created_at"])),
        ("finished", _fmt_time(run["finished_at"])), ("claims", run["claims"]),
        ("recoveries", run["recoveries"]),
    ]
    if run["status"] in ("sleeping", "waiting", "pending"):
        fields.append(("wakes", _fmt_time(run["ready_at"]) if run["ready_at"] else "on signal/child"))
    if run["status"] == "running":
        fields.append(("owner", run["lease_owner"]))
        fields.append(("lease until", _fmt_time(run["lease_expires_at"])))
    if run["parent_id"]:
        fields.append(("parent", f"{run['parent_id']} ({'attached' if run['attached'] else 'detached'})"))
    if run["forked_from"]:
        fields.append(("forked from", run["forked_from"]))
    fields.append(("args", _short(run["args"], 200)))
    if run["status"] == "completed":
        fields.append(("result", _short(run["result"], 200)))
    if run["error"]:
        fields.append(("error", f"{run['error'].get('type')}: {run['error'].get('message')}"))
    w = max(len(k) for k, _ in fields)
    for k, v in fields:
        print(f"{k.rjust(w)}  {v}")
    if journal:
        print()
        rows = [[e["key"], e["kind"], e["name"], e["status"], str(e["attempts"]),
                 _short(e["error"]["message"] if e["error"] else e["output"], 50)]
                for e in journal]
        print(_table(rows, ["KEY", "KIND", "NAME", "STATUS", "ATTEMPTS", "OUTPUT/ERROR"]))
    if children:
        print()
        rows = [[str(c["id"]), c["workflow"], c["status"]] for c in children]
        print(_table(rows, ["CHILD", "WORKFLOW", "STATUS"]))
    return 0


async def cmd_start(flow: Flow, args: argparse.Namespace) -> int:
    run_id = await flow.start(args.workflow, *args.args, idempotency_key=args.idempotency_key,
                              queue=args.queue)
    if not args.wait:
        print(run_id)
        return 0
    print(run_id, file=sys.stderr)
    return await _print_result(flow, run_id, args.timeout)


async def cmd_result(flow: Flow, args: argparse.Namespace) -> int:
    return await _print_result(flow, args.run_id, args.timeout)


async def _print_result(flow: Flow, run_id: uuid.UUID, timeout: float | None) -> int:
    try:
        _print_json(await flow.result(run_id, timeout=timeout))
        return 0
    except WorkflowFailed as e:
        print(f"failed: {e.error.get('type')}: {e.error.get('message')}", file=sys.stderr)
        return 1
    except WorkflowCancelled:
        print("cancelled", file=sys.stderr)
        return 1
    except TimeoutError as e:
        print(f"timeout: {e}", file=sys.stderr)
        return 3


async def cmd_signal(flow: Flow, args: argparse.Namespace) -> int:
    await flow.send_signal(args.run_id, args.name, args.payload)
    print(f"sent {args.name!r} to {args.run_id}")
    return 0


async def cmd_cancel(flow: Flow, args: argparse.Namespace) -> int:
    if await flow.cancel(args.run_id):
        print(f"cancelled {args.run_id}")
        return 0
    print(f"{args.run_id} had already finished", file=sys.stderr)
    return 1


async def cmd_replay(flow: Flow, args: argparse.Namespace) -> int:
    r = await flow.replay(args.run_id)
    if args.json:
        _print_json(r.__dict__)
    else:
        print(f"outcome: {r.outcome} (recorded: {r.recorded_status}, "
              f"ops replayed: {r.ops_replayed}, matches recorded: {r.matches_recorded})")
        if r.stopped_at:
            print(f"stopped at op {r.stopped_at}: no recorded outcome yet")
        if r.error:
            print(f"error: {r.error.get('type')}: {r.error.get('message')}")
        if r.outcome == "completed":
            print(f"result: {_short(r.result, 500)}")
    return 1 if r.outcome == "nondeterministic" else 0


async def cmd_fork(flow: Flow, args: argparse.Namespace) -> int:
    print(await flow.fork(args.run_id, keep=args.keep))
    return 0


async def cmd_purge(flow: Flow, args: argparse.Namespace) -> int:
    statuses = tuple(args.status) if args.status else ("completed", "failed", "cancelled")
    if args.dry_run:
        n = await flow.count_purgeable(args.older_than, workflow=args.workflow, statuses=statuses)
        print(f"would delete {n} run(s)")
        return 0
    n = await flow.purge(args.older_than, workflow=args.workflow, statuses=statuses)
    print(f"deleted {n} run(s)")
    return 0


async def cmd_stats(flow: Flow, args: argparse.Namespace) -> int:
    stats = await flow.stats()
    if args.format == "prometheus":
        sys.stdout.write(render_stats(stats))
        return 0
    if args.format == "json":
        _print_json(stats)
        return 0
    if not stats["runs"]:
        print("no runs")
    else:
        rows = [[r["queue"], r["workflow"], r["status"], str(r["count"])] for r in stats["runs"]]
        print(_table(rows, ["QUEUE", "WORKFLOW", "STATUS", "COUNT"]))
    print()
    for q, lag in sorted(stats["queue_lag_seconds"].items()):
        print(f"queue {q!r}: oldest runnable run waiting {lag:.1f}s")
    print(f"expired leases (awaiting recovery): {stats['expired_leases']}")
    return 0


async def cmd_worker(flow: Flow, args: argparse.Namespace) -> int:
    worker = Worker(
        flow, queues=tuple(args.queue or ("default",)), concurrency=args.concurrency,
        lease_seconds=args.lease, purge_after=args.purge_after,
    )
    server = None
    if args.metrics_port is not None:
        server = await serve_metrics(flow, worker, host=args.metrics_host, port=args.metrics_port)
        print(f"metrics on http://{args.metrics_host}:{args.metrics_port}/metrics", file=sys.stderr)
        if args.metrics_host not in ("127.0.0.1", "localhost", "::1"):
            print("warning: the metrics endpoint has no authentication", file=sys.stderr)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows
            loop.add_signal_handler(sig, stop.set)

    names = ", ".join(sorted(flow.workflows)) or "(none registered!)"
    print(f"worker {worker.worker_id} on queues {list(worker.queues)}; workflows: {names}",
          file=sys.stderr)
    task = asyncio.create_task(worker.run())
    stopper = asyncio.create_task(stop.wait())
    await asyncio.wait({task, stopper}, return_when=asyncio.FIRST_COMPLETED)
    if stop.is_set():
        print("shutting down…", file=sys.stderr)
        await worker.stop(grace=args.grace)
    await task
    stopper.cancel()
    if server is not None:
        server.close()
        await server.wait_closed()
    return 0


# -- argument parsing ------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pgreplay-flow", description="Inspect and operate pgreplay-flow workflows."
    )
    p.add_argument("--dsn", default=None,
                   help="Postgres URL (default: $DATABASE_URL)")
    p.add_argument("--schema", default=None,
                   help=f"schema holding the tables (default: $PGREPLAY_FLOW_SCHEMA or {DEFAULT_SCHEMA})")
    p.add_argument("--app", default=os.environ.get("PGREPLAY_FLOW_APP"),
                   help="your Flow object as module:attr (default: $PGREPLAY_FLOW_APP)")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = p.add_subparsers(dest="command", required=True, metavar="COMMAND")

    def cmd(name: str, fn: Any, help_text: str, *, app: bool = False) -> argparse.ArgumentParser:
        sp = sub.add_parser(name, help=help_text, description=help_text)
        sp.set_defaults(fn=fn, need_app=app)
        return sp

    cmd("migrate", cmd_migrate, "create or upgrade the tables")

    sp = cmd("list", cmd_list, "list recent runs")
    sp.add_argument("--status", choices=["pending", "running", "sleeping", "waiting",
                                         "completed", "failed", "cancelled"])
    sp.add_argument("--workflow")
    sp.add_argument("--parent", type=_parse_uuid, help="only children of this run")
    sp.add_argument("-n", "--limit", type=int, default=50)
    sp.add_argument("--json", action="store_true")

    sp = cmd("status", cmd_status, "show one run with its journal and children")
    sp.add_argument("run_id", type=_parse_uuid)
    sp.add_argument("--json", action="store_true")

    sp = cmd("start", cmd_start, "start a run (arguments are JSON values)", app=True)
    sp.add_argument("workflow")
    sp.add_argument("args", nargs="*", type=_parse_json, help='e.g. 42 \'"text"\' \'{"a": 1}\'')
    sp.add_argument("--idempotency-key")
    sp.add_argument("--queue")
    sp.add_argument("--wait", action="store_true", help="wait and print the result")
    sp.add_argument("--timeout", type=_parse_duration)

    sp = cmd("result", cmd_result, "wait for a run and print its result as JSON")
    sp.add_argument("run_id", type=_parse_uuid)
    sp.add_argument("--timeout", type=_parse_duration)

    sp = cmd("signal", cmd_signal, "send a signal to a run")
    sp.add_argument("run_id", type=_parse_uuid)
    sp.add_argument("name")
    sp.add_argument("payload", nargs="?", type=_parse_json, default=None, help="JSON value")

    sp = cmd("cancel", cmd_cancel, "cancel a run and the children it waits on")
    sp.add_argument("run_id", type=_parse_uuid)

    sp = cmd("replay", cmd_replay, "re-run a run's code against its journal, no side effects",
             app=True)
    sp.add_argument("run_id", type=_parse_uuid)
    sp.add_argument("--json", action="store_true")

    sp = cmd("fork", cmd_fork, "copy a run's journal into a new run and resume it")
    sp.add_argument("run_id", type=_parse_uuid)
    sp.add_argument("--keep", type=int,
                    help="reuse at most the first N completed entries (0 = rerun everything)")

    sp = cmd("purge", cmd_purge, "delete finished runs older than a duration")
    sp.add_argument("--older-than", type=_parse_duration, required=True, help="e.g. 30d, 12h")
    sp.add_argument("--status", action="append", choices=["completed", "failed", "cancelled"],
                    help="repeatable; default: all terminal statuses")
    sp.add_argument("--workflow")
    sp.add_argument("--dry-run", action="store_true")

    sp = cmd("stats", cmd_stats, "run counts, queue lag and expired leases")
    sp.add_argument("--format", choices=["table", "json", "prometheus"], default="table")

    sp = cmd("worker", cmd_worker, "run a worker until SIGINT/SIGTERM", app=True)
    sp.add_argument("-q", "--queue", action="append", help="repeatable; default: default")
    sp.add_argument("-c", "--concurrency", type=int, default=10)
    sp.add_argument("--lease", type=_parse_duration, default=30.0)
    sp.add_argument("--grace", type=_parse_duration, default=10.0,
                    help="time to let in-flight runs finish on shutdown")
    sp.add_argument("--purge-after", type=_parse_duration,
                    help="also delete runs that finished longer ago than this")
    sp.add_argument("--metrics-port", type=int, help="serve Prometheus metrics on this port")
    sp.add_argument("--metrics-host", default="127.0.0.1")
    return p


async def _amain(args: argparse.Namespace) -> int:
    flow = _make_flow(args, need_app=args.need_app)
    # Only `migrate` and `worker` change the schema; inspection commands must
    # not create tables in a database that was never set up.
    await flow.open(migrate=args.command in ("migrate", "worker"))
    try:
        return await args.fn(flow, args)
    finally:
        await flow.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.dsn_explicit = args.dsn is not None
    args.schema_explicit = args.schema is not None
    args.dsn = args.dsn or os.environ.get("DATABASE_URL")
    args.schema = args.schema or os.environ.get("PGREPLAY_FLOW_SCHEMA", DEFAULT_SCHEMA)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        return asyncio.run(_amain(args))
    except (CliError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except RunNotFound as e:
        print(f"error: run not found: {e}", file=sys.stderr)
        return 2
    except FlowError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        # psycopg errors (bad DSN, missing schema) without a wall of traceback.
        import psycopg

        if isinstance(e, psycopg.Error):
            msg = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
            if isinstance(e, psycopg.errors.UndefinedTable):
                msg += " (run `pgreplay-flow migrate` first?)"
            print(f"database error: {msg}", file=sys.stderr)
            return 2
        raise


if __name__ == "__main__":
    sys.exit(main())
