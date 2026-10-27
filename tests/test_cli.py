"""CLI tests: run `main()` in a worker thread (it calls asyncio.run itself)."""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest

from pgreplay_flow.cli import _parse_duration, main
from tests.conftest import DSN


@pytest.fixture
def app_module(tmp_path, schema, monkeypatch):
    """A throwaway app module with a Flow and two workflows."""
    mod = f"cli_app_{uuid.uuid4().hex[:8]}"
    (tmp_path / f"{mod}.py").write_text(textwrap.dedent(f"""
        from pgreplay_flow import Flow

        flow = Flow({DSN!r}, schema={schema!r})

        @flow.workflow(name="double")
        async def double(ctx, x):
            return await ctx.step(lambda: x * 2)

        @flow.workflow(name="gate")
        async def gate(ctx):
            return await ctx.wait_for_signal("open")
    """))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", DSN)
    monkeypatch.setenv("PGREPLAY_FLOW_SCHEMA", schema)
    monkeypatch.setenv("PGREPLAY_FLOW_APP", f"{mod}:flow")
    yield mod
    sys.modules.pop(mod, None)


async def cli(capsys, *argv):
    code = await asyncio.to_thread(main, list(argv))
    out, err = capsys.readouterr()
    if code == 2:
        print(err, file=sys.__stderr__)  # surface usage errors when debugging
    return code, out, err


def test_parse_duration():
    assert _parse_duration("90") == 90
    assert _parse_duration("15m") == 900
    assert _parse_duration("2h") == 7200
    assert _parse_duration("7d") == 7 * 86400
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_duration("soon")


async def test_end_to_end_commands(flow, start_worker, app_module, capsys):
    code, out, _ = await cli(capsys, "migrate")
    assert code == 0 and "up to date" in out

    code, out, _ = await cli(capsys, "start", "double", "21")
    assert code == 0
    run_id = uuid.UUID(out.strip())

    flow.workflows.update(sys.modules[app_module].flow.workflows)
    start_worker()

    code, out, _ = await cli(capsys, "result", str(run_id), "--timeout", "10s")
    assert code == 0 and json.loads(out) == 42

    code, out, _ = await cli(capsys, "status", str(run_id))
    assert code == 0
    assert "completed" in out and "KEY" in out and "double" in out

    code, out, _ = await cli(capsys, "status", str(run_id), "--json")
    data = json.loads(out)
    assert data["run"]["result"] == 42 and len(data["journal"]) == 1

    code, out, _ = await cli(capsys, "list", "--status", "completed")
    assert code == 0 and str(run_id) in out

    code, out, _ = await cli(capsys, "replay", str(run_id))
    assert code == 0 and "outcome: completed" in out and "matches recorded: True" in out

    code, out, _ = await cli(capsys, "start", "gate")
    gate_id = out.strip()
    code, out, _ = await cli(capsys, "signal", gate_id, "open", '{"by": "cli"}')
    assert code == 0
    code, out, _ = await cli(capsys, "result", gate_id, "--timeout", "10")
    assert json.loads(out) == {"by": "cli"}

    code, out, _ = await cli(capsys, "stats")
    assert code == 0 and "double" in out and "expired leases" in out
    code, out, _ = await cli(capsys, "stats", "--format", "prometheus")
    assert 'pgreplay_flow_runs{queue="default",status="completed",workflow="double"} 1' in out

    code, out, _ = await cli(capsys, "fork", str(run_id))
    fork_id = out.strip()
    code, out, _ = await cli(capsys, "result", fork_id, "--timeout", "10")
    assert json.loads(out) == 42

    code, out, _ = await cli(capsys, "purge", "--older-than", "1d", "--dry-run")
    assert code == 0 and "would delete about 0" in out
    code, out, _ = await cli(capsys, "purge", "--older-than", "0s")
    assert code == 0 and "deleted 3 run(s)" in out


async def test_cancel_and_errors(flow, app_module, capsys):
    code, out, _ = await cli(capsys, "start", "gate", "--idempotency-key", "k1")
    run_id = out.strip()
    code, out2, _ = await cli(capsys, "start", "gate", "--idempotency-key", "k1")
    assert out2.strip() == run_id

    code, out, _ = await cli(capsys, "cancel", run_id)
    assert code == 0
    code, _, err = await cli(capsys, "cancel", run_id)
    assert code == 1 and "already finished" in err
    code, _, err = await cli(capsys, "result", run_id)
    assert code == 1 and "cancelled" in err

    code, _, err = await cli(capsys, "status", str(uuid.uuid4()))
    assert code == 2 and "not found" in err
    code, _, err = await cli(capsys, "start", "nope")
    assert code == 1 and "not registered" in err
    code, _, err = await cli(capsys, "--app", "missing_mod:flow", "start", "x")
    assert code == 2 and "cannot import" in err


def test_inspection_without_tables_does_not_create_schema(monkeypatch, capsys):
    schema = f"t_{uuid.uuid4().hex[:12]}"
    monkeypatch.delenv("PGREPLAY_FLOW_APP", raising=False)
    code = main(["--dsn", DSN, "--schema", schema, "list"])
    _, err = capsys.readouterr()
    assert code == 2 and "migrate" in err


def test_worker_command_shuts_down_on_sigterm(app_module, tmp_path):
    env = {**os.environ, "PYTHONPATH": f"{tmp_path}{os.pathsep}{Path(__file__).parents[1]}"}
    proc = subprocess.Popen(
        [sys.executable, "-m", "pgreplay_flow.cli", "worker", "--metrics-port", "0"],
        env=env, cwd=tmp_path, stderr=subprocess.PIPE, text=True,
    )
    assert proc.stderr is not None
    stderr = proc.stderr
    try:
        line = stderr.readline()
        while "worker " not in line:
            assert line, "worker exited early"
            line = stderr.readline()
        assert "double" in line and "gate" in line
        proc.terminate()
        assert proc.wait(15) == 0
    finally:
        if proc.poll() is None:
            proc.kill()
