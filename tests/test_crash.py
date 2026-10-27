"""Crash recovery with a real SIGKILL: no cleanup code runs in the dying worker."""

import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from tests.conftest import DSN

APP = Path(__file__).with_name("crash_app.py")


def _env(schema: str, marks: Path, hang: bool) -> dict[str, str]:
    return {
        **os.environ,
        "PGF_DSN": DSN,
        "PGF_SCHEMA": schema,
        "PGF_MARKS": str(marks),
        "PGF_HANG": "1" if hang else "0",
        "PYTHONPATH": str(APP.parent),
    }


async def test_sigkill_mid_step_recovers_without_rerunning_completed_steps(
    flow, schema, tmp_path
):
    # Register the same workflow name in this process for result()/replay().
    run_id = uuid.UUID(
        subprocess.check_output(
            [sys.executable, str(APP), "start"], env=_env(schema, tmp_path, False), text=True
        ).strip()
    )

    victim = subprocess.Popen([sys.executable, str(APP)], env=_env(schema, tmp_path, True))
    try:
        deadline = time.monotonic() + 15
        while not (tmp_path / "in_second").exists():
            assert time.monotonic() < deadline, "worker never reached step 2"
            await asyncio.sleep(0.05)
        victim.send_signal(signal.SIGKILL)
        victim.wait(5)
    finally:
        if victim.poll() is None:
            victim.kill()

    run = await flow.get_run(run_id)
    assert run["status"] == "running"  # nothing cleaned up: lease must expire

    rescuer = subprocess.Popen([sys.executable, str(APP)], env=_env(schema, tmp_path, False))
    try:
        deadline = time.monotonic() + 20
        while (run := await flow.get_run(run_id))["status"] != "completed":
            assert time.monotonic() < deadline, f"not recovered: {run['status']}"
            await asyncio.sleep(0.1)
    finally:
        rescuer.terminate()
        rescuer.wait(10)

    assert run["result"] == 2
    assert run["recoveries"] == 1

    log = (tmp_path / "log").read_text().splitlines()
    firsts = [line for line in log if line.startswith("first ")]
    seconds = [line for line in log if line.startswith("second ")]
    assert len(firsts) == 1, log  # completed step never re-executed
    # The killed attempt counts; recovery runs attempt 2 with the same key.
    assert [s.split()[1] for s in seconds] == ["attempt=1", "attempt=2"], log
    assert len({s.split()[2] for s in seconds}) == 1, log
    assert len({s.split()[3] for s in seconds}) == 2, log  # different processes

    journal = await flow.get_journal(run_id)
    assert [(e["key"], e["status"], e["attempts"]) for e in journal] == [
        ("1", "completed", 1),
        ("2", "completed", 2),
    ]
