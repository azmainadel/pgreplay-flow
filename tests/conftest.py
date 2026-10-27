import asyncio
import contextlib
import os
import uuid

import psycopg
import pytest
from psycopg import sql

from pgreplay_flow import Flow, Worker

DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:54329/postgres")


@pytest.fixture
def schema():
    """A throwaway Postgres schema per test, dropped at teardown."""
    name = f"t_{uuid.uuid4().hex[:12]}"
    yield name
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))


@pytest.fixture
async def flow(schema):
    f = Flow(DSN, schema=schema)
    await f.open()
    yield f
    await f.close()


@pytest.fixture
async def start_worker(flow):
    """Factory that starts background workers; all are stopped at teardown."""
    started: list[tuple[Worker, asyncio.Task]] = []

    def start(**kw) -> Worker:
        kw.setdefault("poll_interval", 0.5)
        kw.setdefault("lease_seconds", 3.0)
        w = Worker(flow, **kw)
        started.append((w, asyncio.create_task(w.run())))
        return w

    yield start

    for w, t in started:
        await w.stop(grace=0)
        try:
            await asyncio.wait_for(t, 5)  # run() exits on its own after stop()
        except TimeoutError:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
