"""Lease-fenced persistence for a single claimed run.

Every write first locks the run row with a predicate on the lease token
(`UPDATE ... WHERE lease_token = $token AND status = 'running'`). Claiming a
run rotates the token under the same row lock, so a stale worker (paused,
partitioned, lease expired) can never commit a journal entry after another
worker has taken over. That is what makes "result recorded exactly once" hold.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg import AsyncConnection, sql
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from .errors import JournalExhausted, LeaseLost, StoreUnavailable
from .schema import validate_schema_name
from .serde import normalize


def _j(v: Any) -> Jsonb | None:
    return None if v is None else Jsonb(v)


class RunStore:
    def __init__(
        self, pool: AsyncConnectionPool[Any], schema: str, run_id: uuid.UUID, token: uuid.UUID
    ):
        self.pool = pool
        self.s = validate_schema_name(schema)
        self.run_id = run_id
        self.token = token

    @asynccontextmanager
    async def _conn(self) -> AsyncIterator[AsyncConnection]:
        # Connectivity failures become StoreUnavailable (a BaseException) so that
        # `except Exception` in workflow code cannot swallow them.
        try:
            async with self.pool.connection() as conn:
                yield conn
        except (psycopg.OperationalError, PoolTimeout) as e:
            raise StoreUnavailable(str(e)) from e

    async def _fence(self, conn: AsyncConnection) -> None:
        cur = await conn.execute(
            sql.SQL("""
                UPDATE {0}.runs SET updated_at = now()
                WHERE id = %s AND lease_token = %s AND status = 'running' RETURNING 1
            """).format(sql.Identifier(self.s)),
            (self.run_id, self.token),
        )
        if await cur.fetchone() is None:
            raise LeaseLost()

    async def _upsert(
        self,
        conn: AsyncConnection,
        key: str,
        kind: str,
        name: str,
        status: str,
        attempts: int,
        output: Any = None,
        error: Any = None,
    ) -> None:
        # Never overwrite a completed entry: the first committed result wins.
        await conn.execute(
            sql.SQL("""
            INSERT INTO {0}.steps (run_id, key, kind, name, status, output, error, attempts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, key) DO UPDATE SET
                status = EXCLUDED.status, output = EXCLUDED.output, error = EXCLUDED.error,
                attempts = EXCLUDED.attempts, updated_at = now()
            WHERE {0}.steps.status <> 'completed'
            """).format(sql.Identifier(self.s)),
            (self.run_id, key, kind, name, status, _j(output), _j(error), attempts),
        )

    async def put_step(
        self,
        key: str,
        kind: str,
        name: str,
        status: str,
        attempts: int,
        output: Any = None,
        error: Any = None,
    ) -> None:
        async with self._conn() as conn, conn.transaction():
            await self._fence(conn)
            await self._upsert(conn, key, kind, name, status, attempts, output, error)

    async def run_tx_step(
        self,
        key: str,
        kind: str,
        name: str,
        attempts: int,
        fn: Callable[..., Any],
        args: tuple,
        kwargs: dict,
    ) -> tuple[Any, Exception | None]:
        """Run fn(conn, ...) and commit its effects + the journal entry atomically."""
        async with self._conn() as conn:
            try:
                async with conn.transaction():
                    await self._fence(conn)
                    out = normalize(await fn(conn, *args, **kwargs))
                    await self._upsert(conn, key, kind, name, "completed", attempts, output=out)
                return out, None
            except (asyncio.CancelledError, LeaseLost, StoreUnavailable, JournalExhausted):
                raise
            except psycopg.OperationalError as e:
                raise StoreUnavailable(str(e)) from e
            except Exception as e:  # noqa: BLE001 - user code; transaction rolled back
                return None, e

    async def consume_signal(self, key: str, name: str) -> tuple[bool, Any]:
        async with self._conn() as conn, conn.transaction():
            await self._fence(conn)
            cur = await conn.execute(
                sql.SQL("""
                SELECT id, payload FROM {0}.signals
                WHERE run_id = %s AND name = %s AND consumed_key IS NULL
                ORDER BY id LIMIT 1 FOR UPDATE
                """).format(sql.Identifier(self.s)),
                (self.run_id, name),
            )
            row = await cur.fetchone()
            if row is None:
                return False, None
            sig_id, payload = row
            await conn.execute(
                sql.SQL("UPDATE {0}.signals SET consumed_key = %s WHERE id = %s").format(sql.Identifier(self.s)), (key, sig_id)
            )
            await self._upsert(
                conn, key, "signal", name, "completed", 1, output={"payload": payload}
            )
            return True, payload

    # -- run-level transitions (fenced; return False if the lease was lost) ----

    async def heartbeat(self, lease_seconds: float) -> bool:
        async with self._conn() as conn:
            cur = await conn.execute(
                sql.SQL("""
                UPDATE {0}.runs
                SET lease_expires_at = now() + make_interval(secs => %s)
                WHERE id = %s AND lease_token = %s AND status = 'running' RETURNING 1
                """).format(sql.Identifier(self.s)),
                (lease_seconds, self.run_id, self.token),
            )
            return await cur.fetchone() is not None

    async def finish(self, status: str, result: Any = None, error: Any = None) -> bool:
        async with self._conn() as conn, conn.transaction():
            cur = await conn.execute(
                sql.SQL("""
                UPDATE {0}.runs
                SET status = %s, result = %s, error = %s, finished_at = now(), updated_at = now(),
                    lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL, ready_at = NULL
                WHERE id = %s AND lease_token = %s AND status = 'running' RETURNING 1
                """).format(sql.Identifier(self.s)),
                (status, _j(result), _j(error), self.run_id, self.token),
            )
            ok = await cur.fetchone() is not None
            if ok:
                await conn.execute(
                    "SELECT pg_notify(%s, %s)", (f"{self.s}_done", str(self.run_id))
                )
            return ok

    async def suspend(self, status: str, until: float | None) -> bool:
        async with self._conn() as conn, conn.transaction():
            # Statement 1 takes the row lock. send_signal() locks the same row
            # before inserting, so any signal it commits is either visible to
            # statement 2 or observes our new status and wakes the run itself.
            cur = await conn.execute(
                sql.SQL("""
                UPDATE {0}.runs
                SET status = %s,
                    ready_at = CASE WHEN %s::float8 IS NULL THEN NULL
                                    ELSE to_timestamp(%s::float8) END,
                    lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    updated_at = now()
                WHERE id = %s AND lease_token = %s AND status = 'running' RETURNING 1
                """).format(sql.Identifier(self.s)),
                (status, until, until, self.run_id, self.token),
            )
            if await cur.fetchone() is None:
                return False
            # Statement 2: wake immediately if an awaited signal is already buffered.
            await conn.execute(
                sql.SQL("""
                UPDATE {0}.runs SET ready_at = now()
                WHERE id = %s AND EXISTS (
                    SELECT 1 FROM {0}.signals sg
                    JOIN {0}.steps st
                      ON st.run_id = sg.run_id AND st.kind = 'signal'
                     AND st.status = 'pending' AND st.name = sg.name
                    WHERE sg.run_id = %s AND sg.consumed_key IS NULL)
                """).format(sql.Identifier(self.s)),
                (self.run_id, self.run_id),
            )
            await conn.execute("SELECT pg_notify(%s, %s)", (f"{self.s}_wake", ""))
            return True

    async def release(self) -> None:
        """Give the run back immediately (graceful worker shutdown).

        An attempt interrupted by shutdown is not a crash, so it is refunded
        rather than counted against the step's retry budget.
        """
        async with self._conn() as conn, conn.transaction():
            await self._fence(conn)
            await conn.execute(
                sql.SQL("""
                UPDATE {0}.steps SET attempts = attempts - 1, updated_at = now()
                WHERE run_id = %s AND status = 'pending' AND kind = 'step' AND attempts > 0
                """).format(sql.Identifier(self.s)),
                (self.run_id,),
            )
            await conn.execute(
                sql.SQL("""
                UPDATE {0}.runs
                SET status = 'pending', ready_at = now(), lease_token = NULL,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                WHERE id = %s AND lease_token = %s AND status = 'running'
                """).format(sql.Identifier(self.s)),
                (self.run_id, self.token),
            )
            await conn.execute("SELECT pg_notify(%s, %s)", (f"{self.s}_wake", ""))
