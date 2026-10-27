"""A self-healing LISTEN connection shared by many waiters."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import logging
from collections.abc import Callable

import psycopg
from psycopg import AsyncConnection, sql

log = logging.getLogger("pgreplay_flow")


class ChannelListener:
    """Holds one dedicated connection that LISTENs on `channel`.

    `on_notify(payload)` runs for every notification. `on_connect()` runs after
    each (re)connect, because notifications sent while disconnected are lost
    and waiters must re-check state. Reconnects with exponential backoff.
    """

    def __init__(
        self,
        dsn: str,
        channel: str,
        on_notify: Callable[[str], None],
        *,
        on_connect: Callable[[], None] | None = None,
    ):
        self.dsn = dsn
        self.channel = channel
        # Built once: LISTEN takes an identifier, not a bindable parameter, so the
        # channel name is quoted with sql.Identifier rather than interpolated.
        self._listen_stmt = sql.SQL("LISTEN {}").format(sql.Identifier(channel))
        self._on_notify = on_notify
        self._on_connect = on_connect
        self._task: asyncio.Task | None = None
        self._attempted = asyncio.Event()  # first connect attempt finished
        self.connected = False

    def start(self) -> None:
        """Start listening in the background (idempotent)."""
        if self._task is None or self._task.done():
            self._attempted.clear()
            # Empty context: never inherit a workflow's key scope.
            self._task = asyncio.get_running_loop().create_task(
                self._run(), name=f"pgreplay-flow-listen-{self.channel}",
                context=contextvars.Context(),
            )

    async def wait_started(self, timeout: float = 5.0) -> None:
        """Wait until the first connection attempt has finished (either way)."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._attempted.wait(), timeout)

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self.connected = False

    async def _run(self) -> None:
        backoff = 0.5
        while True:
            try:
                async with await AsyncConnection.connect(self.dsn, autocommit=True) as conn:
                    await conn.execute(self._listen_stmt)
                    backoff = 0.5
                    self.connected = True
                    self._attempted.set()
                    if self._on_connect is not None:
                        self._on_connect()
                    async for n in conn.notifies():
                        self._on_notify(n.payload)
            except psycopg.OperationalError as e:
                self.connected = False
                self._attempted.set()
                log.warning("listener on %s disconnected: %s", self.channel, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
