"""Workflow app used by the crash test. Runs in a separate worker process that
gets SIGKILLed mid-step; side effects are logged to files so the parent test
can count executions across processes."""

import asyncio
import os
import pathlib
import sys

from pgreplay_flow import Flow, Worker, step_info

DSN = os.environ["PGF_DSN"]
SCHEMA = os.environ["PGF_SCHEMA"]
MARKS = pathlib.Path(os.environ["PGF_MARKS"])

flow = Flow(DSN, schema=SCHEMA)


def mark(name: str) -> None:
    info = step_info()
    with (MARKS / "log").open("a") as f:
        f.write(f"{name} attempt={info.attempt} key={info.idempotency_key} pid={os.getpid()}\n")


async def first():
    mark("first")
    return 1


async def second(x):
    mark("second")
    if os.environ.get("PGF_HANG") == "1":
        (MARKS / "in_second").touch()
        await asyncio.sleep(3600)  # parent SIGKILLs us here
    return x + 1


@flow.workflow(name="crashy")
async def crashy(ctx):
    x = await ctx.step(first)
    return await ctx.step(second, x)


async def main() -> None:
    await flow.open()
    worker = Worker(flow, lease_seconds=2.0, poll_interval=0.2)
    if sys.argv[1:] == ["start"]:
        print(await flow.start(crashy), flush=True)
        await flow.close()
        return
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
