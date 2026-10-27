"""Quickstart: see a workflow survive being interrupted.

    docker compose up -d
    uv run python examples/quickstart.py

Press Ctrl-C while it says "sleeping", then run it again. Step 1 does not
run a second time: the workflow picks up where it stopped.
"""

import asyncio
import os

from pgreplay_flow import Flow, Worker

DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:54329/postgres")
flow = Flow(DSN, schema="quickstart")


async def fetch_report(name: str) -> str:
    print(f"  step 1: fetching report for {name} (pretend this is slow and costs money)")
    return f"report for {name}"


async def send_email(report: str) -> str:
    print(f"  step 2: emailing {report!r}")
    return "sent"


@flow.workflow(name="daily_report")
async def daily_report(ctx, name: str):
    report = await ctx.step(fetch_report, name)
    if not ctx.is_replaying:
        print("  sleeping 10s -- press Ctrl-C now, then run the script again")
    await ctx.sleep(10)
    return await ctx.step(send_email, report)


async def main() -> None:
    async with flow:  # connects and creates the tables if needed
        worker = Worker(flow)
        worker_task = asyncio.create_task(worker.run())

        # The idempotency key makes re-running this script resume the same
        # run instead of starting a new one.
        run_id = await flow.start(daily_report, "alice", idempotency_key="quickstart-alice")
        print(f"run {run_id}")
        print("result:", await flow.result(run_id))

        await worker.stop()
        await worker_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\ninterrupted -- run the script again to resume")
