"""Agent-pipeline style example.

    docker compose up -d
    uv run python examples/agent_pipeline.py

Kill it (Ctrl-C or kill -9) while it runs and start it again: completed steps
are not re-executed, and the run resumes where it stopped.
"""

import asyncio
import logging
import os
import random
from datetime import timedelta

from pgreplay_flow import Flow, RetryPolicy, Worker, step, step_info

DSN = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:54329/postgres")
flow = Flow(DSN, schema="example")


@step(retry=RetryPolicy(max_attempts=5, initial_delay=0.5))
async def call_llm(prompt: str) -> str:
    # Pass the idempotency key to the provider so a retried call is deduplicated.
    print(f"  llm call ({step_info().idempotency_key}, attempt {step_info().attempt})")
    if random.random() < 0.3:
        raise ConnectionError("provider timeout")
    await asyncio.sleep(0.5)
    return f"answer to {prompt!r}"


async def store(conn, doc: str) -> int:
    # tx_step: this INSERT and the journal entry commit in one transaction.
    await conn.execute("CREATE TABLE IF NOT EXISTS example.docs (id serial, body text)")
    cur = await conn.execute("INSERT INTO example.docs (body) VALUES (%s) RETURNING id", (doc,))
    return (await cur.fetchone())[0]


@flow.workflow(name="research")
async def research(ctx, topic: str):
    if not ctx.is_replaying:
        print(f"researching {topic}")
    questions = [f"{topic}: q{i}" for i in range(3)]
    answers = await asyncio.gather(*(ctx.step(call_llm, q) for q in questions))
    await ctx.sleep(timedelta(seconds=3))  # durable: worker is released meanwhile
    summary = await ctx.step(call_llm, "summarize: " + " | ".join(answers))
    doc_id = await ctx.tx_step(store, summary)
    return {"doc_id": doc_id, "summary": summary}


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    async with flow:
        worker = Worker(flow)
        task = asyncio.create_task(worker.run())
        run_id = await flow.start(research, "postgres queues", idempotency_key="demo-1")
        print("run", run_id)
        print(await flow.result(run_id))
        print(await flow.replay(run_id))
        await worker.stop()
        await task  # run() returns once stop() is called


if __name__ == "__main__":
    asyncio.run(main())
