# Getting started

[← Back to README](../README.md)

Write a workflow, run a worker, start a job.

## 1. Write a workflow

A **workflow** is an `async` function. A **step** is any function you call through `ctx.step(...)`; its result is saved, so it never re-runs once it has succeeded.

```python
from datetime import timedelta
from pgreplay_flow import Flow

flow = Flow("postgresql://user:pass@localhost/mydb")

async def fetch_user(user_id):
    ...  # call an API, query a database, ask an LLM, etc.

async def send_welcome_email(user):
    ...

@flow.workflow(name="onboard")
async def onboard(ctx, user_id):
    user = await ctx.step(fetch_user, user_id)
    await ctx.sleep(timedelta(days=1))
    await ctx.step(send_welcome_email, user)
    return "done"
```

**The one rule:** put anything that touches the outside world inside `ctx.step(...)`. That includes network calls, database writes, the current time and random numbers. Code outside steps runs again after every restart, so it must give the same answer each time.

## 2. Run a worker

A **worker** picks up jobs and runs them. Run as many as you like, on as many machines as you like; they coordinate through Postgres.

```bash
export DATABASE_URL=postgresql://user:pass@localhost/mydb
export PGREPLAY_FLOW_APP=myapp:flow   # where your `flow` object lives

pgreplay-flow worker
```

## 3. Start a job

From your app:

```python
async with flow:
    run_id = await flow.start(onboard, 42)
    print(await flow.result(run_id))   # waits until it finishes
```

Or from the terminal:

```bash
pgreplay-flow start onboard 42 --wait
```
