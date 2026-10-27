<p align="center">
  <img src="assets/banner.webp" alt="pgreplay-flow: Durable Python workflows. Just Postgres." width="800">
</p>

# pgreplay-flow

**Python code that survives crashes, using only the Postgres database you already have.**

Some jobs run in several stages: call an AI model, wait an hour, send an email. If the server restarts halfway through, a normal script starts over from the beginning. You pay for the AI calls again, and someone might get the email twice.

With pgreplay-flow, each finished step is saved to Postgres. After a crash, the job picks up where it stopped: finished steps are skipped and their saved results are reused.

```python
@flow.workflow(name="daily_report")
async def daily_report(ctx, name):
    report = await ctx.step(fetch_report, name)  # saved once it finishes
    await ctx.sleep(timedelta(hours=1))          # doesn't tie up a worker while waiting
    await ctx.step(send_email, report)           # retried automatically if it fails
```

Kill the process anywhere in there and start it again: `fetch_report` will not run a second time.

There is no Redis, no Kafka and no separate server to run. If you have Postgres, you have everything you need.

## Try it in 2 minutes

You need [Docker](https://docs.docker.com/get-docker/) and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/azmainadel/pgreplay-flow
cd pgreplay-flow
docker compose up -d                       # starts a local Postgres
uv sync
uv run python examples/quickstart.py
```

You'll see:

```
  step 1: fetching report for alice (pretend this is slow and costs money)
  sleeping 10s -- press Ctrl-C now, then run the script again
```

Press **Ctrl-C** while it's sleeping, then run the same command again:

```
  step 2: emailing 'report for alice'
result: sent
```

Step 1 didn't run again. The script remembered it from the database and went straight to step 2.

## Is this for me?

**Good fit:**

- AI agent pipelines, where each LLM call is slow and costs money
- Multi-step jobs like onboarding, billing or data imports that must not double-charge or double-send
- Anything that waits: "remind them in 3 days", "wait until a human approves"
- Teams that already run Postgres and don't want another system to operate

**Probably not:**

- Very high throughput (tens of thousands of jobs per second). Look at Temporal or a dedicated queue.
- Simple fire-and-forget background jobs with a single step. A plain task queue is simpler.
- Non-Python services. This is a Python library.

## How to use it

### 1. Write a workflow

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

### 2. Run a worker

A **worker** picks up jobs and runs them. Run as many as you like, on as many machines as you like; they coordinate through Postgres.

```bash
export DATABASE_URL=postgresql://user:pass@localhost/mydb
export PGREPLAY_FLOW_APP=myapp:flow   # where your `flow` object lives

pgreplay-flow worker
```

### 3. Start a job

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

## What else it can do

Each feature below is one short snippet.

**Retry flaky steps** with backoff. The wait between attempts survives restarts, too.

```python
from pgreplay_flow import RetryPolicy, step

@step(retry=RetryPolicy(max_attempts=5, initial_delay=1.0))
async def call_llm(prompt): ...
```

**Wait for a person or another system.** The workflow pauses until something sends it a signal.

```python
approved = await ctx.wait_for_signal("approved", timeout=timedelta(days=2))
```

```bash
pgreplay-flow signal <run-id> approved '{"by": "alice"}'
```

**Run things in parallel**:

```python
answers = await asyncio.gather(*(ctx.step(call_llm, q) for q in questions))
```

**Split big jobs into smaller workflows.** Each child can run on any worker, and the parent waits without using a worker slot.

```python
summaries = await asyncio.gather(*(ctx.child(summarize_doc, url) for url in urls))
```

**Avoid starting a job twice**, for example when a user double-clicks:

```python
await flow.start(onboard, 42, idempotency_key="onboard-user-42")
```

**Fix a bug and resume, without redoing expensive work:**

```bash
pgreplay-flow fork <run-id>    # reuses finished steps, re-runs the rest with your new code
```

**See what happened:**

```bash
pgreplay-flow list --status failed
pgreplay-flow status <run-id>   # every step, its result, how many attempts
```

## Common questions

**What if my step sends an email and the server crashes right after?**
The step's result was never saved, so the step runs again. To make the second run harmless, pass the step's ID to the service you're calling; most email and payment APIs accept one and skip duplicates:

```python
from pgreplay_flow import step_info

async def send_email(user):
    await mailer.send(user["email"], idempotency_key=step_info().idempotency_key)
```

If the step only writes to *your* Postgres, use `ctx.tx_step(...)` instead. The write and the "step done" record are saved together in one transaction, so the step's effect happens exactly once.

**Can I change my workflow code while jobs are running?**
Yes, if the steps already taken still line up with the new code. Check a run before deploying:

```bash
pgreplay-flow replay <run-id>   # re-runs it against its history without side effects
```

If it reports `nondeterministic`, the new code would take a different path for that run.

**What do I need to pass in and get back?**
Plain JSON values: numbers, strings, lists, dicts, `None`.

**Does it fill up my database?**
Finished jobs are kept until you clean them up: `pgreplay-flow worker --purge-after 30d`, or `pgreplay-flow purge --older-than 30d`.

**How do I monitor it?**
`pgreplay-flow stats` prints a summary. `pgreplay-flow worker --metrics-port 9464` serves [Prometheus](https://prometheus.io/) metrics at `/metrics`. That endpoint has no login and only listens on localhost by default; don't expose it publicly.

## Reference

### CLI commands

| Command | What it does |
|---|---|
| `migrate` | Create or upgrade the tables |
| `worker` | Run a worker (`-c` concurrency, `-q` queue, `--purge-after`, `--metrics-port`) |
| `start <workflow> [args...]` | Start a run. Arguments are JSON values; `--wait` prints the result |
| `list` | Recent runs (`--status`, `--workflow`, `--parent`) |
| `status <run-id>` | One run with all its steps and child runs |
| `result <run-id>` | Wait for a run and print its result |
| `signal <run-id> <name> [json]` | Send a signal |
| `cancel <run-id>` | Cancel a run and the child runs it's waiting on |
| `replay <run-id>` | Re-run against recorded history, no side effects |
| `fork <run-id>` | New run that reuses the old one's successful steps and re-runs from the first one that didn't succeed (`--keep N` caps reuse at N; `--keep 0` re-runs everything) |
| `purge --older-than 30d` | Delete old finished runs (`--dry-run` to preview) |
| `stats` | Counts per status, queue lag (how long the oldest job has waited); `--format prometheus` or `--format json` |

Every command reads `DATABASE_URL`. `worker`, `start` and `replay` also need `PGREPLAY_FLOW_APP` (or `--app module:attr`) so they can find your workflow code.

### Workflow API (`ctx`)

| Call | Use it for |
|---|---|
| `await ctx.step(fn, *args)` | Any work with side effects. Saved once it succeeds; retried on failure |
| `await ctx.tx_step(fn, *args)` | Work that only writes to your Postgres. `fn(conn, ...)` runs in the same transaction as the step record, so it happens exactly once |
| `await ctx.sleep(seconds)` / `ctx.sleep_until(dt)` | Waiting. Long waits free the worker |
| `await ctx.wait_for_signal(name, timeout=)` | Waiting for an external event. Returns the payload, or `None` on timeout |
| `await ctx.child(workflow, *args)` | Run another workflow and wait for its result |
| `await ctx.start_child(workflow, *args)` | Start another workflow without waiting |
| `await ctx.now()`, `ctx.uuid4()`, `ctx.random()` | Time and randomness that stay the same on replay |
| `ctx.is_replaying` | `True` while re-running already-finished steps (e.g. to avoid duplicate logs) |

### Guarantees, precisely

| | |
|---|---|
| Step results | Recorded **exactly once**, even with many workers, crashes or network splits |
| Step code | Runs **at least once**. It can run again if the worker dies after the step finishes but before its result is saved. Use `step_info().idempotency_key` for outside services |
| `tx_step` database writes | **Exactly once** (same transaction as the record) |
| Sleeps, retries, signals | Survive restarts. Signals sent early are kept until the workflow asks for them |
| A step that crashes the worker every time | Counts as a failed attempt, so it can't loop forever |

### Limits

- Each time a workflow resumes, it re-runs from the top and skips finished steps using its saved history. That's cheap for hundreds or thousands of steps. For endless loops, start child workflows instead.
- Cancelling doesn't interrupt a step that's already running. The cancellation takes effect as soon as that step finishes.
- Give workflows and steps explicit names (`@flow.workflow(name=...)`, `@step(name=...)`) if you might rename the functions later.

## How it works

Everything lives in three Postgres tables.

- **`runs`** is the job list. Workers grab jobs with `SELECT ... FOR UPDATE SKIP LOCKED`, which means "give me a job nobody else is holding", so they never fight over one. `LISTEN/NOTIFY` wakes workers the moment new work arrives.
- **`steps`** is each job's history. After a restart, a worker runs the workflow function from the top. Every `ctx.step` checks this table first and returns the saved result if there is one. The first step with no saved result is where real work resumes.
- **`signals`** holds messages waiting to be picked up.

While a worker runs a job, it holds a **lease** with a unique token. If the worker dies, the lease expires and another worker takes over with a new token. Every save checks the token, so a worker that froze and woke up late can't overwrite anything.

## Contributing

```bash
docker compose up -d     # Postgres 17 on localhost:54329
uv sync
uv run pytest            # 42 tests, including one that kill -9s a real worker mid-step
```

`examples/agent_pipeline.py` is a bigger example: parallel LLM calls with retries, a durable sleep, and a transactional write.
