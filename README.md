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

## Docs

- [Getting started](docs/getting-started.md): write a workflow, run a worker, start a job
- [Features](docs/features.md): retries, signals, parallel steps, child workflows, fork
- [Common questions](docs/faq.md): crashes mid-step, deploying changes, cleanup, monitoring
- [CLI reference](docs/cli.md)
- [Workflow API](docs/api.md): everything you can call on `ctx`
- [Guarantees and limits](docs/guarantees.md): exactly what is promised, and what isn't
- [How it works](docs/how-it-works.md): the three tables, leases and replay
- [Contributing](docs/contributing.md): running the tests locally
