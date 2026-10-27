# Features

[← Back to README](../README.md)

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
