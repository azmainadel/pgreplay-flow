# Common questions

[← Back to README](../README.md)

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
