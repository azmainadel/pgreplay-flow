# Workflow API (`ctx`)

[← Back to README](../README.md)

Everything a workflow can call on its `ctx` argument.

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
