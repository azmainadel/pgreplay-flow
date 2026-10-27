# Guarantees and limits

[← Back to README](../README.md)

## Guarantees, precisely

| | |
|---|---|
| Step results | Recorded **exactly once**, even with many workers, crashes or network splits |
| Step code | Runs **at least once**. It can run again if the worker dies after the step finishes but before its result is saved. Use `step_info().idempotency_key` for outside services |
| `tx_step` database writes | **Exactly once** (same transaction as the record) |
| Sleeps, retries, signals | Survive restarts. Signals sent early are kept until the workflow asks for them |
| A step that crashes the worker every time | Counts as a failed attempt, so it can't loop forever |

## Limits

- Each time a workflow resumes, it re-runs from the top and skips finished steps using its saved history. That's cheap for hundreds or thousands of steps. For endless loops, start child workflows instead.
- Cancelling doesn't interrupt a step that's already running. The cancellation takes effect as soon as that step finishes.
- Give workflows and steps explicit names (`@flow.workflow(name=...)`, `@step(name=...)`) if you might rename the functions later.
