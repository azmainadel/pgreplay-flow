# CLI reference

[← Back to README](../README.md)

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
