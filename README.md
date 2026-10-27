# pgreplay-flow

Durable async workflows for Python on plain Postgres. Steps survive crashes, retries are durable and idempotent, and any run can be replayed offline or forked. No Redis, no Kafka, no separate server.

```bash
docker compose up -d          # postgres:17 on localhost:54329
uv sync
uv run pytest
uv run python examples/agent_pipeline.py
```
