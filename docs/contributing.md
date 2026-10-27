# Contributing

[← Back to README](../README.md)

```bash
docker compose up -d     # Postgres 17 on localhost:54329
uv sync
uv run pytest            # 42 tests, including one that kill -9s a real worker mid-step
```

`examples/agent_pipeline.py` is a bigger example: parallel LLM calls with retries, a durable sleep, and a transactional write.
