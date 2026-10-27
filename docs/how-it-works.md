# How it works

[← Back to README](../README.md)

Everything lives in three Postgres tables.

- **`runs`** is the job list. Workers grab jobs with `SELECT ... FOR UPDATE SKIP LOCKED`, which means "give me a job nobody else is holding", so they never fight over one. `LISTEN/NOTIFY` wakes workers the moment new work arrives.
- **`steps`** is each job's history. After a restart, a worker runs the workflow function from the top. Every `ctx.step` checks this table first and returns the saved result if there is one. The first step with no saved result is where real work resumes.
- **`signals`** holds messages waiting to be picked up.

While a worker runs a job, it holds a **lease** with a unique token. If the worker dies, the lease expires and another worker takes over with a new token. Every save checks the token, so a worker that froze and woke up late can't overwrite anything.
