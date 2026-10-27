"""DDL. All objects live in a dedicated Postgres schema (default: pgreplay_flow)."""

from __future__ import annotations

import re

_IDENT = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def validate_schema_name(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"invalid schema name: {name!r}")
    return name


DDL = """
CREATE SCHEMA IF NOT EXISTS {s};

-- One row per workflow run. This row is also the queue entry.
CREATE TABLE IF NOT EXISTS {s}.runs (
    id               uuid PRIMARY KEY,
    workflow         text        NOT NULL,
    queue            text        NOT NULL DEFAULT 'default',
    args             jsonb       NOT NULL,
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','running','sleeping','waiting',
                                       'completed','failed','cancelled')),
    result           jsonb,
    error            jsonb,
    idempotency_key  text UNIQUE,
    forked_from      uuid,
    -- When a pending/sleeping/waiting run becomes claimable. NULL = only a
    -- signal can wake it.
    ready_at         timestamptz DEFAULT now(),
    -- Lease: the fencing token changes on every claim. Every journal write
    -- checks it, so a worker that lost its lease cannot commit anything.
    lease_token      uuid,
    lease_owner      text,
    lease_expires_at timestamptz,
    claims           integer     NOT NULL DEFAULT 0,
    recoveries       integer     NOT NULL DEFAULT 0,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

CREATE INDEX IF NOT EXISTS runs_ready_idx
    ON {s}.runs (queue, ready_at)
    WHERE status IN ('pending','sleeping','waiting');

CREATE INDEX IF NOT EXISTS runs_lease_idx
    ON {s}.runs (queue, lease_expires_at)
    WHERE status = 'running';

-- The journal. (run_id, key) identifies an operation; keys are positional
-- ("1", "2", "t1/1", ...). ord is first-write order, used by fork().
CREATE TABLE IF NOT EXISTS {s}.steps (
    run_id      uuid    NOT NULL REFERENCES {s}.runs(id) ON DELETE CASCADE,
    key         text    NOT NULL,
    ord         bigserial,
    kind        text    NOT NULL,   -- step | tx_step | sleep | signal | child | now | uuid | random
    name        text    NOT NULL,
    status      text    NOT NULL CHECK (status IN ('completed','failed','retrying','pending')),
    output      jsonb,
    error       jsonb,
    attempts    integer NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, key)
);

CREATE TABLE IF NOT EXISTS {s}.signals (
    id            bigserial PRIMARY KEY,
    run_id        uuid  NOT NULL REFERENCES {s}.runs(id) ON DELETE CASCADE,
    name          text  NOT NULL,
    payload       jsonb,
    consumed_key  text,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS signals_pending_idx
    ON {s}.signals (run_id, name, id) WHERE consumed_key IS NULL;

-- v2: child workflows. No foreign key on purpose: parent and child runs have
-- independent lifecycles (a child's result is copied into the parent's journal).
ALTER TABLE {s}.runs ADD COLUMN IF NOT EXISTS parent_id  uuid;
ALTER TABLE {s}.runs ADD COLUMN IF NOT EXISTS parent_key text;
-- attached: started with ctx.child() (parent waits; cancelled with parent).
-- Detached children (ctx.start_child) keep parent_id only as lineage.
ALTER TABLE {s}.runs ADD COLUMN IF NOT EXISTS attached   boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS runs_parent_idx
    ON {s}.runs (parent_id) WHERE parent_id IS NOT NULL;
"""


def ddl(schema: str) -> str:
    return DDL.format(s=validate_schema_name(schema))
