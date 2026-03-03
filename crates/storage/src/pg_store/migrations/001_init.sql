-- Tuples Postgres schema — v001

CREATE TABLE IF NOT EXISTS schemas (
    name TEXT PRIMARY KEY,
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS filters (
    id TEXT PRIMARY KEY,
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS tuples (
    uuid7 TEXT PRIMARY KEY,
    tuple_type TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    data JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tuples_tuple_type ON tuples (tuple_type);
CREATE INDEX IF NOT EXISTS idx_tuples_trace_id ON tuples (trace_id);

CREATE TABLE IF NOT EXISTS playbooks (
    name TEXT PRIMARY KEY,
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    name TEXT PRIMARY KEY,
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    trace_id TEXT PRIMARY KEY,
    data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    data JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_trace_id ON agent_runs (trace_id);
