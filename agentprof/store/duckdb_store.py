from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from agentprof.config import DEFAULT_STORE_PATH


TABLES = (
    "raw_spans",
    "raw_traces",
    "normalized_spans",
    "normalized_traces",
    "issues",
    "issue_evidence",
    "cost_ledger",
    "reports",
)


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


MIGRATIONS = (
    Migration(
        version=1,
        name="initial_store_schema",
        sql="""
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS raw_spans (
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    trace_id TEXT,
    span_id TEXT,
    parent_span_id TEXT,
    payload_json TEXT NOT NULL,
    raw_ref TEXT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source, source_id)
);

CREATE TABLE IF NOT EXISTS raw_traces (
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    trace_id TEXT,
    payload_json TEXT NOT NULL,
    raw_ref TEXT,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source, source_id)
);

CREATE TABLE IF NOT EXISTS normalized_spans (
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    source TEXT NOT NULL,
    name TEXT NOT NULL,
    span_type TEXT NOT NULL,
    operation_name TEXT,
    agent_name TEXT,
    tool_name TEXT,
    model_name TEXT,
    provider_name TEXT,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_ms DOUBLE,
    status TEXT NOT NULL DEFAULT 'unknown',
    status_message TEXT,
    error_type TEXT,
    error_signature TEXT,
    input_hash TEXT,
    output_hash TEXT,
    input_preview TEXT,
    output_preview TEXT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    total_tokens BIGINT,
    cost_usd DECIMAL(18, 9),
    cost_confidence TEXT NOT NULL DEFAULT 'unknown',
    attributes_json TEXT NOT NULL DEFAULT '{}',
    raw_ref TEXT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (trace_id, span_id)
);

CREATE TABLE IF NOT EXISTS normalized_traces (
    trace_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    project TEXT,
    root_span_id TEXT,
    root_name TEXT,
    session_id TEXT,
    user_hash TEXT,
    environment TEXT,
    version TEXT,
    tags_json TEXT NOT NULL DEFAULT '[]',
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_ms DOUBLE,
    outcome TEXT NOT NULL DEFAULT 'unknown',
    total_cost_usd DECIMAL(18, 9),
    total_input_tokens BIGINT,
    total_output_tokens BIGINT,
    total_tool_calls BIGINT NOT NULL DEFAULT 0,
    total_model_calls BIGINT NOT NULL DEFAULT 0,
    raw_ref TEXT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS issues (
    issue_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    severity TEXT NOT NULL,
    confidence TEXT NOT NULL,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    affected_traces BIGINT NOT NULL DEFAULT 0,
    affected_spans BIGINT NOT NULL DEFAULT 0,
    total_cost_usd DECIMAL(18, 9),
    wasted_cost_usd DECIMAL(18, 9),
    potential_savings_usd DECIMAL(18, 9),
    recommendation TEXT NOT NULL DEFAULT '',
    recommended_tests_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS issue_evidence (
    issue_id TEXT NOT NULL,
    trace_id TEXT,
    span_id TEXT,
    evidence_type TEXT NOT NULL,
    message TEXT NOT NULL,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS cost_ledger (
    trace_id TEXT NOT NULL,
    span_id TEXT,
    issue_id TEXT,
    cost_type TEXT NOT NULL,
    amount_usd DECIMAL(18, 9),
    attribution_method TEXT NOT NULL,
    confidence TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    project TEXT,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    summary_json TEXT NOT NULL DEFAULT '{}',
    report_md_path TEXT,
    report_json_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
""",
    ),
)


class DuckDBStore:
    def __init__(self, path: Path = DEFAULT_STORE_PATH) -> None:
        self.path = Path(path)

    def connect(self) -> duckdb.DuckDBPyConnection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self.path))

    def ensure_schema(self) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
                )
                """
            )
            for migration in MIGRATIONS:
                applied = connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version = ?",
                    [migration.version],
                ).fetchone()
                if applied:
                    continue

                connection.execute("BEGIN TRANSACTION")
                connection.execute(migration.sql)
                connection.execute(
                    "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                    [migration.version, migration.name],
                )
                connection.execute("COMMIT")

    def reset(self) -> None:
        for suffix in ("", ".wal"):
            path = Path(f"{self.path}{suffix}")
            if path.exists():
                path.unlink()
        self.ensure_schema()

    def stats(self) -> dict[str, int]:
        self.ensure_schema()
        with self.connect() as connection:
            return {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in TABLES
            }

    def migrations(self) -> list[tuple[int, str]]:
        if not self.path.exists():
            raise FileNotFoundError(self.path)

        with duckdb.connect(str(self.path), read_only=True) as connection:
            return connection.execute(
                "SELECT version, name FROM schema_migrations ORDER BY version"
            ).fetchall()
