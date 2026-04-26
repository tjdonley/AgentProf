from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

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


@dataclass(frozen=True)
class RawSpanRecord:
    source: str
    source_id: str
    trace_id: str | None
    span_id: str | None
    parent_span_id: str | None
    payload_json: str
    raw_ref: str | None = None


@dataclass(frozen=True)
class RawSpanRow:
    source: str
    source_id: str
    trace_id: str | None
    span_id: str | None
    parent_span_id: str | None
    payload_json: str
    raw_ref: str | None


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

    def insert_raw_spans(self, records: Sequence[RawSpanRecord]) -> int:
        if not records:
            return 0

        self.ensure_schema()
        with self.connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                for record in records:
                    connection.execute(
                        "DELETE FROM raw_spans WHERE source = ? AND source_id = ?",
                        [record.source, record.source_id],
                    )
                    connection.execute(
                        """
                        INSERT INTO raw_spans (
                            source,
                            source_id,
                            trace_id,
                            span_id,
                            parent_span_id,
                            payload_json,
                            raw_ref
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            record.source,
                            record.source_id,
                            record.trace_id,
                            record.span_id,
                            record.parent_span_id,
                            record.payload_json,
                            record.raw_ref,
                        ],
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return len(records)

    def fetch_raw_spans(self, *, source: str | None = None) -> list[RawSpanRow]:
        self.ensure_schema()
        query = """
            SELECT source, source_id, trace_id, span_id, parent_span_id, payload_json, raw_ref
            FROM raw_spans
        """
        params: list[str] = []
        if source:
            query += " WHERE source = ?"
            params.append(source)
        query += " ORDER BY trace_id, span_id, source_id"

        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [RawSpanRow(*row) for row in rows]

    def replace_normalized(self, *, spans: Sequence, traces: Sequence) -> None:
        trace_ids = sorted(
            {span.trace_id for span in spans} | {trace.trace_id for trace in traces}
        )
        if not trace_ids:
            return

        self.ensure_schema()
        with self.connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                for trace_id in trace_ids:
                    connection.execute(
                        "DELETE FROM normalized_spans WHERE trace_id = ?",
                        [trace_id],
                    )
                    connection.execute(
                        "DELETE FROM normalized_traces WHERE trace_id = ?",
                        [trace_id],
                    )

                for span in spans:
                    connection.execute(
                        """
                        INSERT INTO normalized_spans (
                            trace_id,
                            span_id,
                            parent_span_id,
                            source,
                            name,
                            span_type,
                            operation_name,
                            agent_name,
                            tool_name,
                            model_name,
                            provider_name,
                            start_time,
                            end_time,
                            duration_ms,
                            status,
                            status_message,
                            error_type,
                            error_signature,
                            input_hash,
                            output_hash,
                            input_preview,
                            output_preview,
                            input_tokens,
                            output_tokens,
                            total_tokens,
                            cost_usd,
                            cost_confidence,
                            attributes_json,
                            raw_ref
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?
                        )
                        """,
                        [
                            span.trace_id,
                            span.span_id,
                            span.parent_span_id,
                            span.source,
                            span.name,
                            span.span_type,
                            span.operation_name,
                            span.agent_name,
                            span.tool_name,
                            span.model_name,
                            span.provider_name,
                            span.start_time,
                            span.end_time,
                            span.duration_ms,
                            span.status,
                            span.status_message,
                            span.error_type,
                            span.error_signature,
                            span.input_hash,
                            span.output_hash,
                            span.input_preview,
                            span.output_preview,
                            span.input_tokens,
                            span.output_tokens,
                            span.total_tokens,
                            span.cost_usd,
                            span.cost_confidence,
                            json.dumps(
                                span.attributes,
                                ensure_ascii=True,
                                sort_keys=True,
                                default=str,
                            ),
                            span.raw_ref,
                        ],
                    )

                for trace in traces:
                    connection.execute(
                        """
                        INSERT INTO normalized_traces (
                            trace_id,
                            source,
                            project,
                            root_span_id,
                            root_name,
                            session_id,
                            user_hash,
                            environment,
                            version,
                            tags_json,
                            start_time,
                            end_time,
                            duration_ms,
                            outcome,
                            total_cost_usd,
                            total_input_tokens,
                            total_output_tokens,
                            total_tool_calls,
                            total_model_calls,
                            raw_ref
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            trace.trace_id,
                            trace.source,
                            trace.project,
                            trace.root_span_id,
                            trace.root_name,
                            trace.session_id,
                            trace.user_hash,
                            trace.environment,
                            trace.version,
                            json.dumps(trace.tags, ensure_ascii=True, sort_keys=True),
                            trace.start_time,
                            trace.end_time,
                            trace.duration_ms,
                            trace.outcome,
                            trace.total_cost_usd,
                            trace.total_input_tokens,
                            trace.total_output_tokens,
                            trace.total_tool_calls,
                            trace.total_model_calls,
                            trace.raw_ref,
                        ],
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

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
