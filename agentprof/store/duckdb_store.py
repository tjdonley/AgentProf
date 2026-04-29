from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

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


@dataclass(frozen=True)
class NormalizedSpanCostRow:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    status: str
    cost_usd: Decimal | None
    cost_confidence: str


@dataclass(frozen=True)
class CostLedgerRecord:
    trace_id: str
    span_id: str | None
    issue_id: str | None
    cost_type: str
    amount_usd: Decimal | None
    attribution_method: str
    confidence: str


@dataclass(frozen=True)
class NormalizedSpanAnalysisRow:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    span_type: str
    start_time: datetime | None
    end_time: datetime | None
    status: str
    status_message: str | None
    error_signature: str | None
    input_retry_fingerprint: str | None
    input_preview: str | None
    output_preview: str | None
    cost_usd: Decimal | None
    cost_confidence: str


@dataclass(frozen=True)
class NormalizedSpanAgentAnalysisRow:
    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    span_type: str
    agent_name: str | None
    status: str
    input_hash: str | None
    start_time: datetime | None
    end_time: datetime | None
    cost_usd: Decimal | None
    cost_confidence: str


@dataclass(frozen=True)
class IssueRecord:
    issue_id: str
    kind: str
    title: str
    severity: str
    confidence: str
    first_seen: datetime | None
    last_seen: datetime | None
    affected_traces: int
    affected_spans: int
    total_cost_usd: Decimal | None
    wasted_cost_usd: Decimal | None
    potential_savings_usd: Decimal | None
    recommendation: str
    recommended_tests: list[str]


@dataclass(frozen=True)
class IssueEvidenceRecord:
    issue_id: str
    trace_id: str | None
    span_id: str | None
    evidence_type: str
    message: str
    attributes: dict[str, Any]


@dataclass(frozen=True)
class ReportRecord:
    report_id: str
    project: str | None
    window_start: datetime | None
    window_end: datetime | None
    summary: dict[str, Any]
    report_md_path: str | None
    report_json_path: str | None
    report_html_path: str | None = None


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
    input_retry_fingerprint TEXT,
    output_retry_fingerprint TEXT,
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
    report_html_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
""",
    ),
    Migration(
        version=2,
        name="add_normalized_span_retry_fingerprints",
        sql="""
ALTER TABLE normalized_spans ADD COLUMN IF NOT EXISTS input_retry_fingerprint TEXT;
ALTER TABLE normalized_spans ADD COLUMN IF NOT EXISTS output_retry_fingerprint TEXT;
""",
    ),
    Migration(
        version=3,
        name="add_report_html_path",
        sql="""
ALTER TABLE reports ADD COLUMN IF NOT EXISTS report_html_path TEXT;
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
                            input_retry_fingerprint,
                            output_retry_fingerprint,
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
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
                            span.input_retry_fingerprint,
                            span.output_retry_fingerprint,
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

    def fetch_normalized_span_costs(self) -> list[NormalizedSpanCostRow]:
        self.ensure_schema()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT trace_id, span_id, parent_span_id, status, cost_usd, cost_confidence
                FROM normalized_spans
                ORDER BY trace_id, span_id
                """
            ).fetchall()

        return [NormalizedSpanCostRow(*row) for row in rows]

    def fetch_normalized_spans_for_analysis(self) -> list[NormalizedSpanAnalysisRow]:
        self.ensure_schema()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT trace_id, span_id, parent_span_id, name, span_type,
                       CAST(start_time AS VARCHAR), CAST(end_time AS VARCHAR),
                       status, status_message,
                       error_signature, input_retry_fingerprint, input_preview,
                       output_preview, cost_usd, cost_confidence
                FROM normalized_spans
                ORDER BY trace_id, parent_span_id, name, start_time, span_id
                """
            ).fetchall()

        return [
            NormalizedSpanAnalysisRow(
                trace_id=row[0],
                span_id=row[1],
                parent_span_id=row[2],
                name=row[3],
                span_type=row[4],
                start_time=_datetime_from_store(row[5]),
                end_time=_datetime_from_store(row[6]),
                status=row[7],
                status_message=row[8],
                error_signature=row[9],
                input_retry_fingerprint=row[10],
                input_preview=row[11],
                output_preview=row[12],
                cost_usd=row[13],
                cost_confidence=row[14],
            )
            for row in rows
        ]

    def fetch_normalized_spans_for_agent_analysis(
        self,
    ) -> list[NormalizedSpanAgentAnalysisRow]:
        self.ensure_schema()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT trace_id, span_id, parent_span_id, name, span_type, agent_name,
                       status, input_hash, CAST(start_time AS VARCHAR),
                       CAST(end_time AS VARCHAR), cost_usd, cost_confidence
                FROM normalized_spans
                ORDER BY trace_id, parent_span_id, start_time, span_id
                """
            ).fetchall()

        return [
            NormalizedSpanAgentAnalysisRow(
                trace_id=row[0],
                span_id=row[1],
                parent_span_id=row[2],
                name=row[3],
                span_type=row[4],
                agent_name=row[5],
                status=row[6],
                input_hash=row[7],
                start_time=_datetime_from_store(row[8]),
                end_time=_datetime_from_store(row[9]),
                cost_usd=row[10],
                cost_confidence=row[11],
            )
            for row in rows
        ]

    def replace_cost_ledger(
        self,
        records: Sequence[CostLedgerRecord],
        *,
        attribution_method: str,
    ) -> None:
        if any(record.attribution_method != attribution_method for record in records):
            raise ValueError("Cost ledger records must match the replacement method.")

        self.ensure_schema()
        with self.connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    "DELETE FROM cost_ledger WHERE attribution_method = ?",
                    [attribution_method],
                )
                for record in records:
                    connection.execute(
                        """
                        INSERT INTO cost_ledger (
                            trace_id,
                            span_id,
                            issue_id,
                            cost_type,
                            amount_usd,
                            attribution_method,
                            confidence
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            record.trace_id,
                            record.span_id,
                            record.issue_id,
                            record.cost_type,
                            record.amount_usd,
                            record.attribution_method,
                            record.confidence,
                        ],
                    )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def fetch_cost_ledger(
        self, *, attribution_method: str | None = None
    ) -> list[CostLedgerRecord]:
        self.ensure_schema()
        query = """
            SELECT trace_id, span_id, issue_id, cost_type, amount_usd,
                   attribution_method, confidence
            FROM cost_ledger
        """
        params: list[str] = []
        if attribution_method:
            query += " WHERE attribution_method = ?"
            params.append(attribution_method)
        query += " ORDER BY trace_id, span_id, cost_type, issue_id"

        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [CostLedgerRecord(*row) for row in rows]

    def replace_analysis_results(
        self,
        *,
        issue_kind: str,
        attribution_method: str,
        issues: Sequence[IssueRecord],
        evidence: Sequence[IssueEvidenceRecord],
        cost_records: Sequence[CostLedgerRecord],
    ) -> None:
        if any(issue.kind != issue_kind for issue in issues):
            raise ValueError("Issues must match the replacement kind.")
        if any(record.attribution_method != attribution_method for record in cost_records):
            raise ValueError("Cost ledger records must match the replacement method.")

        issue_ids = {issue.issue_id for issue in issues}
        if any(item.issue_id not in issue_ids for item in evidence):
            raise ValueError("Issue evidence must reference replacement issues.")
        if any(record.issue_id and record.issue_id not in issue_ids for record in cost_records):
            raise ValueError("Cost ledger issue IDs must reference replacement issues.")

        self.ensure_schema()
        with self.connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                existing_issue_ids = [
                    row[0]
                    for row in connection.execute(
                        "SELECT issue_id FROM issues WHERE kind = ?",
                        [issue_kind],
                    ).fetchall()
                ]
                for issue_id in existing_issue_ids:
                    connection.execute(
                        "DELETE FROM issue_evidence WHERE issue_id = ?",
                        [issue_id],
                    )
                connection.execute("DELETE FROM issues WHERE kind = ?", [issue_kind])
                connection.execute(
                    "DELETE FROM cost_ledger WHERE attribution_method = ?",
                    [attribution_method],
                )

                for issue in issues:
                    connection.execute(
                        """
                        INSERT INTO issues (
                            issue_id,
                            kind,
                            title,
                            severity,
                            confidence,
                            first_seen,
                            last_seen,
                            affected_traces,
                            affected_spans,
                            total_cost_usd,
                            wasted_cost_usd,
                            potential_savings_usd,
                            recommendation,
                            recommended_tests_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            issue.issue_id,
                            issue.kind,
                            issue.title,
                            issue.severity,
                            issue.confidence,
                            issue.first_seen,
                            issue.last_seen,
                            issue.affected_traces,
                            issue.affected_spans,
                            issue.total_cost_usd,
                            issue.wasted_cost_usd,
                            issue.potential_savings_usd,
                            issue.recommendation,
                            json.dumps(issue.recommended_tests, ensure_ascii=True),
                        ],
                    )

                for item in evidence:
                    connection.execute(
                        """
                        INSERT INTO issue_evidence (
                            issue_id,
                            trace_id,
                            span_id,
                            evidence_type,
                            message,
                            attributes_json
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            item.issue_id,
                            item.trace_id,
                            item.span_id,
                            item.evidence_type,
                            item.message,
                            json.dumps(
                                item.attributes,
                                ensure_ascii=True,
                                sort_keys=True,
                                default=str,
                            ),
                        ],
                    )

                for record in cost_records:
                    connection.execute(
                        """
                        INSERT INTO cost_ledger (
                            trace_id,
                            span_id,
                            issue_id,
                            cost_type,
                            amount_usd,
                            attribution_method,
                            confidence
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            record.trace_id,
                            record.span_id,
                            record.issue_id,
                            record.cost_type,
                            record.amount_usd,
                            record.attribution_method,
                            record.confidence,
                        ],
                    )

                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def fetch_issues(self, *, kind: str | None = None) -> list[IssueRecord]:
        self.ensure_schema()
        query = """
            SELECT issue_id, kind, title, severity, confidence,
                   CAST(first_seen AS VARCHAR), CAST(last_seen AS VARCHAR),
                   affected_traces, affected_spans, total_cost_usd, wasted_cost_usd,
                   potential_savings_usd, recommendation, recommended_tests_json
            FROM issues
        """
        params: list[str] = []
        if kind:
            query += " WHERE kind = ?"
            params.append(kind)
        query += " ORDER BY issue_id"

        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            IssueRecord(
                issue_id=row[0],
                kind=row[1],
                title=row[2],
                severity=row[3],
                confidence=row[4],
                first_seen=_datetime_from_store(row[5]),
                last_seen=_datetime_from_store(row[6]),
                affected_traces=row[7],
                affected_spans=row[8],
                total_cost_usd=row[9],
                wasted_cost_usd=row[10],
                potential_savings_usd=row[11],
                recommendation=row[12],
                recommended_tests=json.loads(row[13]),
            )
            for row in rows
        ]

    def fetch_issue_evidence(
        self, *, issue_id: str | None = None
    ) -> list[IssueEvidenceRecord]:
        self.ensure_schema()
        query = """
            SELECT issue_id, trace_id, span_id, evidence_type, message, attributes_json
            FROM issue_evidence
        """
        params: list[str] = []
        if issue_id:
            query += " WHERE issue_id = ?"
            params.append(issue_id)
        query += " ORDER BY issue_id, trace_id, span_id, evidence_type"

        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            IssueEvidenceRecord(
                issue_id=row[0],
                trace_id=row[1],
                span_id=row[2],
                evidence_type=row[3],
                message=row[4],
                attributes=json.loads(row[5]),
            )
            for row in rows
        ]

    def upsert_report(self, record: ReportRecord) -> None:
        self.ensure_schema()
        with self.connect() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    "DELETE FROM reports WHERE report_id = ?",
                    [record.report_id],
                )
                connection.execute(
                    """
                    INSERT INTO reports (
                        report_id,
                        project,
                        window_start,
                        window_end,
                        summary_json,
                        report_md_path,
                        report_json_path,
                        report_html_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        record.report_id,
                        record.project,
                        record.window_start,
                        record.window_end,
                        json.dumps(
                            record.summary,
                            ensure_ascii=True,
                            sort_keys=True,
                            default=str,
                        ),
                        record.report_md_path,
                        record.report_json_path,
                        record.report_html_path,
                    ],
                )
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def fetch_reports(self, *, report_id: str | None = None) -> list[ReportRecord]:
        self.ensure_schema()
        query = """
            SELECT report_id, project, CAST(window_start AS VARCHAR),
                   CAST(window_end AS VARCHAR), summary_json, report_md_path,
                   report_json_path, report_html_path
            FROM reports
        """
        params: list[str] = []
        if report_id:
            query += " WHERE report_id = ?"
            params.append(report_id)
        query += " ORDER BY created_at, report_id"

        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()

        return [
            ReportRecord(
                report_id=row[0],
                project=row[1],
                window_start=_datetime_from_store(row[2]),
                window_end=_datetime_from_store(row[3]),
                summary=json.loads(row[4]),
                report_md_path=row[5],
                report_json_path=row[6],
                report_html_path=row[7],
            )
            for row in rows
        ]

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


def _datetime_from_store(value) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    text = re.sub(r"([+-]\d{2})$", r"\1:00", text)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
