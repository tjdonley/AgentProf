from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH
from agentprof.normalize.langfuse import map_langfuse_raw_span
from agentprof.normalize.runner import (
    build_normalized_traces,
    compute_data_quality,
    normalize_store,
)
from agentprof.normalize.schema import NormalizedSpan
from agentprof.store.duckdb_store import DuckDBStore, RawSpanRecord, RawSpanRow


runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures"


def test_map_langfuse_raw_span_preserves_metrics_privacy_and_trace_attributes() -> None:
    row = RawSpanRow(
        source="langfuse",
        source_id="gen-1",
        trace_id=None,
        span_id=None,
        parent_span_id=None,
        payload_json=json.dumps(
            {
                "id": "gen-1",
                "traceId": "trace-1",
                "parentObservationId": "root-1",
                "type": "GENERATION",
                "name": "answer_user",
                "startTime": "2026-04-26T10:00:00.000Z",
                "endTime": "2026-04-26T10:00:01.250Z",
                "providedModelName": "gpt-4o-mini",
                "provider": "openai",
                "usageDetails": {"input": 9, "output": 12, "total": 21},
                "inputUsage": 0,
                "costDetails": {"total": "0.00123"},
                "level": "ERROR",
                "statusMessage": "Timeout 504 after 30000 ms",
                "metadata": {"error_type": "TimeoutError"},
                "sessionId": "session-1",
                "userId": "user-hash-1",
                "environment": "prod",
                "version": "2026.04.26",
                "promptName": "refund_flow",
                "_agentprof_privacy": {
                    "input_hash": "input-hash",
                    "output_hash": "output-hash",
                    "input_preview": "redacted input",
                    "output_preview": "redacted output",
                },
            }
        ),
        raw_ref="observations.json",
    )

    span = map_langfuse_raw_span(row)

    assert span.trace_id == "trace-1"
    assert span.span_id == "gen-1"
    assert span.parent_span_id == "root-1"
    assert span.span_type == "llm"
    assert span.model_name == "gpt-4o-mini"
    assert span.provider_name == "openai"
    assert span.duration_ms == 1250.0
    assert span.status == "error"
    assert span.error_type == "TimeoutError"
    assert span.error_signature == "timeout # after # ms"
    assert span.input_tokens == 0
    assert span.output_tokens == 12
    assert span.total_tokens == 21
    assert span.cost_usd == Decimal("0.00123")
    assert span.cost_confidence == "source"
    assert span.input_hash == "input-hash"
    assert span.output_preview == "redacted output"
    assert span.attributes["promptName"] == "refund_flow"
    assert span.attributes["sessionId"] == "session-1"
    assert span.attributes["environment"] == "prod"


def test_build_normalized_traces_rolls_up_tree_metrics_and_outcome() -> None:
    traces = build_normalized_traces(_normalized_spans())

    assert len(traces) == 1
    trace = traces[0]
    assert trace.trace_id == "trace-1"
    assert trace.root_span_id == "root"
    assert trace.root_name == "support_agent"
    assert trace.session_id == "session-1"
    assert trace.user_hash == "user-hash-1"
    assert trace.environment == "prod"
    assert trace.version == "2026.04.26"
    assert trace.duration_ms == 5000.0
    assert trace.outcome == "failure"
    assert trace.total_cost_usd == Decimal("0.03")
    assert trace.total_input_tokens == 10
    assert trace.total_output_tokens == 20
    assert trace.total_tool_calls == 1
    assert trace.total_model_calls == 1


def test_compute_data_quality_reports_coverage() -> None:
    spans = _normalized_spans() + [
        NormalizedSpan(
            trace_id="trace-1",
            span_id="orphan",
            parent_span_id="missing-parent",
            source="langfuse",
            name="orphan_tool",
            span_type="tool",
            status="ok",
        )
    ]

    quality = compute_data_quality(spans, build_normalized_traces(spans))

    assert quality.total_spans == 4
    assert quality.total_traces == 1
    assert quality.spans_with_parent_ids == 3
    assert quality.spans_with_valid_parent_links == 3
    assert quality.spans_with_status == 4
    assert quality.spans_with_cost == 2
    assert quality.spans_with_token_counts == 1
    assert quality.spans_with_model == 1
    assert quality.spans_with_io_hashes == 1
    assert quality.parent_coverage_pct == 75.0
    assert quality.cost_coverage_pct == 50.0
    assert quality.token_coverage_pct == 25.0


def test_normalize_store_persists_normalized_rows_idempotently(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    store.insert_raw_spans(_raw_records())

    first_result = normalize_store(store)
    second_result = normalize_store(store)

    assert first_result == second_result
    assert first_result.raw_spans_seen == 3
    assert first_result.normalized_spans == 3
    assert first_result.normalized_traces == 1
    assert store.stats()["normalized_spans"] == 3
    assert store.stats()["normalized_traces"] == 1

    with store.connect() as connection:
        trace = connection.execute(
            """
            SELECT root_span_id, outcome, total_tool_calls, total_model_calls,
                   total_input_tokens, total_output_tokens, total_cost_usd
            FROM normalized_traces
            WHERE trace_id = 'trace-store'
            """
        ).fetchone()
        spans = connection.execute(
            """
            SELECT span_id, span_type, status, input_tokens, output_tokens, cost_confidence
            FROM normalized_spans
            WHERE trace_id = 'trace-store'
            ORDER BY span_id
            """
        ).fetchall()

    assert trace == ("root", "failure", 1, 1, 7, 11, Decimal("0.015000000"))
    assert spans == [
        ("llm", "llm", "ok", 7, 11, "source"),
        ("root", "root", "ok", None, None, "unknown"),
        ("tool", "tool", "error", None, None, "unknown"),
    ]


def test_cli_normalize_imported_langfuse_fixture(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt")
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        import_result = runner.invoke(
            app,
            [
                "import",
                "langfuse-export",
                "--observations",
                str(FIXTURES / "langfuse_observations.json"),
            ],
        )
        normalize_result = runner.invoke(app, ["normalize"])

        store = DuckDBStore(DEFAULT_STORE_PATH)
        stats = store.stats()

    assert init_result.exit_code == 0
    assert import_result.exit_code == 0
    assert normalize_result.exit_code == 0
    assert "Normalized imported spans" in normalize_result.output
    assert "raw spans seen: 3" in normalize_result.output
    assert "normalized spans: 3" in normalize_result.output
    assert "normalized traces: 1" in normalize_result.output
    assert "Data quality" in normalize_result.output
    assert stats["normalized_spans"] == 3
    assert stats["normalized_traces"] == 1


def _normalized_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-1",
            span_id="root",
            source="langfuse",
            name="support_agent",
            span_type="root",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="ok",
            input_hash="root-input-hash",
            attributes={
                "sessionId": "session-1",
                "userId": "user-hash-1",
                "environment": "prod",
                "version": "2026.04.26",
            },
        ),
        NormalizedSpan(
            trace_id="trace-1",
            span_id="llm",
            parent_span_id="root",
            source="langfuse",
            name="answer_user",
            span_type="llm",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            end_time=_dt("2026-04-26T10:00:03+00:00"),
            status="ok",
            model_name="gpt-4o-mini",
            input_tokens=10,
            output_tokens=20,
            cost_usd=Decimal("0.02"),
        ),
        NormalizedSpan(
            trace_id="trace-1",
            span_id="tool",
            parent_span_id="root",
            source="langfuse",
            name="lookup_tool",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:03+00:00"),
            end_time=_dt("2026-04-26T10:00:04+00:00"),
            status="error",
            cost_usd=Decimal("0.01"),
        ),
    ]


def _raw_records() -> list[RawSpanRecord]:
    payloads = [
        {
            "id": "root",
            "traceId": "trace-store",
            "type": "SPAN",
            "name": "support_agent",
            "startTime": "2026-04-26T10:00:00.000Z",
            "endTime": "2026-04-26T10:00:05.000Z",
            "level": "DEFAULT",
        },
        {
            "id": "llm",
            "traceId": "trace-store",
            "parentObservationId": "root",
            "type": "GENERATION",
            "name": "answer_user",
            "startTime": "2026-04-26T10:00:01.000Z",
            "endTime": "2026-04-26T10:00:03.000Z",
            "level": "DEFAULT",
            "model": "gpt-4o-mini",
            "usageDetails": {"input": 7, "output": 11, "total": 18},
            "costDetails": {"total": "0.015"},
        },
        {
            "id": "tool",
            "traceId": "trace-store",
            "parentObservationId": "root",
            "type": "SPAN",
            "name": "lookup_tool",
            "startTime": "2026-04-26T10:00:03.000Z",
            "endTime": "2026-04-26T10:00:04.000Z",
            "level": "ERROR",
            "statusMessage": "missing required field region",
        },
    ]
    return [
        RawSpanRecord(
            source="langfuse",
            source_id=payload["id"],
            trace_id=payload["traceId"],
            span_id=payload["id"],
            parent_span_id=payload.get("parentObservationId"),
            payload_json=json.dumps(payload),
            raw_ref="fixture",
        )
        for payload in payloads
    ]


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
