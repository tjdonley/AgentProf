from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.analyze.retry_loop import ATTRIBUTION_METHOD, ISSUE_KIND, analyze_retry_loops
from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH
from agentprof.normalize.runner import build_normalized_traces
from agentprof.normalize.schema import NormalizedSpan
from agentprof.store.duckdb_store import DuckDBStore


runner = CliRunner()
FIXTURES = Path(__file__).parent / "fixtures"


def test_analyze_retry_loops_persists_issue_evidence_and_waste_idempotently(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _retry_loop_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    first_result = analyze_retry_loops(store)
    second_result = analyze_retry_loops(store)
    issues = store.fetch_issues(kind=ISSUE_KIND)
    evidence = store.fetch_issue_evidence(issue_id=issues[0].issue_id)
    costs = store.fetch_cost_ledger(attribution_method=ATTRIBUTION_METHOD)

    assert first_result == second_result
    assert first_result.normalized_spans_seen == 4
    assert first_result.retry_loops == 1
    assert first_result.affected_traces == 1
    assert first_result.affected_spans == 2
    assert first_result.wasted_attempts == 1
    assert first_result.wasted_cost_usd == Decimal("0.020")
    assert first_result.findings[0].affected_span_ids == ["attempt-1", "attempt-2"]
    assert first_result.findings[0].wasted_span_ids == ["attempt-2"]

    assert len(issues) == 1
    assert issues[0].kind == "retry_loop"
    assert issues[0].title == "Repeated failing call to refund_policy_lookup"
    assert issues[0].affected_spans == 2
    assert issues[0].total_cost_usd == Decimal("0.030000000")
    assert issues[0].wasted_cost_usd == Decimal("0.020000000")
    assert len(evidence) == 2
    assert evidence[0].span_id == "attempt-1"
    assert evidence[1].span_id == "attempt-2"
    assert evidence[1].attributes["attempt_index"] == 2
    assert len(costs) == 1
    assert costs[0].span_id == "attempt-2"
    assert costs[0].cost_type == "wasted_retry_cost"
    assert costs[0].amount_usd == Decimal("0.020000000")
    assert costs[0].issue_id == issues[0].issue_id

    stats = store.stats()
    assert stats["issues"] == 1
    assert stats["issue_evidence"] == 2
    assert stats["cost_ledger"] == 1


def test_analyze_retry_loops_clears_stale_results(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _retry_loop_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    analyze_retry_loops(store)

    non_retry_spans = [
        span.model_copy(update={"input_retry_fingerprint": f"fingerprint-{index}"})
        for index, span in enumerate(spans, start=1)
    ]
    store.replace_normalized(
        spans=non_retry_spans,
        traces=build_normalized_traces(non_retry_spans),
    )

    result = analyze_retry_loops(store)

    assert result.retry_loops == 0
    assert store.stats()["issues"] == 0
    assert store.stats()["issue_evidence"] == 0
    assert store.stats()["cost_ledger"] == 0


def test_analyze_retry_loops_handles_mixed_parent_keys(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _retry_loop_spans() + [
        NormalizedSpan(
            trace_id="trace-retry",
            span_id="root-attempt-1",
            source="langfuse",
            name="root_generation",
            span_type="llm",
            start_time=_dt("2026-04-26T10:00:05+00:00"),
            status="error",
            error_signature="rate limited",
            input_retry_fingerprint="root-input",
        ),
        NormalizedSpan(
            trace_id="trace-retry",
            span_id="root-attempt-2",
            source="langfuse",
            name="root_generation",
            span_type="llm",
            start_time=_dt("2026-04-26T10:00:06+00:00"),
            status="error",
            error_signature="rate limited",
            input_retry_fingerprint="root-input",
        ),
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_retry_loops(store)

    assert result.retry_loops == 2
    assert [finding.name for finding in result.findings] == [
        "root_generation",
        "refund_policy_lookup",
    ]


def test_analyze_retry_loops_requires_min_attempts_at_least_two(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    try:
        analyze_retry_loops(store, min_attempts=1)
    except ValueError as exc:
        assert "min_attempts" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_cli_retry_loop_analyzer_detects_fixture(monkeypatch) -> None:
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
        analyze_result = runner.invoke(app, ["analyze", "retry-loops"])

        store = DuckDBStore(DEFAULT_STORE_PATH)
        stats = store.stats()
        issues = store.fetch_issues(kind=ISSUE_KIND)
        evidence = store.fetch_issue_evidence(issue_id=issues[0].issue_id)

    assert init_result.exit_code == 0
    assert import_result.exit_code == 0
    assert normalize_result.exit_code == 0
    assert analyze_result.exit_code == 0
    assert "Analyzed retry loops" in analyze_result.output
    assert "retry loops: 1" in analyze_result.output
    assert "refund_policy_lookup" in analyze_result.output
    assert stats["issues"] == 1
    assert stats["issue_evidence"] == 2
    assert stats["cost_ledger"] == 0
    assert issues[0].affected_spans == 2
    assert len(evidence) == 2


def _retry_loop_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-retry",
            span_id="root",
            source="langfuse",
            name="support_agent",
            span_type="root",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="ok",
        ),
        NormalizedSpan(
            trace_id="trace-retry",
            span_id="attempt-1",
            parent_span_id="root",
            source="langfuse",
            name="refund_policy_lookup",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            end_time=_dt("2026-04-26T10:00:02+00:00"),
            status="error",
            status_message="missing required field region",
            error_signature="missing required field region",
            input_retry_fingerprint="same-input",
            input_preview='{"customer_id":"cust_ABC123","reason":"refund"}',
            cost_usd=Decimal("0.010"),
            cost_confidence="source",
        ),
        NormalizedSpan(
            trace_id="trace-retry",
            span_id="attempt-2",
            parent_span_id="root",
            source="langfuse",
            name="refund_policy_lookup",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:02+00:00"),
            end_time=_dt("2026-04-26T10:00:03+00:00"),
            status="error",
            status_message="missing required field region",
            error_signature="missing required field region",
            input_retry_fingerprint="same-input",
            input_preview='{"customer_id":"cust_ABC123","reason":"refund"}',
            cost_usd=Decimal("0.020"),
            cost_confidence="source",
        ),
        NormalizedSpan(
            trace_id="trace-retry",
            span_id="different-error",
            parent_span_id="root",
            source="langfuse",
            name="refund_policy_lookup",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:03+00:00"),
            end_time=_dt("2026-04-26T10:00:04+00:00"),
            status="error",
            status_message="timeout",
            error_signature="timeout",
            input_retry_fingerprint="same-input",
            cost_usd=Decimal("0.030"),
            cost_confidence="source",
        ),
    ]


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
