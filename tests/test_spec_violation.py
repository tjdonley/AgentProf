from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.analyze.spec_violation import (
    ATTRIBUTION_METHOD,
    ISSUE_KIND,
    analyze_spec_violations,
)
from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH, SpecContractConfig
from agentprof.normalize.runner import build_normalized_traces
from agentprof.normalize.schema import NormalizedSpan
from agentprof.store.duckdb_store import DuckDBStore


runner = CliRunner()


def test_analyze_spec_violations_persists_issue_evidence_and_waste_idempotently(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _spec_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    first_result = analyze_spec_violations(store, contracts=[_contract()])
    second_result = analyze_spec_violations(store, contracts=[_contract()])
    issues = store.fetch_issues(kind=ISSUE_KIND)
    evidence = store.fetch_issue_evidence(issue_id=issues[0].issue_id)
    costs = store.fetch_cost_ledger(attribution_method=ATTRIBUTION_METHOD)

    assert first_result == second_result
    assert first_result.normalized_spans_seen == 2
    assert first_result.contracts_seen == 1
    assert first_result.spec_violations == 1
    assert first_result.affected_traces == 1
    assert first_result.affected_spans == 1
    assert first_result.wasted_cost_usd == Decimal("0.015")
    assert first_result.findings[0].missing_input_fields == ["region"]
    assert first_result.findings[0].missing_output_fields == ["confidence"]

    assert len(issues) == 1
    assert issues[0].kind == "spec_violation"
    assert issues[0].title == "Contract violation in refund_policy_lookup"
    assert issues[0].severity == "medium"
    assert issues[0].total_cost_usd == Decimal("0.015000000")
    assert issues[0].wasted_cost_usd == Decimal("0.015000000")
    assert len(evidence) == 1
    assert evidence[0].span_id == "tool"
    assert evidence[0].attributes["missing_input_fields"] == ["region"]
    assert evidence[0].attributes["missing_output_fields"] == ["confidence"]
    assert len(costs) == 1
    assert costs[0].span_id == "tool"
    assert costs[0].cost_type == "wasted_spec_violation_cost"
    assert costs[0].amount_usd == Decimal("0.015000000")
    assert costs[0].issue_id == issues[0].issue_id

    stats = store.stats()
    assert stats["issues"] == 1
    assert stats["issue_evidence"] == 1
    assert stats["cost_ledger"] == 1


def test_analyze_spec_violations_clears_stale_results(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _spec_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    analyze_spec_violations(store, contracts=[_contract()])

    fixed_spans = [
        span.model_copy(
            update={
                "status": "ok",
                "status_message": None,
                "input_preview": '{"customer_id":"cust_ABC123","region":"US"}',
                "output_preview": '{"answer":"approved","confidence":0.9}',
            }
        )
        if span.span_id == "tool"
        else span
        for span in spans
    ]
    store.replace_normalized(
        spans=fixed_spans,
        traces=build_normalized_traces(fixed_spans),
    )

    result = analyze_spec_violations(store, contracts=[_contract()])

    assert result.spec_violations == 0
    assert store.stats()["issues"] == 0
    assert store.stats()["issue_evidence"] == 0
    assert store.stats()["cost_ledger"] == 0


def test_analyze_spec_violations_uses_span_name_override(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _spec_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    contract = _contract().model_copy(
        update={"name": "refund_tool_contract", "span_name": "refund_policy_lookup"}
    )

    result = analyze_spec_violations(store, contracts=[contract])

    assert result.spec_violations == 1
    assert result.findings[0].contract_name == "refund_tool_contract"


def test_analyze_spec_violations_skips_unparseable_previews_without_error(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [
        span.model_copy(
            update={
                "status": "ok",
                "status_message": None,
                "error_signature": None,
                "input_preview": "truncated preview...",
                "output_preview": "plain text",
            }
        )
        if span.span_id == "tool"
        else span
        for span in _spec_spans()
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_spec_violations(store, contracts=[_contract()])

    assert result.spec_violations == 0


def test_analyze_spec_violations_matches_missing_field_names_exactly(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [
        span.model_copy(
            update={
                "status": "error",
                "status_message": "missing required field customer_id",
                "error_signature": "missing required field customer_id",
                "input_preview": None,
                "output_preview": None,
            }
        )
        if span.span_id == "tool"
        else span
        for span in _spec_spans()
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    contract = SpecContractConfig(
        name="refund_policy_lookup",
        required_input_fields=["id"],
    )

    result = analyze_spec_violations(store, contracts=[contract])

    assert result.spec_violations == 0


def test_analyze_spec_violations_requires_explicit_missing_message(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [
        span.model_copy(
            update={
                "status": "ok",
                "status_message": "validated required field region",
                "error_signature": "validated required field region",
                "input_preview": None,
                "output_preview": None,
            }
        )
        if span.span_id == "tool"
        else span
        for span in _spec_spans()
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_spec_violations(store, contracts=[_contract()])

    assert result.spec_violations == 0


def test_cli_spec_violation_analyzer_reads_configured_contract() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        Path("agentprof.yml").write_text(
            """
analyzers:
  spec_violations:
    contracts:
      - name: refund_policy_lookup
        required_input_fields:
          - customer_id
          - region
store:
  path: .agentprof/data/agentprof.duckdb
""",
            encoding="utf-8",
        )
        store = DuckDBStore(DEFAULT_STORE_PATH)
        spans = _spec_spans()
        store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

        result = runner.invoke(app, ["analyze", "spec-violations"])
        stats = store.stats()
        issues = store.fetch_issues(kind=ISSUE_KIND)

    assert init_result.exit_code == 0
    assert result.exit_code == 0
    assert "Analyzed spec violations" in result.output
    assert "contracts seen: 1" in result.output
    assert "spec violations: 1" in result.output
    assert "refund_policy_lookup" in result.output
    assert stats["issues"] == 1
    assert stats["issue_evidence"] == 1
    assert stats["cost_ledger"] == 1
    assert issues[0].affected_spans == 1


def _contract() -> SpecContractConfig:
    return SpecContractConfig(
        name="refund_policy_lookup",
        required_input_fields=["customer_id", "region"],
        required_output_fields=["answer", "confidence"],
    )


def _spec_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-spec",
            span_id="root",
            source="langfuse",
            name="support_agent",
            span_type="root",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="ok",
        ),
        NormalizedSpan(
            trace_id="trace-spec",
            span_id="tool",
            parent_span_id="root",
            source="langfuse",
            name="refund_policy_lookup",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            end_time=_dt("2026-04-26T10:00:02+00:00"),
            status="error",
            status_message="missing required field region",
            error_signature="missing required field region",
            input_preview='{"customer_id":"cust_ABC123","reason":"refund"}',
            output_preview='{"answer":"approved"}',
            cost_usd=Decimal("0.015"),
            cost_confidence="source",
        ),
    ]


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
