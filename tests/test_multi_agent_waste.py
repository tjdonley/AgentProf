from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.analyze.multi_agent_waste import (
    ATTRIBUTION_METHOD,
    ISSUE_KIND,
    WASTED_COST_TYPE,
    analyze_multi_agent_waste,
)
from agentprof.analyze.retry_loop import analyze_retry_loops
from agentprof.analyze.spec_violation import analyze_spec_violations
from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH, SpecContractConfig
from agentprof.cost.runner import LEDGER_ATTRIBUTION_METHOD, build_cost_ledger
from agentprof.normalize.runner import build_normalized_traces
from agentprof.normalize.schema import NormalizedSpan
from agentprof.store.duckdb_store import DuckDBStore


runner = CliRunner()


def test_analyze_multi_agent_waste_detects_costed_root_and_child_agent_trace(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _multi_agent_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_multi_agent_waste(store)

    assert result.normalized_spans_seen == 2
    assert result.multi_agent_traces == 1
    assert result.affected_traces == 1
    assert result.affected_spans == 2
    assert result.estimated_overhead_usd == Decimal("0.0400")

    finding = result.findings[0]
    assert finding.trace_id == "trace-multi"
    assert finding.root_span_id == "root"
    assert finding.root_name == "manager_agent"
    assert finding.agent_count == 2
    assert finding.agent_names == ["manager_agent", "research_agent"]
    assert finding.actual_cost_usd == Decimal("0.080")
    assert finding.baseline_cost_usd == Decimal("0.0400")
    assert finding.cost_multiple == Decimal("2")


def test_analyze_multi_agent_waste_ignores_root_with_tools_only(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _root_tool_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_multi_agent_waste(store)

    assert result.multi_agent_traces == 0
    assert result.affected_traces == 0
    assert result.findings == []
    assert store.fetch_issues(kind=ISSUE_KIND) == []


def test_analyze_multi_agent_waste_ignores_duplicate_agent_names(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [
        span.model_copy(update={"agent_name": "support_agent", "name": "support_agent"})
        for span in _multi_agent_spans()
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_multi_agent_waste(store)

    assert result.multi_agent_traces == 0
    assert result.affected_traces == 0
    assert result.findings == []


def test_analyze_multi_agent_waste_ignores_multi_agent_traces_without_cost(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [span.model_copy(update={"cost_usd": None}) for span in _multi_agent_spans()]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_multi_agent_waste(store)

    assert result.multi_agent_traces == 1
    assert result.affected_traces == 0
    assert result.estimated_overhead_usd == Decimal("0")
    assert result.findings == []
    assert store.fetch_issues(kind=ISSUE_KIND) == []


def test_analyze_multi_agent_waste_excludes_costed_ancestors(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _aggregate_cost_multi_agent_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = analyze_multi_agent_waste(store)
    issues = store.fetch_issues(kind=ISSUE_KIND)
    costs = store.fetch_cost_ledger(attribution_method=ATTRIBUTION_METHOD)

    assert result.affected_traces == 1
    assert result.findings[0].actual_cost_usd == Decimal("0.030")
    assert result.findings[0].estimated_overhead_usd == Decimal("0.0150")
    assert issues[0].total_cost_usd == Decimal("0.030")
    assert issues[0].wasted_cost_usd == Decimal("0.0150")
    assert costs[0].amount_usd == Decimal("0.0150")


def test_analyze_multi_agent_waste_persists_issue_evidence_and_waste_idempotently(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _multi_agent_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    first_result = analyze_multi_agent_waste(store)
    second_result = analyze_multi_agent_waste(store)
    issues = store.fetch_issues(kind=ISSUE_KIND)
    evidence = store.fetch_issue_evidence(issue_id=issues[0].issue_id)
    costs = store.fetch_cost_ledger(attribution_method=ATTRIBUTION_METHOD)

    assert first_result == second_result
    assert len(issues) == 1
    assert issues[0].kind == "multi_agent_waste"
    assert issues[0].confidence == "medium"
    assert issues[0].affected_traces == 1
    assert issues[0].affected_spans == 2
    assert issues[0].total_cost_usd == Decimal("0.080")
    assert issues[0].wasted_cost_usd == Decimal("0.040")
    assert len(evidence) == 1
    assert evidence[0].span_id == "root"
    assert evidence[0].attributes["basis"] == "configured_ratio_estimate"
    assert evidence[0].attributes["agent_names"] == ["manager_agent", "research_agent"]
    assert len(costs) == 1
    assert costs[0].span_id == "root"
    assert costs[0].cost_type == WASTED_COST_TYPE
    assert costs[0].amount_usd == Decimal("0.040")
    assert costs[0].attribution_method == ATTRIBUTION_METHOD
    assert costs[0].confidence == "estimated"
    assert costs[0].issue_id == issues[0].issue_id

    stats = store.stats()
    assert stats["issues"] == 1
    assert stats["issue_evidence"] == 1
    assert stats["cost_ledger"] == 1


def test_analyze_multi_agent_waste_clears_stale_results_only(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _multi_agent_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    analyze_multi_agent_waste(store)
    old_issue_id = store.fetch_issues(kind=ISSUE_KIND)[0].issue_id

    fixed_spans = [
        span.model_copy(update={"trace_id": "trace-multi"})
        for span in _root_tool_spans()
    ]
    store.replace_normalized(
        spans=fixed_spans,
        traces=build_normalized_traces(fixed_spans),
    )
    result = analyze_multi_agent_waste(store)

    assert result.affected_traces == 0
    assert store.fetch_issues(kind=ISSUE_KIND) == []
    assert store.fetch_issue_evidence(issue_id=old_issue_id) == []
    assert store.fetch_cost_ledger(attribution_method=ATTRIBUTION_METHOD) == []


def test_analyze_multi_agent_waste_preserves_retry_and_spec_results(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _retry_and_spec_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    retry_result = analyze_retry_loops(store)
    spec_result = analyze_spec_violations(store, contracts=[_contract()])

    result = analyze_multi_agent_waste(store)

    assert retry_result.retry_loops == 1
    assert spec_result.spec_violations == 2
    assert result.affected_traces == 0
    assert len(store.fetch_issues(kind="retry_loop")) == 1
    assert len(store.fetch_issues(kind="spec_violation")) == 2
    assert store.fetch_issues(kind=ISSUE_KIND) == []


def test_cost_ledger_preserves_multi_agent_waste_entries(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _multi_agent_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    analyze_multi_agent_waste(store)

    result = build_cost_ledger(store)
    multi_agent_costs = store.fetch_cost_ledger(attribution_method=ATTRIBUTION_METHOD)
    status_costs = store.fetch_cost_ledger(attribution_method=LEDGER_ATTRIBUTION_METHOD)

    assert result.ledger_entries == 1
    assert len(multi_agent_costs) == 1
    assert multi_agent_costs[0].cost_type == WASTED_COST_TYPE
    assert multi_agent_costs[0].amount_usd == Decimal("0.040")
    assert len(status_costs) == 1
    assert status_costs[0].span_id == "research-agent"


def test_cli_multi_agent_waste_analyzer_detects_store_spans() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        store = DuckDBStore(DEFAULT_STORE_PATH)
        spans = _multi_agent_spans()
        store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

        result = runner.invoke(app, ["analyze", "multi-agent-waste"])
        stats = store.stats()
        issues = store.fetch_issues(kind=ISSUE_KIND)

    assert init_result.exit_code == 0
    assert result.exit_code == 0
    assert "Analyzed multi-agent waste" in result.output
    assert "multi-agent traces: 1" in result.output
    assert "estimated orchestration overhead" in result.output
    assert stats["issues"] == 1
    assert stats["issue_evidence"] == 1
    assert stats["cost_ledger"] == 1
    assert issues[0].affected_spans == 2


def test_cli_multi_agent_waste_validates_baseline_ratio() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        store = DuckDBStore(DEFAULT_STORE_PATH)
        spans = _multi_agent_spans()
        store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

        zero_result = runner.invoke(
            app,
            ["analyze", "multi-agent-waste", "--baseline-ratio", "0"],
        )
        one_result = runner.invoke(
            app,
            ["analyze", "multi-agent-waste", "--baseline-ratio", "1"],
        )

    assert init_result.exit_code == 0
    assert zero_result.exit_code == 2
    assert "baseline_ratio" in zero_result.output
    assert one_result.exit_code == 2
    assert "baseline_ratio" in one_result.output


def _multi_agent_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-multi",
            span_id="root",
            source="langfuse",
            name="manager_agent",
            span_type="root",
            agent_name="manager_agent",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="ok",
        ),
        NormalizedSpan(
            trace_id="trace-multi",
            span_id="research-agent",
            parent_span_id="root",
            source="langfuse",
            name="research_agent",
            span_type="agent",
            agent_name="research_agent",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            end_time=_dt("2026-04-26T10:00:04+00:00"),
            status="ok",
            cost_usd=Decimal("0.080"),
            cost_confidence="source",
        ),
    ]


def _root_tool_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-root-tools",
            span_id="root",
            source="langfuse",
            name="support_agent",
            span_type="root",
            agent_name="support_agent",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:03+00:00"),
            status="ok",
        ),
        NormalizedSpan(
            trace_id="trace-root-tools",
            span_id="tool",
            parent_span_id="root",
            source="langfuse",
            name="policy_lookup",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            end_time=_dt("2026-04-26T10:00:02+00:00"),
            status="ok",
            cost_usd=Decimal("0.020"),
            cost_confidence="source",
        ),
    ]


def _aggregate_cost_multi_agent_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-aggregate-multi",
            span_id="root",
            source="langfuse",
            name="manager_agent",
            span_type="root",
            agent_name="manager_agent",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            status="ok",
            cost_usd=Decimal("0.100"),
            cost_confidence="source",
        ),
        NormalizedSpan(
            trace_id="trace-aggregate-multi",
            span_id="research-agent",
            parent_span_id="root",
            source="langfuse",
            name="research_agent",
            span_type="agent",
            agent_name="research_agent",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            status="ok",
            cost_usd=Decimal("0.080"),
            cost_confidence="source",
        ),
        NormalizedSpan(
            trace_id="trace-aggregate-multi",
            span_id="research-llm",
            parent_span_id="research-agent",
            source="langfuse",
            name="research_generation",
            span_type="llm",
            start_time=_dt("2026-04-26T10:00:02+00:00"),
            status="ok",
            cost_usd=Decimal("0.030"),
            cost_confidence="source",
        ),
    ]


def _retry_and_spec_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-retry-spec",
            span_id="root",
            source="langfuse",
            name="support_agent",
            span_type="root",
            agent_name="support_agent",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="ok",
        ),
        NormalizedSpan(
            trace_id="trace-retry-spec",
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
            input_preview='{"customer_id":"cust_ABC123"}',
            cost_usd=Decimal("0.010"),
            cost_confidence="source",
        ),
        NormalizedSpan(
            trace_id="trace-retry-spec",
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
            input_preview='{"customer_id":"cust_ABC123"}',
            cost_usd=Decimal("0.020"),
            cost_confidence="source",
        ),
    ]


def _contract() -> SpecContractConfig:
    return SpecContractConfig(
        name="refund_policy_lookup",
        required_input_fields=["customer_id", "region"],
    )


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
