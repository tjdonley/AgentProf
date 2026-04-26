from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH
from agentprof.cost.runner import LEDGER_ATTRIBUTION_METHOD, build_cost_ledger
from agentprof.normalize.runner import build_normalized_traces
from agentprof.normalize.schema import NormalizedSpan
from agentprof.store.duckdb_store import CostLedgerRecord, DuckDBStore


runner = CliRunner()


def test_build_cost_ledger_persists_status_waterfall_idempotently(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _cost_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    first_result = build_cost_ledger(store)
    second_result = build_cost_ledger(store)
    records = store.fetch_cost_ledger(attribution_method=LEDGER_ATTRIBUTION_METHOD)
    by_span = {record.span_id: record for record in records}

    assert first_result == second_result
    assert first_result.normalized_spans_seen == 4
    assert first_result.ledger_entries == 3
    assert first_result.traces_with_cost == 1
    assert first_result.total_cost_usd == Decimal("0.018")
    assert store.stats()["cost_ledger"] == 3
    assert _waterfall_amounts(first_result) == {
        "successful_span_cost": Decimal("0.010"),
        "failed_span_cost": Decimal("0.006"),
        "unknown_span_cost": Decimal("0.002"),
    }

    assert by_span["llm-ok"].cost_type == "successful_span_cost"
    assert by_span["tool-error"].cost_type == "failed_span_cost"
    assert by_span["retriever-unknown"].cost_type == "unknown_span_cost"
    assert by_span["tool-error"].issue_id is None
    assert by_span["tool-error"].attribution_method == LEDGER_ATTRIBUTION_METHOD
    assert by_span["tool-error"].confidence == "estimated"


def test_build_cost_ledger_clears_stale_entries_when_costs_disappear(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = _cost_spans()
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))
    build_cost_ledger(store)

    no_cost_spans = [span.model_copy(update={"cost_usd": None}) for span in spans]
    store.replace_normalized(
        spans=no_cost_spans,
        traces=build_normalized_traces(no_cost_spans),
    )
    result = build_cost_ledger(store)

    assert result.ledger_entries == 0
    assert result.total_cost_usd == Decimal("0")
    assert result.waterfall == []
    assert store.stats()["cost_ledger"] == 0


def test_build_cost_ledger_excludes_costed_ancestors(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [
        NormalizedSpan(
            trace_id="trace-cost-rollup",
            span_id="root",
            source="langfuse",
            name="root",
            span_type="root",
            status="ok",
            cost_usd=Decimal("0.10"),
        ),
        NormalizedSpan(
            trace_id="trace-cost-rollup",
            span_id="llm",
            parent_span_id="root",
            source="langfuse",
            name="llm",
            span_type="llm",
            status="ok",
            cost_usd=Decimal("0.04"),
        ),
        NormalizedSpan(
            trace_id="trace-cost-rollup",
            span_id="tool",
            parent_span_id="root",
            source="langfuse",
            name="tool",
            span_type="tool",
            status="error",
            cost_usd=Decimal("0.01"),
        ),
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = build_cost_ledger(store)
    records = store.fetch_cost_ledger(attribution_method=LEDGER_ATTRIBUTION_METHOD)

    assert result.ledger_entries == 2
    assert result.total_cost_usd == Decimal("0.05")
    assert {record.span_id for record in records} == {"llm", "tool"}


def test_build_cost_ledger_keeps_same_span_ids_separate_by_trace(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    spans = [
        NormalizedSpan(
            trace_id="trace-a",
            span_id="root",
            source="langfuse",
            name="root",
            span_type="root",
            status="ok",
            cost_usd=Decimal("0.10"),
        ),
        NormalizedSpan(
            trace_id="trace-a",
            span_id="child",
            parent_span_id="root",
            source="langfuse",
            name="child",
            span_type="llm",
            status="ok",
            cost_usd=Decimal("0.04"),
        ),
        NormalizedSpan(
            trace_id="trace-b",
            span_id="root",
            source="langfuse",
            name="root",
            span_type="root",
            status="ok",
            cost_usd=Decimal("0.03"),
        ),
    ]
    store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

    result = build_cost_ledger(store)
    records = store.fetch_cost_ledger(attribution_method=LEDGER_ATTRIBUTION_METHOD)

    assert result.ledger_entries == 2
    assert result.total_cost_usd == Decimal("0.07")
    assert {(record.trace_id, record.span_id) for record in records} == {
        ("trace-a", "child"),
        ("trace-b", "root"),
    }


def test_replace_cost_ledger_rejects_mismatched_attribution_method(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    try:
        store.replace_cost_ledger(
            [
                CostLedgerRecord(
                    trace_id="trace-1",
                    span_id="span-1",
                    issue_id=None,
                    cost_type="successful_span_cost",
                    amount_usd=Decimal("0.01"),
                    attribution_method="other_method",
                    confidence="source",
                )
            ],
            attribution_method=LEDGER_ATTRIBUTION_METHOD,
        )
    except ValueError as exc:
        assert "match the replacement method" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_cli_cost_ledger_builds_waterfall() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        store = DuckDBStore(DEFAULT_STORE_PATH)
        spans = _cost_spans()
        store.replace_normalized(spans=spans, traces=build_normalized_traces(spans))

        result = runner.invoke(app, ["cost", "ledger"])
        stats = store.stats()

    assert init_result.exit_code == 0
    assert result.exit_code == 0
    assert "Built cost ledger" in result.output
    assert "normalized spans seen: 4" in result.output
    assert "ledger entries: 3" in result.output
    assert "Cost waterfall" in result.output
    assert "Successful span cost" in result.output
    assert stats["cost_ledger"] == 3


def _cost_spans() -> list[NormalizedSpan]:
    return [
        NormalizedSpan(
            trace_id="trace-cost-1",
            span_id="root",
            source="langfuse",
            name="support_agent",
            span_type="root",
            start_time=_dt("2026-04-26T10:00:00+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="ok",
        ),
        NormalizedSpan(
            trace_id="trace-cost-1",
            span_id="llm-ok",
            parent_span_id="root",
            source="langfuse",
            name="answer_user",
            span_type="llm",
            start_time=_dt("2026-04-26T10:00:01+00:00"),
            end_time=_dt("2026-04-26T10:00:03+00:00"),
            status="ok",
            cost_usd=Decimal("0.010"),
            cost_confidence="source",
        ),
        NormalizedSpan(
            trace_id="trace-cost-1",
            span_id="tool-error",
            parent_span_id="root",
            source="langfuse",
            name="lookup_tool",
            span_type="tool",
            start_time=_dt("2026-04-26T10:00:03+00:00"),
            end_time=_dt("2026-04-26T10:00:04+00:00"),
            status="error",
            cost_usd=Decimal("0.006"),
            cost_confidence="estimated",
        ),
        NormalizedSpan(
            trace_id="trace-cost-1",
            span_id="retriever-unknown",
            parent_span_id="root",
            source="langfuse",
            name="lookup_context",
            span_type="retriever",
            start_time=_dt("2026-04-26T10:00:04+00:00"),
            end_time=_dt("2026-04-26T10:00:05+00:00"),
            status="unknown",
            cost_usd=Decimal("0.002"),
            cost_confidence="unknown",
        ),
    ]


def _waterfall_amounts(result) -> dict[str, Decimal]:
    return {row.cost_type: row.amount_usd for row in result.waterfall}


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)
