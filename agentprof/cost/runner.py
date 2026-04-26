from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from agentprof.cost.schema import CostLedgerBuildResult, CostType, CostWaterfallRow
from agentprof.store.duckdb_store import (
    CostLedgerRecord,
    DuckDBStore,
    NormalizedSpanCostRow,
)


LEDGER_ATTRIBUTION_METHOD = "normalized_span_status"
FAILURE_STATUSES = {"error", "timeout", "cancelled"}
COST_TYPE_ORDER: tuple[CostType, ...] = (
    "successful_span_cost",
    "failed_span_cost",
    "unknown_span_cost",
)


def build_cost_ledger(store: DuckDBStore) -> CostLedgerBuildResult:
    spans = store.fetch_normalized_span_costs()
    records = [
        _cost_record_from_span(span)
        for span in _cost_leaf_spans(spans)
    ]

    store.replace_cost_ledger(
        records,
        attribution_method=LEDGER_ATTRIBUTION_METHOD,
    )
    return CostLedgerBuildResult(
        normalized_spans_seen=len(spans),
        ledger_entries=len(records),
        traces_with_cost=len({record.trace_id for record in records}),
        total_cost_usd=_sum_amounts(record.amount_usd for record in records),
        waterfall=cost_waterfall(records),
    )


def cost_waterfall(records: list[CostLedgerRecord]) -> list[CostWaterfallRow]:
    grouped: dict[CostType, list[CostLedgerRecord]] = defaultdict(list)
    for record in records:
        if record.cost_type in COST_TYPE_ORDER:
            grouped[record.cost_type].append(record)

    return [
        CostWaterfallRow(
            cost_type=cost_type,
            entries=len(grouped[cost_type]),
            amount_usd=_sum_amounts(record.amount_usd for record in grouped[cost_type]),
        )
        for cost_type in COST_TYPE_ORDER
        if grouped[cost_type]
    ]


def _cost_record_from_span(span: NormalizedSpanCostRow) -> CostLedgerRecord:
    return CostLedgerRecord(
        trace_id=span.trace_id,
        span_id=span.span_id,
        issue_id=None,
        cost_type=_cost_type_for_status(span.status),
        amount_usd=span.cost_usd,
        attribution_method=LEDGER_ATTRIBUTION_METHOD,
        confidence=span.cost_confidence,
    )


def _cost_leaf_spans(spans: list[NormalizedSpanCostRow]) -> list[NormalizedSpanCostRow]:
    costed_spans = [span for span in spans if span.cost_usd is not None]
    by_id = {(span.trace_id, span.span_id): span for span in spans}
    ancestors_with_costed_descendants: set[tuple[str, str]] = set()
    for span in costed_spans:
        parent_id = span.parent_span_id
        trace_id = span.trace_id
        visited: set[str] = set()
        while parent_id and (trace_id, parent_id) in by_id and parent_id not in visited:
            ancestors_with_costed_descendants.add((trace_id, parent_id))
            visited.add(parent_id)
            parent_id = by_id[(trace_id, parent_id)].parent_span_id

    return [
        span
        for span in costed_spans
        if (span.trace_id, span.span_id) not in ancestors_with_costed_descendants
    ]


def _cost_type_for_status(status: str) -> CostType:
    if status == "ok":
        return "successful_span_cost"
    if status in FAILURE_STATUSES:
        return "failed_span_cost"
    return "unknown_span_cost"


def _sum_amounts(values) -> Decimal:
    return sum((value for value in values if value is not None), Decimal("0"))
