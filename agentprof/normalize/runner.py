from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from math import inf

from agentprof.normalize.langfuse import map_langfuse_raw_span
from agentprof.normalize.schema import (
    DataQualityMetrics,
    NormalizationResult,
    NormalizedSpan,
    NormalizedTrace,
)
from agentprof.store.duckdb_store import DuckDBStore, RawSpanRow


def normalize_store(store: DuckDBStore, *, source: str | None = None) -> NormalizationResult:
    raw_rows = store.fetch_raw_spans(source=source)
    spans = [_map_raw_span(row) for row in raw_rows if _is_supported_source(row.source)]
    traces = build_normalized_traces(spans)
    data_quality = compute_data_quality(spans, traces)
    store.replace_normalized(spans=spans, traces=traces)
    return NormalizationResult(
        raw_spans_seen=len(raw_rows),
        normalized_spans=len(spans),
        normalized_traces=len(traces),
        data_quality=data_quality,
    )


def build_normalized_traces(spans: list[NormalizedSpan]) -> list[NormalizedTrace]:
    by_trace: dict[str, list[NormalizedSpan]] = defaultdict(list)
    for span in spans:
        by_trace[span.trace_id].append(span)

    traces: list[NormalizedTrace] = []
    for trace_id, trace_spans in sorted(by_trace.items()):
        ordered = sorted(
            trace_spans,
            key=lambda span: (_datetime_sort_value(span.start_time), span.span_id),
        )
        span_ids = {span.span_id for span in trace_spans}
        root = _trace_root(ordered, span_ids)
        start_time = _min_datetime(span.start_time for span in trace_spans)
        end_time = _max_datetime(span.end_time for span in trace_spans)
        total_cost = _trace_cost_usd(trace_spans)
        total_input_tokens = _sum_ints(span.input_tokens for span in trace_spans)
        total_output_tokens = _sum_ints(span.output_tokens for span in trace_spans)

        traces.append(
            NormalizedTrace(
                trace_id=trace_id,
                source=trace_spans[0].source,
                root_span_id=root.span_id if root else None,
                root_name=root.name if root else None,
                session_id=_first_attribute(trace_spans, "sessionId"),
                user_hash=_first_attribute(trace_spans, "userId"),
                environment=_first_attribute(trace_spans, "environment"),
                version=_first_attribute(trace_spans, "version"),
                start_time=start_time,
                end_time=end_time,
                duration_ms=_trace_duration_ms(start_time, end_time),
                outcome=_trace_outcome(trace_spans, root=root),
                total_cost_usd=total_cost,
                total_input_tokens=total_input_tokens,
                total_output_tokens=total_output_tokens,
                total_tool_calls=sum(1 for span in trace_spans if span.span_type == "tool"),
                total_model_calls=sum(1 for span in trace_spans if span.span_type == "llm"),
                raw_ref=root.raw_ref if root else None,
            )
        )
    return traces


def compute_data_quality(
    spans: list[NormalizedSpan], traces: list[NormalizedTrace]
) -> DataQualityMetrics:
    total_spans = len(spans)
    if total_spans == 0:
        return DataQualityMetrics(total_traces=len(traces))

    spans_by_trace: dict[str, set[str]] = defaultdict(set)
    for span in spans:
        spans_by_trace[span.trace_id].add(span.span_id)

    spans_with_parent_ids = sum(1 for span in spans if span.parent_span_id is not None)
    valid_parent_links = sum(
        1
        for span in spans
        if span.parent_span_id is None or span.parent_span_id in spans_by_trace[span.trace_id]
    )
    spans_with_status = sum(1 for span in spans if span.status != "unknown")
    spans_with_cost = sum(1 for span in spans if span.cost_usd is not None)
    spans_with_token_counts = sum(
        1 for span in spans if any(value is not None for value in _token_values(span))
    )
    spans_with_model = sum(1 for span in spans if span.model_name)
    spans_with_io_hashes = sum(1 for span in spans if span.input_hash or span.output_hash)

    return DataQualityMetrics(
        total_spans=total_spans,
        total_traces=len(traces),
        spans_with_parent_ids=spans_with_parent_ids,
        spans_with_valid_parent_links=valid_parent_links,
        spans_with_status=spans_with_status,
        spans_with_cost=spans_with_cost,
        spans_with_token_counts=spans_with_token_counts,
        spans_with_model=spans_with_model,
        spans_with_io_hashes=spans_with_io_hashes,
        parent_coverage_pct=_pct(valid_parent_links, total_spans),
        status_coverage_pct=_pct(spans_with_status, total_spans),
        cost_coverage_pct=_pct(spans_with_cost, total_spans),
        token_coverage_pct=_pct(spans_with_token_counts, total_spans),
        model_coverage_pct=_pct(spans_with_model, total_spans),
        io_hash_coverage_pct=_pct(spans_with_io_hashes, total_spans),
    )


def _map_raw_span(row: RawSpanRow) -> NormalizedSpan:
    if row.source == "langfuse":
        return map_langfuse_raw_span(row)
    raise ValueError(f"Unsupported source: {row.source}")


def _is_supported_source(source: str) -> bool:
    return source == "langfuse"


def _trace_outcome(spans: list[NormalizedSpan], *, root: NormalizedSpan | None) -> str:
    if not spans:
        return "unknown"
    if root is not None and root.parent_span_id is None and root.status != "unknown":
        return _span_status_outcome(root.status)
    if any(span.status in {"error", "timeout", "cancelled"} for span in spans):
        return "failure"
    if all(span.status == "ok" for span in spans):
        return "success"
    return "unknown"


def _span_status_outcome(status: str) -> str:
    if status == "ok":
        return "success"
    if status in {"error", "timeout", "cancelled"}:
        return "failure"
    return "unknown"


def _trace_root(
    ordered_spans: list[NormalizedSpan], span_ids: set[str]
) -> NormalizedSpan | None:
    explicit_root = next(
        (span for span in ordered_spans if span.parent_span_id is None),
        None,
    )
    if explicit_root is not None:
        return explicit_root
    return next(
        (span for span in ordered_spans if span.parent_span_id not in span_ids),
        ordered_spans[0] if ordered_spans else None,
    )


def _first_attribute(spans: list[NormalizedSpan], key: str) -> str | None:
    for span in spans:
        value = span.attributes.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _trace_duration_ms(start_time: datetime | None, end_time: datetime | None) -> float | None:
    if not start_time or not end_time:
        return None
    return max(
        0.0,
        (_datetime_sort_value(end_time) - _datetime_sort_value(start_time)) * 1000,
    )


def _min_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return min(present, key=_datetime_sort_value) if present else None


def _max_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present, key=_datetime_sort_value) if present else None


def _trace_cost_usd(spans: list[NormalizedSpan]) -> Decimal | None:
    costed_spans = [span for span in spans if span.cost_usd is not None]
    if not costed_spans:
        return None

    by_id = {span.span_id: span for span in spans}
    ancestors_with_costed_descendants: set[str] = set()
    for span in costed_spans:
        parent_id = span.parent_span_id
        visited: set[str] = set()
        while parent_id and parent_id in by_id and parent_id not in visited:
            ancestors_with_costed_descendants.add(parent_id)
            visited.add(parent_id)
            parent_id = by_id[parent_id].parent_span_id

    return _sum_decimals(
        span.cost_usd
        for span in costed_spans
        if span.span_id not in ancestors_with_costed_descendants
    )


def _datetime_sort_value(value: datetime | None) -> float:
    if value is None:
        return inf
    return value.timestamp()


def _sum_decimals(values) -> Decimal | None:
    present = [value for value in values if value is not None]
    return sum(present, Decimal("0")) if present else None


def _sum_ints(values) -> int | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _token_values(span: NormalizedSpan) -> tuple[int | None, int | None, int | None]:
    return span.input_tokens, span.output_tokens, span.total_tokens


def _pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 1)
