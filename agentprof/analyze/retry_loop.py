from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from agentprof.analyze.schema import RetryLoopAnalysisResult, RetryLoopFinding
from agentprof.store.duckdb_store import (
    CostLedgerRecord,
    DuckDBStore,
    IssueEvidenceRecord,
    IssueRecord,
    NormalizedSpanAnalysisRow,
)


ISSUE_KIND = "retry_loop"
ATTRIBUTION_METHOD = "retry_loop"
WASTED_COST_TYPE = "wasted_retry_cost"
FAILURE_STATUSES = {"error", "timeout", "cancelled"}


def analyze_retry_loops(
    store: DuckDBStore, *, min_attempts: int = 2
) -> RetryLoopAnalysisResult:
    if min_attempts < 2:
        raise ValueError("min_attempts must be at least 2")

    spans = store.fetch_normalized_spans_for_analysis()
    cost_leaf_keys = _cost_leaf_keys(spans)
    groups = _retry_groups(spans)
    finding_groups = [
        (key, _finding_from_group(group, cost_leaf_keys=cost_leaf_keys))
        for key, group in sorted(groups.items(), key=lambda item: _sortable_key(item[0]))
        if len(group) >= min_attempts
    ]
    findings = [finding for _, finding in finding_groups]
    issues = [_issue_from_finding(finding) for finding in findings]
    evidence = [
        item
        for key, finding in finding_groups
        for item in _evidence_from_finding(finding, groups[key])
    ]
    costs = [
        record
        for key, finding in finding_groups
        for record in _costs_from_finding(
            finding,
            groups[key],
            cost_leaf_keys=cost_leaf_keys,
        )
    ]

    store.replace_analysis_results(
        issue_kind=ISSUE_KIND,
        attribution_method=ATTRIBUTION_METHOD,
        issues=issues,
        evidence=evidence,
        cost_records=costs,
    )
    return RetryLoopAnalysisResult(
        normalized_spans_seen=len(spans),
        retry_loops=len(findings),
        affected_traces=len({finding.trace_id for finding in findings}),
        affected_spans=sum(finding.attempts for finding in findings),
        wasted_attempts=sum(finding.wasted_attempts for finding in findings),
        wasted_cost_usd=sum(
            (finding.wasted_cost_usd for finding in findings), Decimal("0")
        ),
        findings=findings,
    )


def _retry_groups(
    spans: list[NormalizedSpanAnalysisRow],
) -> dict[tuple[str, str | None, str, str, str], list[NormalizedSpanAnalysisRow]]:
    groups: dict[
        tuple[str, str | None, str, str, str], list[NormalizedSpanAnalysisRow]
    ] = defaultdict(list)
    for span in spans:
        if not _is_retry_candidate(span):
            continue
        groups[_group_key(span)].append(span)

    return {
        key: sorted(
            group,
            key=lambda span: (_datetime_sort_value(span.start_time), span.span_id),
        )
        for key, group in groups.items()
    }


def _is_retry_candidate(span: NormalizedSpanAnalysisRow) -> bool:
    return (
        span.status in FAILURE_STATUSES
        and span.input_retry_fingerprint is not None
        and span.error_signature is not None
    )


def _group_key(
    span: NormalizedSpanAnalysisRow,
) -> tuple[str, str | None, str, str, str]:
    return (
        span.trace_id,
        span.parent_span_id,
        span.name,
        span.input_retry_fingerprint or "",
        span.error_signature or "",
    )


def _sortable_key(
    key: tuple[str, str | None, str, str, str],
) -> tuple[str, str, str, str, str]:
    trace_id, parent_span_id, name, retry_fingerprint, error_signature = key
    return (trace_id, parent_span_id or "", name, retry_fingerprint, error_signature)


def _finding_from_group(
    group: list[NormalizedSpanAnalysisRow],
    *,
    cost_leaf_keys: set[tuple[str, str]],
) -> RetryLoopFinding:
    first = group[0]
    wasted = group[1:]
    return RetryLoopFinding(
        issue_id=_issue_id(first),
        trace_id=first.trace_id,
        parent_span_id=first.parent_span_id,
        name=first.name,
        attempts=len(group),
        wasted_attempts=len(wasted),
        affected_span_ids=[span.span_id for span in group],
        wasted_span_ids=[span.span_id for span in wasted],
        first_seen=_min_datetime(span.start_time for span in group),
        last_seen=_max_datetime((span.end_time or span.start_time) for span in group),
        total_cost_usd=_sum_leaf_costs(group, cost_leaf_keys),
        wasted_cost_usd=_sum_leaf_costs(wasted, cost_leaf_keys),
        error_signature=first.error_signature,
    )


def _issue_from_finding(finding: RetryLoopFinding) -> IssueRecord:
    return IssueRecord(
        issue_id=finding.issue_id,
        kind=ISSUE_KIND,
        title=f"Repeated failing call to {finding.name}",
        severity="medium" if finding.wasted_cost_usd > 0 else "low",
        confidence="high",
        first_seen=finding.first_seen,
        last_seen=finding.last_seen,
        affected_traces=1,
        affected_spans=finding.attempts,
        total_cost_usd=finding.total_cost_usd,
        wasted_cost_usd=finding.wasted_cost_usd,
        potential_savings_usd=finding.wasted_cost_usd,
        recommendation=(
            "Stop retrying deterministic failures until the input, schema, or tool "
            "precondition has changed."
        ),
        recommended_tests=[
            "Add a regression test that identical failing tool input is not retried without mutation."
        ],
    )


def _evidence_from_finding(
    finding: RetryLoopFinding, group: list[NormalizedSpanAnalysisRow]
) -> list[IssueEvidenceRecord]:
    return [
        IssueEvidenceRecord(
            issue_id=finding.issue_id,
            trace_id=span.trace_id,
            span_id=span.span_id,
            evidence_type="retry_attempt",
            message=f"Attempt {index} failed with {span.error_signature}.",
            attributes={
                "attempt_index": index,
                "name": span.name,
                "status": span.status,
                "cost_usd": str(span.cost_usd) if span.cost_usd is not None else None,
                "input_preview": span.input_preview,
            },
        )
        for index, span in enumerate(group, start=1)
    ]


def _costs_from_finding(
    finding: RetryLoopFinding,
    group: list[NormalizedSpanAnalysisRow],
    *,
    cost_leaf_keys: set[tuple[str, str]],
) -> list[CostLedgerRecord]:
    return [
        CostLedgerRecord(
            trace_id=span.trace_id,
            span_id=span.span_id,
            issue_id=finding.issue_id,
            cost_type=WASTED_COST_TYPE,
            amount_usd=span.cost_usd,
            attribution_method=ATTRIBUTION_METHOD,
            confidence=span.cost_confidence,
        )
        for span in group[1:]
        if span.cost_usd is not None and _span_key(span) in cost_leaf_keys
    ]


def _issue_id(span: NormalizedSpanAnalysisRow) -> str:
    payload = {
        "trace_id": span.trace_id,
        "parent_span_id": span.parent_span_id,
        "name": span.name,
        "input_retry_fingerprint": span.input_retry_fingerprint,
        "error_signature": span.error_signature,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
    return f"{ISSUE_KIND}:{digest}"


def _min_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return min(present, key=_datetime_sort_value) if present else None


def _max_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present, key=_datetime_sort_value) if present else None


def _datetime_sort_value(value: datetime | None) -> float:
    if value is None:
        return float("inf")
    return value.timestamp()


def _cost_leaf_keys(spans: list[NormalizedSpanAnalysisRow]) -> set[tuple[str, str]]:
    costed_spans = [span for span in spans if span.cost_usd is not None]
    by_id = {_span_key(span): span for span in spans}
    ancestors_with_costed_descendants: set[tuple[str, str]] = set()
    for span in costed_spans:
        parent_id = span.parent_span_id
        trace_id = span.trace_id
        visited: set[str] = set()
        while parent_id and (trace_id, parent_id) in by_id and parent_id not in visited:
            ancestors_with_costed_descendants.add((trace_id, parent_id))
            visited.add(parent_id)
            parent_id = by_id[(trace_id, parent_id)].parent_span_id

    return {
        _span_key(span)
        for span in costed_spans
        if _span_key(span) not in ancestors_with_costed_descendants
    }


def _sum_leaf_costs(
    spans: list[NormalizedSpanAnalysisRow], cost_leaf_keys: set[tuple[str, str]]
) -> Decimal:
    return _sum_costs(
        span.cost_usd for span in spans if _span_key(span) in cost_leaf_keys
    )


def _span_key(span: NormalizedSpanAnalysisRow) -> tuple[str, str]:
    return span.trace_id, span.span_id


def _sum_costs(values) -> Decimal:
    return sum((value for value in values if value is not None), Decimal("0"))
