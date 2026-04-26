from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from agentprof.analyze.schema import (
    MultiAgentWasteAnalysisResult,
    MultiAgentWasteFinding,
)
from agentprof.store.duckdb_store import (
    CostLedgerRecord,
    DuckDBStore,
    IssueEvidenceRecord,
    IssueRecord,
    NormalizedSpanAgentAnalysisRow,
)


ISSUE_KIND = "multi_agent_waste"
ATTRIBUTION_METHOD = "multi_agent_waste"
WASTED_COST_TYPE = "estimated_multi_agent_overhead"
EVIDENCE_BASIS = "configured_ratio_estimate"
AGENT_SPAN_TYPES = {"root", "agent"}


def analyze_multi_agent_waste(
    store: DuckDBStore,
    *,
    baseline_ratio: Decimal = Decimal("0.50"),
    min_agents: int = 2,
    min_overhead: Decimal = Decimal("0"),
) -> MultiAgentWasteAnalysisResult:
    _require_finite_decimal(baseline_ratio, "baseline_ratio")
    _require_finite_decimal(min_overhead, "min_overhead")
    if baseline_ratio <= 0 or baseline_ratio >= 1:
        raise ValueError("baseline_ratio must be greater than 0 and less than 1")
    if min_agents < 2:
        raise ValueError("min_agents must be at least 2")
    if min_overhead < 0:
        raise ValueError("min_overhead must be greater than or equal to 0")

    spans = store.fetch_normalized_spans_for_agent_analysis()
    spans_by_trace = _spans_by_trace(spans)
    cost_leaf_keys = _cost_leaf_keys(spans)

    multi_agent_traces = 0
    findings: list[MultiAgentWasteFinding] = []
    affected_span_counts: dict[str, int] = {}
    for trace_id, trace_spans in sorted(spans_by_trace.items()):
        agent_names = _agent_names(trace_spans)
        if len(agent_names) < min_agents:
            continue

        multi_agent_traces += 1
        actual_cost_usd = _sum_leaf_costs(trace_spans, cost_leaf_keys)
        if actual_cost_usd <= 0:
            continue

        baseline_cost_usd = actual_cost_usd * baseline_ratio
        estimated_overhead_usd = actual_cost_usd - baseline_cost_usd
        if estimated_overhead_usd < min_overhead:
            continue

        finding = _finding_from_trace(
            trace_id=trace_id,
            trace_spans=trace_spans,
            agent_names=agent_names,
            actual_cost_usd=actual_cost_usd,
            baseline_cost_usd=baseline_cost_usd,
            estimated_overhead_usd=estimated_overhead_usd,
            baseline_ratio=baseline_ratio,
        )
        findings.append(finding)
        affected_span_counts[finding.issue_id] = len(trace_spans)

    issues = [
        _issue_from_finding(finding, affected_spans=affected_span_counts[finding.issue_id])
        for finding in findings
    ]
    evidence = [_evidence_from_finding(finding) for finding in findings]
    costs = [_cost_from_finding(finding) for finding in findings]

    store.replace_analysis_results(
        issue_kind=ISSUE_KIND,
        attribution_method=ATTRIBUTION_METHOD,
        issues=issues,
        evidence=evidence,
        cost_records=costs,
    )
    return MultiAgentWasteAnalysisResult(
        normalized_spans_seen=len(spans),
        multi_agent_traces=multi_agent_traces,
        affected_traces=len(findings),
        affected_spans=sum(affected_span_counts.values()),
        estimated_overhead_usd=sum(
            (finding.estimated_overhead_usd for finding in findings), Decimal("0")
        ),
        findings=findings,
    )


def _require_finite_decimal(value: Decimal, name: str) -> None:
    if not value.is_finite():
        raise ValueError(f"{name} must be a finite decimal value")


def _spans_by_trace(
    spans: list[NormalizedSpanAgentAnalysisRow],
) -> dict[str, list[NormalizedSpanAgentAnalysisRow]]:
    grouped: dict[str, list[NormalizedSpanAgentAnalysisRow]] = defaultdict(list)
    for span in spans:
        grouped[span.trace_id].append(span)

    return {
        trace_id: sorted(
            trace_spans,
            key=lambda span: (_datetime_sort_value(span.start_time), span.span_id),
        )
        for trace_id, trace_spans in grouped.items()
    }


def _agent_names(spans: list[NormalizedSpanAgentAnalysisRow]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for span in spans:
        if span.span_type not in AGENT_SPAN_TYPES:
            continue
        name = _actor_name(span)
        if not name or name in seen:
            continue
        names.append(name)
        seen.add(name)
    return names


def _actor_name(span: NormalizedSpanAgentAnalysisRow) -> str:
    return (span.agent_name or span.name).strip()


def _finding_from_trace(
    *,
    trace_id: str,
    trace_spans: list[NormalizedSpanAgentAnalysisRow],
    agent_names: list[str],
    actual_cost_usd: Decimal,
    baseline_cost_usd: Decimal,
    estimated_overhead_usd: Decimal,
    baseline_ratio: Decimal,
) -> MultiAgentWasteFinding:
    root = _root_span(trace_spans)
    return MultiAgentWasteFinding(
        issue_id=_issue_id(trace_id, root, agent_names),
        trace_id=trace_id,
        root_span_id=root.span_id if root else None,
        root_name=root.name if root else None,
        agent_count=len(agent_names),
        agent_names=agent_names,
        handoff_span_ids=[span.span_id for span in trace_spans if span.span_type == "handoff"],
        first_seen=_min_datetime(span.start_time for span in trace_spans),
        last_seen=_max_datetime((span.end_time or span.start_time) for span in trace_spans),
        actual_cost_usd=actual_cost_usd,
        baseline_cost_usd=baseline_cost_usd,
        estimated_overhead_usd=estimated_overhead_usd,
        cost_multiple=actual_cost_usd / baseline_cost_usd,
        baseline_ratio=baseline_ratio,
    )


def _root_span(
    spans: list[NormalizedSpanAgentAnalysisRow],
) -> NormalizedSpanAgentAnalysisRow | None:
    parentless = [span for span in spans if span.parent_span_id is None]
    explicit_roots = [span for span in parentless if span.span_type == "root"]
    if explicit_roots:
        return explicit_roots[0]
    if parentless:
        return parentless[0]

    typed_roots = [span for span in spans if span.span_type == "root"]
    return typed_roots[0] if typed_roots else (spans[0] if spans else None)


def _issue_from_finding(
    finding: MultiAgentWasteFinding, *, affected_spans: int
) -> IssueRecord:
    root_label = finding.root_name or finding.trace_id
    return IssueRecord(
        issue_id=finding.issue_id,
        kind=ISSUE_KIND,
        title=f"Estimated orchestration overhead in {root_label}",
        severity="medium",
        confidence="medium",
        first_seen=finding.first_seen,
        last_seen=finding.last_seen,
        affected_traces=1,
        affected_spans=affected_spans,
        total_cost_usd=finding.actual_cost_usd,
        wasted_cost_usd=finding.estimated_overhead_usd,
        potential_savings_usd=finding.estimated_overhead_usd,
        recommendation=(
            "Compare this multi-agent trace with a configured single-agent baseline "
            "before keeping the orchestration path."
        ),
        recommended_tests=[
            "Add an eval that compares the multi-agent trace against the configured single-agent baseline."
        ],
    )


def _evidence_from_finding(finding: MultiAgentWasteFinding) -> IssueEvidenceRecord:
    return IssueEvidenceRecord(
        issue_id=finding.issue_id,
        trace_id=finding.trace_id,
        span_id=finding.root_span_id,
        evidence_type="multi_agent_waste",
        message=(
            f"Trace used {finding.agent_count} distinct agents; estimated orchestration "
            "overhead uses a configured single-agent baseline ratio."
        ),
        attributes={
            "basis": EVIDENCE_BASIS,
            "agent_count": finding.agent_count,
            "agent_names": finding.agent_names,
            "handoff_span_ids": finding.handoff_span_ids,
            "actual_cost_usd": str(finding.actual_cost_usd),
            "baseline_cost_usd": str(finding.baseline_cost_usd),
            "estimated_overhead_usd": str(finding.estimated_overhead_usd),
            "cost_multiple": str(finding.cost_multiple),
            "baseline_ratio": str(finding.baseline_ratio),
        },
    )


def _cost_from_finding(finding: MultiAgentWasteFinding) -> CostLedgerRecord:
    return CostLedgerRecord(
        trace_id=finding.trace_id,
        span_id=finding.root_span_id,
        issue_id=finding.issue_id,
        cost_type=WASTED_COST_TYPE,
        amount_usd=finding.estimated_overhead_usd,
        attribution_method=ATTRIBUTION_METHOD,
        confidence="estimated",
    )


def _issue_id(
    trace_id: str,
    root: NormalizedSpanAgentAnalysisRow | None,
    agent_names: list[str],
) -> str:
    payload = {
        "trace_id": trace_id,
        "root_span_id": root.span_id if root else None,
        "agent_names": sorted(agent_names),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
    return f"{ISSUE_KIND}:{digest}"


def _cost_leaf_keys(
    spans: list[NormalizedSpanAgentAnalysisRow],
) -> set[tuple[str, str]]:
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
    spans: list[NormalizedSpanAgentAnalysisRow], cost_leaf_keys: set[tuple[str, str]]
) -> Decimal:
    return sum(
        (
            span.cost_usd
            for span in spans
            if span.cost_usd is not None and _span_key(span) in cost_leaf_keys
        ),
        Decimal("0"),
    )


def _span_key(span: NormalizedSpanAgentAnalysisRow) -> tuple[str, str]:
    return span.trace_id, span.span_id


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
