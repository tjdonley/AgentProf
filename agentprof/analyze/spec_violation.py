from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Sequence

from agentprof.analyze.schema import (
    SpecViolationAnalysisResult,
    SpecViolationFinding,
)
from agentprof.config import SpecContractConfig
from agentprof.store.duckdb_store import (
    CostLedgerRecord,
    DuckDBStore,
    IssueEvidenceRecord,
    IssueRecord,
    NormalizedSpanAnalysisRow,
)


ISSUE_KIND = "spec_violation"
ATTRIBUTION_METHOD = "spec_violation"
WASTED_COST_TYPE = "wasted_spec_violation_cost"
FAILURE_STATUSES = {"error", "timeout", "cancelled"}


def analyze_spec_violations(
    store: DuckDBStore, *, contracts: Sequence[SpecContractConfig]
) -> SpecViolationAnalysisResult:
    spans = store.fetch_normalized_spans_for_analysis()
    span_lookup = _span_by_key(spans)
    cost_leaf_keys = _cost_leaf_keys(spans)
    findings = [
        finding
        for span in spans
        for contract in _sorted_contracts(contracts)
        for finding in _finding_from_span(
            span,
            contract,
            cost_leaf_keys=cost_leaf_keys,
        )
    ]
    issues = [_issue_from_finding(finding) for finding in findings]
    evidence = [
        _evidence_from_finding(finding, span_lookup[(finding.trace_id, finding.span_id)])
        for finding in findings
    ]
    costs = [
        _cost_from_finding(finding, span_lookup[(finding.trace_id, finding.span_id)])
        for finding in findings
        if finding.wasted_cost_usd
    ]

    store.replace_analysis_results(
        issue_kind=ISSUE_KIND,
        attribution_method=ATTRIBUTION_METHOD,
        issues=issues,
        evidence=evidence,
        cost_records=costs,
    )
    return SpecViolationAnalysisResult(
        normalized_spans_seen=len(spans),
        contracts_seen=len(contracts),
        spec_violations=len(findings),
        affected_traces=len({finding.trace_id for finding in findings}),
        affected_spans=len({(finding.trace_id, finding.span_id) for finding in findings}),
        wasted_cost_usd=sum(
            (finding.wasted_cost_usd for finding in findings), Decimal("0")
        ),
        findings=findings,
    )


def _sorted_contracts(
    contracts: Sequence[SpecContractConfig],
) -> list[SpecContractConfig]:
    return sorted(
        contracts,
        key=lambda contract: (contract.span_name or contract.name, contract.name),
    )


def _finding_from_span(
    span: NormalizedSpanAnalysisRow,
    contract: SpecContractConfig,
    *,
    cost_leaf_keys: set[tuple[str, str]],
) -> list[SpecViolationFinding]:
    if not _matches_contract(span, contract):
        return []

    missing_input_fields = _missing_required_fields(
        span=span,
        preview=span.input_preview,
        required_fields=contract.required_input_fields,
    )
    missing_output_fields = _missing_required_fields(
        span=span,
        preview=span.output_preview,
        required_fields=contract.required_output_fields,
    )
    if not missing_input_fields and not missing_output_fields:
        return []

    cost_usd = _leaf_cost_usd(span, cost_leaf_keys)
    return [
        SpecViolationFinding(
            issue_id=_issue_id(
                span,
                contract=contract,
                missing_input_fields=missing_input_fields,
                missing_output_fields=missing_output_fields,
            ),
            trace_id=span.trace_id,
            span_id=span.span_id,
            name=span.name,
            contract_name=contract.name,
            missing_input_fields=missing_input_fields,
            missing_output_fields=missing_output_fields,
            first_seen=span.start_time,
            last_seen=span.end_time or span.start_time,
            total_cost_usd=cost_usd,
            wasted_cost_usd=cost_usd,
            status=span.status,
        )
    ]


def _matches_contract(
    span: NormalizedSpanAnalysisRow, contract: SpecContractConfig
) -> bool:
    return span.name == (contract.span_name or contract.name)


def _missing_required_fields(
    *,
    span: NormalizedSpanAnalysisRow,
    preview: str | None,
    required_fields: list[str],
) -> list[str]:
    if not required_fields:
        return []

    parsed = _json_object_preview(preview)
    present: set[str] = set()
    missing: list[str] = []
    if parsed is not None:
        for field in required_fields:
            if _has_required_value(parsed, field):
                present.add(field)
            else:
                missing.append(field)

    for field in required_fields:
        if field in present or field in missing:
            continue
        if _message_mentions_missing_field(span, field):
            missing.append(field)

    return missing


def _json_object_preview(preview: str | None) -> dict[str, Any] | None:
    if not preview or preview.endswith("..."):
        return None
    try:
        parsed = json.loads(preview)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _has_required_value(data: dict[str, Any], field: str) -> bool:
    current: Any = data
    for part in field.split("."):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return current not in (None, "")


def _message_mentions_missing_field(span: NormalizedSpanAnalysisRow, field: str) -> bool:
    text = " ".join(
        value for value in (span.status_message, span.error_signature) if value
    )
    if not text:
        return False
    normalized = _normalized_message_text(text)
    candidates = {field, field.split(".")[-1]}
    for candidate in candidates:
        normalized_field = _normalized_message_text(candidate)
        if not normalized_field:
            continue
        if _contains_phrase(normalized, f"missing required field {normalized_field}"):
            return True
        if _contains_phrase(normalized, f"missing field {normalized_field}"):
            return True
        if _contains_phrase(normalized, f"required field {normalized_field} missing"):
            return True
        if _contains_phrase(normalized, f"{normalized_field} is missing"):
            return True
        if _contains_phrase(normalized, f"{normalized_field} absent"):
            return True
        if _contains_phrase(normalized, f"{normalized_field} omitted"):
            return True
    return False


def _normalized_message_text(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9_.]+", " ", value.lower()).split())


def _contains_phrase(normalized_text: str, phrase: str) -> bool:
    boundary = r"[a-z0-9_.]"
    pattern = rf"(?<!{boundary}){re.escape(phrase)}(?!{boundary})"
    return re.search(pattern, normalized_text) is not None


def _issue_from_finding(finding: SpecViolationFinding) -> IssueRecord:
    return IssueRecord(
        issue_id=finding.issue_id,
        kind=ISSUE_KIND,
        title=f"Contract violation in {finding.name}",
        severity="medium" if finding.status in FAILURE_STATUSES else "low",
        confidence="high",
        first_seen=finding.first_seen,
        last_seen=finding.last_seen,
        affected_traces=1,
        affected_spans=1,
        total_cost_usd=finding.total_cost_usd,
        wasted_cost_usd=finding.wasted_cost_usd,
        potential_savings_usd=finding.wasted_cost_usd,
        recommendation=(
            "Validate tool inputs and outputs against the configured contract before "
            "continuing the agent trace."
        ),
        recommended_tests=[
            f"Add a contract test for {finding.contract_name} covering required fields."
        ],
    )


def _evidence_from_finding(
    finding: SpecViolationFinding, span: NormalizedSpanAnalysisRow
) -> IssueEvidenceRecord:
    return IssueEvidenceRecord(
        issue_id=finding.issue_id,
        trace_id=finding.trace_id,
        span_id=finding.span_id,
        evidence_type="spec_violation",
        message=_evidence_message(finding),
        attributes={
            "contract_name": finding.contract_name,
            "name": finding.name,
            "status": finding.status,
            "missing_input_fields": finding.missing_input_fields,
            "missing_output_fields": finding.missing_output_fields,
            "status_message": span.status_message,
            "input_preview": span.input_preview,
            "output_preview": span.output_preview,
        },
    )


def _evidence_message(finding: SpecViolationFinding) -> str:
    parts: list[str] = []
    if finding.missing_input_fields:
        parts.append(f"missing input fields: {', '.join(finding.missing_input_fields)}")
    if finding.missing_output_fields:
        parts.append(
            f"missing output fields: {', '.join(finding.missing_output_fields)}"
        )
    return f"{finding.name} violated {finding.contract_name}: {'; '.join(parts)}."


def _cost_from_finding(
    finding: SpecViolationFinding, span: NormalizedSpanAnalysisRow
) -> CostLedgerRecord:
    return CostLedgerRecord(
        trace_id=finding.trace_id,
        span_id=finding.span_id,
        issue_id=finding.issue_id,
        cost_type=WASTED_COST_TYPE,
        amount_usd=finding.wasted_cost_usd,
        attribution_method=ATTRIBUTION_METHOD,
        confidence=span.cost_confidence,
    )


def _issue_id(
    span: NormalizedSpanAnalysisRow,
    *,
    contract: SpecContractConfig,
    missing_input_fields: list[str],
    missing_output_fields: list[str],
) -> str:
    payload = {
        "trace_id": span.trace_id,
        "span_id": span.span_id,
        "contract_name": contract.name,
        "missing_input_fields": sorted(missing_input_fields),
        "missing_output_fields": sorted(missing_output_fields),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
    return f"{ISSUE_KIND}:{digest}"


def _leaf_cost_usd(
    span: NormalizedSpanAnalysisRow, cost_leaf_keys: set[tuple[str, str]]
) -> Decimal:
    if span.cost_usd is None or _span_key(span) not in cost_leaf_keys:
        return Decimal("0")
    return span.cost_usd


def _cost_leaf_keys(spans: list[NormalizedSpanAnalysisRow]) -> set[tuple[str, str]]:
    costed_spans = [span for span in spans if span.cost_usd is not None]
    by_id = _span_by_key(spans)
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


def _span_by_key(
    spans: list[NormalizedSpanAnalysisRow],
) -> dict[tuple[str, str], NormalizedSpanAnalysisRow]:
    return {_span_key(span): span for span in spans}


def _span_key(span: NormalizedSpanAnalysisRow) -> tuple[str, str]:
    return span.trace_id, span.span_id
