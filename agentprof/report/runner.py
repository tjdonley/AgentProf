from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from agentprof.config import APP_DIR
from agentprof.report.schema import ReportBuildResult
from agentprof.store.duckdb_store import (
    CostLedgerRecord,
    DuckDBStore,
    IssueEvidenceRecord,
    IssueRecord,
    ReportRecord,
)


DEFAULT_REPORT_DIR = APP_DIR / "reports"
REPORT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def generate_report(
    store: DuckDBStore,
    *,
    project: str,
    output_dir: Path = DEFAULT_REPORT_DIR,
    report_id: str | None = None,
    generated_at: datetime | None = None,
) -> ReportBuildResult:
    generated_at = generated_at or datetime.now(UTC)
    report_id = report_id or _default_report_id(generated_at)
    _validate_report_id(report_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    issues = store.fetch_issues()
    evidence = store.fetch_issue_evidence()
    costs = store.fetch_cost_ledger()
    summary = _summary(
        issues=issues,
        evidence=evidence,
        costs=costs,
        generated_at=generated_at,
    )
    payload = _json_payload(
        report_id=report_id,
        project=project,
        generated_at=generated_at,
        summary=summary,
        issues=issues,
        evidence=evidence,
        costs=costs,
    )

    markdown_path = output_dir / f"{report_id}.md"
    json_path = output_dir / f"{report_id}.json"
    markdown_path.write_text(_markdown_report(payload), encoding="utf-8")
    json_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    store.upsert_report(
        ReportRecord(
            report_id=report_id,
            project=project,
            window_start=_min_datetime(issue.first_seen for issue in issues),
            window_end=_max_datetime(issue.last_seen for issue in issues),
            summary=summary,
            report_md_path=str(markdown_path),
            report_json_path=str(json_path),
        )
    )

    return ReportBuildResult(
        report_id=report_id,
        project=project,
        issues=len(issues),
        evidence_items=len(evidence),
        cost_entries=len(costs),
        total_wasted_cost_usd=_sum_decimals(issue.wasted_cost_usd for issue in issues),
        report_md_path=markdown_path,
        report_json_path=json_path,
    )


def _summary(
    *,
    issues: list[IssueRecord],
    evidence: list[IssueEvidenceRecord],
    costs: list[CostLedgerRecord],
    generated_at: datetime,
) -> dict[str, Any]:
    affected_traces = {item.trace_id for item in evidence if item.trace_id}
    affected_spans = {
        (item.trace_id, item.span_id)
        for item in evidence
        if item.trace_id and item.span_id
    }
    issue_kinds = Counter(issue.kind for issue in issues)
    severities = Counter(issue.severity for issue in issues)
    cost_types = defaultdict(Decimal)
    for record in costs:
        if record.amount_usd is not None:
            cost_types[record.cost_type] += record.amount_usd

    return {
        "generated_at": _datetime_to_json(generated_at),
        "issue_count": len(issues),
        "evidence_count": len(evidence),
        "cost_entry_count": len(costs),
        "affected_trace_count": len(affected_traces),
        "affected_span_count": len(affected_spans),
        "issues_by_kind": dict(sorted(issue_kinds.items())),
        "issues_by_severity": dict(sorted(severities.items())),
        "total_wasted_cost_usd": _decimal_to_json(
            _sum_decimals(issue.wasted_cost_usd for issue in issues)
        ),
        "total_potential_savings_usd": _decimal_to_json(
            _sum_decimals(issue.potential_savings_usd for issue in issues)
        ),
        "costs_by_type_usd": {
            cost_type: _decimal_to_json(amount)
            for cost_type, amount in sorted(cost_types.items())
        },
    }


def _json_payload(
    *,
    report_id: str,
    project: str,
    generated_at: datetime,
    summary: dict[str, Any],
    issues: list[IssueRecord],
    evidence: list[IssueEvidenceRecord],
    costs: list[CostLedgerRecord],
) -> dict[str, Any]:
    evidence_by_issue: dict[str, list[IssueEvidenceRecord]] = defaultdict(list)
    for item in evidence:
        evidence_by_issue[item.issue_id].append(item)

    return {
        "report_id": report_id,
        "project": project,
        "generated_at": _datetime_to_json(generated_at),
        "summary": summary,
        "issues": [
            _issue_to_json(issue, evidence_by_issue[issue.issue_id]) for issue in issues
        ],
        "cost_ledger": [_cost_to_json(record) for record in costs],
    }


def _issue_to_json(
    issue: IssueRecord, evidence: list[IssueEvidenceRecord]
) -> dict[str, Any]:
    return {
        "issue_id": issue.issue_id,
        "kind": issue.kind,
        "title": issue.title,
        "severity": issue.severity,
        "confidence": issue.confidence,
        "first_seen": _datetime_to_json(issue.first_seen),
        "last_seen": _datetime_to_json(issue.last_seen),
        "affected_traces": issue.affected_traces,
        "affected_spans": issue.affected_spans,
        "total_cost_usd": _decimal_to_json(issue.total_cost_usd),
        "wasted_cost_usd": _decimal_to_json(issue.wasted_cost_usd),
        "potential_savings_usd": _decimal_to_json(issue.potential_savings_usd),
        "recommendation": issue.recommendation,
        "recommended_tests": issue.recommended_tests,
        "evidence": [_evidence_to_json(item) for item in evidence],
    }


def _evidence_to_json(item: IssueEvidenceRecord) -> dict[str, Any]:
    return {
        "trace_id": item.trace_id,
        "span_id": item.span_id,
        "evidence_type": item.evidence_type,
        "message": item.message,
        "attributes": item.attributes,
    }


def _cost_to_json(record: CostLedgerRecord) -> dict[str, Any]:
    return {
        "trace_id": record.trace_id,
        "span_id": record.span_id,
        "issue_id": record.issue_id,
        "cost_type": record.cost_type,
        "amount_usd": _decimal_to_json(record.amount_usd),
        "attribution_method": record.attribution_method,
        "confidence": record.confidence,
    }


def _markdown_report(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        f"# AgentProf Report: {payload['project']}",
        "",
        f"Report ID: `{payload['report_id']}`",
        f"Generated at: `{payload['generated_at']}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Issues | {summary['issue_count']} |",
        f"| Evidence items | {summary['evidence_count']} |",
        f"| Affected traces | {summary['affected_trace_count']} |",
        f"| Affected spans | {summary['affected_span_count']} |",
        f"| Total wasted cost | {_format_usd(summary['total_wasted_cost_usd'])} |",
        f"| Potential savings | {_format_usd(summary['total_potential_savings_usd'])} |",
        "",
        "## Issues",
        "",
    ]

    if not payload["issues"]:
        lines.extend(["No issues have been generated yet.", ""])
    for issue in payload["issues"]:
        lines.extend(_markdown_issue(issue))

    lines.extend(["## Cost Ledger", ""])
    if not payload["cost_ledger"]:
        lines.extend(["No cost ledger entries have been generated yet.", ""])
    else:
        lines.extend(
            [
                "| Cost type | Amount | Attribution | Issue |",
                "| --- | ---: | --- | --- |",
            ]
        )
        for record in payload["cost_ledger"]:
            lines.append(
                "| "
                f"{record['cost_type']} | "
                f"{_format_usd(record['amount_usd'])} | "
                f"{record['attribution_method']} | "
                f"{record['issue_id'] or ''} |"
            )
        lines.append("")

    return "\n".join(lines)


def _markdown_issue(issue: dict[str, Any]) -> list[str]:
    lines = [
        f"### {issue['title']}",
        "",
        f"- Issue ID: `{issue['issue_id']}`",
        f"- Kind: `{issue['kind']}`",
        f"- Severity: `{issue['severity']}`",
        f"- Confidence: `{issue['confidence']}`",
        f"- Affected traces: {issue['affected_traces']}",
        f"- Affected spans: {issue['affected_spans']}",
        f"- Wasted cost: {_format_usd(issue['wasted_cost_usd'])}",
        f"- Potential savings: {_format_usd(issue['potential_savings_usd'])}",
        "",
        f"Recommendation: {issue['recommendation']}",
        "",
    ]
    if issue["recommended_tests"]:
        lines.extend(["Recommended tests:", ""])
        lines.extend(f"- {test}" for test in issue["recommended_tests"])
        lines.append("")
    if issue["evidence"]:
        lines.extend(["Evidence:", ""])
        for item in issue["evidence"]:
            location = ":".join(
                part for part in (item["trace_id"], item["span_id"]) if part
            )
            lines.append(f"- `{location}` {item['message']}")
        lines.append("")
    return lines


def _default_report_id(generated_at: datetime) -> str:
    return f"agentprof-{generated_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def _validate_report_id(report_id: str) -> None:
    if REPORT_ID_RE.fullmatch(report_id) is None:
        raise ValueError(
            "report_id must start with a letter or number and contain only letters, "
            "numbers, dots, underscores, or hyphens."
        )


def _datetime_to_json(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _decimal_to_json(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.9f}"


def _format_usd(value: str | None) -> str:
    if value is None:
        return "$0.000000000"
    return f"${Decimal(value):.9f}"


def _min_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _max_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _sum_decimals(values) -> Decimal:
    return sum((value for value in values if value is not None), Decimal("0"))
