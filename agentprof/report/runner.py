from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from html import escape
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
REPORT_ID_MAX_LENGTH = 128
REPORT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
MARKDOWN_ESCAPE_RE = re.compile(r"([\\`*\[\]()!|])")
MARKDOWN_AUTO_LINK_RE = re.compile(r"(?i)\b((?:https?|ftp)://)")
MARKDOWN_WWW_LINK_RE = re.compile(r"(?i)\bwww\.")
MULTI_AGENT_WASTE_KIND = "multi_agent_waste"


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
    validate_report_id(report_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    issues = store.fetch_issues()
    evidence = store.fetch_issue_evidence()
    costs = store.fetch_cost_ledger()
    multi_agent_visual = _multi_agent_waste_visual(issues=issues, evidence=evidence)
    artifacts = {}
    if multi_agent_visual is not None:
        artifacts["multi_agent_waste_svg"] = f"{report_id}-multi-agent-waste.svg"

    summary = _summary(
        issues=issues,
        evidence=evidence,
        costs=costs,
        generated_at=generated_at,
        artifacts=artifacts,
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
    svg_path = output_dir / f"{report_id}-multi-agent-waste.svg"
    if multi_agent_visual is not None:
        svg_path.write_text(_multi_agent_waste_svg(multi_agent_visual), encoding="utf-8")
    elif svg_path.exists():
        svg_path.unlink()
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
            report_html_path=None,
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
        report_html_path=None,
    )


def _summary(
    *,
    issues: list[IssueRecord],
    evidence: list[IssueEvidenceRecord],
    costs: list[CostLedgerRecord],
    generated_at: datetime,
    artifacts: dict[str, str],
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
        "artifacts": dict(sorted(artifacts.items())),
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
        f"# AgentProf Report: {_markdown_text(payload['project'])}",
        "",
        f"Report ID: {_markdown_inline_code(payload['report_id'])}",
        f"Generated at: {_markdown_inline_code(payload['generated_at'])}",
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
    ]

    multi_agent_svg = summary.get("artifacts", {}).get("multi_agent_waste_svg")
    if multi_agent_svg:
        lines.extend(
            [
                "## Visuals",
                "",
                f"![Multi-agent waste estimate]({_markdown_link_target(multi_agent_svg)})",
                "",
            ]
        )

    lines.extend(["## Issues", ""])

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
                f"{_markdown_table_cell(record['cost_type'])} | "
                f"{_format_usd(record['amount_usd'])} | "
                f"{_markdown_table_cell(record['attribution_method'])} | "
                f"{_markdown_table_cell(record['issue_id'] or '')} |"
            )
        lines.append("")

    return "\n".join(lines) + "\n"


def _markdown_issue(issue: dict[str, Any]) -> list[str]:
    lines = [
        f"### {_markdown_text(issue['title'])}",
        "",
        f"- Issue ID: {_markdown_inline_code(issue['issue_id'])}",
        f"- Kind: {_markdown_inline_code(issue['kind'])}",
        f"- Severity: {_markdown_inline_code(issue['severity'])}",
        f"- Confidence: {_markdown_inline_code(issue['confidence'])}",
        f"- Affected traces: {issue['affected_traces']}",
        f"- Affected spans: {issue['affected_spans']}",
        f"- Wasted cost: {_format_usd(issue['wasted_cost_usd'])}",
        f"- Potential savings: {_format_usd(issue['potential_savings_usd'])}",
        "",
        f"Recommendation: {_markdown_text(issue['recommendation'])}",
        "",
    ]
    if issue["recommended_tests"]:
        lines.extend(["Recommended tests:", ""])
        lines.extend(f"- {_markdown_text(test)}" for test in issue["recommended_tests"])
        lines.append("")
    if issue["evidence"]:
        lines.extend(["Evidence:", ""])
        for item in issue["evidence"]:
            location = ":".join(
                part for part in (item["trace_id"], item["span_id"]) if part
            )
            lines.append(
                f"- {_markdown_inline_code(location or 'unknown')} "
                f"{_markdown_text(item['message'])}"
            )
        lines.append("")
    return lines


def _html_report(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8">',
        '  <meta name="viewport" content="width=device-width, initial-scale=1">',
        f"  <title>AgentProf Report: {_html_text(payload['project'])}</title>",
        "  <style>",
        *_html_style().splitlines(),
        "  </style>",
        "</head>",
        "<body>",
        '  <main class="report">',
        '    <header class="hero">',
        "      <p>AgentProf Report</p>",
        f"      <h1>{_html_text(payload['project'])}</h1>",
        "      <dl>",
        f"        <div><dt>Report ID</dt><dd>{_html_code(payload['report_id'])}</dd></div>",
        f"        <div><dt>Generated</dt><dd>{_html_code(payload['generated_at'])}</dd></div>",
        "      </dl>",
        "    </header>",
        "",
        '    <section class="panel">',
        "      <h2>Summary</h2>",
        '      <div class="summary-grid">',
        _html_summary_card("Issues", summary["issue_count"]),
        _html_summary_card("Evidence Items", summary["evidence_count"]),
        _html_summary_card("Affected Traces", summary["affected_trace_count"]),
        _html_summary_card("Affected Spans", summary["affected_span_count"]),
        _html_summary_card(
            "Total Wasted Cost",
            _format_usd(summary["total_wasted_cost_usd"]),
        ),
        _html_summary_card(
            "Potential Savings",
            _format_usd(summary["total_potential_savings_usd"]),
        ),
        "      </div>",
        "    </section>",
    ]

    multi_agent_svg = summary.get("artifacts", {}).get("multi_agent_waste_svg")
    if multi_agent_svg:
        lines.extend(
            [
                "",
                '    <section class="panel">',
                "      <h2>Visuals</h2>",
                '      <figure class="visual">',
                f'        <img src="{_html_attr(multi_agent_svg)}" alt="Multi-agent waste estimate">',
                "        <figcaption>Multi-agent waste estimate generated by AgentProf.</figcaption>",
                "      </figure>",
                "    </section>",
            ]
        )

    lines.extend(["", '    <section class="panel">', "      <h2>Issues</h2>"])
    if not payload["issues"]:
        lines.append('      <p class="empty">No issues have been generated yet.</p>')
    for issue in payload["issues"]:
        lines.extend(_html_issue(issue))
    lines.append("    </section>")

    lines.extend(["", '    <section class="panel">', "      <h2>Cost Ledger</h2>"])
    if not payload["cost_ledger"]:
        lines.append('      <p class="empty">No cost ledger entries have been generated yet.</p>')
    else:
        lines.extend(_html_cost_ledger(payload["cost_ledger"]))
    lines.extend(["    </section>", "  </main>", "</body>", "</html>"])
    return "\n".join(lines) + "\n"


def _html_style() -> str:
    return """    :root {
      color-scheme: light;
      --bg: #f8fafc;
      --panel: #ffffff;
      --ink: #0f172a;
      --muted: #64748b;
      --line: #dbe3ef;
      --accent: #2563eb;
      --accent-soft: #dbeafe;
      --code-bg: #eef2f7;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }
    .report {
      width: min(1120px, calc(100% - 32px));
      margin: 0 auto;
      padding: 32px 0 56px;
    }
    .hero, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: 0 18px 45px rgba(15, 23, 42, 0.08);
    }
    .hero {
      padding: 32px;
      background: linear-gradient(135deg, #0f172a, #1d4ed8);
      color: #f8fafc;
    }
    .hero p {
      margin: 0 0 8px;
      color: #bfdbfe;
      font-size: 0.85rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
    .hero h1 {
      margin: 0;
      font-size: clamp(2rem, 6vw, 4rem);
      line-height: 0.95;
    }
    .hero dl {
      display: flex;
      flex-wrap: wrap;
      gap: 16px 32px;
      margin: 28px 0 0;
    }
    .hero dt, .card span, .issue-meta dt, .evidence-type {
      color: var(--muted);
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .hero dt { color: #bfdbfe; }
    .hero dd, .issue-meta dd { margin: 0; }
    .panel {
      margin-top: 24px;
      padding: 28px;
    }
    .panel h2 {
      margin: 0 0 18px;
      font-size: 1.35rem;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 14px;
    }
    .card {
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: #fbfdff;
    }
    .card strong {
      display: block;
      margin-top: 8px;
      font-size: 1.45rem;
      line-height: 1.1;
    }
    .visual {
      margin: 0;
    }
    .visual img {
      display: block;
      max-width: 100%;
      height: auto;
      border-radius: 18px;
    }
    .visual figcaption {
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.92rem;
    }
    .issue {
      margin-top: 16px;
      padding: 20px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fbfdff;
    }
    .issue-header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
    }
    .issue h3 {
      margin: 0;
      font-size: 1.15rem;
    }
    .badges {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
    }
    .badge {
      border-radius: 999px;
      background: var(--accent-soft);
      color: #1e40af;
      font-size: 0.78rem;
      font-weight: 800;
      padding: 5px 10px;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .issue-meta {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 14px;
      margin: 18px 0;
    }
    code {
      padding: 0.15rem 0.35rem;
      border-radius: 0.4rem;
      background: var(--code-bg);
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.88em;
    }
    .hero code { background: rgba(255, 255, 255, 0.14); }
    .evidence-list {
      display: grid;
      gap: 10px;
      padding: 0;
      list-style: none;
    }
    .evidence-list li {
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #ffffff;
    }
    .evidence-list p { margin: 8px 0 0; }
    details { margin-top: 10px; }
    pre {
      overflow-x: auto;
      padding: 12px;
      border-radius: 12px;
      background: #0f172a;
      color: #e2e8f0;
    }
    .table-wrap { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      color: var(--muted);
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .number { text-align: right; white-space: nowrap; }
    .empty {
      margin: 0;
      color: var(--muted);
    }
    @media (max-width: 680px) {
      .report { width: min(100% - 20px, 1120px); padding-top: 10px; }
      .hero, .panel { border-radius: 16px; padding: 20px; }
      .issue-header { display: block; }
      .badges { justify-content: flex-start; margin-top: 12px; }
    }"""


def _html_summary_card(label: str, value: Any) -> str:
    return (
        '        <div class="card">'
        f"<span>{_html_text(label)}</span>"
        f"<strong>{_html_text(_single_line(value))}</strong>"
        "</div>"
    )


def _html_issue(issue: dict[str, Any]) -> list[str]:
    lines = [
        '      <article class="issue">',
        '        <header class="issue-header">',
        f"          <h3>{_html_text(issue['title'])}</h3>",
        '          <div class="badges">',
        f"            <span class=\"badge\">{_html_text(issue['severity'])}</span>",
        f"            <span class=\"badge\">{_html_text(issue['confidence'])} confidence</span>",
        "          </div>",
        "        </header>",
        '        <dl class="issue-meta">',
        _html_meta_item("Issue ID", _html_code(issue["issue_id"]), html=True),
        _html_meta_item("Kind", _html_code(issue["kind"]), html=True),
        _html_meta_item("Affected Traces", issue["affected_traces"]),
        _html_meta_item("Affected Spans", issue["affected_spans"]),
        _html_meta_item("Wasted Cost", _format_usd(issue["wasted_cost_usd"])),
        _html_meta_item("Potential Savings", _format_usd(issue["potential_savings_usd"])),
        "        </dl>",
        f"        <p><strong>Recommendation:</strong> {_html_text(issue['recommendation'])}</p>",
    ]
    if issue["recommended_tests"]:
        lines.extend(['        <p><strong>Recommended tests:</strong></p>', "        <ul>"])
        lines.extend(
            f"          <li>{_html_text(test)}</li>"
            for test in issue["recommended_tests"]
        )
        lines.append("        </ul>")
    if issue["evidence"]:
        lines.extend(['        <p><strong>Evidence:</strong></p>', '        <ul class="evidence-list">'])
        for item in issue["evidence"]:
            lines.extend(_html_evidence_item(item))
        lines.append("        </ul>")
    lines.append("      </article>")
    return lines


def _html_meta_item(label: str, value: Any, *, html: bool = False) -> str:
    return (
        "          <div>"
        f"<dt>{_html_text(label)}</dt>"
        f"<dd>{value if html else _html_text(_single_line(value))}</dd>"
        "</div>"
    )


def _html_evidence_item(item: dict[str, Any]) -> list[str]:
    location = ":".join(
        part for part in (item.get("trace_id"), item.get("span_id")) if part
    )
    lines = [
        "          <li>",
        f"            <div>{_html_code(location or 'unknown')} <span class=\"evidence-type\">{_html_text(item['evidence_type'])}</span></div>",
        f"            <p>{_html_text(item['message'])}</p>",
    ]
    if item.get("attributes"):
        lines.extend(
            [
                "            <details>",
                "              <summary>Attributes</summary>",
                f"              <pre>{_html_json(item['attributes'])}</pre>",
                "            </details>",
            ]
        )
    lines.append("          </li>")
    return lines


def _html_cost_ledger(records: list[dict[str, Any]]) -> list[str]:
    lines = [
        '      <div class="table-wrap">',
        "        <table>",
        "          <thead>",
        "            <tr>",
        "              <th>Cost type</th>",
        "              <th>Trace</th>",
        "              <th>Span</th>",
        "              <th>Attribution</th>",
        "              <th>Issue</th>",
        "              <th class=\"number\">Amount</th>",
        "            </tr>",
        "          </thead>",
        "          <tbody>",
    ]
    for record in records:
        lines.extend(
            [
                "            <tr>",
                f"              <td>{_html_text(record['cost_type'])}</td>",
                f"              <td>{_html_code(record['trace_id'])}</td>",
                f"              <td>{_html_code(record['span_id'] or '')}</td>",
                f"              <td>{_html_text(record['attribution_method'])}</td>",
                f"              <td>{_html_code(record['issue_id'] or '')}</td>",
                f"              <td class=\"number\">{_format_usd(record['amount_usd'])}</td>",
                "            </tr>",
            ]
        )
    lines.extend(["          </tbody>", "        </table>", "      </div>"])
    return lines


def _html_json(value: Any) -> str:
    return _html_text(
        json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True, default=str)
    )


def _html_code(value: Any) -> str:
    return f"<code>{_html_text(_single_line(value))}</code>"


def _html_text(value: Any) -> str:
    return escape("" if value is None else str(value), quote=True)


def _html_attr(value: Any) -> str:
    return _html_text(_single_line(value))


def _multi_agent_waste_visual(
    *,
    issues: list[IssueRecord],
    evidence: list[IssueEvidenceRecord],
) -> dict[str, Any] | None:
    multi_agent_issues = [
        issue for issue in issues if issue.kind == MULTI_AGENT_WASTE_KIND
    ]
    if not multi_agent_issues:
        return None

    issue_ids = {issue.issue_id for issue in multi_agent_issues}
    issue_evidence = [item for item in evidence if item.issue_id in issue_ids]
    actual_cost_usd = _sum_decimals(issue.total_cost_usd for issue in multi_agent_issues)
    overhead_usd = _sum_decimals(issue.wasted_cost_usd for issue in multi_agent_issues)
    baseline_cost_usd = max(Decimal("0"), actual_cost_usd - overhead_usd)
    cost_multiple = (
        actual_cost_usd / baseline_cost_usd if baseline_cost_usd > 0 else None
    )
    agent_names = _distinct_agent_names(issue_evidence)
    max_agent_count = max(
        (_evidence_agent_count(item) for item in issue_evidence),
        default=0,
    )

    return {
        "trace_count": len(multi_agent_issues),
        "actual_cost_usd": actual_cost_usd,
        "baseline_cost_usd": baseline_cost_usd,
        "overhead_usd": overhead_usd,
        "cost_multiple": cost_multiple,
        "agent_count": len(agent_names) if agent_names else max_agent_count,
        "agent_names": agent_names,
        "baseline_basis": _multi_agent_baseline_basis(issue_evidence),
    }


def _multi_agent_baseline_basis(evidence: list[IssueEvidenceRecord]) -> str:
    if any(
        item.attributes.get("basis") == "observed_single_agent_baseline"
        for item in evidence
    ):
        return "observed"
    return "estimated"


def _distinct_agent_names(evidence: list[IssueEvidenceRecord]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for item in evidence:
        raw_names = item.attributes.get("agent_names")
        if not isinstance(raw_names, list):
            continue
        for raw_name in raw_names:
            name = str(raw_name).strip()
            if not name or name in seen:
                continue
            names.append(name)
            seen.add(name)
    return names


def _evidence_agent_count(item: IssueEvidenceRecord) -> int:
    value = item.attributes.get("agent_count")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _multi_agent_waste_svg(visual: dict[str, Any]) -> str:
    actual = visual["actual_cost_usd"]
    baseline = visual["baseline_cost_usd"]
    overhead = visual["overhead_usd"]
    max_cost = max(actual, baseline, overhead, Decimal("0.000000001"))
    actual_width = _svg_bar_width(actual, max_cost)
    baseline_width = _svg_bar_width(baseline, max_cost)
    multiple = visual["cost_multiple"]
    multiple_label = f"{multiple:.2f}x" if multiple is not None else "n/a"
    agent_label = _agent_label(visual)
    subtitle = (
        "Observed single-agent baseline estimate"
        if visual["baseline_basis"] == "observed"
        else "Configurable single-agent baseline estimate"
    )
    basis_label = (
        "Basis: observed single-agent baseline traces. Validate match quality before acting."
        if visual["baseline_basis"] == "observed"
        else "Basis: configurable estimate. Validate with observed project-specific single-agent baselines."
    )

    return f"""<svg xmlns="http://www.w3.org/2000/svg" width="760" height="360" viewBox="0 0 760 360" role="img" aria-label="Multi-agent waste estimate">
  <rect width="760" height="360" rx="24" fill="#0f172a"/>
  <text x="40" y="48" fill="#f8fafc" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="26" font-weight="700">Multi-Agent Waste Estimate</text>
  <text x="40" y="80" fill="#cbd5e1" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="14">{escape(subtitle)}</text>

  <text x="40" y="128" fill="#e2e8f0" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="15">Actual multi-agent traces</text>
  <rect x="260" y="111" width="420" height="28" rx="14" fill="#1e293b"/>
  <rect x="260" y="111" width="{actual_width}" height="28" rx="14" fill="#38bdf8"/>
  <text x="696" y="131" fill="#f8fafc" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="15" font-weight="700" text-anchor="end">{_format_decimal_usd(actual)}</text>

  <text x="40" y="178" fill="#e2e8f0" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="15">Estimated single-agent baseline</text>
  <rect x="260" y="161" width="420" height="28" rx="14" fill="#1e293b"/>
  <rect x="260" y="161" width="{baseline_width}" height="28" rx="14" fill="#22c55e"/>
  <text x="696" y="181" fill="#f8fafc" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="15" font-weight="700" text-anchor="end">{_format_decimal_usd(baseline)}</text>

  <rect x="40" y="222" width="206" height="76" rx="16" fill="#111827" stroke="#334155"/>
  <text x="60" y="251" fill="#94a3b8" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="13">Estimated overhead</text>
  <text x="60" y="280" fill="#f8fafc" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="22" font-weight="700">{_format_decimal_usd(overhead)}</text>

  <rect x="277" y="222" width="206" height="76" rx="16" fill="#111827" stroke="#334155"/>
  <text x="297" y="251" fill="#94a3b8" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="13">Cost multiple</text>
  <text x="297" y="280" fill="#f8fafc" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="22" font-weight="700">{escape(multiple_label)}</text>

  <rect x="514" y="222" width="206" height="76" rx="16" fill="#111827" stroke="#334155"/>
  <text x="534" y="251" fill="#94a3b8" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="13">Agents detected</text>
  <text x="534" y="280" fill="#f8fafc" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="22" font-weight="700">{escape(agent_label)}</text>

  <text x="40" y="330" fill="#94a3b8" font-family="Inter, ui-sans-serif, system-ui, sans-serif" font-size="12">{escape(basis_label)}</text>
</svg>
"""


def _svg_bar_width(value: Decimal, max_value: Decimal) -> int:
    if max_value <= 0:
        return 0
    return int((value / max_value) * Decimal("420"))


def _agent_label(visual: dict[str, Any]) -> str:
    count = visual["agent_count"]
    names = visual["agent_names"]
    if names:
        return f"{count}: {', '.join(names[:2])}{'...' if len(names) > 2 else ''}"
    return str(count)


def _markdown_text(value: Any) -> str:
    return _escape_markdown(_single_line(value))


def _markdown_table_cell(value: Any) -> str:
    return _markdown_text(value)


def _markdown_inline_code(value: Any) -> str:
    text = _single_line(value)
    max_backticks = max(
        (len(match.group(0)) for match in re.finditer(r"`+", text)),
        default=0,
    )
    marker = "`" * (max_backticks + 1)
    padding = " " if text.startswith("`") or text.endswith("`") else ""
    return f"{marker}{padding}{text}{padding}{marker}"


def _markdown_link_target(value: Any) -> str:
    text = _single_line(value)
    return (
        text.replace("\\", "%5C")
        .replace(" ", "%20")
        .replace("(", "%28")
        .replace(")", "%29")
    )


def _escape_markdown(value: str) -> str:
    text = escape(value, quote=False)
    text = MARKDOWN_ESCAPE_RE.sub(r"\\\1", text)
    text = MARKDOWN_AUTO_LINK_RE.sub(
        lambda match: match.group(1).replace(":", "&#58;"),
        text,
    )
    text = MARKDOWN_WWW_LINK_RE.sub(
        lambda match: match.group(0).replace(".", "&#46;"),
        text,
    )
    return text


def _single_line(value: Any) -> str:
    return "" if value is None else " ".join(str(value).split())


def _default_report_id(generated_at: datetime) -> str:
    return f"agentprof-{generated_at.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')}"


def validate_report_id(report_id: str) -> None:
    if len(report_id) > REPORT_ID_MAX_LENGTH:
        raise ValueError(f"report_id must be {REPORT_ID_MAX_LENGTH} characters or fewer.")
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


def _format_decimal_usd(value: Decimal) -> str:
    return _format_usd(_decimal_to_json(value))


def _min_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _max_datetime(values) -> datetime | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _sum_decimals(values) -> Decimal:
    return sum((value for value in values if value is not None), Decimal("0"))
