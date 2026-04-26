from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import app
from agentprof.config import DEFAULT_STORE_PATH
from agentprof.report.runner import generate_report
from agentprof.store.duckdb_store import (
    CostLedgerRecord,
    DuckDBStore,
    IssueEvidenceRecord,
    IssueRecord,
)


runner = CliRunner()


def test_generate_report_writes_markdown_json_and_store_row(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _seed_retry_issue(store)

    result = generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        report_id="test-report",
        generated_at=_dt("2026-04-26T12:00:00+00:00"),
    )
    reports = store.fetch_reports(report_id="test-report")
    payload = json.loads(result.report_json_path.read_text(encoding="utf-8"))
    markdown = result.report_md_path.read_text(encoding="utf-8")

    assert result.report_id == "test-report"
    assert result.issues == 1
    assert result.evidence_items == 1
    assert result.cost_entries == 1
    assert result.total_wasted_cost_usd == Decimal("0.020000000")
    assert result.report_md_path.is_file()
    assert result.report_json_path.is_file()

    assert payload["report_id"] == "test-report"
    assert payload["project"] == "tracer"
    assert payload["summary"]["issue_count"] == 1
    assert payload["summary"]["issues_by_kind"] == {"retry_loop": 1}
    assert payload["summary"]["total_wasted_cost_usd"] == "0.020000000"
    assert payload["issues"][0]["title"] == "Repeated failing call to refund_policy_lookup"
    assert payload["issues"][0]["evidence"][0]["span_id"] == "attempt-2"

    assert "# AgentProf Report: tracer" in markdown
    assert "Repeated failing call to refund_policy_lookup" in markdown
    assert "$0.020000000" in markdown

    assert len(reports) == 1
    assert reports[0].project == "tracer"
    assert reports[0].summary["issue_count"] == 1
    assert reports[0].report_md_path == str(result.report_md_path)
    assert store.stats()["reports"] == 1


def test_generate_report_writes_multi_agent_waste_svg_when_present(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _seed_multi_agent_issue(store)

    result = generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        report_id="multi-agent-report",
        generated_at=_dt("2026-04-26T12:00:00+00:00"),
    )
    svg_path = tmp_path / "reports" / "multi-agent-report-multi-agent-waste.svg"
    payload = json.loads(result.report_json_path.read_text(encoding="utf-8"))
    markdown = result.report_md_path.read_text(encoding="utf-8")
    svg = svg_path.read_text(encoding="utf-8")

    assert svg_path.is_file()
    assert payload["summary"]["artifacts"] == {
        "multi_agent_waste_svg": "multi-agent-report-multi-agent-waste.svg"
    }
    assert (
        "![Multi-agent waste estimate](multi-agent-report-multi-agent-waste.svg)"
        in markdown
    )
    assert "Multi-Agent Waste Estimate" in svg
    assert "$0.084000000" in svg
    assert "$0.042000000" in svg
    assert "2.00x" in svg
    assert "3: triage_agent, research_agent..." in svg
    assert "Basis: configurable estimate" in svg


def test_generate_report_skips_multi_agent_waste_svg_when_absent(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _seed_retry_issue(store)

    result = generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        report_id="retry-report",
        generated_at=_dt("2026-04-26T12:00:00+00:00"),
    )
    payload = json.loads(result.report_json_path.read_text(encoding="utf-8"))
    markdown = result.report_md_path.read_text(encoding="utf-8")

    assert not (tmp_path / "reports" / "retry-report-multi-agent-waste.svg").exists()
    assert payload["summary"]["artifacts"] == {}
    assert "![Multi-agent waste estimate]" not in markdown


def test_generate_report_upserts_existing_report_id(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")
    _seed_retry_issue(store)

    generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        report_id="same-report",
        generated_at=_dt("2026-04-26T12:00:00+00:00"),
    )
    generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        report_id="same-report",
        generated_at=_dt("2026-04-26T12:05:00+00:00"),
    )

    reports = store.fetch_reports(report_id="same-report")

    assert len(reports) == 1
    assert reports[0].summary["generated_at"] == "2026-04-26T12:05:00Z"
    assert store.stats()["reports"] == 1


def test_generate_report_default_ids_include_subsecond_precision(
    tmp_path: Path,
) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    first = generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        generated_at=_dt("2026-04-26T12:00:00.100000+00:00"),
    )
    second = generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        generated_at=_dt("2026-04-26T12:00:00.200000+00:00"),
    )

    assert first.report_id == "agentprof-20260426T120000100000Z"
    assert second.report_id == "agentprof-20260426T120000200000Z"
    assert first.report_id != second.report_id
    assert store.stats()["reports"] == 2


def test_generate_report_handles_empty_analysis_results(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    result = generate_report(
        store,
        project="tracer",
        output_dir=tmp_path / "reports",
        report_id="empty-report",
        generated_at=_dt("2026-04-26T12:00:00+00:00"),
    )
    payload = json.loads(result.report_json_path.read_text(encoding="utf-8"))
    markdown = result.report_md_path.read_text(encoding="utf-8")

    assert result.issues == 0
    assert result.evidence_items == 0
    assert result.cost_entries == 0
    assert payload["summary"]["issue_count"] == 0
    assert payload["issues"] == []
    assert "No issues have been generated yet." in markdown
    assert store.stats()["reports"] == 1


def test_generate_report_rejects_unsafe_report_id(tmp_path: Path) -> None:
    store = DuckDBStore(tmp_path / "agentprof.duckdb")

    try:
        generate_report(
            store,
            project="tracer",
            output_dir=tmp_path / "reports",
            report_id="../outside",
            generated_at=_dt("2026-04-26T12:00:00+00:00"),
        )
    except ValueError as exc:
        assert "report_id" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_cli_report_generate_writes_outputs() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        store = DuckDBStore(DEFAULT_STORE_PATH)
        _seed_retry_issue(store)

        result = runner.invoke(
            app,
            ["report", "generate", "--report-id", "cli-report"],
        )
        reports = store.fetch_reports(report_id="cli-report")
        markdown_path = Path(".agentprof/reports/cli-report.md")
        json_path = Path(".agentprof/reports/cli-report.json")

        assert init_result.exit_code == 0
        assert result.exit_code == 0
        assert "Generated AgentProf report" in result.output
        assert "report id: cli-report" in result.output
        assert "issues: 1" in result.output
        assert markdown_path.is_file()
        assert json_path.is_file()
        assert len(reports) == 1


def test_cli_report_list_and_show_generated_reports() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        store = DuckDBStore(DEFAULT_STORE_PATH)
        _seed_retry_issue(store)
        generate_result = runner.invoke(
            app,
            ["report", "generate", "--report-id", "cli-report"],
        )

        list_result = runner.invoke(app, ["report", "list"])
        markdown_result = runner.invoke(app, ["report", "show", "cli-report"])
        json_result = runner.invoke(
            app,
            ["report", "show", "cli-report", "--format", "json"],
        )
        markdown_path = Path(".agentprof/reports/cli-report.md")
        json_path = Path(".agentprof/reports/cli-report.json")

        assert init_result.exit_code == 0
        assert generate_result.exit_code == 0
        assert list_result.exit_code == 0
        assert "Generated reports" in list_result.output
        assert "cli-report" in list_result.output
        assert markdown_result.exit_code == 0
        assert "# AgentProf Report: tracer" in markdown_result.output
        assert "Repeated failing call to refund_policy_lookup" in markdown_result.output
        assert markdown_result.output == markdown_path.read_text(encoding="utf-8")
        assert json_result.exit_code == 0
        assert '"report_id": "cli-report"' in json_result.output
        assert json_result.output == json_path.read_text(encoding="utf-8")


def test_cli_report_list_handles_empty_store() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])

        result = runner.invoke(app, ["report", "list"])

        assert init_result.exit_code == 0
        assert result.exit_code == 0
        assert "No reports have been generated yet" in result.output


def test_cli_report_show_requires_existing_report() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])

        result = runner.invoke(app, ["report", "show", "missing-report"])

        assert init_result.exit_code == 0
        assert result.exit_code == 2
        assert "was not found" in result.output


def test_cli_report_generate_rejects_unsafe_report_id() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])

        result = runner.invoke(
            app,
            ["report", "generate", "--report-id", "../outside"],
        )

        assert init_result.exit_code == 0
        assert result.exit_code == 2
        assert "report_id" in result.output


def _seed_retry_issue(store: DuckDBStore) -> None:
    issue = IssueRecord(
        issue_id="retry_loop:test",
        kind="retry_loop",
        title="Repeated failing call to refund_policy_lookup",
        severity="medium",
        confidence="high",
        first_seen=_dt("2026-04-26T10:00:00+00:00"),
        last_seen=_dt("2026-04-26T10:00:02+00:00"),
        affected_traces=1,
        affected_spans=2,
        total_cost_usd=Decimal("0.030"),
        wasted_cost_usd=Decimal("0.020"),
        potential_savings_usd=Decimal("0.020"),
        recommendation="Stop retrying deterministic failures.",
        recommended_tests=["Assert identical failing input is not retried."],
    )
    evidence = IssueEvidenceRecord(
        issue_id=issue.issue_id,
        trace_id="trace-retry",
        span_id="attempt-2",
        evidence_type="retry_attempt",
        message="Attempt 2 failed with missing required field region.",
        attributes={"attempt_index": 2},
    )
    cost = CostLedgerRecord(
        trace_id="trace-retry",
        span_id="attempt-2",
        issue_id=issue.issue_id,
        cost_type="wasted_retry_cost",
        amount_usd=Decimal("0.020"),
        attribution_method="retry_loop",
        confidence="source",
    )
    store.replace_analysis_results(
        issue_kind="retry_loop",
        attribution_method="retry_loop",
        issues=[issue],
        evidence=[evidence],
        cost_records=[cost],
    )


def _seed_multi_agent_issue(store: DuckDBStore) -> None:
    issue = IssueRecord(
        issue_id="multi_agent_waste:test",
        kind="multi_agent_waste",
        title="Estimated orchestration overhead in triage_agent",
        severity="medium",
        confidence="medium",
        first_seen=_dt("2026-04-26T11:00:00+00:00"),
        last_seen=_dt("2026-04-26T11:00:08+00:00"),
        affected_traces=1,
        affected_spans=5,
        total_cost_usd=Decimal("0.084"),
        wasted_cost_usd=Decimal("0.042"),
        potential_savings_usd=Decimal("0.042"),
        recommendation="Compare this trace with a configured single-agent baseline.",
        recommended_tests=["Add a baseline eval for this workflow."],
    )
    evidence = IssueEvidenceRecord(
        issue_id=issue.issue_id,
        trace_id="trace-multi-agent-1",
        span_id="ma-root-1",
        evidence_type="multi_agent_waste",
        message="Trace used 3 distinct agents.",
        attributes={
            "basis": "configured_ratio_estimate",
            "agent_count": 3,
            "agent_names": ["triage_agent", "research_agent", "policy_agent"],
            "actual_cost_usd": "0.084",
            "baseline_cost_usd": "0.042",
            "estimated_overhead_usd": "0.042",
            "cost_multiple": "2",
            "baseline_ratio": "0.50",
        },
    )
    cost = CostLedgerRecord(
        trace_id="trace-multi-agent-1",
        span_id="ma-root-1",
        issue_id=issue.issue_id,
        cost_type="estimated_multi_agent_overhead",
        amount_usd=Decimal("0.042"),
        attribution_method="multi_agent_waste",
        confidence="estimated",
    )
    store.replace_analysis_results(
        issue_kind="multi_agent_waste",
        attribution_method="multi_agent_waste",
        issues=[issue],
        evidence=[evidence],
        cost_records=[cost],
    )


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)
