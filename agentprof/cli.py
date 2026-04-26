from __future__ import annotations

from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from agentprof import __version__
from agentprof.analyze.multi_agent_waste import analyze_multi_agent_waste
from agentprof.analyze.retry_loop import analyze_retry_loops
from agentprof.analyze.spec_violation import analyze_spec_violations
from agentprof.config import (
    APP_DIR,
    APP_SUBDIRS,
    CONFIG_FILE,
    AgentProfConfig,
    ensure_workspace_dirs,
    load_config,
    write_default_config,
)
from agentprof.cost.runner import build_cost_ledger
from agentprof.ingest.langfuse_export import (
    LangfuseExportFormat,
    LangfuseExportImportError,
    import_langfuse_export,
)
from agentprof.normalize.runner import normalize_store
from agentprof.privacy.hashing import MissingSaltError
from agentprof.report.runner import DEFAULT_REPORT_DIR, generate_report
from agentprof.store.duckdb_store import DuckDBStore


class ReportShowFormat(StrEnum):
    markdown = "markdown"
    json = "json"


class MultiAgentBaselineMode(StrEnum):
    estimated = "estimated"
    observed = "observed"


console = Console()
app = typer.Typer(
    name="agentprof",
    help="Profile AI-agent traces and produce local failure-and-waste reports.",
    no_args_is_help=True,
)
store_app = typer.Typer(help="Manage the local AgentProf DuckDB store.")
import_app = typer.Typer(help="Import trace data into the local AgentProf store.")
cost_app = typer.Typer(help="Analyze normalized trace costs.")
analyze_app = typer.Typer(help="Run deterministic analyzers over normalized traces.")
report_app = typer.Typer(help="Generate local AgentProf reports.")
app.add_typer(store_app, name="store")
app.add_typer(import_app, name="import")
app.add_typer(cost_app, name="cost")
app.add_typer(analyze_app, name="analyze")
app.add_typer(report_app, name="report")


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"agentprof {__version__}")
        raise typer.Exit()


def _load_config_or_exit() -> AgentProfConfig:
    try:
        return load_config()
    except FileNotFoundError as exc:
        console.print("[red]agentprof.yml was not found.[/red]")
        console.print("Run `agentprof init` to create the local workspace.")
        raise typer.Exit(code=2) from exc
    except Exception as exc:
        console.print("[red]agentprof.yml is invalid.[/red]")
        console.print(str(exc))
        raise typer.Exit(code=2) from exc


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the AgentProf version and exit.",
    ),
) -> None:
    """AgentProf command line interface."""


@app.command()
def init(
    force: bool = typer.Option(
        False,
        "--force",
        help="Overwrite agentprof.yml if it already exists.",
    ),
) -> None:
    """Create local AgentProf config and workspace directories."""

    created: list[str] = []

    if not write_default_config(force=force):
        console.print("[yellow]agentprof.yml already exists; leaving it unchanged.[/yellow]")
    else:
        created.append(str(CONFIG_FILE))

    for path in ensure_workspace_dirs():
        created.append(str(path))

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    store.ensure_schema()
    created.append(str(config.store.path))

    console.print("[green]AgentProf initialized.[/green]")
    if created:
        console.print("Created or verified:")
        for path in created:
            console.print(f"  {path}")


@app.command()
def doctor() -> None:
    """Check whether the local AgentProf workspace is ready."""

    missing: list[str] = []

    if not CONFIG_FILE.exists():
        missing.append(str(CONFIG_FILE))

    for subdir in APP_SUBDIRS:
        path = APP_DIR / subdir
        if not path.is_dir():
            missing.append(str(path))

    if missing:
        console.print("[red]AgentProf workspace is incomplete.[/red]")
        console.print("Missing:")
        for path in missing:
            console.print(f"  {path}")
        console.print("Run `agentprof init` to create the local workspace.")
        raise typer.Exit(code=2)

    try:
        config = load_config()
        DuckDBStore(config.store.path).migrations()
    except Exception as exc:
        console.print("[red]AgentProf workspace is not usable.[/red]")
        console.print(f"Store/config check failed: {exc}")
        raise typer.Exit(code=2) from exc

    console.print("[green]AgentProf workspace looks ready.[/green]")


@app.command()
def normalize(
    source: str | None = typer.Option(
        None,
        "--source",
        help="Only normalize raw spans from this source, such as langfuse.",
    ),
) -> None:
    """Normalize raw imported spans into AgentProf trace/span tables."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    result = normalize_store(store, source=source)
    quality = result.data_quality

    console.print("[green]Normalized imported spans.[/green]")
    console.print(f"  raw spans seen: {result.raw_spans_seen}")
    console.print(f"  normalized spans: {result.normalized_spans}")
    console.print(f"  normalized traces: {result.normalized_traces}")

    table = Table(title="Data quality")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Parent coverage", f"{quality.parent_coverage_pct:.1f}%")
    table.add_row("Status coverage", f"{quality.status_coverage_pct:.1f}%")
    table.add_row("Cost coverage", f"{quality.cost_coverage_pct:.1f}%")
    table.add_row("Token coverage", f"{quality.token_coverage_pct:.1f}%")
    table.add_row("Model coverage", f"{quality.model_coverage_pct:.1f}%")
    table.add_row("I/O hash coverage", f"{quality.io_hash_coverage_pct:.1f}%")
    console.print(table)


@cost_app.command("ledger")
def cost_ledger() -> None:
    """Build a cost ledger and print a waterfall from normalized spans."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    result = build_cost_ledger(store)

    console.print("[green]Built cost ledger.[/green]")
    console.print(f"  normalized spans seen: {result.normalized_spans_seen}")
    console.print(f"  ledger entries: {result.ledger_entries}")
    console.print(f"  traces with cost: {result.traces_with_cost}")
    console.print(f"  total cost: {_format_usd(result.total_cost_usd)}")

    table = Table(title="Cost waterfall")
    table.add_column("Cost type")
    table.add_column("Entries", justify="right")
    table.add_column("Amount", justify="right")
    for row in result.waterfall:
        table.add_row(
            _cost_type_label(row.cost_type),
            str(row.entries),
            _format_usd(row.amount_usd),
        )
    console.print(table)


@analyze_app.command("retry-loops")
def analyze_retry_loops_command(
    min_attempts: int = typer.Option(
        2,
        "--min-attempts",
        help="Minimum repeated failing attempts required to emit an issue.",
    ),
) -> None:
    """Detect repeated failing calls with the same retry fingerprint."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    try:
        result = analyze_retry_loops(store, min_attempts=min_attempts)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=2) from exc

    console.print("[green]Analyzed retry loops.[/green]")
    console.print(f"  normalized spans seen: {result.normalized_spans_seen}")
    console.print(f"  retry loops: {result.retry_loops}")
    console.print(f"  affected traces: {result.affected_traces}")
    console.print(f"  affected spans: {result.affected_spans}")
    console.print(f"  wasted attempts: {result.wasted_attempts}")
    console.print(f"  wasted cost: {_format_usd(result.wasted_cost_usd)}")
    if result.findings:
        console.print(f"  top retry loop: {result.findings[0].name}")

    table = Table(title="Retry loops")
    table.add_column("Issue")
    table.add_column("Trace")
    table.add_column("Name")
    table.add_column("Attempts", justify="right")
    table.add_column("Wasted", justify="right")
    table.add_column("Cost", justify="right")
    for finding in result.findings:
        table.add_row(
            finding.issue_id,
            finding.trace_id,
            finding.name,
            str(finding.attempts),
            str(finding.wasted_attempts),
            _format_usd(finding.wasted_cost_usd),
        )
    console.print(table)


@analyze_app.command("spec-violations")
def analyze_spec_violations_command() -> None:
    """Detect spans that violate configured input/output field contracts."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    contracts = config.analyzers.spec_violations.contracts
    result = analyze_spec_violations(store, contracts=contracts)

    console.print("[green]Analyzed spec violations.[/green]")
    console.print(f"  normalized spans seen: {result.normalized_spans_seen}")
    console.print(f"  contracts seen: {result.contracts_seen}")
    console.print(f"  spec violations: {result.spec_violations}")
    console.print(f"  affected traces: {result.affected_traces}")
    console.print(f"  affected spans: {result.affected_spans}")
    console.print(f"  wasted cost: {_format_usd(result.wasted_cost_usd)}")
    if result.findings:
        console.print(f"  top spec violation: {result.findings[0].name}")

    table = Table(title="Spec violations")
    table.add_column("Issue")
    table.add_column("Trace")
    table.add_column("Span")
    table.add_column("Contract")
    table.add_column("Missing")
    table.add_column("Cost", justify="right")
    for finding in result.findings:
        table.add_row(
            finding.issue_id,
            finding.trace_id,
            finding.span_id,
            finding.contract_name,
            _spec_missing_label(finding.missing_input_fields, finding.missing_output_fields),
            _format_usd(finding.wasted_cost_usd),
        )
    console.print(table)


@analyze_app.command("multi-agent-waste")
def analyze_multi_agent_waste_command(
    baseline_ratio: str = typer.Option(
        "0.50",
        "--baseline-ratio",
        help="Configured single-agent baseline cost ratio, greater than 0 and less than 1.",
    ),
    baseline_mode: MultiAgentBaselineMode = typer.Option(
        MultiAgentBaselineMode.estimated,
        "--baseline-mode",
        help="Use a configured estimate or observed matching single-agent traces.",
    ),
    min_agents: int = typer.Option(
        2,
        "--min-agents",
        help="Minimum distinct root/agent actors required to analyze a trace.",
    ),
    min_overhead: str = typer.Option(
        "0",
        "--min-overhead",
        help="Minimum estimated orchestration overhead required to emit an issue.",
    ),
    min_baseline_matches: int = typer.Option(
        1,
        "--min-baseline-matches",
        help="Minimum observed single-agent baseline matches required in observed mode.",
    ),
) -> None:
    """Estimate multi-agent orchestration overhead against a configured baseline."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    try:
        result = analyze_multi_agent_waste(
            store,
            baseline_ratio=_parse_decimal_option(baseline_ratio, "baseline_ratio"),
            baseline_mode=baseline_mode.value,
            min_agents=min_agents,
            min_overhead=_parse_decimal_option(min_overhead, "min_overhead"),
            min_baseline_matches=min_baseline_matches,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=2) from exc

    console.print("[green]Analyzed multi-agent waste.[/green]")
    console.print(f"  normalized spans seen: {result.normalized_spans_seen}")
    console.print(f"  baseline mode: {baseline_mode.value}")
    console.print(f"  multi-agent traces: {result.multi_agent_traces}")
    console.print(f"  affected traces: {result.affected_traces}")
    console.print(f"  affected spans: {result.affected_spans}")
    console.print(
        "  estimated orchestration overhead: "
        f"{_format_usd(result.estimated_overhead_usd)}"
    )
    if result.findings:
        console.print(
            f"  top multi-agent trace: {result.findings[0].root_name or result.findings[0].trace_id}"
        )

    table = Table(title="Multi-agent waste")
    table.add_column("Issue")
    table.add_column("Trace")
    table.add_column("Agents", justify="right")
    table.add_column("Actual", justify="right")
    table.add_column("Baseline", justify="right")
    table.add_column("Overhead", justify="right")
    table.add_column("Multiple", justify="right")
    for finding in result.findings:
        table.add_row(
            finding.issue_id,
            finding.trace_id,
            str(finding.agent_count),
            _format_usd(finding.actual_cost_usd),
            _format_usd(finding.baseline_cost_usd),
            _format_usd(finding.estimated_overhead_usd),
            f"{finding.cost_multiple:.2f}x",
        )
    console.print(table)


@report_app.command("generate")
def report_generate(
    output_dir: Path = typer.Option(
        DEFAULT_REPORT_DIR,
        "--output-dir",
        help="Directory where Markdown and JSON report files will be written.",
    ),
    report_id: str | None = typer.Option(
        None,
        "--report-id",
        help="Optional stable report ID. Defaults to a UTC timestamp-based ID.",
    ),
) -> None:
    """Generate Markdown and JSON reports from persisted analysis results."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    try:
        result = generate_report(
            store,
            project=config.project.name,
            output_dir=output_dir,
            report_id=report_id,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=2) from exc

    console.print("[green]Generated AgentProf report.[/green]")
    console.print(f"  report id: {result.report_id}")
    console.print(f"  issues: {result.issues}")
    console.print(f"  evidence items: {result.evidence_items}")
    console.print(f"  cost entries: {result.cost_entries}")
    console.print(f"  total wasted cost: {_format_usd(result.total_wasted_cost_usd)}")
    console.print(f"  markdown: {result.report_md_path}")
    console.print(f"  json: {result.report_json_path}")


@report_app.command("list")
def report_list() -> None:
    """List generated reports recorded in the local store."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    reports = store.fetch_reports()

    if not reports:
        console.print("No reports have been generated yet.")
        return

    table = Table(title="Generated reports")
    table.add_column("Report ID")
    table.add_column("Project")
    table.add_column("Issues", justify="right")
    table.add_column("Wasted cost", justify="right")
    table.add_column("Markdown")
    table.add_column("JSON")
    for report in reports:
        table.add_row(
            report.report_id,
            report.project or "",
            str(report.summary.get("issue_count", 0)),
            _format_usd(Decimal(str(report.summary.get("total_wasted_cost_usd") or "0"))),
            report.report_md_path or "",
            report.report_json_path or "",
        )
    console.print(table)


@report_app.command("show")
def report_show(
    report_id: str = typer.Argument(..., help="Report ID to show."),
    output_format: ReportShowFormat = typer.Option(
        ReportShowFormat.markdown,
        "--format",
        help="Report artifact format to print.",
    ),
) -> None:
    """Print a generated report artifact."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    reports = store.fetch_reports(report_id=report_id)
    if not reports:
        console.print(f"[red]Report `{report_id}` was not found.[/red]")
        raise typer.Exit(code=2)

    path = _report_artifact_path(reports[0], output_format)
    if path is None or not path.is_file():
        console.print(f"[red]Report `{report_id}` {output_format} artifact was not found.[/red]")
        raise typer.Exit(code=2)
    typer.echo(path.read_text(encoding="utf-8"), nl=False)


@store_app.command("stats")
def store_stats() -> None:
    """Show local store row counts."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    stats = store.stats()

    table = Table(title=f"AgentProf store: {config.store.path}")
    table.add_column("Table")
    table.add_column("Rows", justify="right")
    for name, rows in stats.items():
        table.add_row(name, str(rows))

    console.print(table)


@store_app.command("reset")
def store_reset(
    yes: bool = typer.Option(
        False,
        "--yes",
        help="Reset without an interactive confirmation prompt.",
    ),
) -> None:
    """Delete and recreate the local store."""

    config = _load_config_or_exit()
    if not yes:
        typer.confirm(
            f"Delete and recreate local store at {config.store.path}?",
            abort=True,
        )

    DuckDBStore(config.store.path).reset()
    console.print(f"[green]Reset local store at {config.store.path}.[/green]")


@import_app.command("langfuse-export")
def import_langfuse_export_command(
    observations: Path = typer.Option(
        ...,
        "--observations",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to a Langfuse observations JSON or CSV export.",
    ),
    file_format: LangfuseExportFormat = typer.Option(
        LangfuseExportFormat.auto,
        "--format",
        help="Observation export format. Defaults to extension-based detection.",
    ),
) -> None:
    """Import a Langfuse observations export."""

    config = _load_config_or_exit()
    store = DuckDBStore(config.store.path)
    try:
        result = import_langfuse_export(
            observations_path=observations,
            store=store,
            config=config,
            file_format=file_format,
        )
    except MissingSaltError as exc:
        console.print("[red]Cannot hash Langfuse I/O without a configured salt.[/red]")
        console.print(
            f"Set `{config.privacy.hmac_salt_env}` or disable `privacy.hash_inputs`."
        )
        raise typer.Exit(code=2) from exc
    except LangfuseExportImportError as exc:
        console.print("[red]Could not import Langfuse export.[/red]")
        console.print(str(exc))
        raise typer.Exit(code=2) from exc

    console.print("[green]Imported Langfuse observations.[/green]")
    console.print(f"  observations seen: {result.observations_seen}")
    console.print(f"  observations imported: {result.observations_imported}")
    console.print(f"  source: {result.raw_ref}")


def _format_usd(value: Decimal) -> str:
    return f"${value:.9f}"


def _cost_type_label(cost_type: str) -> str:
    return cost_type.replace("_", " ").capitalize()


def _spec_missing_label(input_fields: list[str], output_fields: list[str]) -> str:
    parts: list[str] = []
    if input_fields:
        parts.append(f"input: {', '.join(input_fields)}")
    if output_fields:
        parts.append(f"output: {', '.join(output_fields)}")
    return "; ".join(parts)


def _parse_decimal_option(value: str, name: str) -> Decimal:
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal value") from exc


def _report_artifact_path(report, output_format: ReportShowFormat) -> Path | None:
    if output_format == ReportShowFormat.json:
        return Path(report.report_json_path) if report.report_json_path else None
    return Path(report.report_md_path) if report.report_md_path else None


if __name__ == "__main__":
    app()
