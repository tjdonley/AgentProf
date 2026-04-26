from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from agentprof import __version__
from agentprof.config import (
    APP_DIR,
    APP_SUBDIRS,
    CONFIG_FILE,
    AgentProfConfig,
    ensure_workspace_dirs,
    load_config,
    write_default_config,
)
from agentprof.ingest.langfuse_export import (
    LangfuseExportFormat,
    LangfuseExportImportError,
    import_langfuse_export,
)
from agentprof.normalize.runner import normalize_store
from agentprof.privacy.hashing import MissingSaltError
from agentprof.store.duckdb_store import DuckDBStore

console = Console()
app = typer.Typer(
    name="agentprof",
    help="Profile AI-agent traces and produce local failure-and-waste reports.",
    no_args_is_help=True,
)
store_app = typer.Typer(help="Manage the local AgentProf DuckDB store.")
import_app = typer.Typer(help="Import trace data into the local AgentProf store.")
app.add_typer(store_app, name="store")
app.add_typer(import_app, name="import")


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


if __name__ == "__main__":
    app()
