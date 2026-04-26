from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from agentprof import __version__


APP_DIR = Path(".agentprof")
CONFIG_FILE = Path("agentprof.yml")
APP_SUBDIRS = ("data", "baselines", "reports", "cache")

DEFAULT_CONFIG = """project:
  name: tracer
  environment: development

privacy:
  store_raw_io: false
  store_redacted_io: true
  hash_inputs: true
  hmac_salt_env: AGENTPROF_HASH_SALT
  max_evidence_chars: 500

sources:
  langfuse:
    host_env: LANGFUSE_HOST
    public_key_env: LANGFUSE_PUBLIC_KEY
    secret_key_env: LANGFUSE_SECRET_KEY
    default_lag: 5m
"""

console = Console()
app = typer.Typer(
    name="agentprof",
    help="Profile AI-agent traces and produce local failure-and-waste reports.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"agentprof {__version__}")
        raise typer.Exit()


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

    if CONFIG_FILE.exists() and not force:
        console.print("[yellow]agentprof.yml already exists; leaving it unchanged.[/yellow]")
    else:
        CONFIG_FILE.write_text(DEFAULT_CONFIG, encoding="utf-8")
        created.append(str(CONFIG_FILE))

    for subdir in APP_SUBDIRS:
        path = APP_DIR / subdir
        path.mkdir(parents=True, exist_ok=True)
        created.append(str(path))

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

    console.print("[green]AgentProf workspace looks ready.[/green]")


if __name__ == "__main__":
    app()
