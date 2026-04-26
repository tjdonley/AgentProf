from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from agentprof.config import APP_SUBDIRS, DEFAULT_STORE_PATH
from agentprof.cli import app


runner = CliRunner()


def test_help() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "failure-and-waste" in result.output


def test_init_creates_workspace() -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["init"])

        assert result.exit_code == 0
        assert "AgentProf initialized" in result.output

        assert (Path("agentprof.yml")).is_file()
        for subdir in APP_SUBDIRS:
            assert (Path(".agentprof") / subdir).is_dir()
        assert DEFAULT_STORE_PATH.is_file()


def test_doctor_requires_init() -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["doctor"])

        assert result.exit_code == 2
        assert "workspace is incomplete" in result.output


def test_doctor_after_init() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 0
        assert "workspace looks ready" in doctor_result.output


def test_store_stats_after_init() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        stats_result = runner.invoke(app, ["store", "stats"])

        assert init_result.exit_code == 0
        assert stats_result.exit_code == 0
        assert "raw_spans" in stats_result.output
        assert "normalized_traces" in stats_result.output


def test_store_reset_requires_config() -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["store", "reset", "--yes"])

        assert result.exit_code == 2
        assert "agentprof.yml was not found" in result.output


def test_store_reset_after_init() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        reset_result = runner.invoke(app, ["store", "reset", "--yes"])

        assert init_result.exit_code == 0
        assert reset_result.exit_code == 0
        assert DEFAULT_STORE_PATH.is_file()
