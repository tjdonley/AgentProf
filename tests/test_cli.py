from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from agentprof.cli import APP_SUBDIRS, app


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
