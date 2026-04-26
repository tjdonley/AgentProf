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
        assert (
            Path(".agentprof/.gitignore").read_text(encoding="utf-8")
            == "*\n!.gitignore\n"
        )
        for subdir in APP_SUBDIRS:
            assert (Path(".agentprof") / subdir).is_dir()
        assert DEFAULT_STORE_PATH.is_file()


def test_doctor_requires_init() -> None:
    with runner.isolated_filesystem():
        result = runner.invoke(app, ["doctor"])

        assert result.exit_code == 2
        assert "workspace is incomplete" in result.output


def test_doctor_after_init(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 0
        assert "workspace looks ready" in doctor_result.output


def test_doctor_allows_existing_workspace_without_gitignore(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        Path(".agentprof/.gitignore").unlink()

        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 0
        assert "workspace looks ready" in doctor_result.output


def test_doctor_fails_when_hash_salt_is_missing() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 2
        assert "privacy configuration issues" in doctor_result.output
        assert "AGENTPROF_HASH_SALT is not set" in doctor_result.output


def test_doctor_fails_when_hash_salt_is_weak(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "short")
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 2
        assert "at least" in doctor_result.output


def test_doctor_fails_when_raw_io_storage_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "test-salt-value-123")
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        Path("agentprof.yml").write_text(
            "privacy:\n  store_raw_io: true\n",
            encoding="utf-8",
        )

        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 2
        assert "store_raw_io is true" in doctor_result.output


def test_cli_warns_when_raw_io_storage_is_enabled() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        Path("agentprof.yml").write_text(
            "privacy:\n  store_raw_io: true\n  hash_inputs: false\n",
            encoding="utf-8",
        )

        stats_result = runner.invoke(app, ["store", "stats"])

        assert init_result.exit_code == 0
        assert stats_result.exit_code == 0
        assert "WARNING: privacy.store_raw_io is true" in stats_result.output


def test_doctor_fails_when_store_is_missing() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        DEFAULT_STORE_PATH.unlink()

        doctor_result = runner.invoke(app, ["doctor"])

        assert init_result.exit_code == 0
        assert doctor_result.exit_code == 2
        assert "workspace is not usable" in doctor_result.output
        assert not DEFAULT_STORE_PATH.exists()


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
