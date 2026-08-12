from __future__ import annotations

import json
import os
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


def test_demo_runs_self_contained_pipeline(monkeypatch) -> None:
    monkeypatch.delenv("AGENTPROF_HASH_SALT", raising=False)

    with runner.isolated_filesystem():
        result = runner.invoke(app, ["demo"])
        report_dir = Path("agentprof-demo/reports")
        payload = json.loads((report_dir / "demo.json").read_text(encoding="utf-8"))

        assert result.exit_code == 0
        assert "Demo complete" in result.output
        assert "AgentProf found 4 issue(s)" in result.output
        assert "$0.054000000" in result.output
        assert "2.00x" in result.output
        assert "baseline across 3 agents" in result.output
        assert not Path(".agentprof").exists()
        assert Path("agentprof-demo/.agentprof-demo").is_file()
        assert Path("agentprof-demo/data/agentprof.duckdb").is_file()
        assert (report_dir / "demo.html").is_file()
        assert (report_dir / "demo.md").is_file()
        assert (report_dir / "demo.json").is_file()
        assert (report_dir / "demo-multi-agent-waste.svg").is_file()
        assert payload["summary"]["issues_by_kind"] == {
            "multi_agent_waste": 1,
            "retry_loop": 1,
            "spec_violation": 2,
        }
        assert payload["summary"]["total_wasted_cost_usd"] == "0.054000000"
        assert payload["summary"]["gross_wasted_cost_usd"] == "0.060000000"
        assert payload["summary"]["overlapping_wasted_cost_usd"] == "0.006000000"
        assert "AGENTPROF_HASH_SALT" not in os.environ


def test_demo_uses_demo_salt_when_existing_salt_is_weak(monkeypatch) -> None:
    monkeypatch.setenv("AGENTPROF_HASH_SALT", "short")

    with runner.isolated_filesystem():
        result = runner.invoke(app, ["demo"])

        assert result.exit_code == 0
        assert "Demo complete" in result.output
        assert os.environ["AGENTPROF_HASH_SALT"] == "short"


def test_demo_refuses_to_reset_existing_unmarked_store() -> None:
    with runner.isolated_filesystem():
        init_result = runner.invoke(app, ["init"])
        demo_result = runner.invoke(app, ["demo", "--dir", ".agentprof"])

        assert init_result.exit_code == 0
        assert demo_result.exit_code == 2
        assert "Refusing to reset an existing AgentProf store" in demo_result.output


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
