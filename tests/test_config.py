from __future__ import annotations

from pathlib import Path

import pytest

from agentprof.config import AgentProfConfig, load_config, write_default_config


def test_load_default_config(tmp_path: Path) -> None:
    config_path = tmp_path / "agentprof.yml"
    wrote = write_default_config(config_path)

    config = load_config(config_path)

    assert wrote is True
    assert isinstance(config, AgentProfConfig)
    assert config.project.name == "tracer"
    assert config.privacy.store_raw_io is False
    assert config.store.path == Path(".agentprof/data/agentprof.duckdb")


def test_write_default_config_does_not_overwrite_without_force(tmp_path: Path) -> None:
    config_path = tmp_path / "agentprof.yml"
    config_path.write_text("project:\n  name: custom\n", encoding="utf-8")

    wrote = write_default_config(config_path)
    config = load_config(config_path)

    assert wrote is False
    assert config.project.name == "custom"


def test_load_config_requires_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yml")
