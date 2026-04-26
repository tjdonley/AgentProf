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
    assert config.privacy.redact.emails is True
    assert config.privacy.redact.custom_patterns == []
    assert config.store.path == Path(".agentprof/data/agentprof.duckdb")


def test_write_default_config_does_not_overwrite_without_force(tmp_path: Path) -> None:
    config_path = tmp_path / "agentprof.yml"
    config_path.write_text("project:\n  name: custom\n", encoding="utf-8")

    wrote = write_default_config(config_path)
    config = load_config(config_path)

    assert wrote is False
    assert config.project.name == "custom"


def test_load_config_supports_custom_redaction_patterns(tmp_path: Path) -> None:
    config_path = tmp_path / "agentprof.yml"
    config_path.write_text(
        """
privacy:
  redact:
    custom_patterns:
      - name: internal_customer_id
        regex: "cust_[A-Za-z0-9]+"
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.privacy.redact.custom_patterns[0].name == "internal_customer_id"
    assert config.privacy.redact.custom_patterns[0].regex == "cust_[A-Za-z0-9]+"


def test_load_config_requires_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.yml")
