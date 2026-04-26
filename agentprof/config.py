from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


APP_DIR = Path(".agentprof")
CONFIG_FILE = Path("agentprof.yml")
APP_SUBDIRS = ("data", "baselines", "reports", "cache")
DEFAULT_STORE_PATH = APP_DIR / "data" / "agentprof.duckdb"

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

store:
  path: .agentprof/data/agentprof.duckdb
"""


class ProjectConfig(BaseModel):
    name: str = "tracer"
    environment: str = "development"


class PrivacyConfig(BaseModel):
    store_raw_io: bool = False
    store_redacted_io: bool = True
    hash_inputs: bool = True
    hmac_salt_env: str = "AGENTPROF_HASH_SALT"
    max_evidence_chars: int = Field(default=500, ge=0)


class LangfuseSourceConfig(BaseModel):
    host_env: str = "LANGFUSE_HOST"
    public_key_env: str = "LANGFUSE_PUBLIC_KEY"
    secret_key_env: str = "LANGFUSE_SECRET_KEY"
    default_lag: str = "5m"


class SourcesConfig(BaseModel):
    langfuse: LangfuseSourceConfig = Field(default_factory=LangfuseSourceConfig)


class StoreConfig(BaseModel):
    path: Path = DEFAULT_STORE_PATH


class AgentProfConfig(BaseModel):
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    privacy: PrivacyConfig = Field(default_factory=PrivacyConfig)
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)


def write_default_config(path: Path = CONFIG_FILE, *, force: bool = False) -> bool:
    if path.exists() and not force:
        return False

    path.write_text(DEFAULT_CONFIG, encoding="utf-8")
    return True


def load_config(path: Path = CONFIG_FILE) -> AgentProfConfig:
    if not path.exists():
        raise FileNotFoundError(path)

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    data: dict[str, Any] = raw or {}
    return AgentProfConfig.model_validate(data)


def ensure_workspace_dirs(app_dir: Path = APP_DIR) -> list[Path]:
    paths: list[Path] = []
    for subdir in APP_SUBDIRS:
        path = app_dir / subdir
        path.mkdir(parents=True, exist_ok=True)
        paths.append(path)
    return paths
