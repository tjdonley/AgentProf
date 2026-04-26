from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from agentprof.config import AgentProfConfig
from agentprof.privacy.hashing import content_hash, salt_from_env
from agentprof.privacy.redactor import evidence_preview, redact_value, rules_from_config
from agentprof.store.duckdb_store import DuckDBStore, RawSpanRecord


LANGFUSE_SOURCE = "langfuse"
INPUT_FIELD = "input"
OUTPUT_FIELD = "output"


class LangfuseExportFormat(StrEnum):
    auto = "auto"
    json = "json"
    csv = "csv"


@dataclass(frozen=True)
class LangfuseExportImportResult:
    observations_seen: int
    observations_imported: int
    raw_ref: str


class LangfuseExportImportError(RuntimeError):
    """Raised when a Langfuse export cannot be imported safely."""


def import_langfuse_export(
    *,
    observations_path: Path,
    store: DuckDBStore,
    config: AgentProfConfig,
    file_format: LangfuseExportFormat = LangfuseExportFormat.auto,
) -> LangfuseExportImportResult:
    observations = load_observations(observations_path, file_format=file_format)
    raw_ref = str(observations_path)
    records = [
        observation_to_raw_span_record(
            observation,
            config=config,
            raw_ref=raw_ref,
            ordinal=ordinal,
        )
        for ordinal, observation in enumerate(observations, start=1)
    ]

    imported = store.insert_raw_spans(records)
    return LangfuseExportImportResult(
        observations_seen=len(observations),
        observations_imported=imported,
        raw_ref=raw_ref,
    )


def load_observations(
    path: Path,
    *,
    file_format: LangfuseExportFormat = LangfuseExportFormat.auto,
) -> list[dict[str, Any]]:
    resolved_format = _resolve_format(path, file_format)
    if resolved_format == LangfuseExportFormat.csv:
        return _load_csv_observations(path)
    return _load_json_observations(path)


def observation_to_raw_span_record(
    observation: dict[str, Any],
    *,
    config: AgentProfConfig,
    raw_ref: str,
    ordinal: int,
) -> RawSpanRecord:
    payload = sanitize_observation_payload(observation, config=config)
    source_id = _string_field(observation, "id") or f"{raw_ref}#{ordinal}"
    trace_id = _string_field(observation, "traceId", "trace_id")
    span_id = _string_field(observation, "id", "spanId", "span_id")
    parent_span_id = _string_field(
        observation,
        "parentObservationId",
        "parent_observation_id",
        "parentSpanId",
        "parent_span_id",
    )

    return RawSpanRecord(
        source=LANGFUSE_SOURCE,
        source_id=source_id,
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        payload_json=json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            default=str,
        ),
        raw_ref=raw_ref,
    )


def sanitize_observation_payload(
    observation: dict[str, Any],
    *,
    config: AgentProfConfig,
) -> dict[str, Any]:
    payload = dict(observation)
    privacy = config.privacy
    rules = rules_from_config(privacy.redact)
    raw_input = payload.get(INPUT_FIELD)
    raw_output = payload.get(OUTPUT_FIELD)
    salt = _hash_salt(config, raw_input=raw_input, raw_output=raw_output)

    payload.pop(INPUT_FIELD, None)
    payload.pop(OUTPUT_FIELD, None)
    payload = redact_value(payload, rules)
    if privacy.store_raw_io:
        if raw_input is not None:
            payload[INPUT_FIELD] = raw_input
        if raw_output is not None:
            payload[OUTPUT_FIELD] = raw_output

    payload["_agentprof_privacy"] = _privacy_metadata(
        raw_input=raw_input,
        raw_output=raw_output,
        config=config,
        salt=salt,
    )
    return payload


def _privacy_metadata(
    *,
    raw_input: Any,
    raw_output: Any,
    config: AgentProfConfig,
    salt: bytes | None,
) -> dict[str, Any]:
    privacy = config.privacy
    rules = rules_from_config(privacy.redact)
    metadata: dict[str, Any] = {
        "raw_io_stored": privacy.store_raw_io,
        "redacted_io_stored": privacy.store_redacted_io,
        "input_hash": None,
        "output_hash": None,
        "input_preview": None,
        "output_preview": None,
    }

    if privacy.hash_inputs and salt is not None:
        if raw_input is not None:
            metadata["input_hash"] = content_hash(raw_input, salt)
        if raw_output is not None:
            metadata["output_hash"] = content_hash(raw_output, salt)

    if privacy.store_redacted_io:
        if raw_input is not None:
            metadata["input_preview"] = evidence_preview(
                raw_input,
                max_chars=privacy.max_evidence_chars,
                rules=rules,
            )
        if raw_output is not None:
            metadata["output_preview"] = evidence_preview(
                raw_output,
                max_chars=privacy.max_evidence_chars,
                rules=rules,
            )

    return metadata


def _hash_salt(
    config: AgentProfConfig,
    *,
    raw_input: Any,
    raw_output: Any,
) -> bytes | None:
    if not config.privacy.hash_inputs or (raw_input is None and raw_output is None):
        return None
    return salt_from_env(config.privacy.hmac_salt_env)


def _load_json_observations(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as file:
            parsed = json.load(file)
    except json.JSONDecodeError as exc:
        raise LangfuseExportImportError(
            f"Could not parse Langfuse observations JSON: {exc}"
        ) from exc

    if isinstance(parsed, list):
        observations = parsed
    elif isinstance(parsed, dict):
        if "data" in parsed:
            observations = parsed["data"]
        else:
            observations = parsed.get("observations")
    else:
        observations = None

    if not isinstance(observations, list):
        raise LangfuseExportImportError(
            "Expected a Langfuse observations JSON array or an object with a data/observations array."
        )

    return _validate_observations(observations)


def _load_csv_observations(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as file:
            return _validate_observations(list(csv.DictReader(file, strict=True)))
    except csv.Error as exc:
        raise LangfuseExportImportError(
            f"Could not parse Langfuse observations CSV: {exc}"
        ) from exc


def _validate_observations(observations: list[Any]) -> list[dict[str, Any]]:
    invalid = [
        index
        for index, item in enumerate(observations, start=1)
        if not isinstance(item, dict)
    ]
    if invalid:
        raise LangfuseExportImportError(
            f"Expected observation objects; invalid entries at positions: {invalid[:5]}"
        )
    return observations


def _resolve_format(
    path: Path, file_format: LangfuseExportFormat
) -> LangfuseExportFormat:
    if file_format != LangfuseExportFormat.auto:
        return file_format
    if path.suffix.lower() == ".csv":
        return LangfuseExportFormat.csv
    return LangfuseExportFormat.json


def _string_field(observation: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = observation.get(key)
        if value is not None and value != "":
            return str(value)
    return None
