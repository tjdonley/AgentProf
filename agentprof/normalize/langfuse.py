from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from agentprof.normalize.schema import NormalizedSpan, SpanStatus, SpanType
from agentprof.store.duckdb_store import RawSpanRow


KNOWN_PAYLOAD_FIELDS = {
    "id",
    "traceId",
    "trace_id",
    "parentObservationId",
    "parent_observation_id",
    "parentSpanId",
    "parent_span_id",
    "type",
    "name",
    "level",
    "statusMessage",
    "startTime",
    "start_time",
    "endTime",
    "end_time",
    "completionStartTime",
    "createdAt",
    "updatedAt",
    "model",
    "providedModelName",
    "modelName",
    "provider",
    "providerName",
    "usageDetails",
    "costDetails",
    "totalCost",
    "inputUsage",
    "outputUsage",
    "totalUsage",
    "latency",
    "environment",
    "version",
    "sessionId",
    "userId",
    "metadata",
    "_agentprof_privacy",
}
TRACE_ATTRIBUTE_FIELDS = ("sessionId", "userId", "environment", "version")


def map_langfuse_raw_span(row: RawSpanRow) -> NormalizedSpan:
    payload = json.loads(row.payload_json)
    privacy = payload.get("_agentprof_privacy") or {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    name = _string_field(payload, "name") or row.span_id or row.source_id
    parent_span_id = row.parent_span_id or _string_field(
        payload,
        "parentObservationId",
        "parent_observation_id",
        "parentSpanId",
        "parent_span_id",
    )
    span_type = classify_langfuse_span(payload, parent_span_id)
    usage = payload.get("usageDetails") if isinstance(payload.get("usageDetails"), dict) else {}
    input_tokens = _first_present_int(payload, ("inputUsage",), usage, ("input",))
    output_tokens = _first_present_int(payload, ("outputUsage",), usage, ("output",))
    total_tokens = _first_present_int(payload, ("totalUsage",), usage, ("total",))
    cost = _cost(payload)
    start_time = _datetime_field(payload, "startTime", "start_time")
    end_time = _datetime_field(payload, "endTime", "end_time")

    return NormalizedSpan(
        trace_id=row.trace_id
        or _string_field(payload, "traceId", "trace_id")
        or f"langfuse:{row.source_id}",
        span_id=(
            row.span_id
            or _string_field(payload, "id", "spanId", "span_id")
            or row.source_id
        ),
        parent_span_id=parent_span_id,
        source="langfuse",
        name=name,
        span_type=span_type,
        operation_name=name,
        agent_name=name if span_type in {"agent", "root"} else None,
        tool_name=name if span_type == "tool" else None,
        model_name=_string_field(payload, "providedModelName", "model", "modelName"),
        provider_name=_string_field(payload, "providerName", "provider"),
        start_time=start_time,
        end_time=end_time,
        duration_ms=_duration_ms(start_time, end_time, payload.get("latency")),
        status=_status(payload),
        status_message=_string_field(payload, "statusMessage"),
        error_type=_string_field(payload, "errorType")
        or _string_field(metadata, "error_type", "errorType"),
        error_signature=_error_signature(_string_field(payload, "statusMessage")),
        input_hash=_string_field(privacy, "input_hash"),
        output_hash=_string_field(privacy, "output_hash"),
        input_preview=_string_field(privacy, "input_preview"),
        output_preview=_string_field(privacy, "output_preview"),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        cost_usd=cost,
        cost_confidence="source" if cost is not None else "unknown",
        attributes=_attributes(payload),
        raw_ref=row.raw_ref,
    )


def classify_langfuse_span(payload: dict[str, Any], parent_span_id: str | None) -> SpanType:
    if parent_span_id is None:
        return "root"

    observation_type = str(payload.get("type") or "").upper()
    name = str(payload.get("name") or "").lower()

    if observation_type == "GENERATION":
        return "llm"
    if "retriever" in name or "retrieval" in name:
        return "retriever"
    if "embedding" in name:
        return "embedding"
    if "agent" in name:
        return "agent"
    if any(marker in name for marker in ("tool", "lookup", "search", "function")):
        return "tool"
    if observation_type in {"SPAN", "EVENT"}:
        return "custom"
    return "unknown"


def _status(payload: dict[str, Any]) -> SpanStatus:
    level = str(payload.get("level") or "").upper()
    status = str(payload.get("status") or "").lower()
    message = str(payload.get("statusMessage") or "").lower()
    if level == "ERROR" or status == "error":
        return "error"
    if "timeout" in message or status == "timeout":
        return "timeout"
    if status == "cancelled":
        return "cancelled"
    if level in {"DEFAULT", "DEBUG", "WARNING"} or status in {"ok", "success"}:
        return "ok"
    return "unknown"


def _attributes(payload: dict[str, Any]) -> dict[str, Any]:
    attributes = {
        key: value for key, value in payload.items() if key not in KNOWN_PAYLOAD_FIELDS
    }
    for key in TRACE_ATTRIBUTE_FIELDS:
        value = payload.get(key)
        if value not in (None, ""):
            attributes[key] = value
    return attributes


def _cost(payload: dict[str, Any]) -> Decimal | None:
    direct = _decimal(payload.get("totalCost"))
    if direct is not None:
        return direct
    cost_details = payload.get("costDetails")
    if isinstance(cost_details, dict):
        return _decimal(cost_details.get("total"))
    return None


def _duration_ms(
    start_time: datetime | None, end_time: datetime | None, latency: Any
) -> float | None:
    if start_time and end_time:
        return max(0.0, (end_time.timestamp() - start_time.timestamp()) * 1000)
    numeric_latency = _float(latency)
    if numeric_latency is not None:
        return numeric_latency * 1000
    return None


def _datetime_field(mapping: dict[str, Any], *keys: str) -> datetime | None:
    value = _field(mapping, *keys)
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _string_field(mapping: dict[str, Any], *keys: str) -> str | None:
    value = _field(mapping, *keys)
    if value is None or value == "":
        return None
    return str(value)


def _int_field(mapping: dict[str, Any], *keys: str) -> int | None:
    value = _field(mapping, *keys)
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _first_present_int(
    first_mapping: dict[str, Any],
    first_keys: tuple[str, ...],
    second_mapping: dict[str, Any],
    second_keys: tuple[str, ...],
) -> int | None:
    first = _int_field(first_mapping, *first_keys)
    if first is not None:
        return first
    return _int_field(second_mapping, *second_keys)


def _field(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _error_signature(message: str | None) -> str | None:
    if not message:
        return None
    text = re.sub(r"\b\d+\b", "#", message.lower())
    return " ".join(text.split())
