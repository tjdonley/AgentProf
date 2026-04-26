from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any


DEFAULT_VOLATILE_KEYS = frozenset(
    {
        "created_at",
        "event_id",
        "idempotency_key",
        "message_id",
        "observation_id",
        "parent_observation_id",
        "parent_span_id",
        "request_id",
        "run_id",
        "span_id",
        "timestamp",
        "trace_id",
        "traceparent",
        "tracestate",
        "updated_at",
    }
)

DEFAULT_VOLATILE_SUFFIXES = (
    "_request_id",
    "_span_id",
    "_trace_id",
    "_timestamp",
)

UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
LONG_HEX_RE = re.compile(r"\b[0-9a-fA-F]{16,}\b")
ISO_TIMESTAMP_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
)


class MissingSaltError(RuntimeError):
    """Raised when an HMAC salt is required but not configured."""


def salt_from_env(env_name: str) -> bytes:
    value = os.getenv(env_name)
    if not value:
        raise MissingSaltError(f"{env_name} is not set")
    return value.encode("utf-8")


def hmac_sha256_hexdigest(content: str | bytes, salt: str | bytes) -> str:
    content_bytes = _to_bytes(content)
    salt_bytes = _to_bytes(salt)
    return hmac.new(salt_bytes, content_bytes, hashlib.sha256).hexdigest()


def content_hash(content: Any, salt: str | bytes) -> str:
    return hmac_sha256_hexdigest(
        canonicalize_for_hash(content, strip_volatile=False), salt
    )


def retry_fingerprint(content: Any, salt: str | bytes) -> str:
    return hmac_sha256_hexdigest(
        canonicalize_for_hash(content, strip_volatile=True), salt
    )


def canonicalize_for_hash(content: Any, *, strip_volatile: bool = False) -> str:
    normalized = _normalize(content, strip_volatile=strip_volatile)
    return json.dumps(
        normalized,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _normalize(value: Any, *, strip_volatile: bool) -> Any:
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")

    if isinstance(value, str):
        parsed = _try_parse_json(value)
        if parsed is not None:
            return _normalize(parsed, strip_volatile=strip_volatile)
        return _normalize_text(value, strip_volatile=strip_volatile)

    if isinstance(value, Mapping):
        normalized_items: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key)
            if strip_volatile and _is_volatile_key(key):
                continue
            normalized_items[key] = _normalize(raw_value, strip_volatile=strip_volatile)
        return normalized_items

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize(item, strip_volatile=strip_volatile) for item in value]

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if value is None or isinstance(value, (bool, int, float)):
        return value

    return str(value)


def _normalize_text(value: str, *, strip_volatile: bool) -> str:
    text = " ".join(value.split())
    if not strip_volatile:
        return text

    text = ISO_TIMESTAMP_RE.sub("[TIMESTAMP]", text)
    text = UUID_RE.sub("[UUID]", text)
    text = LONG_HEX_RE.sub("[HEX]", text)
    return text


def _try_parse_json(value: str) -> Any | None:
    value = value.strip()
    if not value or value[0] not in "[{\"":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _is_volatile_key(key: str) -> bool:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key.strip())
    normalized = normalized.lower().replace("-", "_").replace(".", "_")
    return normalized in DEFAULT_VOLATILE_KEYS or normalized.endswith(
        DEFAULT_VOLATILE_SUFFIXES
    )


def _to_bytes(value: str | bytes) -> bytes:
    if isinstance(value, bytes):
        return value
    return value.encode("utf-8")
