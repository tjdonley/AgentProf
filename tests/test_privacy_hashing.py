from __future__ import annotations

import os

import pytest

from agentprof.privacy.hashing import (
    MissingSaltError,
    canonicalize_for_hash,
    content_hash,
    retry_fingerprint,
    salt_from_env,
)


def test_content_hash_is_hmac_sha256_and_salt_sensitive() -> None:
    first = content_hash({"message": "hello"}, "salt-a")
    second = content_hash({"message": "hello"}, "salt-a")
    third = content_hash({"message": "hello"}, "salt-b")

    assert first == second
    assert first != third
    assert len(first) == 64


def test_canonical_json_ignores_key_order() -> None:
    left = canonicalize_for_hash({"b": 2, "a": 1})
    right = canonicalize_for_hash('{"a": 1, "b": 2}')

    assert left == right


def test_canonical_json_preserves_quoted_string_payloads() -> None:
    raw_text = canonicalize_for_hash("hello")
    json_encoded_text = canonicalize_for_hash('"hello"')

    assert raw_text != json_encoded_text
    assert content_hash("hello", "salt") != content_hash('"hello"', "salt")


def test_retry_fingerprint_strips_volatile_keys_and_values() -> None:
    first = {
        "tool": "refund_policy_lookup",
        "region": "us-east",
        "request_id": "req_123",
        "traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        "created_at": "2026-04-26T10:11:12Z",
    }
    second = {
        "created_at": "2026-04-26T10:13:14Z",
        "region": "us-east",
        "request_id": "req_456",
        "tool": "refund_policy_lookup",
        "traceparent": "00-fedcba9876543210fedcba9876543210-fedcba9876543210-01",
    }

    assert retry_fingerprint(first, "salt") == retry_fingerprint(second, "salt")


def test_retry_fingerprint_strips_camel_case_vendor_ids() -> None:
    first = {
        "traceId": "trace-a",
        "parentObservationId": "parent-a",
        "region": "us-east",
    }
    second = {
        "traceId": "trace-b",
        "parentObservationId": "parent-b",
        "region": "us-east",
    }

    assert retry_fingerprint(first, "salt") == retry_fingerprint(second, "salt")


def test_content_hash_keeps_volatile_keys_for_raw_hashing() -> None:
    first = {"tool": "lookup", "request_id": "req_123"}
    second = {"tool": "lookup", "request_id": "req_456"}

    assert content_hash(first, "salt") != content_hash(second, "salt")


def test_retry_fingerprint_normalizes_long_hex_inside_text() -> None:
    first = "failed request 0123456789abcdef at 2026-04-26T10:11:12Z"
    second = "failed request fedcba9876543210 at 2026-04-26T10:13:14Z"

    assert retry_fingerprint(first, "salt") == retry_fingerprint(second, "salt")


def test_salt_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENTPROF_TEST_SALT", "secret")

    assert salt_from_env("AGENTPROF_TEST_SALT") == b"secret"


def test_salt_from_env_requires_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AGENTPROF_TEST_SALT", raising=False)

    with pytest.raises(MissingSaltError):
        salt_from_env("AGENTPROF_TEST_SALT")

    assert os.getenv("AGENTPROF_TEST_SALT") is None
