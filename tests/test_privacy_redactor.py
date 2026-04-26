from __future__ import annotations

from agentprof.config import CustomRedactionPatternConfig, RedactionConfig
from agentprof.privacy.redactor import (
    CustomPattern,
    RedactionRules,
    evidence_preview,
    redact_text,
    redact_value,
    rules_from_config,
)


def test_redact_text_removes_common_sensitive_values() -> None:
    text = (
        "email user@example.com phone (415) 555-0101 card 4242 4242 4242 4242 "
        "jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.signature "
        "key sk-abcdefghijklmnopqrstuvwxyz123456 "
        "url https://example.com/callback?token=secret"
    )

    redacted = redact_text(text)

    assert "user@example.com" not in redacted
    assert "415" not in redacted
    assert "4242 4242" not in redacted
    assert "eyJhbGci" not in redacted
    assert "sk-abcdefghijklmnopqrstuvwxyz123456" not in redacted
    assert "token=secret" not in redacted
    assert "[EMAIL]" in redacted
    assert "[PHONE]" in redacted
    assert "[CARD]" in redacted
    assert "[JWT]" in redacted
    assert "[SECRET]" in redacted
    assert "[URL_WITH_QUERY_REDACTED]" in redacted


def test_redact_text_supports_custom_patterns() -> None:
    rules = RedactionRules(
        custom_patterns=(
            CustomPattern(name="internal_customer_id", regex=r"cust_[A-Za-z0-9]+"),
        )
    )

    assert redact_text("customer cust_ABC123", rules) == "customer [INTERNAL_CUSTOMER_ID]"


def test_rules_from_config_builds_redaction_rules() -> None:
    config = RedactionConfig(
        emails=False,
        custom_patterns=[
            CustomRedactionPatternConfig(
                name="internal_customer_id", regex=r"cust_[A-Za-z0-9]+"
            )
        ],
    )

    rules = rules_from_config(config)

    redacted = redact_text("user@example.com cust_ABC123", rules)

    assert "user@example.com" in redacted
    assert "[INTERNAL_CUSTOMER_ID]" in redacted


def test_redact_value_recurses_through_structures() -> None:
    value = {
        "input": "contact user@example.com",
        "messages": ["call 415-555-0101"],
        "count": 2,
    }

    redacted = redact_value(value)

    assert redacted == {
        "input": "contact [EMAIL]",
        "messages": ["call [PHONE]"],
        "count": 2,
    }


def test_evidence_preview_redacts_and_caps_length() -> None:
    preview = evidence_preview(
        {"body": "email user@example.com and lots of text"},
        max_chars=30,
    )

    assert len(preview) == 30
    assert preview.endswith("...")
    assert "user@example.com" not in preview


def test_evidence_preview_can_be_disabled_with_zero_chars() -> None:
    assert evidence_preview("secret user@example.com", max_chars=0) == ""
