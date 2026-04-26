from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any


EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}(?!\d)"
)
CREDIT_CARD_RE = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
URL_WITH_QUERY_RE = re.compile(r"https?://[^\s?#)\]}'\"]+\?[^\s)\]}'\"]+")
API_KEY_RE = re.compile(
    r"(?i)\b(?:api[_-]?key|secret|token|authorization)\s*[:=]\s*['\"]?[^'\"\s,}]+"
    r"|\b(?:sk|pk|rk|key|secret)[-_][A-Za-z0-9_-]{16,}\b"
)


@dataclass(frozen=True)
class CustomPattern:
    name: str
    regex: str


@dataclass(frozen=True)
class RedactionRules:
    emails: bool = True
    phone_numbers: bool = True
    api_keys: bool = True
    credit_cards: bool = True
    jwt_tokens: bool = True
    custom_patterns: Sequence[CustomPattern] = field(default_factory=tuple)


DEFAULT_REDACTION_RULES = RedactionRules()


def redact_text(text: str, rules: RedactionRules = DEFAULT_REDACTION_RULES) -> str:
    redacted = text

    if rules.jwt_tokens:
        redacted = JWT_RE.sub("[JWT]", redacted)
    redacted = URL_WITH_QUERY_RE.sub("[URL_WITH_QUERY_REDACTED]", redacted)
    if rules.api_keys:
        redacted = API_KEY_RE.sub("[SECRET]", redacted)
    if rules.emails:
        redacted = EMAIL_RE.sub("[EMAIL]", redacted)
    if rules.phone_numbers:
        redacted = PHONE_RE.sub("[PHONE]", redacted)
    if rules.credit_cards:
        redacted = CREDIT_CARD_RE.sub(_redact_credit_card_match, redacted)

    for pattern in rules.custom_patterns:
        label = _custom_label(pattern.name)
        redacted = re.sub(pattern.regex, label, redacted)

    return redacted


def rules_from_config(redact_config: Any) -> RedactionRules:
    custom_patterns = tuple(
        CustomPattern(name=pattern.name, regex=pattern.regex)
        for pattern in getattr(redact_config, "custom_patterns", [])
    )
    return RedactionRules(
        emails=getattr(redact_config, "emails", True),
        phone_numbers=getattr(redact_config, "phone_numbers", True),
        api_keys=getattr(redact_config, "api_keys", True),
        credit_cards=getattr(redact_config, "credit_cards", True),
        jwt_tokens=getattr(redact_config, "jwt_tokens", True),
        custom_patterns=custom_patterns,
    )


def redact_value(value: Any, rules: RedactionRules = DEFAULT_REDACTION_RULES) -> Any:
    if isinstance(value, str):
        return redact_text(value, rules)

    if isinstance(value, bytes):
        return redact_text(value.decode("utf-8", errors="replace"), rules)

    if isinstance(value, Mapping):
        return {key: redact_value(item, rules) for key, item in value.items()}

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact_value(item, rules) for item in value]

    return value


def evidence_preview(
    value: Any,
    *,
    max_chars: int,
    rules: RedactionRules = DEFAULT_REDACTION_RULES,
) -> str:
    if max_chars <= 0:
        return ""

    redacted = redact_value(value, rules)
    if isinstance(redacted, str):
        preview = redacted
    else:
        preview = json.dumps(redacted, ensure_ascii=True, sort_keys=True, default=str)

    if len(preview) <= max_chars:
        return preview

    if max_chars <= 3:
        return "." * max_chars
    return f"{preview[: max_chars - 3]}..."


def _custom_label(name: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").upper()
    return f"[{normalized or 'REDACTED'}]"


def _redact_credit_card_match(match: re.Match[str]) -> str:
    candidate = match.group(0)
    digits = re.sub(r"\D", "", candidate)
    if _passes_luhn(digits):
        return "[CARD]"
    return candidate


def _passes_luhn(digits: str) -> bool:
    if len(digits) < 13 or len(digits) > 19:
        return False

    total = 0
    reverse_digits = digits[::-1]
    for index, char in enumerate(reverse_digits):
        value = int(char)
        if index % 2 == 1:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0
