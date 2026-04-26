from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class RetryLoopFinding(BaseModel):
    issue_id: str
    trace_id: str
    parent_span_id: str | None = None
    name: str
    attempts: int
    wasted_attempts: int
    affected_span_ids: list[str] = Field(default_factory=list)
    wasted_span_ids: list[str] = Field(default_factory=list)
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    total_cost_usd: Decimal = Field(default=Decimal("0"))
    wasted_cost_usd: Decimal = Field(default=Decimal("0"))
    error_signature: str | None = None


class RetryLoopAnalysisResult(BaseModel):
    normalized_spans_seen: int
    retry_loops: int
    affected_traces: int
    affected_spans: int
    wasted_attempts: int
    wasted_cost_usd: Decimal = Field(default=Decimal("0"))
    findings: list[RetryLoopFinding] = Field(default_factory=list)
