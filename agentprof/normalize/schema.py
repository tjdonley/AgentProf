from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field


SourceName = Literal["langfuse", "phoenix", "otel", "file"]
SpanType = Literal[
    "root",
    "agent",
    "llm",
    "tool",
    "retriever",
    "embedding",
    "handoff",
    "guardrail",
    "eval",
    "custom",
    "unknown",
]
SpanStatus = Literal["ok", "error", "timeout", "cancelled", "unknown"]
TraceOutcome = Literal["success", "failure", "partial", "unknown"]
CostConfidence = Literal["source", "estimated", "unknown"]


class NormalizedTrace(BaseModel):
    trace_id: str
    source: SourceName
    project: str | None = None
    root_span_id: str | None = None
    root_name: str | None = None
    session_id: str | None = None
    user_hash: str | None = None
    environment: str | None = None
    version: str | None = None
    tags: list[str] = Field(default_factory=list)
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_ms: float | None = None
    outcome: TraceOutcome = "unknown"
    total_cost_usd: Decimal | None = None
    total_input_tokens: int | None = None
    total_output_tokens: int | None = None
    total_tool_calls: int = 0
    total_model_calls: int = 0
    raw_ref: str | None = None


class NormalizedSpan(BaseModel):
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    source: SourceName
    name: str
    span_type: SpanType = "unknown"
    operation_name: str | None = None
    agent_name: str | None = None
    tool_name: str | None = None
    model_name: str | None = None
    provider_name: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_ms: float | None = None
    status: SpanStatus = "unknown"
    status_message: str | None = None
    error_type: str | None = None
    error_signature: str | None = None
    input_hash: str | None = None
    output_hash: str | None = None
    input_retry_fingerprint: str | None = None
    output_retry_fingerprint: str | None = None
    input_preview: str | None = None
    output_preview: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: Decimal | None = None
    cost_confidence: CostConfidence = "unknown"
    attributes: dict[str, Any] = Field(default_factory=dict)
    raw_ref: str | None = None


class DataQualityMetrics(BaseModel):
    total_spans: int = 0
    total_traces: int = 0
    spans_with_parent_ids: int = 0
    spans_with_valid_parent_links: int = 0
    spans_with_status: int = 0
    spans_with_cost: int = 0
    spans_with_token_counts: int = 0
    spans_with_model: int = 0
    spans_with_io_hashes: int = 0
    parent_coverage_pct: float = 0.0
    status_coverage_pct: float = 0.0
    cost_coverage_pct: float = 0.0
    token_coverage_pct: float = 0.0
    model_coverage_pct: float = 0.0
    io_hash_coverage_pct: float = 0.0


class NormalizationResult(BaseModel):
    raw_spans_seen: int
    normalized_spans: int
    normalized_traces: int
    data_quality: DataQualityMetrics
