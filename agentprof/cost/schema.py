from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


CostType = Literal["successful_span_cost", "failed_span_cost", "unknown_span_cost"]


class CostWaterfallRow(BaseModel):
    cost_type: CostType
    entries: int
    amount_usd: Decimal = Field(default=Decimal("0"))


class CostLedgerBuildResult(BaseModel):
    normalized_spans_seen: int
    ledger_entries: int
    traces_with_cost: int
    total_cost_usd: Decimal = Field(default=Decimal("0"))
    waterfall: list[CostWaterfallRow] = Field(default_factory=list)
