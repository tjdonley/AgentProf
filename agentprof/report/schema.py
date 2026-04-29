from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from pydantic import BaseModel, Field


class ReportBuildResult(BaseModel):
    report_id: str
    project: str
    issues: int
    evidence_items: int
    cost_entries: int
    total_wasted_cost_usd: Decimal = Field(default=Decimal("0"))
    report_md_path: Path
    report_json_path: Path
    report_html_path: Path
