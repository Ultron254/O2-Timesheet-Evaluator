from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    upload_id: int
    status: str


class UploadStatusResponse(BaseModel):
    id: int
    filename: str
    uploaded_at: Optional[datetime] = None
    row_count: Optional[int] = None
    employee_count: Optional[int] = None
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    status: str
    error_message: Optional[str] = None


class FindingResponse(BaseModel):
    id: int
    upload_id: int
    row_index: int
    employee: str
    department: str
    date: str
    hours: float
    task: str
    client: str
    composite_score: float
    severity: str
    rules_triggered: List[str] = Field(default_factory=list)
    ml_scores: Dict[str, Any] = Field(default_factory=dict)
    explanation: str
    created_at: Optional[datetime] = None


class SummaryResponse(BaseModel):
    upload_id: int
    total_entries: int
    critical_count: int
    high_count: int
    moderate_count: int
    low_count: int
    billable_utilization: float
    total_hours: float
    employee_count: int
    department_count: int
    summary_json: Dict[str, Any] = Field(default_factory=dict)


class ResultsResponse(BaseModel):
    upload: UploadStatusResponse
    summary: Optional[SummaryResponse] = None
    findings: List[FindingResponse] = Field(default_factory=list)
