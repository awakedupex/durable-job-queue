from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    job_type: str = Field(..., min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = Field(default=None, max_length=255)
    max_attempts: int = Field(default=5, ge=1, le=20)
    run_after: Optional[datetime] = None
    priority: int = Field(default=0, ge=-100, le=100)


class JobRead(BaseModel):
    id: UUID
    idempotency_key: Optional[str] = None
    job_type: str
    payload: dict[str, Any]
    status: str
    priority: int
    attempts: int
    max_attempts: int
    leased_by: Optional[str] = None
    leased_until: Optional[datetime] = None
    run_after: datetime
    last_error: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class JobList(BaseModel):
    jobs: list[JobRead]
    total: int
