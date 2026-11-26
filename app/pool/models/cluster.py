from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from .compute_pool import ClusterResourceSummary
from .node import ApiModel, NodeRecord


class ClusterStatus(StrEnum):
    ONLINE = "ONLINE"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"
    UNKNOWN = "UNKNOWN"


class ClusterRecord(ApiModel):
    cluster_id: str = Field(min_length=1, max_length=100)
    cluster_name: str = Field(min_length=1, max_length=100)
    version: str = Field(default="0.1.0", min_length=1, max_length=100)
    status: ClusterStatus = ClusterStatus.UNKNOWN
    enterprise: str = Field(default="", max_length=200)
    project: str = Field(default="", max_length=200)
    management_endpoint: str | None = Field(default=None, max_length=2048)
    last_sync_time: datetime | None = None
    created_at: datetime
    updated_at: datetime

    @field_validator("cluster_id")
    @classmethod
    def validate_cluster_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("cluster_id cannot be empty")
        return normalized

    @field_validator("cluster_name", "version")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value cannot be blank")
        return normalized

    @field_validator("enterprise", "project")
    @classmethod
    def strip_optional_text(cls, value: str) -> str:
        return value.strip()

    @field_validator("management_endpoint")
    @classmethod
    def normalize_endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class ClusterCreateRequest(ApiModel):
    cluster_id: str | None = Field(default=None, min_length=1, max_length=100)
    cluster_name: str = Field(min_length=1, max_length=100)
    version: str = Field(default="0.1.0", min_length=1, max_length=100)
    enterprise: str = Field(default="", max_length=200)
    project: str = Field(default="", max_length=200)
    management_endpoint: str | None = Field(default=None, max_length=2048)
    node_ids: list[UUID] = Field(default_factory=list, max_length=500)

    @field_validator("cluster_id")
    @classmethod
    def validate_optional_cluster_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return ClusterRecord.validate_cluster_id(value)

    @field_validator("cluster_name", "version")
    @classmethod
    def strip_required_text(cls, value: str) -> str:
        return ClusterRecord.strip_required_text(value)

    @field_validator("enterprise", "project")
    @classmethod
    def strip_optional_text(cls, value: str) -> str:
        return value.strip()

    @field_validator("management_endpoint")
    @classmethod
    def normalize_endpoint(cls, value: str | None) -> str | None:
        return ClusterRecord.normalize_endpoint(value)

    @field_validator("node_ids")
    @classmethod
    def reject_duplicate_node_ids(cls, value: list[UUID]) -> list[UUID]:
        if len(value) != len(set(value)):
            raise ValueError("node_ids cannot contain duplicates")
        return value

    def resolved_cluster_id(self) -> str:
        return self.cluster_id or str(uuid4())


class ClusterUpdateRequest(ApiModel):
    cluster_name: str | None = Field(default=None, min_length=1, max_length=100)
    version: str | None = Field(default=None, min_length=1, max_length=100)
    enterprise: str | None = Field(default=None, max_length=200)
    project: str | None = Field(default=None, max_length=200)
    management_endpoint: str | None = Field(default=None, max_length=2048)
    last_sync_time: datetime | None = None

    @field_validator("cluster_name", "version")
    @classmethod
    def strip_required_text(cls, value: str | None) -> str | None:
        return ClusterRecord.strip_required_text(value) if value is not None else None

    @field_validator("enterprise", "project")
    @classmethod
    def strip_optional_text(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None

    @field_validator("management_endpoint")
    @classmethod
    def normalize_endpoint(cls, value: str | None) -> str | None:
        return ClusterRecord.normalize_endpoint(value)

    @model_validator(mode="after")
    def require_change(self) -> Self:
        if not self.model_fields_set:
            raise ValueError("at least one cluster field must be supplied")
        return self


class ClusterView(ClusterRecord):
    node_count: int = Field(ge=0)
    resource_summary: ClusterResourceSummary


class ClusterListResponse(ApiModel):
    items: list[ClusterView]
    total: int = Field(ge=0)
    updated_at: datetime


class ClusterNodeListResponse(ApiModel):
    items: list[NodeRecord]
    total: int = Field(ge=0)
    updated_at: datetime


ClusterEventCategory = Literal["lifecycle", "sync", "recovery"]


class ClusterEvent(ApiModel):
    id: UUID = Field(default_factory=uuid4)
    cluster_id: str
    category: ClusterEventCategory
    event_type: str
    target_id: str
    target_name: str
    result: Literal["success", "rejected", "observed"]
    occurred_at: datetime
    before: dict[str, str | None] = Field(default_factory=dict)
    after: dict[str, str | None] = Field(default_factory=dict)
    reason: str | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
