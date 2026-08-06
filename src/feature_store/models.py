from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,62}$")
VERSION_PATTERN = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
FEATURE_REF_PATTERN = re.compile(
    r"^(?P<view>[a-z][a-z0-9_]{1,62})@(?P<version>\d+\.\d+\.\d+):(?P<feature>[a-z][a-z0-9_]{1,62})$"
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ValueType(StrEnum):
    STRING = "string"
    INT64 = "int64"
    FLOAT64 = "float64"
    BOOL = "bool"
    TIMESTAMP = "timestamp"


class Entity(StrictModel):
    name: str
    join_keys: dict[str, ValueType]

    @model_validator(mode="after")
    def validate_entity(self) -> Entity:
        if not NAME_PATTERN.fullmatch(self.name):
            raise ValueError("entity name must be lowercase snake_case")
        if not self.join_keys:
            raise ValueError("entity requires at least one join key")
        for key in self.join_keys:
            if not NAME_PATTERN.fullmatch(key):
                raise ValueError(f"invalid join key: {key}")
        return self


class BatchSource(StrictModel):
    name: str
    uri: str
    event_timestamp_field: str = "event_timestamp"

    @field_validator("name", "event_timestamp_field")
    @classmethod
    def valid_names(cls, value: str) -> str:
        if not NAME_PATTERN.fullmatch(value):
            raise ValueError("names must be lowercase snake_case")
        return value


class StreamSource(StrictModel):
    name: str
    topic: str
    dead_letter_topic: str | None = None

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not NAME_PATTERN.fullmatch(value):
            raise ValueError("name must be lowercase snake_case")
        return value


class Feature(StrictModel):
    name: str
    dtype: ValueType
    description: str = ""

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not NAME_PATTERN.fullmatch(value):
            raise ValueError("feature name must be lowercase snake_case")
        return value


class FeatureView(StrictModel):
    name: str
    version: str
    entity: str
    features: list[Feature]
    batch_source: str
    batch_sql: str
    stream_source: str | None = None
    ttl_seconds: int | None = Field(default=None, gt=0)
    description: str = ""

    @model_validator(mode="after")
    def validate_view(self) -> FeatureView:
        if not NAME_PATTERN.fullmatch(self.name):
            raise ValueError("feature view name must be lowercase snake_case")
        if not VERSION_PATTERN.fullmatch(self.version):
            raise ValueError("version must be semantic major.minor.patch")
        if not self.features:
            raise ValueError("feature view requires at least one feature")
        names = [feature.name for feature in self.features]
        if len(names) != len(set(names)):
            raise ValueError("feature names must be unique")
        if not self.batch_sql.strip().lower().startswith(("select", "with")):
            raise ValueError("batch_sql must be a SELECT query")
        return self

    @property
    def ref(self) -> str:
        return f"{self.name}@{self.version}"


class FeatureService(StrictModel):
    name: str
    features: list[str]
    description: str = ""

    @model_validator(mode="after")
    def validate_refs(self) -> FeatureService:
        if not NAME_PATTERN.fullmatch(self.name):
            raise ValueError("feature service name must be lowercase snake_case")
        if not self.features:
            raise ValueError("feature service requires at least one feature")
        invalid = [ref for ref in self.features if not FEATURE_REF_PATTERN.fullmatch(ref)]
        if invalid:
            raise ValueError(f"feature references must be view@x.y.z:feature: {invalid}")
        return self


class RegistryManifest(StrictModel):
    entities: list[Entity] = []
    batch_sources: list[BatchSource] = []
    stream_sources: list[StreamSource] = []
    feature_views: list[FeatureView] = []
    feature_services: list[FeatureService] = []

    def fingerprint(self) -> str:
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()


class Observation(StrictModel):
    entity_values: dict[str, Any]
    event_timestamp: datetime

    @model_validator(mode="after")
    def timezone_required(self) -> Observation:
        if self.event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must include a timezone")
        return self


class HistoricalQuery(StrictModel):
    observations: list[Observation]
    features: list[str] = []
    feature_service: str | None = None

    @model_validator(mode="after")
    def one_selector(self) -> HistoricalQuery:
        if bool(self.features) == bool(self.feature_service):
            raise ValueError("provide exactly one of features or feature_service")
        return self


class OnlineQuery(StrictModel):
    entities: list[dict[str, Any]]
    features: list[str] = []
    feature_service: str | None = None

    @model_validator(mode="after")
    def one_selector(self) -> OnlineQuery:
        if bool(self.features) == bool(self.feature_service):
            raise ValueError("provide exactly one of features or feature_service")
        return self


class JobKind(StrEnum):
    BACKFILL = "backfill"
    MATERIALIZE = "materialize"
    OFFLINE_APPEND = "offline_append"
    HISTORICAL_QUERY = "historical_query"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    EXHAUSTED = "exhausted"
    CANCELLED = "cancelled"


class JobFailureKind(StrEnum):
    RETRYABLE = "retryable"
    TERMINAL = "terminal"
    LEASE_EXPIRED = "lease_expired"


class JobRequest(StrictModel):
    feature_view: str
    start: datetime
    end: datetime

    @model_validator(mode="after")
    def valid_range(self) -> JobRequest:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("job timestamps must include a timezone")
        if self.start >= self.end:
            raise ValueError("start must be before end")
        return self


class IncrementalMaterializationRequest(StrictModel):
    feature_view: str
    end: datetime | None = None
    lookback_seconds: int | None = Field(default=None, ge=0)

    @field_validator("end")
    @classmethod
    def timezone_required(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("end must include a timezone")
        return value


class StreamFeatureEvent(StrictModel):
    event_id: str = Field(min_length=1, max_length=256)
    feature_view: str
    entity_values: dict[str, Any]
    event_timestamp: datetime
    values: dict[str, Any]

    @model_validator(mode="after")
    def timezone_required(self) -> StreamFeatureEvent:
        if self.event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must include a timezone")
        return self


class StreamEventState(StrEnum):
    PENDING = "pending"
    STAGED = "staged"
    APPLIED = "applied"


def validate_feature_value(dtype: ValueType, value: Any) -> None:
    """Validate a stream value without coercing producer data."""
    if value is None:
        return
    valid = False
    if dtype == ValueType.STRING:
        valid = isinstance(value, str)
    elif dtype == ValueType.INT64:
        valid = isinstance(value, int) and not isinstance(value, bool) and -(2**63) <= value < 2**63
    elif dtype == ValueType.FLOAT64:
        valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif dtype == ValueType.BOOL:
        valid = isinstance(value, bool)
    elif dtype == ValueType.TIMESTAMP:
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
            valid = parsed.tzinfo is not None
        except (TypeError, ValueError):
            valid = False
    if not valid:
        raise ValueError(f"value {value!r} does not match {dtype}")


class ApplyResult(StrictModel):
    fingerprint: str
    created: int
    unchanged: int


class RegistryObjectStatus(StrEnum):
    CREATED = "created"
    UNCHANGED = "unchanged"
    REJECTED = "rejected"


class RegistryObjectKind(StrEnum):
    ENTITY = "entity"
    BATCH_SOURCE = "batch_source"
    STREAM_SOURCE = "stream_source"
    FEATURE_VIEW = "feature_view"
    FEATURE_SERVICE = "feature_service"


class RegistryIssueCode(StrEnum):
    IMMUTABLE_CONFLICT = "immutable_conflict"
    MISSING_ENTITY = "missing_entity"
    MISSING_BATCH_SOURCE = "missing_batch_source"
    MISSING_STREAM_SOURCE = "missing_stream_source"
    MISSING_FEATURE_VIEW = "missing_feature_view"
    MISSING_FEATURE = "missing_feature"


class RegistryDiffOperation(StrEnum):
    ADDED = "added"
    REMOVED = "removed"
    CHANGED = "changed"


class RegistryObjectIdentity(StrictModel):
    kind: RegistryObjectKind
    name: str
    version: str | None = None


class RegistryIssue(StrictModel):
    code: RegistryIssueCode
    path: str
    message: str


class RegistryDifference(StrictModel):
    path: str
    operation: RegistryDiffOperation
    existing: Any = None
    proposed: Any = None


class RegistryObjectPlan(StrictModel):
    identity: RegistryObjectIdentity
    status: RegistryObjectStatus
    issues: list[RegistryIssue] = []
    differences: list[RegistryDifference] = []


class RegistryPlanSummary(StrictModel):
    created: int
    unchanged: int
    rejected: int


class RegistryPlan(StrictModel):
    fingerprint: str
    summary: RegistryPlanSummary
    objects: list[RegistryObjectPlan]


class QueryStatus(StrEnum):
    PRESENT = "present"
    MISSING = "missing"
    EXPIRED = "expired"


class QueryResponse(StrictModel):
    resolved_features: list[str]
    rows: list[dict[str, Any]]


class HistoricalResult(StrictModel):
    format: Literal["parquet"] = "parquet"
    content_type: str = "application/vnd.apache.parquet"
    row_count: int
    byte_size: int
    resolved_features: list[str]
    download_url: str
    expires_at: datetime
    cleaned_up: bool = False


class MaterializationSummary(StrictModel):
    mode: Literal["explicit", "incremental"]
    effective_start: datetime | None
    effective_end: datetime
    lookback_seconds: int
    scanned_rows: int
    candidate_entities: int
    updated_entities: int
    skipped_entities: int
    source_freshness_at: datetime | None
    freshness_lag_seconds: float | None
    resulting_watermark: datetime | None


class JobResponse(StrictModel):
    id: str
    kind: JobKind
    status: JobStatus
    payload: dict[str, Any]
    checkpoints: list[str]
    error: str | None
    failure_kind: JobFailureKind | None
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    worker_id: str | None
    lease_expires_at: datetime | None
    last_heartbeat_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    artifact_expires_at: datetime | None = None
    artifacts_cleaned_at: datetime | None = None
    result: HistoricalResult | MaterializationSummary | None = None


JsonObject = dict[str, Any]
FeatureSelector = Literal["features", "feature_service"]
