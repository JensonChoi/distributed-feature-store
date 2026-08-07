from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa

from feature_store.models import Feature, QualityConstraintType, StrictModel


class QualityViolation(StrictModel):
    row_index: int
    feature: str
    constraint: QualityConstraintType


class QualityValidation(StrictModel):
    evaluated_rows: int
    violations: list[QualityViolation]

    @property
    def invalid_row_indexes(self) -> list[int]:
        return sorted({violation.row_index for violation in self.violations})

    @property
    def valid_row_indexes(self) -> list[int]:
        invalid = set(self.invalid_row_indexes)
        return [index for index in range(self.evaluated_rows) if index not in invalid]

    @property
    def counts_by_constraint(self) -> dict[QualityConstraintType, int]:
        counts = Counter(violation.constraint for violation in self.violations)
        return {
            constraint: counts[constraint]
            for constraint in QualityConstraintType
            if counts[constraint]
        }

    def bounded_message(self) -> str:
        constraints = ",".join(item.value for item in self.counts_by_constraint)
        return (
            f"data quality violations: rows={len(self.invalid_row_indexes)} "
            f"constraints={constraints}"
        )


_CONSTRAINT_ORDER = {constraint: index for index, constraint in enumerate(QualityConstraintType)}


def _value_key(value: Any) -> str:
    if isinstance(value, datetime):
        value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return f"datetime:{value.isoformat(timespec='microseconds')}"
    return f"{type(value).__name__}:{json.dumps(value, sort_keys=True, default=str)}"


def validate_quality_rows(
    rows: list[dict[str, Any]],
    features: list[Feature],
    *,
    event_timestamps: list[datetime],
    reference_time: datetime,
    check_unique: bool = True,
) -> QualityValidation:
    """Evaluate semantic contracts without including producer values in diagnostics."""
    reference = (
        reference_time.replace(tzinfo=UTC)
        if reference_time.tzinfo is None
        else reference_time.astimezone(UTC)
    )
    violations: list[QualityViolation] = []
    duplicate_rows: dict[str, set[int]] = {}
    if check_unique:
        for feature in features:
            if feature.quality is None or not feature.quality.unique:
                continue
            indexes_by_value: dict[str, list[int]] = {}
            for index, row in enumerate(rows):
                value = row.get(feature.name)
                if value is not None:
                    indexes_by_value.setdefault(_value_key(value), []).append(index)
            duplicate_rows[feature.name] = {
                index
                for indexes in indexes_by_value.values()
                if len(indexes) > 1
                for index in indexes
            }

    for row_index, (row, event_timestamp) in enumerate(zip(rows, event_timestamps, strict=True)):
        for feature in features:
            quality = feature.quality
            if quality is None:
                continue
            value = row.get(feature.name)
            if value is None:
                if not quality.nullable:
                    violations.append(
                        QualityViolation(
                            row_index=row_index,
                            feature=feature.name,
                            constraint=QualityConstraintType.NULLABLE,
                        )
                    )
            else:
                if quality.minimum is not None and value < quality.minimum:
                    violations.append(
                        QualityViolation(
                            row_index=row_index,
                            feature=feature.name,
                            constraint=QualityConstraintType.MINIMUM,
                        )
                    )
                if quality.maximum is not None and value > quality.maximum:
                    violations.append(
                        QualityViolation(
                            row_index=row_index,
                            feature=feature.name,
                            constraint=QualityConstraintType.MAXIMUM,
                        )
                    )
                if quality.accepted_values is not None and not any(
                    value == accepted for accepted in quality.accepted_values
                ):
                    violations.append(
                        QualityViolation(
                            row_index=row_index,
                            feature=feature.name,
                            constraint=QualityConstraintType.ACCEPTED_VALUES,
                        )
                    )
                if row_index in duplicate_rows.get(feature.name, set()):
                    violations.append(
                        QualityViolation(
                            row_index=row_index,
                            feature=feature.name,
                            constraint=QualityConstraintType.UNIQUE,
                        )
                    )
            timestamp = (
                event_timestamp.replace(tzinfo=UTC)
                if event_timestamp.tzinfo is None
                else event_timestamp.astimezone(UTC)
            )
            if (
                quality.max_age_seconds is not None
                and timestamp < reference - timedelta(seconds=quality.max_age_seconds)
            ):
                violations.append(
                    QualityViolation(
                        row_index=row_index,
                        feature=feature.name,
                        constraint=QualityConstraintType.MAX_AGE_SECONDS,
                    )
                )
    feature_order = {feature.name: index for index, feature in enumerate(features)}
    violations.sort(
        key=lambda item: (
            item.row_index,
            feature_order[item.feature],
            _CONSTRAINT_ORDER[item.constraint],
        )
    )
    return QualityValidation(evaluated_rows=len(rows), violations=violations)


def validate_quality_table(
    table: pa.Table, features: list[Feature], *, reference_time: datetime
) -> QualityValidation:
    rows = table.to_pylist()
    return validate_quality_rows(
        rows,
        features,
        event_timestamps=[row["event_timestamp"] for row in rows],
        reference_time=reference_time,
    )


def validate_quality_event(
    values: dict[str, Any],
    features: list[Feature],
    *,
    event_timestamp: datetime,
    ingestion_time: datetime,
) -> QualityValidation:
    return validate_quality_rows(
        [values],
        features,
        event_timestamps=[event_timestamp],
        reference_time=ingestion_time,
        check_unique=False,
    )
