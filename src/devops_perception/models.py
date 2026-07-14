"""Immutable canonical domain models."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from collections.abc import Iterator
from typing import Any, Literal, Mapping

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    field_validator,
)


class FrozenMapping(Mapping[str, Any]):
    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = {key: _freeze(value) for key, value in values.items()}

    def __getitem__(self, key: str) -> Any:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __hash__(self) -> int:
        return hash(
            tuple(
                sorted(
                    (key, _hashable(value)) for key, value in self._values.items()
                )
            )
        )


def _hashable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((key, _hashable(item)) for key, item in value.items()))
    if isinstance(value, (tuple, frozenset)):
        return tuple(sorted((_hashable(item) for item in value), key=repr))
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return FrozenMapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze(item) for item in value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError(f"payload value is not JSON-compatible: {type(value).__name__}")


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    if isinstance(value, frozenset):
        return sorted((_thaw(item) for item in value), key=repr)
    return value


class ImmutableModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class EntityRef(ImmutableModel):
    type: str = Field(
        min_length=1, validation_alias=AliasChoices("type", "kind")
    )
    id: str = Field(min_length=1)
    name: str | None = None

    @property
    def kind(self) -> str:
        """Backward-compatible internal name; JSON always uses ``type``."""
        return self.type


EventType = Literal[
    "deployment.started",
    "deployment.completed",
    "metric.observed",
    "log.observed",
    "incident.created",
    "incident.closed",
    "rollback.completed",
    "service.recovered",
    "secret.detected",
    "security.gate.blocked",
    "build.blocked",
    "secret.remediated",
    "secret.rescan",
    "security.gate.passed",
    "build.completed",
]


class CanonicalEvent(ImmutableModel):
    id: str = Field(min_length=1)
    scenario_id: str = Field(min_length=1)
    occurred_at: datetime
    type: EventType
    source: str = Field(min_length=1)
    actor: EntityRef
    subject: EntityRef
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None
    trace_id: str = Field(min_length=1)
    schema_version: Literal[1] = 1
    payload: Mapping[str, Any] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def occurred_at_must_be_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(timezone.utc)

    @field_validator("payload", mode="after")
    @classmethod
    def freeze_payload(cls, value: Any) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError("payload must be an object")
        return _freeze(dict(value))

    @field_serializer("payload")
    def serialize_payload(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return _thaw(value)


class Scenario(ImmutableModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str
    events: tuple[CanonicalEvent, ...]

    @field_validator("events")
    @classmethod
    def validate_events(
        cls, events: tuple[CanonicalEvent, ...]
    ) -> tuple[CanonicalEvent, ...]:
        ids = [event.id for event in events]
        if len(ids) != len(set(ids)):
            raise ValueError("scenario event IDs must be unique")
        if (
            tuple(sorted(events, key=lambda event: (event.occurred_at, event.id)))
            != events
        ):
            raise ValueError("scenario events must be chronologically ordered")
        return events


class Insight(ImmutableModel):
    id: str = Field(min_length=1)
    scenario_id: str = Field(min_length=1)
    rule_id: str = Field(min_length=1)
    status: Literal["active", "resolved"]
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    severity: Literal["info", "low", "medium", "high", "critical"]
    occurred_at: datetime
    affected: EntityRef
    evidence_event_ids: tuple[str, ...] = ()

    @field_validator("occurred_at")
    @classmethod
    def normalize_occurred_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value.astimezone(timezone.utc)


class PlaybackSnapshot(ImmutableModel):
    scenario_id: str
    cursor: int = Field(ge=0)
    total_events: int = Field(ge=0)
    status: Literal["idle", "paused", "playing", "completed", "error"]
    speed: float = Field(gt=0)
    current_time: datetime | None = None
    operational_error: str | None = None

    @computed_field
    @property
    def progress(self) -> float:
        if self.total_events == 0:
            return 0.0
        return self.cursor / self.total_events
