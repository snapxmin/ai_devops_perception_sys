from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from devops_perception.models import (
    CanonicalEvent,
    EntityRef,
    Insight,
    PlaybackSnapshot,
    Scenario,
)


def event_values() -> dict:
    return {
        "id": "evt-1",
        "scenario_id": "healthy-delivery",
        "occurred_at": "2026-07-14T03:00:00+02:00",
        "type": "deployment.completed",
        "source": "deploy",
        "actor": {"kind": "user", "id": "release-bot"},
        "subject": {"kind": "deployment", "id": "payment-v2.4"},
        "correlation_id": "corr-1",
        "causation_id": None,
        "trace_id": "trace-1",
        "schema_version": 1,
        "payload": {"version": "v2.4"},
    }


def test_canonical_event_has_exact_immutable_utc_contract() -> None:
    event = CanonicalEvent(**event_values())

    assert event.occurred_at == datetime(2026, 7, 14, 1, tzinfo=timezone.utc)
    assert event.schema_version == 1
    assert event.actor.id == "release-bot"
    assert event.subject.id == "payment-v2.4"
    with pytest.raises(ValidationError):
        event.id = "other"  # type: ignore[misc]
    with pytest.raises(TypeError):
        event.payload["version"] = "other"  # type: ignore[index]


def test_entity_ref_accepts_and_emits_json_type_field() -> None:
    entity = EntityRef(type="service", id="payment-service", name="Payment")

    assert entity.type == "service"
    assert entity.model_dump() == {
        "type": "service",
        "id": "payment-service",
        "name": "Payment",
    }
    assert '"type":"service"' in entity.model_dump_json()


def test_payload_is_deeply_immutable_through_tuples_and_sets() -> None:
    class HashableDict(dict):
        __hash__ = object.__hash__

    tuple_child = {"items": [{"value": 1}]}
    set_child = HashableDict({"items": [2]})
    values = event_values()
    values["payload"] = {
        "tuple": (tuple_child,),
        "set": {set_child},
    }

    event = CanonicalEvent(**values)
    tuple_child["items"][0]["value"] = 9
    set_child["items"].append(3)

    assert event.payload["tuple"][0]["items"][0]["value"] == 1
    frozen_set_child = next(iter(event.payload["set"]))
    assert frozen_set_child["items"] == (2,)
    with pytest.raises(TypeError):
        frozen_set_child["other"] = True
    assert '"value":1' in event.model_dump_json()


@pytest.mark.parametrize(
    "changes",
    [
        {"occurred_at": "2026-07-14T01:00:00"},
        {"schema_version": 2},
        {"type": "unsupported.event"},
        {"extra_field": "forbidden"},
    ],
)
def test_canonical_event_rejects_invalid_contract(changes: dict) -> None:
    values = event_values()
    values.update(changes)
    with pytest.raises(ValidationError):
        CanonicalEvent(**values)


def test_insight_contract_and_composite_models_are_immutable() -> None:
    event = CanonicalEvent(**event_values())
    scenario = Scenario(
        id="healthy-delivery", name="Healthy delivery", description="Normal", events=[event]
    )
    insight = Insight(
        id="insight-1",
        scenario_id=scenario.id,
        rule_id="deployment-correlated-regression",
        status="active",
        title="Regression",
        summary="Latency increased after deploy",
        severity="high",
        occurred_at=event.occurred_at,
        affected=event.subject,
        evidence_event_ids=[event.id],
    )
    snapshot = PlaybackSnapshot(
        scenario_id=scenario.id,
        cursor=1,
        total_events=1,
        status="paused",
        speed=2.0,
        current_time=event.occurred_at,
    )

    assert scenario.events == (event,)
    assert insight.rule_id == "deployment-correlated-regression"
    assert insight.status == "active"
    assert insight.affected == event.subject
    assert insight.evidence_event_ids == ("evt-1",)
    assert snapshot.progress == 1.0


def test_payload_deep_freezes_nested_json_values() -> None:
    values = event_values()
    values["payload"] = {
        "items": ({"nested": ["a", {"value": 1}]},),
    }

    event = CanonicalEvent(**values)

    with pytest.raises(TypeError):
        event.payload["items"][0]["nested"][1]["value"] = 2
    assert event.model_dump(mode="json")["payload"] == {
        "items": [{"nested": ["a", {"value": 1}]}]
    }


def test_payload_rejects_unsupported_mutable_leaf_values() -> None:
    values = event_values()
    values["payload"] = {"buffer": bytearray(b"mutable")}

    with pytest.raises(ValidationError):
        CanonicalEvent(**values)
