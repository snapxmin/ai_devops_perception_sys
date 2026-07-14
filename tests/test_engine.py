from pathlib import Path

import pytest

from devops_perception.engine import PerceptionEngine
from devops_perception.models import CanonicalEvent
from devops_perception.scenarios import get_scenario
from devops_perception.store import SQLiteStore


@pytest.fixture
def engine_store(tmp_path: Path) -> tuple[PerceptionEngine, SQLiteStore]:
    store = SQLiteStore(tmp_path / "engine.db")
    yield PerceptionEngine(store), store
    store.close()


def test_engine_maps_every_event_and_is_idempotent(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    scenario = get_scenario("performance-regression")
    store.load_scenario(scenario)

    for event in scenario.events:
        engine.process(event)
        engine.process(event)

    snapshot = store.snapshot(scenario.id)
    assert len(snapshot["timeline"]) == len(scenario.events)
    assert len(snapshot["processed_events"]) == len(scenario.events)
    assert {event["type"] for event in snapshot["timeline"]} == {
        event.type for event in scenario.events
    }
    assert snapshot["nodes"]
    assert snapshot["edges"]
    assert snapshot["state"]


def test_engine_emits_explainable_regression_and_recovery_insights(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    scenario = get_scenario("performance-regression")
    for event in scenario.events:
        engine.process(event)

    insights = store.list_insights(scenario.id)
    rules = {insight.rule_id for insight in insights}
    assert {
        "high-latency",
        "high-error-rate",
        "deployment-correlated-regression",
        "incident-evidence",
    } <= rules
    assert all(insight.evidence_event_ids for insight in insights)


def test_engine_emits_secret_gate_insight(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    finding = get_scenario("security-gate").events[0]
    engine.process(finding)
    assert "secret-gate" in {
        insight.rule_id for insight in store.list_insights("security-gate")
    }


def test_scenario_graphs_remain_isolated_when_entities_overlap(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    healthy = get_scenario("healthy-delivery")
    regression = get_scenario("performance-regression")
    for event in healthy.events:
        engine.process(event)
    healthy_nodes = store.snapshot(healthy.id)["nodes"]

    for event in regression.events:
        engine.process(event)

    assert store.snapshot(healthy.id)["nodes"] == healthy_nodes
    assert store.snapshot(regression.id)["nodes"]


def test_incident_evidence_links_deployment_metrics_log_and_incident(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    events = get_scenario("performance-regression").events
    incident_index = next(
        index for index, event in enumerate(events) if event.type == "incident.created"
    )
    for event in events[: incident_index + 1]:
        engine.process(event)

    insight = next(
        item
        for item in store.list_insights("performance-regression")
        if item.rule_id == "incident-evidence"
    )
    evidence_types = {
        event.type for event in events if event.id in insight.evidence_event_ids
    }
    assert evidence_types == {
        "deployment.completed",
        "metric.observed",
        "log.observed",
        "incident.created",
    }
    evidence_edges = [
        edge
        for edge in store.snapshot("performance-regression")["edges"]
        if edge["relation"] == "evidence_for"
    ]
    assert len(evidence_edges) == 5


def test_recovery_requires_healthy_metrics_then_closes_incident(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    events = get_scenario("performance-regression").events
    for event in events[:-1]:
        engine.process(event)
    assert "rollback-recovery" not in {
        item.rule_id for item in store.list_insights("performance-regression")
    }
    assert (
        store.get_state(
            "performance-regression", "service:payment-service", "incident_status"
        )
        == "open"
    )

    engine.process(events[-1])

    recovery = next(
        item
        for item in store.list_insights("performance-regression")
        if item.rule_id == "rollback-recovery"
    )
    assert {events[-3].id, events[-2].id, events[-1].id} <= set(
        recovery.evidence_event_ids
    )
    assert (
        store.get_state(
            "performance-regression", "service:payment-service", "incident_status"
        )
        == "closed"
    )


def test_engine_projections_join_shared_outer_transaction(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    event = get_scenario("healthy-delivery").events[0]

    with pytest.raises(RuntimeError):
        with store.transaction():
            engine.process(event)
            raise RuntimeError("rollback all projections")

    assert store.list_timeline(event.scenario_id) == ()
    assert not store.is_processed(event.id)


def test_deployment_and_rollback_project_current_service_version(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    events = get_scenario("performance-regression").events
    deployment = events[0]
    rollback = next(event for event in events if event.type == "rollback.completed")

    engine.process(deployment)
    assert (
        store.get_state(
            deployment.scenario_id, "service:payment-service", "current_version"
        )
        == "v2.4"
    )

    for event in events[1 : events.index(rollback) + 1]:
        engine.process(event)
    assert (
        store.get_state(
            rollback.scenario_id, "service:payment-service", "current_version"
        )
        == "v2.3"
    )


def test_recovery_rejects_stale_healthy_flag_after_new_regression(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    events = get_scenario("performance-regression").events
    for event in events[:-1]:
        engine.process(event)
    template = events[-2].model_dump(mode="python")
    template.update(
        id="performance-regression-new-latency",
        occurred_at="2026-01-15T10:07:30Z",
        type="metric.observed",
        causation_id=events[-2].id,
        payload={"metric": "latency_ms", "value": 900},
    )
    engine.process(CanonicalEvent(**template))
    engine.process(events[-1])

    assert "rollback-recovery" not in {
        insight.rule_id for insight in store.list_insights(events[-1].scenario_id)
    }
    assert (
        store.get_state(
            events[-1].scenario_id, "service:payment-service", "incident_status"
        )
        == "open"
    )


def test_recovery_requires_healthy_values_in_recovery_event_payload(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    events = get_scenario("performance-regression").events
    for event in events[:-1]:
        engine.process(event)
    values = events[-1].model_dump(mode="python")
    values.update(
        id="performance-regression-unhealthy-recovery",
        payload={"latency_ms": 700, "error_rate": 0.01},
    )
    engine.process(CanonicalEvent(**values))

    assert "rollback-recovery" not in {
        insight.rule_id for insight in store.list_insights(events[-1].scenario_id)
    }


@pytest.mark.parametrize(
    ("metric", "value"),
    [
        ("latency_ms", True),
        ("latency_ms", -1),
        ("error_rate", True),
        ("error_rate", -0.01),
        ("error_rate", 1.01),
    ],
)
def test_invalid_metric_values_are_rejected_before_projection(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
    metric: str,
    value: object,
) -> None:
    engine, store = engine_store
    template = get_scenario("performance-regression").events[1].model_dump(
        mode="python"
    )
    template.update(
        id=f"invalid-{metric}-{value}",
        payload={"metric": metric, "value": value},
    )
    event = CanonicalEvent(**template)

    with pytest.raises(ValueError):
        engine.process(event)

    assert store.list_timeline(event.scenario_id) == ()
    assert store.snapshot(event.scenario_id)["state"] == ()


def test_error_rate_uses_decimal_fraction_semantics(
    engine_store: tuple[PerceptionEngine, SQLiteStore],
) -> None:
    engine, store = engine_store
    template = get_scenario("performance-regression").events[2].model_dump(
        mode="python"
    )
    template.update(id="decimal-error-rate", payload={"metric": "error_rate", "value": 0.06})

    engine.process(CanonicalEvent(**template))

    insight = next(
        item
        for item in store.list_insights("performance-regression")
        if item.rule_id == "high-error-rate"
    )
    assert "6.0%" in insight.summary
