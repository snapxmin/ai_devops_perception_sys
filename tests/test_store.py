from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from devops_perception.engine import PerceptionEngine
from devops_perception.models import CanonicalEvent
from devops_perception.models import Scenario
from devops_perception.scenarios import get_scenario
from devops_perception.store import SQLiteStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    database = SQLiteStore(tmp_path / "perception.db")
    yield database
    database.close()


def test_store_persists_events_and_explicit_transaction_rolls_back(
    store: SQLiteStore,
) -> None:
    scenario = get_scenario("healthy-delivery")
    store.load_scenario(scenario)
    assert store.list_scenario_events(scenario.id) == scenario.events

    event = CanonicalEvent(
        id="transient",
        scenario_id=scenario.id,
        type="metric.observed",
        occurred_at="2026-07-14T02:00:00Z",
        source="test",
        actor={"kind": "user", "id": "tester"},
        subject={"kind": "service", "id": "payment-service"},
        correlation_id="corr-test",
        trace_id="trace-test",
    )
    with pytest.raises(RuntimeError):
        with store.transaction():
            store.append_timeline(event)
            raise RuntimeError("rollback")
    assert all(item.id != event.id for item in store.list_timeline(scenario.id))


def test_store_snapshot_reset_and_bounded_impact(store: SQLiteStore) -> None:
    scenario = get_scenario("healthy-delivery")
    store.load_scenario(scenario)
    event = scenario.events[0]
    with store.transaction():
        store.append_timeline(event)
        store.mark_processed(event.id)
        store.upsert_node("service:api", "service", {"status": "healthy"})
        store.upsert_edge("deployment:v1", "service:api", "deploys", event.id)
        store.set_state("service:api", "status", "healthy", event.id)
        store.save_playback(scenario.id, 1, "paused", 1.0)

    snapshot = store.snapshot(scenario.id)
    assert snapshot["timeline"][0]["id"] == event.id
    assert store.is_processed(event.id)
    store.reset(scenario.id)
    assert store.list_timeline(scenario.id) == ()
    assert store.list_scenario_events(scenario.id) == scenario.events


def test_nested_transactions_use_savepoints(store: SQLiteStore) -> None:
    scenario = get_scenario("healthy-delivery")
    store.load_scenario(scenario)
    first, second = scenario.events[:2]

    with store.transaction():
        store.append_timeline(first)
        with pytest.raises(RuntimeError):
            with store.transaction():
                store.append_timeline(second)
                raise RuntimeError("inner")
        store.mark_processed(first.id)

    assert store.list_timeline(scenario.id) == (first,)
    assert store.is_processed(first.id)


def test_loading_changed_same_length_scenario_replaces_content(
    store: SQLiteStore,
) -> None:
    original = get_scenario("healthy-delivery")
    store.load_scenario(original)
    changed_event = original.events[0].model_copy(
        update={"payload": {"version": "replacement"}}
    )
    replacement = Scenario(
        id=original.id,
        name=original.name,
        description=original.description,
        events=(changed_event, *original.events[1:]),
    )

    store.load_scenario(replacement)

    assert store.list_scenario_events(original.id) == replacement.events


def test_bounded_impact_traverses_incoming_and_outgoing_edges(
    store: SQLiteStore,
) -> None:
    scenario = get_scenario("healthy-delivery")
    store.load_scenario(scenario)
    event = scenario.events[0]
    with store.transaction():
        store.append_timeline(event)
        for node in ("a", "b", "c", "d"):
            store.upsert_node(node, "service", {}, event.id, scenario.id)
        store.upsert_edge("a", "b", "calls", event.id)
        store.upsert_edge("b", "c", "calls", event.id)
        store.upsert_edge("d", "a", "depends_on", event.id)

    impact = store.bounded_impact(scenario.id, "a", 2)

    assert impact["nodes"] == ("a", "b", "c", "d")
    assert impact["edges"] == (
        ("a", "b", "calls"),
        ("b", "c", "calls"),
        ("d", "a", "depends_on"),
    )
    assert ("a", "b", "c") in impact["paths"]
    assert ("a", "d") in impact["paths"]


def test_bounded_impact_never_crosses_scenario_boundary(store: SQLiteStore) -> None:
    healthy = get_scenario("healthy-delivery")
    security = get_scenario("security-gate")
    store.load_scenario(healthy)
    store.load_scenario(security)
    with store.transaction():
        store.append_timeline(healthy.events[0])
        store.upsert_edge("shared", "healthy-only", "calls", healthy.events[0].id)
        store.append_timeline(security.events[0])
        store.upsert_edge("shared", "security-only", "calls", security.events[0].id)

    impact = store.bounded_impact(healthy.id, "shared", 1)

    assert impact["nodes"] == ("healthy-only", "shared")
    assert impact["edges"] == (("shared", "healthy-only", "calls"),)


def test_store_serializes_shared_connection_across_threads(
    store: SQLiteStore,
) -> None:
    scenario = get_scenario("healthy-delivery")
    store.load_scenario(scenario)
    engine = PerceptionEngine(store)
    event = scenario.events[0]

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(engine.process, [event] * 8))

    assert results.count(True) == 1
    assert store.list_timeline(scenario.id) == (event,)
