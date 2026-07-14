from pathlib import Path

import pytest

from devops_perception.engine import PerceptionEngine
from devops_perception.scenarios import list_scenarios
from devops_perception.store import SQLiteStore


@pytest.fixture
def replay_store(tmp_path: Path):
    store = SQLiteStore(tmp_path / "replay.db")
    yield store
    store.close()


def test_rebuilding_all_scenarios_produces_identical_snapshots(
    replay_store: SQLiteStore,
) -> None:
    engine = PerceptionEngine(replay_store)
    for scenario in list_scenarios():
        replay_store.load_scenario(scenario)

        first = engine.rebuild(scenario.id)
        second = engine.rebuild(scenario.id)

        assert first == second
