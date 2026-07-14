import asyncio
from pathlib import Path
from sqlite3 import ProgrammingError

import pytest

from devops_perception.engine import PerceptionEngine
from devops_perception.playback import PlaybackService
from devops_perception.scenarios import get_scenario
from devops_perception.store import SQLiteStore


@pytest.fixture
def playback(tmp_path: Path) -> PlaybackService:
    store = SQLiteStore(tmp_path / "playback.db")
    service = PlaybackService(store, PerceptionEngine(store))
    yield service
    service.close()


def test_load_step_reset_and_rebuild(playback: PlaybackService) -> None:
    scenario = get_scenario("healthy-delivery")
    loaded = playback.load(scenario)
    assert loaded.cursor == 0
    assert playback.step().cursor == 1
    rebuilt = playback.rebuild(2)
    assert rebuilt.cursor == 2
    assert len(playback.store.list_timeline(scenario.id)) == 2
    with pytest.raises(ValueError):
        playback.rebuild(len(scenario.events) + 1)
    assert playback.reset().cursor == 0


@pytest.mark.asyncio
async def test_async_play_pause_speed_and_listeners(playback: PlaybackService) -> None:
    scenario = get_scenario("performance-regression")
    playback.load(scenario)
    observed: list[str] = []

    async def listener(snapshot) -> None:
        observed.append(snapshot.status)

    playback.add_listener(listener)
    playback.set_speed(1)
    started = await playback.play()
    task = playback.runner_task
    assert started.status == "playing"
    assert task is not None
    await playback.play()
    assert playback.runner_task is task
    paused = await playback.pause()
    assert paused.status == "paused"
    assert task.done()
    assert "playing" in observed
    assert "paused" in observed

    playback.set_speed(1000)
    await playback.play()
    await playback.wait()
    assert playback.snapshot().status == "completed"
    assert playback.snapshot().cursor == len(scenario.events)
    assert "completed" in observed


def test_step_and_cursor_join_one_outer_transaction(playback: PlaybackService) -> None:
    scenario = get_scenario("healthy-delivery")
    playback.load(scenario)

    with pytest.raises(RuntimeError):
        with playback.store.transaction():
            playback.step()
            raise RuntimeError("rollback")

    assert playback.snapshot().cursor == 0
    assert playback.store.list_timeline(scenario.id) == ()


@pytest.mark.asyncio
async def test_load_invalidates_stale_runner(playback: PlaybackService) -> None:
    playback.load(get_scenario("performance-regression"))
    playback.set_speed(1)
    await playback.play()
    old_task = playback.runner_task

    loaded = playback.load(get_scenario("security-gate"))
    await asyncio.sleep(0)

    assert old_task is not None and old_task.done()
    assert loaded.scenario_id == "security-gate"
    assert playback.snapshot().scenario_id == "security-gate"
    assert playback.snapshot().cursor == 0


@pytest.mark.asyncio
async def test_background_operational_errors_are_recorded(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))
    observed_errors: list[str | None] = []

    def broken_listener(snapshot) -> None:
        if snapshot.status == "playing":
            raise RuntimeError("listener failed")

    playback.add_listener(
        lambda snapshot: observed_errors.append(snapshot.operational_error)
    )
    playback.add_listener(broken_listener)
    await playback.play()
    await playback.wait()

    assert playback.operational_errors
    assert "listener failed" in playback.operational_errors[-1]
    persisted = playback.store.get_playback("healthy-delivery")
    assert persisted is not None
    assert "listener failed" in persisted["operational_error"]
    assert playback.snapshot().status == "error"
    assert any(error and "listener failed" in error for error in observed_errors)


@pytest.mark.asyncio
async def test_async_startup_listener_failure_survives_runner_cancellation(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))

    async def broken_async_listener(snapshot) -> None:
        if snapshot.status == "playing":
            await asyncio.sleep(0)
            raise RuntimeError("async startup failed")

    playback.add_listener(broken_async_listener)
    returned = await playback.play()
    persisted = playback.store.get_playback("healthy-delivery")

    assert returned.status == "error"
    assert returned.operational_error == "RuntimeError: async startup failed"
    assert persisted is not None
    assert persisted["status"] == "error"
    assert persisted["operational_error"] == "RuntimeError: async startup failed"
    assert playback.runner_task is not None and playback.runner_task.done()


@pytest.mark.asyncio
async def test_concurrent_play_calls_create_exactly_one_runner(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))
    entered = 0
    release = asyncio.Event()

    async def blocking_listener(snapshot) -> None:
        nonlocal entered
        if snapshot.status == "playing":
            entered += 1
            await release.wait()

    playback.add_listener(blocking_listener)
    first = asyncio.create_task(playback.play())
    second = asyncio.create_task(playback.play())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    entries_before_release = entered
    runner_during_listener = playback.runner_task
    release.set()
    await asyncio.gather(first, second)
    await playback.pause()

    assert entries_before_release == 1
    assert runner_during_listener is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("invalidation", ["load", "reset"])
async def test_invalidation_awaits_old_runner_before_immediate_restart(
    playback: PlaybackService, invalidation: str
) -> None:
    playback.load(get_scenario("performance-regression"))
    await playback.play()
    old_runner = playback.runner_task
    assert old_runner is not None

    if invalidation == "load":
        playback.load(get_scenario("security-gate"))
    else:
        playback.reset()
    restarted = await playback.play()

    assert old_runner.done()
    assert restarted.status == "playing"
    assert playback.runner_task is not old_runner
    await playback.pause()


@pytest.mark.asyncio
@pytest.mark.parametrize("interleaving", ["load", "reset", "pause"])
async def test_play_returns_authoritative_snapshot_when_lifecycle_interleaves(
    playback: PlaybackService, interleaving: str
) -> None:
    playback.load(get_scenario("performance-regression"))
    listener_entered = asyncio.Event()
    release_listener = asyncio.Event()

    async def blocking_listener(snapshot) -> None:
        if snapshot.status == "playing":
            listener_entered.set()
            await release_listener.wait()

    playback.add_listener(blocking_listener)
    play_call = asyncio.create_task(playback.play())
    await listener_entered.wait()

    if interleaving == "load":
        playback.load(get_scenario("security-gate"))
    elif interleaving == "reset":
        playback.reset()
    else:
        await playback.pause()
    release_listener.set()
    returned = await play_call
    authoritative = playback.snapshot()

    assert returned == authoritative
    assert returned.status == "paused"
    assert playback.runner_task is not None
    assert playback.runner_task.done()


@pytest.mark.asyncio
async def test_deferred_listener_error_stays_bound_to_original_scenario(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("performance-regression"))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def stale_listener(snapshot) -> None:
        if snapshot.scenario_id == "performance-regression":
            entered.set()
            await release.wait()
            raise RuntimeError("stale performance notification")

    playback.add_listener(stale_listener)
    playback.set_speed(2)
    await entered.wait()
    playback.load(get_scenario("security-gate"))
    release.set()
    await playback.wait_for_notifications()

    old_state = playback.store.get_playback("performance-regression")
    new_state = playback.store.get_playback("security-gate")
    assert old_state is not None
    assert old_state["status"] == "error"
    assert "stale performance notification" in old_state["operational_error"]
    assert new_state is not None
    assert new_state["status"] == "paused"
    assert new_state["operational_error"] is None


@pytest.mark.asyncio
async def test_async_close_cancels_and_awaits_deferred_listener_tasks(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "close.db")
    service = PlaybackService(store, PerceptionEngine(store))
    service.load(get_scenario("healthy-delivery"))
    entered = asyncio.Event()

    async def forever_listener(_snapshot) -> None:
        entered.set()
        await asyncio.Event().wait()

    service.add_listener(forever_listener)
    service.set_speed(2)
    await entered.wait()
    tasks = service.deferred_listener_tasks

    await service.aclose()

    assert tasks
    assert all(task.done() for task in tasks)


@pytest.mark.asyncio
async def test_step_async_stops_runner_before_processing(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))
    playback.set_speed(1)
    await playback.play()
    old_runner = playback.runner_task

    stepped = await playback.step_async()
    await asyncio.sleep(0)

    assert old_runner is not None and old_runner.done()
    assert stepped.cursor == 1
    assert playback.snapshot().cursor == 1


@pytest.mark.asyncio
async def test_delayed_same_scenario_error_preserves_authoritative_progress(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_failure(snapshot) -> None:
        if snapshot.status == "paused" and snapshot.cursor == 0:
            entered.set()
            await release.wait()
            raise RuntimeError("delayed failure")

    remove = playback.add_listener(delayed_failure)
    playback.set_speed(2)
    await entered.wait()
    remove()
    advanced = playback.step()
    release.set()
    await playback.wait_for_notifications()
    persisted = playback.store.get_playback("healthy-delivery")

    assert advanced.cursor == 1
    assert persisted is not None
    assert persisted["status"] == "error"
    assert persisted["cursor"] == 1
    assert persisted["speed"] == 2
    assert persisted["current_time"] == advanced.current_time.isoformat()
    assert persisted["operational_error"] == "RuntimeError: delayed failure"
    assert len(playback.store.list_timeline("healthy-delivery")) == 1


@pytest.mark.asyncio
async def test_aclose_cancels_permanently_blocked_direct_listener(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "blocked-close.db")
    service = PlaybackService(store, PerceptionEngine(store))
    service.load(get_scenario("healthy-delivery"))
    entered = asyncio.Event()
    cleanup_release = asyncio.Event()

    async def permanently_blocked(snapshot) -> None:
        if snapshot.status == "playing":
            entered.set()
            await cleanup_release.wait()

    service.add_listener(permanently_blocked)
    play_call = asyncio.create_task(service.play())
    await entered.wait()
    close_call = asyncio.create_task(service.aclose())
    try:
        await asyncio.wait_for(asyncio.shield(close_call), timeout=0.1)
        closed_without_release = True
    except TimeoutError:
        closed_without_release = False
    finally:
        cleanup_release.set()
        await asyncio.gather(play_call, close_call, return_exceptions=True)

    assert closed_without_release
    assert play_call.done()


@pytest.mark.asyncio
async def test_queued_play_rechecks_shutdown_after_acquiring_lock(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "queued-close.db")
    service = PlaybackService(store, PerceptionEngine(store))
    service.load(get_scenario("healthy-delivery"))
    entered = asyncio.Event()
    cleanup_release = asyncio.Event()

    async def blocking_start(snapshot) -> None:
        if snapshot.status == "playing":
            entered.set()
            await cleanup_release.wait()

    service.add_listener(blocking_start)
    first_play = asyncio.create_task(service.play())
    await entered.wait()
    queued_play = asyncio.create_task(service.play())
    await asyncio.sleep(0)
    close_call = asyncio.create_task(service.aclose())
    try:
        await asyncio.wait_for(asyncio.shield(close_call), timeout=0.1)
        closed_without_release = True
    except TimeoutError:
        closed_without_release = False
    finally:
        cleanup_release.set()
        first_result, queued_result, _ = await asyncio.gather(
            first_play, queued_play, close_call, return_exceptions=True
        )

    assert closed_without_release
    assert isinstance(queued_result, RuntimeError)
    assert "closing or closed" in str(queued_result)
    assert not isinstance(queued_result, ProgrammingError)


@pytest.mark.asyncio
async def test_runner_listener_can_pause_without_self_await(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))
    playback.set_speed(1000)
    callback_results = []

    async def pause_from_listener(snapshot) -> None:
        if snapshot.status == "playing" and snapshot.cursor == 1:
            callback_results.append(await playback.pause())

    playback.add_listener(pause_from_listener)
    await playback.play()
    await playback.wait()

    assert callback_results
    assert callback_results[0].status == "paused"
    assert playback.snapshot().status == "paused"
    assert playback.operational_errors == []


@pytest.mark.asyncio
async def test_runner_listener_can_wait_without_self_await(
    playback: PlaybackService,
) -> None:
    playback.load(get_scenario("healthy-delivery"))
    playback.set_speed(1000)
    callback_results = []

    async def wait_from_listener(snapshot) -> None:
        if snapshot.status == "playing" and snapshot.cursor == 1:
            callback_results.append(await playback.wait())

    playback.add_listener(wait_from_listener)
    await playback.play()
    await playback.wait()

    assert callback_results
    assert playback.snapshot().status == "completed"
    assert playback.operational_errors == []


@pytest.mark.asyncio
async def test_runner_listener_can_aclose_without_self_cancel(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "listener-close.db")
    service = PlaybackService(store, PerceptionEngine(store))
    service.load(get_scenario("healthy-delivery"))
    service.set_speed(1000)
    callback_completed = asyncio.Event()

    async def close_from_listener(snapshot) -> None:
        if snapshot.status == "playing" and snapshot.cursor == 1:
            await service.aclose()
            callback_completed.set()

    service.add_listener(close_from_listener)
    await service.play()
    await callback_completed.wait()
    await service.wait_closed()

    assert service.runner_task is not None and service.runner_task.done()
    assert service.operational_errors == []
