"""Atomic stepping and single-runner asynchronous playback."""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import Awaitable, Callable
from contextlib import suppress

from .engine import PerceptionEngine
from .models import PlaybackSnapshot, Scenario
from .scenarios import get_scenario
from .store import SQLiteStore

Listener = Callable[[PlaybackSnapshot], Awaitable[None] | None]


class PlaybackService:
    def __init__(
        self,
        store: SQLiteStore,
        engine: PerceptionEngine,
        *,
        interval_seconds: float = 1.0,
    ) -> None:
        self.store = store
        self.engine = engine
        self.interval_seconds = interval_seconds
        self._scenario_id: str | None = None
        self._listeners: list[Listener] = []
        self._runner_task: asyncio.Task[None] | None = None
        self._runner_generation: int | None = None
        self._generation = 0
        self._play_lock = asyncio.Lock()
        self._deferred_tasks: set[asyncio.Task[PlaybackSnapshot]] = set()
        self._notification_tasks: set[asyncio.Task] = set()
        self._closing = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._closed_event = asyncio.Event()
        self.operational_errors: list[str] = []

    @property
    def runner_task(self) -> asyncio.Task[None] | None:
        return self._runner_task

    @property
    def deferred_listener_tasks(
        self,
    ) -> tuple[asyncio.Task[PlaybackSnapshot], ...]:
        return tuple(self._deferred_tasks)

    def load(self, scenario: Scenario | str) -> PlaybackSnapshot:
        self._invalidate_runner()
        scenario = get_scenario(scenario) if isinstance(scenario, str) else scenario
        self.store.load_scenario(scenario)
        self._scenario_id = scenario.id
        self.store.reset(scenario.id)
        snapshot = self.snapshot()
        self._emit_later(snapshot)
        return snapshot

    def snapshot(self) -> PlaybackSnapshot:
        scenario_id = self._require_scenario()
        return self._snapshot_for(scenario_id)

    def _snapshot_for(self, scenario_id: str) -> PlaybackSnapshot:
        events = self.store.list_scenario_events(scenario_id)
        playback = self.store.get_playback(scenario_id)
        if playback is None:
            raise RuntimeError("playback state is missing")
        return PlaybackSnapshot(
            scenario_id=scenario_id,
            cursor=playback["cursor"],
            total_events=len(events),
            status=playback["status"],
            speed=playback["speed"],
            current_time=playback["current_time"],
            operational_error=playback["operational_error"],
        )

    def step(self) -> PlaybackSnapshot:
        if self._runner_task is not None and not self._runner_task.done():
            raise RuntimeError("active playback requires await step_async()")
        snapshot = self._step(next_status="paused")
        self._emit_later(snapshot)
        return snapshot

    async def step_async(self) -> PlaybackSnapshot:
        task = self._runner_task
        if task is not None and not task.done():
            self._invalidate_runner()
            with suppress(asyncio.CancelledError):
                await task
        snapshot = self._step(next_status="paused")
        self._emit_later(snapshot)
        return snapshot

    def _step(self, *, next_status: str) -> PlaybackSnapshot:
        before = self.snapshot()
        if before.cursor >= before.total_events:
            return self._persist(before.cursor, "completed", before.speed)
        event = self.store.list_scenario_events(before.scenario_id)[before.cursor]
        with self.store.transaction():
            self.engine.process(event)
            cursor = before.cursor + 1
            status = "completed" if cursor == before.total_events else next_status
            self.store.save_playback(
                before.scenario_id,
                cursor,
                status,
                before.speed,
                event.occurred_at.isoformat(),
            )
        return self.snapshot()

    def reset(self) -> PlaybackSnapshot:
        self._invalidate_runner()
        scenario_id = self._require_scenario()
        speed = self.snapshot().speed
        with self.store.transaction():
            self.store.reset(scenario_id)
            self.store.save_playback(scenario_id, 0, "paused", speed)
        snapshot = self.snapshot()
        self._emit_later(snapshot)
        return snapshot

    def rebuild(self, position: int | None = None) -> PlaybackSnapshot:
        self._invalidate_runner()
        scenario_id = self._require_scenario()
        events = self.store.list_scenario_events(scenario_id)
        position = len(events) if position is None else position
        if position < 0 or position > len(events):
            raise ValueError(f"position must be between 0 and {len(events)}")
        speed = self.snapshot().speed
        with self.store.transaction():
            self.store.reset(scenario_id)
            for event in events[:position]:
                self.engine.process(event)
            status = "completed" if position == len(events) else "paused"
            current_time = (
                events[position - 1].occurred_at.isoformat() if position else None
            )
            self.store.save_playback(
                scenario_id, position, status, speed, current_time
            )
        snapshot = self.snapshot()
        self._emit_later(snapshot)
        return snapshot

    async def play(self) -> PlaybackSnapshot:
        if self._closing or self._closed:
            raise RuntimeError("playback service is closing or closed")
        async with self._play_lock:
            if self._closing or self._closed:
                raise RuntimeError("playback service is closing or closed")
            task = self._runner_task
            if task is not None and not task.done():
                if self._runner_generation == self._generation:
                    return self.snapshot()
                with suppress(asyncio.CancelledError):
                    await task
            current = self.snapshot()
            if current.status == "completed":
                return await self._notify(current)
            current = self._persist(
                current.cursor,
                "playing",
                current.speed,
                current_time=(
                    current.current_time.isoformat()
                    if current.current_time
                    else None
                ),
            )
            generation = self._generation
            start_gate = asyncio.Event()
            self._runner_generation = generation
            runner = asyncio.create_task(
                self._run(generation, start_gate),
                name=f"playback:{current.scenario_id}",
            )
            self._runner_task = runner
            current = await self._notify(current)
            if current.status == "error":
                runner.cancel()
                with suppress(asyncio.CancelledError):
                    await runner
                return self.snapshot()
            if (
                generation != self._generation
                or self._runner_task is not runner
                or runner.done()
                or runner.cancelling()
            ):
                if not runner.done():
                    runner.cancel()
                with suppress(asyncio.CancelledError):
                    await runner
                return self.snapshot()
            start_gate.set()
            return self.snapshot()

    async def _run(self, generation: int, start_gate: asyncio.Event) -> None:
        try:
            await start_gate.wait()
            while generation == self._generation:
                current = self.snapshot()
                if current.cursor >= current.total_events:
                    if current.status != "completed":
                        current = self._persist(
                            current.cursor,
                            "completed",
                            current.speed,
                            current_time=(
                                current.current_time.isoformat()
                                if current.current_time
                                else None
                            ),
                        )
                    await self._notify(current)
                    return
                current = self._step(next_status="playing")
                current = await self._notify(current)
                if generation != self._generation:
                    return
                if current.status in {"completed", "error"}:
                    return
                await asyncio.sleep(self.interval_seconds / current.speed)
        except asyncio.CancelledError:
            if generation == self._generation:
                current = self.snapshot()
                if current.status not in {"completed", "error"}:
                    current = self._persist(
                        current.cursor,
                        "paused",
                        current.speed,
                        current_time=(
                            current.current_time.isoformat()
                            if current.current_time
                            else None
                        ),
                    )
                    await self._notify(current)
        except Exception as error:
            if generation == self._generation:
                current = self.snapshot()
                current = self._record_operational_error(error, current)
                await self._notify(current)

    async def pause(self) -> PlaybackSnapshot:
        task = self._runner_task
        current_task = asyncio.current_task()
        if task is not None and not task.done():
            if task is current_task:
                self._generation += 1
            else:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        current = self.snapshot()
        if current.status != "completed" and current.status != "paused":
            current = self._persist(
                current.cursor,
                "paused",
                current.speed,
                current_time=(
                    current.current_time.isoformat() if current.current_time else None
                ),
            )
            if task is current_task:
                self._emit_later(current)
            else:
                await self._notify(current)
        return current

    async def wait(self) -> PlaybackSnapshot:
        task = self._runner_task
        if task is asyncio.current_task():
            return self.snapshot()
        if task is not None:
            with suppress(asyncio.CancelledError):
                await task
        return self.snapshot()

    def set_speed(self, speed: float) -> PlaybackSnapshot:
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError("speed must be a finite positive number")
        current = self.snapshot()
        snapshot = self._persist(
            current.cursor,
            current.status,
            speed,
            current_time=(
                current.current_time.isoformat() if current.current_time else None
            ),
        )
        self._emit_later(snapshot)
        return snapshot

    def add_listener(self, listener: Listener) -> Callable[[], None]:
        self._listeners.append(listener)

        def remove() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return remove

    async def _notify(self, snapshot: PlaybackSnapshot) -> PlaybackSnapshot:
        task = asyncio.current_task()
        if task is not None:
            self._notification_tasks.add(task)
        try:
            return await self._notify_listeners(snapshot)
        finally:
            if task is not None:
                self._notification_tasks.discard(task)

    async def _notify_listeners(
        self, snapshot: PlaybackSnapshot
    ) -> PlaybackSnapshot:
        current = snapshot
        failed: list[Listener] = []
        errors: list[Exception] = []
        for listener in tuple(self._listeners):
            try:
                result = listener(snapshot)
                if inspect.isawaitable(result):
                    await result
            except Exception as error:
                failed.append(listener)
                errors.append(error)
        for error in errors:
            current = self._record_operational_error(error, current)
        if errors:
            for listener in tuple(self._listeners):
                if any(listener is item for item in failed):
                    continue
                try:
                    result = listener(current)
                    if inspect.isawaitable(result):
                        await result
                except Exception as error:
                    current = self._record_operational_error(error, current)
        return current

    def _record_operational_error(
        self, error: Exception, snapshot: PlaybackSnapshot
    ) -> PlaybackSnapshot:
        message = f"{type(error).__name__}: {error}"
        self.operational_errors.append(message)
        self.store.set_playback_error(snapshot.scenario_id, message)
        return self._snapshot_for(snapshot.scenario_id)

    def _emit_later(self, snapshot: PlaybackSnapshot) -> None:
        if not self._listeners or self._closing or self._closed:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            for listener in tuple(self._listeners):
                try:
                    result = listener(snapshot)
                    if inspect.isawaitable(result):
                        asyncio.run(result)
                except Exception as error:
                    self._record_operational_error(error, snapshot)
        else:
            task = loop.create_task(self._notify(snapshot))
            self._deferred_tasks.add(task)
            task.add_done_callback(self._deferred_tasks.discard)

    async def wait_for_notifications(self) -> None:
        while self._deferred_tasks:
            await asyncio.gather(
                *tuple(self._deferred_tasks), return_exceptions=True
            )

    def _persist(
        self,
        cursor: int,
        status: str,
        speed: float,
        *,
        current_time: str | None = None,
        operational_error: str | None = None,
        scenario_id: str | None = None,
    ) -> PlaybackSnapshot:
        scenario_id = scenario_id or self._require_scenario()
        self.store.save_playback(
            scenario_id,
            cursor,
            status,
            speed,
            current_time=current_time,
            operational_error=operational_error,
        )
        return self._snapshot_for(scenario_id)

    def _invalidate_runner(self) -> None:
        self._generation += 1
        if self._runner_task is not None and not self._runner_task.done():
            self._runner_task.cancel()

    def _require_scenario(self) -> str:
        if self._scenario_id is None:
            raise RuntimeError("load a scenario before controlling playback")
        return self._scenario_id

    def close(self) -> None:
        self._closing = True
        self._invalidate_runner()
        for task in tuple(self._deferred_tasks):
            task.cancel()
        for task in tuple(self._notification_tasks):
            task.cancel()
        self.store.close()
        self._closed = True
        self._closed_event.set()

    async def aclose(self) -> None:
        current = asyncio.current_task()
        if current is self._runner_task or current in self._notification_tasks:
            if not self._closing:
                self._closing = True
                if current is self._runner_task:
                    self._generation += 1
                else:
                    self._invalidate_runner()
            if self._close_task is None:
                self._close_task = asyncio.create_task(
                    self._finish_close_after(current)
                )
            return
        self._closing = True
        self._invalidate_runner()
        if self._runner_task is not None:
            with suppress(asyncio.CancelledError):
                await self._runner_task
        tasks = tuple(self._deferred_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._deferred_tasks.clear()
        while True:
            direct = tuple(
                task
                for task in self._notification_tasks
                if task is not current and task is not self._runner_task
            )
            if not direct:
                break
            for task in direct:
                task.cancel()
            await asyncio.gather(*direct, return_exceptions=True)
        self.store.close()
        self._closed = True
        self._closed_event.set()

    async def _finish_close_after(self, owner: asyncio.Task) -> None:
        with suppress(asyncio.CancelledError):
            await owner
        runner = self._runner_task
        if runner is not None and runner is not owner and not runner.done():
            runner.cancel()
            with suppress(asyncio.CancelledError):
                await runner
        tasks = tuple(self._deferred_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        current = asyncio.current_task()
        direct = tuple(
            task
            for task in self._notification_tasks
            if task is not current and task is not owner
        )
        for task in direct:
            task.cancel()
        if direct:
            await asyncio.gather(*direct, return_exceptions=True)
        if not self._closed:
            self.store.close()
            self._closed = True
            self._closed_event.set()

    async def wait_closed(self) -> None:
        if self._close_task is not None:
            await self._close_task
        elif not self._closed:
            await self._closed_event.wait()
