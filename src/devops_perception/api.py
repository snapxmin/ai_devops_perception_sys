"""FastAPI application factory for the perception platform."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from .engine import PerceptionEngine
from .models import PlaybackSnapshot
from .playback import PlaybackService
from .scenarios import get_scenario, list_scenarios
from .store import SQLiteStore

STATIC_DIR = Path(__file__).with_name("static")
DEFAULT_SCENARIO = "healthy-delivery"
LOGGER = logging.getLogger(__name__)
SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; img-src 'self' data:; object-src 'none'; "
        "base-uri 'none'; frame-ancestors 'none'"
    ),
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}
MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


class PlayRequest(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    speed: float | None = Field(default=None, gt=0)


class RebuildRequest(BaseModel):
    position: int | None = Field(default=None, ge=0)


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "unknown")


def _error_response(
    request: Request,
    status_code: int,
    code: str,
    message: str,
    *,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    request_id = _request_id(request)
    error = {
        "code": code,
        "message": message,
        "request_id": request_id,
    }
    response_headers = dict(headers or {})
    response_headers["X-Request-ID"] = request_id
    return JSONResponse(
        status_code=status_code,
        content={"error": error, "detail": error},
        headers=response_headers,
    )


def _playback_json(snapshot: PlaybackSnapshot) -> dict[str, Any]:
    return snapshot.model_dump(mode="json")


def _origin_tuple(url: str) -> tuple[str, str, int | None] | None:
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        return None
    default_port = 443 if parsed.scheme == "https" else 80
    try:
        port = parsed.port
    except ValueError:
        return None
    return parsed.scheme, parsed.hostname.lower(), port or default_port


def _cross_origin_mutation(request: Request) -> bool:
    if request.method not in MUTATING_METHODS:
        return False
    fetch_site = request.headers.get("sec-fetch-site")
    if fetch_site and fetch_site.lower() not in {"same-origin", "none"}:
        return True
    origin = request.headers.get("origin")
    if origin is None:
        return False
    default_port = 443 if request.url.scheme == "https" else 80
    request_origin = (
        request.url.scheme,
        (request.url.hostname or "").lower(),
        request.url.port or default_port,
    )
    return _origin_tuple(origin) != request_origin


def _secure(response):
    for name, value in SECURITY_HEADERS.items():
        response.headers[name] = value
    return response


def create_app(
    *,
    database_path: str | Path | None = None,
    playback_interval: float = 1.0,
    heartbeat_interval: float = 15.0,
    default_scenario: str = DEFAULT_SCENARIO,
) -> FastAPI:
    """Create an isolated application with injectable persistence and timing."""

    configured_path = Path(
        database_path
        if database_path is not None
        else os.getenv("DEVOPS_PERCEPTION_DB", "perception.db")
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = SQLiteStore(configured_path)
        playback: PlaybackService | None = None
        try:
            engine = PerceptionEngine(store)
            playback = PlaybackService(
                store, engine, interval_seconds=playback_interval
            )
            app.state.store = store
            app.state.engine = engine
            app.state.playback = playback
            playback.load(get_scenario(default_scenario))
            yield
        finally:
            if playback is None:
                store.close()
            else:
                await playback.aclose()

    app = FastAPI(
        title="AI DevOps Perception",
        version="0.1.0",
        lifespan=lifespan,
    )
    install_api_problem_handler(app)

    @app.middleware("http")
    async def correlate_request(request: Request, call_next):
        request.state.request_id = request.headers.get("x-request-id") or str(
            uuid.uuid4()
        )
        try:
            if _cross_origin_mutation(request):
                response = _error_response(
                    request,
                    403,
                    "cross_origin_forbidden",
                    "Cross-origin state changes are not allowed",
                )
            else:
                response = await call_next(request)
        except Exception:
            LOGGER.exception(
                "Unexpected request failure request_id=%s",
                request.state.request_id,
            )
            response = _error_response(
                request,
                500,
                "internal_error",
                "An unexpected error occurred",
            )
        response.headers["x-request-id"] = request.state.request_id
        return _secure(response)

    @app.exception_handler(StarletteHTTPException)
    async def framework_http_error(
        request: Request, error: StarletteHTTPException
    ) -> JSONResponse:
        code = (
            "not_found"
            if error.status_code == 404
            else "method_not_allowed"
            if error.status_code == 405
            else "http_error"
        )
        return _error_response(
            request,
            error.status_code,
            code,
            str(error.detail),
            headers=error.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        messages = "; ".join(
            f"{'.'.join(map(str, item['loc']))}: {item['msg']}"
            for item in error.errors()
        )
        return _error_response(
            request, 422, "validation_error", messages or "Invalid request"
        )

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, _error: Exception) -> JSONResponse:
        LOGGER.exception(
            "Unexpected request failure request_id=%s",
            _request_id(request),
            exc_info=_error,
        )
        return _error_response(
            request,
            500,
            "internal_error",
            "An unexpected error occurred",
        )

    def playback() -> PlaybackService:
        return app.state.playback

    def store() -> SQLiteStore:
        return app.state.store

    def current_scenario(request: Request, scenario: str | None = None) -> str:
        if scenario:
            if not store().list_scenario_events(scenario):
                raise ApiProblem(
                    request, 404, "scenario_not_loaded", f"Scenario is not loaded: {scenario}"
                )
            return scenario
        try:
            return playback().snapshot().scenario_id
        except RuntimeError as error:
            raise ApiProblem(
                request, 409, "scenario_not_loaded", str(error)
            ) from error

    def graph_data(scenario_id: str) -> dict[str, list[dict[str, Any]]]:
        snapshot = store().snapshot(scenario_id)
        nodes = []
        for node in snapshot["nodes"]:
            nodes.append(
                {
                    "id": node["node_id"],
                    "kind": node["kind"],
                    "data": json.loads(node["data_json"]),
                    "event_id": node["event_id"],
                }
            )
        edges = [
            {
                "source": edge["source_id"],
                "target": edge["target_id"],
                "relation": edge["relation"],
                "event_id": edge["event_id"],
            }
            for edge in snapshot["edges"]
        ]
        return {"nodes": nodes, "edges": edges}

    def context_data(scenario_id: str) -> dict[str, Any]:
        snapshot = store().snapshot(scenario_id)
        return {
            "scenario_id": scenario_id,
            "playback": _playback_json(playback()._snapshot_for(scenario_id)),
            "timeline": snapshot["timeline"],
            "graph": graph_data(scenario_id),
            "state": [
                {
                    "entity_id": item["entity_id"],
                    "key": item["key"],
                    "value": json.loads(item["value_json"]),
                    "event_id": item["event_id"],
                }
                for item in snapshot["state"]
            ],
            "insights": snapshot["insights"],
        }

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/scenarios")
    async def scenarios() -> list[dict[str, Any]]:
        return [
            {
                "id": scenario.id,
                "name": scenario.name,
                "description": scenario.description,
                "event_count": len(scenario.events),
            }
            for scenario in list_scenarios()
        ]

    @app.post("/api/scenarios/{scenario_id}/load")
    async def load_scenario(request: Request, scenario_id: str) -> dict[str, Any]:
        try:
            scenario = get_scenario(scenario_id)
        except KeyError as error:
            raise ApiProblem(
                request,
                404,
                "scenario_not_found",
                f"Unknown scenario: {scenario_id}",
            ) from error
        return _playback_json(playback().load(scenario))

    @app.get("/api/playback")
    async def get_playback(request: Request) -> dict[str, Any]:
        current_scenario(request)
        return _playback_json(playback().snapshot())

    @app.post("/api/playback/play")
    async def play(
        request: Request, command: PlayRequest | None = None
    ) -> dict[str, Any]:
        current_scenario(request)
        if command and command.speed is not None:
            playback().set_speed(command.speed)
        return _playback_json(await playback().play())

    @app.post("/api/playback/pause")
    async def pause(request: Request) -> dict[str, Any]:
        current_scenario(request)
        return _playback_json(await playback().pause())

    @app.post("/api/playback/step")
    async def step(request: Request) -> dict[str, Any]:
        current_scenario(request)
        return _playback_json(await playback().step_async())

    @app.post("/api/playback/reset")
    async def reset(request: Request) -> dict[str, Any]:
        current_scenario(request)
        return _playback_json(playback().reset())

    @app.post("/api/playback/rebuild")
    async def rebuild(
        request: Request, command: RebuildRequest | None = None
    ) -> dict[str, Any]:
        current_scenario(request)
        try:
            position = command.position if command else None
            return _playback_json(playback().rebuild(position))
        except ValueError as error:
            raise ApiProblem(
                request, 422, "invalid_position", str(error)
            ) from error

    @app.get("/api/context/current")
    async def context(request: Request) -> dict[str, Any]:
        return context_data(current_scenario(request))

    @app.get("/api/context/services/{service_id}")
    async def service_context(
        request: Request,
        service_id: str,
        scenario: str | None = None,
    ) -> dict[str, Any]:
        scenario_id = current_scenario(request, scenario)
        node_id = (
            service_id if service_id.startswith("service:") else f"service:{service_id}"
        )
        snapshot = store().snapshot(scenario_id)
        node = next(
            (item for item in snapshot["nodes"] if item["node_id"] == node_id),
            None,
        )
        if node is None:
            raise ApiProblem(
                request, 404, "service_not_found", f"Unknown service: {service_id}"
            )
        state = {
            item["key"]: json.loads(item["value_json"])
            for item in snapshot["state"]
            if item["entity_id"] == node_id
        }
        graph = graph_data(scenario_id)
        edges = [
            edge
            for edge in graph["edges"]
            if node_id in {edge["source"], edge["target"]}
        ]
        neighbor_ids = {
            endpoint
            for edge in edges
            for endpoint in (edge["source"], edge["target"])
        }
        return {
            "id": node_id,
            "scenario_id": scenario_id,
            "data": json.loads(node["data_json"]),
            "state": state,
            "graph": {
                "nodes": [
                    item for item in graph["nodes"] if item["id"] in neighbor_ids
                ],
                "edges": edges,
            },
            "insights": [
                item
                for item in snapshot["insights"]
                if item["affected"]["id"] == service_id.removeprefix("service:")
            ],
        }

    @app.get("/api/context/impact/{node_id:path}")
    async def impact(
        request: Request,
        node_id: str,
        scenario: str | None = None,
        depth: Annotated[int, Query(ge=0, le=5)] = 3,
    ) -> dict[str, Any]:
        scenario_id = current_scenario(request, scenario)
        known_nodes = {
            item["node_id"] for item in store().snapshot(scenario_id)["nodes"]
        }
        if node_id not in known_nodes:
            raise ApiProblem(
                request,
                404,
                "impact_node_not_found",
                f"Unknown impact root: {node_id}",
            )
        result = store().bounded_impact(scenario_id, node_id, depth)
        return {
            "scenario_id": scenario_id,
            "root": node_id,
            "depth": depth,
            **result,
        }

    @app.get("/api/events")
    async def events(
        request: Request, scenario: str | None = None
    ) -> list[dict[str, Any]]:
        scenario_id = current_scenario(request, scenario)
        return [
            event.model_dump(mode="json")
            for event in store().list_timeline(scenario_id)
        ]

    @app.get("/api/graph")
    async def graph(
        request: Request, scenario: str | None = None
    ) -> dict[str, Any]:
        return graph_data(current_scenario(request, scenario))

    @app.get("/api/insights")
    async def insights(
        request: Request, scenario: str | None = None
    ) -> list[dict[str, Any]]:
        scenario_id = current_scenario(request, scenario)
        return [
            insight.model_dump(mode="json")
            for insight in store().list_insights(scenario_id)
        ]

    @app.get("/api/stream")
    async def stream() -> StreamingResponse:
        queue: asyncio.Queue[PlaybackSnapshot] = asyncio.Queue(maxsize=1)

        def updated(snapshot: PlaybackSnapshot) -> None:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(snapshot)

        async def body() -> AsyncIterator[str]:
            remove = None
            sequence = 0
            try:
                remove = playback().add_listener(updated)
                updated(playback().snapshot())
                while True:
                    try:
                        snapshot = await asyncio.wait_for(
                            queue.get(), timeout=heartbeat_interval
                        )
                    except TimeoutError:
                        yield ": heartbeat\n\n"
                    else:
                        sequence += 1
                        data = {
                            "reconnect_safe": True,
                            "playback": _playback_json(snapshot),
                        }
                        yield (
                            f"id: {snapshot.scenario_id}:{snapshot.cursor}:{sequence}\n"
                            "event: perception.updated\n"
                            f"data: {json.dumps(data, separators=(',', ':'))}\n\n"
                        )
            finally:
                if remove is not None:
                    remove()

        return StreamingResponse(
            body(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


class ApiProblem(Exception):
    def __init__(
        self,
        request: Request,
        status_code: int,
        code: str,
        message: str,
    ) -> None:
        self.request = request
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(message)


def install_api_problem_handler(app: FastAPI) -> None:
    @app.exception_handler(ApiProblem)
    async def api_problem(request: Request, error: ApiProblem) -> JSONResponse:
        return _error_response(
            request, error.status_code, error.code, error.message
        )
