import asyncio
import json
from pathlib import Path
from sqlite3 import ProgrammingError

import pytest
from fastapi.testclient import TestClient
from starlette.responses import StreamingResponse

from devops_perception.api import create_app
from devops_perception.playback import PlaybackService


@pytest.fixture
def client(tmp_path: Path):
    app = create_app(
        database_path=tmp_path / "api.db",
        playback_interval=60,
        heartbeat_interval=0.01,
    )
    with TestClient(app) as test_client:
        yield test_client


def test_health_and_scenario_catalog_contract(client: TestClient) -> None:
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["x-request-id"]

    scenarios = client.get("/api/scenarios").json()
    assert [item["id"] for item in scenarios] == [
        "healthy-delivery",
        "performance-regression",
        "security-gate",
    ]
    assert all({"name", "description", "event_count"} <= item.keys() for item in scenarios)


def test_responses_include_browser_security_headers(client: TestClient) -> None:
    response = client.get("/")

    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["referrer-policy"] == "no-referrer"


def test_mutations_reject_cross_origin_browser_requests(client: TestClient) -> None:
    foreign_origin = client.post(
        "/api/playback/step", headers={"origin": "https://attacker.example"}
    )
    cross_site = client.post(
        "/api/playback/step", headers={"sec-fetch-site": "cross-site"}
    )
    same_site_cross_origin = client.post(
        "/api/playback/step", headers={"sec-fetch-site": "same-site"}
    )
    malformed_origin = client.post(
        "/api/playback/step", headers={"origin": "http://attacker.example:invalid"}
    )

    for response in (
        foreign_origin,
        cross_site,
        same_site_cross_origin,
        malformed_origin,
    ):
        assert response.status_code == 403
        assert response.json()["error"]["code"] == "cross_origin_forbidden"
        assert response.headers["x-request-id"]
        assert response.headers["x-frame-options"] == "DENY"


def test_mutations_allow_same_origin_and_non_browser_clients(
    client: TestClient,
) -> None:
    same_origin = client.post(
        "/api/playback/reset", headers={"origin": "http://testserver"}
    )
    no_browser_headers = client.post("/api/playback/reset")

    assert same_origin.status_code == 200
    assert no_browser_headers.status_code == 200


def test_scenario_step_updates_current_context(client: TestClient) -> None:
    loaded = client.post("/api/scenarios/performance-regression/load")
    assert loaded.status_code == 200
    assert loaded.json()["cursor"] == 0

    stepped = client.post("/api/playback/step")
    assert stepped.status_code == 200
    context = client.get("/api/context/current").json()
    assert context["playback"]["cursor"] == 1
    assert len(context["timeline"]) == 1
    assert context["timeline"][0]["type"] == "deployment.completed"


def test_playback_controls_speed_reset_and_position_rebuild(
    client: TestClient,
) -> None:
    client.post("/api/scenarios/healthy-delivery/load")

    playing = client.post("/api/playback/play", json={"speed": 2})
    assert playing.status_code == 200
    assert playing.json()["speed"] == 2
    assert playing.json()["status"] == "playing"
    assert client.get("/api/playback").json()["status"] == "playing"

    paused = client.post("/api/playback/pause")
    assert paused.json()["status"] == "paused"
    rebuilt = client.post("/api/playback/rebuild", json={"position": 3})
    assert rebuilt.json()["cursor"] == 3
    assert len(client.get("/api/events").json()) == 3
    reset = client.post("/api/playback/reset")
    assert reset.json()["cursor"] == 0


def test_replay_context_service_impact_graph_and_insights(client: TestClient) -> None:
    client.post("/api/scenarios/performance-regression/load")
    rebuilt = client.post("/api/playback/rebuild", json={})
    assert rebuilt.status_code == 200
    assert rebuilt.json()["status"] == "completed"

    service = client.get("/api/context/services/payment-service")
    assert service.status_code == 200
    assert service.json()["id"] == "service:payment-service"
    assert service.json()["state"]["status"] == "healthy"
    assert service.json()["state"]["current_version"] == "v2.3"

    impact = client.get(
        "/api/context/impact/service:payment-service",
        params={"scenario": "performance-regression", "depth": 1},
    )
    assert impact.status_code == 200
    assert "service:payment-service" in impact.json()["nodes"]
    assert impact.json()["depth"] == 1

    graph = client.get("/api/graph").json()
    insights = client.get("/api/insights").json()
    assert graph["nodes"] and graph["edges"]
    assert any(item["severity"] == "critical" for item in insights)
    assert len(client.get("/api/events").json()) == 9


@pytest.mark.parametrize(
    ("method", "path", "json_body", "status", "code"),
    [
        ("post", "/api/scenarios/missing/load", None, 404, "scenario_not_found"),
        (
            "get",
            "/api/context/services/missing",
            None,
            404,
            "service_not_found",
        ),
        (
            "post",
            "/api/playback/play",
            {"speed": 0},
            422,
            "validation_error",
        ),
    ],
)
def test_failures_are_structured_and_request_correlated(
    client: TestClient,
    method: str,
    path: str,
    json_body: dict | None,
    status: int,
    code: str,
) -> None:
    response = client.request(method, path, json=json_body)
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["request_id"] == response.headers["x-request-id"]
    assert response.json()["error"]["message"]


def test_framework_404_and_405_errors_are_structured(client: TestClient) -> None:
    missing = client.get("/api/does-not-exist")
    wrong_method = client.delete("/api/health")

    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"
    assert missing.json()["error"]["request_id"] == missing.headers["x-request-id"]
    assert wrong_method.status_code == 405
    assert wrong_method.json()["error"]["code"] == "method_not_allowed"
    assert (
        wrong_method.json()["error"]["request_id"]
        == wrong_method.headers["x-request-id"]
    )


def test_unexpected_error_is_structured_and_always_request_correlated(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = create_app(database_path=tmp_path / "unexpected.db")

    @app.get("/api/test/unexpected")
    async def unexpected() -> None:
        raise RuntimeError("sensitive internal detail")

    with TestClient(app, raise_server_exceptions=False) as test_client:
        response = test_client.get(
            "/api/test/unexpected", headers={"x-request-id": "review-request-id"}
        )

    assert response.status_code == 500
    assert response.headers["x-request-id"] == "review-request-id"
    assert response.json()["error"] == {
        "code": "internal_error",
        "message": "An unexpected error occurred",
        "request_id": "review-request-id",
    }
    assert "sensitive internal detail" not in response.text
    assert "request_id=review-request-id" in caplog.text


def test_invalid_rebuild_and_impact_depth_are_bounded(client: TestClient) -> None:
    client.post("/api/scenarios/healthy-delivery/load")
    invalid_position = client.post(
        "/api/playback/rebuild", json={"position": 99}
    )
    assert invalid_position.status_code == 422
    assert invalid_position.json()["error"]["code"] == "invalid_position"

    invalid_depth = client.get(
        "/api/context/impact/service:payment-service", params={"depth": 99}
    )
    assert invalid_depth.status_code == 422
    assert invalid_depth.json()["error"]["code"] == "validation_error"


def test_impact_rejects_an_unknown_root_node(client: TestClient) -> None:
    response = client.get("/api/context/impact/service:missing")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "impact_node_not_found"


def test_stream_response_contract_and_reconnect_safe_payload(
    client: TestClient,
) -> None:
    client.post("/api/scenarios/healthy-delivery/load")
    route = next(route for route in client.app.routes if route.path == "/api/stream")

    async def receive_update_and_heartbeat():
        response = await route.endpoint()
        first = await anext(response.body_iterator)
        heartbeat = await anext(response.body_iterator)
        await response.body_iterator.aclose()
        return response, first, heartbeat

    response, first, heartbeat = asyncio.run(receive_update_and_heartbeat())
    assert isinstance(response, StreamingResponse)
    assert response.media_type == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"

    payload = first.decode() if isinstance(first, bytes) else first
    assert "event: perception.updated" in payload
    data = json.loads(payload.split("data: ", 1)[1])
    assert data["reconnect_safe"] is True
    assert data["playback"]["scenario_id"] == "healthy-delivery"
    heartbeat_payload = (
        heartbeat.decode() if isinstance(heartbeat, bytes) else heartbeat
    )
    assert heartbeat_payload == ": heartbeat\n\n"


def test_stream_disconnect_removes_listener(client: TestClient) -> None:
    route = next(route for route in client.app.routes if route.path == "/api/stream")
    playback = client.app.state.playback
    before = len(playback._listeners)

    async def connect_then_disconnect() -> int:
        response = await route.endpoint()
        await anext(response.body_iterator)
        connected = len(playback._listeners)
        await response.body_iterator.aclose()
        return connected

    connected = asyncio.run(connect_then_disconnect())

    assert connected == before + 1
    assert len(playback._listeners) == before


def test_stream_initialization_failure_removes_listener(client: TestClient) -> None:
    route = next(route for route in client.app.routes if route.path == "/api/stream")
    playback = client.app.state.playback
    original_snapshot = playback.snapshot
    before = len(playback._listeners)

    def fail_snapshot():
        raise RuntimeError("initial snapshot failed")

    async def connect() -> None:
        response = await route.endpoint()
        with pytest.raises(RuntimeError, match="initial snapshot failed"):
            await anext(response.body_iterator)

    playback.snapshot = fail_snapshot
    try:
        asyncio.run(connect())
    finally:
        playback.snapshot = original_snapshot

    assert len(playback._listeners) == before


def test_startup_load_failure_still_closes_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_load(_service, _scenario):
        raise RuntimeError("default scenario failed")

    monkeypatch.setattr(PlaybackService, "load", fail_load)
    app = create_app(database_path=tmp_path / "startup-failure.db")

    with pytest.raises(RuntimeError, match="default scenario failed"):
        with TestClient(app):
            pass

    with pytest.raises(ProgrammingError):
        app.state.store.connection.execute("SELECT 1")


def test_default_scenario_lookup_failure_still_closes_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_lookup(_scenario_id):
        raise RuntimeError("scenario initialization failed")

    monkeypatch.setattr("devops_perception.api.get_scenario", fail_lookup)
    app = create_app(database_path=tmp_path / "lookup-failure.db")

    with pytest.raises(RuntimeError, match="scenario initialization failed"):
        with TestClient(app):
            pass

    with pytest.raises(ProgrammingError):
        app.state.store.connection.execute("SELECT 1")


def test_lifespan_closes_playback_and_database(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "cleanup.db")
    with TestClient(app):
        service = app.state.playback
        connection = app.state.store.connection
        assert service.snapshot().scenario_id == "healthy-delivery"

    assert service.runner_task is None or service.runner_task.done()
    with pytest.raises(ProgrammingError):
        connection.execute("SELECT 1")
