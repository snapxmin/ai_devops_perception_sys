import shutil
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from devops_perception.api import create_app

NODE = shutil.which("node")
REQUIRES_NODE = pytest.mark.skipif(
    NODE is None,
    reason="Node.js is optional and unavailable; skipping extra JavaScript checks",
)


def test_dashboard_contains_all_perception_surfaces(tmp_path: Path) -> None:
    app = create_app(database_path=tmp_path / "dashboard.db")
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    for element_id in (
        "controls",
        "state-grid",
        "timeline",
        "graph",
        "insights",
        "context",
    ):
        assert f'id="{element_id}"' in html
    assert "<header" in html
    assert "<main" in html
    assert 'aria-live="polite"' in html
    for category in ("build", "release", "service-health", "incident", "security"):
        assert f'data-state-category="{category}"' in html
    assert html.count('class="state-card') >= 5
    assert "Not observed" in html
    assert "No active incident" in html
    assert '<marker id="arrow"' in html
    assert 'aria-describedby="graph-summary"' in html
    assert 'id="graph-summary"' in html
    assert 'id="graph-node-list"' in html
    assert 'id="graph-relationship-list"' in html
    assert 'aria-label="Playback speed"' in html
    assert 'aria-label="Rebuild position"' in html
    assert 'tabindex="0"' in html
    assert 'class="skip-link"' in html
    assert html.count("aria-labelledby=") >= 6


def test_dashboard_static_assets_are_served_and_wire_live_controls(
    tmp_path: Path,
) -> None:
    app = create_app(database_path=tmp_path / "assets.db")
    with TestClient(app) as client:
        css = client.get("/static/styles.css")
        script = client.get("/static/app.js")
        control_logic = client.get("/static/control.js")

    assert css.status_code == 200
    assert "--surface" in css.text
    assert ":focus-visible" in css.text
    assert "@media" in css.text
    assert script.status_code == 200
    assert control_logic.status_code == 200
    assert "EventSource" in script.text
    assert "/api/stream" in script.text
    assert "perception.updated" in script.text
    assert "setTimeout(connectStream" in script.text
    assert "keydown" in script.text
    assert "Map.groupBy" not in script.text
    assert 'createElement("details")' in script.text
    assert "event.source" in script.text
    assert "event.actor" in script.text
    assert "event.payload" in script.text
    assert "timelineById" in script.text
    assert "evidence_event_ids" in script.text
    assert '"marker-end": "url(#arrow)"' in script.text
    assert 'tabindex: "0"' not in script.text
    assert "graphNodeList" in script.text
    assert "graphRelationshipList" in script.text
    assert "let refreshQueued = false" in script.text
    assert "refreshQueued = true" in script.text
    assert "if (refreshQueued)" in script.text
    assert "createIntentQueue" in script.text
    assert 'return request("/api/context/current")' in script.text
    assert "(context) => render(context)" in script.text
    assert "setMutationPending" in script.text
    assert ".disabled = active" in script.text


@REQUIRES_NODE
@pytest.mark.parametrize(
    "script",
    [
        "src/devops_perception/static/app.js",
        "src/devops_perception/static/control.js",
        "tests/test_dashboard_logic.js",
    ],
)
def test_dashboard_javascript_syntax(script: str) -> None:
    result = subprocess.run(
        [NODE, "--check", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


@REQUIRES_NODE
def test_dashboard_intent_queue_node_contract() -> None:
    result = subprocess.run(
        [NODE, "tests/test_dashboard_logic.js"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
