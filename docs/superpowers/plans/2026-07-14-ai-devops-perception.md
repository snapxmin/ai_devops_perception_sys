# AI DevOps Perception Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a one-command, deterministic DevOps digital-twin PoC with explainable projections, replay controls, APIs, and a live dashboard.

**Architecture:** A FastAPI modular monolith accepts canonical scenario events and projects them transactionally into SQLite-backed timeline, graph, state, and insight views. A playback service controls deterministic replay; HTTP and SSE expose the same context consumed by a dependency-free browser dashboard.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic, SQLite, Uvicorn, pytest, HTTPX, HTML/CSS/JavaScript

---

## File Map

- `pyproject.toml`: packaging, runtime dependencies, and pytest configuration.
- `src/devops_perception/models.py`: canonical event, scenario, playback, and insight contracts.
- `src/devops_perception/scenarios.py`: deterministic event scenario catalog.
- `src/devops_perception/store.py`: SQLite schema, transactions, projections, and queries.
- `src/devops_perception/engine.py`: event handlers and explainable perception rules.
- `src/devops_perception/playback.py`: scenario lifecycle and asynchronous playback.
- `src/devops_perception/api.py`: FastAPI routes, error mapping, SSE, and app lifecycle.
- `src/devops_perception/main.py`: Uvicorn entry point.
- `src/devops_perception/static/`: dashboard HTML, CSS, and JavaScript.
- `tests/`: domain, engine, playback, API, and deterministic replay tests.
- `README.md`: setup, architecture, API, and demonstration guide.

### Task 1: Foundation and Canonical Models

**Files:**
- Create: `pyproject.toml`
- Create: `src/devops_perception/__init__.py`
- Create: `src/devops_perception/models.py`
- Test: `tests/test_models.py`

- [ ] **Step 1: Write the failing canonical-model tests**

```python
def test_event_requires_utc_timestamp():
    with pytest.raises(ValidationError):
        CanonicalEvent(
            id="evt-1", occurred_at="2026-07-14T10:00:00",
            type="RequirementCreated", source="planning",
            actor={"type": "user", "id": "alice"},
            subject={"type": "requirement", "id": "REQ-1"},
            correlation_id="REQ-1", trace_id="trace-1", payload={},
        )

def test_event_serialization_is_stable():
    assert make_event().model_dump(mode="json") == make_event().model_dump(mode="json")
```

- [ ] **Step 2: Verify the tests fail because models do not exist**

Run: `pytest tests/test_models.py -q`  
Expected: import failure for `devops_perception.models`.

- [ ] **Step 3: Implement immutable Pydantic contracts**

Define `EntityRef`, `CanonicalEvent`, `Scenario`, `PlaybackSnapshot`, and
`Insight` with forbidden extra fields, UTC timestamp validation, supported
schema version `1`, and immutable event fields.

- [ ] **Step 4: Verify the model tests pass**

Run: `pytest tests/test_models.py -q`  
Expected: all model tests pass.

### Task 2: Deterministic Scenario Catalog

**Files:**
- Create: `src/devops_perception/scenarios.py`
- Test: `tests/test_scenarios.py`

- [ ] **Step 1: Write failing catalog tests**

```python
def test_catalog_contains_three_scenarios():
    assert set(get_catalog()) == {
        "healthy-delivery", "performance-regression", "security-gate"
    }

def test_scenario_generation_is_deterministic():
    first = get_scenario("performance-regression").model_dump(mode="json")
    second = get_scenario("performance-regression").model_dump(mode="json")
    assert first == second
```

- [ ] **Step 2: Verify catalog tests fail**

Run: `pytest tests/test_scenarios.py -q`  
Expected: import failure for `devops_perception.scenarios`.

- [ ] **Step 3: Implement scenario builders**

Create fixed, correlated event streams for healthy delivery, a v2.4 payment
regression ending in rollback, and a secret detection ending in a passing
security gate. Use stable IDs and timestamps.

- [ ] **Step 4: Verify catalog tests pass**

Run: `pytest tests/test_scenarios.py -q`  
Expected: all scenario tests pass.

### Task 3: Transactional Store and Graph Queries

**Files:**
- Create: `src/devops_perception/store.py`
- Test: `tests/test_store.py`

- [ ] **Step 1: Write failing repository tests**

```python
def test_transaction_rolls_back_all_projections(store):
    with pytest.raises(RuntimeError):
        with store.transaction():
            store.upsert_state("service:payment:health", "degraded", "evt-1")
            raise RuntimeError("stop")
    assert store.current_state() == {}

def test_impact_query_returns_evidence_path(store):
    store.upsert_node("deployment:v2.4", "deployment", "v2.4", {})
    store.upsert_node("service:payment", "service", "Payment", {})
    store.upsert_edge("deployment:v2.4", "service:payment", "affects", "evt-1")
    assert store.impact("deployment:v2.4", 2)["edges"][0]["relation"] == "affects"
```

- [ ] **Step 2: Verify store tests fail**

Run: `pytest tests/test_store.py -q`  
Expected: import failure for `devops_perception.store`.

- [ ] **Step 3: Implement SQLite schema and repository**

Create tables for scenario events, timeline, processed events, graph nodes,
graph edges, state facts, insights, insight evidence, and playback. Implement
explicit nested-safe transactions, JSON serialization, projection reset,
snapshot queries, and bounded recursive impact traversal.

- [ ] **Step 4: Verify store tests pass**

Run: `pytest tests/test_store.py -q`  
Expected: all store tests pass.

### Task 4: Perception Engine and Explainable Rules

**Files:**
- Create: `src/devops_perception/engine.py`
- Test: `tests/test_engine.py`
- Test: `tests/test_replay.py`

- [ ] **Step 1: Write failing projection tests**

```python
def test_metric_after_deployment_creates_release_regression(engine, store):
    replay_prefix(engine, "performance-regression", through="evt-perf-009")
    rules = {item["rule_id"] for item in store.insights()}
    assert "release-regression" in rules
    regression = next(i for i in store.insights() if i["rule_id"] == "release-regression")
    assert {"evt-perf-006", "evt-perf-008"} <= set(regression["evidence_event_ids"])

def test_duplicate_event_is_idempotent(engine, store, requirement_event):
    assert engine.process(requirement_event) is True
    assert engine.process(requirement_event) is False
    assert len(store.events()) == 1
```

- [ ] **Step 2: Verify engine tests fail**

Run: `pytest tests/test_engine.py tests/test_replay.py -q`  
Expected: import failure for `devops_perception.engine`.

- [ ] **Step 3: Implement handlers and rules**

Map every scenario event type into timeline, graph, and state facts. Evaluate
latency, error rate, release correlation, incident aggregation, security gate,
and recovery rules. Persist stable insight IDs and evidence event IDs in the
same transaction as processed-event registration.

- [ ] **Step 4: Add deterministic replay assertion**

Replay every scenario twice around a full projection reset and compare
canonical JSON snapshots of timeline, graph, state, and insights.

- [ ] **Step 5: Verify perception tests pass**

Run: `pytest tests/test_engine.py tests/test_replay.py -q`  
Expected: all engine and replay tests pass.

### Task 5: Playback Service

**Files:**
- Create: `src/devops_perception/playback.py`
- Test: `tests/test_playback.py`

- [ ] **Step 1: Write failing playback tests**

```python
def test_step_reset_and_rebuild(playback):
    playback.load("performance-regression")
    assert playback.step().cursor == 1
    assert playback.reset().cursor == 0
    snapshot = playback.rebuild(4)
    assert snapshot.cursor == 4
    assert snapshot.total > snapshot.cursor

def test_invalid_rebuild_position_is_rejected(playback):
    playback.load("healthy-delivery")
    with pytest.raises(ValueError, match="position"):
        playback.rebuild(999)
```

- [ ] **Step 2: Verify playback tests fail**

Run: `pytest tests/test_playback.py -q`  
Expected: import failure for `devops_perception.playback`.

- [ ] **Step 3: Implement playback lifecycle**

Implement load, snapshot, step, reset, rebuild, async play, pause, speed
validation, listener notification, completion, and safe cancellation. Keep one
runner task per application.

- [ ] **Step 4: Verify playback tests pass**

Run: `pytest tests/test_playback.py -q`  
Expected: all playback tests pass.

### Task 6: Context API and SSE

**Files:**
- Create: `src/devops_perception/api.py`
- Create: `src/devops_perception/main.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write failing API contract tests**

```python
def test_scenario_step_updates_context(client):
    client.post("/api/scenarios/performance-regression/load")
    response = client.post("/api/playback/step")
    assert response.status_code == 200
    context = client.get("/api/context/current").json()
    assert context["playback"]["cursor"] == 1
    assert len(context["timeline"]) == 1

def test_unknown_scenario_returns_structured_404(client):
    response = client.post("/api/scenarios/missing/load")
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "scenario_not_found"
```

- [ ] **Step 2: Verify API tests fail**

Run: `pytest tests/test_api.py -q`  
Expected: import failure for `devops_perception.api`.

- [ ] **Step 3: Implement FastAPI application**

Add lifecycle-managed store and playback dependencies, all designed routes,
structured HTTP errors, static mounting, dashboard fallback, and an SSE endpoint
that emits update notifications plus heartbeats.

- [ ] **Step 4: Verify API tests pass**

Run: `pytest tests/test_api.py -q`  
Expected: all API tests pass.

### Task 7: Live Dashboard

**Files:**
- Create: `src/devops_perception/static/index.html`
- Create: `src/devops_perception/static/styles.css`
- Create: `src/devops_perception/static/app.js`
- Test: `tests/test_dashboard.py`

- [ ] **Step 1: Write failing dashboard smoke test**

```python
def test_dashboard_contains_all_perception_surfaces(client):
    html = client.get("/").text
    for element_id in ("controls", "state-grid", "timeline", "graph", "insights", "context"):
        assert 'id="%s"' % element_id in html
```

- [ ] **Step 2: Verify dashboard test fails**

Run: `pytest tests/test_dashboard.py -q`  
Expected: dashboard route or required elements are missing.

- [ ] **Step 3: Implement responsive dashboard**

Build semantic HTML, a dark operations-console visual system, responsive panels,
accessible controls, dynamic timeline cards, an SVG force-like layered graph,
severity-coded insight evidence, a JSON inspector, SSE reconnect behavior, and
HTTP control actions.

- [ ] **Step 4: Verify dashboard test passes**

Run: `pytest tests/test_dashboard.py -q`  
Expected: dashboard smoke test passes.

### Task 8: Documentation and Final Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document setup and the demonstration flow**

Document Python prerequisites, virtual environment setup, dependency install,
`uvicorn devops_perception.main:app --reload`, dashboard URL, tests, API
examples, architecture, scenarios, extension ports, and a five-minute demo.

- [ ] **Step 2: Run formatting and static checks**

Run: `python -m compileall -q src tests && python -m ruff check src tests`  
Expected: exit code 0 with no diagnostics.

- [ ] **Step 3: Run the complete suite**

Run: `pytest -q`  
Expected: all tests pass.

- [ ] **Step 4: Run an HTTP smoke test**

Run the application, request `/api/health`, load and rebuild the performance
scenario, request `/api/context/current`, and verify successful JSON responses.

- [ ] **Step 5: Commit the completed implementation**

```bash
git add .
git commit -m "feat: build AI DevOps perception platform PoC"
git push -u origin cursor/build-ai-devops-perception-8e99
```
