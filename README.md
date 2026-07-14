# AI DevOps Perception

A deterministic proof of concept that turns canonical delivery events into a
replayable timeline, graph, materialized state, and explainable operational
insights. It includes a FastAPI context API, SQLite persistence, server-sent
updates, and a dependency-free operations dashboard.

## Setup

Python 3.11 or newer is required.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

Start the application:

```bash
python -m uvicorn devops_perception.main:app --reload
```

Open <http://127.0.0.1:8000>. The default database is `perception.db`; override
it with `DEVOPS_PERCEPTION_DB=/path/to/perception.db`. Tests and embedded use
can avoid global configuration with
`create_app(database_path=..., playback_interval=..., heartbeat_interval=...)`.

## Architecture

The system is a lifecycle-managed modular monolith:

1. `models.py` defines immutable canonical events, scenarios, insights, and
   playback snapshots.
2. `scenarios.py` provides the deterministic event catalog.
3. `store.py` owns SQLite schema, transactions, projected state, graph, and
   timeline persistence.
4. `engine.py` atomically and idempotently projects each event and evaluates
   explainable rules.
5. `playback.py` owns one asynchronous replay runner, stepping, reset, speed,
   rebuild, and update listeners.
6. `api.py` creates the FastAPI app, initializes and closes resources through
   its lifespan, serves context routes, structured errors, SSE, and static
   assets.
7. `static/` is a semantic HTML/CSS/JavaScript console. SSE notifications are
   intentionally small: clients reconnect and refetch authoritative context.

SQLite transactions keep timeline, graph, state, insights, and cursor updates
consistent. Rebuild clears derived data and deterministically replays source
events to the requested position.

## Built-in scenarios

- **Healthy delivery** — Payment v2.3 deploys with healthy latency and errors.
- **Performance regression** — v2.4 introduces latency and error regressions,
  an evidence-backed incident, rollback, and confirmed recovery.
- **Security gate** — a secret blocks delivery until remediation, rescan, and a
  successful build.

## API

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/api/health` | Process health |
| GET | `/api/scenarios` | Built-in scenario summaries |
| POST | `/api/scenarios/{id}/load` | Select and reset a scenario |
| GET | `/api/playback` | Current playback snapshot |
| POST | `/api/playback/play` | Play; accepts `{"speed": 2}` |
| POST | `/api/playback/pause` | Pause the active runner |
| POST | `/api/playback/step` | Atomically process one event |
| POST | `/api/playback/reset` | Clear derived context |
| POST | `/api/playback/rebuild` | Replay to `{"position": 4}` or the end |
| GET | `/api/context/current` | Complete agent-facing context |
| GET | `/api/context/services/{service_id}` | Service state and neighborhood |
| GET | `/api/context/impact/{node_id}` | Impact paths; `depth` is bounded 0–5 |
| GET | `/api/events` | Perceived event timeline |
| GET | `/api/graph` | Directed nodes and edges |
| GET | `/api/insights` | Severity, status, and evidence |
| GET | `/api/stream` | `perception.updated` SSE and heartbeats |

Context routes accept a `scenario` query parameter where useful. Mutation
responses are playback snapshots. Errors include stable `code`, safe `message`,
and `request_id` fields; the same request ID is returned in `X-Request-ID`.

Examples:

```bash
curl -s http://127.0.0.1:8000/api/scenarios
curl -s -X POST http://127.0.0.1:8000/api/scenarios/performance-regression/load
curl -s -X POST -H 'content-type: application/json' \
  -d '{"position":5}' http://127.0.0.1:8000/api/playback/rebuild
curl -s 'http://127.0.0.1:8000/api/context/impact/service:payment-service?depth=2'
curl -N http://127.0.0.1:8000/api/stream
```

## Extension ports

- Add immutable scenario fixtures or an external event adapter at the canonical
  `CanonicalEvent` boundary.
- Add graph/state projection handlers and insight rules in `PerceptionEngine`;
  evidence remains canonical event IDs.
- Replace `SQLiteStore` behind the existing store operations for another
  persistence backend.
- Register playback listeners for audit, metrics, or another notification
  transport.
- Build another UI or agent against `/api/context/current`; the dashboard has
  no framework or build-chain dependency.

## Tests and checks

```bash
python -m pytest -q
python -m compileall -q src tests
python -m ruff check src tests
```

Node.js is not a runtime or build dependency. When Node is available, the test
suite also runs optional JavaScript syntax and request-order behavior checks;
those extra checks are explicitly skipped when Node is absent.

The suite covers model validation, scenario determinism, transactional storage,
projection and evidence rules, replay equivalence, playback concurrency and
cleanup, API contracts and errors, SSE response shape, lifespan cleanup, and
dashboard assets.

## Five-minute demo

1. Start the server and open the dashboard. Point out the default healthy
   scenario and the live connection indicator.
2. Select **Performance regression**, set speed to `2×`, and step through the
   deployment, elevated latency, error rate, log, and incident. Show the state,
   graph, timeline, and evidence IDs changing together.
3. Choose **Rebuild** with position `9`. Show v2.3 restored, service health,
   critical incident evidence, and the resolved recovery insight.
4. Reset, then rebuild to position `4` to demonstrate deterministic
   time-travel. Open the JSON context panel as the exact context an agent sees.
5. Select **Security gate** and rebuild to the end. Show detection, blocked
   gate/build, remediation, rescan, and passing delivery. Finish with the
   context and impact API `curl` examples above.
