# AI DevOps Perception Platform Design

## 1. Goal

Build a locally runnable proof of concept that continuously converts simulated
software-delivery events into an explainable digital twin. The platform must
show what happened, what the delivery system currently looks like, how entities
are related, and why the platform believes a risk or incident exists.

The PoC proves the perception loop without depending on Kafka, Redis, Neo4j,
PostgreSQL, an LLM, or external DevOps systems.

## 2. Scope

The first release includes:

- Three deterministic scenarios: healthy delivery, performance regression and
  rollback, and secret-scan build blocking.
- Playback controls: play, pause, single-step, reset, speed selection, and
  deterministic state rebuild from an event position.
- A canonical event envelope with correlation, causation, trace, actor, subject,
  payload, and source fields.
- Atomic projections into a timeline, a delivery graph, current state, and
  explainable insights.
- Context APIs for platform state, service context, graph impact, event history,
  scenarios, and playback control.
- A real-time browser dashboard with scenario controls, timeline, graph,
  current-state cards, insights, and raw context inspection.
- SQLite persistence and Server-Sent Events for browser updates.

The first release excludes authentication, multi-tenancy, real integrations,
distributed messaging, autonomous actions, and LLM inference.

## 3. Options Considered

### Option A: Infrastructure-faithful microservices

Kafka, PostgreSQL, Neo4j, Redis, independent consumers, and a React frontend
would closely resemble a production deployment. It has high operational cost,
makes deterministic reset harder, and obscures the perception logic in a PoC.

### Option B: Pure in-memory demonstrator

A single process with in-memory collections is fastest to write, but it cannot
prove replay, recovery, transactionality, or durable context. Its interfaces
would also encourage coupling.

### Option C: Modular monolith with ports and SQLite

This design keeps a single-command experience while preserving explicit
boundaries around the event bus, timeline, graph, and state projections.
SQLite provides transactional persistence and recursive graph queries. These
ports can later be backed by Kafka, PostgreSQL, Neo4j, and Redis.

Option C is selected.

## 4. Architecture

```text
Deterministic Scenario Catalog
             |
             v
       Playback Runner -----> Canonical Event
                                   |
                                   v
                            Perception Engine
                                   |
              +--------------------+-------------------+
              |                    |                   |
              v                    v                   v
          Timeline            Delivery Graph      Current State
              \                    |                   /
               +-------------------+------------------+
                                   |
                                   v
                         Insight/Evidence Rules
                                   |
                    +--------------+--------------+
                    v                             v
               Context API                 SSE Dashboard
```

The application is a modular monolith. The domain layer defines event and
scenario models. The application layer owns playback and projection
orchestration. The infrastructure layer implements repositories with SQLite.
The web layer exposes FastAPI endpoints, SSE, and static dashboard assets.

## 5. Canonical Event Model

Every generated event uses one immutable envelope:

```json
{
  "id": "evt-perf-008",
  "occurred_at": "2026-07-14T10:32:00Z",
  "type": "MetricObserved",
  "source": "observability",
  "actor": {"type": "system", "id": "prometheus"},
  "subject": {"type": "service", "id": "payment"},
  "correlation_id": "delivery-v2.4",
  "causation_id": "evt-perf-007",
  "trace_id": "trace-performance-regression",
  "schema_version": 1,
  "payload": {"latency_ms": 820, "error_rate": 0.12}
}
```

Validation rejects unsupported schema versions, unknown event types, malformed
timestamps, and missing subject identifiers before projection.

## 6. Perception Projections

### Timeline

Stores the immutable ordered event stream. Ordering is deterministic by scenario
position, not wall-clock arrival time. Raw payloads remain available as
evidence.

### Delivery graph

Stores typed nodes and directed edges. Event handlers upsert nodes such as
requirement, task, developer, commit, pull request, build, artifact, deployment,
service, environment, metric, finding, and incident. Edges express relations
such as implements, authored-by, included-in, triggers, produces, deploys,
affects, emits, detects, causes, and rolls-back.

Impact queries traverse both incoming and outgoing edges with a bounded depth
and return the paths that justify the result.

### Current state

Stores namespaced key-value facts with the event that last changed each fact.
Examples include scenario playback status, service version, deployment phase,
build status, health, latency, error rate, open incident, and security gate.

### Explainable insights

Rules consume the event plus current projected context and produce insights:

- Elevated latency when latency crosses 500 ms.
- Error-rate degradation when error rate crosses 5%.
- Correlated release regression when degradation follows a deployment for the
  same service.
- Incident evidence aggregation linking deployment, metrics, logs, and incident.
- Security gate blocking when a secret finding is detected.
- Recovery when rollback returns the service to healthy thresholds.

Each insight contains severity, summary, affected entity, rule ID, status, and
evidence event IDs. Replaying the same prefix produces the same insights.

## 7. Playback and Rebuild Semantics

A scenario has a fixed ID, title, description, base timestamp, and ordered event
templates. Loading a scenario persists its generated canonical events and sets
the cursor to zero.

- `step` projects exactly one next event.
- `play` starts a background task that repeatedly steps at the selected speed.
- `pause` stops future steps without undoing projected events.
- `reset` clears projections and returns the cursor to zero.
- `rebuild(position)` clears projections and synchronously reprojects the event
  prefix ending at `position`.

Projection of one event and cursor advancement occur in one SQLite transaction.
The event ID is recorded in a processed-events table, making duplicate delivery
idempotent.

## 8. API

- `GET /api/health`
- `GET /api/scenarios`
- `POST /api/scenarios/{scenario_id}/load`
- `GET /api/playback`
- `POST /api/playback/play`
- `POST /api/playback/pause`
- `POST /api/playback/step`
- `POST /api/playback/reset`
- `POST /api/playback/rebuild`
- `GET /api/context/current`
- `GET /api/context/services/{service_id}`
- `GET /api/context/impact/{node_id}?depth=3`
- `GET /api/events`
- `GET /api/graph`
- `GET /api/insights`
- `GET /api/stream`

Mutation responses return the resulting playback snapshot. Invalid scenarios,
positions, or speeds return structured `4xx` errors. Unexpected failures return
a request ID and do not expose stack traces.

## 9. Dashboard

The browser dashboard is delivered by the same FastAPI process and uses plain
HTML, CSS, and JavaScript to avoid a Node build dependency. It contains:

- A header showing connection and playback status.
- Scenario selector and play, pause, step, reset, speed, and rebuild controls.
- Current-state cards for build, release, service health, incident, and security.
- A vertical delivery timeline with source, actor, subject, and event details.
- An SVG relationship graph with typed nodes and directional edges.
- An insight panel showing severity and expandable evidence.
- A JSON context inspector for demonstrating the API an agent would consume.

The dashboard initially fetches snapshots over HTTP and refreshes them whenever
an SSE `perception.updated` notification arrives.

## 10. Error Handling and Operations

SQLite is initialized at application startup. A transaction rollback prevents
partial projections. Playback task failures pause the runner and record an
operational error in playback state. SSE clients receive heartbeats and can
reconnect safely because the browser always refetches authoritative snapshots.

Structured application logs include event ID, scenario ID, cursor, rule ID, and
request ID where applicable.

## 11. Testing

Tests use temporary SQLite databases and real repositories:

- Model validation and deterministic scenario generation.
- Event-by-event projection and idempotency.
- Graph nodes, edges, and bounded impact paths.
- State transitions for all three scenarios.
- Insight thresholds, correlation, evidence, and recovery.
- Playback step, reset, rebuild, and completion semantics.
- FastAPI endpoint contracts and dashboard delivery.

The critical acceptance test replays each scenario twice and asserts that the
timeline, graph, state, and insights are identical.

## 12. Acceptance Criteria

Running one documented command starts the platform. A user can open the
dashboard, select any scenario, control its deterministic playback, see all four
projections evolve in real time, inspect evidence for every derived insight,
reset or rebuild to any valid position, and query the equivalent machine-readable
context through the API. The automated suite passes without external services.
