"""SQLite persistence for deterministic perception state."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from threading import RLock
from typing import Any, Iterator

from .models import CanonicalEvent, Insight, Scenario


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _synchronized(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return locked


class SQLiteStore:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._lock = RLock()
        self.connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self._transaction_depth = 0
        self._savepoint_sequence = 0
        self._create_schema()

    @_synchronized
    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS scenario_events (
                scenario_id TEXT NOT NULL,
                event_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL,
                event_json TEXT NOT NULL,
                UNIQUE (scenario_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS timeline (
                scenario_id TEXT NOT NULL,
                event_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL,
                event_json TEXT NOT NULL,
                UNIQUE (scenario_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY,
                scenario_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS graph_nodes (
                node_id TEXT NOT NULL,
                scenario_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                data_json TEXT NOT NULL,
                event_id TEXT,
                PRIMARY KEY (scenario_id, node_id)
            );
            CREATE TABLE IF NOT EXISTS graph_edges (
                edge_id TEXT NOT NULL,
                scenario_id TEXT NOT NULL,
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relation TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY (scenario_id, edge_id)
            );
            CREATE TABLE IF NOT EXISTS state (
                scenario_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value_json TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY (scenario_id, entity_id, key)
            );
            CREATE TABLE IF NOT EXISTS insights (
                insight_id TEXT PRIMARY KEY,
                scenario_id TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                insight_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS insight_evidence (
                insight_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY (insight_id, event_id),
                FOREIGN KEY (insight_id) REFERENCES insights(insight_id)
                    ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS playback (
                scenario_id TEXT PRIMARY KEY,
                cursor INTEGER NOT NULL,
                status TEXT NOT NULL,
                speed REAL NOT NULL,
                current_time TEXT,
                operational_error TEXT
            );
            """
        )

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self._lock:
            outermost = self._transaction_depth == 0
            savepoint: str | None = None
            if outermost:
                self.connection.execute("BEGIN IMMEDIATE")
            else:
                self._savepoint_sequence += 1
                savepoint = f"perception_{self._savepoint_sequence}"
                self.connection.execute(f"SAVEPOINT {savepoint}")
            self._transaction_depth += 1
            try:
                yield
            except BaseException:
                if outermost:
                    self.connection.execute("ROLLBACK")
                else:
                    self.connection.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self.connection.execute(f"RELEASE SAVEPOINT {savepoint}")
                raise
            else:
                if outermost:
                    self.connection.execute("COMMIT")
                else:
                    self.connection.execute(f"RELEASE SAVEPOINT {savepoint}")
            finally:
                self._transaction_depth -= 1

    @_synchronized
    def close(self) -> None:
        if self.connection:
            self.connection.close()

    @_synchronized
    def load_scenario(self, scenario: Scenario) -> None:
        if any(event.scenario_id != scenario.id for event in scenario.events):
            raise ValueError("every event must belong to the loaded scenario")
        with self.transaction():
            existing = tuple(
                row[0]
                for row in self.connection.execute(
                    """
                    SELECT event_json FROM scenario_events
                    WHERE scenario_id = ? ORDER BY sequence
                    """,
                    (scenario.id,),
                )
            )
            incoming = tuple(event.model_dump_json() for event in scenario.events)
            if existing and existing != incoming:
                self.reset(scenario.id)
                self.connection.execute(
                    "DELETE FROM scenario_events WHERE scenario_id = ?",
                    (scenario.id,),
                )
            for sequence, (event, event_json) in enumerate(
                zip(scenario.events, incoming, strict=True)
            ):
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO scenario_events
                        (scenario_id, event_id, sequence, event_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (scenario.id, event.id, sequence, event_json),
                )
            self.connection.execute(
                """
                INSERT OR IGNORE INTO playback
                    (scenario_id, cursor, status, speed, current_time,
                     operational_error)
                VALUES (?, 0, 'paused', 1.0, NULL, NULL)
                """,
                (scenario.id,),
            )

    @_synchronized
    def list_scenario_events(self, scenario_id: str) -> tuple[CanonicalEvent, ...]:
        rows = self.connection.execute(
            """
            SELECT event_json FROM scenario_events
            WHERE scenario_id = ? ORDER BY sequence
            """,
            (scenario_id,),
        )
        return tuple(CanonicalEvent.model_validate_json(row[0]) for row in rows)

    @_synchronized
    def append_timeline(self, event: CanonicalEvent) -> None:
        sequence = self.connection.execute(
            "SELECT COUNT(*) FROM timeline WHERE scenario_id = ?",
            (event.scenario_id,),
        ).fetchone()[0]
        self.connection.execute(
            """
            INSERT OR IGNORE INTO timeline
                (scenario_id, event_id, sequence, event_json)
            VALUES (?, ?, ?, ?)
            """,
            (event.scenario_id, event.id, sequence, event.model_dump_json()),
        )

    @_synchronized
    def list_timeline(self, scenario_id: str) -> tuple[CanonicalEvent, ...]:
        rows = self.connection.execute(
            "SELECT event_json FROM timeline WHERE scenario_id = ? ORDER BY sequence",
            (scenario_id,),
        )
        return tuple(CanonicalEvent.model_validate_json(row[0]) for row in rows)

    @_synchronized
    def mark_processed(self, event_id: str, scenario_id: str | None = None) -> None:
        scenario_id = scenario_id or self._scenario_for_event(event_id)
        self.connection.execute(
            """
            INSERT OR IGNORE INTO processed_events (event_id, scenario_id)
            VALUES (?, ?)
            """,
            (event_id, scenario_id),
        )

    @_synchronized
    def is_processed(self, event_id: str) -> bool:
        row = self.connection.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return row is not None

    @_synchronized
    def _scenario_for_event(self, event_id: str) -> str:
        row = self.connection.execute(
            """
            SELECT scenario_id FROM timeline WHERE event_id = ?
            UNION ALL
            SELECT scenario_id FROM scenario_events WHERE event_id = ?
            LIMIT 1
            """,
            (event_id, event_id),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown event: {event_id}")
        return str(row[0])

    @_synchronized
    def upsert_node(
        self,
        node_id: str,
        kind: str,
        data: dict[str, Any],
        event_id: str | None = None,
        scenario_id: str | None = None,
    ) -> None:
        scenario_id = scenario_id or (
            self._scenario_for_event(event_id) if event_id else self._only_scenario_id()
        )
        self.connection.execute(
            """
            INSERT INTO graph_nodes
                (node_id, scenario_id, kind, data_json, event_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(scenario_id, node_id) DO UPDATE SET
                kind=excluded.kind,
                data_json=excluded.data_json,
                event_id=COALESCE(excluded.event_id, graph_nodes.event_id)
            """,
            (node_id, scenario_id, kind, _json(data), event_id),
        )

    @_synchronized
    def _only_scenario_id(self) -> str:
        rows = self.connection.execute(
            "SELECT DISTINCT scenario_id FROM scenario_events"
        ).fetchall()
        if len(rows) != 1:
            raise ValueError("scenario_id or event_id is required")
        return str(rows[0][0])

    @_synchronized
    def upsert_edge(
        self,
        source_id: str,
        target_id: str,
        relation: str,
        event_id: str,
    ) -> None:
        scenario_id = self._scenario_for_event(event_id)
        edge_id = f"{source_id}|{relation}|{target_id}"
        self.connection.execute(
            """
            INSERT INTO graph_edges
                (edge_id, scenario_id, source_id, target_id, relation, event_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(scenario_id, edge_id) DO UPDATE SET event_id=excluded.event_id
            """,
            (edge_id, scenario_id, source_id, target_id, relation, event_id),
        )

    @_synchronized
    def set_state(
        self,
        entity_id: str,
        key: str,
        value: Any,
        event_id: str,
    ) -> None:
        scenario_id = self._scenario_for_event(event_id)
        self.connection.execute(
            """
            INSERT INTO state
                (scenario_id, entity_id, key, value_json, event_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(scenario_id, entity_id, key) DO UPDATE SET
                value_json=excluded.value_json,
                event_id=excluded.event_id
            """,
            (scenario_id, entity_id, key, _json(value), event_id),
        )

    @_synchronized
    def get_state(
        self, scenario_id: str, entity_id: str, key: str, default: Any = None
    ) -> Any:
        row = self.connection.execute(
            """
            SELECT value_json FROM state
            WHERE scenario_id = ? AND entity_id = ? AND key = ?
            """,
            (scenario_id, entity_id, key),
        ).fetchone()
        return default if row is None else json.loads(row[0])

    @_synchronized
    def save_insight(self, insight: Insight) -> None:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO insights
                (insight_id, scenario_id, rule_id, occurred_at, insight_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                insight.id,
                insight.scenario_id,
                insight.rule_id,
                insight.occurred_at.isoformat(),
                insight.model_dump_json(),
            ),
        )
        for event_id in insight.evidence_event_ids:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO insight_evidence (insight_id, event_id)
                VALUES (?, ?)
                """,
                (insight.id, event_id),
            )

    @_synchronized
    def list_insights(self, scenario_id: str) -> tuple[Insight, ...]:
        rows = self.connection.execute(
            """
            SELECT insight_json FROM insights
            WHERE scenario_id = ? ORDER BY occurred_at, insight_id
            """,
            (scenario_id,),
        )
        return tuple(Insight.model_validate_json(row[0]) for row in rows)

    @_synchronized
    def save_playback(
        self,
        scenario_id: str,
        cursor: int,
        status: str,
        speed: float,
        current_time: str | None = None,
        operational_error: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO playback
                (scenario_id, cursor, status, speed, current_time,
                 operational_error)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(scenario_id) DO UPDATE SET
                cursor=excluded.cursor,
                status=excluded.status,
                speed=excluded.speed,
                current_time=excluded.current_time,
                operational_error=excluded.operational_error
            """,
            (
                scenario_id,
                cursor,
                status,
                speed,
                current_time,
                operational_error,
            ),
        )

    @_synchronized
    def get_playback(self, scenario_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM playback WHERE scenario_id = ?", (scenario_id,)
        ).fetchone()
        return None if row is None else dict(row)

    @_synchronized
    def set_playback_error(self, scenario_id: str, message: str) -> None:
        cursor = self.connection.execute(
            """
            UPDATE playback
            SET status = 'error', operational_error = ?
            WHERE scenario_id = ?
            """,
            (message, scenario_id),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"playback state is missing: {scenario_id}")

    @_synchronized
    def reset(self, scenario_id: str) -> None:
        with self.transaction():
            for table in (
                "insights",
                "state",
                "graph_edges",
                "graph_nodes",
                "processed_events",
                "timeline",
            ):
                self.connection.execute(
                    f"DELETE FROM {table} WHERE scenario_id = ?", (scenario_id,)
                )
            self.connection.execute(
                """
                INSERT INTO playback
                    (scenario_id, cursor, status, speed, current_time,
                     operational_error)
                VALUES (?, 0, 'paused', 1.0, NULL, NULL)
                ON CONFLICT(scenario_id) DO UPDATE SET
                    cursor=0, status='paused', current_time=NULL,
                    operational_error=NULL
                """,
                (scenario_id,),
            )

    @_synchronized
    def bounded_impact(
        self, scenario_id: str, node_id: str, depth: int
    ) -> dict[str, tuple[Any, ...]]:
        if depth < 0:
            raise ValueError("depth must be non-negative")
        nodes = {node_id}
        edges: set[tuple[str, str, str]] = set()
        paths: set[tuple[str, ...]] = {(node_id,)}
        frontier = {(node_id, (node_id,))}
        for _ in range(depth):
            next_frontier: set[tuple[str, tuple[str, ...]]] = set()
            for current, path in frontier:
                rows = self.connection.execute(
                    """
                    SELECT source_id, target_id, relation FROM graph_edges
                    WHERE scenario_id = ?
                      AND (source_id = ? OR target_id = ?)
                    ORDER BY source_id, target_id, relation
                    """,
                    (scenario_id, current, current),
                )
                for source, target, relation in rows:
                    edge = (str(source), str(target), str(relation))
                    edges.add(edge)
                    neighbor = str(target) if source == current else str(source)
                    nodes.add(neighbor)
                    if neighbor not in path:
                        new_path = (*path, neighbor)
                        paths.add(new_path)
                        next_frontier.add((neighbor, new_path))
            frontier = next_frontier
        return {
            "nodes": tuple(sorted(nodes)),
            "edges": tuple(sorted(edges)),
            "paths": tuple(sorted(paths)),
        }

    @_synchronized
    def snapshot(self, scenario_id: str) -> dict[str, Any]:
        def rows(query: str) -> tuple[dict[str, Any], ...]:
            return tuple(dict(row) for row in self.connection.execute(query, (scenario_id,)))

        timeline = tuple(
            event.model_dump(mode="json") for event in self.list_timeline(scenario_id)
        )
        processed = tuple(
            row[0]
            for row in self.connection.execute(
                """
                SELECT event_id FROM processed_events
                WHERE scenario_id = ? ORDER BY event_id
                """,
                (scenario_id,),
            )
        )
        nodes = rows(
            """
            SELECT node_id, kind, data_json, event_id FROM graph_nodes
            WHERE scenario_id = ? ORDER BY node_id
            """
        )
        edges = rows(
            """
            SELECT source_id, target_id, relation, event_id FROM graph_edges
            WHERE scenario_id = ? ORDER BY edge_id
            """
        )
        state = rows(
            """
            SELECT entity_id, key, value_json, event_id FROM state
            WHERE scenario_id = ? ORDER BY entity_id, key
            """
        )
        insights = tuple(
            insight.model_dump(mode="json") for insight in self.list_insights(scenario_id)
        )
        return {
            "timeline": timeline,
            "processed_events": processed,
            "nodes": nodes,
            "edges": edges,
            "state": state,
            "insights": insights,
            "playback": self.get_playback(scenario_id),
        }
