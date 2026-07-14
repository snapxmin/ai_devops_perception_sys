"""Deterministic event-to-perception engine."""

from __future__ import annotations

import math
from typing import Any

from .models import CanonicalEvent, EntityRef, Insight
from .store import SQLiteStore


class PerceptionEngine:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def process(self, event: CanonicalEvent) -> bool:
        """Project one event atomically into timeline, graph, state, and insights."""
        self._validate_measurements(event)
        with self.store.transaction():
            if self.store.is_processed(event.id):
                return False
            self.store.append_timeline(event)
            self._map_graph_and_state(event)
            for insight in self._evaluate(event):
                self.store.save_insight(insight)
            self.store.mark_processed(event.id, event.scenario_id)
        return True

    @staticmethod
    def _validate_measurements(event: CanonicalEvent) -> None:
        measurements: tuple[tuple[str, Any], ...] = ()
        if event.type == "metric.observed":
            measurements = (
                (str(event.payload.get("metric")), event.payload.get("value")),
            )
        elif event.type == "service.recovered":
            measurements = (
                ("latency_ms", event.payload.get("latency_ms")),
                ("error_rate", event.payload.get("error_rate")),
            )
        for metric, value in measurements:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{metric} must be a number, not bool")
            numeric = float(value)
            if not math.isfinite(numeric):
                raise ValueError(f"{metric} must be finite")
            if metric == "latency_ms" and numeric < 0:
                raise ValueError("latency_ms must be non-negative")
            if metric == "error_rate" and not 0 <= numeric <= 1:
                raise ValueError("error_rate must be between 0 and 1")

    def rebuild(self, scenario_id: str) -> dict[str, Any]:
        events = self.store.list_scenario_events(scenario_id)
        if not events:
            raise KeyError(f"scenario is not loaded: {scenario_id}")
        with self.store.transaction():
            self.store.reset(scenario_id)
            for event in events:
                self.process(event)
        return self.store.snapshot(scenario_id)

    @staticmethod
    def _node_id(entity: EntityRef) -> str:
        return f"{entity.kind}:{entity.id}"

    @staticmethod
    def _service_node(service_id: str) -> str:
        return f"service:{service_id}"

    def _map_graph_and_state(self, event: CanonicalEvent) -> None:
        subject_node = self._node_id(event.subject)
        actor_node = self._node_id(event.actor)
        event_node = f"event:{event.id}"
        self.store.upsert_node(
            subject_node,
            event.subject.kind,
            {"id": event.subject.id, "name": event.subject.name},
            event.id,
            event.scenario_id,
        )
        self.store.upsert_node(
            actor_node,
            event.actor.kind,
            {"id": event.actor.id, "name": event.actor.name},
            event.id,
            event.scenario_id,
        )
        self.store.upsert_node(
            event_node,
            "event",
            {"id": event.id, "type": event.type},
            event.id,
            event.scenario_id,
        )
        self.store.upsert_edge(actor_node, event_node, "performed", event.id)
        self.store.upsert_edge(event_node, subject_node, "concerns", event.id)
        self.store.set_state(subject_node, "last_event_type", event.type, event.id)

        service_id = event.payload.get("service_id")
        service_node = (
            self._service_node(service_id) if isinstance(service_id, str) else None
        )
        if service_node:
            self.store.upsert_node(
                service_node,
                "service",
                {"id": service_id},
                event.id,
                event.scenario_id,
            )
            self.store.upsert_edge(subject_node, service_node, "deploys", event.id)

        if event.type.startswith("deployment."):
            self.store.set_state(
                subject_node,
                "status",
                event.type.removeprefix("deployment."),
                event.id,
            )
            if service_node and event.type == "deployment.completed":
                self.store.set_state(
                    service_node, "last_deployment_event", event.id, event.id
                )
                self.store.set_state(service_node, "status", "deployed", event.id)
                version = event.payload.get("version")
                if isinstance(version, str):
                    self.store.set_state(
                        service_node, "current_version", version, event.id
                    )

        elif event.type == "metric.observed":
            metric = str(event.payload.get("metric", "unknown"))
            value = event.payload.get("value")
            self.store.set_state(subject_node, f"metric:{metric}", value, event.id)
            rollback_id = self.store.get_state(
                event.scenario_id, subject_node, "last_rollback_event"
            )
            if rollback_id:
                self.store.set_state(
                    subject_node,
                    f"healthy:{metric}:event",
                    event.id if self._is_healthy_metric(metric, value) else None,
                    event.id,
                )

        elif event.type == "log.observed":
            self.store.set_state(
                subject_node, "last_log_message", event.payload.get("message"), event.id
            )

        elif event.type == "incident.created":
            incident_id = str(event.payload.get("incident_id", event.id))
            incident_node = f"incident:{incident_id}"
            self.store.upsert_node(
                incident_node,
                "incident",
                {"id": incident_id, "status": "open"},
                event.id,
                event.scenario_id,
            )
            self.store.upsert_edge(incident_node, subject_node, "affects", event.id)
            self.store.set_state(subject_node, "incident_status", "open", event.id)
            self.store.set_state(
                subject_node, "active_incident_node", incident_node, event.id
            )
            for evidence_id in self._incident_evidence_ids(event):
                self.store.upsert_edge(
                    f"event:{evidence_id}", incident_node, "evidence_for", event.id
                )

        elif event.type == "secret.detected":
            finding_node = f"finding:{event.id}"
            self.store.upsert_node(
                finding_node,
                "security_finding",
                dict(event.payload),
                event.id,
                event.scenario_id,
            )
            self.store.upsert_edge(finding_node, subject_node, "found_in", event.id)
            self.store.set_state(subject_node, "secret_status", "detected", event.id)

        elif event.type in {"security.gate.blocked", "build.blocked"}:
            key = "gate_status" if event.type.startswith("security.") else "build_status"
            self.store.set_state(subject_node, key, "blocked", event.id)

        elif event.type == "secret.remediated":
            self.store.set_state(subject_node, "secret_status", "remediated", event.id)

        elif event.type == "secret.rescan":
            self.store.set_state(
                subject_node, "rescan_findings", event.payload.get("findings"), event.id
            )

        elif event.type in {"security.gate.passed", "build.completed"}:
            key = "gate_status" if event.type.startswith("security.") else "build_status"
            self.store.set_state(subject_node, key, "passed", event.id)

        elif event.type == "rollback.completed" and service_node:
            self.store.set_state(
                service_node, "last_rollback_event", event.id, event.id
            )
            target_version = event.payload.get("target_version")
            if isinstance(target_version, str):
                self.store.set_state(
                    service_node, "current_version", target_version, event.id
                )
            self.store.set_state(
                service_node, "healthy:latency_ms:event", None, event.id
            )
            self.store.set_state(
                service_node, "healthy:error_rate:event", None, event.id
            )
            self.store.set_state(service_node, "status", "rolled_back", event.id)

        elif event.type == "service.recovered":
            if self._healthy_recovery_evidence(event):
                self.store.set_state(subject_node, "status", "healthy", event.id)
                self.store.set_state(
                    subject_node, "incident_status", "closed", event.id
                )
                incident_node = self.store.get_state(
                    event.scenario_id, subject_node, "active_incident_node"
                )
                if incident_node:
                    self.store.set_state(
                        str(incident_node), "status", "closed", event.id
                    )

    def _evaluate(self, event: CanonicalEvent) -> tuple[Insight, ...]:
        insights: list[Insight] = []
        if event.type == "metric.observed":
            metric = event.payload.get("metric")
            value = event.payload.get("value")
            if metric == "latency_ms" and isinstance(value, (int, float)) and value > 500:
                insights.append(
                    self._insight(
                        event,
                        "high-latency",
                        "High latency detected",
                        f"Observed latency {value:g}ms exceeds the 500ms threshold.",
                        "high",
                        (event.id,),
                    )
                )
                insights.extend(self._deployment_regression(event, "latency"))
            if metric == "error_rate" and isinstance(value, (int, float)):
                if value > 0.05:
                    insights.append(
                        self._insight(
                            event,
                            "high-error-rate",
                            "High error rate detected",
                            f"Observed error rate {value:.1%} exceeds the 5% threshold.",
                            "high",
                            (event.id,),
                        )
                    )
                    insights.extend(self._deployment_regression(event, "errors"))

        elif event.type == "incident.created":
            evidence = self._incident_evidence_ids(event)
            insights.append(
                self._insight(
                    event,
                    "incident-evidence",
                    "Incident supported by correlated evidence",
                    "Deployment, metrics, and logs explain the incident.",
                    "critical",
                    evidence,
                )
            )

        elif event.type == "secret.detected":
            insights.append(
                self._insight(
                    event,
                    "secret-gate",
                    "Secret detected",
                    "A credential finding requires the security gate to block delivery.",
                    "critical",
                    (event.id,),
                )
            )

        elif event.type == "service.recovered":
            evidence = self._healthy_recovery_evidence(event)
            if evidence:
                insights.append(
                    self._insight(
                        event,
                        "rollback-recovery",
                        "Service recovered after rollback",
                        "Healthy latency and error-rate observations confirm recovery.",
                        "info",
                        (*evidence, event.id),
                        status="resolved",
                    )
                )
        return tuple(insights)

    def _incident_evidence_ids(self, event: CanonicalEvent) -> tuple[str, ...]:
        relevant = {
            "deployment.completed",
            "metric.observed",
            "log.observed",
            "incident.created",
        }
        return tuple(
            item.id
            for item in self.store.list_timeline(event.scenario_id)
            if item.correlation_id == event.correlation_id and item.type in relevant
        )

    def _healthy_recovery_evidence(
        self, event: CanonicalEvent
    ) -> tuple[str, str, str] | tuple[()]:
        service_node = self._node_id(event.subject)
        rollback = self.store.get_state(
            event.scenario_id, service_node, "last_rollback_event"
        )
        latency = self.store.get_state(
            event.scenario_id, service_node, "healthy:latency_ms:event"
        )
        errors = self.store.get_state(
            event.scenario_id, service_node, "healthy:error_rate:event"
        )
        payload_latency = event.payload.get("latency_ms")
        payload_errors = event.payload.get("error_rate")
        payload_is_healthy = self._is_healthy_metric(
            "latency_ms", payload_latency
        ) and self._is_healthy_metric("error_rate", payload_errors)
        if rollback and latency and errors and payload_is_healthy:
            return str(rollback), str(latency), str(errors)
        return ()

    @staticmethod
    def _is_healthy_metric(metric: str, value: Any) -> bool:
        if not isinstance(value, (int, float)):
            return False
        if metric == "latency_ms":
            return value <= 500
        if metric == "error_rate":
            return value <= 0.05
        return False

    def _deployment_regression(
        self, event: CanonicalEvent, symptom: str
    ) -> tuple[Insight, ...]:
        deployment_id = self.store.get_state(
            event.scenario_id,
            self._node_id(event.subject),
            "last_deployment_event",
        )
        if not deployment_id:
            return ()
        return (
            self._insight(
                event,
                "deployment-correlated-regression",
                "Regression correlated with deployment",
                f"Elevated {symptom} followed the most recent deployment.",
                "high",
                (str(deployment_id), event.id),
            ),
        )

    @staticmethod
    def _insight(
        event: CanonicalEvent,
        rule_id: str,
        title: str,
        summary: str,
        severity: str,
        evidence: tuple[str, ...],
        *,
        status: str = "active",
    ) -> Insight:
        return Insight(
            id=f"{event.scenario_id}:{rule_id}:{event.id}",
            scenario_id=event.scenario_id,
            rule_id=rule_id,
            status=status,
            title=title,
            summary=summary,
            severity=severity,
            occurred_at=event.occurred_at,
            affected=event.subject,
            evidence_event_ids=evidence,
        )
