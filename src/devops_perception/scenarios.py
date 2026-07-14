"""Deterministic built-in scenario catalog."""

from __future__ import annotations

from typing import Any

from .models import CanonicalEvent, EntityRef, EventType, Scenario


def _event(
    scenario_id: str,
    sequence: int,
    timestamp: str,
    event_type: EventType,
    subject: EntityRef,
    actor: EntityRef,
    **payload: Any,
) -> CanonicalEvent:
    return CanonicalEvent(
        id=f"{scenario_id}-{sequence:03d}",
        scenario_id=scenario_id,
        occurred_at=timestamp,
        type=event_type,
        source="deterministic-catalog",
        actor=actor,
        subject=subject,
        correlation_id=f"{scenario_id}-correlation",
        causation_id=(
            f"{scenario_id}-trigger"
            if sequence == 1
            else f"{scenario_id}-{sequence - 1:03d}"
        ),
        trace_id=f"{scenario_id}-trace",
        payload=payload,
    )


_PAYMENT = EntityRef(kind="service", id="payment-service", name="Payment")
_DEPLOYMENT_V23 = EntityRef(
    kind="deployment", id="payment-v2.3", name="Payment v2.3"
)
_DEPLOYMENT_V24 = EntityRef(
    kind="deployment", id="payment-v2.4", name="Payment v2.4"
)
_REPOSITORY = EntityRef(kind="repository", id="payment-api", name="Payment API")
_RELEASE_BOT = EntityRef(kind="actor", id="release-bot", name="Release Bot")
_OBSERVABILITY = EntityRef(
    kind="actor", id="observability", name="Observability"
)
_SECURITY_BOT = EntityRef(kind="actor", id="security-bot", name="Security Bot")

_SCENARIOS = (
    Scenario(
        id="healthy-delivery",
        name="Healthy delivery",
        description="Payment v2.3 deploys and remains healthy.",
        events=(
            _event(
                "healthy-delivery",
                1,
                "2026-01-15T09:00:00Z",
                "deployment.started",
                _DEPLOYMENT_V23,
                _RELEASE_BOT,
                service_id=_PAYMENT.id,
                version="v2.3",
            ),
            _event(
                "healthy-delivery",
                2,
                "2026-01-15T09:01:00Z",
                "deployment.completed",
                _DEPLOYMENT_V23,
                _RELEASE_BOT,
                service_id=_PAYMENT.id,
                version="v2.3",
            ),
            _event(
                "healthy-delivery",
                3,
                "2026-01-15T09:02:00Z",
                "metric.observed",
                _PAYMENT,
                _OBSERVABILITY,
                metric="latency_ms",
                value=120,
            ),
            _event(
                "healthy-delivery",
                4,
                "2026-01-15T09:03:00Z",
                "metric.observed",
                _PAYMENT,
                _OBSERVABILITY,
                metric="error_rate",
                value=0.01,
            ),
        ),
    ),
    Scenario(
        id="performance-regression",
        name="Performance regression",
        description="Payment v2.4 causes a Redis regression, incident, and rollback.",
        events=(
            _event(
                "performance-regression",
                1,
                "2026-01-15T10:00:00Z",
                "deployment.completed",
                _DEPLOYMENT_V24,
                _RELEASE_BOT,
                service_id=_PAYMENT.id,
                version="v2.4",
            ),
            _event(
                "performance-regression",
                2,
                "2026-01-15T10:01:00Z",
                "metric.observed",
                _PAYMENT,
                _OBSERVABILITY,
                metric="latency_ms",
                value=780,
            ),
            _event(
                "performance-regression",
                3,
                "2026-01-15T10:02:00Z",
                "metric.observed",
                _PAYMENT,
                _OBSERVABILITY,
                metric="error_rate",
                value=0.12,
            ),
            _event(
                "performance-regression",
                4,
                "2026-01-15T10:03:00Z",
                "log.observed",
                _PAYMENT,
                _OBSERVABILITY,
                level="error",
                message="Redis timeout",
            ),
            _event(
                "performance-regression",
                5,
                "2026-01-15T10:04:00Z",
                "incident.created",
                _PAYMENT,
                _OBSERVABILITY,
                incident_id="inc-2026-001",
            ),
            _event(
                "performance-regression",
                6,
                "2026-01-15T10:05:00Z",
                "rollback.completed",
                _DEPLOYMENT_V24,
                _RELEASE_BOT,
                service_id=_PAYMENT.id,
                target_version="v2.3",
            ),
            _event(
                "performance-regression",
                7,
                "2026-01-15T10:06:00Z",
                "metric.observed",
                _PAYMENT,
                _OBSERVABILITY,
                metric="latency_ms",
                value=135,
            ),
            _event(
                "performance-regression",
                8,
                "2026-01-15T10:07:00Z",
                "metric.observed",
                _PAYMENT,
                _OBSERVABILITY,
                metric="error_rate",
                value=0.01,
            ),
            _event(
                "performance-regression",
                9,
                "2026-01-15T10:08:00Z",
                "service.recovered",
                _PAYMENT,
                _OBSERVABILITY,
                latency_ms=135,
                error_rate=0.01,
            ),
        ),
    ),
    Scenario(
        id="security-gate",
        name="Security gate",
        description="A secret blocks Payment until remediation and a passing rescan.",
        events=(
            _event(
                "security-gate",
                1,
                "2026-01-15T11:00:00Z",
                "secret.detected",
                _REPOSITORY,
                _SECURITY_BOT,
                detector="credential-scanner",
                location="config.py:12",
            ),
            _event(
                "security-gate",
                2,
                "2026-01-15T11:01:00Z",
                "security.gate.blocked",
                _REPOSITORY,
                _SECURITY_BOT,
                reason="secret detected",
            ),
            _event(
                "security-gate",
                3,
                "2026-01-15T11:02:00Z",
                "build.blocked",
                _REPOSITORY,
                _RELEASE_BOT,
                reason="secret detected",
            ),
            _event(
                "security-gate",
                4,
                "2026-01-15T11:03:00Z",
                "secret.remediated",
                _REPOSITORY,
                _SECURITY_BOT,
                remediation="credential removed and rotated",
            ),
            _event(
                "security-gate",
                5,
                "2026-01-15T11:04:00Z",
                "secret.rescan",
                _REPOSITORY,
                _SECURITY_BOT,
                findings=0,
            ),
            _event(
                "security-gate",
                6,
                "2026-01-15T11:05:00Z",
                "security.gate.passed",
                _REPOSITORY,
                _SECURITY_BOT,
                findings=0,
            ),
            _event(
                "security-gate",
                7,
                "2026-01-15T11:06:00Z",
                "build.completed",
                _REPOSITORY,
                _RELEASE_BOT,
                result="passed",
            ),
        ),
    ),
)

_BY_ID = {scenario.id: scenario for scenario in _SCENARIOS}


def list_scenarios() -> tuple[Scenario, ...]:
    return _SCENARIOS


def get_scenario(scenario_id: str) -> Scenario:
    try:
        return _BY_ID[scenario_id]
    except KeyError as error:
        raise KeyError(f"unknown scenario: {scenario_id}") from error
