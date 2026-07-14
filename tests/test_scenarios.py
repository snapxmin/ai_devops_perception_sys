from devops_perception.scenarios import get_scenario, list_scenarios


def test_catalog_has_exactly_three_deterministic_scenarios() -> None:
    first = list_scenarios()
    second = list_scenarios()

    assert [scenario.id for scenario in first] == [
        "healthy-delivery",
        "performance-regression",
        "security-gate",
    ]
    assert first == second
    event_ids = [event.id for scenario in first for event in scenario.events]
    assert len(event_ids) == len(set(event_ids))
    assert all(
        tuple(sorted(scenario.events, key=lambda event: (event.occurred_at, event.id)))
        == scenario.events
        for scenario in first
    )
    assert all(
        event.actor
        and event.correlation_id
        and event.causation_id
        and event.trace_id
        for scenario in first
        for event in scenario.events
    )


def test_performance_scenario_is_payment_v24_full_regression_story() -> None:
    scenario = get_scenario("performance-regression")
    assert "Payment" in scenario.description
    assert "v2.4" in scenario.description
    assert [event.type for event in scenario.events] == [
        "deployment.completed",
        "metric.observed",
        "metric.observed",
        "log.observed",
        "incident.created",
        "rollback.completed",
        "metric.observed",
        "metric.observed",
        "service.recovered",
    ]
    assert scenario.events[3].payload["message"] == "Redis timeout"
    assert {event.subject.id for event in scenario.events} >= {
        "payment-service",
        "payment-v2.4",
    }


def test_security_scenario_blocks_then_remediates_and_passes() -> None:
    types = [event.type for event in get_scenario("security-gate").events]
    assert types == [
        "secret.detected",
        "security.gate.blocked",
        "build.blocked",
        "secret.remediated",
        "secret.rescan",
        "security.gate.passed",
        "build.completed",
    ]
