from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from agent.monitoring.durable_health import build_durable_health_snapshot
from gateway.durable_actions import PreparedDurableAction
from gateway.durable_operations import DurableOperationsStore
from gateway.durable_operations import (
    _DURABLE_EVENT_COUNTS,
    record_durable_command_response,
    record_durable_delivery_event,
)
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
    DurableResolveStatus,
)


def _create(store: DurableRequestStore, reference: str, *, action=False):
    spec = DurableRequestSpec(
        kind="PHASE4D-CANARY-kind",
        title="PHASE4D-CANARY-title",
        body="PHASE4D-CANARY-body",
        options=(DurableRequestOption("approve", "yes"), DurableRequestOption("reject", "no")),
        ttl_seconds=1_000,
        reference=reference,
    )
    kwargs = dict(
        target=DurableRequestTarget("discord", "actor-secret", "thread-secret"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id=f"actor-{reference}",
    )
    if action:
        return store.create_with_action(
            spec,
            action=PreparedDurableAction(
                action_type="cron.resume",
                action_version=1,
                payload_json='{"canary":"PHASE4D-CANARY"}',
                fingerprint=f"fingerprint-secret-{reference}",
                idempotency_key=f"idempotency-secret-{reference}",
            ),
            origin_session_id="session-secret",
            **kwargs,
        )
    return store.create(spec, **kwargs)


def seeded_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 100.0)
    pending = _create(store, "pending").request.request_id
    unknown = _create(store, "unknown", action=True).request.request_id
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_requests SET created_at = 100, updated_at = 100, "
            "delivery_claim = 'claim-secret', delivery_claimed_at = 100 "
            "WHERE request_id = ?",
            (pending,),
        )
        db.execute(
            "UPDATE durable_requests SET status = 'selected', resolved_at = 200, "
            "updated_at = 200 WHERE request_id = ?",
            (unknown,),
        )
        db.execute(
            "UPDATE durable_action_tickets SET status = 'unknown', created_at = 200, "
            "completed_at = 250, notification_status = 'pending', updated_at = 250 "
            "WHERE request_id = ?",
            (unknown,),
        )
    DurableOperationsStore(db_path).reconcile_action(
        unknown, outcome="abandoned", operator_identity="actor-operator", note="PHASE4D-CANARY-note", now=300
    )
    return db_path


def _values(snapshot, name):
    return [
        (metric.value, metric.attributes)
        for metric in snapshot.metrics
        if metric.name == name
    ]


def test_snapshot_contains_authoritative_counts_and_ages(tmp_path: Path) -> None:
    snapshot = build_durable_health_snapshot(db_path=seeded_db(tmp_path), now=500.0)

    assert _values(snapshot, "hermes_durable_requests") == [
        (1, {"platform": "discord", "status": "pending_delivery"}),
        (1, {"platform": "discord", "status": "selected"}),
    ]
    assert _values(snapshot, "hermes_durable_actions") == [
        (1, {"action_type": "cron.resume", "status": "unknown"})
    ]
    assert _values(snapshot, "hermes_durable_notifications") == [
        (1, {"origin_type": "session", "status": "pending"})
    ]
    assert _values(snapshot, "hermes_durable_stale_claims") == [
        (1, {"claim_type": "delivery"})
    ]
    assert _values(snapshot, "hermes_durable_unknown_actions") == [
        (1, {"action_type": "cron.resume", "reconciled": "true"})
    ]
    assert _values(snapshot, "hermes_durable_reconciliations") == [
        (1, {"action_type": "cron.resume", "outcome": "abandoned"})
    ]
    assert (400.0, {"queue_type": "delivery"}) in _values(
        snapshot, "hermes_durable_oldest_backlog_seconds"
    )
    assert (250.0, {"queue_type": "notification"}) in _values(
        snapshot, "hermes_durable_oldest_backlog_seconds"
    )


def test_snapshot_contains_no_high_cardinality_or_content_labels(tmp_path: Path) -> None:
    snapshot = build_durable_health_snapshot(db_path=seeded_db(tmp_path), now=500.0)
    rendered = " ".join(str(metric.attributes) for metric in snapshot.metrics)
    for canary in (
        "drq_",
        "PHASE4D-CANARY",
        "actor-",
        "fingerprint-",
        "idempotency-",
    ):
        assert canary not in rendered


def test_snapshot_contains_bounded_command_and_delivery_counters(tmp_path: Path) -> None:
    _DURABLE_EVENT_COUNTS.clear()
    record_durable_command_response(DurableResolveStatus.RESOLVED)
    record_durable_command_response(DurableResolveStatus.WRONG_CHANNEL)
    record_durable_delivery_event("native_to_text_fallback")
    record_durable_delivery_event("text_delivery_failed")
    record_durable_delivery_event("text_delivery_unknown")

    snapshot = build_durable_health_snapshot(db_path=seeded_db(tmp_path), now=500.0)

    assert _values(snapshot, "hermes_durable_command_responses") == [
        (1, {"result": "resolved"}),
        (1, {"result": "wrong_channel"}),
    ]
    assert _values(snapshot, "hermes_durable_delivery_events") == [
        (1, {"event": "native_to_text_fallback"}),
        (1, {"event": "text_delivery_failed"}),
        (1, {"event": "text_delivery_unknown"}),
    ]
    rendered = repr(snapshot.metrics)
    for secret in ("drq_", "actor-", "channel-", "PHASE4D-CANARY"):
        assert secret not in rendered
    _DURABLE_EVENT_COUNTS.clear()


@pytest.mark.parametrize("count", [0, -1, True, 1.5, 1_000_001])
def test_delivery_counter_rejects_invalid_counts(count: object) -> None:
    _DURABLE_EVENT_COUNTS.clear()

    record_durable_delivery_event("text_delivery_unknown", count=count)  # type: ignore[arg-type]

    assert _DURABLE_EVENT_COUNTS == {}


def test_snapshot_reports_text_command_delivery_backlog_without_receipts(
    tmp_path: Path,
) -> None:
    db_path = seeded_db(tmp_path)
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_requests SET delivery_mode = 'text_command' "
            "WHERE status = 'pending_delivery'"
        )

    snapshot = build_durable_health_snapshot(db_path=db_path, now=500.0)

    assert (400.0, {"queue_type": "text_command_delivery"}) in _values(
        snapshot, "hermes_durable_oldest_backlog_seconds"
    )
    assert "drq_" not in repr(snapshot.metrics)


def test_snapshot_normalizes_malformed_enum_labels(tmp_path: Path) -> None:
    db_path = seeded_db(tmp_path)
    store = DurableRequestStore(db_path, wall_clock=lambda: 100.0)
    second = _create(store, "second-malformed", action=True).request.request_id
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_requests SET status = 'selected', resolved_at = 200 "
            "WHERE request_id = ?",
            (second,),
        )
        db.execute(
            "UPDATE durable_action_tickets SET status = 'unknown', completed_at = 250 "
            "WHERE request_id = ?",
            (second,),
        )
    DurableOperationsStore(db_path).reconcile_action(
        second, outcome="abandoned", operator_identity="operator", now=300.0
    )
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_action_tickets SET notification_status = "
            "'PHASE4D-CANARY-notification-' || request_id"
        )
        db.execute(
            "UPDATE durable_action_reconciliations SET outcome = "
            "'PHASE4D-CANARY-outcome-' || request_id"
        )

    snapshot = build_durable_health_snapshot(db_path=db_path, now=500.0)

    assert _values(snapshot, "hermes_durable_notifications") == [
        (2, {"origin_type": "session", "status": "unknown"})
    ]
    assert _values(snapshot, "hermes_durable_reconciliations") == [
        (2, {"action_type": "cron.resume", "outcome": "unknown"})
    ]


def test_snapshot_uses_read_only_query_only_connection(tmp_path: Path, monkeypatch) -> None:
    db_path = seeded_db(tmp_path)
    original = DurableOperationsStore._read_connection
    observed = []

    def checked(self):
        conn = original(self)
        observed.append(conn.execute("PRAGMA query_only").fetchone()[0])
        return conn

    monkeypatch.setattr(DurableOperationsStore, "_read_connection", checked)

    build_durable_health_snapshot(db_path=db_path, now=500.0)

    assert observed == [1]
