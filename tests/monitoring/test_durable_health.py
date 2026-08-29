from __future__ import annotations

import sqlite3
from pathlib import Path

from agent.monitoring.durable_health import build_durable_health_snapshot
from gateway.durable_actions import PreparedDurableAction
from gateway.durable_operations import DurableOperationsStore
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
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
                fingerprint="fingerprint-secret",
                idempotency_key="idempotency-secret",
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
