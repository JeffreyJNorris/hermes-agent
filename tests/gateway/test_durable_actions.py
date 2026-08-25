from __future__ import annotations

import sqlite3
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from gateway.durable_requests import DurableRequestStore
from hermes_state_common import SCHEMA_SQL


_PHASE4B_DURABLE_REQUEST_SCHEMA_SQL = """
CREATE TABLE durable_requests (
    request_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    options_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    platform TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    responder_mode TEXT NOT NULL,
    origin_type TEXT NOT NULL,
    origin_id TEXT NOT NULL,
    reference TEXT,
    resolved_at REAL,
    delivery_claimed_at REAL
);
"""


EXPECTED_COLUMNS = {
    "request_id",
    "action_type",
    "action_version",
    "payload_json",
    "fingerprint",
    "idempotency_key",
    "status",
    "execution_claim",
    "execution_claimed_at",
    "execution_started_at",
    "outcome_reason",
    "outcome_json",
    "completed_at",
    "notification_status",
    "notification_claim",
    "notification_claimed_at",
    "notification_completed_at",
    "created_at",
    "updated_at",
}


def _schema_details(
    db_path: Path,
) -> tuple[dict[str, sqlite3.Row], dict[str, list[str]]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        columns = {
            row["name"]: row
            for row in conn.execute("PRAGMA table_info(durable_action_tickets)")
        }
        indexes = {
            row["name"]: [
                info["name"]
                for info in conn.execute(f'PRAGMA index_info("{row["name"]}")')
            ]
            for row in conn.execute("PRAGMA index_list(durable_action_tickets)")
            if row["origin"] == "c"
        }
        return columns, indexes
    finally:
        conn.close()


@pytest.mark.parametrize("initializer", ["central", "store"])
def test_fresh_database_contains_action_ticket_ledger(
    tmp_path: Path,
    initializer: str,
) -> None:
    db_path = tmp_path / f"{initializer}.db"
    if initializer == "central":
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(SCHEMA_SQL)
        finally:
            conn.close()
    else:
        DurableRequestStore(db_path)

    columns, indexes = _schema_details(db_path)

    assert set(columns) == EXPECTED_COLUMNS
    assert columns["request_id"]["pk"] == 1
    assert indexes["idx_durable_action_tickets_idempotency"] == [
        "action_type",
        "idempotency_key",
    ]
    assert indexes["idx_durable_action_tickets_runnable"] == [
        "status",
        "execution_claimed_at",
        "created_at",
    ]
    assert indexes["idx_durable_action_tickets_notification"] == [
        "notification_status",
        "notification_claimed_at",
        "updated_at",
    ]

    conn = sqlite3.connect(db_path)
    try:
        unique = {
            row[1]: row[2]
            for row in conn.execute("PRAGMA index_list(durable_action_tickets)")
        }
    finally:
        conn.close()
    assert unique["idx_durable_action_tickets_idempotency"] == 1


def test_action_ticket_payload_and_outcome_are_database_bounded_by_bytes(
    tmp_path: Path,
) -> None:
    from gateway.durable_actions import (
        MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES,
        MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH,
        MAX_DURABLE_ACTION_PAYLOAD_BYTES,
    )

    db_path = tmp_path / "bounded.db"
    DurableRequestStore(db_path)

    def insert(*, payload_json: str = "{}", outcome_reason=None, outcome_json=None):
        suffix = f"{len(payload_json)}-{len(outcome_reason or '')}-{len(outcome_json or '')}"
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """INSERT INTO durable_action_tickets (
                       request_id, action_type, action_version, payload_json,
                       fingerprint, idempotency_key, status, outcome_reason,
                       outcome_json, notification_status, created_at, updated_at
                ) VALUES (?, 'cron.resume', 1, ?, 'fingerprint', ?,
                             'waiting_for_decision', ?, ?, 'not_ready', 1.0, 1.0)""",
                (
                    f"request-{suffix}",
                    payload_json,
                    f"key-{suffix}",
                    outcome_reason,
                    outcome_json,
                ),
            )

    payload_at_limit = (
        '"' + "é" * ((MAX_DURABLE_ACTION_PAYLOAD_BYTES - 2) // 2) + '"'
    )
    payload_over_limit = payload_at_limit[:-1] + 'é"'
    outcome_at_limit = (
        '"' + "é" * ((MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES - 2) // 2) + '"'
    )
    outcome_over_limit = outcome_at_limit[:-1] + 'é"'

    assert len(payload_at_limit.encode("utf-8")) == MAX_DURABLE_ACTION_PAYLOAD_BYTES
    assert len(payload_over_limit) < MAX_DURABLE_ACTION_PAYLOAD_BYTES
    assert len(payload_over_limit.encode("utf-8")) > MAX_DURABLE_ACTION_PAYLOAD_BYTES
    assert (
        len(outcome_at_limit.encode("utf-8"))
        == MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES
    )
    assert len(outcome_over_limit) < MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES
    assert (
        len(outcome_over_limit.encode("utf-8"))
        > MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES
    )

    insert(
        payload_json=payload_at_limit,
        outcome_reason="x" * MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH,
        outcome_json=outcome_at_limit,
    )
    with pytest.raises(sqlite3.IntegrityError):
        insert(payload_json=payload_over_limit)
    with pytest.raises(sqlite3.IntegrityError):
        insert(outcome_reason="x" * (MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH + 1))
    with pytest.raises(sqlite3.IntegrityError):
        insert(outcome_json=outcome_over_limit)


def test_phase4b_database_reconciliation_preserves_requests_without_tickets(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "phase4b.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_PHASE4B_DURABLE_REQUEST_SCHEMA_SQL)
        conn.execute(
            """INSERT INTO durable_requests (
                   request_id, kind, title, body, options_json, status,
                   created_at, updated_at, expires_at, platform, chat_id,
                   responder_mode, origin_type, origin_id
               ) VALUES (
                   'drq_existing', 'decision', 'Existing', 'Keep me', '[]',
                   'open', 1.0, 1.0, 2.0, 'discord', 'chat-1', 'initiator',
                   'session', 'session-1'
               )"""
        )
        conn.commit()
    finally:
        conn.close()

    DurableRequestStore(db_path)

    conn = sqlite3.connect(db_path)
    try:
        request = conn.execute(
            "SELECT title, body, status FROM durable_requests WHERE request_id = ?",
            ("drq_existing",),
        ).fetchone()
        ticket_count = conn.execute(
            "SELECT COUNT(*) FROM durable_action_tickets"
        ).fetchone()[0]
    finally:
        conn.close()

    assert request == ("Existing", "Keep me", "open")
    assert ticket_count == 0


def test_action_value_types_are_closed_and_immutable() -> None:
    from gateway.durable_actions import (
        DurableActionNotificationStatus,
        DurableActionOutcome,
        DurableActionStatus,
        DurableActionTicket,
    )

    assert {status.value for status in DurableActionStatus} == {
        "waiting_for_decision",
        "ready",
        "claimed",
        "executing",
        "succeeded",
        "already_applied",
        "declined",
        "expired",
        "cancelled",
        "abandoned",
        "invalidated",
        "failed",
        "unknown",
        "handler_unavailable",
    }
    assert {status.value for status in DurableActionNotificationStatus} == {
        "not_ready",
        "pending",
        "claimed",
        "delivered",
        "dropped",
    }

    ticket = DurableActionTicket(
        request_id="drq_1",
        action_type="cron.resume",
        action_version=1,
        payload_json="{}",
        fingerprint="fingerprint",
        idempotency_key="key",
        status=DurableActionStatus.WAITING_FOR_DECISION,
        execution_claim=None,
        execution_claimed_at=None,
        execution_started_at=None,
        outcome_reason=None,
        outcome_json=None,
        completed_at=None,
        notification_status=DurableActionNotificationStatus.NOT_READY,
        notification_claim=None,
        notification_claimed_at=None,
        notification_completed_at=None,
        created_at=1.0,
        updated_at=1.0,
    )
    outcome = DurableActionOutcome(
        status=DurableActionStatus.SUCCEEDED,
        reason="resumed",
        result_json='{"changed":true}',
    )

    with pytest.raises(FrozenInstanceError):
        ticket.status = DurableActionStatus.READY  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        outcome.reason = "changed"  # type: ignore[misc]
