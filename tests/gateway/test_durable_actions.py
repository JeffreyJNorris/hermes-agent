from __future__ import annotations

import sqlite3
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from gateway import durable_actions
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


@pytest.fixture(autouse=True)
def _isolate_durable_action_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(durable_actions, "_DURABLE_ACTION_HANDLERS", {})


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


def _prepared_action(_job_id: str) -> durable_actions.PreparedDurableAction:
    return durable_actions.PreparedDurableAction(
        action_type="test.resume",
        action_version=1,
        payload_json=durable_actions.encode_durable_action_payload(
            {"job_id": "job-1"}
        ),
        fingerprint="sha256:fingerprint",
        idempotency_key="test.resume:job-1:fingerprint",
    )


def _execute_action(
    _context: durable_actions.DurableActionExecutionContext,
) -> durable_actions.DurableActionOutcome:
    return durable_actions.DurableActionOutcome(
        status=durable_actions.DurableActionStatus.SUCCEEDED,
        reason="done",
    )


def test_handler_registry_uses_exact_action_type_and_version() -> None:
    durable_actions.register_durable_action_handler(
        "test.resume",
        1,
        prepare=_prepared_action,
        execute=_execute_action,
    )

    assert (
        durable_actions.lookup_durable_action_preparer("test.resume", 1)
        is _prepared_action
    )
    assert (
        durable_actions.lookup_durable_action_handler("test.resume", 1)
        is _execute_action
    )
    assert durable_actions.lookup_durable_action_handler("test.resume", 2) is None
    assert durable_actions.lookup_durable_action_handler("test.pause", 1) is None
    assert durable_actions.lookup_durable_action_handler("test.resume", True) is None


def test_handler_registry_rejects_duplicate_registration() -> None:
    durable_actions.register_durable_action_handler(
        "test.resume",
        1,
        prepare=_prepared_action,
        execute=_execute_action,
    )

    with pytest.raises(ValueError, match="already registered"):
        durable_actions.register_durable_action_handler(
            "test.resume",
            1,
            prepare=_prepared_action,
            execute=_execute_action,
        )


@pytest.mark.parametrize("field", ["prepare", "execute"])
def test_handler_registry_rejects_noncallable_handlers(field: str) -> None:
    handlers = {"prepare": _prepared_action, "execute": _execute_action}
    handlers[field] = "persisted.callback"

    with pytest.raises(TypeError, match=field):
        durable_actions.register_durable_action_handler(
            "test.resume",
            1,
            **handlers,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("action_type", "version"),
    [
        ("", 1),
        ("cron", 1),
        ("Cron.resume", 1),
        ("cron resume", 1),
        ("cron.__import__()", 1),
        ("cron.resume", 0),
        ("cron.resume", -1),
        ("cron.resume", True),
    ],
)
def test_handler_registry_rejects_invalid_identity(
    action_type: str,
    version: int,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        durable_actions.register_durable_action_handler(
            action_type,
            version,
            prepare=_prepared_action,
            execute=_execute_action,
        )


def test_payload_json_is_canonical_and_round_trips() -> None:
    payload = {"z": "é", "a": [1, True, None]}

    encoded = durable_actions.encode_durable_action_payload(payload)

    assert encoded == '{"a":[1,true,null],"z":"é"}'
    assert durable_actions.decode_durable_action_payload(encoded) == payload


@pytest.mark.parametrize(
    "value",
    [object(), {"not-a-json-key": {1, 2}}, {1: "integer key"}, ("tuple",)],
)
def test_payload_json_rejects_non_json_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        durable_actions.encode_durable_action_payload(value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_payload_json_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError):
        durable_actions.encode_durable_action_payload(value)


def test_payload_json_rejects_noncanonical_persisted_text() -> None:
    with pytest.raises(ValueError, match="canonical"):
        durable_actions.decode_durable_action_payload('{"b": 2, "a": 1}')


def test_payload_json_normalizes_cyclic_container_errors() -> None:
    cyclic: list[object] = []
    cyclic.append(cyclic)

    with pytest.raises(ValueError, match="canonical JSON"):
        durable_actions.encode_durable_action_payload(cyclic)


def test_payload_and_result_helpers_bound_utf8_encoded_bytes() -> None:
    payload_at_limit = "é" * (
        (durable_actions.MAX_DURABLE_ACTION_PAYLOAD_BYTES - 2) // 2
    )
    payload_over_limit = payload_at_limit + "é"
    result_at_limit = "é" * (
        (durable_actions.MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES - 2) // 2
    )
    result_over_limit = result_at_limit + "é"

    encoded_payload = durable_actions.encode_durable_action_payload(payload_at_limit)
    encoded_result = durable_actions.encode_durable_action_result(result_at_limit)

    assert (
        len(encoded_payload.encode("utf-8"))
        == durable_actions.MAX_DURABLE_ACTION_PAYLOAD_BYTES
    )
    assert (
        len(encoded_result.encode("utf-8"))
        == durable_actions.MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES
    )
    assert (
        durable_actions.decode_durable_action_payload(encoded_payload)
        == payload_at_limit
    )
    assert durable_actions.decode_durable_action_result(encoded_result) == result_at_limit
    with pytest.raises(ValueError, match="too large"):
        durable_actions.encode_durable_action_payload(payload_over_limit)
    with pytest.raises(ValueError, match="too large"):
        durable_actions.encode_durable_action_result(result_over_limit)


@pytest.mark.parametrize("field", ["fingerprint", "idempotency_key"])
def test_prepared_action_rejects_blank_security_fields(field: str) -> None:
    values = {
        "action_type": "test.resume",
        "action_version": 1,
        "payload_json": durable_actions.encode_durable_action_payload({}),
        "fingerprint": "sha256:fingerprint",
        "idempotency_key": "test.resume:key",
    }
    values[field] = " \t"

    with pytest.raises(ValueError, match=field):
        durable_actions.PreparedDurableAction(**values)


@pytest.mark.parametrize("field", ["fingerprint", "idempotency_key"])
def test_prepared_action_rejects_lone_unicode_surrogates(field: str) -> None:
    values = {
        "action_type": "test.resume",
        "action_version": 1,
        "payload_json": durable_actions.encode_durable_action_payload({}),
        "fingerprint": "sha256:fingerprint",
        "idempotency_key": "test.resume:key",
    }
    values[field] = "\ud800"

    with pytest.raises(ValueError, match="UTF-8"):
        durable_actions.PreparedDurableAction(**values)


def test_prepared_action_rejects_noncanonical_or_oversized_payload() -> None:
    values = {
        "action_type": "test.resume",
        "action_version": 1,
        "fingerprint": "sha256:fingerprint",
        "idempotency_key": "test.resume:key",
    }

    with pytest.raises(ValueError, match="canonical"):
        durable_actions.PreparedDurableAction(
            payload_json='{ "job_id": "job-1" }',
            **values,
        )
    with pytest.raises(ValueError, match="too large"):
        durable_actions.PreparedDurableAction(
            payload_json=(
                '"'
                + "é" * durable_actions.MAX_DURABLE_ACTION_PAYLOAD_BYTES
                + '"'
            ),
            **values,
        )


def test_execution_context_is_frozen_and_validates_persisted_fields() -> None:
    context = durable_actions.DurableActionExecutionContext(
        request_id="drq_1",
        action_type="test.resume",
        action_version=1,
        payload_json=durable_actions.encode_durable_action_payload(
            {"job_id": "job-1"}
        ),
        fingerprint="sha256:fingerprint",
        idempotency_key="test.resume:key",
    )

    with pytest.raises(FrozenInstanceError):
        context.fingerprint = "changed"  # type: ignore[misc]


def test_outcome_is_structural_canonical_and_bounded() -> None:
    result_json = durable_actions.encode_durable_action_result({"changed": True})
    outcome = durable_actions.DurableActionOutcome(
        status=durable_actions.DurableActionStatus.SUCCEEDED,
        reason="x" * durable_actions.MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH,
        result_json=result_json,
    )

    assert outcome.result_json == '{"changed":true}'
    with pytest.raises(TypeError, match="status"):
        durable_actions.DurableActionOutcome(status="succeeded")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="execution outcome"):
        durable_actions.DurableActionOutcome(
            status=durable_actions.DurableActionStatus.READY,
        )
    with pytest.raises(ValueError, match="reason"):
        durable_actions.DurableActionOutcome(
            status=durable_actions.DurableActionStatus.FAILED,
            reason="x"
            * (durable_actions.MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH + 1),
        )
    with pytest.raises(ValueError, match="UTF-8"):
        durable_actions.DurableActionOutcome(
            status=durable_actions.DurableActionStatus.FAILED,
            reason="\ud800",
        )
    with pytest.raises(ValueError, match="canonical"):
        durable_actions.DurableActionOutcome(
            status=durable_actions.DurableActionStatus.SUCCEEDED,
            result_json='{ "changed": true }',
        )
    with pytest.raises(TypeError, match="result_json"):
        durable_actions.DurableActionOutcome(
            status=durable_actions.DurableActionStatus.FAILED,
            result_json=RuntimeError("do not persist me"),  # type: ignore[arg-type]
        )


def test_decoding_persisted_payload_cannot_select_or_invoke_a_handler() -> None:
    called = False

    def safe_execute(
        _context: durable_actions.DurableActionExecutionContext,
    ) -> durable_actions.DurableActionOutcome:
        nonlocal called
        called = True
        return durable_actions.DurableActionOutcome(
            status=durable_actions.DurableActionStatus.SUCCEEDED,
        )

    durable_actions.register_durable_action_handler(
        "test.safe",
        1,
        prepare=_prepared_action,
        execute=safe_execute,
    )
    persisted_payload = durable_actions.encode_durable_action_payload(
        {
            "action_type": "test.safe",
            "action_version": 1,
            "import_path": "malicious.module:run",
            "callback": "builtins.eval",
        }
    )

    decoded = durable_actions.decode_durable_action_payload(persisted_payload)

    assert decoded["import_path"] == "malicious.module:run"
    assert (
        durable_actions.lookup_durable_action_handler("persisted.payload", 1)
        is None
    )
    assert called is False
