from __future__ import annotations

import sqlite3
import threading
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from gateway import durable_actions
from gateway.durable_requests import (
    DurableRequestError,
    DurableRequestLimitError,
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStatus,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
    DurableResolveStatus,
    DurableWakeStatus,
    PromptStyle,
)
from hermes_state_common import DURABLE_ACTION_SCHEMA_SQL, SCHEMA_SQL


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
    "notification_reason",
    "inline_wait_owner",
    "inline_wait_until",
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


def test_action_store_explicitly_closes_schema_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        closed = False

        def executescript(self, _sql: str) -> None:
            pass

        def execute(self, _sql: str):
            return []

        def close(self) -> None:
            self.closed = True

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    connection = Connection()
    monkeypatch.setattr(
        durable_actions.DurableActionStore,
        "_connect",
        lambda _self: connection,
    )

    durable_actions.DurableActionStore(tmp_path / "state.db")

    assert connection.closed is True


def test_action_store_connect_closes_connection_when_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Connection:
        closed = False
        row_factory = None

        def execute(self, _sql: str) -> None:
            raise sqlite3.OperationalError("setup failed")

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(
        durable_actions.sqlite3,
        "connect",
        lambda *_args, **_kwargs: connection,
    )

    with pytest.raises(sqlite3.OperationalError, match="setup failed"):
        durable_actions.DurableActionStore(tmp_path / "state.db")

    assert connection.closed is True


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

    assert EXPECTED_COLUMNS <= set(columns)
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


def test_existing_action_ledger_adds_notification_reason(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-action.db"
    block = """    notification_reason TEXT
        CHECK (
            notification_reason IS NULL
            OR length(notification_reason) <= 1024
        ),
"""
    legacy_schema = DURABLE_ACTION_SCHEMA_SQL.replace(block, "")
    assert legacy_schema != DURABLE_ACTION_SCHEMA_SQL
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(legacy_schema)
    finally:
        conn.close()

    durable_actions.DurableActionStore(db_path)

    columns, _ = _schema_details(db_path)
    assert "notification_reason" in columns
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO durable_action_tickets (
                   request_id, action_type, action_version, payload_json,
                   fingerprint, idempotency_key, status, notification_status,
                   created_at, updated_at
               ) VALUES ('drq_legacy', 'test.action', 1, '{}', 'fingerprint',
                         'idempotency', 'failed', 'pending', 1, 1)"""
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE durable_action_tickets SET notification_reason = ?",
                (
                    "x"
                    * (
                        durable_actions.MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH
                        + 1
                    ),
                ),
            )
    finally:
        conn.close()


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
    for reason in ("two words", "line\nbreak", "MixedCase", "bad/slash"):
        with pytest.raises(ValueError, match="structural token"):
            durable_actions.DurableActionOutcome(
                status=durable_actions.DurableActionStatus.FAILED,
                reason=reason,
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


def _approval_spec(
    *,
    reference: str | None = "resume-job-1",
    approve_label: str = "Resume now",
    reject_label: str = "Keep paused",
) -> DurableRequestSpec:
    return DurableRequestSpec(
        kind="cron.resume_approval",
        title="Resume scheduled job?",
        body="The job is paused and requires approval before it can resume.",
        options=(
            DurableRequestOption("approve", approve_label, PromptStyle.SUCCESS),
            DurableRequestOption("reject", reject_label, PromptStyle.DANGER),
        ),
        ttl_seconds=60,
        reference=reference,
    )


def _action(
    *,
    action_type: str = "cron.resume",
    action_version: int = 1,
    job_id: str = "job-1",
    fingerprint: str = "sha256:fingerprint",
    idempotency_key: str = "cron.resume:job-1:fingerprint",
) -> durable_actions.PreparedDurableAction:
    return durable_actions.PreparedDurableAction(
        action_type=action_type,
        action_version=action_version,
        payload_json=durable_actions.encode_durable_action_payload(
            {"job_id": job_id}
        ),
        fingerprint=fingerprint,
        idempotency_key=idempotency_key,
    )


def _create_approval(
    store: DurableRequestStore,
    *,
    spec: DurableRequestSpec | None = None,
    action: durable_actions.PreparedDurableAction | None = None,
    command_response_allowed: bool = False,
    text_command_fallback_allowed: bool = False,
):
    return store.create_with_action(
        spec or _approval_spec(),
        action=action or _action(),
        target=DurableRequestTarget("discord", "chat-1", "thread-1"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="discord:guild:user",
        origin_session_id="session-1",
        origin_ui_session_id="ui-session-1",
        origin_profile="work",
        command_response_allowed=command_response_allowed,
        text_command_fallback_allowed=text_command_fallback_allowed,
    )


def _action_ticket(db_path: Path, request_id: str) -> sqlite3.Row | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT * FROM durable_action_tickets WHERE request_id = ?",
            (request_id,),
        ).fetchone()


def _open_approval(store: DurableRequestStore):
    created = _create_approval(store)
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    opened = store.bind_delivery(
        request_id=created.request.request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="message-1",
    )
    assert opened is not None
    return created


def _resolve_approval(store: DurableRequestStore, request_id: str, slot: str):
    return store.resolve(
        request_id=request_id,
        option_token=slot,
        platform="discord",
        channel_id="thread-1",
        message_id="message-1",
        actor_id="actor-1",
    )


def test_create_with_action_persists_one_joined_approval_and_retains_lineage(
    tmp_path: Path,
) -> None:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 1_000.0)

    result = _create_approval(store)

    assert result.created is True
    assert result.request.options == (
        DurableRequestOption("approve", "Resume now", PromptStyle.SUCCESS),
        DurableRequestOption("reject", "Keep paused", PromptStyle.DANGER),
    )
    assert result.request.status is DurableRequestStatus.PENDING_DELIVERY
    assert result.request.wake_requested is False
    assert result.request.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert result.request.origin_session_id == "session-1"
    assert result.request.origin_ui_session_id == "ui-session-1"
    assert result.request.origin_profile == "work"
    assert result.ticket == durable_actions.DurableActionTicket(
        request_id=result.request.request_id,
        action_type="cron.resume",
        action_version=1,
        payload_json='{"job_id":"job-1"}',
        fingerprint="sha256:fingerprint",
        idempotency_key="cron.resume:job-1:fingerprint",
        status=durable_actions.DurableActionStatus.WAITING_FOR_DECISION,
        execution_claim=None,
        execution_claimed_at=None,
        execution_started_at=None,
        outcome_reason=None,
        outcome_json=None,
        completed_at=None,
        notification_status=(
            durable_actions.DurableActionNotificationStatus.NOT_READY
        ),
        notification_claim=None,
        notification_claimed_at=None,
        notification_completed_at=None,
        created_at=1_000.0,
        updated_at=1_000.0,
    )
    with pytest.raises(FrozenInstanceError):
        result.created = False  # type: ignore[misc]

    with sqlite3.connect(tmp_path / "state.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 1
        )
        assert conn.execute(
            """SELECT COUNT(*)
               FROM durable_requests AS request
               JOIN durable_action_tickets AS ticket
                 ON ticket.request_id = request.request_id"""
        ).fetchone()[0] == 1


def test_create_with_action_preserves_command_permissions(tmp_path: Path) -> None:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 1_000.0)

    result = _create_approval(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )

    assert result.request.command_response_allowed is True
    assert result.request.text_command_fallback_allowed is True


def test_create_with_action_returns_terminal_ticket_idempotently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    first = _create_approval(store)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE durable_action_tickets
               SET status = 'failed', outcome_reason = 'bounded_failure',
                   outcome_json = '{"changed":false}', completed_at = 1001.0,
                   notification_status = 'pending', updated_at = 1001.0
               WHERE request_id = ?""",
            (first.request.request_id,),
        )

    repeated = _create_approval(
        store,
        spec=_approval_spec(reference="another-reference"),
    )

    assert repeated.created is False
    assert repeated.request.request_id == first.request.request_id
    assert repeated.ticket.status is durable_actions.DurableActionStatus.FAILED
    assert repeated.ticket.outcome_reason == "bounded_failure"
    assert repeated.ticket.outcome_json == '{"changed":false}'
    assert repeated.ticket.notification_status is (
        durable_actions.DurableActionNotificationStatus.PENDING
    )
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 1
        )


@pytest.mark.parametrize("reference", ["resume-job-1", None])
@pytest.mark.parametrize(
    "action",
    [
        _action(action_version=2),
        _action(job_id="job-2"),
        _action(fingerprint="sha256:changed"),
    ],
    ids=["version", "payload", "fingerprint"],
)
def test_create_with_action_rejects_idempotency_conflicts_without_changes(
    tmp_path: Path,
    reference: str | None,
    action: durable_actions.PreparedDurableAction,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    first = _create_approval(store)
    with sqlite3.connect(db_path) as conn:
        before_requests = conn.execute(
            "SELECT * FROM durable_requests ORDER BY request_id"
        ).fetchall()
        before_tickets = conn.execute(
            "SELECT * FROM durable_action_tickets ORDER BY request_id"
        ).fetchall()

    with pytest.raises(DurableRequestError, match="conflict"):
        _create_approval(
            store,
            spec=_approval_spec(reference=reference),
            action=action,
        )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT * FROM durable_requests ORDER BY request_id"
        ).fetchall() == before_requests
        assert conn.execute(
            "SELECT * FROM durable_action_tickets ORDER BY request_id"
        ).fetchall() == before_tickets
    assert store.get(request_id=first.request.request_id) == first.request


def test_create_with_action_reference_cannot_attach_to_generic_request(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    generic = store.create(
        _approval_spec(),
        target=DurableRequestTarget("discord", "chat-1"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="discord:guild:user",
    )

    with pytest.raises(DurableRequestError, match="reference"):
        _create_approval(store)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 0
        )
    assert store.get(request_id=generic.request.request_id) == generic.request


def test_create_with_action_reference_cannot_return_a_different_action(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    first = _create_approval(store)

    with pytest.raises(DurableRequestError, match="reference"):
        _create_approval(
            store,
            action=_action(
                action_type="cron.pause",
                idempotency_key="cron.pause:job-1:fingerprint",
            ),
        )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 1
        )
    assert store.get(request_id=first.request.request_id) == first.request


def test_create_with_action_rolls_back_request_when_ticket_insert_fails(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TRIGGER fail_ticket_insert
               BEFORE INSERT ON durable_action_tickets
               BEGIN
                   SELECT RAISE(ABORT, 'injected ticket failure');
               END"""
        )

    with caplog.at_level("INFO", logger="gateway.durable_requests"):
        with pytest.raises(sqlite3.IntegrityError, match="injected ticket failure"):
            _create_approval(store)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 0
        )
    assert not any(
        record.getMessage() == "Durable request registered"
        for record in caplog.records
    )


def test_create_with_action_rolls_back_ticket_when_request_insert_helper_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)

    def insert_ticket_then_fail(
        conn: sqlite3.Connection,
        *,
        values: object,
        now: float,
    ) -> None:
        conn.execute(
            """INSERT INTO durable_action_tickets (
                   request_id, action_type, action_version, payload_json,
                   fingerprint, idempotency_key, status,
                   notification_status, created_at, updated_at
               ) VALUES (
                   'drq_injected_transaction_failure', 'test.injected', 1,
                   '{}', 'fingerprint', 'injected-key',
                   'waiting_for_decision', 'not_ready', ?, ?
               )""",
            (now, now),
        )
        raise sqlite3.IntegrityError("injected request failure")

    monkeypatch.setattr(store, "_insert_request_locked", insert_ticket_then_fail)

    with pytest.raises(sqlite3.IntegrityError, match="injected request failure"):
        _create_approval(store)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("trusted", [False, True], ids=["generic", "trusted"])
def test_create_logs_nothing_when_commit_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    trusted: bool,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)

    class FailingCommitConnection(sqlite3.Connection):
        def commit(self) -> None:
            self.rollback()
            raise sqlite3.OperationalError("injected commit failure")

    def failing_connect() -> sqlite3.Connection:
        conn = sqlite3.connect(
            db_path,
            timeout=10.0,
            factory=FailingCommitConnection,
        )
        conn.row_factory = sqlite3.Row
        return conn

    monkeypatch.setattr(store, "_connect", failing_connect)
    with caplog.at_level("INFO", logger="gateway.durable_requests"):
        with pytest.raises(sqlite3.OperationalError, match="commit failure"):
            if trusted:
                _create_approval(store)
            else:
                store.create(
                    _approval_spec(),
                    target=DurableRequestTarget("discord", "chat-1"),
                    responder_mode=DurableResponderMode.ANY_AUTHORIZED,
                    expected_actor_id=None,
                    origin_type="session",
                    origin_id="generic-session",
                )

    assert not any(
        record.getMessage() == "Durable request registered"
        for record in caplog.records
    )
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 0
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("trusted", [False, True], ids=["generic", "trusted"])
def test_create_logs_one_registration_after_successful_commit(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    trusted: bool,
) -> None:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 1_000.0)

    with caplog.at_level("INFO", logger="gateway.durable_requests"):
        if trusted:
            _create_approval(store)
        else:
            store.create(
                _approval_spec(),
                target=DurableRequestTarget("discord", "chat-1"),
                responder_mode=DurableResponderMode.ANY_AUTHORIZED,
                expected_actor_id=None,
                origin_type="session",
                origin_id="generic-session",
            )

    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == "gateway.durable_requests"
    ] == ["Durable request registered"]


def test_concurrent_create_with_action_uses_one_request_and_ticket(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    stores = [
        DurableRequestStore(db_path, wall_clock=lambda: 1_000.0),
        DurableRequestStore(db_path, wall_clock=lambda: 1_000.0),
    ]
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def create(store: DurableRequestStore) -> None:
        try:
            barrier.wait()
            results.append(_create_approval(store))
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=create, args=(store,)) for store in stores]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert sorted(result.created for result in results) == [False, True]
    assert len({result.request.request_id for result in results}) == 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 1
        )


def test_generic_creation_remains_ticket_free(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)

    result = store.create(
        _approval_spec(),
        target=DurableRequestTarget("discord", "chat-1"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="generic-session",
        wake_on_terminal=False,
        origin_session_id="must-not-be-retained",
        origin_ui_session_id="must-not-be-retained",
        origin_profile="must-not-be-retained",
    )

    assert result.request.origin_session_id is None
    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
            == 0
        )


def test_create_with_action_requires_canonical_binary_options_and_session_lineage(
    tmp_path: Path,
) -> None:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 1_000.0)
    noncanonical = DurableRequestSpec(
        kind="approval",
        title="Approve?",
        body="Choose.",
        options=(
            DurableRequestOption("yes", "Approve"),
            DurableRequestOption("no", "Reject"),
        ),
        ttl_seconds=60,
    )

    with pytest.raises(DurableRequestError, match="approve.*reject"):
        _create_approval(store, spec=noncanonical)
    with pytest.raises(DurableRequestError, match="session id"):
        store.create_with_action(
            _approval_spec(),
            action=_action(),
            target=DurableRequestTarget("discord", "chat-1"),
            responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            expected_actor_id=None,
            origin_type="session",
            origin_id="discord:guild:user",
        )


def test_create_with_action_accepts_only_prepared_trusted_actions(
    tmp_path: Path,
) -> None:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 1_000.0)

    with pytest.raises(DurableRequestError, match="PreparedDurableAction"):
        _create_approval(
            store,
            action={  # type: ignore[arg-type]
                "action_type": "cron.resume",
                "payload_json": '{"command":"run anything"}',
            },
        )


def test_create_with_action_shares_validation_capacity_and_pruning(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    now = [1_000.0]
    store = DurableRequestStore(
        db_path,
        wall_clock=lambda: now[0],
        max_active=1,
    )
    generic = store.create(
        _approval_spec(reference="old"),
        target=DurableRequestTarget("discord", "chat-1"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="generic-session",
    )

    with pytest.raises(DurableRequestLimitError):
        _create_approval(store)
    assert store.cancel(
        origin_type="session",
        origin_id="generic-session",
        request_id=generic.request.request_id,
    ) is not None
    now[0] += 11

    created = store.create_with_action(
        _approval_spec(approve_label=" A ", reject_label=" R "),
        action=_action(),
        target=DurableRequestTarget("DISCORD", " chat-1 "),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type=" session ",
        origin_id=" trusted-session ",
        origin_session_id=" durable-session ",
        maximum_ttl_seconds=60,
        terminal_retention_seconds=10,
    )

    assert created.request.options[0].label == "A"
    assert created.request.target.platform == "discord"
    assert created.request.origin_type == "session"
    assert store.get(request_id=generic.request.request_id) is None


def test_create_with_action_logs_no_action_secrets(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 1_000.0)
    action = durable_actions.PreparedDurableAction(
        action_type="cron.resume",
        action_version=1,
        payload_json=durable_actions.encode_durable_action_payload(
            {"job_id": "secret-job-marker"}
        ),
        fingerprint="secret-fingerprint-marker",
        idempotency_key="secret-idempotency-marker",
    )

    with caplog.at_level("INFO", logger="gateway.durable_requests"):
        store.create_with_action(
            _approval_spec(),
            action=action,
            target=DurableRequestTarget(
                "discord",
                "secret-chat-marker",
                "secret-thread-marker",
            ),
            responder_mode=DurableResponderMode.INITIATOR,
            expected_actor_id="secret-actor-marker",
            origin_type="session",
            origin_id="secret-origin-marker",
            origin_session_id="secret-session-marker",
            origin_ui_session_id="secret-ui-session-marker",
            origin_profile="secret-profile-marker",
        )

    rendered = repr(
        [
            {
                "message": record.msg,
                "args": record.args,
                "fields": record.__dict__,
            }
            for record in caplog.records
        ]
    )
    for secret in (
        "secret-job-marker",
        "secret-fingerprint-marker",
        "secret-idempotency-marker",
        "secret-chat-marker",
        "secret-thread-marker",
        "secret-actor-marker",
        "secret-origin-marker",
        "secret-session-marker",
        "secret-ui-session-marker",
        "secret-profile-marker",
    ):
        assert secret not in rendered


@pytest.mark.parametrize(
    ("slot", "status", "notification", "completed", "reason"),
    [
        ("1", "ready", "not_ready", False, None),
        ("2", "declined", "pending", True, "rejected"),
    ],
)
def test_action_decision_transitions_atomically(
    tmp_path: Path,
    slot: str,
    status: str,
    notification: str,
    completed: bool,
    reason: str | None,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = _open_approval(store)

    result = _resolve_approval(store, created.request.request_id, slot)

    assert result.status.value == "resolved"
    assert result.request is not None
    assert result.request.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert result.request.terminal_reason == reason
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    assert ticket["status"] == status
    assert ticket["notification_status"] == notification
    assert (ticket["completed_at"] is not None) is completed
    assert ticket["outcome_reason"] == reason


@pytest.mark.parametrize(
    ("slot", "status", "notification", "reason"),
    [
        (1, "ready", "not_ready", None),
        (2, "declined", "pending", "rejected"),
    ],
)
def test_command_action_decision_uses_existing_atomic_transition(
    tmp_path: Path,
    slot: int,
    status: str,
    notification: str,
    reason: str | None,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = _create_approval(store, command_response_allowed=True)
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    assert store.bind_delivery(
        request_id=created.request.request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="message-1",
    ) is not None

    result = store.resolve_command(
        request_id=created.request.request_id,
        option_slots=(slot,),
        platform="discord",
        channel_id="thread-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.RESOLVED
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    assert (ticket["status"], ticket["notification_status"]) == (
        status,
        notification,
    )
    assert ticket["outcome_reason"] == reason


def test_action_decision_uses_internal_id_not_approve_label(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = _open_approval(store)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE durable_requests SET options_json = ? WHERE request_id = ?",
            (
                '[{"id":"not_approve","label":"Approve","style":"success"},'
                '{"id":"reject","label":"Reject","style":"danger"}]',
                created.request.request_id,
            ),
        )

    result = _resolve_approval(store, created.request.request_id, "1")

    assert result.status.value == "resolved"
    assert result.request is not None
    assert result.request.terminal_reason == "invalid_decision"
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    assert (ticket["status"], ticket["notification_status"]) == (
        "abandoned",
        "pending",
    )
    assert ticket["completed_at"] == 1_000.0


def test_duplicate_action_resolution_does_not_requeue_terminal_ticket(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = _open_approval(store)
    _resolve_approval(store, created.request.request_id, "2")
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    before = tuple(ticket)

    replay = _resolve_approval(store, created.request.request_id, "2")

    assert replay.status.value == "already_resolved"
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    assert tuple(ticket) == before


@pytest.mark.parametrize("bulk", [False, True], ids=["get", "expire_due"])
def test_action_expiry_transitions_through_single_and_bulk_paths(
    tmp_path: Path,
    bulk: bool,
) -> None:
    db_path = tmp_path / "state.db"
    now = [1_000.0]
    store = DurableRequestStore(db_path, wall_clock=lambda: now[0])
    created = _create_approval(store)
    now[0] = 1_061.0

    if bulk:
        assert store.expire_due() == 1
    else:
        assert store.get(request_id=created.request.request_id).status.value == "expired"

    request = store.get(request_id=created.request.request_id)
    ticket = _action_ticket(db_path, created.request.request_id)
    assert request is not None and request.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert ticket is not None
    assert (
        ticket["status"],
        ticket["notification_status"],
        ticket["outcome_reason"],
        ticket["completed_at"],
    ) == ("expired", "pending", "deadline_elapsed", 1_061.0)


def test_action_cancellation_is_terminal_and_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = _create_approval(store)

    cancelled = store.cancel(
        origin_type="session",
        origin_id="discord:guild:user",
        request_id=created.request.request_id,
        reason="private caller text",
    )
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    before = tuple(ticket)
    replay = store.cancel(
        origin_type="session",
        origin_id="discord:guild:user",
        request_id=created.request.request_id,
    )

    assert cancelled is not None and cancelled.terminal_reason == "cancelled_by_origin"
    assert cancelled.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert replay is not None
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None
    assert tuple(ticket) == before
    assert ticket["status"] == "cancelled"
    assert ticket["outcome_reason"] == "cancelled_by_origin"
    assert ticket["notification_status"] == "pending"


@pytest.mark.parametrize(
    "terminal_status",
    [
        DurableRequestStatus.UNSUPPORTED,
        DurableRequestStatus.DELIVERY_FAILED,
        DurableRequestStatus.DELIVERY_UNKNOWN,
    ],
)
def test_all_fail_delivery_outcomes_abandon_action(
    tmp_path: Path,
    terminal_status: DurableRequestStatus,
) -> None:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = _create_approval(store)
    claim = store.claim_next_pending(["discord"])
    assert claim is not None

    request = store.fail_delivery(
        request_id=created.request.request_id,
        claim_id=claim.claim_id,
        status=terminal_status,
        reason="private provider text",
    )

    ticket = _action_ticket(db_path, created.request.request_id)
    assert request is not None and request.terminal_reason == terminal_status.value
    assert request.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert ticket is not None
    assert (ticket["status"], ticket["notification_status"]) == (
        "abandoned",
        "pending",
    )
    assert ticket["outcome_reason"] == terminal_status.value
    before = tuple(ticket)
    assert store.fail_delivery(
        request_id=created.request.request_id,
        claim_id=claim.claim_id,
        status=terminal_status,
        reason="different private provider text",
    ) is None
    ticket = _action_ticket(db_path, created.request.request_id)
    assert ticket is not None and tuple(ticket) == before


def test_stale_delivery_and_open_failure_abandon_actions(tmp_path: Path) -> None:
    stale_db = tmp_path / "stale.db"
    now = [1_000.0]
    stale_store = DurableRequestStore(stale_db, wall_clock=lambda: now[0])
    stale = _create_approval(stale_store)
    assert stale_store.claim_next_pending(["discord"]) is not None
    now[0] = 1_006.0
    assert stale_store.recover_stale_delivery_claims(stale_after_seconds=5) == 1

    stale_ticket = _action_ticket(stale_db, stale.request.request_id)
    assert stale_ticket is not None
    assert (stale_ticket["status"], stale_ticket["outcome_reason"]) == (
        "abandoned",
        "delivery_unknown",
    )
    stale_before = tuple(stale_ticket)
    now[0] = 1_007.0
    assert stale_store.recover_stale_delivery_claims(stale_after_seconds=5) == 0
    stale_ticket = _action_ticket(stale_db, stale.request.request_id)
    assert stale_ticket is not None and tuple(stale_ticket) == stale_before

    open_db = tmp_path / "open.db"
    open_store = DurableRequestStore(open_db, wall_clock=lambda: 2_000.0)
    opened = _open_approval(open_store)
    failed = open_store.mark_open_failed(
        request_id=opened.request.request_id,
        message_id="message-1",
        reason="private provider text",
    )
    open_ticket = _action_ticket(open_db, opened.request.request_id)
    assert failed is not None and failed.terminal_reason == "delivery_failed"
    assert open_ticket is not None
    assert (open_ticket["status"], open_ticket["outcome_reason"]) == (
        "abandoned",
        "delivery_failed",
    )


@pytest.mark.parametrize("overflow", [False, True], ids=["age", "overflow"])
@pytest.mark.parametrize("action_status", ["ready", "claimed", "executing"])
def test_pruning_protects_nonterminal_action_tickets(
    tmp_path: Path,
    action_status: str,
    overflow: bool,
) -> None:
    db_path = tmp_path / "state.db"
    now = [1_000.0]
    store = DurableRequestStore(
        db_path,
        wall_clock=lambda: now[0],
        max_terminal=0 if overflow else 10,
    )
    created = _open_approval(store)
    _resolve_approval(store, created.request.request_id, "1")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET status = ? WHERE request_id = ?",
            (action_status, created.request.request_id),
        )
        if overflow:
            conn.execute(
                "UPDATE durable_requests SET resolved_at = 1999.0 WHERE request_id = ?",
                (created.request.request_id,),
            )
    now[0] = 2_000.0

    assert store.prune_terminal(retention_seconds=10) == 0
    assert store.get(request_id=created.request.request_id) is not None
    assert _action_ticket(db_path, created.request.request_id) is not None


@pytest.mark.parametrize("overflow", [False, True], ids=["age", "overflow"])
@pytest.mark.parametrize("notification", ["pending", "claimed"])
def test_pruning_protects_undelivered_action_notifications(
    tmp_path: Path,
    notification: str,
    overflow: bool,
) -> None:
    db_path = tmp_path / "state.db"
    now = [1_000.0]
    store = DurableRequestStore(
        db_path,
        wall_clock=lambda: now[0],
        max_terminal=0 if overflow else 10,
    )
    created = _open_approval(store)
    _resolve_approval(store, created.request.request_id, "2")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE durable_action_tickets
               SET notification_status = ?, completed_at = 1000.0
               WHERE request_id = ?""",
            (notification, created.request.request_id),
        )
        if overflow:
            conn.execute(
                "UPDATE durable_requests SET resolved_at = 1999.0 WHERE request_id = ?",
                (created.request.request_id,),
            )
    now[0] = 2_000.0

    assert store.prune_terminal(retention_seconds=10) == 0
    assert store.get(request_id=created.request.request_id) is not None
    assert _action_ticket(db_path, created.request.request_id) is not None


@pytest.mark.parametrize("notification", ["delivered", "dropped"])
@pytest.mark.parametrize("overflow", [False, True], ids=["age", "overflow"])
def test_pruning_deletes_terminal_action_and_parent_together(
    tmp_path: Path,
    notification: str,
    overflow: bool,
) -> None:
    db_path = tmp_path / "state.db"
    now = [1_000.0]
    store = DurableRequestStore(
        db_path,
        wall_clock=lambda: now[0],
        max_terminal=0 if overflow else 10,
    )
    created = _open_approval(store)
    _resolve_approval(store, created.request.request_id, "2")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE durable_action_tickets
               SET notification_status = ?, notification_completed_at = 1001.0,
                   completed_at = ?
               WHERE request_id = ?""",
            (
                notification,
                1_999.0 if overflow else 1_000.0,
                created.request.request_id,
            ),
        )
        if overflow:
            conn.execute(
                "UPDATE durable_requests SET resolved_at = 1999.0 WHERE request_id = ?",
                (created.request.request_id,),
            )
    now[0] = 2_000.0

    assert store.prune_terminal(retention_seconds=10) == 1
    assert store.get(request_id=created.request.request_id) is None
    assert _action_ticket(db_path, created.request.request_id) is None
