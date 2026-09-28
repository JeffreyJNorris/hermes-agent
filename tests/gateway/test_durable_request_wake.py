from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import gateway.durable_requests as durable_requests
from gateway.durable_requests import (
    DurableRequestError,
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStatus,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
    DurableResolveStatus,
    DurableWakeStatus,
)


class Clock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _store(tmp_path: Path, clock: Clock, **kwargs) -> DurableRequestStore:
    return DurableRequestStore(tmp_path / "state.db", wall_clock=clock, **kwargs)


def _create(
    store: DurableRequestStore,
    *,
    wake_on_terminal: bool,
    reference: str = "wake-test",
):
    return store.create(
        DurableRequestSpec(
            kind="test.wake",
            title="Choose",
            body="Choose one option.",
            options=(
                DurableRequestOption("first", "First"),
                DurableRequestOption("second", "Second"),
            ),
            ttl_seconds=60,
            reference=reference,
        ),
        target=DurableRequestTarget("discord", "chat", "thread"),
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id="actor",
        origin_type="session",
        origin_id="agent:main:discord:dm:chat:actor",
        wake_on_terminal=wake_on_terminal,
        origin_session_id="physical-session" if wake_on_terminal else None,
        origin_ui_session_id="ui-session" if wake_on_terminal else None,
        origin_profile="operations" if wake_on_terminal else None,
    ).request


def _bind(store: DurableRequestStore, request_id: str) -> None:
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    bound = store.bind_delivery(
        request_id=request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="message",
    )
    assert bound is not None


def _resolve(store: DurableRequestStore, request_id: str) -> None:
    result = store.resolve(
        request_id=request_id,
        option_token="2",
        platform="discord",
        channel_id="thread",
        message_id="message",
        actor_id="actor",
    )
    assert result.status is DurableResolveStatus.RESOLVED


def test_poll_only_terminal_request_does_not_create_wake(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, wake_on_terminal=False)
    _bind(store, request.request_id)
    _resolve(store, request.request_id)

    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.wake_requested is False
    assert persisted.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert persisted.origin_session_id is None
    assert persisted.origin_ui_session_id is None
    assert persisted.origin_profile is None
    assert store.claim_next_wake() is None


def test_selected_request_queues_claims_and_completes_one_wake(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store, wake_on_terminal=True)
    _bind(store, request.request_id)
    _resolve(store, request.request_id)

    pending = store.get(request_id=request.request_id)
    assert pending is not None
    assert pending.status is DurableRequestStatus.SELECTED
    assert pending.selected_option_id == "second"
    assert pending.wake_status is DurableWakeStatus.PENDING
    assert pending.origin_session_id == "physical-session"
    assert pending.origin_ui_session_id == "ui-session"
    assert pending.origin_profile == "operations"

    first_claim = store.claim_next_wake()
    assert first_claim is not None
    assert first_claim.request.request_id == request.request_id
    assert first_claim.request.wake_status is DurableWakeStatus.CLAIMED
    assert first_claim.request.wake_attempts == 1

    assert store.release_wake_claim(
        request_id=request.request_id,
        claim_id=first_claim.claim_id,
    )
    second_claim = store.claim_next_wake()
    assert second_claim is not None
    assert second_claim.claim_id != first_claim.claim_id
    assert second_claim.request.wake_attempts == 2

    clock.value += 1
    assert store.complete_wake(
        request_id=request.request_id,
        claim_id=second_claim.claim_id,
    )
    delivered = store.get(request_id=request.request_id)

    assert delivered is not None
    assert delivered.status is DurableRequestStatus.SELECTED
    assert delivered.selected_option_id == "second"
    assert delivered.wake_status is DurableWakeStatus.DELIVERED
    assert delivered.wake_delivered_at == clock.value
    assert delivered.wake_reason is None
    assert store.claim_next_wake() is None


def test_command_selected_request_queues_generic_wake(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = store.create(
        DurableRequestSpec(
            kind="test.wake",
            title="Choose",
            body="Choose one option.",
            options=(
                DurableRequestOption("first", "First"),
                DurableRequestOption("second", "Second"),
            ),
            ttl_seconds=60,
            reference="command-wake",
        ),
        target=DurableRequestTarget("discord", "chat", "thread"),
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id="actor",
        origin_type="session",
        origin_id="agent:main:discord:dm:chat:actor",
        command_response_allowed=True,
        wake_on_terminal=True,
        origin_session_id="physical-session",
    ).request
    _bind(store, request.request_id)

    result = store.resolve_command(
        request_id=request.request_id,
        option_slots=(2,),
        platform="discord",
        channel_id="thread",
        actor_id="actor",
    )

    assert result.status is DurableResolveStatus.RESOLVED
    claim = store.claim_next_wake()
    assert claim is not None
    assert claim.request.request_id == request.request_id
    assert claim.request.selected_option_id == "second"


def test_retrying_wake_does_not_starve_later_terminal_events(
    tmp_path: Path,
) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    first = _create(
        store,
        wake_on_terminal=True,
        reference="wake-first",
    )
    first_delivery = store.claim_next_pending(["discord"])
    assert first_delivery is not None
    assert store.bind_delivery(
        request_id=first.request_id,
        claim_id=first_delivery.claim_id,
        platform="discord",
        message_id="message-first",
    ) is not None
    assert store.resolve(
        request_id=first.request_id,
        option_token="1",
        platform="discord",
        channel_id="thread",
        message_id="message-first",
        actor_id="actor",
    ).status is DurableResolveStatus.RESOLVED

    clock.value += 1
    second = _create(
        store,
        wake_on_terminal=True,
        reference="wake-second",
    )
    second_delivery = store.claim_next_pending(["discord"])
    assert second_delivery is not None
    assert store.bind_delivery(
        request_id=second.request_id,
        claim_id=second_delivery.claim_id,
        platform="discord",
        message_id="message-second",
    ) is not None
    assert store.resolve(
        request_id=second.request_id,
        option_token="1",
        platform="discord",
        channel_id="thread",
        message_id="message-second",
        actor_id="actor",
    ).status is DurableResolveStatus.RESOLVED

    first_wake = store.claim_next_wake()
    assert first_wake is not None
    assert first_wake.request.request_id == first.request_id
    assert store.release_wake_claim(
        request_id=first.request_id,
        claim_id=first_wake.claim_id,
    )

    next_wake = store.claim_next_wake()
    assert next_wake is not None
    assert next_wake.request.request_id == second.request_id


def test_dropped_wake_preserves_result_and_records_reason(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, wake_on_terminal=True)
    _bind(store, request.request_id)
    _resolve(store, request.request_id)
    claim = store.claim_next_wake()
    assert claim is not None

    assert store.drop_wake(
        request_id=request.request_id,
        claim_id=claim.claim_id,
        reason="origin_session_ended",
    )
    dropped = store.get(request_id=request.request_id)

    assert dropped is not None
    assert dropped.status is DurableRequestStatus.SELECTED
    assert dropped.selected_option_id == "second"
    assert dropped.wake_status is DurableWakeStatus.DROPPED
    assert dropped.wake_reason == "origin_session_ended"
    assert dropped.wake_delivered_at is None


@pytest.mark.parametrize(
    "terminal_path",
    [
        "cancel",
        "expire",
        "unsupported",
        "delivery_failed",
        "delivery_unknown",
    ],
)
def test_every_terminal_path_queues_requested_wake(
    tmp_path: Path,
    terminal_path: str,
) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(
        store,
        wake_on_terminal=True,
        reference=f"terminal-{terminal_path}",
    )

    if terminal_path == "cancel":
        terminal = store.cancel(
            origin_type="session",
            origin_id="agent:main:discord:dm:chat:actor",
            request_id=request.request_id,
        )
        assert terminal is not None
        assert terminal.status is DurableRequestStatus.CANCELLED
    elif terminal_path == "expire":
        clock.value = request.expires_at
        assert store.expire_due() == 1
    else:
        claim = store.claim_next_pending(["discord"])
        assert claim is not None
        delivery_status = {
            "unsupported": DurableRequestStatus.UNSUPPORTED,
            "delivery_failed": DurableRequestStatus.DELIVERY_FAILED,
            "delivery_unknown": DurableRequestStatus.DELIVERY_UNKNOWN,
        }[terminal_path]
        terminal = store.fail_delivery(
            request_id=request.request_id,
            claim_id=claim.claim_id,
            status=delivery_status,
            reason="provider_delivery_failed",
        )
        assert terminal is not None
        assert terminal.status is delivery_status

    persisted = store.get(request_id=request.request_id)
    assert persisted is not None
    assert persisted.status in {
        DurableRequestStatus.CANCELLED,
        DurableRequestStatus.EXPIRED,
        DurableRequestStatus.UNSUPPORTED,
        DurableRequestStatus.DELIVERY_FAILED,
        DurableRequestStatus.DELIVERY_UNKNOWN,
    }
    assert persisted.wake_status is DurableWakeStatus.PENDING
    claim = store.claim_next_wake()
    assert claim is not None
    assert claim.request.request_id == request.request_id


def test_stale_provider_claim_queues_delivery_unknown_wake(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store, wake_on_terminal=True)
    delivery_claim = store.claim_next_pending(["discord"])
    assert delivery_claim is not None

    clock.value += 6
    assert store.recover_stale_delivery_claims(stale_after_seconds=5) == 1
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert persisted.wake_status is DurableWakeStatus.PENDING


def test_provider_message_loss_queues_terminal_wake(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, wake_on_terminal=True)
    _bind(store, request.request_id)

    failed = store.mark_open_failed(
        request_id=request.request_id,
        message_id="message",
        reason="provider_message_missing",
    )

    assert failed is not None
    assert failed.status is DurableRequestStatus.DELIVERY_FAILED
    assert failed.wake_status is DurableWakeStatus.PENDING


def test_lookup_expiry_queues_terminal_wake(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store, wake_on_terminal=True)

    clock.value = request.expires_at
    expired = store.get(request_id=request.request_id)

    assert expired is not None
    assert expired.status is DurableRequestStatus.EXPIRED
    assert expired.wake_status is DurableWakeStatus.PENDING


def test_stale_wake_claim_is_released_after_restart_window(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store, wake_on_terminal=True)
    _bind(store, request.request_id)
    _resolve(store, request.request_id)
    first = store.claim_next_wake()
    assert first is not None

    clock.value += 6
    restarted = DurableRequestStore(tmp_path / "state.db", wall_clock=clock)

    assert restarted.recover_stale_wake_claims(stale_after_seconds=5) == 1
    second = restarted.claim_next_wake()
    assert second is not None
    assert second.request.request_id == request.request_id
    assert second.claim_id != first.claim_id


def test_pending_and_claimed_wakes_are_not_pruned(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock, max_terminal=0)
    request = _create(store, wake_on_terminal=True)
    _bind(store, request.request_id)
    _resolve(store, request.request_id)

    clock.value += 100
    assert store.prune_terminal(retention_seconds=1) == 0
    assert store.get(request_id=request.request_id) is not None

    claim = store.claim_next_wake()
    assert claim is not None
    assert store.prune_terminal(retention_seconds=1) == 0
    assert store.get(request_id=request.request_id) is not None

    assert store.complete_wake(
        request_id=request.request_id,
        claim_id=claim.claim_id,
    )
    clock.value += 2
    assert store.prune_terminal(retention_seconds=1) == 1
    assert store.get(request_id=request.request_id) is None


def test_session_wake_requires_physical_session_id(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())

    with pytest.raises(
        DurableRequestError,
        match="session wake-up requires a durable origin session id",
    ):
        store.create(
            DurableRequestSpec(
                kind="test.wake",
                title="Choose",
                body="Choose one option.",
                options=(
                    DurableRequestOption("a", "A"),
                    DurableRequestOption("b", "B"),
                ),
                ttl_seconds=60,
            ),
            target=DurableRequestTarget("discord", "chat"),
            responder_mode=DurableResponderMode.INITIATOR,
            expected_actor_id="actor",
            origin_type="session",
            origin_id="session-key",
            wake_on_terminal=True,
        )


def test_existing_phase4a_database_is_reconciled_in_place(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
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
                thread_id TEXT,
                message_id TEXT,
                responder_mode TEXT NOT NULL,
                expected_actor_id TEXT,
                origin_type TEXT NOT NULL,
                origin_id TEXT NOT NULL,
                reference TEXT,
                selected_option_id TEXT,
                resolved_by_actor_id TEXT,
                resolved_at REAL,
                terminal_reason TEXT,
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                delivery_claim TEXT,
                delivery_claimed_at REAL,
                delivered_at REAL
            );
            """
        )
        conn.execute(
            """INSERT INTO durable_requests (
                   request_id, kind, title, body, options_json, status,
                   created_at, updated_at, expires_at, platform, chat_id,
                   responder_mode, origin_type, origin_id
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "drq_abcdefghijklmnopqrstuvwxyz",
                "test",
                "Title",
                "Body",
                '[{"id":"a","label":"A","style":"secondary"},'
                '{"id":"b","label":"B","style":"secondary"}]',
                "pending_delivery",
                1.0,
                1.0,
                2.0,
                "discord",
                "chat",
                "any_authorized",
                "session",
                "session-1",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    DurableRequestStore(db_path)

    conn = sqlite3.connect(db_path)
    try:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(durable_requests)")
        }
        indexes = {
            row[1] for row in conn.execute("PRAGMA index_list(durable_requests)")
        }
    finally:
        conn.close()

    assert {
        "wake_requested",
        "wake_status",
        "wake_attempts",
        "wake_claim",
        "wake_claimed_at",
        "wake_delivered_at",
        "wake_reason",
        "origin_session_id",
        "origin_ui_session_id",
        "origin_profile",
        "delivery_mode",
        "command_response_allowed",
        "text_command_fallback_allowed",
        "resolution_source",
    }.issubset(columns)
    assert "idx_durable_requests_wake" in indexes

    migrated = DurableRequestStore(db_path).get(
        request_id="drq_abcdefghijklmnopqrstuvwxyz"
    )
    assert migrated is not None
    assert (
        migrated.delivery_mode is durable_requests.DurableRequestDeliveryMode.NATIVE
    )
    assert migrated.command_response_allowed is False
    assert migrated.text_command_fallback_allowed is False
    assert migrated.resolution_source is None


def test_central_state_schema_reconciles_phase4a_database(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
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
                thread_id TEXT,
                message_id TEXT,
                responder_mode TEXT NOT NULL,
                expected_actor_id TEXT,
                origin_type TEXT NOT NULL,
                origin_id TEXT NOT NULL,
                reference TEXT,
                selected_option_id TEXT,
                resolved_by_actor_id TEXT,
                resolved_at REAL,
                terminal_reason TEXT,
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                delivery_claim TEXT,
                delivery_claimed_at REAL,
                delivered_at REAL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()

    from hermes_state import SessionDB

    database = SessionDB(db_path=db_path)
    database.close()

    conn = sqlite3.connect(db_path)
    try:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(durable_requests)")
        }
        indexes = {
            row[1] for row in conn.execute("PRAGMA index_list(durable_requests)")
        }
    finally:
        conn.close()

    assert "wake_status" in columns
    assert "origin_session_id" in columns
    assert "delivery_mode" in columns
    assert "command_response_allowed" in columns
    assert "text_command_fallback_allowed" in columns
    assert "resolution_source" in columns
    assert "idx_durable_requests_wake" in indexes
