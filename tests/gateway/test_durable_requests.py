from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

import gateway.durable_requests as durable_requests
from gateway.durable_requests import (
    DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS,
    DurableRequestDeliveryMode,
    DurableRequestError,
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStatus,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
    DurableResolveStatus,
    PromptStyle,
    load_durable_requests_config,
)


class Clock:
    def __init__(self, value: float = 1_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _store(tmp_path: Path, clock: Clock) -> DurableRequestStore:
    return DurableRequestStore(tmp_path / "state.db", wall_clock=clock)


def _spec(*, reference: str | None = "weekly") -> DurableRequestSpec:
    return DurableRequestSpec(
        kind="test.release",
        title="Choose release action",
        body="The checks passed. Choose the next action.",
        options=(
            DurableRequestOption("approve", "Approve", PromptStyle.SUCCESS),
            DurableRequestOption("reject", "Reject", PromptStyle.DANGER),
        ),
        ttl_seconds=60,
        reference=reference,
    )


def _create(
    store: DurableRequestStore,
    *,
    reference: str | None = "weekly",
    actor: str | None = "actor-1",
    origin_id: str = "session-1",
    command_response_allowed: bool = False,
    text_command_fallback_allowed: bool = False,
):
    return store.create(
        _spec(reference=reference),
        target=DurableRequestTarget("discord", "chat-1", "thread-1"),
        responder_mode=(
            DurableResponderMode.INITIATOR
            if actor is not None
            else DurableResponderMode.ANY_AUTHORIZED
        ),
        expected_actor_id=actor,
        origin_type="session",
        origin_id=origin_id,
        command_response_allowed=command_response_allowed,
        text_command_fallback_allowed=text_command_fallback_allowed,
    )


def _bind(store: DurableRequestStore, request_id: str, message_id: str = "msg-1"):
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    assert claim.request.request_id == request_id
    bound = store.bind_delivery(
        request_id=request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id=message_id,
    )
    assert bound is not None
    return bound


def test_request_id_is_random_and_contains_no_scope_identifiers(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)

    first = _create(store, reference="one").request
    second = _create(store, reference="two").request

    assert first.request_id.startswith("drq_")
    assert first.request_id != second.request_id
    for forbidden in ("session-1", "chat-1", "thread-1", "actor-1"):
        assert forbidden not in first.request_id


def test_command_responses_default_disabled(monkeypatch) -> None:
    from hermes_cli import config as config_module
    from hermes_cli.config_defaults import DEFAULT_CONFIG

    monkeypatch.setattr(config_module, "load_config_readonly", lambda: {})

    loaded = load_durable_requests_config()

    assert DEFAULT_CONFIG["durable_requests"]["command_responses"] == {
        "enabled": False,
        "allow_on_native_delivery": True,
        "allow_text_delivery_fallback": True,
    }
    assert loaded.command_responses_enabled is False
    assert loaded.allow_on_native_delivery is True
    assert loaded.allow_text_delivery_fallback is True
    assert (
        durable_requests.DurableRequestDeliveryMode.TEXT_COMMAND.value
        == "text_command"
    )
    assert durable_requests.DurableRequestResolutionSource.NATIVE.value == "native"
    assert durable_requests.DurableRequestResolutionSource.COMMAND.value == "command"


def test_create_snapshots_command_response_permission(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())

    default_request = _create(store, reference="default").request
    enabled_request = store.create(
        _spec(reference="enabled"),
        target=DurableRequestTarget("discord", "chat-1", "thread-1"),
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id="actor-1",
        origin_type="session",
        origin_id="session-1",
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    ).request

    assert (
        default_request.delivery_mode
        is durable_requests.DurableRequestDeliveryMode.NATIVE
    )
    assert default_request.command_response_allowed is False
    assert default_request.text_command_fallback_allowed is False
    assert default_request.resolution_source is None
    assert (
        enabled_request.delivery_mode
        is durable_requests.DurableRequestDeliveryMode.NATIVE
    )
    assert enabled_request.command_response_allowed is True
    assert enabled_request.text_command_fallback_allowed is True
    assert enabled_request.resolution_source is None


def test_origin_reference_is_idempotent(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)

    first = _create(store)
    second = _create(store)

    assert first.created is True
    assert second.created is False
    assert second.request.request_id == first.request.request_id


def test_same_reference_is_independent_across_origins(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)

    first = _create(store, origin_id="session-a")
    second = _create(store, origin_id="session-b")

    assert first.request.request_id != second.request.request_id


@pytest.mark.parametrize(
    "spec, message",
    [
        (
            DurableRequestSpec(
                kind="k",
                title="t",
                body="b",
                options=(),
                ttl_seconds=10,
            ),
            "options must contain",
        ),
        (
            DurableRequestSpec(
                kind="k",
                title="t",
                body="b",
                options=(
                    DurableRequestOption("same", "A"),
                    DurableRequestOption("same", "B"),
                ),
                ttl_seconds=10,
            ),
            "option ids must be unique",
        ),
        (
            DurableRequestSpec(
                kind="k",
                title="t",
                body="b",
                options=(
                    DurableRequestOption("a", "A"),
                    DurableRequestOption("b", "B"),
                ),
                ttl_seconds=0,
            ),
            "ttl_seconds must be positive",
        ),
    ],
)
def test_validation_rejects_invalid_specs(
    tmp_path: Path,
    spec: DurableRequestSpec,
    message: str,
) -> None:
    store = _store(tmp_path, Clock())

    with pytest.raises(DurableRequestError, match=message):
        store.create(
            spec,
            target=DurableRequestTarget("discord", "chat"),
            responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            expected_actor_id=None,
            origin_type="cron",
            origin_id="job",
        )


def test_response_before_delivery_is_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request

    result = store.resolve(
        request_id=request.request_id,
        option_token="1",
        platform="discord",
        channel_id="thread-1",
        message_id="msg-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.NOT_DELIVERED


@pytest.mark.parametrize(
    "overrides",
    [
        {"platform": "telegram"},
        {"channel_id": "chat-1"},
        {"message_id": "other"},
        {"actor_id": "actor-2"},
    ],
)
def test_resolution_validates_exact_provider_scope(
    tmp_path: Path,
    overrides: dict[str, str],
) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request
    _bind(store, request.request_id)
    arguments = {
        "request_id": request.request_id,
        "option_token": "1",
        "platform": "discord",
        "channel_id": "thread-1",
        "message_id": "msg-1",
        "actor_id": "actor-1",
    }
    arguments.update(overrides)

    result = store.resolve(**arguments)

    assert result.status is DurableResolveStatus.SCOPE_MISMATCH


def test_resolution_is_exactly_once_and_survives_store_restart(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store).request
    _bind(store, request.request_id)

    result = store.resolve(
        request_id=request.request_id,
        option_token="2",
        platform="discord",
        channel_id="thread-1",
        message_id="msg-1",
        actor_id="actor-1",
    )
    restarted = DurableRequestStore(tmp_path / "state.db", wall_clock=clock)
    replay = restarted.resolve(
        request_id=request.request_id,
        option_token="1",
        platform="discord",
        channel_id="thread-1",
        message_id="msg-1",
        actor_id="actor-1",
    )
    persisted = restarted.get(
        request_id=request.request_id,
        origin_type="session",
        origin_id="session-1",
    )

    assert result.status is DurableResolveStatus.RESOLVED
    assert result.request is not None
    assert result.request.selected_option_id == "reject"
    assert (
        result.request.resolution_source
        is durable_requests.DurableRequestResolutionSource.NATIVE
    )
    assert replay.status is DurableResolveStatus.ALREADY_RESOLVED
    assert persisted is not None
    assert persisted.selected_option_id == "reject"


def test_command_invalid_request_id_returns_not_found_without_sqlite_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path, Clock())

    def fail_connect():
        raise AssertionError("SQLite must not be opened")

    monkeypatch.setattr(store, "_connect", fail_connect)

    result = store.resolve_command(
        request_id="not-a-request",
        option_slots=(1,),
        platform="discord",
        channel_id="thread-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.NOT_FOUND


@pytest.mark.parametrize("option_slots", [(), (1, 2), (0,), (-1,), ("1",), (True,)])
def test_command_requires_one_positive_numeric_slot(
    tmp_path: Path,
    option_slots: tuple[object, ...],
) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, command_response_allowed=True).request
    _bind(store, request.request_id)

    result = store.resolve_command(
        request_id=request.request_id,
        option_slots=option_slots,  # type: ignore[arg-type]
        platform="discord",
        channel_id="thread-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.INVALID_OPTION


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"platform": "telegram"}, DurableResolveStatus.SCOPE_MISMATCH),
        ({"channel_id": "chat-1"}, DurableResolveStatus.WRONG_CHANNEL),
        ({"actor_id": "actor-2"}, DurableResolveStatus.WRONG_ACTOR),
        ({"actor_id": None}, DurableResolveStatus.WRONG_ACTOR),
    ],
)
def test_command_validates_platform_channel_and_actor(
    tmp_path: Path,
    overrides: dict[str, str | None],
    expected: DurableResolveStatus,
) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, command_response_allowed=True).request
    _bind(store, request.request_id)
    arguments = {
        "request_id": request.request_id,
        "option_slots": (1,),
        "platform": "discord",
        "channel_id": "thread-1",
        "actor_id": "actor-1",
    }
    arguments.update(overrides)

    result = store.resolve_command(**arguments)

    assert result.status is expected


def test_command_disabled_request_cannot_resolve(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request
    _bind(store, request.request_id)

    result = store.resolve_command(
        request_id=request.request_id,
        option_slots=(1,),
        platform="discord",
        channel_id="thread-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.COMMAND_DISABLED


@pytest.mark.parametrize(
    ("answered", "overrides", "expected"),
    [
        (False, {"channel_id": "chat-1"}, DurableResolveStatus.WRONG_CHANNEL),
        (False, {"actor_id": "actor-2"}, DurableResolveStatus.WRONG_ACTOR),
        (True, {"channel_id": "chat-1"}, DurableResolveStatus.WRONG_CHANNEL),
        (True, {"actor_id": "actor-2"}, DurableResolveStatus.WRONG_ACTOR),
    ],
)
def test_command_state_does_not_leak_across_scope(
    tmp_path: Path,
    answered: bool,
    overrides: dict[str, str],
    expected: DurableResolveStatus,
) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, command_response_allowed=answered).request
    _bind(store, request.request_id)
    if answered:
        assert store.resolve_command(
            request_id=request.request_id,
            option_slots=(1,),
            platform="discord",
            channel_id="thread-1",
            actor_id="actor-1",
        ).status is DurableResolveStatus.RESOLVED
    arguments = {
        "request_id": request.request_id,
        "option_slots": (1,),
        "platform": "discord",
        "channel_id": "thread-1",
        "actor_id": "actor-1",
    }
    arguments.update(overrides)

    result = store.resolve_command(**arguments)

    assert result.status is expected


def test_command_maps_slot_server_side_and_records_source(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, command_response_allowed=True).request
    _bind(store, request.request_id)

    result = store.resolve_command(
        request_id=request.request_id,
        option_slots=(2,),
        platform="discord",
        channel_id="thread-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.RESOLVED
    assert result.request is not None
    assert result.request.selected_option_id == "reject"
    assert (
        result.request.resolution_source
        is durable_requests.DurableRequestResolutionSource.COMMAND
    )


def test_command_and_native_resolution_race_has_one_winner(tmp_path: Path) -> None:
    clock = Clock()
    first_store = _store(tmp_path, clock)
    request = _create(first_store, command_response_allowed=True).request
    _bind(first_store, request.request_id)
    second_store = DurableRequestStore(tmp_path / "state.db", wall_clock=clock)
    barrier = threading.Barrier(2)
    results = []

    def resolve_native() -> None:
        barrier.wait()
        results.append(
            first_store.resolve(
                request_id=request.request_id,
                option_token="1",
                platform="discord",
                channel_id="thread-1",
                message_id="msg-1",
                actor_id="actor-1",
            )
        )

    def resolve_command() -> None:
        barrier.wait()
        results.append(
            second_store.resolve_command(
                request_id=request.request_id,
                option_slots=(2,),
                platform="discord",
                channel_id="thread-1",
                actor_id="actor-1",
            )
        )

    threads = [threading.Thread(target=resolve_native), threading.Thread(target=resolve_command)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result.status.value for result in results) == [
        DurableResolveStatus.ALREADY_RESOLVED.value,
        DurableResolveStatus.RESOLVED.value,
    ]
    winner = next(result.request for result in results if result.request is not None)
    persisted = second_store.get(request_id=request.request_id)
    assert persisted is not None
    assert persisted.selected_option_id == winner.selected_option_id
    assert persisted.resolution_source is winner.resolution_source


def test_public_command_resolution_facade_uses_default_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = durable_requests.DurableResolveResult(DurableResolveStatus.NOT_FOUND)

    class Store:
        def resolve_command(self, **kwargs):
            assert kwargs["option_slots"] == (2,)
            return expected

    monkeypatch.setattr(durable_requests, "get_durable_request_store", lambda: Store())

    assert durable_requests.resolve_durable_request_command(
        request_id="drq_valid",
        option_slots=(2,),
        platform="discord",
        channel_id="channel",
        actor_id="actor",
    ) is expected


def test_any_authorized_mode_accepts_authorized_actor_metadata(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store, actor=None).request
    _bind(store, request.request_id)

    result = store.resolve(
        request_id=request.request_id,
        option_token="1",
        platform="discord",
        channel_id="thread-1",
        message_id="msg-1",
        actor_id="another-authorized-user",
    )

    assert result.status is DurableResolveStatus.RESOLVED


def test_expiry_uses_persisted_absolute_deadline_across_restart(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store).request
    _bind(store, request.request_id)
    clock.value = request.expires_at

    restarted = DurableRequestStore(tmp_path / "state.db", wall_clock=clock)
    result = restarted.resolve(
        request_id=request.request_id,
        option_token="1",
        platform="discord",
        channel_id="thread-1",
        message_id="msg-1",
        actor_id="actor-1",
    )

    assert result.status is DurableResolveStatus.EXPIRED
    assert restarted.get(request_id=request.request_id).status is DurableRequestStatus.EXPIRED


def test_stale_delivery_claim_becomes_unknown_and_is_not_requeued(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store).request
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    clock.value += 6

    assert store.recover_stale_delivery_claims(stale_after_seconds=5) == 1
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert store.claim_next_pending(["discord"]) is None


def test_stale_delivery_recovery_can_filter_mode_without_changing_default(
    tmp_path: Path,
) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    text = _create(
        store,
        reference="text",
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    ).request
    text_claim = store.claim_next_pending(["discord"])
    assert text_claim is not None
    store.switch_claim_to_text_command(
        request_id=text_claim.request.request_id,
        claim_id=text_claim.claim_id,
    )
    native = _create(store, reference="native").request
    native_claim = store.claim_next_pending(["discord"])
    assert native_claim is not None
    clock.value += 6

    assert store.recover_stale_delivery_claims(
        stale_after_seconds=5,
        delivery_mode=DurableRequestDeliveryMode.TEXT_COMMAND,
    ) == 1
    assert store.get(request_id=text.request_id).status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert store.get(request_id=native.request_id).status is DurableRequestStatus.PENDING_DELIVERY
    assert store.recover_stale_delivery_claims(stale_after_seconds=5) == 1
    assert store.get(request_id=native.request_id).status is DurableRequestStatus.DELIVERY_UNKNOWN


def test_cancel_is_origin_scoped_and_terminal(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request

    assert (
        store.cancel(
            origin_type="session",
            origin_id="other",
            request_id=request.request_id,
        )
        is None
    )
    cancelled = store.cancel(
        origin_type="session",
        origin_id="session-1",
        request_id=request.request_id,
    )

    assert cancelled is not None
    assert cancelled.status is DurableRequestStatus.CANCELLED


def test_bound_terminal_rows_remain_restorable_for_stale_clicks(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request
    _bind(store, request.request_id)
    store.cancel(
        origin_type="session",
        origin_id="session-1",
        request_id=request.request_id,
    )

    rows = store.list_bound_requests(["discord"])

    assert [row.request_id for row in rows] == [request.request_id]
    assert rows[0].status is DurableRequestStatus.CANCELLED


def test_terminal_retention_prunes_content_and_reference(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store).request
    store.cancel(
        origin_type="session",
        origin_id="session-1",
        request_id=request.request_id,
    )
    clock.value += DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS + 1

    assert store.prune_terminal(DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS) == 1
    assert store.get(request_id=request.request_id) is None
    with sqlite3.connect(tmp_path / "state.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_requests").fetchone()[0] == 0


def test_safe_pre_send_failure_can_release_delivery_claim(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request
    claim = store.claim_next_pending(["discord"])

    assert claim is not None
    assert store.release_delivery_claim(
        request_id=request.request_id,
        claim_id=claim.claim_id,
    ) is True

    persisted = store.get(request_id=request.request_id)
    assert persisted is not None
    assert persisted.status is DurableRequestStatus.PENDING_DELIVERY
    assert persisted.delivery_claim is None
    assert persisted.delivery_claimed_at is None

    retry = store.claim_next_pending(["discord"])
    assert retry is not None
    assert retry.request.request_id == request.request_id
    assert retry.claim_id != claim.claim_id


def test_wrong_delivery_claim_cannot_be_released(tmp_path: Path) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request
    claim = store.claim_next_pending(["discord"])

    assert claim is not None
    assert store.release_delivery_claim(
        request_id=request.request_id,
        claim_id="wrong-claim",
    ) is False

    persisted = store.get(request_id=request.request_id)
    assert persisted is not None
    assert persisted.delivery_claim == claim.claim_id


def test_central_state_schema_contains_durable_request_ledger(tmp_path: Path) -> None:
    from hermes_state_common import SCHEMA_SQL, SCHEMA_VERSION

    conn = sqlite3.connect(tmp_path / "central-state.db")
    try:
        conn.executescript(SCHEMA_SQL)
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("durable_requests",),
        ).fetchone()
        index = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("idx_durable_requests_origin_reference",),
        ).fetchone()
    finally:
        conn.close()

    assert SCHEMA_VERSION >= 26
    assert table == ("durable_requests",)
    assert index == ("idx_durable_requests_origin_reference",)


def test_delivery_message_is_bound_when_expiry_wins_send_race(tmp_path: Path) -> None:
    clock = Clock()
    store = _store(tmp_path, clock)
    request = _create(store).request
    claim = store.claim_next_pending(["discord"])
    assert claim is not None

    clock.value = request.expires_at
    bound = store.bind_delivery(
        request_id=request.request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="late-message",
    )

    assert bound is not None
    assert bound.status is DurableRequestStatus.EXPIRED
    assert bound.message_id == "late-message"


def test_delivery_message_is_bound_when_cancellation_wins_send_race(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path, Clock())
    request = _create(store).request
    claim = store.claim_next_pending(["discord"])
    assert claim is not None

    cancelled = store.cancel(
        origin_type="session",
        origin_id="session-1",
        request_id=request.request_id,
    )
    assert cancelled is not None
    assert cancelled.status is DurableRequestStatus.CANCELLED

    bound = store.bind_delivery(
        request_id=request.request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="cancelled-message",
    )

    assert bound is not None
    assert bound.status is DurableRequestStatus.CANCELLED
    assert bound.message_id == "cancelled-message"


def test_concurrent_store_instances_resolve_exactly_once(tmp_path: Path) -> None:
    clock = Clock()
    first_store = _store(tmp_path, clock)
    request = _create(first_store).request
    _bind(first_store, request.request_id)
    second_store = DurableRequestStore(tmp_path / "state.db", wall_clock=clock)
    barrier = threading.Barrier(2)
    results = []

    def resolve(store: DurableRequestStore, token: str) -> None:
        barrier.wait()
        results.append(
            store.resolve(
                request_id=request.request_id,
                option_token=token,
                platform="discord",
                channel_id="thread-1",
                message_id="msg-1",
                actor_id="actor-1",
            ).status
        )

    threads = [
        threading.Thread(target=resolve, args=(first_store, "1")),
        threading.Thread(target=resolve, args=(second_store, "2")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(status.value for status in results) == [
        DurableResolveStatus.ALREADY_RESOLVED.value,
        DurableResolveStatus.RESOLVED.value,
    ]
