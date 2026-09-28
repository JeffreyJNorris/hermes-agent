from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import gateway.durable_request_bridge as bridge_module
from gateway.durable_request_bridge import (
    DurableRequestBridge,
    DurableWakeDispatchResult,
    DurableWakeDisposition,
)
from gateway.durable_requests import (
    DurableRequestDeliveryMode,
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStatus,
    DurableRequestStore,
    DurableRequestTarget,
    DurableRequestsConfig,
    DurableResponderMode,
    DurableWakeStatus,
)
from gateway.platforms.base import SendResult


class Adapter:
    def __init__(
        self,
        *,
        send_result: SendResult | None = None,
        restore_result: SendResult | None = None,
        send_error: Exception | None = None,
        text_result: SendResult | None = None,
        text_error: Exception | None = None,
    ) -> None:
        self.send_result = send_result or SendResult(success=True, message_id="m1")
        self.restore_result = restore_result or SendResult(success=True, message_id="m1")
        self.send_error = send_error
        self.text_result = text_result or SendResult(success=True, message_id="tm1")
        self.text_error = text_error
        self.sent = []
        self.text_sent = []
        self.restored = []

    async def send_durable_request(self, chat_id, request, metadata=None):
        self.sent.append((chat_id, request, metadata))
        if self.send_error is not None:
            raise self.send_error
        return self.send_result

    async def send_durable_request_text(self, chat_id, request, metadata=None):
        self.text_sent.append((chat_id, request, metadata))
        if self.text_error is not None:
            raise self.text_error
        return self.text_result

    async def restore_durable_request(self, request):
        self.restored.append(request)
        return self.restore_result


def _config() -> DurableRequestsConfig:
    return DurableRequestsConfig(
        enabled=True,
        target=DurableRequestTarget("discord", "chat"),
        default_ttl_seconds=86400,
        max_ttl_seconds=604800,
        terminal_retention_seconds=604800,
        scheduled_responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        command_responses_enabled=True,
        allow_text_delivery_fallback=True,
    )


def _create(
    store: DurableRequestStore,
    reference: str = "ref",
    *,
    wake_on_terminal: bool = False,
    command_response_allowed: bool = False,
    text_command_fallback_allowed: bool = False,
):
    return store.create(
        DurableRequestSpec(
            kind="test",
            title="Choose",
            body="Choose one.",
            options=(
                DurableRequestOption("a", "First"),
                DurableRequestOption("b", "Second"),
            ),
            ttl_seconds=3600,
            reference=reference,
        ),
        target=DurableRequestTarget("discord", "chat", "thread"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="cron",
        origin_id="job",
        wake_on_terminal=wake_on_terminal,
        command_response_allowed=command_response_allowed,
        text_command_fallback_allowed=text_command_fallback_allowed,
    ).request


@pytest.mark.asyncio
async def test_native_unsupported_falls_back_to_text_once(
    store: DurableRequestStore,
    monkeypatch,
) -> None:
    events = []
    monkeypatch.setattr(
        "gateway.durable_operations.record_durable_delivery_event", events.append
    )
    request = _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    adapter = Adapter(
        send_result=SendResult(success=False, error_kind="unsupported"),
        text_result=SendResult(success=True, message_id="text-7"),
    )

    await DurableRequestBridge().process_once({"discord": adapter})
    await DurableRequestBridge().process_once({"discord": adapter})

    persisted = store.get(request_id=request.request_id)
    assert persisted is not None
    assert persisted.status is DurableRequestStatus.OPEN
    assert persisted.delivery_mode is DurableRequestDeliveryMode.TEXT_COMMAND
    assert persisted.message_id == "text-7"
    assert len(adapter.sent) == 1
    assert len(adapter.text_sent) == 1
    assert events == ["native_to_text_fallback"]


@pytest.mark.asyncio
async def test_native_success_never_sends_text_fallback(
    store: DurableRequestStore,
) -> None:
    request = _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    adapter = Adapter(send_result=SendResult(success=True, message_id="native-7"))

    await DurableRequestBridge().process_once({"discord": adapter})

    assert store.get(request_id=request.request_id).message_id == "native-7"
    assert adapter.text_sent == []


@pytest.mark.asyncio
async def test_ambiguous_native_failure_never_sends_text_fallback(
    store: DurableRequestStore,
) -> None:
    request = _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    adapter = Adapter(
        send_result=SendResult(success=False, error_kind="delivery_unknown")
    )

    await DurableRequestBridge().process_once({"discord": adapter})

    assert store.get(request_id=request.request_id).status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert adapter.text_sent == []


@pytest.mark.asyncio
async def test_ambiguous_text_delivery_is_not_retried_after_restart(
    store: DurableRequestStore,
    monkeypatch,
) -> None:
    events = []
    monkeypatch.setattr(
        "gateway.durable_operations.record_durable_delivery_event", events.append
    )
    request = _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    adapter = Adapter(
        send_result=SendResult(success=False, error_kind="unsupported"),
        text_result=SendResult(success=False, error_kind="delivery_unknown"),
    )

    await DurableRequestBridge().process_once({"discord": adapter})
    await DurableRequestBridge().process_once({"discord": adapter})

    persisted = store.get(request_id=request.request_id)
    assert persisted.status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert len(adapter.text_sent) == 1
    assert events == ["native_to_text_fallback", "text_delivery_unknown"]


@pytest.mark.asyncio
async def test_definite_text_delivery_failure_is_counted_once(
    store: DurableRequestStore,
    monkeypatch,
) -> None:
    events = []
    monkeypatch.setattr(
        "gateway.durable_operations.record_durable_delivery_event", events.append
    )
    _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    adapter = Adapter(
        send_result=SendResult(success=False, error_kind="unsupported"),
        text_result=SendResult(success=False, error_kind="forbidden"),
    )

    await DurableRequestBridge().process_once({"discord": adapter})
    await DurableRequestBridge().process_once({"discord": adapter})

    assert events == ["native_to_text_fallback", "text_delivery_failed"]


@pytest.mark.asyncio
async def test_stale_text_delivery_claim_is_recovered_and_counted_once(
    store: DurableRequestStore,
    monkeypatch,
) -> None:
    events = []
    monkeypatch.setattr(
        "gateway.durable_operations.record_durable_delivery_event",
        lambda event, count=1: events.extend([event] * count),
    )
    request = _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    store.switch_claim_to_text_command(
        request_id=request.request_id,
        claim_id=claim.claim_id,
    )
    with sqlite3.connect(store._db_path) as db:
        db.execute(
            "UPDATE durable_requests SET delivery_claimed_at = 0 WHERE request_id = ?",
            (request.request_id,),
        )

    bridge = DurableRequestBridge()
    await bridge.process_once({"discord": Adapter()})
    await bridge.process_once({"discord": Adapter()})

    assert store.get(request_id=request.request_id).status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert events == ["text_delivery_unknown"]


@pytest.mark.asyncio
async def test_bound_text_request_skips_native_view_restore(
    store: DurableRequestStore,
) -> None:
    _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=True,
    )
    first = Adapter(send_result=SendResult(success=False, error_kind="unsupported"))
    await DurableRequestBridge().process_once({"discord": first})

    restarted = Adapter()
    await DurableRequestBridge().process_once({"discord": restarted})

    assert restarted.restored == []
    assert restarted.sent == []
    assert restarted.text_sent == []


@pytest.mark.asyncio
async def test_config_change_cannot_enable_fallback_for_existing_request(
    store: DurableRequestStore,
    monkeypatch,
) -> None:
    request = _create(
        store,
        command_response_allowed=True,
        text_command_fallback_allowed=False,
    )
    monkeypatch.setattr(
        bridge_module,
        "load_durable_requests_config",
        lambda: DurableRequestsConfig(
            **{
                **_config().__dict__,
                "allow_text_delivery_fallback": True,
            }
        ),
    )
    adapter = Adapter(send_result=SendResult(success=False, error_kind="unsupported"))

    await DurableRequestBridge().process_once({"discord": adapter})

    persisted = store.get(request_id=request.request_id)
    assert persisted.status is DurableRequestStatus.UNSUPPORTED
    assert adapter.text_sent == []


@pytest.mark.asyncio
async def test_config_change_cannot_disable_persisted_fallback(
    store: DurableRequestStore,
    monkeypatch,
) -> None:
    request = _create(
        store,
        command_response_allowed=False,
        text_command_fallback_allowed=True,
    )
    monkeypatch.setattr(
        bridge_module,
        "load_durable_requests_config",
        lambda: DurableRequestsConfig(
            **{
                **_config().__dict__,
                "allow_text_delivery_fallback": False,
            }
        ),
    )
    adapter = Adapter(send_result=SendResult(success=False, error_kind="unsupported"))

    await DurableRequestBridge().process_once({"discord": adapter})

    persisted = store.get(request_id=request.request_id)
    assert persisted.status is DurableRequestStatus.OPEN
    assert persisted.delivery_mode is DurableRequestDeliveryMode.TEXT_COMMAND
    assert persisted.command_response_allowed is True
    assert len(adapter.text_sent) == 1


@pytest.fixture
def store(tmp_path: Path, monkeypatch) -> DurableRequestStore:
    value = DurableRequestStore(tmp_path / "state.db")
    monkeypatch.setattr(bridge_module, "get_durable_request_store", lambda: value)
    monkeypatch.setattr(bridge_module, "load_durable_requests_config", _config)
    return value


@pytest.mark.asyncio
async def test_successful_delivery_binds_exact_message_before_restore(
    store: DurableRequestStore,
) -> None:
    request = _create(store)
    adapter = Adapter(send_result=SendResult(success=True, message_id="message-7"))
    bridge = DurableRequestBridge()

    await bridge.process_once({"discord": adapter})
    persisted = store.get(request_id=request.request_id)
    await bridge.process_once({"discord": adapter})

    assert persisted is not None
    assert persisted.status is DurableRequestStatus.OPEN
    assert persisted.message_id == "message-7"
    assert len(adapter.sent) == 1
    assert adapter.sent[0][0] == "chat"
    assert adapter.sent[0][2] == {"thread_id": "thread"}
    assert adapter.restored == []


@pytest.mark.asyncio
async def test_new_bridge_restores_bound_view_after_restart(
    store: DurableRequestStore,
) -> None:
    request = _create(store)
    first = Adapter(send_result=SendResult(success=True, message_id="message-7"))
    await DurableRequestBridge().process_once({"discord": first})

    restarted_adapter = Adapter(
        restore_result=SendResult(success=True, message_id="message-7")
    )
    await DurableRequestBridge().process_once({"discord": restarted_adapter})

    assert [row.request_id for row in restarted_adapter.restored] == [
        request.request_id
    ]
    assert restarted_adapter.sent == []


@pytest.mark.asyncio
async def test_missing_message_id_is_delivery_unknown(store: DurableRequestStore) -> None:
    request = _create(store)
    adapter = Adapter(send_result=SendResult(success=True))

    await DurableRequestBridge().process_once({"discord": adapter})
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert persisted.terminal_reason == "missing_message_id"


@pytest.mark.asyncio
async def test_raised_provider_send_is_delivery_unknown(store: DurableRequestStore) -> None:
    request = _create(store)
    adapter = Adapter(send_error=RuntimeError("socket closed after send"))

    await DurableRequestBridge().process_once({"discord": adapter})
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.status is DurableRequestStatus.DELIVERY_UNKNOWN
    assert persisted.terminal_reason == "provider_send_outcome_unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "status"),
    [
        (
            SendResult(
                success=False,
                error="unsupported",
                error_kind="unsupported",
            ),
            DurableRequestStatus.UNSUPPORTED,
        ),
        (
            SendResult(
                success=False,
                error="forbidden",
                error_kind="forbidden",
            ),
            DurableRequestStatus.DELIVERY_FAILED,
        ),
        (
            SendResult(
                success=False,
                error="ambiguous",
                error_kind="delivery_unknown",
            ),
            DurableRequestStatus.DELIVERY_UNKNOWN,
        ),
    ],
)
async def test_explicit_delivery_results_map_to_terminal_status(
    store: DurableRequestStore,
    result: SendResult,
    status: DurableRequestStatus,
) -> None:
    request = _create(store)

    await DurableRequestBridge().process_once(
        {"discord": Adapter(send_result=result)}
    )

    assert store.get(request_id=request.request_id).status is status


@pytest.mark.asyncio
async def test_permanently_missing_open_message_is_failed(
    store: DurableRequestStore,
) -> None:
    request = _create(store)
    await DurableRequestBridge().process_once(
        {"discord": Adapter(send_result=SendResult(success=True, message_id="m"))}
    )
    restarted = Adapter(
        restore_result=SendResult(
            success=False,
            error="Unknown Message",
            error_kind="not_found",
        )
    )

    await DurableRequestBridge().process_once({"discord": restarted})
    persisted = store.get(request_id=request.request_id)

    assert persisted.status is DurableRequestStatus.DELIVERY_FAILED
    assert persisted.terminal_reason == "provider_message_missing"


@pytest.mark.asyncio
async def test_terminal_bound_request_is_restored_for_accurate_stale_clicks(
    store: DurableRequestStore,
) -> None:
    request = _create(store)
    await DurableRequestBridge().process_once(
        {"discord": Adapter(send_result=SendResult(success=True, message_id="m"))}
    )
    store.cancel(
        origin_type="cron",
        origin_id="job",
        request_id=request.request_id,
    )
    restarted = Adapter(restore_result=SendResult(success=True, message_id="m"))

    await DurableRequestBridge().process_once({"discord": restarted})

    assert len(restarted.restored) == 1
    assert restarted.restored[0].status is DurableRequestStatus.CANCELLED


@pytest.mark.asyncio
async def test_no_adapter_leaves_pending_request_unclaimed(
    store: DurableRequestStore,
) -> None:
    request = _create(store)

    await DurableRequestBridge().process_once({})
    persisted = store.get(request_id=request.request_id)

    assert persisted.status is DurableRequestStatus.PENDING_DELIVERY
    assert persisted.delivery_claim is None


@pytest.mark.asyncio
@pytest.mark.parametrize("error_kind", ["transient", "rate_limited"])
async def test_safe_pre_send_failure_is_requeued_without_tight_retry(
    store: DurableRequestStore,
    error_kind: str,
) -> None:
    request = _create(store)
    adapter = Adapter(
        send_result=SendResult(
            success=False,
            error="temporarily unavailable",
            error_kind=error_kind,
        )
    )

    await DurableRequestBridge().process_once({"discord": adapter})
    persisted = store.get(request_id=request.request_id)

    assert len(adapter.sent) == 1
    assert persisted is not None
    assert persisted.status is DurableRequestStatus.PENDING_DELIVERY
    assert persisted.delivery_claim is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_kind", "reason"),
    [
        ("forbidden", "provider_message_unavailable"),
        ("bad_format", "persistent_view_restore_failed"),
    ],
)
async def test_permanent_restore_failure_closes_open_request(
    store: DurableRequestStore,
    error_kind: str,
    reason: str,
) -> None:
    request = _create(store)
    await DurableRequestBridge().process_once(
        {"discord": Adapter(send_result=SendResult(success=True, message_id="m"))}
    )
    restarted = Adapter(
        restore_result=SendResult(
            success=False,
            error="permanent restore failure",
            error_kind=error_kind,
        )
    )

    await DurableRequestBridge().process_once({"discord": restarted})
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.status is DurableRequestStatus.DELIVERY_FAILED
    assert persisted.terminal_reason == reason


@pytest.mark.asyncio
async def test_disabled_feature_does_not_open_or_poll_the_ledger(monkeypatch) -> None:
    monkeypatch.setattr(
        bridge_module,
        "load_durable_requests_config",
        lambda: DurableRequestsConfig(
            enabled=False,
            target=None,
            default_ttl_seconds=86400,
            max_ttl_seconds=604800,
            terminal_retention_seconds=604800,
            scheduled_responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            reason="disabled",
        ),
    )

    def fail_if_opened():
        raise AssertionError("disabled durable requests must not open state.db")

    monkeypatch.setattr(
        bridge_module,
        "get_durable_request_store",
        fail_if_opened,
    )

    await DurableRequestBridge().process_once({"discord": Adapter()})


@pytest.mark.asyncio
async def test_terminal_wake_is_acknowledged_after_handler_accepts(
    store: DurableRequestStore,
) -> None:
    request = _create(store, wake_on_terminal=True)
    store.cancel(
        origin_type="cron",
        origin_id="job",
        request_id=request.request_id,
    )
    calls = []

    async def handler(claim):
        calls.append(claim)
        return DurableWakeDispatchResult(DurableWakeDisposition.DELIVERED)

    bridge = DurableRequestBridge()
    bridge.set_wake_handler(handler)
    await bridge.process_once({})
    persisted = store.get(request_id=request.request_id)

    assert len(calls) == 1
    assert calls[0].request.request_id == request.request_id
    assert persisted is not None
    assert persisted.status is DurableRequestStatus.CANCELLED
    assert persisted.wake_status is DurableWakeStatus.DELIVERED


@pytest.mark.asyncio
async def test_retryable_wake_is_released_without_tight_loop(
    store: DurableRequestStore,
) -> None:
    request = _create(store, wake_on_terminal=True)
    store.cancel(
        origin_type="cron",
        origin_id="job",
        request_id=request.request_id,
    )
    calls = 0

    async def handler(_claim):
        nonlocal calls
        calls += 1
        return DurableWakeDispatchResult(
            DurableWakeDisposition.RETRY,
            "origin_adapter_unavailable",
        )

    bridge = DurableRequestBridge()
    bridge.set_wake_handler(handler)
    await bridge.process_once({})
    persisted = store.get(request_id=request.request_id)

    assert calls == 1
    assert persisted is not None
    assert persisted.wake_status is DurableWakeStatus.PENDING
    assert persisted.wake_claim is None


@pytest.mark.asyncio
async def test_permanently_undeliverable_wake_is_dropped(
    store: DurableRequestStore,
) -> None:
    request = _create(store, wake_on_terminal=True)
    store.cancel(
        origin_type="cron",
        origin_id="job",
        request_id=request.request_id,
    )

    async def handler(_claim):
        return DurableWakeDispatchResult(
            DurableWakeDisposition.DROPPED,
            "origin_job_missing",
        )

    bridge = DurableRequestBridge()
    bridge.set_wake_handler(handler)
    await bridge.process_once({})
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.wake_status is DurableWakeStatus.DROPPED
    assert persisted.wake_reason == "origin_job_missing"


@pytest.mark.asyncio
async def test_handler_exception_releases_wake_for_later_retry(
    store: DurableRequestStore,
) -> None:
    request = _create(store, wake_on_terminal=True)
    store.cancel(
        origin_type="cron",
        origin_id="job",
        request_id=request.request_id,
    )

    async def handler(_claim):
        raise RuntimeError("temporary routing failure")

    bridge = DurableRequestBridge()
    bridge.set_wake_handler(handler)
    await bridge.process_once({})
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.wake_status is DurableWakeStatus.PENDING
    assert persisted.wake_claim is None


@pytest.mark.asyncio
async def test_worker_cancellation_leaves_ambiguous_wake_claimed(
    store: DurableRequestStore,
) -> None:
    request = _create(store, wake_on_terminal=True)
    store.cancel(
        origin_type="cron",
        origin_id="job",
        request_id=request.request_id,
    )

    async def cancelled_handler(_claim):
        raise __import__("asyncio").CancelledError

    bridge = DurableRequestBridge()
    bridge.set_wake_handler(cancelled_handler)
    with pytest.raises(__import__("asyncio").CancelledError):
        await bridge.process_once({})
    persisted = store.get(request_id=request.request_id)

    assert persisted is not None
    assert persisted.wake_status is DurableWakeStatus.CLAIMED
    assert persisted.wake_claim is not None
