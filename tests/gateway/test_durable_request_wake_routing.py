from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import Platform
from gateway.durable_request_bridge import DurableWakeDisposition
from gateway.durable_requests import (
    DurableRequest,
    DurableRequestOption,
    DurableRequestStatus,
    DurableRequestTarget,
    DurableResponderMode,
    DurableWakeClaim,
    DurableWakeStatus,
)
from gateway.run import GatewayRunner, _format_gateway_process_notification
from gateway.session import SessionSource


def _request(*, origin_type: str = "session") -> DurableRequest:
    return DurableRequest(
        request_id="drq_abcdefghijklmnopqrstuvwxyz012345",
        kind="tool.durable_request",
        title="Approve release",
        body="Should the reviewed release continue?",
        options=(
            DurableRequestOption("option_1", "Approve"),
            DurableRequestOption("option_2", "Reject"),
        ),
        status=DurableRequestStatus.SELECTED,
        created_at=1.0,
        updated_at=2.0,
        expires_at=100.0,
        target=DurableRequestTarget("discord", "inbox"),
        responder_mode=(
            DurableResponderMode.INITIATOR
            if origin_type == "session"
            else DurableResponderMode.ANY_AUTHORIZED
        ),
        expected_actor_id="actor" if origin_type == "session" else None,
        origin_type=origin_type,
        origin_id=(
            "agent:main:discord:dm:conversation:actor"
            if origin_type == "session"
            else "cron-job"
        ),
        reference="weekly-release",
        wake_requested=True,
        wake_status=DurableWakeStatus.CLAIMED,
        origin_session_id=("physical-session" if origin_type == "session" else None),
        origin_ui_session_id=("ui-session" if origin_type == "session" else None),
        origin_profile="operations",
        message_id="approval-message",
        selected_option_id="option_1",
        resolved_by_actor_id="operator",
        resolved_at=2.0,
        wake_attempts=1,
        wake_claim="claim",
        wake_claimed_at=3.0,
    )


def _claim(*, origin_type: str = "session") -> DurableWakeClaim:
    return DurableWakeClaim(request=_request(origin_type=origin_type), claim_id="claim")


@pytest.mark.asyncio
async def test_live_session_wake_injects_one_fresh_internal_turn() -> None:
    runner = object.__new__(GatewayRunner)
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="conversation",
        chat_type="dm",
        user_id="actor",
        profile="operations",
    )
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._build_process_event_source = MagicMock(return_value=source)
    runner._delivery_adapter_for = MagicMock(return_value=object())
    runner._inject_watch_notification = AsyncMock(return_value=True)

    result = await runner._dispatch_durable_request_wake(
        _claim(),
        profile_name="operations",
        adapters={Platform.DISCORD: object()},
    )

    assert result.disposition is DurableWakeDisposition.DELIVERED
    runner._classify_completion_target.assert_awaited_once_with("physical-session")
    runner._inject_watch_notification.assert_awaited_once()
    synth_text, event = runner._inject_watch_notification.await_args.args
    assert event["type"] == "durable_request_terminal"
    assert event["session_key"] == "agent:main:discord:dm:conversation:actor"
    assert event["parent_session_id"] == "physical-session"
    assert event["profile"] == "operations"
    assert event["message_id"] == "durable-request:drq_abcdefghijklmnopqrstuvwxyz012345"
    assert "Selected option: 1. Approve" in synth_text
    assert "not an authorization token" in synth_text
    for forbidden in ("approval-message", "operator", "actor"):
        assert forbidden not in synth_text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_verdict", "disposition", "reason"),
    [
        ("terminal", DurableWakeDisposition.DROPPED, "origin_session_ended"),
        ("retry", DurableWakeDisposition.RETRY, "origin_session_unavailable"),
    ],
)
async def test_session_lineage_controls_wake_disposition(
    target_verdict: str,
    disposition: DurableWakeDisposition,
    reason: str,
) -> None:
    runner = object.__new__(GatewayRunner)
    runner._classify_completion_target = AsyncMock(return_value=target_verdict)
    runner._inject_watch_notification = AsyncMock()

    result = await runner._dispatch_durable_request_wake(
        _claim(),
        profile_name="operations",
        adapters={},
    )

    assert result.disposition is disposition
    assert result.reason == reason
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_session_wake_retries_when_profile_adapter_is_offline() -> None:
    runner = object.__new__(GatewayRunner)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._build_process_event_source = MagicMock(
        return_value=SessionSource(
            platform=Platform.DISCORD,
            chat_id="conversation",
            chat_type="dm",
            profile="operations",
        )
    )
    runner._delivery_adapter_for = MagicMock(return_value=None)
    runner._inject_watch_notification = AsyncMock()

    result = await runner._dispatch_durable_request_wake(
        _claim(),
        profile_name="operations",
        adapters={},
    )

    assert result.disposition is DurableWakeDisposition.RETRY
    assert result.reason == "origin_adapter_unavailable"
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scheduler_outcome", "scheduler_reason", "disposition"),
    [
        ("accepted", None, DurableWakeDisposition.DELIVERED),
        ("retry", "origin_job_running", DurableWakeDisposition.RETRY),
        ("dropped", "origin_job_missing", DurableWakeDisposition.DROPPED),
    ],
)
async def test_cron_wake_maps_schedule_neutral_continuation_result(
    monkeypatch,
    scheduler_outcome: str,
    scheduler_reason: str | None,
    disposition: DurableWakeDisposition,
) -> None:
    import cron.scheduler as scheduler

    calls = []

    def enqueue(**kwargs):
        calls.append(kwargs)
        return scheduler_outcome, scheduler_reason

    monkeypatch.setattr(scheduler, "enqueue_durable_request_continuation", enqueue)
    runner = object.__new__(GatewayRunner)
    adapters = {Platform.DISCORD: object()}

    result = await runner._dispatch_durable_request_wake(
        _claim(origin_type="cron"),
        profile_name="operations",
        adapters=adapters,
    )

    assert result.disposition is disposition
    assert result.reason == scheduler_reason
    assert len(calls) == 1
    call = calls[0]
    assert call["job_id"] == "cron-job"
    assert call["request_id"] == "drq_abcdefghijklmnopqrstuvwxyz012345"
    assert call["selected_index"] == 1
    assert call["selected_label"] == "Approve"
    assert call["adapters"] is adapters


@pytest.mark.asyncio
async def test_synthetic_wake_uses_secondary_profile_adapter() -> None:
    async def accept(event):
        event._gateway_accepted = True

    default_adapter = SimpleNamespace(handle_message=AsyncMock())
    profile_adapter = SimpleNamespace(handle_message=AsyncMock(side_effect=accept))
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.DISCORD: default_adapter}
    runner._profile_adapters = {
        "operations": {Platform.DISCORD: profile_adapter},
    }
    runner._active_profile_name = lambda: "primary"
    runner._registered_transport_adapter = lambda _source: None
    runner.session_store = SimpleNamespace(
        _ensure_loaded=lambda: None,
        _entries={},
    )
    runner._session_source_cache = {}
    runner._get_cached_session_source = lambda _session_key: None

    event = {
        "type": "durable_request_terminal",
        "request_id": "drq_abcdefghijklmnopqrstuvwxyz012345",
        "session_key": "agent:operations:discord:dm:conversation:actor",
        "parent_session_id": "physical-session",
        "platform": "discord",
        "chat_type": "dm",
        "chat_id": "conversation",
        "profile": "operations",
        "message_id": "durable-request:drq_abcdefghijklmnopqrstuvwxyz012345",
    }

    delivered = await runner._inject_watch_notification("terminal event", event)

    assert delivered is True
    default_adapter.handle_message.assert_not_awaited()
    profile_adapter.handle_message.assert_awaited_once()
    injected = profile_adapter.handle_message.await_args.args[0]
    assert injected.internal is True
    assert injected.source.profile == "operations"
    assert injected.metadata["gateway_session_id"] == "physical-session"


def test_terminal_event_formatter_is_self_contained_and_provider_neutral() -> None:
    text = _format_gateway_process_notification(
        {
            "type": "durable_request_terminal",
            "request_id": "drq_abcdefghijklmnopqrstuvwxyz012345",
            "reference": "weekly-release",
            "status": "selected",
            "title": "Approve release",
            "question": "Should the reviewed release continue?",
            "selected_index": 1,
            "selected_label": "Approve",
            "message_id": "discord-message",
            "actor_id": "operator",
            "channel_id": "inbox",
        }
    )

    assert text is not None
    assert "Request ID: drq_abcdefghijklmnopqrstuvwxyz012345" in text
    assert "Reference: weekly-release" in text
    assert "Selected option: 1. Approve" in text
    assert "discord-message" not in text
    assert "operator" not in text
    assert "inbox" not in text


def test_multi_wake_preserves_canonical_indices_labels_without_option_ids():
    event = {"type": "durable_request_terminal", "request_id": "drq_test",
             "status": "selected", "title": "Bills", "question": "Choose bills.",
             "selected_indices": [1, 3], "selected_labels": ["Electricity", "Water"],
             "selected_option_ids": ["private_first", "private_third"]}
    text = _format_gateway_process_notification(event)
    assert "Selected options:\n1. Electricity\n3. Water" in text
    assert "private_first" not in text and "private_third" not in text
    assert "not an authorization token" in text


@pytest.mark.parametrize("status", ["expired", "cancelled"])
def test_unselected_multi_wake_keeps_terminal_reason(status):
    text = _format_gateway_process_notification({
        "type": "durable_request_terminal", "status": status, "selected_indices": [],
        "selected_labels": [], "terminal_reason": "deadline_elapsed",
    })
    assert "Reason: deadline_elapsed" in text
    assert "Selected options:" not in text
