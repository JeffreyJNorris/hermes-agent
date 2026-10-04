from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.durable_requests import DurableResolveResult, DurableResolveStatus
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource
from hermes_cli.commands import resolve_command


REQUEST_ID = "drq_" + "A" * 32


def _event(text: str, *, thread_id: str | None = None) -> MessageEvent:
    return MessageEvent(
        text=text,
        source=SessionSource(
            platform=Platform.DISCORD,
            chat_id="channel-1",
            thread_id=thread_id,
            user_id="actor-1",
        ),
    )


def _runner():
    from gateway.run import GatewayRunner

    return object.__new__(GatewayRunner)


def test_request_command_is_gateway_only_and_dispatches_while_busy():
    command = resolve_command("request")

    assert command is not None
    assert command.gateway_only is True
    assert command.args_hint == "respond <request-id> <number[,number...]>"
    assert command.busy_policy == "dispatch"
    runner = _runner()
    assert runner._gateway_plain_command_handlers()["request"] == runner._handle_request_command


@pytest.mark.asyncio
async def test_valid_request_response_uses_actual_scope(monkeypatch):
    observed = {}
    recorded = []

    def resolve(**kwargs):
        observed.update(kwargs)
        return SimpleNamespace(status=DurableResolveStatus.RESOLVED)

    monkeypatch.setattr("gateway.durable_requests.resolve_durable_request_command", resolve)
    monkeypatch.setattr(
        "gateway.durable_operations.record_durable_command_response",
        recorded.append,
    )

    result = await _runner()._handle_request_command(
        _event(f"/request respond {REQUEST_ID} 2", thread_id="thread-9")
    )

    assert result == "Response recorded."
    assert observed == {
        "request_id": REQUEST_ID,
        "option_slots": (2,),
        "platform": "discord",
        "channel_id": "thread-9",
        "actor_id": "actor-1",
    }
    assert recorded == [DurableResolveStatus.RESOLVED]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args",
    [
        "",
        "respond",
        f"respond {REQUEST_ID}",
        f"respond {REQUEST_ID} 0",
        f"respond {REQUEST_ID} approve",
        f"respond {REQUEST_ID} option_internal_id",
        f"respond {REQUEST_ID} 1,,3",
        f"respond {REQUEST_ID} 1-3",
        f"respond {REQUEST_ID} 1 3",
        f"respond drq_short 1",
        f"respond {REQUEST_ID} 1 trailing",
        f"Respond {REQUEST_ID} 1",
    ],
)
async def test_malformed_request_commands_return_usage_without_store_lookup(
    monkeypatch, args
):
    resolve = AsyncMock()
    monkeypatch.setattr("gateway.durable_requests.resolve_durable_request_command", resolve)

    result = await _runner()._handle_request_command(_event(f"/request {args}".rstrip()))

    assert result == "Usage: /request respond <request-id> <number[,number...]>"
    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_command_rejects_oversized_input_before_parsing(monkeypatch):
    resolve = AsyncMock()
    monkeypatch.setattr("gateway.durable_requests.resolve_durable_request_command", resolve)

    result = await _runner()._handle_request_command(_event("/request " + "x" * 1000))

    assert result == "Usage: /request respond <request-id> <number[,number...]>"
    resolve.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "message"),
    [
        (DurableResolveStatus.ALREADY_RESOLVED, "This request has already been answered."),
        (DurableResolveStatus.EXPIRED, "This request has expired."),
        (DurableResolveStatus.CANCELLED, "This request is no longer active."),
        (DurableResolveStatus.INVALID_OPTION, "That option is not valid for this request."),
        (DurableResolveStatus.WRONG_CHANNEL, "This request is unavailable here."),
        (DurableResolveStatus.WRONG_ACTOR, "You cannot answer this request."),
        (DurableResolveStatus.NOT_FOUND, "This request is unavailable."),
        (DurableResolveStatus.COMMAND_DISABLED, "This request cannot be answered by command."),
        (DurableResolveStatus.SCOPE_MISMATCH, "This request is unavailable."),
        (DurableResolveStatus.DELIVERY_UNKNOWN, "This request is unavailable."),
    ],
)
async def test_request_command_maps_results_without_scope_details(
    monkeypatch, status, message
):
    monkeypatch.setattr(
        "gateway.durable_requests.resolve_durable_request_command",
        lambda **_kwargs: DurableResolveResult(status),
    )

    result = await _runner()._handle_request_command(
        _event(f"/request respond {REQUEST_ID} 1")
    )

    assert result == message
    assert "channel-1" not in result
    assert "actor-1" not in result
    assert REQUEST_ID not in result


@pytest.mark.asyncio
@pytest.mark.parametrize("slots", ["3,1", "3, 1", "1,1"])
async def test_request_command_passes_complete_numeric_set_to_authoritative_store(monkeypatch, slots):
    calls = []
    def resolve(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status=DurableResolveStatus.RESOLVED)
    monkeypatch.setattr("gateway.durable_requests.resolve_durable_request_command", resolve)
    assert await _runner()._handle_request_command(_event(f"/request respond {REQUEST_ID} {slots}")) == "Response recorded."
    assert calls[0]["option_slots"] == tuple(int(part) for part in slots.split(","))
