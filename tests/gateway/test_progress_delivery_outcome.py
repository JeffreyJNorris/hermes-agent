"""Progress cleanup can distinguish final delivery success from run completion."""

import asyncio

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, SendResult
from gateway.session import SessionSource


class DeliveryAdapter(BasePlatformAdapter):
    def __init__(self, *, send_success=True):
        super().__init__(PlatformConfig(enabled=True, typing_indicator=False), Platform.DISCORD)
        self.send_success = send_success
        self.sent = []

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(content)
        return SendResult(
            success=self.send_success,
            message_id="final" if self.send_success else None,
            error=None if self.send_success else "Forbidden",
        )

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


def make_turn(tmp_path, monkeypatch, *, send_success=True):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    adapter = DeliveryAdapter(send_success=send_success)
    event = MessageEvent(
        text="Question",
        source=SessionSource(platform=Platform.DISCORD, chat_id="42", user_id="7"),
    )
    interrupt_event = asyncio.Event()
    interrupt_event._hermes_run_generation = 7
    adapter._active_sessions["session"] = interrupt_event
    observed = []
    adapter.register_post_delivery_callback(
        "session",
        lambda: observed.append(getattr(interrupt_event, "_hermes_delivery_outcome", None)),
        generation=7,
    )
    return adapter, event, interrupt_event, observed


@pytest.mark.asyncio
@pytest.mark.parametrize("send_success", [True, False])
async def test_delivery_outcome_visible_to_callbacks_even_on_send_failure(
    tmp_path, monkeypatch, send_success,
):
    adapter, event, interrupt_event, observed = make_turn(
        tmp_path, monkeypatch, send_success=send_success,
    )

    async def handler(event):
        return "The answer."

    adapter._message_handler = handler
    await adapter._process_message_background(event, "session")

    assert adapter.sent
    assert observed == [(7, send_success)]
    assert interrupt_event._hermes_delivery_outcome == (7, send_success)


@pytest.mark.asyncio
async def test_handler_exception_keeps_cleanup_disabled_but_invokes_callbacks(tmp_path, monkeypatch):
    adapter, event, _, observed = make_turn(tmp_path, monkeypatch)

    async def handler(event):
        raise RuntimeError("Run failed")

    adapter._message_handler = handler
    await adapter._process_message_background(event, "session")

    # Even a successfully delivered error notice is not a successful final answer.
    assert adapter.sent
    assert observed == [(7, False)]


@pytest.mark.asyncio
async def test_cancellation_keeps_cleanup_disabled_but_invokes_callbacks(tmp_path, monkeypatch):
    adapter, event, _, observed = make_turn(tmp_path, monkeypatch)
    started = asyncio.Event()

    async def handler(event):
        started.set()
        await asyncio.Event().wait()

    adapter._message_handler = handler
    task = asyncio.create_task(adapter._process_message_background(event, "session"))
    await asyncio.wait_for(started.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert observed == [(7, False)]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_already_delivered_response_keeps_success_outcome(tmp_path, monkeypatch):
    adapter, event, _, observed = make_turn(tmp_path, monkeypatch)

    async def handler(event):
        # Gateway returns None after confirmed streamed delivery.
        return None

    adapter._message_handler = handler
    await adapter._process_message_background(event, "session")

    assert observed == [(7, True)]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_interrupted_turn_preserves_status_even_without_a_reply(tmp_path, monkeypatch):
    adapter, event, interrupt_event, observed = make_turn(tmp_path, monkeypatch)

    async def handler(event):
        interrupt_event.set()
        return None

    adapter._message_handler = handler
    await adapter._process_message_background(event, "session")

    assert observed == [(7, False)]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_interrupted_turn_keeps_failure_outcome_when_handing_off_queued_message(
    tmp_path, monkeypatch,
):
    adapter, event, interrupt_event, observed = make_turn(tmp_path, monkeypatch)
    drained = []

    async def handler(event):
        interrupt_event.set()
        adapter._pending_messages["session"] = event
        return None

    adapter._message_handler = handler
    # Exercise the real guard clear and callback finalizer without starting the
    # successor: this outcome belongs to the interrupted run alone.
    monkeypatch.setattr(
        adapter, "_spawn_drain_task",
        lambda queued, session_key, **kwargs: drained.append((queued, session_key)),
    )
    await adapter._process_message_background(event, "session")

    assert drained == [(event, "session")]
    assert observed == [(7, False)]
    assert interrupt_event._hermes_delivery_outcome == (7, False)
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_generation_and_outcome_are_snapshotted_before_finally_awaits(tmp_path, monkeypatch):
    adapter, event, interrupt_event, observed = make_turn(tmp_path, monkeypatch)

    async def handler(event):
        return "The answer."

    adapter._message_handler = handler
    original_stop = adapter._stop_typing_refresh

    async def stop_typing(*args, **kwargs):
        assert interrupt_event._hermes_delivery_outcome == (7, True)
        interrupt_event._hermes_run_generation = 8
        await original_stop(*args, **kwargs)

    monkeypatch.setattr(adapter, "_stop_typing_refresh", stop_typing)
    await adapter._process_message_background(event, "session")

    assert observed == [(7, True)]
    assert interrupt_event._hermes_run_generation == 8
