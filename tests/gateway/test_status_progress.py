"""Behavior of the gateway's single-message, argument-free status mode."""

import asyncio
import itertools
import queue
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.session import SessionSource
from gateway.turn_context import TurnContext


class StatusAdapter(BasePlatformAdapter):
    supports_code_blocks = True

    def __init__(self):
        super().__init__(PlatformConfig(enabled=True), Platform.DISCORD)
        self.sent = []
        self.edits = []
        self.sent_event = asyncio.Event()
        self.edit_event = asyncio.Event()
        self.edit_result = None

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        pass

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        message_id = f"status-{len(self.sent) + 1}"
        self.sent.append((chat_id, message_id, content, reply_to, metadata))
        self.sent_event.set()
        return SendResult(success=True, message_id=message_id)

    async def edit_message(self, chat_id, message_id, content, *, metadata=None):
        self.edits.append((chat_id, message_id, content, metadata))
        self.edit_event.set()
        return self.edit_result or SendResult(success=True, message_id=message_id)

    async def send_typing(self, chat_id, metadata=None):
        pass

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


def make_runner(monkeypatch, **kwargs):
    from gateway import run
    from agent.display import set_friendly_tool_labels

    set_friendly_tool_labels(True)
    clock = itertools.count(10, 10)
    # Advance the progress throttle deterministically without changing the
    # event loop's real clock or patching asyncio.sleep.
    monkeypatch.setattr(run, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    adapter = StatusAdapter()
    ctx = TurnContext(
        source=SessionSource(platform=Platform.DISCORD, chat_id="123", thread_id="456"),
        _run_still_current=lambda: True,
        progress_queue=queue.Queue(),
        progress_mode="status",
        tool_progress_enabled=True,
        _progress_metadata={"thread_id": "456"},
        _cleanup_progress=True,
        **kwargs,
    )
    owner = SimpleNamespace(_adapter_for_source=lambda source: adapter)
    return run.TurnRunner(owner, ctx), ctx, adapter


@pytest.mark.parametrize("tool, args, expected", [
    ("write_file", {"path": "/private/vault/Secret.md"}, "Writing..."),
    ("search_files", {"pattern": "sensitive query"}, "Searching files..."),
    ("terminal", {"command": "some private command"}, "Running..."),
    ("mcp.private_server.private_tool", {"secret": "credential"}, "Working..."),
])
def test_callback_uses_plain_verbs_without_arguments(monkeypatch, tool, args, expected):
    runner, ctx, _ = make_runner(monkeypatch)
    runner.progress_callback("tool.started", tool, "raw private preview", args)
    assert ctx.progress_queue.get_nowait() == expected


def test_repeated_activity_has_no_counter(monkeypatch):
    runner, ctx, _ = make_runner(monkeypatch)
    for name in ("First.md", "Second.md", "Third.md"):
        runner.progress_callback("tool.started", "write_file", name, {"path": name})
    assert list(ctx.progress_queue.queue) == ["Writing..."]


def test_thinking_updates_do_not_expose_scratch_text_or_emojis(monkeypatch):
    runner, ctx, _ = make_runner(monkeypatch, _thinking_enabled=True)
    runner.progress_callback("reasoning.available", "_thinking", "Private scratch text")
    assert ctx.progress_queue.get_nowait() == "Thinking..."


def test_activity_resumes_after_thinking(monkeypatch):
    runner, ctx, _ = make_runner(monkeypatch, _thinking_enabled=True)
    runner.progress_callback("tool.started", "write_file", "First.md", {"path": "First.md"})
    runner.progress_callback("reasoning.available", "_thinking", "Private scratch text")
    runner.progress_callback("tool.started", "write_file", "Second.md", {"path": "Second.md"})
    assert list(ctx.progress_queue.queue) == ["Writing...", "Thinking...", "Writing..."]


def test_routine_compression_status_uses_the_same_plain_status_queue(monkeypatch):
    from gateway import run

    runner, ctx, adapter = make_runner(monkeypatch)
    ctx._status_adapter = adapter
    monkeypatch.setattr(run, "_load_gateway_config", lambda: {"compression": {"progress_notices": True}})
    runner._status_callback_sync("lifecycle", run.COMPACTION_STATUS)
    assert ctx.progress_queue.get_nowait() == "Updating context..."
    assert adapter.sent == []


def test_interrupt_suppresses_status_callback(monkeypatch):
    runner, ctx, _ = make_runner(monkeypatch)
    ctx.agent_holder[0] = SimpleNamespace(is_interrupted=True)
    runner.progress_callback("tool.started", "write_file", "Secret.md", {"path": "Secret.md"})
    assert ctx.progress_queue.empty()


@pytest.mark.asyncio
async def test_sender_replaces_activity_and_keeps_thread_metadata(monkeypatch):
    runner, ctx, adapter = make_runner(monkeypatch)
    ctx.progress_queue.put("Reading...")
    task = asyncio.create_task(runner.send_progress_messages())
    try:
        await asyncio.wait_for(adapter.sent_event.wait(), 2)
        ctx.progress_queue.put("Writing...")
        await asyncio.wait_for(adapter.edit_event.wait(), 2)
    finally:
        task.cancel()
        await task

    assert len(adapter.sent) == 1
    assert adapter.sent[0][2] == "Reading..."
    assert adapter.edits[-1] == ("123", "status-1", "Writing...", {"thread_id": "456"})
    assert ctx._cleanup_msg_ids == ["status-1"]


@pytest.mark.asyncio
async def test_narration_reset_does_not_create_another_status_message(monkeypatch):
    runner, ctx, adapter = make_runner(monkeypatch)
    ctx.progress_queue.put("Reading...")
    task = asyncio.create_task(runner.send_progress_messages())
    try:
        await asyncio.wait_for(adapter.sent_event.wait(), 2)
        ctx.progress_queue.put(("__reset__",))
        ctx.progress_queue.put("Writing...")
        await asyncio.wait_for(adapter.edit_event.wait(), 2)
    finally:
        task.cancel()
        await task

    assert len(adapter.sent) == 1
    assert adapter.edits[-1][1:3] == ("status-1", "Writing...")


def test_compaction_completion_uses_the_same_plain_status_queue(monkeypatch):
    from agent.conversation_compression import COMPACTION_DONE_STATUS

    runner, ctx, adapter = make_runner(monkeypatch)
    ctx._status_adapter = adapter
    runner._status_callback_sync("compacted", COMPACTION_DONE_STATUS)

    assert list(ctx.progress_queue.queue) == ["Working..."]
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_status_mode_preserves_lifecycle_warnings(monkeypatch):
    from gateway import run

    runner, ctx, adapter = make_runner(monkeypatch)
    ctx._status_adapter = adapter
    ctx._status_chat_id = "123"
    scheduled = []

    def schedule(coro, *args, **kwargs):
        task = asyncio.create_task(coro)
        scheduled.append(task)
        return task

    monkeypatch.setattr(run, "safe_schedule_threadsafe", schedule)
    warning = "The provider is unavailable; switching to the fallback model."
    runner._status_callback_sync("lifecycle", warning)
    await asyncio.gather(*scheduled)

    assert ctx.progress_queue.empty()
    assert len(adapter.sent) == 1
    assert adapter.sent[0][2] == warning


@pytest.mark.asyncio
async def test_rapid_updates_coalesce_to_latest_activity(monkeypatch):
    runner, ctx, adapter = make_runner(monkeypatch)
    for activity in ("Reading...", "Searching files...", "Writing..."):
        ctx.progress_queue.put(activity)
    task = asyncio.create_task(runner.send_progress_messages())
    try:
        await asyncio.wait_for(adapter.sent_event.wait(), 2)
    finally:
        task.cancel()
        await task

    assert [message[2] for message in adapter.sent] == ["Writing..."]


@pytest.mark.asyncio
async def test_cancellation_flushes_only_latest_queued_activity(monkeypatch):
    runner, ctx, adapter = make_runner(monkeypatch)
    ctx.progress_queue.put("Reading...")
    task = asyncio.create_task(runner.send_progress_messages())
    await asyncio.wait_for(adapter.sent_event.wait(), 2)
    ctx.progress_queue.put("Searching files...")
    ctx.progress_queue.put(("__reset__",))
    ctx.progress_queue.put("Writing...")
    task.cancel()
    await task

    assert len(adapter.sent) == 1
    assert adapter.edits[-1][2] == "Writing..."


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [False, True])
async def test_edit_failure_never_sends_additional_messages(monkeypatch, retryable):
    runner, ctx, adapter = make_runner(monkeypatch)
    adapter.edit_result = SendResult(success=False, error="Edit failed", retryable=retryable)
    ctx.progress_queue.put("Reading...")
    task = asyncio.create_task(runner.send_progress_messages())
    try:
        await asyncio.wait_for(adapter.sent_event.wait(), 2)
        ctx.progress_queue.put("Writing...")
        await asyncio.wait_for(adapter.edit_event.wait(), 2)
        ctx.progress_queue.put("Running...")
    finally:
        task.cancel()
        await task

    assert len(adapter.sent) == 1


@pytest.mark.asyncio
async def test_stale_or_interrupted_run_does_not_flush_pending_updates(monkeypatch):
    runner, ctx, adapter = make_runner(monkeypatch)
    ctx.progress_queue.put("Reading...")
    task = asyncio.create_task(runner.send_progress_messages())
    await asyncio.wait_for(adapter.sent_event.wait(), 2)
    ctx.progress_queue.put("Writing...")
    ctx.agent_holder[0] = SimpleNamespace(is_interrupted=True)
    task.cancel()
    await task

    assert adapter.edits == []
