from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

import gateway.interactive_bridge as bridge_module
from gateway.interactive import (
    InteractivePromptRegistry,
    InteractivePromptSpec,
    PromptOption,
    PromptOutcomeStatus,
    PromptScope,
    ResolveStatus,
)
from gateway.interactive_bridge import InteractivePromptBridge
from gateway.interactive_decisions import (
    DecisionOption,
    DecisionRequest,
    DecisionStatus,
    request_decision_async,
)


@dataclass
class _SendResult:
    success: bool
    message_id: str | None = None
    error: str | None = None
    error_kind: str | None = None


class _Adapter:
    def __init__(self, *, block_native: bool = False) -> None:
        self.block_native = block_native
        self.prompt = None
        self.native_called = asyncio.Event()
        self.native_cancelled = asyncio.Event()

    async def send_interactive_prompt(self, chat_id, prompt, metadata=None):
        self.prompt = prompt
        self.native_called.set()
        if self.block_native:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.native_cancelled.set()
                raise
        return _SendResult(True, message_id="provider-message")

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        raise AssertionError("text fallback was not expected")


class _CancellationResistantAdapter(_Adapter):
    def __init__(self) -> None:
        super().__init__()
        self.release_native = asyncio.Event()
        self.native_finished = asyncio.Event()

    async def send_interactive_prompt(self, chat_id, prompt, metadata=None):
        self.prompt = prompt
        self.native_called.set()
        while not self.release_native.is_set():
            try:
                await self.release_native.wait()
            except asyncio.CancelledError:
                self.native_cancelled.set()
                task = asyncio.current_task()
                if task is not None:
                    task.uncancel()
        self.native_finished.set()
        return _SendResult(True, message_id="late-provider-message")


@pytest.fixture
def local_registry(monkeypatch):
    registry = InteractivePromptRegistry()
    monkeypatch.setattr(bridge_module, "register_prompt", registry.register)
    monkeypatch.setattr(bridge_module, "bind_prompt_delivery", registry.bind_delivery)
    monkeypatch.setattr(
        bridge_module,
        "activate_prompt_text_fallback",
        registry.activate_text_fallback,
    )
    monkeypatch.setattr(bridge_module, "cancel_prompt", registry.cancel)
    monkeypatch.setattr(bridge_module, "expire_prompt", registry.expire)
    monkeypatch.setattr(bridge_module, "wait_for_prompt", registry.wait)
    return registry


def _scope() -> PromptScope:
    return PromptScope(
        session_key="session",
        platform="discord",
        chat_id="chat",
        thread_id=None,
        expected_actor_id="actor",
    )


def _bridge(adapter: _Adapter) -> InteractivePromptBridge:
    return InteractivePromptBridge(
        adapter=adapter,
        scope=_scope(),
        event_loop=asyncio.get_running_loop(),
    )


def _prompt(timeout: float = 5.0) -> InteractivePromptSpec:
    return InteractivePromptSpec(
        kind="test.async",
        title="Choose",
        body="Choose one option.",
        options=(PromptOption("first", "First"), PromptOption("second", "Second")),
        timeout_seconds=timeout,
    )


async def _resolve_after_binding(
    registry: InteractivePromptRegistry,
    prompt_id: str,
    *,
    option_token: str,
):
    for _ in range(100):
        result = registry.resolve(
            prompt_id=prompt_id,
            option_token=option_token,
            platform="discord",
            channel_id="chat",
            message_id="provider-message",
            actor_id="actor",
            source="native",
        )
        if result.status is not ResolveStatus.NOT_DELIVERED:
            return result
        await asyncio.sleep(0)
    raise AssertionError("prompt delivery was not bound")


@pytest.mark.asyncio
async def test_typed_async_decision_uses_public_bridge(local_registry) -> None:
    adapter = _Adapter()
    bridge = _bridge(adapter)

    task = asyncio.create_task(
        request_decision_async(
            DecisionRequest(
                kind="deployment.target",
                title="Choose target",
                body="Choose the reviewed deployment target.",
                options=(
                    DecisionOption("staging", "Staging"),
                    DecisionOption("production", "Production"),
                ),
            ),
            callback=bridge.request_async,
        )
    )

    await asyncio.wait_for(adapter.native_called.wait(), timeout=1)
    result = await _resolve_after_binding(
        local_registry,
        adapter.prompt.prompt_id,
        option_token="2",
    )
    assert result.status is ResolveStatus.RESOLVED

    decision = await asyncio.wait_for(task, timeout=1)
    assert decision.status is DecisionStatus.SELECTED
    assert decision.value == "production"


@pytest.mark.asyncio
async def test_async_caller_cancellation_cancels_prompt_during_delivery(
    local_registry,
) -> None:
    adapter = _Adapter(block_native=True)
    bridge = _bridge(adapter)
    task = asyncio.create_task(bridge.request_async(_prompt()))

    await asyncio.wait_for(adapter.native_called.wait(), timeout=1)
    prompt_id = adapter.prompt.prompt_id
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert adapter.native_cancelled.is_set()
    replay = local_registry.resolve(
        prompt_id=prompt_id,
        option_token="1",
        platform="discord",
        channel_id="chat",
        message_id="provider-message",
        actor_id="actor",
        source="native",
    )
    assert replay.status is ResolveStatus.CANCELLED


@pytest.mark.asyncio
async def test_async_caller_cancellation_cancels_prompt_while_waiting(
    local_registry,
) -> None:
    adapter = _Adapter()
    bridge = _bridge(adapter)
    task = asyncio.create_task(bridge.request_async(_prompt()))

    await asyncio.wait_for(adapter.native_called.wait(), timeout=1)
    prompt_id = adapter.prompt.prompt_id
    for _ in range(100):
        result = local_registry.resolve(
            prompt_id=prompt_id,
            option_token="forged",
            platform="discord",
            channel_id="chat",
            message_id="provider-message",
            actor_id="actor",
            source="native",
        )
        if result.status is not ResolveStatus.NOT_DELIVERED:
            assert result.status is ResolveStatus.INVALID_OPTION
            break
        await asyncio.sleep(0)
    else:
        raise AssertionError("prompt delivery was not bound")

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    replay = local_registry.resolve(
        prompt_id=prompt_id,
        option_token="1",
        platform="discord",
        channel_id="chat",
        message_id="provider-message",
        actor_id="actor",
        source="native",
    )
    assert replay.status is ResolveStatus.CANCELLED


@pytest.mark.asyncio
async def test_async_timeout_does_not_wait_for_cancellation_resistant_adapter(
    local_registry,
) -> None:
    adapter = _CancellationResistantAdapter()
    bridge = _bridge(adapter)

    try:
        outcome = await asyncio.wait_for(
            bridge.request_async(_prompt(timeout=0.02)),
            timeout=1,
        )
        assert outcome.status is PromptOutcomeStatus.EXPIRED
        assert adapter.native_cancelled.is_set()
        prompt_id = adapter.prompt.prompt_id
        replay = local_registry.resolve(
            prompt_id=prompt_id,
            option_token="1",
            platform="discord",
            channel_id="chat",
            message_id="late-provider-message",
            actor_id="actor",
            source="native",
        )
        assert replay.status is ResolveStatus.EXPIRED
    finally:
        adapter.release_native.set()
        await asyncio.wait_for(adapter.native_finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_async_request_rejects_foreign_event_loop(local_registry) -> None:
    adapter = _Adapter()
    foreign_loop = asyncio.new_event_loop()
    bridge = InteractivePromptBridge(
        adapter=adapter,
        scope=_scope(),
        event_loop=foreign_loop,
    )
    try:
        with pytest.raises(RuntimeError, match="gateway event loop"):
            await bridge.request_async(_prompt())
    finally:
        foreign_loop.close()

    assert not adapter.native_called.is_set()
