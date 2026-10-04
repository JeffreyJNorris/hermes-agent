from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

import gateway.interactive as interactive
from gateway.config import Platform
from gateway.interactive import (
    InteractiveMultiSelectSpec, InteractivePromptRegistry, MultiSelectOutcome,
    MultiSelectResponse, PromptOption, PromptOutcomeStatus, ResolveStatus,
)
from gateway.run_turn_runner import TurnRunner
from gateway.session import SessionSource
from gateway.turn_context import TurnContext
from tools.interactive_prompt_context import (
    bind_interactive_multi_select_callback, get_interactive_multi_select_callback,
    reset_interactive_multi_select_callback,
)


def _spec():
    return InteractiveMultiSelectSpec(
        kind="test.multi", title="Bills", body="Choose bills.",
        options=tuple(PromptOption(name, name.title()) for name in ("electricity", "gas", "water")),
        timeout_seconds=2, min_selections=1, max_selections=2,
    )


class _Adapter:
    async def send_interactive_multi_select_prompt(self, chat_id, prompt, metadata=None):
        self.prompt = prompt
        self.called.set()
        return SimpleNamespace(success=True, message_id="delivered", error=None, error_kind=None)


def _turn(adapter, loop, current=lambda: True):
    return TurnRunner(SimpleNamespace(), TurnContext(
        source=SessionSource(platform=Platform.DISCORD, chat_id="channel", thread_id="thread", user_id="initiator"),
        session_key="trusted-session", _status_adapter=adapter, _loop_for_step=loop,
        _run_still_current=current,
    ))


@pytest.mark.asyncio
async def test_async_turn_uses_exact_initiator_and_delivery_scope(monkeypatch):
    registry = InteractivePromptRegistry()
    monkeypatch.setattr(interactive, "_registry", registry)
    adapter = _Adapter()
    adapter.called = asyncio.Event()
    turn = _turn(adapter, asyncio.get_running_loop())
    task = asyncio.create_task(turn.request_multi_select_async(_spec()))
    await adapter.called.wait()
    metadata = dict(prompt_id=adapter.prompt.prompt_id, platform="discord", channel_id="thread",
                    message_id="delivered", actor_id="other", source="native")
    for _ in range(100):
        result = registry.resolve_multi_select_prompt(option_tokens=("3", "1"), **metadata)
        if result.status is not ResolveStatus.NOT_DELIVERED:
            break
        await asyncio.sleep(0)
    assert result.status is ResolveStatus.SCOPE_MISMATCH
    metadata["actor_id"] = "initiator"
    assert registry.resolve_multi_select_prompt(option_tokens=("3", "1"), **metadata).status is ResolveStatus.RESOLVED
    outcome = await task
    assert outcome.response.option_ids == ("electricity", "water")
    assert outcome.response.chat_id == "channel"
    assert outcome.response.thread_id == "thread"
    assert registry.resolve_multi_select_prompt(option_tokens=("2",), **metadata).status is ResolveStatus.ALREADY_RESOLVED


@pytest.mark.asyncio
async def test_sync_callback_forbidden_on_gateway_loop_and_stale_turn_unavailable():
    turn = _turn(_Adapter(), asyncio.get_running_loop())
    with pytest.raises(RuntimeError):
        turn._interactive_multi_select_callback_sync(_spec())
    turn._ctx._run_still_current = lambda: False
    assert turn._multi_select_callback_for_turn() is None
    assert (await turn.request_multi_select_async(_spec())).status is PromptOutcomeStatus.UNSUPPORTED


def test_tool_discovery_cache_tracks_callback_without_leaking_into_cron():
    import model_tools
    callback = lambda prompt: MultiSelectOutcome(PromptOutcomeStatus.CANCELLED)
    def names(enabled, disabled=None):
        return {tool["function"]["name"] for tool in model_tools.get_tool_definitions(
            enabled_toolsets=enabled, disabled_toolsets=disabled, quiet_mode=True,
        )}
    assert "request_multi_select" not in names(["multi_select"])
    token = bind_interactive_multi_select_callback(callback)
    try:
        assert "request_multi_select" in names(["multi_select"])
        assert "request_multi_select" not in names(["multi_select"], ["multi_select"])
        assert "request_multi_select" not in names(["hermes-cron"])
    finally:
        reset_interactive_multi_select_callback(token)
    assert "request_multi_select" not in names(["multi_select"])


def test_agent_load_and_registry_execution_bind_own_callback_and_restore_context():
    from agent.agent_init import _load_tools
    from agent.tool_executor import _ToolCallRef, _resolve_sequential_dispatch
    def callback(prompt):
        return MultiSelectOutcome(PromptOutcomeStatus.SELECTED, response=MultiSelectResponse(
            prompt_id="opaque", option_ids=(prompt.options[0].id, prompt.options[2].id),
            platform="discord", chat_id="channel", thread_id=None, message_id="message", actor_id="actor",
        ))
    outer = lambda prompt: MultiSelectOutcome(PromptOutcomeStatus.CANCELLED)
    token = bind_interactive_multi_select_callback(outer)
    try:
        agent = SimpleNamespace(quiet_mode=True, interactive_multi_select_callback=None)
        _load_tools(agent, ["multi_select"], None)
        assert "request_multi_select" not in agent.valid_tool_names
        agent.interactive_multi_select_callback = callback
        _load_tools(agent, ["multi_select"], None)
        assert "request_multi_select" in agent.valid_tool_names
        assert get_interactive_multi_select_callback() is outer
        agent.quiet_mode = False
        agent._context_engine_tool_names = set()
        agent._memory_manager = None
        agent.session_id = "session"
        agent.enabled_toolsets = ["multi_select"]
        agent.disabled_toolsets = None
        args = dict(title="Bills", question="Choose bills.", options=["Electricity", "Gas", "Water"],
                    min_selections=1, max_selections=2)
        dispatch = _resolve_sequential_dispatch(agent, _ToolCallRef("request_multi_select", args, "task", "call", []), [])
        assert json.loads(dispatch.execute(args)) == {
            "status": "selected", "selected_indices": [1, 3], "selected_labels": ["Electricity", "Water"],
        }
        assert get_interactive_multi_select_callback() is outer
    finally:
        reset_interactive_multi_select_callback(token)


def test_multi_select_is_eager_and_sequential_even_inside_tool_call():
    from agent.tool_dispatch_helpers import _plan_tool_batch_segments
    from tools.tool_search import is_deferrable_tool_name
    assert not is_deferrable_tool_name("request_multi_select")
    for name, args in (("request_multi_select", {}), ("tool_call", {"name": "request_multi_select", "arguments": {}})):
        calls = [SimpleNamespace(function=SimpleNamespace(name=tool, arguments=json.dumps(arguments)))
                 for tool, arguments in (("web_search", {}), ("web_search", {}), (name, args))]
        segments = _plan_tool_batch_segments(calls)
        assert [kind for kind, _ in segments] == ["parallel", "sequential"]


def test_multi_select_log_progress_is_structural_only():
    import queue
    ctx = TurnContext(log_queue=queue.Queue(), progress_queue=None)
    turn = TurnRunner(SimpleNamespace(), ctx)
    turn.progress_callback("tool.started", "request_multi_select", preview="Private question and choices",
                           args={"question": "Private question", "options": ["Private choice"]})
    line = ctx.log_queue.get_nowait()
    assert "request_multi_select" in line
    assert "Private" not in line
