"""Atomic sets use the same authority and delivery lifecycle as single prompts."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace

import pytest

import gateway.interactive as interactive
from gateway.interactive import (
    InteractiveMultiSelectSpec, InteractivePromptRegistry, MultiSelectOutcome,
    PromptOption, PromptOutcomeStatus, PromptScope, ResolveStatus,
    make_multi_select_component_id, parse_multi_select_component_id,
    parse_selection_slots, render_text_fallback,
)
from gateway.interactive_bridge import InteractivePromptBridge
from gateway.interactive_decisions import (
    DecisionStatus, MultiDecisionOption, MultiDecisionRequest, request_multi_decision,
)

SCOPE = PromptScope("session", "discord", "chat", "thread", "actor")
SPEC = InteractiveMultiSelectSpec(
    kind="test.multi", title="Choose", body="Choose a set.",
    options=tuple(PromptOption(f"private-{n}", f"Choice {n}") for n in range(1, 5)),
    timeout_seconds=2, min_selections=2, max_selections=3,
)


def resolve(registry, handle, slots, **overrides):
    metadata = dict(prompt_id=handle.prompt.prompt_id, option_tokens=slots,
                    platform="discord", channel_id="thread", message_id="message",
                    actor_id="actor", source="native")
    metadata.update(overrides)
    return registry.resolve_multi_select_prompt(**metadata)


def native(registry, **kwargs):
    handle = registry.register_multi_select_prompt(SPEC, scope=SCOPE, **kwargs)
    assert registry.bind_delivery(prompt_id=handle.prompt.prompt_id, platform="discord", message_id="message")
    return handle


def test_atomic_native_set_canonical_order_replay_and_callback_outside_lock():
    registry = InteractivePromptRegistry()
    observed = []
    def callback(response):
        # Another thread must be able to take the registry lock while the resolver runs.
        with ThreadPoolExecutor(1) as pool:
            observed.append(pool.submit(registry.selection_metrics).result(timeout=1))
        observed.append(response.option_ids)
    handle = native(registry, resolver=callback)
    result = resolve(registry, handle, ("4", "1", "3"))
    assert result.response.option_ids == ("private-1", "private-3", "private-4")
    assert observed[0]["active"] == 0
    assert observed[1] == result.response.option_ids
    outcome = registry.wait_for_multi_select(handle)
    assert isinstance(outcome, MultiSelectOutcome)
    assert outcome.response == result.response
    assert resolve(registry, handle, ("1", "2")).status is ResolveStatus.ALREADY_RESOLVED


@pytest.mark.parametrize("slots", [("1",), ("1", "2", "3", "4"), ("1", "1"),
                                  ("0", "2"), ("5", "2"), ("private-1", "2"),
                                  ("01", "2"), (1, "2"), "1,2", None])
def test_invalid_sets_do_not_terminalize(slots):
    registry = InteractivePromptRegistry()
    handle = native(registry)
    assert resolve(registry, handle, slots).status is ResolveStatus.INVALID_OPTION
    assert registry.selection_metrics()["invalid_sets"] == 1
    assert resolve(registry, handle, ("2", "1")).status is ResolveStatus.RESOLVED


@pytest.mark.parametrize("metadata", [dict(platform="telegram"), dict(channel_id="chat"),
                                     dict(message_id="wrong"), dict(actor_id="other"),
                                     dict(source="text")])
def test_scope_and_delivery_checks_precede_set_commit(metadata):
    registry = InteractivePromptRegistry()
    handle = native(registry)
    assert resolve(registry, handle, ("1", "2"), **metadata).status is not ResolveStatus.RESOLVED
    assert resolve(registry, handle, ("1", "2")).status is ResolveStatus.RESOLVED


def test_text_grammar_one_fallback_and_unrelated_routing():
    registry = InteractivePromptRegistry()
    handle = registry.register_multi_select_prompt(SPEC, scope=SCOPE)
    assert registry.activate_text_fallback(handle.prompt.prompt_id)
    other = registry.register_multi_select_prompt(SPEC, scope=SCOPE)
    assert not registry.activate_text_fallback(other.prompt.prompt_id)
    text = render_text_fallback(handle.prompt)
    assert "2–3" in text and all(option.label in text for option in SPEC.options)
    def respond(text):
        return registry.resolve_text_for_session(session_key="session", text=text,
                                                platform="discord", channel_id="thread", actor_id="actor")
    for text in ("all", "none", "1-3", "Choice 1", "private-1", "1 and 3", "١,٢", "1,", "1\n,2"):
        assert respond(text) is None
    assert respond("1, 1").status is ResolveStatus.INVALID_OPTION
    assert respond("4, 01").response.option_ids == ("private-1", "private-4")
    assert registry.activate_text_fallback(other.prompt.prompt_id)
    registry.clear_all()


@pytest.mark.parametrize("changes", [dict(min_selections=0), dict(min_selections=True),
    dict(max_selections=5), dict(min_selections=3, max_selections=2),
    dict(options=(PromptOption("x", "X"),)),
    dict(options=(PromptOption("x", "X"), PromptOption("x", "Y"))),
    dict(options=(PromptOption("x", ""), PromptOption("y", "Y"))),
    dict(options=(PromptOption("x", "X", "unknown"), PromptOption("y", "Y"))),
    dict(body="b" * 1200, options=tuple(PromptOption(str(n), "x" * 200) for n in range(10))),
])
def test_spec_validation_before_publication(changes):
    registry = InteractivePromptRegistry()
    with pytest.raises(ValueError):
        registry.register_multi_select_prompt(replace(SPEC, **changes), scope=SCOPE)
    assert registry.selection_metrics()["active"] == 0


def test_deadline_cleanup_and_content_free_health():
    now = [10.0]
    registry = InteractivePromptRegistry(clock=lambda: now[0])
    expired = native(registry)
    now[0] = 13.0
    assert registry.selection_metrics()["stuck"] == 1
    assert resolve(registry, expired, ("1", "2")).status is ResolveStatus.EXPIRED
    assert registry.wait_for_multi_select(expired).status is PromptOutcomeStatus.EXPIRED
    first = native(registry)
    other = registry.register_multi_select_prompt(SPEC, scope=replace(SCOPE, session_key="other"))
    assert registry.clear_session("session") == 1
    assert registry.wait_for_multi_select(first).status is PromptOutcomeStatus.CANCELLED
    assert registry.selection_metrics()["active"] == 1
    registry.clear_platform("discord")
    assert registry.wait_for_multi_select(other).status is PromptOutcomeStatus.CANCELLED
    assert set(registry.selection_metrics()) == {"submissions", "invalid_sets", "active", "stuck"}


def test_two_simultaneous_complete_sets_have_one_winner():
    registry = InteractivePromptRegistry()
    handle = native(registry)
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(resolve, registry, handle, slots) for slots in (("1", "2"), ("3", "4"))]
        statuses = [future.result().status for future in futures]
    assert sorted(status.value for status in statuses) == ["already_resolved", "resolved"]
    assert registry.wait_for_multi_select(handle).response.option_ids in (("private-1", "private-2"), ("private-3", "private-4"))


def test_opaque_component_id_and_bounded_grammar():
    token = "a" * 43
    assert make_multi_select_component_id(token) == f"ipm:{token}"
    assert parse_multi_select_component_id(f"ipm:{token}") == token
    for custom_id in (f"ip:{token}:1", f"ipm:{token}:1", "ipm:short", "ipm:" + "a" * 100):
        assert parse_multi_select_component_id(custom_id) is None
    assert parse_selection_slots("1, 03") == ("1", "3")
    assert parse_selection_slots("9" * 5000 + ",2") == ("9" * 5000, "2")


def test_typed_values_never_enter_prompt_and_return_in_original_order():
    values = [object(), object(), object()]
    request = MultiDecisionRequest("test", "Choose", "Choose", tuple(
        MultiDecisionOption(value, f"Choice {index}") for index, value in enumerate(values)
    ), 1, 2)
    def callback(spec):
        registry = InteractivePromptRegistry()
        handle = registry.register_multi_select_prompt(spec, scope=SCOPE)
        assert all(option.id.startswith("decision_") for option in handle.prompt.options)
        registry.bind_delivery(prompt_id=handle.prompt.prompt_id, platform="discord", message_id="message")
        resolve(registry, handle, ("3", "1"))
        return registry.wait_for_multi_select(handle)
    result = request_multi_decision(request, callback=callback)
    assert result.status is DecisionStatus.SELECTED
    assert result.values == (values[0], values[2])


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery", ["native", "fallback", "failed", "stuck"])
@pytest.mark.parametrize("mode", ["async", "sync"])
async def test_bridge_async_delivery_and_deadline(monkeypatch, delivery, mode):
    registry = InteractivePromptRegistry()
    monkeypatch.setattr(interactive, "_registry", registry)
    sent = asyncio.Event()
    class Adapter:
        prompt = None
        async def send_interactive_multi_select_prompt(self, chat_id, prompt, metadata=None):
            self.prompt = prompt
            sent.set()
            if delivery == "stuck":
                await asyncio.Event().wait()
            return SimpleNamespace(success=delivery == "native", message_id="message",
                                   error_kind="unsupported" if delivery == "fallback" else "network")
        async def send(self, **kwargs):
            assert all(option.label in kwargs["content"] for option in SPEC.options)
            return SimpleNamespace(success=True)
    adapter = Adapter()
    bridge = InteractivePromptBridge(adapter=adapter, scope=SCOPE, event_loop=asyncio.get_running_loop())
    with pytest.raises(RuntimeError, match="event-loop"):
        bridge.request_multi_select_sync(SPEC)
    spec = replace(SPEC, timeout_seconds=.2)
    request = (bridge.request_multi_select_async(spec) if mode == "async"
               else asyncio.to_thread(bridge.request_multi_select_sync, spec))
    task = asyncio.create_task(request)
    await sent.wait()
    await asyncio.sleep(0)
    if delivery == "native":
        while registry._active[adapter.prompt.prompt_id].message_id is None:
            await asyncio.sleep(0)
        resolve(registry, SimpleNamespace(prompt=adapter.prompt), ("4", "1"))
    elif delivery == "fallback":
        while not registry.has_text_fallback_for_session("session"):
            await asyncio.sleep(0)
        registry.resolve_text_for_session(session_key="session", text="4,1", platform="discord", channel_id="thread", actor_id="actor")
    outcome = await task
    assert isinstance(outcome, MultiSelectOutcome)
    expected = "selected" if delivery in ("native", "fallback") else "expired" if delivery == "stuck" else "delivery_failed"
    assert outcome.status.value == expected
    assert registry.selection_metrics()["active"] == 0


def test_resolver_failure_after_commit_cannot_reopen_or_log_selection(caplog):
    registry = InteractivePromptRegistry()
    def callback(response):
        raise RuntimeError("private response content")
    handle = native(registry, resolver=callback)
    assert resolve(registry, handle, ("1", "2")).status is ResolveStatus.RESOLVED
    assert registry.wait_for_multi_select(handle).response.option_ids == ("private-1", "private-2")
    assert resolve(registry, handle, ("3", "4")).status is ResolveStatus.ALREADY_RESOLVED
    assert "private" not in caplog.text


@pytest.mark.asyncio
async def test_async_cancellation_removes_only_its_prompt(monkeypatch):
    registry = InteractivePromptRegistry()
    monkeypatch.setattr(interactive, "_registry", registry)
    sent = asyncio.Event()
    class Adapter:
        async def send_interactive_multi_select_prompt(self, **kwargs):
            sent.set()
            await asyncio.Event().wait()
    bridge = InteractivePromptBridge(adapter=Adapter(), scope=SCOPE, event_loop=asyncio.get_running_loop())
    unrelated = registry.register_multi_select_prompt(SPEC, scope=replace(SCOPE, session_key="other"))
    task = asyncio.create_task(bridge.request_multi_select_async(SPEC))
    await sent.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert registry.selection_metrics()["active"] == 1
    assert registry.cancel(unrelated.prompt.prompt_id)
    assert registry.wait_for_multi_select(unrelated).status is PromptOutcomeStatus.CANCELLED


def test_response_waiting_on_registry_lock_rechecks_deadline():
    import threading
    now = [1.0]
    about_to_lock = threading.Event()
    registry = InteractivePromptRegistry(clock=lambda: now[0])
    handle = native(registry)
    original = registry._lock
    class ObservedLock:
        def __enter__(self):
            if threading.current_thread().name.startswith("ThreadPoolExecutor"):
                about_to_lock.set()
            original.acquire()
        def __exit__(self, *args):
            original.release()
    registry._lock = ObservedLock()
    with ThreadPoolExecutor(1) as pool:
        with registry._lock:
            future = pool.submit(resolve, registry, handle, ("1", "2"))
            assert about_to_lock.wait(1)
            now[0] = handle.deadline + 1
        assert future.result(timeout=1).status is ResolveStatus.EXPIRED
    assert registry.wait_for_multi_select(handle).status is PromptOutcomeStatus.EXPIRED
