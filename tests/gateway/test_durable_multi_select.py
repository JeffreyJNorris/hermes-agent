from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading

import pytest

from gateway.durable_actions import PreparedDurableAction
from gateway.durable_requests import (
    DurableRequestError, DurableRequestOption, DurableRequestSpec,
    DurableRequestStatus, DurableRequestStore, DurableRequestTarget,
    DurableResponderMode, DurableResolveStatus, DurableWakeStatus,
    make_durable_multi_select_component_id, parse_durable_multi_select_component_id,
    render_durable_request_content, render_durable_request_text_command_content,
)
from gateway.durable_operations import DurableOperationsStore
from tools.durable_request_tool import _serialize_request


def _create(store, *, spec=None):
    return store.create(
        spec or DurableRequestSpec(
            kind="test.set", title="Bills", body="Choose the available bills.",
            options=tuple(DurableRequestOption(f"option_{i}", label) for i, label in enumerate(
                ("Electricity", "Gas", "Water", "Internet"), start=1)),
            selection_mode="multi", min_selections=2, max_selections=3,
            ttl_seconds=60,
        ),
        target=DurableRequestTarget("discord", "chat", "thread"),
        responder_mode=DurableResponderMode.INITIATOR, expected_actor_id="actor",
        origin_type="session", origin_id="origin", command_response_allowed=True,
        wake_on_terminal=True, origin_session_id="parent",
    ).request


def _bind(store, request):
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    return store.bind_delivery(request_id=request.request_id, claim_id=claim.claim_id,
                               platform="discord", message_id="message")


def _native(store, request, tokens=("4", "2"), **metadata):
    context = dict(platform="discord", channel_id="thread", message_id="message", actor_id="actor")
    context.update(metadata)
    return store.resolve_multi_select(request_id=request.request_id, option_tokens=tokens, **context)


def _command(store, request, slots=(4, 2)):
    return store.resolve_command(request_id=request.request_id, option_slots=slots,
                                platform="discord", channel_id="thread", actor_id="actor")


def test_native_set_commits_canonical_order_and_restart_wake(tmp_path):
    path = tmp_path / "state.db"
    store = DurableRequestStore(path, wall_clock=lambda: 100)
    request = _create(store)
    bound = _bind(store, request)
    assert bound.selection_mode == "multi" and bound.max_selections == 3
    content = render_durable_request_content(bound)
    assert "Choose 2–3 options" in content
    assert all(option.label in content for option in request.options)
    assert "comma-separated" in render_durable_request_text_command_content(bound)
    component = make_durable_multi_select_component_id(request.request_id)
    assert component == f"drm:{request.request_id}"
    assert parse_durable_multi_select_component_id(component) == request.request_id
    assert parse_durable_multi_select_component_id(component + ":option_2") is None
    assert _native(store, request).status is DurableResolveStatus.RESOLVED
    restarted = DurableRequestStore(path, wall_clock=lambda: 101)
    selected = restarted.get(request_id=request.request_id)
    assert selected.selected_option_ids == ("option_2", "option_4")
    assert selected.selected_option_id is None
    payload = _serialize_request(selected)
    assert payload["selected_indices"] == [2, 4]
    assert payload["selected_labels"] == ["Gas", "Internet"]
    assert "selected_index" not in payload and "selected_label" not in payload
    assert _command(restarted, request).status is DurableResolveStatus.ALREADY_RESOLVED
    wake = restarted.claim_next_wake()
    assert wake is not None and wake.request.selected_option_ids == ("option_2", "option_4")
    assert restarted.complete_wake(request_id=request.request_id, claim_id=wake.claim_id)
    assert restarted.get(request_id=request.request_id).wake_status is DurableWakeStatus.DELIVERED
    with sqlite3.connect(path) as conn:
        scalar, ordered_json = conn.execute("SELECT selected_option_id, selected_option_ids_json FROM durable_requests").fetchone()
    assert scalar is None and json.loads(ordered_json) == ["option_2", "option_4"]


@pytest.mark.parametrize("slots", [(), (1,), (1, 2, 3, 4), (1, 1), (0, 2), (2, 5), (True, 2), ("1", "2"), [1, 2]])
def test_invalid_command_sets_leave_no_partial_state(tmp_path, slots):
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 100)
    request = _create(store)
    _bind(store, request)
    assert _command(store, request, slots).status is DurableResolveStatus.INVALID_OPTION
    current = store.get(request_id=request.request_id)
    assert current.status is DurableRequestStatus.OPEN
    assert current.selected_option_ids == () and current.selected_option_id is None
    assert current.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert store.selection_metrics() == {"submissions": 1, "invalid_sets": 1}


@pytest.mark.parametrize("tokens", [("1", "1"), ("0", "2"), ("1", "5"), ("option_1", "2"), ("1,2",), ("１", "2"), (1, 2), ("01", "2")])
def test_invalid_native_sets_leave_request_open(tmp_path, tokens):
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 100)
    request = _create(store)
    _bind(store, request)
    assert _native(store, request, tokens).status is DurableResolveStatus.INVALID_OPTION
    assert store.get(request_id=request.request_id).status is DurableRequestStatus.OPEN


@pytest.mark.parametrize("metadata", [dict(platform="slack"), dict(channel_id="chat"), dict(message_id="other"), dict(actor_id="other"), dict(actor_id=None)])
def test_multi_native_authority_requires_actual_scope(tmp_path, metadata):
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 100)
    request = _create(store)
    _bind(store, request)
    assert _native(store, request, **metadata).status is DurableResolveStatus.SCOPE_MISMATCH
    assert store.get(request_id=request.request_id).status is DurableRequestStatus.OPEN


def test_native_command_race_and_terminal_transaction_rollback(tmp_path, monkeypatch):
    path = tmp_path / "state.db"
    store = DurableRequestStore(path, wall_clock=lambda: 100)
    request = _create(store)
    _bind(store, request)
    transition = store._transition_action_for_terminal_request_locked
    def failed_transition(*args, **kwargs):
        raise RuntimeError("transaction boundary")
    monkeypatch.setattr(store, "_transition_action_for_terminal_request_locked", failed_transition)
    with pytest.raises(RuntimeError, match="transaction boundary"):
        _native(store, request)
    current = store.get(request_id=request.request_id)
    assert current.status is DurableRequestStatus.OPEN and current.selected_option_ids == ()
    assert current.wake_status is DurableWakeStatus.NOT_REQUESTED
    monkeypatch.setattr(store, "_transition_action_for_terminal_request_locked", transition)
    other = DurableRequestStore(path, wall_clock=lambda: 100)
    barrier = threading.Barrier(2)
    def submit(method, owner):
        barrier.wait(timeout=5)
        return method(owner, request)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda pair: submit(*pair), [(_native, store), (_command, other)]))
    assert sorted(result.status.value for result in results) == ["already_resolved", "resolved"]
    assert store.get(request_id=request.request_id).selected_option_ids == ("option_2", "option_4")


def test_historical_schema_defaults_and_trusted_actions_stay_single(tmp_path):
    path = tmp_path / "state.db"
    store = DurableRequestStore(path, wall_clock=lambda: 100)
    spec = DurableRequestSpec(kind="approve", title="Approval", body="Proceed?", options=(
        DurableRequestOption("approve", "Approve"), DurableRequestOption("reject", "Reject")))
    historical = _create(store, spec=spec)
    _bind(store, historical)
    assert store.resolve(request_id=historical.request_id, option_token="1", platform="discord",
                         channel_id="thread", message_id="message", actor_id="actor").status is DurableResolveStatus.RESOLVED
    with sqlite3.connect(path) as conn:
        for column in ("selection_mode", "min_selections", "max_selections", "selected_option_ids_json"):
            conn.execute(f"ALTER TABLE durable_requests DROP COLUMN {column}")
    restarted = DurableRequestStore(path, wall_clock=lambda: 101)
    old = restarted.get(request_id=historical.request_id)
    assert old.selection_mode == "single" and old.min_selections == old.max_selections == 1
    assert _serialize_request(old)["selected_index"] == 1 and old.selected_option_ids == ()
    with pytest.raises(DurableRequestError, match="approve and reject"):
        restarted.create_with_action(replace(spec, selection_mode="multi", max_selections=2),
            action=PreparedDurableAction("cron.resume", 1, '{}', "fingerprint", "key"),
            target=DurableRequestTarget("discord", "thread"), responder_mode=DurableResponderMode.INITIATOR,
            expected_actor_id="actor", origin_type="session", origin_id="origin", origin_session_id="parent")


@pytest.mark.parametrize("changes", [dict(selection_mode="unknown"), dict(min_selections=0), dict(max_selections=5), dict(min_selections=3, max_selections=2), dict(min_selections=True), dict(selection_mode="single", max_selections=2)])
def test_invalid_persisted_selection_policy_rejected(tmp_path, changes):
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 100)
    spec = DurableRequestSpec(kind="test", title="Choose", body="Context", options=tuple(
        DurableRequestOption(str(i), str(i)) for i in range(4)), selection_mode="multi", min_selections=2, max_selections=3)
    with pytest.raises(ValueError):
        _create(store, spec=replace(spec, **changes))


def test_durable_multi_health_exposes_active_and_due_counts_without_content(tmp_path):
    path = tmp_path / "state.db"
    store = DurableRequestStore(path, wall_clock=lambda: 100)
    request = _create(store)
    _bind(store, request)
    health = DurableOperationsStore(path, reconcile_schema=False).collect_health_data(now=161)
    metrics = {item.name: item for item in health}
    assert metrics["hermes_durable_multi_select_active"].value == 1
    assert metrics["hermes_durable_multi_select_stuck"].value == 1
    for metric in health:
        assert not any(value in repr(metric.attributes) for value in (request.request_id, "Electricity", "Gas", "actor", "origin"))


@pytest.mark.asyncio
async def test_restart_wake_failure_retries_committed_set_without_reopening(tmp_path):
    from unittest.mock import AsyncMock, MagicMock
    from gateway.durable_request_bridge import DurableRequestBridge
    from gateway.run import GatewayRunner
    from gateway.config import Platform
    from gateway.session import SessionSource

    path = tmp_path / "state.db"
    store = DurableRequestStore(path, wall_clock=lambda: 100)
    request = _create(store)
    _bind(store, request)
    assert _command(store, request).status is DurableResolveStatus.RESOLVED
    restarted = DurableRequestStore(path, wall_clock=lambda: 101)
    runner = object.__new__(GatewayRunner)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._build_process_event_source = MagicMock(return_value=SessionSource(
        platform=Platform.DISCORD, chat_id="chat", chat_type="dm", user_id="actor"))
    runner._delivery_adapter_for = MagicMock(return_value=object())
    runner._inject_watch_notification = AsyncMock(side_effect=[False, True])
    async def dispatch(claim):
        return await runner._dispatch_durable_request_wake(claim, profile_name="primary", adapters={})
    bridge = DurableRequestBridge()
    await bridge._deliver_terminal_wakes(store=restarted, wake_handler=dispatch)
    pending = restarted.get(request_id=request.request_id)
    assert pending.wake_status is DurableWakeStatus.PENDING
    assert pending.status is DurableRequestStatus.SELECTED and pending.selected_option_ids == ("option_2", "option_4")
    assert _native(restarted, request).status is DurableResolveStatus.ALREADY_RESOLVED
    await bridge._deliver_terminal_wakes(store=restarted, wake_handler=dispatch)
    assert restarted.get(request_id=request.request_id).wake_status is DurableWakeStatus.DELIVERED
    for call in runner._inject_watch_notification.call_args_list:
        text, event = call.args
        assert event["selected_indices"] == [2, 4] and event["selected_labels"] == ["Gas", "Internet"]
        assert "2. Gas" in text and "4. Internet" in text and "option_2" not in text
    assert runner._inject_watch_notification.call_count == 2


@pytest.mark.parametrize("submit", [_native, _command], ids=["native", "command"])
def test_waiting_for_sqlite_writer_rechecks_deadline_after_lock(tmp_path, monkeypatch, submit):
    clock = [100.0]
    path = tmp_path / "state.db"
    store = DurableRequestStore(path, wall_clock=lambda: clock[0])
    request = _create(store)
    _bind(store, request)
    attempted_begin = threading.Event()
    connect = store._connect
    writer = connect()
    writer.execute("BEGIN IMMEDIATE")

    def traced_connect():
        connection = connect()
        connection.set_trace_callback(
            lambda sql: attempted_begin.set() if sql == "BEGIN IMMEDIATE" else None
        )
        return connection

    monkeypatch.setattr(store, "_connect", traced_connect)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(submit, store, request)
        try:
            assert attempted_begin.wait(timeout=5)
            clock[0] = request.expires_at
        finally:
            writer.rollback()
            writer.close()
        result = future.result(timeout=5)
    assert result.status is DurableResolveStatus.EXPIRED
    terminal = store.get(request_id=request.request_id)
    assert terminal.status is DurableRequestStatus.EXPIRED
    assert terminal.selected_option_ids == () and terminal.selected_option_id is None
