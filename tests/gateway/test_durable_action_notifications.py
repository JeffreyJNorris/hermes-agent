from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from gateway.config import Platform
from gateway import durable_actions
from gateway.durable_action_runner import (
    DurableActionNotificationDisposition,
    DurableActionNotificationResult,
    DurableActionRunner,
)
from gateway.durable_actions import DurableActionStatus, DurableActionStore
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)
from gateway.run import GatewayRunner, _format_gateway_process_notification
from gateway.session import SessionSource
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace


@pytest.fixture(autouse=True)
def _isolate_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(durable_actions, "_DURABLE_ACTION_HANDLERS", {})


def _terminal_ticket(
    db_path: Path,
    *,
    status: DurableActionStatus = DurableActionStatus.SUCCEEDED,
    reason: str | None = "applied",
    origin_type: str = "session",
    suffix: str = "default",
) -> str:
    created = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0).create_with_action(
        DurableRequestSpec(
            kind="trusted.action",
            title="Resume job?",
            body="Resume the unchanged paused job.",
            options=(
                DurableRequestOption("approve", "Resume"),
                DurableRequestOption("reject", "Keep paused"),
            ),
            ttl_seconds=60,
        ),
        action=durable_actions.PreparedDurableAction(
            action_type="cron.resume",
            action_version=1,
            payload_json=durable_actions.encode_durable_action_payload(
                {"job_id": "private-job"}
            ),
            fingerprint="private-fingerprint",
            idempotency_key=f"private-idempotency-{suffix}",
        ),
        target=DurableRequestTarget("discord", "private-inbox"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type=origin_type,
        origin_id="physical-job" if origin_type == "cron" else "logical-session",
        origin_session_id="physical-session" if origin_type == "session" else None,
        origin_ui_session_id="ui-session" if origin_type == "session" else None,
        origin_profile="operations",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE durable_action_tickets
               SET status = ?, outcome_reason = ?, outcome_json = ?,
                   completed_at = 1001, notification_status = 'pending'
               WHERE request_id = ?""",
            (
                status.value,
                reason,
                durable_actions.encode_durable_action_result({"changed": True}),
                created.request.request_id,
            ),
        )
    return created.request.request_id


def _row(db_path: Path, request_id: str) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM durable_action_tickets WHERE request_id = ?",
            (request_id,),
        ).fetchone()
    assert row is not None
    return row


def test_execution_output_survives_notification_store_restart(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _terminal_ticket(db_path)
    output = {"tool": "terminal", "output": "active\n", "exit_code": 0}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET action_type = ?, outcome_json = ? WHERE request_id = ?",
            ("terminal.execute", durable_actions.encode_durable_action_result(output), request_id),
        )
    claim = DurableActionStore(db_path).claim_next_notification()
    assert claim is not None
    assert claim.tool_result == output


def test_execution_notification_gives_agent_result_without_repeating_command() -> None:
    text = _format_gateway_process_notification({
        "type": "durable_action_terminal", "request_id": "test-request",
        "action_type": "terminal.execute", "action_status": "succeeded",
        "tool_result": {"tool": "terminal", "output": "active\n", "exit_code": 0},
    })
    assert "active" in text
    assert '"exit_code": 0' in text
    assert "Do not execute the approved operation again" in text


def test_failed_command_notification_does_not_claim_no_changes_were_made() -> None:
    text = _format_gateway_process_notification({
        "type": "durable_action_terminal", "request_id": "test-request",
        "action_type": "host.sudo", "action_status": "failed",
        "tool_result": {"output": "first step completed; second step failed", "exit_code": 1},
    })
    assert "No action was applied" not in text
    assert "partial changes" in text


@pytest.mark.asyncio
@pytest.mark.parametrize("action_type", ["terminal.execute", "execute_code.execute", "host.sudo"])
async def test_gateway_continues_execution_with_only_public_result_fields(
    tmp_path: Path, action_type: str,
) -> None:
    claim = _claimed_notification(tmp_path)
    from dataclasses import replace
    claim = replace(claim, action_type=action_type, tool_result=durable_actions._notification_tool_result(action_type, durable_actions.encode_durable_action_result({
        "tool": "terminal", "output": "active\n", "exit_code": 0,
        "private_args": "NEVER SHOW PRIVATE EXECUTION PAYLOAD",
    })))
    runner = object.__new__(GatewayRunner)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._build_process_event_source = MagicMock(return_value=SessionSource(
        platform=Platform.DISCORD, chat_id="conversation", chat_type="dm", user_id="operator",
    ))
    runner._delivery_adapter_for = MagicMock(return_value=object())
    runner._inject_watch_notification = AsyncMock(return_value=True)
    result = await runner._dispatch_durable_action_notification(
        claim, profile_name="operations", adapters={Platform.DISCORD: object()},
    )
    assert result.disposition is DurableActionNotificationDisposition.DELIVERED
    text, event = runner._inject_watch_notification.await_args.args
    assert event["tool_result"] == {"tool": "terminal", "output": "active\n", "exit_code": 0}
    assert event["parent_session_id"] == "physical-session"
    assert "NEVER SHOW" not in text


@pytest.mark.asyncio
@pytest.mark.parametrize('edit_fails', [False, True])
async def test_tool_outcome_updates_original_card_without_blocking_continuation(tmp_path, monkeypatch, edit_fails):
    db_path = tmp_path / 'state.db'
    request_id = _terminal_ticket(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE durable_action_tickets SET action_type='terminal.execute' WHERE request_id=?", (request_id,))
        conn.execute("UPDATE durable_requests SET kind='tool.durable_approval.terminal.execute', message_id='original-card' WHERE request_id=?", (request_id,))
    import gateway.durable_requests as requests
    monkeypatch.setattr(requests, 'get_durable_request_store', lambda: DurableRequestStore(db_path))
    claim = DurableActionStore(db_path, wall_clock=lambda: 2000).claim_next_notification()
    edits = []
    class Adapter:
        async def update_durable_execution_result(self, request, status):
            edits.append((request.request_id, request.message_id, status))
            if edit_fails:
                raise RuntimeError('Discord edit unavailable')
    adapter = Adapter()
    runner = object.__new__(GatewayRunner)
    runner._classify_completion_target = AsyncMock(return_value='deliver')
    runner._build_process_event_source = MagicMock(return_value=SessionSource(
        platform=Platform.DISCORD, chat_id='conversation', chat_type='dm', user_id='operator'))
    runner._delivery_adapter_for = MagicMock(return_value=adapter)
    runner._inject_watch_notification = AsyncMock(return_value=True)
    result = await runner._dispatch_durable_action_notification(
        claim, profile_name='operations', adapters={Platform.DISCORD: adapter})
    assert edits == [(request_id, 'original-card', 'succeeded')]
    assert result.disposition is DurableActionNotificationDisposition.DELIVERED
    runner._inject_watch_notification.assert_awaited_once()


def test_notification_claim_is_bounded_and_acknowledged_once(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _terminal_ticket(db_path)
    store = DurableActionStore(db_path, wall_clock=lambda: 2_000.0)

    claim = store.claim_next_notification()

    assert claim is not None
    assert claim.request_id == request_id
    assert claim.action_type == "cron.resume"
    assert claim.action_status is DurableActionStatus.SUCCEEDED
    assert claim.outcome_reason == "applied"
    assert claim.origin_type == "session"
    assert claim.origin_id == "logical-session"
    assert claim.origin_session_id == "physical-session"
    assert claim.origin_ui_session_id == "ui-session"
    assert claim.origin_profile == "operations"
    assert not hasattr(claim, "payload_json")
    assert not hasattr(claim, "fingerprint")
    assert not hasattr(claim, "outcome_json")
    assert store.complete_notification(request_id, claim.token) is True
    assert store.complete_notification(request_id, claim.token) is False
    assert store.claim_next_notification() is None
    assert _row(db_path, request_id)["notification_status"] == "delivered"


def test_notification_release_and_drop_never_change_action_outcome(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _terminal_ticket(db_path, status=DurableActionStatus.INVALIDATED)
    store = DurableActionStore(db_path, wall_clock=lambda: 2_000.0)
    first = store.claim_next_notification()
    assert first is not None

    assert store.release_notification(request_id, first.token) is True
    second = store.claim_next_notification()
    assert second is not None and second.token != first.token
    assert store.drop_notification(
        request_id, second.token, reason="origin_session_ended"
    ) is True

    row = _row(db_path, request_id)
    assert row["status"] == "invalidated"
    assert row["outcome_reason"] == "applied"
    assert row["notification_status"] == "dropped"
    assert row["notification_reason"] == "origin_session_ended"


def test_notification_claim_retry_and_delivery_clear_drop_reason(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _terminal_ticket(db_path)
    store = DurableActionStore(db_path, wall_clock=lambda: 2_000.0)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET notification_reason = 'old_reason'"
        )

    first = store.claim_next_notification()
    assert first is not None
    assert _row(db_path, request_id)["notification_reason"] is None
    assert store.release_notification(request_id, first.token)
    assert _row(db_path, request_id)["notification_reason"] is None
    second = store.claim_next_notification()
    assert second is not None
    assert store.complete_notification(request_id, second.token)
    assert _row(db_path, request_id)["notification_reason"] is None


@pytest.mark.asyncio
async def test_runner_dispatches_each_terminal_notification_once(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _terminal_ticket(db_path)
    delivered = []

    async def notify(claim):
        assert _row(db_path, request_id)["status"] == "succeeded"
        delivered.append(claim)
        return DurableActionNotificationResult(
            DurableActionNotificationDisposition.DELIVERED
        )

    runner = DurableActionRunner(
        DurableActionStore(db_path),
        enabled=True,
        allowed_actions={"cron.resume"},
        notification_handler=notify,
    )

    assert await runner.process_once() is True
    assert await runner.process_once() is False
    assert len(delivered) == 1
    assert _row(db_path, request_id)["notification_status"] == "delivered"


@pytest.mark.asyncio
async def test_ready_actions_do_not_starve_pending_notifications(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    notified_id = _terminal_ticket(db_path)
    ready_id = _terminal_ticket(
        db_path,
        status=DurableActionStatus.READY,
        reason=None,
        suffix="ready",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE durable_action_tickets
               SET status = 'ready', completed_at = NULL,
                   notification_status = 'not_ready'
               WHERE request_id = ?""",
            (ready_id,),
        )
    durable_actions.register_durable_action_handler(
        "cron.resume",
        1,
        prepare=lambda: None,
        execute=lambda _context: durable_actions.DurableActionOutcome(
            DurableActionStatus.SUCCEEDED, reason="applied"
        ),
    )
    delivered = []

    async def notify(claim):
        delivered.append(claim.request_id)
        return DurableActionNotificationResult(
            DurableActionNotificationDisposition.DELIVERED
        )

    runner = DurableActionRunner(
        DurableActionStore(db_path),
        enabled=True,
        allowed_actions={"cron.resume"},
        notification_handler=notify,
    )

    assert await runner.process_once() is True
    assert delivered == [notified_id]
    assert _row(db_path, notified_id)["notification_status"] == "delivered"
    assert _row(db_path, ready_id)["status"] == "succeeded"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("disposition", "expected", "reason", "processed"),
    [
        (DurableActionNotificationDisposition.RETRY, "pending", None, False),
        (
            DurableActionNotificationDisposition.DROPPED,
            "dropped",
            "origin_session_ended",
            True,
        ),
    ],
)
async def test_runner_maps_retry_and_drop_without_changing_outcome(
    tmp_path: Path,
    disposition: DurableActionNotificationDisposition,
    expected: str,
    reason: str | None,
    processed: bool,
) -> None:
    db_path = tmp_path / f"{disposition.value}.db"
    request_id = _terminal_ticket(db_path, status=DurableActionStatus.FAILED)

    async def notify(_claim):
        return DurableActionNotificationResult(disposition, reason)

    runner = DurableActionRunner(
        DurableActionStore(db_path),
        enabled=True,
        notification_handler=notify,
    )
    assert await runner.process_once() is processed
    row = _row(db_path, request_id)
    assert row["status"] == "failed"
    assert row["notification_status"] == expected


def _claimed_notification(tmp_path: Path, *, status=DurableActionStatus.SUCCEEDED):
    db_path = tmp_path / f"{status.value}.db"
    _terminal_ticket(db_path, status=status, reason="bounded_reason")
    claim = DurableActionStore(db_path).claim_next_notification()
    assert claim is not None
    return claim


@pytest.mark.asyncio
async def test_gateway_routes_bounded_action_completion_to_session(
    tmp_path: Path,
) -> None:
    claim = _claimed_notification(tmp_path)
    runner = object.__new__(GatewayRunner)
    source = SessionSource(
        platform=Platform.DISCORD,
        chat_id="conversation",
        chat_type="dm",
        user_id="operator",
        profile="operations",
    )
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._build_process_event_source = MagicMock(return_value=source)
    runner._delivery_adapter_for = MagicMock(return_value=object())
    runner._inject_watch_notification = AsyncMock(return_value=True)

    result = await runner._dispatch_durable_action_notification(
        claim,
        profile_name="operations",
        adapters={Platform.DISCORD: object()},
    )

    assert result.disposition is DurableActionNotificationDisposition.DELIVERED
    runner._classify_completion_target.assert_awaited_once_with("physical-session")
    text, event = runner._inject_watch_notification.await_args.args
    assert event == {
        "type": "durable_action_terminal",
        "request_id": claim.request_id,
        "action_type": "cron.resume",
        "action_status": "succeeded",
        "outcome_reason": "bounded_reason",
        "session_key": "logical-session",
        "parent_session_id": "physical-session",
        "origin_session_id": "physical-session",
        "origin_ui_session_id": "ui-session",
        "profile": "operations",
        "message_id": f"durable-action:{claim.request_id}",
    }
    assert "trusted durable action reached a terminal state" in text
    assert "Decision: approved" in text
    assert "Action status: succeeded" in text
    assert "Action was applied" in text
    for forbidden in (
        "private-job",
        "private-fingerprint",
        "private-idempotency",
        "private-inbox",
        claim.token,
    ):
        assert forbidden not in text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("verdict", "disposition", "reason"),
    [
        ("terminal", DurableActionNotificationDisposition.DROPPED, "origin_session_ended"),
        ("retry", DurableActionNotificationDisposition.RETRY, "origin_session_unavailable"),
    ],
)
async def test_gateway_action_completion_obeys_session_lineage(
    tmp_path: Path,
    verdict: str,
    disposition: DurableActionNotificationDisposition,
    reason: str,
) -> None:
    claim = _claimed_notification(tmp_path)
    runner = object.__new__(GatewayRunner)
    runner._classify_completion_target = AsyncMock(return_value=verdict)
    runner._inject_watch_notification = AsyncMock()

    result = await runner._dispatch_durable_action_notification(
        claim, profile_name="operations", adapters={}
    )

    assert (result.disposition, result.reason) == (disposition, reason)
    runner._inject_watch_notification.assert_not_awaited()


@pytest.mark.asyncio
async def test_session_reset_drop_reason_is_persisted(tmp_path: Path) -> None:
    db_path = tmp_path / "session-reset.db"
    request_id = _terminal_ticket(db_path)
    gateway = object.__new__(GatewayRunner)
    gateway._classify_completion_target = AsyncMock(return_value="terminal")

    async def notify(claim):
        return await gateway._dispatch_durable_action_notification(
            claim, profile_name="operations", adapters={}
        )

    runner = DurableActionRunner(
        DurableActionStore(db_path), enabled=True, notification_handler=notify
    )
    assert await runner.process_once() is True
    row = _row(db_path, request_id)
    assert (row["notification_status"], row["notification_reason"]) == (
        "dropped",
        "origin_session_ended",
    )


@pytest.mark.asyncio
async def test_cron_drop_reason_is_persisted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import cron.scheduler as scheduler

    db_path = tmp_path / "cron-drop.db"
    request_id = _terminal_ticket(db_path, origin_type="cron")
    monkeypatch.setattr(
        scheduler,
        "enqueue_durable_action_continuation",
        lambda **_kwargs: ("dropped", "origin_job_missing"),
    )
    gateway = object.__new__(GatewayRunner)

    async def notify(claim):
        return await gateway._dispatch_durable_action_notification(
            claim, profile_name="operations", adapters={}
        )

    runner = DurableActionRunner(
        DurableActionStore(db_path), enabled=True, notification_handler=notify
    )
    assert await runner.process_once() is True
    row = _row(db_path, request_id)
    assert (row["notification_status"], row["notification_reason"]) == (
        "dropped",
        "origin_job_missing",
    )


@pytest.mark.parametrize(
    ("status", "decision", "effect"),
    [
        (DurableActionStatus.SUCCEEDED, "approved", "Action was applied"),
        (DurableActionStatus.ALREADY_APPLIED, "approved", "already applied"),
        (DurableActionStatus.DECLINED, "rejected", "No action was applied"),
        (DurableActionStatus.EXPIRED, "expired", "No action was applied"),
        (DurableActionStatus.CANCELLED, "cancelled", "No action was applied"),
        (DurableActionStatus.ABANDONED, "abandoned", "No action was applied"),
        (DurableActionStatus.INVALIDATED, "approved", "No action was applied"),
        (DurableActionStatus.FAILED, "approved", "No action was applied"),
        (DurableActionStatus.UNKNOWN, "approved", "ambiguous"),
        (
            DurableActionStatus.HANDLER_UNAVAILABLE,
            "approved",
            "No action was applied",
        ),
    ],
)
def test_action_completion_formatter_is_structural_and_accurate(
    status: DurableActionStatus,
    decision: str,
    effect: str,
) -> None:
    text = _format_gateway_process_notification(
        {
            "type": "durable_action_terminal",
            "request_id": "drq_safe",
            "action_type": "cron.resume",
            "action_status": status.value,
            "outcome_reason": "bounded_reason",
            "payload_json": "private-payload",
            "fingerprint": "private-fingerprint",
            "actor_id": "private-actor",
            "claim": "private-claim",
            "raw_exception": "private-exception",
        }
    )
    assert text is not None
    assert f"Decision: {decision}" in text
    assert effect in text
    assert "bounded_reason" in text
    for forbidden in (
        "private-payload",
        "private-fingerprint",
        "private-actor",
        "private-claim",
        "private-exception",
    ):
        assert forbidden not in text


def test_gateway_starts_one_supervised_action_worker() -> None:
    runner = object.__new__(GatewayRunner)
    runner._durable_action_task = None
    task = SimpleNamespace(done=lambda: False)
    runner._spawn_supervised = MagicMock(return_value=task)

    runner._start_durable_action_worker()
    runner._start_durable_action_worker()

    runner._spawn_supervised.assert_called_once()
    assert runner._spawn_supervised.call_args.args[1] == "durable_action_worker"


@pytest.mark.asyncio
async def test_supervised_action_worker_is_owned_by_gateway_shutdown() -> None:
    runner = object.__new__(GatewayRunner)
    runner._background_tasks = set()
    runner._durable_action_task = None
    runner._running = True
    started = asyncio.Event()

    async def worker() -> None:
        started.set()
        await asyncio.Event().wait()

    runner._run_durable_action_worker = worker
    runner._start_durable_action_worker()
    task = runner._durable_action_task
    assert task in runner._background_tasks
    await started.wait()

    runner._running = False
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled()
