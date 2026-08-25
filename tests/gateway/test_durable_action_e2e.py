from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cron import jobs
from gateway import durable_actions
from gateway.config import Platform
from gateway.durable_action_runner import (
    DurableActionNotificationDisposition,
    DurableActionNotificationResult,
    DurableActionRunner,
)
from gateway.durable_actions import (
    DurableActionStatus,
    DurableActionStore,
    PreparedDurableAction,
    encode_durable_action_payload,
)
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestTarget,
    DurableResponderMode,
    get_durable_request_store,
    reset_durable_request_store_cache,
)
from gateway.run import GatewayRunner
from gateway.session_context import clear_session_vars, reset_session_vars, set_session_vars
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
import tools.cronjob_tools  # noqa: F401 - registers the real model-facing tool
from tools import approval
from tools.registry import registry


def _config(*, approvals_enabled: bool = True) -> str:
    return f'''durable_requests:
  enabled: true
  inbox:
    platform: discord
    chat_id: "123456789012345678"
  default_ttl_seconds: 3600
  max_ttl_seconds: 7200
  terminal_retention_seconds: 86400
  scheduled_responder_policy: any_authorized
durable_approvals:
  enabled: {str(approvals_enabled).lower()}
  allowed_actions: ["cron.resume"]
'''


@pytest.fixture()
def profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    token = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    reset_session_vars()
    (tmp_path / "config.yaml").write_text(_config(), encoding="utf-8")
    try:
        yield tmp_path
    finally:
        reset_session_vars()
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)


def _paused_job() -> dict:
    job = jobs.create_job(prompt="Harmless check", schedule="every 1h")
    paused = jobs.pause_job(job["id"])
    assert paused is not None
    return paused


def _post_resume(job_id: str, *, actor_id: str = "42") -> dict:
    entry = registry.get_entry("cronjob")
    assert entry is not None
    tokens = set_session_vars(
        platform="discord",
        session_key="discord:guild:source:user",
        session_id="physical-session",
        user_id=actor_id,
        profile="test-profile",
        cron_session="",
        cron_job_id="",
    )
    try:
        return json.loads(entry.handler({"action": "resume", "job_id": job_id}))
    finally:
        clear_session_vars(tokens)


def _approve(
    request_id: str,
    *,
    actor_id: str = "42",
    message_id: str = "provider-message",
) -> str:
    store = get_durable_request_store()
    claim = store.claim_next_pending(["discord"])
    assert claim is not None and claim.request.request_id == request_id
    assert store.bind_delivery(
        request_id=request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id=message_id,
    ) is not None
    result = store.resolve(
        request_id=request_id,
        option_token="1",
        platform="discord",
        channel_id="123456789012345678",
        message_id=message_id,
        actor_id=actor_id,
    )
    assert result.status.value == "resolved"
    return claim.claim_id


def _ticket(profile: Path, request_id: str) -> sqlite3.Row:
    with sqlite3.connect(profile / "state.db") as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM durable_action_tickets WHERE request_id = ?",
            (request_id,),
        ).fetchone()
    assert row is not None
    return row


def test_phase4b_profile_upgrade_preserves_request_without_ticket_or_wake(
    profile: Path,
) -> None:
    store = get_durable_request_store()
    old = store.create(
        DurableRequestSpec(
            kind="decision",
            title="Existing",
            body="Keep me",
            options=(
                DurableRequestOption("one", "One"),
                DurableRequestOption("two", "Two"),
            ),
            ttl_seconds=3600,
        ),
        target=DurableRequestTarget("discord", "123456789012345678"),
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id="42",
        origin_type="session",
        origin_id="old-session",
        wake_on_terminal=False,
    )
    reset_durable_request_store_cache()
    with sqlite3.connect(profile / "state.db") as conn:
        conn.execute("DROP TABLE durable_action_tickets")

    DurableActionStore(profile / "state.db")

    with sqlite3.connect(profile / "state.db") as conn:
        request = conn.execute(
            "SELECT title, status, wake_requested, wake_status FROM durable_requests "
            "WHERE request_id = ?",
            (old.request.request_id,),
        ).fetchone()
        tickets = conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0]
    assert request == ("Existing", "pending_delivery", 0, "not_requested")
    assert tickets == 0


@pytest.mark.asyncio
async def test_generic_approve_and_stored_import_path_cannot_execute(
    profile: Path,
) -> None:
    job = _paused_job()
    store = get_durable_request_store()
    generic = store.create(
        DurableRequestSpec(
            kind="decision",
            title="Generic input",
            body="This is not authorization.",
            options=(
                DurableRequestOption("yes", "Approve"),
                DurableRequestOption("no", "Reject"),
            ),
            ttl_seconds=3600,
        ),
        target=DurableRequestTarget("discord", "123456789012345678"),
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id="42",
        origin_type="session",
        origin_id="generic-session",
    )
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    store.bind_delivery(
        request_id=generic.request.request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="generic-message",
    )
    store.resolve(
        request_id=generic.request.request_id,
        option_token="1",
        platform="discord",
        channel_id="123456789012345678",
        message_id="generic-message",
        actor_id="42",
    )

    assert await DurableActionRunner(
        DurableActionStore(profile / "state.db"),
        enabled=True,
        allowed_actions={"cron.resume"},
    ).process_once() is False
    assert jobs.get_job(job["id"])["enabled"] is False
    with sqlite3.connect(profile / "state.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM durable_action_tickets").fetchone()[0] == 0

    forged = store.create_with_action(
        DurableRequestSpec(
            kind="trusted.action",
            title="Forged",
            body="Must fail closed.",
            options=(
                DurableRequestOption("approve", "Approve"),
                DurableRequestOption("reject", "Reject"),
            ),
            ttl_seconds=3600,
        ),
        action=PreparedDurableAction(
            action_type="evil.import",
            action_version=1,
            payload_json=encode_durable_action_payload(
                {"handler": "os.system", "credential": "secret-canary"}
            ),
            fingerprint="private-fingerprint",
            idempotency_key="evil-import-once",
        ),
        target=DurableRequestTarget("discord", "123456789012345678"),
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id="42",
        origin_type="session",
        origin_id="forged-session",
        origin_session_id="forged-physical-session",
    )
    claim = store.claim_next_pending(["discord"])
    assert claim is not None
    store.bind_delivery(
        request_id=forged.request.request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="forged-message",
    )
    store.resolve(
        request_id=forged.request.request_id,
        option_token="1",
        platform="discord",
        channel_id="123456789012345678",
        message_id="forged-message",
        actor_id="42",
    )
    await DurableActionRunner(
        DurableActionStore(profile / "state.db"),
        enabled=True,
        allowed_actions={"evil.import"},
    ).process_once()
    ticket = _ticket(profile, forged.request.request_id)
    assert ticket["status"] == "handler_unavailable"
    assert jobs.get_job(job["id"])["enabled"] is False


@pytest.mark.asyncio
async def test_approved_cron_resume_executes_and_notifies_exactly_once(profile: Path) -> None:
    job = _paused_job()
    assert approval.has_blocking_approval("discord:guild:source:user") is False
    response = _post_resume(job["id"])
    assert set(response) == {"status", "request_id", "action", "expires_at", "created"}
    assert response["status"] == "approval_pending"
    _approve(response["request_id"])
    notifications = []

    async def notify(claim):
        notifications.append(claim)
        return DurableActionNotificationResult(
            DurableActionNotificationDisposition.DELIVERED
        )

    runner = DurableActionRunner(
        DurableActionStore(profile / "state.db"),
        enabled=True,
        allowed_actions={"cron.resume"},
        notification_handler=notify,
    )
    assert await runner.process_once() is True
    assert await runner.process_once() is False

    assert jobs.get_job(job["id"])["enabled"] is True
    ticket = _ticket(profile, response["request_id"])
    assert (ticket["status"], ticket["notification_status"]) == (
        "succeeded",
        "delivered",
    )
    assert len(notifications) == 1
    assert approval.has_blocking_approval("discord:guild:source:user") is False
    claim = notifications[0]
    assert claim.request_id == response["request_id"]
    assert not hasattr(claim, "payload_json")
    assert not hasattr(claim, "fingerprint")
    assert not hasattr(claim, "actor_id")
    assert not hasattr(claim, "provider_message_id")


@pytest.mark.asyncio
async def test_changed_state_invalidates_without_mutation(
    profile: Path,
) -> None:
    job = _paused_job()
    response = _post_resume(job["id"])
    _approve(response["request_id"])
    jobs.update_job(job["id"], {"prompt": "Changed after approval"})
    runner = DurableActionRunner(
        DurableActionStore(profile / "state.db"),
        enabled=True,
        allowed_actions={"cron.resume"},
    )
    assert await runner.process_once() is True
    assert await runner.process_once() is False
    ticket = _ticket(profile, response["request_id"])
    assert ticket["status"] == DurableActionStatus.INVALIDATED.value
    assert ticket["outcome_reason"] == "job_changed"
    assert jobs.get_job(job["id"])["enabled"] is False


@pytest.mark.asyncio
async def test_composed_failure_notification_and_logs_are_structural_only(
    profile: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload_canary = "payload-credential-canary"
    actor_canary = "actor-canary"
    provider_canary = "provider-message-canary"
    exception_canary = "raw-handler-exception-canary"
    _paused_job()
    records = jobs.load_jobs()
    records[0]["id"] = payload_canary
    jobs.save_jobs(records)

    with caplog.at_level("INFO"):
        response = _post_resume(payload_canary, actor_id=actor_canary)
        ticket = _ticket(profile, response["request_id"])
        fingerprint_canary = ticket["fingerprint"]
        idempotency_canary = ticket["idempotency_key"]
        delivery_claim_canary = _approve(
            response["request_id"],
            actor_id=actor_canary,
            message_id=provider_canary,
        )
        registered = durable_actions._DURABLE_ACTION_HANDLERS[("cron.resume", 1)]

        def fail_with_private_detail(_context):
            raise RuntimeError(exception_canary)

        monkeypatch.setitem(
            durable_actions._DURABLE_ACTION_HANDLERS,
            ("cron.resume", 1),
            (registered[0], fail_with_private_detail),
        )
        gateway = object.__new__(GatewayRunner)
        gateway._classify_completion_target = AsyncMock(return_value="active")
        gateway._build_process_event_source = MagicMock(return_value=object())
        gateway._adapter_for_source = MagicMock(return_value=object())
        gateway._inject_watch_notification = AsyncMock(return_value=True)
        notification_claims = []

        async def notify(claim):
            notification_claims.append(claim.token)
            return await gateway._dispatch_durable_action_notification(
                claim,
                profile_name="primary",
                adapters={Platform.DISCORD: object()},
            )

        runner = DurableActionRunner(
            DurableActionStore(profile / "state.db"),
            enabled=True,
            allowed_actions={"cron.resume"},
            notification_handler=notify,
        )
        assert await runner.process_once() is True

    terminal = _ticket(profile, response["request_id"])
    assert (terminal["status"], terminal["outcome_reason"]) == (
        "failed",
        "handler_exception",
    )
    text, event = gateway._inject_watch_notification.await_args.args
    assert "Action status: failed" in text
    assert "Reason: handler_exception" in text
    rendered_logs = repr(
        [(record.msg, record.args, record.__dict__) for record in caplog.records]
    )
    public_surfaces = (repr(response), text, repr(event), rendered_logs)
    private_values = (
        payload_canary,
        fingerprint_canary,
        idempotency_canary,
        actor_canary,
        provider_canary,
        delivery_claim_canary,
        notification_claims[0],
        exception_canary,
    )
    for surface in public_surfaces:
        for private in private_values:
            assert private not in surface


@pytest.mark.asyncio
async def test_disable_pauses_ready_ticket_and_reenable_executes_once(
    profile: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = _paused_job()
    response = _post_resume(job["id"])
    _approve(response["request_id"])
    passes: list[bool] = []
    pass_finished = asyncio.Event()
    original_process_once = DurableActionRunner.process_once

    async def observed_process_once(runner: DurableActionRunner) -> bool:
        result = await original_process_once(runner)
        passes.append(runner._enabled)
        pass_finished.set()
        return result

    monkeypatch.setattr(DurableActionRunner, "process_once", observed_process_once)
    provider_notifications = []
    monkeypatch.setattr(
        "cron.scheduler._notify_provider_jobs_changed",
        lambda: provider_notifications.append(True),
    )

    async def run_gateway_pass() -> None:
        gateway = object.__new__(GatewayRunner)
        gateway._running = True
        gateway._shutdown_event = asyncio.Event()
        gateway._durable_request_home = profile
        gateway._durable_action_task = None
        gateway.adapters = {}
        gateway.config = SimpleNamespace(multiplex_profiles=False)
        gateway._dispatch_durable_action_notification = AsyncMock(
            return_value=DurableActionNotificationResult(
                DurableActionNotificationDisposition.DELIVERED
            )
        )
        task = asyncio.create_task(gateway._run_durable_action_worker())
        await asyncio.wait_for(pass_finished.wait(), timeout=2)
        gateway._running = False
        gateway._shutdown_event.set()
        await task
        pass_finished.clear()

    (profile / "config.yaml").write_text(
        _config(approvals_enabled=False), encoding="utf-8"
    )

    await run_gateway_pass()
    assert passes == [False]
    assert _ticket(profile, response["request_id"])["status"] == "ready"
    assert jobs.get_job(job["id"])["enabled"] is False

    (profile / "config.yaml").write_text(_config(), encoding="utf-8")
    await run_gateway_pass()
    await run_gateway_pass()
    assert passes == [False, True, True]
    assert _ticket(profile, response["request_id"])["status"] == "succeeded"
    assert jobs.get_job(job["id"])["enabled"] is True
    assert provider_notifications == [True]
