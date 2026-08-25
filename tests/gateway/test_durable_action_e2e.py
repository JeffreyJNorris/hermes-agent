from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from cron import jobs
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


def _post_resume(job_id: str) -> dict:
    entry = registry.get_entry("cronjob")
    assert entry is not None
    tokens = set_session_vars(
        platform="discord",
        session_key="discord:guild:source:user",
        session_id="physical-session",
        user_id="42",
        profile="test-profile",
        cron_session="",
        cron_job_id="",
    )
    try:
        return json.loads(entry.handler({"action": "resume", "job_id": job_id}))
    finally:
        clear_session_vars(tokens)


def _approve(request_id: str) -> None:
    store = get_durable_request_store()
    claim = store.claim_next_pending(["discord"])
    assert claim is not None and claim.request.request_id == request_id
    assert store.bind_delivery(
        request_id=request_id,
        claim_id=claim.claim_id,
        platform="discord",
        message_id="provider-message",
    ) is not None
    result = store.resolve(
        request_id=request_id,
        option_token="1",
        platform="discord",
        channel_id="123456789012345678",
        message_id="provider-message",
        actor_id="42",
    )
    assert result.status.value == "resolved"


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
async def test_disable_pauses_ready_ticket_and_reenable_executes_once(
    profile: Path,
) -> None:
    job = _paused_job()
    response = _post_resume(job["id"])
    _approve(response["request_id"])
    (profile / "config.yaml").write_text(
        _config(approvals_enabled=False), encoding="utf-8"
    )

    assert await DurableActionRunner(
        DurableActionStore(profile / "state.db"),
        enabled=False,
        allowed_actions={"cron.resume"},
    ).process_once() is False
    assert _ticket(profile, response["request_id"])["status"] == "ready"
    assert jobs.get_job(job["id"])["enabled"] is False

    (profile / "config.yaml").write_text(_config(), encoding="utf-8")
    runner = DurableActionRunner(
        DurableActionStore(profile / "state.db"),
        enabled=True,
        allowed_actions={"cron.resume"},
    )
    assert await runner.process_once() is True
    assert await runner.process_once() is False
    assert _ticket(profile, response["request_id"])["status"] == "succeeded"
    assert jobs.get_job(job["id"])["enabled"] is True
