from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from cron import jobs
from gateway.durable_requests import (
    DurableRequestStatus,
    get_durable_request_store,
    reset_durable_request_store_cache,
)
from gateway.session_context import clear_session_vars, reset_session_vars, set_session_vars
from hermes_cli.config_defaults import DEFAULT_CONFIG
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tools.cronjob_tools import CRONJOB_SCHEMA, cronjob
from tools.registry import registry


def _config(*, enabled: bool, allowlisted: bool, inbox: bool = True) -> str:
    allowed = '["cron.resume"]' if allowlisted else "[]"
    inbox_block = """
  inbox:
    platform: discord
    chat_id: "123456789012345678"
""" if inbox else ""
    return f"""durable_requests:
  enabled: true
{inbox_block}  default_ttl_seconds: 3600
  max_ttl_seconds: 7200
  terminal_retention_seconds: 86400
  scheduled_responder_policy: any_authorized
durable_approvals:
  enabled: {str(enabled).lower()}
  allowed_actions: {allowed}
"""


@pytest.fixture()
def durable_cron_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    token = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    reset_session_vars()
    try:
        yield tmp_path
    finally:
        reset_session_vars()
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)


def _paused_job(*, name: str | None = None) -> dict:
    job = jobs.create_job(
        prompt="Harmless check",
        schedule="every 1h",
        name=name,
    )
    paused = jobs.pause_job(job["id"])
    assert paused is not None
    return paused


def _model_handler(job_id: str, **untrusted: object) -> dict:
    entry = registry.get_entry("cronjob_manage")
    assert entry is not None
    return json.loads(
        entry.handler({"action": "resume", "job_id": job_id, **untrusted})
    )


def _bind_discord_session():
    return set_session_vars(
        platform="discord",
        session_key="discord:guild:channel:user",
        session_id="session-physical-1",
        user_id="42",
        profile="test-profile",
        cron_session="",
        cron_job_id="",
    )


def test_defaults_keep_trusted_durable_approvals_disabled() -> None:
    assert DEFAULT_CONFIG["durable_approvals"] == {
        "enabled": False,
        "allowed_actions": [],
    }


def test_disabled_policy_preserves_immediate_model_resume(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=False, allowlisted=False), encoding="utf-8"
    )
    job = _paused_job()

    result = _model_handler(job["id"])

    assert result["success"] is True
    assert jobs.get_job(job["id"])["enabled"] is True


@pytest.mark.parametrize(
    "policy_yaml",
    [
        "durable_approvals: malformed\n",
        "durable_approvals:\n  enabled: \"false\"\n  allowed_actions: []\n",
        "durable_approvals:\n  enabled: 0\n  allowed_actions: []\n",
        (
            "durable_approvals:\n"
            "  enabled: true\n"
            "  allowed_actions: [cron.resume, 1]\n"
        ),
    ],
)
def test_malformed_policy_fails_closed_without_resuming(
    durable_cron_home: Path,
    policy_yaml: str,
) -> None:
    config = _config(enabled=False, allowlisted=False)
    config = config[: config.index("durable_approvals:")] + policy_yaml
    (durable_cron_home / "config.yaml").write_text(config, encoding="utf-8")
    job = _paused_job()

    result = _model_handler(job["id"])

    assert result == {"error": "durable approvals configuration is invalid"}
    assert jobs.get_job(job["id"])["enabled"] is False
    assert not (durable_cron_home / "state.db").exists()


def test_allowlisted_model_resume_creates_redacted_approval(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    job = _paused_job()
    tokens = _bind_discord_session()
    try:
        result = _model_handler(
            job["id"],
            action_type="evil.run",
            action_version=99,
            payload={"command": "evil"},
            fingerprint="evil",
            origin="evil",
            responder_policy="everyone",
            handler="evil.module:run",
            deliver="evil:destination",
        )
    finally:
        clear_session_vars(tokens)

    assert set(result) == {
        "status",
        "request_id",
        "action",
        "expires_at",
        "created",
    }
    assert result["status"] == "approval_pending"
    assert result["request_id"].startswith("drq_")
    assert result["action"] == "cron.resume"
    assert result["created"] is True
    assert jobs.get_job(job["id"])["enabled"] is False
    with sqlite3.connect(durable_cron_home / "state.db") as conn:
        request = conn.execute(
            "SELECT platform, chat_id, origin_type, origin_id FROM durable_requests"
        ).fetchone()
        ticket = conn.execute(
            "SELECT action_type, action_version, payload_json FROM durable_action_tickets"
        ).fetchone()
    assert request == (
        "discord",
        "123456789012345678",
        "session",
        "discord:guild:channel:user",
    )
    assert ticket == (
        "cron.resume",
        1,
        json.dumps({"job_id": job["id"]}, separators=(",", ":")),
    )


def test_duplicate_unchanged_model_resume_returns_existing_ticket(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    job = _paused_job()
    tokens = _bind_discord_session()
    try:
        first = _model_handler(job["id"])
        second = _model_handler(job["id"])
    finally:
        clear_session_vars(tokens)

    assert first["request_id"] == second["request_id"]
    assert first["created"] is True
    assert second["created"] is False


@pytest.mark.parametrize(
    ("transition", "expected_status"),
    [
        ("reject", "declined"),
        ("expire", "expired"),
        ("cancel", "cancelled"),
        ("delivery_failure", "abandoned"),
    ],
)
def test_terminal_ticket_reuse_reports_persisted_action_status(
    durable_cron_home: Path,
    transition: str,
    expected_status: str,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    job = _paused_job()
    tokens = _bind_discord_session()
    try:
        first = _model_handler(job["id"])
        store = get_durable_request_store()
        if transition == "cancel":
            store.cancel(
                request_id=first["request_id"],
                origin_type="session",
                origin_id="discord:guild:channel:user",
            )
        elif transition == "expire":
            with sqlite3.connect(durable_cron_home / "state.db") as conn:
                conn.execute(
                    "UPDATE durable_requests SET expires_at = 0 WHERE request_id = ?",
                    (first["request_id"],),
                )
            store.get(request_id=first["request_id"])
        else:
            claim = store.claim_next_pending(["discord"])
            assert claim is not None
            if transition == "delivery_failure":
                store.fail_delivery(
                    request_id=first["request_id"],
                    claim_id=claim.claim_id,
                    status=DurableRequestStatus.DELIVERY_FAILED,
                    reason="provider_message_missing",
                )
            else:
                store.bind_delivery(
                    request_id=first["request_id"],
                    claim_id=claim.claim_id,
                    platform="discord",
                    message_id="message-1",
                )
                store.resolve(
                    request_id=first["request_id"],
                    option_token="2",
                    platform="discord",
                    channel_id="123456789012345678",
                    message_id="message-1",
                    actor_id="42",
                )
        reused = _model_handler(job["id"])
    finally:
        clear_session_vars(tokens)

    assert set(reused) == {
        "status",
        "request_id",
        "action",
        "expires_at",
        "created",
    }
    assert reused["status"] == expected_status
    assert reused["request_id"] == first["request_id"]
    assert reused["action"] == "cron.resume"
    assert reused["created"] is False


def test_allowlisted_model_resume_resolves_job_name_to_canonical_id(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    job = _paused_job(name="Nightly report")
    tokens = _bind_discord_session()
    try:
        result = _model_handler("nightly report")
    finally:
        clear_session_vars(tokens)

    assert result["status"] == "approval_pending"
    with sqlite3.connect(durable_cron_home / "state.db") as conn:
        payload_json = conn.execute(
            "SELECT payload_json FROM durable_action_tickets"
        ).fetchone()[0]
    assert json.loads(payload_json) == {"job_id": job["id"]}


def test_allowlisted_model_resume_preserves_ambiguous_name_error(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    first = _paused_job(name="Duplicate")
    second = _paused_job(name="Duplicate")
    tokens = _bind_discord_session()
    try:
        result = _model_handler("duplicate")
    finally:
        clear_session_vars(tokens)

    assert result["success"] is False
    assert "ambiguous" in result["error"]
    assert {match["id"] for match in result["matches"]} == {
        first["id"],
        second["id"],
    }


def test_allowlisted_model_resume_preserves_not_found_error(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    tokens = _bind_discord_session()
    try:
        result = _model_handler("missing job")
    finally:
        clear_session_vars(tokens)

    assert result == {
        "success": False,
        "error": (
            "Job with ID or name 'missing job' not found. "
            "Use cronjob(action='list') to inspect jobs."
        ),
    }


def test_enabled_but_not_allowlisted_fails_closed_without_resuming(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=False), encoding="utf-8"
    )
    job = _paused_job()

    result = _model_handler(job["id"])

    assert "error" in result
    assert jobs.get_job(job["id"])["enabled"] is False
    state_db = durable_cron_home / "state.db"
    assert not state_db.exists()


@pytest.mark.parametrize("failure", ["missing_inbox", "missing_context"])
def test_missing_trusted_configuration_or_origin_fails_closed(
    durable_cron_home: Path,
    failure: str,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True, inbox=failure != "missing_inbox"),
        encoding="utf-8",
    )
    job = _paused_job()
    tokens = _bind_discord_session() if failure == "missing_inbox" else None
    try:
        result = _model_handler(job["id"])
    finally:
        if tokens is not None:
            clear_session_vars(tokens)

    assert "error" in result
    assert jobs.get_job(job["id"])["enabled"] is False


def test_direct_python_resume_remains_immediate_when_approvals_enabled(
    durable_cron_home: Path,
) -> None:
    (durable_cron_home / "config.yaml").write_text(
        _config(enabled=True, allowlisted=True), encoding="utf-8"
    )
    job = _paused_job()

    result = json.loads(cronjob(action="resume", job_id=job["id"]))

    assert result["success"] is True
    assert jobs.get_job(job["id"])["enabled"] is True


def test_model_schema_cannot_supply_trusted_action_metadata() -> None:
    properties = CRONJOB_SCHEMA["parameters"]["properties"]
    for forbidden in (
        "action_type",
        "action_version",
        "payload",
        "fingerprint",
        "origin",
        "responder_policy",
        "handler",
    ):
        assert forbidden not in properties
