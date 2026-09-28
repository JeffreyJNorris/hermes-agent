from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cron import jobs
from gateway import durable_actions
from gateway.durable_actions import (
    DurableActionDiagnostic,
    DurableActionExecutionContext,
    PreparedDurableAction,
)
from gateway.durable_operations import DurableOperationsStore
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)


@pytest.fixture()
def cron_store(tmp_path, monkeypatch):
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    return tmp_path


def _job(**updates):
    job = {
        "id": "job-123",
        "name": "PHASE4D-CANARY private",
        "prompt": "PHASE4D-CANARY prompt",
        "skills": [],
        "skill": None,
        "model": None,
        "provider": None,
        "provider_snapshot": None,
        "model_snapshot": None,
        "base_url": None,
        "script": "secret.py",
        "no_agent": False,
        "context_from": None,
        "schedule": {"kind": "interval", "minutes": 30},
        "repeat": {"times": 4, "completed": 1},
        "enabled": False,
        "state": "paused",
        "pause_generation": "a" * 32,
        "deliver": "origin",
        "origin": None,
        "enabled_toolsets": None,
        "workdir": None,
        "attach_to_session": False,
    }
    job.update(updates)
    return job


def _unknown_action(tmp_path: Path, prepared: PreparedDurableAction) -> str:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 100.0)
    created = store.create_with_action(
        DurableRequestSpec(
            kind="approval",
            title="title",
            body="body",
            options=(DurableRequestOption("approve", "yes"), DurableRequestOption("reject", "no")),
            ttl_seconds=1_000,
            reference=prepared.idempotency_key,
        ),
        action=prepared,
        target=DurableRequestTarget("discord", "channel"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="origin",
        origin_session_id="session",
    )
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_requests SET status = 'selected' WHERE request_id = ?",
            (created.request.request_id,),
        )
        db.execute(
            "UPDATE durable_action_tickets SET status = 'unknown', completed_at = 100, "
            "notification_status = 'delivered' WHERE request_id = ?",
            (created.request.request_id,),
        )
    return created.request.request_id


def _prepared(job) -> PreparedDurableAction:
    return PreparedDurableAction(
        action_type="cron.resume",
        action_version=1,
        payload_json='{"job_id":"job-123"}',
        fingerprint=jobs.cron_job_definition_fingerprint(job),
        idempotency_key="cron.resume:diagnostic",
    )


@pytest.mark.parametrize(
    "updates,code",
    [
        ({}, "state_matches"),
        ({"enabled": True, "state": "scheduled"}, "state_applied"),
        ({"prompt": "changed"}, "state_differs"),
    ],
)
def test_cron_resume_diagnostic_is_structural_and_read_only(
    cron_store, updates, code
) -> None:
    original = _job()
    prepared = _prepared(original)
    jobs.save_jobs([{**original, **updates}])
    before = jobs.JOBS_FILE.read_bytes()
    request_id = _unknown_action(cron_store, prepared)

    detail = DurableOperationsStore(cron_store / "state.db").inspect_action(
        request_id, diagnose=True
    )

    assert detail is not None and detail.diagnostic is not None
    assert detail.diagnostic.code == code
    assert jobs.JOBS_FILE.read_bytes() == before
    assert "PHASE4D-CANARY" not in repr(detail.diagnostic)


def test_cron_resume_diagnostic_reports_missing_target(cron_store) -> None:
    original = _job()
    request_id = _unknown_action(cron_store, _prepared(original))

    detail = DurableOperationsStore(cron_store / "state.db").inspect_action(
        request_id, diagnose=True
    )

    assert detail is not None and detail.diagnostic.code == "target_missing"


def test_missing_diagnostic_and_callback_error_are_bounded(tmp_path, monkeypatch) -> None:
    prepared = PreparedDurableAction(
        action_type="test.action",
        action_version=1,
        payload_json="{}",
        fingerprint="fingerprint",
        idempotency_key="test-action",
    )
    request_id = _unknown_action(tmp_path, prepared)
    ops = DurableOperationsStore(tmp_path / "state.db")

    missing = ops.inspect_action(request_id, diagnose=True)
    assert missing is not None and missing.diagnostic.code == "handler_unavailable"

    def explode(context: DurableActionExecutionContext) -> DurableActionDiagnostic:
        raise RuntimeError("PHASE4D-CANARY secret failure")

    monkeypatch.setitem(
        durable_actions._DURABLE_ACTION_DIAGNOSTICS, ("test.action", 1), explode
    )
    failed = ops.inspect_action(request_id, diagnose=True)
    assert failed is not None and failed.diagnostic.code == "diagnostic_failed"
    assert failed.diagnostic.facts == (("exception", "RuntimeError"),)
    assert "PHASE4D-CANARY" not in repr(failed.diagnostic)


def test_diagnostic_runs_after_connection_closes(tmp_path, monkeypatch) -> None:
    prepared = PreparedDurableAction(
        action_type="test.action",
        action_version=1,
        payload_json="{}",
        fingerprint="fingerprint",
        idempotency_key="connection-close",
    )
    request_id = _unknown_action(tmp_path, prepared)
    ops = DurableOperationsStore(tmp_path / "state.db")

    class ProbeConnection(sqlite3.Connection):
        closed = False

        def close(self) -> None:
            self.closed = True
            super().close()

    connection = sqlite3.connect(
        tmp_path / "state.db", factory=ProbeConnection
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    monkeypatch.setattr(ops, "_read_connection", lambda: connection)

    def diagnose(context: DurableActionExecutionContext) -> DurableActionDiagnostic:
        assert connection.closed is True
        return DurableActionDiagnostic("state_matches")

    monkeypatch.setitem(
        durable_actions._DURABLE_ACTION_DIAGNOSTICS, ("test.action", 1), diagnose
    )

    detail = ops.inspect_action(request_id, diagnose=True)

    assert detail is not None and detail.diagnostic.code == "state_matches"
