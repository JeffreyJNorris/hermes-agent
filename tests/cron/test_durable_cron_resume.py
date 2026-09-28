from __future__ import annotations

import logging
import subprocess
import sys
import threading
from datetime import timedelta

import pytest

from cron import jobs
from gateway.durable_actions import (
    DurableActionExecutionContext,
    DurableActionStatus,
    decode_durable_action_payload,
    lookup_durable_action_handler,
    lookup_durable_action_preparer,
)
from hermes_time import now as hermes_now


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
        "name": "private report",
        "prompt": "prompt SECRET_VALUE",
        "skills": ["reporter"],
        "skill": "reporter",
        "model": "private-model",
        "provider": "private-provider",
        "provider_snapshot": "private-provider-snapshot",
        "model_snapshot": "private-model-snapshot",
        "base_url": "https://secret.example/token",
        "script": "secret-script.py",
        "no_agent": False,
        "context_from": ["upstream"],
        "schedule": {"kind": "interval", "minutes": 30, "display": "every 30m"},
        "schedule_display": "every 30m",
        "repeat": {"times": 4, "completed": 1},
        "enabled": False,
        "state": "paused",
        "paused_at": "2026-08-25T00:00:00+00:00",
        "paused_reason": "manual",
        "pause_generation": "a" * 32,
        "created_at": "2026-01-01T00:00:00+00:00",
        "next_run_at": None,
        "last_run_at": "2026-08-24T00:00:00+00:00",
        "last_status": "ok",
        "last_error": "SECRET_ERROR",
        "last_delivery_error": "SECRET_DELIVERY_ERROR",
        "deliver": "origin",
        "origin": {"platform": "discord", "chat_id": "private-chat"},
        "enabled_toolsets": ["web"],
        "workdir": "/private/workdir",
        "attach_to_session": True,
    }
    job.update(updates)
    return job


def _save(job):
    jobs.save_jobs([job])


def _context(prepared):
    return DurableActionExecutionContext(
        request_id="request-1",
        action_type=prepared.action_type,
        action_version=prepared.action_version,
        payload_json=prepared.payload_json,
        fingerprint=prepared.fingerprint,
        idempotency_key=prepared.idempotency_key,
    )


def _allow(monkeypatch):
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda: {"durable_approvals": {"enabled": True, "allowed_actions": ["cron.resume"]}},
    )


@pytest.mark.parametrize(
    "imports",
    [
        "import gateway.durable_actions as durable_actions",
        "import cron.jobs; import gateway.durable_actions as durable_actions",
    ],
)
def test_registry_is_eager_for_both_fresh_import_orders(imports):
    code = (
        f"{imports}; "
        "assert ('cron.resume', 1) in durable_actions._DURABLE_ACTION_HANDLERS; "
        "assert ('cron.resume', 2) not in durable_actions._DURABLE_ACTION_HANDLERS"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.parametrize(
    "imports",
    [
        "import gateway.durable_actions as durable_actions; import cron.jobs as cron_jobs",
        "import cron.jobs as cron_jobs; import gateway.durable_actions as durable_actions",
    ],
)
def test_registry_survives_gateway_and_cron_module_reloads(imports):
    code = (
        "import importlib; "
        f"{imports}; "
        "importlib.reload(durable_actions); "
        "assert ('cron.resume', 1) in durable_actions._DURABLE_ACTION_HANDLERS; "
        "importlib.reload(cron_jobs); "
        "assert durable_actions.lookup_durable_action_handler('cron.resume', 1) is cron_jobs.execute_durable_cron_resume; "
        "assert durable_actions.lookup_durable_action_handler('cron.resume', 2) is None"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_registry_has_exact_cron_resume_v1():
    assert lookup_durable_action_preparer("cron.resume", 1) is not None
    assert lookup_durable_action_handler("cron.resume", 1) is not None
    assert lookup_durable_action_handler("cron.resume", 2) is None


def test_builtin_registration_rejects_foreign_collision(monkeypatch):
    from gateway import durable_actions

    monkeypatch.setattr(
        durable_actions,
        "_DURABLE_ACTION_HANDLERS",
        {("cron.resume", 1): (lambda: None, lambda _: None)},
    )
    with pytest.raises(ValueError, match="already registered"):
        durable_actions._register_cron_resume_builtin(
            prepare=jobs.prepare_durable_cron_resume,
            execute=jobs.execute_durable_cron_resume,
        )


def test_prepare_uses_canonical_id_and_minimal_payload(cron_store):
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    assert prepare is not None

    by_id = prepare("job-123")

    assert decode_durable_action_payload(by_id.payload_json) == {"job_id": "job-123"}
    assert by_id.idempotency_key == f"cron.resume:job-123:{by_id.fingerprint}"
    assert "SECRET_VALUE" not in repr(by_id)
    assert "secret.example" not in repr(by_id)


@pytest.mark.parametrize("ref", ["private report", "private", "job-12"])
def test_prepare_rejects_noncanonical_job_references(cron_store, ref):
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    with pytest.raises(ValueError, match="not found"):
        prepare(ref)


def test_prepare_rejects_ambiguous_name_instead_of_resolving_it(cron_store):
    jobs.save_jobs(
        [
            _job(id="job-123"),
            _job(id="job-456"),
        ]
    )
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    with pytest.raises(ValueError, match="not found"):
        prepare("private report")


def test_legacy_pause_generation_backfill_is_stable(cron_store):
    legacy = _job()
    legacy.pop("pause_generation", None)
    _save(legacy)
    prepare = lookup_durable_action_preparer("cron.resume", 1)

    first = prepare("job-123")
    first_job = jobs.get_job("job-123")
    second = prepare("job-123")
    second_job = jobs.get_job("job-123")

    assert first == second
    assert len(first_job["pause_generation"]) == 32
    assert second_job["pause_generation"] == first_job["pause_generation"]


def test_stateless_legacy_paused_job_backfills_once_and_executes(
    cron_store,
    monkeypatch,
):
    _allow(monkeypatch)
    legacy = _job()
    legacy.pop("state")
    legacy.pop("pause_generation")
    _save(legacy)
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)

    first = prepare("job-123")
    persisted = jobs.load_jobs()[0]
    second = prepare("job-123")
    outcome = execute(_context(first))

    assert first == second
    assert persisted["state"] == "paused"
    assert len(persisted["pause_generation"]) == 32
    assert outcome.status is DurableActionStatus.SUCCEEDED


def test_stateless_enabled_legacy_job_is_ineligible_without_mutation(cron_store):
    legacy = _job(enabled=True)
    legacy.pop("state")
    legacy.pop("pause_generation")
    _save(legacy)
    before = jobs.JOBS_FILE.read_bytes()
    prepare = lookup_durable_action_preparer("cron.resume", 1)

    with pytest.raises(ValueError, match="not paused"):
        prepare("job-123")

    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize(
    "job",
    [
        _job(enabled=True, state="scheduled"),
        _job(enabled=True, state="paused"),
        _job(enabled=False, state="scheduled"),
    ],
)
def test_prepare_requires_consistently_paused_job(cron_store, job):
    _save(job)
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    with pytest.raises(ValueError, match="not paused"):
        prepare("job-123")


def test_fingerprint_tracks_definition_but_not_runtime_state():
    original = _job()
    fingerprint = jobs.cron_job_definition_fingerprint(original)
    relevant_changes = [
        {"id": "job-456"},
        {"pause_generation": "b" * 32},
        {"name": "renamed"},
        {"schedule": {"kind": "interval", "minutes": 31}},
        {"repeat": {"times": 5, "completed": 1}},
        {"prompt": "changed prompt"},
        {"skills": ["other"], "skill": "other"},
        {"script": "other.py"},
        {"no_agent": True},
        {"context_from": ["other"]},
        {"provider": "other"},
        {"model": "other"},
        {"base_url": "https://other.example"},
        {"provider_snapshot": "other"},
        {"model_snapshot": "other"},
        {"workdir": "/other"},
        {"enabled_toolsets": ["terminal"]},
        {"deliver": "local"},
        {"origin": {"platform": "telegram", "chat_id": "other"}},
        {"attach_to_session": False},
    ]
    for changes in relevant_changes:
        assert jobs.cron_job_definition_fingerprint({**original, **changes}) != fingerprint

    volatile = {
        "enabled": True,
        "state": "scheduled",
        "paused_at": None,
        "paused_reason": None,
        "next_run_at": "2099-01-01T00:00:00+00:00",
        "last_run_at": "2098-01-01T00:00:00+00:00",
        "last_status": "failed",
        "last_error": "different",
        "last_delivery_error": "different",
        "repeat": {"times": 4, "completed": 999},
        "run_claim": {"token": "volatile"},
        "created_at": "2099-01-01T00:00:00+00:00",
    }
    assert jobs.cron_job_definition_fingerprint({**original, **volatile}) == fingerprint


def test_execute_resumes_once_and_then_reports_already_applied(cron_store, monkeypatch):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    prepared = prepare("job-123")
    notifications = []
    monkeypatch.setattr("cron.scheduler._notify_provider_jobs_changed", lambda: notifications.append(True))

    first = execute(_context(prepared))
    second = execute(_context(prepared))

    resumed = jobs.get_job("job-123")
    assert first.status is DurableActionStatus.SUCCEEDED
    assert second.status is DurableActionStatus.ALREADY_APPLIED
    assert resumed["enabled"] is True
    assert resumed["state"] == "scheduled"
    assert resumed["repeat"] == {"times": 4, "completed": 1}
    assert resumed["pause_generation"]
    assert resumed["next_run_at"] is not None
    assert notifications == [True]


def test_old_approval_cannot_replay_across_a_new_pause_generation(
    cron_store,
    monkeypatch,
):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    pause_a = prepare("job-123")

    jobs.resume_job("job-123")
    jobs.pause_job("job-123")
    paused_b = jobs.get_job("job-123")
    before = jobs.JOBS_FILE.read_bytes()

    old_outcome = execute(_context(pause_a))
    after_old = jobs.JOBS_FILE.read_bytes()
    pause_b = prepare("job-123")
    new_outcome = execute(_context(pause_b))

    assert old_outcome.status is DurableActionStatus.INVALIDATED
    assert old_outcome.reason == "job_changed"
    assert paused_b["enabled"] is False
    assert after_old == before
    assert pause_b.fingerprint != pause_a.fingerprint
    assert pause_b.idempotency_key != pause_a.idempotency_key
    assert new_outcome.status is DurableActionStatus.SUCCEEDED


def test_every_pause_call_advances_generation(cron_store):
    _save(_job(enabled=True, state="scheduled"))

    first = jobs.pause_job("job-123")
    second = jobs.pause_job("job-123")

    assert first["pause_generation"] != second["pause_generation"]


@pytest.mark.parametrize("case", ["changed", "missing", "invalid"])
def test_execute_invalidates_changed_missing_or_invalid_job(cron_store, monkeypatch, case):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    if case == "changed":
        _save(_job(prompt="new secret"))
    elif case == "missing":
        jobs.save_jobs([], replace=True)
    else:
        _save(_job(schedule={"kind": "interval"}))
    before = jobs.JOBS_FILE.read_bytes()
    notifications = []
    monkeypatch.setattr(
        "cron.scheduler._notify_provider_jobs_changed",
        lambda: notifications.append(True),
    )

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert jobs.JOBS_FILE.read_bytes() == before
    assert notifications == []
    assert "secret" not in (outcome.reason or "").lower()


@pytest.mark.parametrize(
    "invalid_update",
    [
        {"schedule": {"kind": "cron", "expr": "not a cron expression"}},
        {"repeat": {"times": 4, "completed": -1}},
        {"repeat": {"times": 4, "completed": 4}},
        {"repeat": {"times": 4, "completed": True}},
    ],
)
def test_execute_invalidates_malformed_cron_and_repeat_state(
    cron_store,
    monkeypatch,
    invalid_update,
):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    _save({**_job(), **invalid_update})
    before = jobs.JOBS_FILE.read_bytes()

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert outcome.reason == "job_invalid"
    assert jobs.JOBS_FILE.read_bytes() == before


def test_execute_invalidates_cron_when_croniter_is_unavailable(
    cron_store,
    monkeypatch,
):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    _save(_job(schedule={"kind": "cron", "expr": "0 9 * * *"}))
    monkeypatch.setattr(jobs, "HAS_CRONITER", False)
    before = jobs.JOBS_FILE.read_bytes()

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert outcome.reason == "job_invalid"
    assert jobs.JOBS_FILE.read_bytes() == before


def test_execute_revalidates_policy_before_mutation(cron_store, monkeypatch):
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda: {"durable_approvals": {"enabled": False, "allowed_actions": ["cron.resume"]}},
    )
    before = jobs.JOBS_FILE.read_bytes()

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert outcome.reason == "permission_revoked"
    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize("malformed_policy", ["enabled", ["cron.resume"], 1])
def test_execute_denies_malformed_truthy_policy(
    cron_store,
    monkeypatch,
    malformed_policy,
):
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    monkeypatch.setattr(
        "hermes_cli.config.load_config",
        lambda: {"durable_approvals": malformed_policy},
    )
    before = jobs.JOBS_FILE.read_bytes()

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert outcome.reason == "permission_revoked"
    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("name", 1),
        ("prompt", []),
        ("skills", "reporter"),
        ("skills", [1]),
        ("skill", 1),
        ("script", 1),
        ("no_agent", 1),
        ("context_from", "upstream"),
        ("enabled_toolsets", "web"),
        ("workdir", 1),
        ("provider", 1),
        ("model", []),
        ("base_url", {}),
        ("provider_snapshot", 1),
        ("model_snapshot", []),
        ("deliver", 1),
        ("origin", "discord"),
        ("attach_to_session", 1),
        ("pause_generation", "not-a-generation"),
    ],
)
def test_execute_invalidates_corrupted_definition_field(
    cron_store,
    monkeypatch,
    field,
    bad_value,
):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    current = jobs.load_jobs()[0]
    current[field] = bad_value
    _save(current)
    before = jobs.JOBS_FILE.read_bytes()

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert outcome.reason == "job_invalid"
    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize("unsafe_id", ["../escape", "bad\x00id", "bad\x1fid", "bad\x7fid"])
def test_execute_invalidates_unsafe_canonical_id(
    cron_store,
    monkeypatch,
    unsafe_id,
):
    _allow(monkeypatch)
    unsafe = _job(id=unsafe_id, pause_generation="a" * 32)
    _save(unsafe)
    fingerprint = jobs.cron_job_definition_fingerprint(unsafe)
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload

    context = _context(
        PreparedDurableAction(
            action_type="cron.resume",
            action_version=1,
            payload_json=encode_durable_action_payload({"job_id": unsafe_id}),
            fingerprint=fingerprint,
            idempotency_key="unsafe",
        )
    )
    before = jobs.JOBS_FILE.read_bytes()

    outcome = lookup_durable_action_handler("cron.resume", 1)(context)

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert outcome.reason == "job_invalid"
    assert jobs.JOBS_FILE.read_bytes() == before


@pytest.mark.parametrize("unsafe_id", ["bad\x00id", "bad\x1fid", "bad\x7fid"])
def test_job_output_dir_rejects_ascii_control_characters(cron_store, unsafe_id):
    with pytest.raises(ValueError, match="Invalid cron job id"):
        jobs._job_output_dir(unsafe_id)


def test_past_one_shot_fails_bounded_without_mutation(cron_store, monkeypatch):
    _allow(monkeypatch)
    run_at = (hermes_now() + timedelta(seconds=30)).isoformat()
    _save(_job(schedule={"kind": "once", "run_at": run_at}, repeat={"times": 1, "completed": 0}))
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    monkeypatch.setattr(jobs, "_hermes_now", lambda: hermes_now() + timedelta(hours=1))
    before = jobs.JOBS_FILE.read_bytes()

    outcome = execute(context)

    assert outcome.status is DurableActionStatus.FAILED
    assert outcome.reason == "schedule_unavailable"
    assert jobs.JOBS_FILE.read_bytes() == before


def test_provider_notification_is_after_save_and_best_effort(cron_store, monkeypatch):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    observed = []

    def notify():
        observed.append(jobs.get_job("job-123")["enabled"])
        raise RuntimeError("provider secret")

    monkeypatch.setattr("cron.scheduler._notify_provider_jobs_changed", notify)
    outcome = execute(context)
    assert outcome.status is DurableActionStatus.SUCCEEDED
    assert observed == [True]


def test_provider_notification_import_failure_is_best_effort(
    cron_store,
    monkeypatch,
):
    import builtins

    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    context = _context(prepare("job-123"))
    original_import = builtins.__import__

    def fail_scheduler_import(name, *args, **kwargs):
        if name == "cron.scheduler":
            raise ImportError("provider secret")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_scheduler_import)
    outcome = execute(context)

    assert outcome.status is DurableActionStatus.SUCCEEDED
    assert jobs.get_job("job-123")["enabled"] is True


def test_fingerprint_and_save_share_one_lock(cron_store, monkeypatch):
    _save(_job())
    expected = jobs.cron_job_definition_fingerprint(_job())
    entered_save = threading.Event()
    competitor_done = threading.Event()
    original_save = jobs._save_jobs_unlocked

    def blocked_save(records, **kwargs):
        entered_save.set()
        assert not competitor_done.wait(0.05)
        original_save(records, **kwargs)

    monkeypatch.setattr(jobs, "_save_jobs_unlocked", blocked_save)
    result = []
    worker = threading.Thread(target=lambda: result.append(jobs.resume_job_if_unchanged("job-123", expected)))
    worker.start()
    assert entered_save.wait(2)

    competitor = threading.Thread(
        target=lambda: (jobs.update_job("job-123", {"prompt": "competing update"}), competitor_done.set())
    )
    competitor.start()
    worker.join(2)
    competitor.join(2)

    assert result[0] == ("succeeded", None)
    assert competitor_done.is_set()
    assert jobs.get_job("job-123")["prompt"] == "competing update"


def test_no_sensitive_values_are_logged(cron_store, monkeypatch, caplog):
    _allow(monkeypatch)
    _save(_job())
    prepare = lookup_durable_action_preparer("cron.resume", 1)
    execute = lookup_durable_action_handler("cron.resume", 1)
    with caplog.at_level(logging.DEBUG):
        execute(_context(prepare("job-123")))
    text = caplog.text
    for secret in ("SECRET_VALUE", "secret-script.py", "secret.example", "private-model"):
        assert secret not in text
