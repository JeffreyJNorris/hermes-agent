from __future__ import annotations

from copy import deepcopy

import pytest

import cron.jobs as jobs
import cron.scheduler as scheduler
from cron.executions import ExecutionAlreadyRunning


@pytest.fixture(autouse=True)
def _reset_scheduler_guards():
    with scheduler._running_lock:
        scheduler._running_job_ids.clear()
        scheduler._interrupted_job_ids.clear()
    yield
    with scheduler._running_lock:
        scheduler._running_job_ids.clear()
        scheduler._interrupted_job_ids.clear()


def _job(**overrides):
    value = {
        "id": "job-1",
        "name": "Weekly release",
        "enabled": True,
        "state": "scheduled",
        "prompt": "Inspect the release and follow the job policy.",
        "script": "collect-release-state.sh",
        "context_from": ["upstream-job"],
        "workdir": "",
        "next_run_at": "2026-08-10T00:00:00+00:00",
        "repeat": {"times": 4, "completed": 1},
        "deliver": "discord:channel",
    }
    value.update(overrides)
    return value


def _enqueue(**overrides):
    arguments = {
        "job_id": "job-1",
        "request_id": "drq_abcdefghijklmnopqrstuvwxyz012345",
        "reference": "weekly-release",
        "status": "selected",
        "title": "Approve weekly release",
        "question": "Should the reviewed release continue?",
        "selected_index": 1,
        "selected_label": "Approve",
        "terminal_reason": None,
        "adapters": {"discord": object()},
        "loop": object(),
    }
    arguments.update(overrides)
    return scheduler.enqueue_durable_request_continuation(**arguments)


def _enqueue_action(**overrides):
    arguments = {
        "job_id": "job-1",
        "request_id": "drq_abcdefghijklmnopqrstuvwxyz012345",
        "action_type": "cron.resume",
        "action_status": "succeeded",
        "outcome_reason": "job_resumed",
        "adapters": {"discord": object()},
        "loop": object(),
    }
    arguments.update(overrides)
    return scheduler.enqueue_durable_action_continuation(**arguments)


class ImmediatePool:
    def __init__(self) -> None:
        self.submissions = 0

    def submit(self, fn):
        self.submissions += 1
        fn()
        return object()


class BrokenPool:
    def submit(self, _fn):
        raise RuntimeError("executor unavailable")


def test_continuation_uses_current_job_without_mutating_schedule(
    monkeypatch,
) -> None:
    job = _job()
    original = deepcopy(job)
    pool = ImmediatePool()
    executions = []
    runs = []

    monkeypatch.setattr(jobs, "get_job", lambda _job_id: job)
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda job_id, *, source, trigger_id: (
            executions.append((job_id, source, trigger_id))
            or ({"id": "execution-1"}, True)
        ),
    )
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: pool)

    def run_one_job(continuation_job, **kwargs):
        runs.append((continuation_job, kwargs))
        return True

    monkeypatch.setattr(scheduler, "run_one_job", run_one_job)

    outcome = _enqueue()

    assert outcome == ("accepted", None)
    assert executions == [
        (
            "job-1",
            "durable_request",
            "drq_abcdefghijklmnopqrstuvwxyz012345",
        )
    ]
    assert pool.submissions == 1
    assert len(runs) == 1
    continuation, kwargs = runs[0]
    assert continuation["execution_id"] == "execution-1"
    assert "script" not in continuation
    assert "context_from" not in continuation
    assert "Request ID: drq_abcdefghijklmnopqrstuvwxyz012345" in continuation["prompt"]
    assert "Selected option: 1. Approve" in continuation["prompt"]
    assert "not an authorization token" in continuation["prompt"]
    assert continuation["_durable_schedule_neutral"] is True
    assert "schedule_run" not in kwargs
    assert kwargs["adapters"] is not None
    assert kwargs["loop"] is not None
    assert job == original
    assert scheduler.get_running_job_ids() == frozenset()


def test_action_completion_uses_separate_idempotent_schedule_neutral_source(
    monkeypatch,
) -> None:
    job = _job(prompt="Use the current job policy.")
    original = deepcopy(job)
    executions = []
    runs = []
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: job)
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda job_id, *, source, trigger_id: (
            executions.append((job_id, source, trigger_id))
            or ({"id": "action-execution"}, True)
        ),
    )
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: ImmediatePool())
    monkeypatch.setattr(
        scheduler,
        "run_one_job",
        lambda continuation, **kwargs: runs.append((continuation, kwargs)) or True,
    )

    assert _enqueue_action() == ("accepted", None)

    assert executions == [
        ("job-1", "durable_action", "drq_abcdefghijklmnopqrstuvwxyz012345")
    ]
    assert len(runs) == 1
    continuation, kwargs = runs[0]
    assert "script" not in continuation
    assert "context_from" not in continuation
    assert "Use the current job policy." in continuation["prompt"]
    assert "Action: cron.resume" in continuation["prompt"]
    assert "Action status: succeeded" in continuation["prompt"]
    assert "job_resumed" in continuation["prompt"]
    assert "payload" not in continuation["prompt"]
    assert continuation["_durable_schedule_neutral"] is True
    assert "schedule_run" not in kwargs
    assert job == original


def test_action_completion_replay_does_not_submit_twice(monkeypatch) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: ({"id": "existing"}, False),
    )
    monkeypatch.setattr(
        scheduler,
        "_get_parallel_pool",
        lambda _workers: pytest.fail("must not submit twice"),
    )

    assert _enqueue_action() == ("accepted", "continuation_already_recorded")


@pytest.mark.parametrize(
    ("job", "reason"),
    [
        (None, "origin_job_missing"),
        (_job(state="paused"), "origin_job_inactive"),
        (_job(enabled=False, state="scheduled"), "origin_job_inactive"),
        (_job(no_agent=True), "origin_job_has_no_agent"),
    ],
)
def test_unavailable_origin_job_is_dropped(
    monkeypatch,
    job,
    reason: str,
) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: job)
    create = lambda *_args, **_kwargs: pytest.fail("must not create execution")
    monkeypatch.setattr(scheduler, "create_triggered_execution", create)

    assert _enqueue() == ("dropped", reason)


def test_retained_completed_one_shot_can_receive_continuation(
    monkeypatch,
) -> None:
    job = _job(enabled=False, state="completed")
    pool = ImmediatePool()
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: job)
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: ({"id": "execution"}, True),
    )
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: pool)
    monkeypatch.setattr(scheduler, "run_one_job", lambda *_args, **_kwargs: True)

    assert _enqueue() == ("accepted", None)
    assert pool.submissions == 1


def test_running_origin_job_defers_continuation(monkeypatch) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    with scheduler._running_lock:
        scheduler._running_job_ids.add(scheduler._inflight_key("job-1"))

    assert _enqueue() == ("retry", "origin_job_running")


def test_cross_process_ledger_contention_defers_continuation(monkeypatch) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ExecutionAlreadyRunning("owned by another process")
        ),
    )

    assert _enqueue() == ("retry", "origin_job_running")
    assert scheduler.get_running_job_ids() == frozenset()


def test_existing_trigger_is_acknowledged_without_second_submission(
    monkeypatch,
) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: (
            {"id": "existing", "status": "unknown"},
            False,
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "_get_parallel_pool",
        lambda _workers: pytest.fail("must not submit twice"),
    )

    assert _enqueue() == ("accepted", "continuation_already_recorded")
    assert scheduler.get_running_job_ids() == frozenset()


def test_definite_submit_failure_releases_reservation_for_retry(
    monkeypatch,
) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: ({"id": "execution"}, True),
    )
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: BrokenPool())
    abandoned = []
    monkeypatch.setattr(
        scheduler,
        "abandon_claimed_execution",
        lambda execution_id: abandoned.append(execution_id) or True,
    )

    assert _enqueue() == ("retry", "executor_dispatch_failed")
    assert abandoned == ["execution"]
    assert scheduler.get_running_job_ids() == frozenset()


def test_ambiguous_submit_cleanup_preserves_at_most_once(
    monkeypatch,
) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: ({"id": "execution"}, True),
    )
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: BrokenPool())
    monkeypatch.setattr(
        scheduler,
        "abandon_claimed_execution",
        lambda _execution_id: False,
    )

    assert _enqueue() == ("accepted", "continuation_dispatch_unknown")
    assert scheduler.get_running_job_ids() == frozenset()


def test_executor_initialization_failure_releases_reservation(
    monkeypatch,
) -> None:
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: _job())
    monkeypatch.setattr(
        scheduler,
        "create_triggered_execution",
        lambda *_args, **_kwargs: ({"id": "execution"}, True),
    )
    def fail_to_create_pool(_workers):
        raise RuntimeError("pool unavailable")

    monkeypatch.setattr(scheduler, "_get_parallel_pool", fail_to_create_pool)
    abandoned = []
    monkeypatch.setattr(
        scheduler,
        "abandon_claimed_execution",
        lambda execution_id: abandoned.append(execution_id) or True,
    )

    assert _enqueue() == ("retry", "executor_dispatch_failed")
    assert abandoned == ["execution"]
    assert scheduler.get_running_job_ids() == frozenset()


def test_schedule_neutral_run_skips_dispatch_and_job_state_mutation(
    monkeypatch,
) -> None:
    events = []
    monkeypatch.setattr(
        scheduler,
        "claim_dispatch",
        lambda _job_id: pytest.fail("continuation must not consume dispatch"),
    )
    monkeypatch.setattr(
        scheduler,
        "mark_job_run",
        lambda *_args, **_kwargs: pytest.fail("continuation must not mutate schedule"),
    )
    monkeypatch.setattr(
        scheduler,
        "mark_execution_running",
        lambda execution_id: events.append(("running", execution_id)) or {"id": execution_id},
    )
    monkeypatch.setattr(
        scheduler,
        "finish_execution",
        lambda execution_id, **kwargs: events.append(
            ("finished", execution_id, kwargs)
        ),
    )
    monkeypatch.setattr(
        scheduler,
        "run_job",
        lambda _job, **_kwargs: (
            True,
            "output",
            "continuation complete",
            None,
        ),
    )
    monkeypatch.setattr(scheduler, "save_job_output", lambda *_args: None)
    monkeypatch.setattr(scheduler, "_deliver_result", lambda *_args, **_kwargs: None)

    assert scheduler.run_one_job(
        {"id": "job-1", "execution_id": "execution", "_durable_schedule_neutral": True},
    ) is True
    assert events[0] == ("running", "execution")
    assert events[-1][0:2] == ("finished", "execution")
    assert events[-1][2]["success"] is True


def test_shutdown_marks_schedule_neutral_execution_without_job_status_write(
    monkeypatch,
) -> None:
    calls = []
    monkeypatch.setattr(
        scheduler,
        "mark_job_run",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    with scheduler._running_lock:
        scheduler._running_job_ids.add(scheduler._inflight_key("job-1"))

    assert scheduler.mark_running_jobs_interrupted("shutdown") == ["job-1"]
    assert calls == []
    assert scheduler._is_interrupted("job-1") is True


def test_multi_continuation_keeps_canonical_labels_and_current_schedule(monkeypatch):
    job = _job()
    original = deepcopy(job)
    runs = []
    monkeypatch.setattr(jobs, "get_job", lambda _job_id: job)
    monkeypatch.setattr(scheduler, "create_triggered_execution", lambda *args, **kwargs: ({"id": "set-continuation"}, True))
    monkeypatch.setattr(scheduler, "_get_parallel_pool", lambda _workers: ImmediatePool())
    monkeypatch.setattr(scheduler, "run_one_job", lambda continuation, **kwargs: runs.append(continuation) or True)
    assert _enqueue(selected_index=None, selected_label=None,
                    selected_indices=[2, 4], selected_labels=["Gas", "Internet"]) == ("accepted", None)
    assert "Selected options:\n2. Gas\n4. Internet" in runs[0]["prompt"]
    assert "option_" not in runs[0]["prompt"]
    assert job == original
