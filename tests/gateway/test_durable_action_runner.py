from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest

from gateway import durable_actions
from gateway.durable_action_runner import DurableActionRunner
from gateway.durable_actions import (
    DurableActionOutcome,
    DurableActionStatus,
    DurableActionStore,
)
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)


@pytest.fixture(autouse=True)
def _isolate_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(durable_actions, "_DURABLE_ACTION_HANDLERS", {})


def _ready_ticket(
    db_path: Path,
    *,
    action_type: str = "test.execute",
    version: int = 1,
) -> str:
    request_store = DurableRequestStore(db_path, wall_clock=lambda: 1_000.0)
    created = request_store.create_with_action(
        DurableRequestSpec(
            kind="test.approval",
            title="Approve?",
            body="Run the test action.",
            options=(
                DurableRequestOption("approve", "Approve"),
                DurableRequestOption("reject", "Reject"),
            ),
            ttl_seconds=60,
        ),
        action=durable_actions.PreparedDurableAction(
            action_type=action_type,
            action_version=version,
            payload_json=durable_actions.encode_durable_action_payload({"value": 7}),
            fingerprint="sha256:test",
            idempotency_key=f"{action_type}:{version}",
        ),
        target=DurableRequestTarget("discord", "chat"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="origin",
        origin_session_id="session",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET status = 'ready' WHERE request_id = ?",
            (created.request.request_id,),
        )
    return created.request.request_id


def _ticket(db_path: Path, request_id: str) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM durable_action_tickets WHERE request_id = ?", (request_id,)
        ).fetchone()
    assert row is not None
    return row


def _register(execute) -> None:
    durable_actions.register_durable_action_handler(
        "test.execute", 1, prepare=lambda: None, execute=execute
    )


def _run_after_transaction_wait(store: DurableActionStore, now, advanced, operation):
    original = store._transaction
    waiting = threading.Event()
    release = threading.Event()

    @contextmanager
    def blocked():
        waiting.set()
        assert release.wait(5)
        with original() as conn:
            yield conn

    store._transaction = blocked  # type: ignore[method-assign]
    results = []
    thread = threading.Thread(target=lambda: results.append(operation()))
    thread.start()
    assert waiting.wait(5)
    now[0] = advanced
    release.set()
    thread.join()
    return results[0]


def test_two_independent_stores_have_exactly_one_claim_winner(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    stores = [DurableActionStore(db_path), DurableActionStore(db_path)]
    barrier = threading.Barrier(2)
    claims: list[object] = []

    def claim(store: DurableActionStore) -> None:
        barrier.wait()
        claims.append(store.claim_next())

    threads = [threading.Thread(target=claim, args=(store,)) for store in stores]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0].context.request_id == request_id


def test_claim_token_timestamp_and_token_guards(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    store = DurableActionStore(db_path, wall_clock=lambda: 2_000.0)

    claim = store.claim_next()
    assert claim is not None
    assert len(claim.token) >= 32
    assert claim.claimed_at == 2_000.0
    claimed_row = _ticket(db_path, request_id)
    assert claimed_row["execution_claim"] == claim.token
    assert claimed_row["execution_claimed_at"] == 2_000.0
    assert store.mark_executing(request_id, "wrong") is False
    assert store.complete(
        request_id,
        "wrong",
        DurableActionOutcome(DurableActionStatus.SUCCEEDED),
    ) is False
    assert store.mark_executing(request_id, claim.token) is True
    assert store.renew_execution_claim(request_id, "wrong") is False
    assert store.renew_execution_claim(request_id, claim.token) is True
    assert store.complete(
        request_id,
        claim.token,
        DurableActionOutcome(
            DurableActionStatus.SUCCEEDED,
            result_json=durable_actions.encode_durable_action_result({"ok": True}),
        ),
    ) is True
    assert store.complete(
        request_id,
        claim.token,
        DurableActionOutcome(DurableActionStatus.FAILED, reason="replay"),
    ) is False
    assert store.renew_execution_claim(request_id, claim.token) is False
    row = _ticket(db_path, request_id)
    assert row["execution_started_at"] == 2_000.0
    assert row["status"] == "succeeded"


def test_claim_timestamp_is_sampled_after_lock_wait(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    now = [1_000.0]
    store = DurableActionStore(db_path, wall_clock=lambda: now[0])
    claim = _run_after_transaction_wait(store, now, 1_400.0, store.claim_next)
    assert claim is not None
    assert claim.claimed_at == 1_400.0
    assert _ticket(db_path, request_id)["execution_claimed_at"] == 1_400.0
    assert store.recover_stale(300) == (0, 0)


def test_execution_timestamp_is_sampled_after_lock_wait(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    now = [1_000.0]
    store = DurableActionStore(db_path, wall_clock=lambda: now[0])
    claim = store.claim_next()
    assert claim is not None
    started = _run_after_transaction_wait(
        store,
        now,
        1_800.0,
        lambda: store.mark_executing(request_id, claim.token),
    )
    assert started is True
    assert _ticket(db_path, request_id)["execution_started_at"] == 1_800.0
    assert store.recover_stale(300) == (0, 0)


@pytest.mark.parametrize("transition", ["complete", "handler_unavailable"])
def test_terminal_timestamps_are_sampled_after_lock_wait(
    tmp_path: Path,
    transition: str,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    now = [1_000.0]
    store = DurableActionStore(db_path, wall_clock=lambda: now[0])
    claim = store.claim_next()
    assert claim is not None
    if transition == "complete":
        assert store.mark_executing(request_id, claim.token)
        operation = lambda: store.complete(
            request_id,
            claim.token,
            DurableActionOutcome(DurableActionStatus.SUCCEEDED),
        )
    else:
        operation = lambda: store.mark_handler_unavailable(
            request_id,
            claim.token,
            reason="handler_unavailable",
        )
    changed = _run_after_transaction_wait(store, now, 1_400.0, operation)

    assert changed is True
    row = _ticket(db_path, request_id)
    assert row["completed_at"] == 1_400.0
    assert row["updated_at"] == 1_400.0


def test_stale_recovery_samples_cutoff_after_lock_wait(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    now = [1_000.0]
    store = DurableActionStore(db_path, wall_clock=lambda: now[0])
    assert store.claim_next() is not None
    recovered = _run_after_transaction_wait(
        store, now, 1_400.0, lambda: store.recover_stale(300)
    )

    assert recovered == (1, 0)
    row = _ticket(db_path, request_id)
    assert row["status"] == "ready"
    assert row["updated_at"] == 1_400.0


def test_stale_claim_recovery_and_stale_execution_terminalization(tmp_path: Path) -> None:
    claimed_db = tmp_path / "claimed.db"
    claimed_id = _ready_ticket(claimed_db)
    now = [1_000.0]
    store = DurableActionStore(claimed_db, wall_clock=lambda: now[0])
    old_claim = store.claim_next()
    assert old_claim is not None
    now[0] = 1_301.0
    assert store.recover_stale(300) == (1, 0)
    replacement = store.claim_next()
    assert replacement is not None and replacement.token != old_claim.token
    assert store.mark_executing(claimed_id, old_claim.token) is False

    executing_db = tmp_path / "executing.db"
    executing_id = _ready_ticket(executing_db)
    now[0] = 2_000.0
    executing_store = DurableActionStore(executing_db, wall_clock=lambda: now[0])
    claim = executing_store.claim_next()
    assert claim is not None
    assert executing_store.mark_executing(executing_id, claim.token)
    now[0] = 2_301.0
    assert executing_store.recover_stale(300) == (0, 0)
    observed = _ticket(executing_db, executing_id)
    assert observed["status"] == "executing"
    assert observed["execution_claimed_at"] == 2_000.0
    assert observed["updated_at"] == 2_301.0
    now[0] = 2_602.0
    assert executing_store.recover_stale(300) == (0, 1)
    row = _ticket(executing_db, executing_id)
    assert (row["status"], row["outcome_reason"]) == (
        "unknown",
        "execution_interrupted",
    )
    assert row["notification_status"] == "pending"
    assert row["completed_at"] == 2_602.0
    assert executing_store.claim_next() is None


def test_forward_clock_jump_gets_grace_and_heartbeat_clears_marker(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    now = [1_000.0]
    store = DurableActionStore(db_path, wall_clock=lambda: now[0])
    claim = store.claim_next()
    assert claim is not None and store.mark_executing(request_id, claim.token)
    now[0] = 10_000.0

    assert store.recover_stale(300) == (0, 0)
    observed = _ticket(db_path, request_id)
    assert observed["status"] == "executing"
    assert observed["execution_claimed_at"] == 1_000.0
    assert observed["updated_at"] == 10_000.0
    assert store.renew_execution_claim(request_id, claim.token) is True
    renewed = _ticket(db_path, request_id)
    assert renewed["execution_claimed_at"] == 10_000.0
    assert renewed["updated_at"] == 10_000.0
    assert store.complete(
        request_id,
        claim.token,
        DurableActionOutcome(DurableActionStatus.SUCCEEDED),
    )
    assert _ticket(db_path, request_id)["status"] == "succeeded"


@pytest.mark.parametrize("sample", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_wall_clock_is_rejected_without_mutation(
    tmp_path: Path,
    sample: float,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    store = DurableActionStore(db_path, wall_clock=lambda: sample)

    with pytest.raises(ValueError, match="wall clock"):
        store.claim_next()

    assert _ticket(db_path, request_id)["status"] == "ready"


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf")])
def test_stale_interval_must_be_finite_and_positive(
    tmp_path: Path,
    interval: float,
) -> None:
    store = DurableActionStore(tmp_path / "state.db")

    with pytest.raises(ValueError, match="stale"):
        store.recover_stale(interval)
    with pytest.raises(ValueError, match="stale"):
        DurableActionRunner(store, enabled=True, stale_claim_seconds=interval)


@pytest.mark.parametrize(
    "status",
    [
        DurableActionStatus.SUCCEEDED,
        DurableActionStatus.ALREADY_APPLIED,
        DurableActionStatus.INVALIDATED,
        DurableActionStatus.FAILED,
        DurableActionStatus.UNKNOWN,
        DurableActionStatus.HANDLER_UNAVAILABLE,
    ],
)
def test_terminal_outcomes_are_immutable(tmp_path: Path, status: DurableActionStatus) -> None:
    db_path = tmp_path / f"{status.value}.db"
    request_id = _ready_ticket(db_path)
    store = DurableActionStore(db_path)
    claim = store.claim_next()
    assert claim is not None and store.mark_executing(request_id, claim.token)
    assert store.complete(
        request_id, claim.token, DurableActionOutcome(status, reason="first")
    )
    assert not store.complete(
        request_id,
        claim.token,
        DurableActionOutcome(DurableActionStatus.FAILED, reason="second"),
    )
    assert store.claim_next() is None
    assert _ticket(db_path, request_id)["outcome_reason"] == "first"


def test_disabled_runner_leaves_ready_ticket_untouched(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    runner = DurableActionRunner(DurableActionStore(db_path), enabled=False)

    assert asyncio.run(runner.process_once()) is False
    row = _ticket(db_path, request_id)
    assert row["status"] == "ready" and row["execution_claim"] is None


@pytest.mark.parametrize(
    ("allowed", "version", "reason"),
    [([], 1, "action_not_allowlisted"), (["test.execute"], 2, "handler_unavailable")],
)
def test_unavailable_actions_are_terminal_without_handler_call(
    tmp_path: Path, allowed: list[str], version: int, reason: str
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path, version=version)
    calls: list[object] = []
    _register(lambda context: calls.append(context))
    runner = DurableActionRunner(
        DurableActionStore(db_path), enabled=True, allowed_actions=allowed
    )

    assert asyncio.run(runner.process_once()) is True
    row = _ticket(db_path, request_id)
    assert (row["status"], row["outcome_reason"]) == (
        "handler_unavailable",
        reason,
    )
    assert row["execution_started_at"] is None
    assert row["notification_status"] == "pending"
    assert calls == []


def test_runner_persists_start_before_handler_and_holds_no_sqlite_lock(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)

    def execute(context):
        assert _ticket(db_path, context.request_id)["status"] == "executing"
        with sqlite3.connect(db_path, timeout=0.1) as conn:
            conn.execute("CREATE TABLE handler_probe (value TEXT)")
            conn.execute("INSERT INTO handler_probe VALUES ('written')")
        return DurableActionOutcome(DurableActionStatus.SUCCEEDED)

    _register(execute)
    runner = DurableActionRunner(
        DurableActionStore(db_path), enabled=True, allowed_actions=["test.execute"]
    )

    assert asyncio.run(runner.process_once()) is True
    assert _ticket(db_path, request_id)["status"] == "succeeded"
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT value FROM handler_probe").fetchone() == ("written",)


@pytest.mark.asyncio
async def test_live_handler_heartbeat_prevents_stale_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    now = [1_000.0]
    store = DurableActionStore(db_path, wall_clock=lambda: now[0])
    started = threading.Event()
    release = threading.Event()
    renewed = threading.Event()

    def execute(_context):
        started.set()
        assert release.wait(5)
        return DurableActionOutcome(DurableActionStatus.SUCCEEDED)

    original_renew = store.renew_execution_claim

    def renew(request_id: str, token: str) -> bool:
        result = original_renew(request_id, token)
        renewed.set()
        return result

    monkeypatch.setattr(store, "renew_execution_claim", renew)
    _register(execute)
    runner = DurableActionRunner(
        store,
        enabled=True,
        allowed_actions=["test.execute"],
        stale_claim_seconds=0.03,
    )
    task = asyncio.create_task(runner.process_once())
    assert await asyncio.to_thread(started.wait, 5)
    now[0] = 1_400.0
    assert await asyncio.to_thread(renewed.wait, 5)

    recovery = DurableActionStore(db_path, wall_clock=lambda: now[0])
    assert await asyncio.to_thread(recovery.recover_stale, 0.03) == (0, 0)
    assert _ticket(db_path, request_id)["status"] == "executing"
    release.set()
    assert await task is True
    assert _ticket(db_path, request_id)["status"] == "succeeded"


@pytest.mark.asyncio
async def test_heartbeat_error_does_not_orphan_live_handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    started = threading.Event()
    release = threading.Event()
    renewal_attempted = threading.Event()

    def execute(_context):
        started.set()
        assert release.wait(5)
        return DurableActionOutcome(DurableActionStatus.SUCCEEDED)

    store = DurableActionStore(db_path)

    def fail_renewal(_request_id: str, _token: str) -> bool:
        renewal_attempted.set()
        raise sqlite3.OperationalError("private database detail")

    monkeypatch.setattr(store, "renew_execution_claim", fail_renewal)
    _register(execute)
    runner = DurableActionRunner(
        store,
        enabled=True,
        allowed_actions=["test.execute"],
        stale_claim_seconds=0.03,
    )
    task = asyncio.create_task(runner.process_once())
    assert await asyncio.to_thread(started.wait, 5)
    assert await asyncio.to_thread(renewal_attempted.wait, 5)
    assert not task.done()
    release.set()

    assert await task is True
    assert _ticket(db_path, request_id)["status"] == "succeeded"


@pytest.mark.asyncio
async def test_cancellation_drains_handler_and_persists_outcome(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    started = threading.Event()
    release = threading.Event()

    def execute(_context):
        started.set()
        assert release.wait(5)
        return DurableActionOutcome(DurableActionStatus.SUCCEEDED)

    _register(execute)
    runner = DurableActionRunner(
        DurableActionStore(db_path),
        enabled=True,
        allowed_actions=["test.execute"],
        stale_claim_seconds=0.03,
    )
    task = asyncio.create_task(runner.process_once())
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0.02)
    task.cancel()
    await asyncio.sleep(0.03)
    was_pending = not task.done()
    release.set()
    result = await asyncio.gather(task, return_exceptions=True)

    assert was_pending is True
    assert isinstance(result[0], asyncio.CancelledError)
    assert _ticket(db_path, request_id)["status"] == "succeeded"


@pytest.mark.asyncio
async def test_cancellation_during_start_still_owns_and_completes_handler(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    store = DurableActionStore(db_path)
    start_entered = threading.Event()
    release_start = threading.Event()
    handler_called = threading.Event()
    calls = 0
    original_start = store.mark_executing

    def blocked_start(request_id: str, token: str) -> bool:
        start_entered.set()
        assert release_start.wait(5)
        return original_start(request_id, token)

    def execute(_context):
        nonlocal calls
        calls += 1
        handler_called.set()
        return DurableActionOutcome(DurableActionStatus.SUCCEEDED)

    monkeypatch.setattr(store, "mark_executing", blocked_start)
    _register(execute)
    runner = DurableActionRunner(
        store,
        enabled=True,
        allowed_actions=["test.execute"],
    )
    task = asyncio.create_task(runner.process_once())
    assert await asyncio.to_thread(start_entered.wait, 5)
    task.cancel()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release_start.set()
    assert await asyncio.to_thread(handler_called.wait, 5)
    result = await asyncio.gather(task, return_exceptions=True)

    assert isinstance(result[0], asyncio.CancelledError)
    assert calls == 1
    assert _ticket(db_path, request_id)["status"] == "succeeded"


@pytest.mark.asyncio
async def test_store_contention_does_not_block_event_loop(tmp_path: Path) -> None:
    store = DurableActionStore(tmp_path / "state.db")
    release = threading.Event()

    def blocked_recovery(_interval: float):
        assert release.wait(5)
        return (0, 0)

    store.recover_stale = blocked_recovery  # type: ignore[method-assign]
    runner = DurableActionRunner(store, enabled=True)
    timer = threading.Timer(0.3, release.set)
    timer.start()
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    task = asyncio.create_task(runner.process_once())
    await asyncio.sleep(0)
    elapsed = loop.time() - started_at
    release.set()
    await task
    timer.cancel()

    assert elapsed < 0.1


@pytest.mark.parametrize(
    ("execute", "reason"),
    [
        (
            lambda _context: (_ for _ in ()).throw(RuntimeError("private-secret")),
            "handler_exception",
        ),
        (
            lambda _context: {
                "status": "succeeded",
                "secret": "private-secret",
            },
            "invalid_handler_outcome",
        ),
    ],
)
def test_runner_converts_ambiguous_handler_results_to_bounded_unknown(
    tmp_path: Path, execute, reason: str
) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    _register(execute)
    runner = DurableActionRunner(
        DurableActionStore(db_path), enabled=True, allowed_actions=["test.execute"]
    )

    assert asyncio.run(runner.process_once()) is True
    row = _ticket(db_path, request_id)
    assert (row["status"], row["outcome_reason"], row["outcome_json"]) == (
        "unknown",
        reason,
        None,
    )
    assert "private-secret" not in repr(dict(row))


def test_repeated_process_once_executes_ticket_once(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    request_id = _ready_ticket(db_path)
    calls = 0

    def execute(_context):
        nonlocal calls
        calls += 1
        return DurableActionOutcome(
            DurableActionStatus.SUCCEEDED,
            result_json=durable_actions.encode_durable_action_result({"value": 7}),
        )

    _register(execute)
    runner = DurableActionRunner(
        DurableActionStore(db_path), enabled=True, allowed_actions=["test.execute"]
    )

    assert asyncio.run(runner.process_once()) is True
    assert asyncio.run(runner.process_once()) is False
    assert calls == 1
    row = _ticket(db_path, request_id)
    assert row["status"] == "succeeded"
    assert durable_actions.decode_durable_action_result(row["outcome_json"]) == {
        "value": 7
    }
