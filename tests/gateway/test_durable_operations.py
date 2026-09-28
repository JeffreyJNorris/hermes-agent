from __future__ import annotations

import sqlite3
import threading
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from gateway.durable_actions import DurableActionStatus, DurableActionStore, PreparedDurableAction
from gateway.durable_operations import DurableOperationsStore
from gateway.durable_requests import (
    DurableRequestDeliveryMode,
    DurableRequestOption,
    DurableRequestResolutionSource,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)


def _spec(reference: str) -> DurableRequestSpec:
    return DurableRequestSpec(
        kind="test.release",
        title="PHASE4D-CANARY title",
        body="PHASE4D-CANARY body",
        options=(
            DurableRequestOption("approve", "PHASE4D-CANARY approve"),
            DurableRequestOption("reject", "PHASE4D-CANARY reject"),
        ),
        ttl_seconds=1_000,
        reference=reference,
    )


def _create_request(store: DurableRequestStore, reference: str, *, action=False):
    kwargs = dict(
        target=DurableRequestTarget("discord", "channel-secret", "thread-secret"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id=f"origin-secret-{reference}",
    )
    if action:
        return store.create_with_action(
            _spec(reference),
            action=PreparedDurableAction(
                action_type="cron.resume",
                action_version=1,
                payload_json='{"canary":"PHASE4D-CANARY"}',
                fingerprint=f"fingerprint-secret-{reference}",
                idempotency_key=f"idempotency-secret-{reference}",
            ),
            origin_session_id=f"session-secret-{reference}",
            **kwargs,
        )
    return store.create(_spec(reference), **kwargs)


def seeded_operations_store(tmp_path: Path) -> DurableOperationsStore:
    db_path = tmp_path / "state.db"
    requests = DurableRequestStore(db_path, wall_clock=lambda: 100.0)
    older = _create_request(requests, "older").request.request_id
    newer = _create_request(requests, "newer").request.request_id
    action = _create_request(requests, "action", action=True).request.request_id
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_requests SET request_id = 'drq_older', created_at = 100, "
            "updated_at = 100 WHERE request_id = ?",
            (older,),
        )
        db.execute(
            "UPDATE durable_requests SET request_id = 'drq_newer', created_at = 200, "
            "updated_at = 200 WHERE request_id = ?",
            (newer,),
        )
        db.execute(
            "UPDATE durable_requests SET request_id = 'drq_action', created_at = 150, "
            "updated_at = 150 WHERE request_id = ?",
            (action,),
        )
        db.execute(
            "UPDATE durable_action_tickets SET request_id = 'drq_action', "
            "created_at = 150, updated_at = 150 WHERE request_id = ?",
            (action,),
        )
    return DurableOperationsStore(db_path, wall_clock=lambda: 500.0)


def test_operator_store_does_not_create_partial_schema_for_missing_database(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"

    DurableOperationsStore(db_path)

    assert not db_path.exists()


def test_request_list_is_bounded_stable_and_redacted(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)

    page = store.list_requests(limit=1, offset=0, now=500.0)

    assert tuple(item.request_id for item in page.items) == ("drq_newer",)
    assert page.next_offset == 1
    for name in (
        "title",
        "body",
        "options",
        "chat_id",
        "thread_id",
        "origin_id",
        "expected_actor_id",
    ):
        assert not hasattr(page.items[0], name)
    with pytest.raises(FrozenInstanceError):
        page.items[0].status = "cancelled"  # type: ignore[misc]


def test_request_projection_exposes_only_structural_command_fields(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            "UPDATE durable_requests SET delivery_mode = ?, resolution_source = ? "
            "WHERE request_id = 'drq_newer'",
            (
                DurableRequestDeliveryMode.TEXT_COMMAND.value,
                DurableRequestResolutionSource.COMMAND.value,
            ),
        )

    summary = store.list_requests(limit=1, now=500.0).items[0]
    detail = store.get_request("drq_newer", now=500.0)

    assert summary.delivery_mode == "text_command"
    assert summary.resolution_source == "command"
    assert detail is not None
    assert detail.delivery_mode == "text_command"
    assert detail.resolution_source == "command"
    for name in ("chat_id", "thread_id", "message_id", "expected_actor_id"):
        assert not hasattr(detail, name)


def test_request_list_filters_and_equal_timestamps_are_stable(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute("UPDATE durable_requests SET created_at = 200")
        db.execute(
            "UPDATE durable_requests SET status = 'open', message_id = 'secret' "
            "WHERE request_id = 'drq_newer'"
        )

    page = store.list_requests(
        status="pending_delivery", origin_type="session", now=500.0
    )

    assert tuple(item.request_id for item in page.items) == (
        "drq_older",
        "drq_action",
    )
    assert page.next_offset is None


def test_request_projection_uses_one_clock_for_age_fields(tmp_path: Path) -> None:
    calls = 0

    def clock() -> float:
        nonlocal calls
        calls += 1
        return 500.0

    store = seeded_operations_store(tmp_path)
    store = DurableOperationsStore(tmp_path / "state.db", wall_clock=clock)

    page = store.list_requests()

    assert calls == 1
    assert {item.pending_age_seconds for item in page.items} == {300.0, 350.0, 400.0}


@pytest.mark.parametrize("limit", [0, 101, True])
def test_request_list_rejects_invalid_limits(tmp_path: Path, limit: object) -> None:
    store = seeded_operations_store(tmp_path)

    with pytest.raises((TypeError, ValueError)):
        store.list_requests(limit=limit)  # type: ignore[arg-type]


def test_request_list_rejects_negative_offset(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="offset"):
        seeded_operations_store(tmp_path).list_requests(offset=-1)


def test_request_lookup_returns_none_for_unknown_receipt(tmp_path: Path) -> None:
    assert seeded_operations_store(tmp_path).get_request("drq_missing") is None


def test_action_lookup_by_request_omits_execution_material(tmp_path: Path) -> None:
    detail = seeded_operations_store(tmp_path).get_action("drq_action", now=500.0)

    assert detail is not None
    assert detail.action_type == "cron.resume"
    assert detail.request_status == "pending_delivery"
    for name in (
        "payload_json",
        "fingerprint",
        "idempotency_key",
        "execution_claim",
        "notification_claim",
        "outcome_json",
    ):
        assert not hasattr(detail, name)


def test_action_list_filters_status_type_and_minimum_age(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)

    matching = store.list_actions(
        status="waiting_for_decision",
        action_type="cron.resume",
        minimum_age_seconds=300,
        now=500.0,
    )
    too_old = store.list_actions(minimum_age_seconds=351, now=500.0)

    assert tuple(item.request_id for item in matching.items) == ("drq_action",)
    assert too_old.items == ()


def test_ready_action_age_starts_at_ready_transition(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            "UPDATE durable_action_tickets SET status = 'ready', updated_at = 490 "
            "WHERE request_id = 'drq_action'"
        )

    page = store.list_actions(
        status="ready", minimum_age_seconds=20, now=500.0
    )
    detail = store.get_action("drq_action", now=500.0)

    assert page.items == ()
    assert detail is not None and detail.actionable_age_seconds == 10.0


def test_health_warns_for_stale_executing_claim(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            "UPDATE durable_action_tickets SET status = 'executing', "
            "execution_claim = 'claim', execution_claimed_at = 100 "
            "WHERE request_id = 'drq_action'"
        )

    health = store.check_health(
        actions_enabled=True, action_worker_alive=True, now=500.0
    )

    assert health.severity == "warning"
    assert dict(health.counts)["stale_action_claims"] == 1


@pytest.mark.parametrize("existing_database", [False, True])
def test_health_checks_enabled_worker_before_ledger_initialization(
    tmp_path: Path,
    existing_database: bool,
) -> None:
    db_path = tmp_path / "state.db"
    if existing_database:
        with sqlite3.connect(db_path) as db:
            db.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
    health = DurableOperationsStore(db_path, reconcile_schema=False).check_health(
        requests_enabled=True, request_worker_alive=False
    )

    assert health.severity == "warning"
    assert health.code == "worker_unavailable"
    assert health.counts == (("workers", 1),)


def test_list_filters_reject_status_injection_and_treat_wildcards_literally(
    tmp_path: Path,
) -> None:
    store = seeded_operations_store(tmp_path)

    with pytest.raises(ValueError, match="status"):
        store.list_requests(status="open' OR 1=1 --")
    with pytest.raises(ValueError, match="status"):
        store.list_actions(status="ready' OR 1=1 --")
    assert store.list_requests(origin_type="%").items == ()
    assert store.list_actions(action_type="cron.%").items == ()
    with pytest.raises(TypeError, match="origin_type"):
        store.list_requests(origin_type=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="action_type"):
        store.list_actions(action_type=1)  # type: ignore[arg-type]


def test_phase4_rows_without_action_tickets_remain_inspectable(tmp_path: Path) -> None:
    store = seeded_operations_store(tmp_path)

    request = store.get_request("drq_older", now=500.0)

    assert request is not None
    assert request.action_status is None
    assert store.get_action("drq_older", now=500.0) is None


def _set_action_state(
    tmp_path: Path,
    status: str,
    *,
    request_status: str = "selected",
    execution_claim: str | None = None,
) -> tuple[DurableOperationsStore, DurableActionStore]:
    ops = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            "UPDATE durable_requests SET status = ?, resolved_at = 250 "
            "WHERE request_id = 'drq_action'",
            (request_status,),
        )
        db.execute(
            "UPDATE durable_action_tickets SET status = ?, execution_claim = ?, "
            "execution_claimed_at = CASE WHEN ? IS NULL THEN NULL ELSE 250 END "
            "WHERE request_id = 'drq_action'",
            (status, execution_claim, execution_claim),
        )
    return ops, DurableActionStore(tmp_path / "state.db", wall_clock=lambda: 500.0)


def test_cancel_open_generic_request(tmp_path: Path) -> None:
    ops = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            "UPDATE durable_requests SET status = 'open' WHERE request_id = 'drq_newer'"
        )

    result = ops.cancel_request("drq_newer", now=500.0)
    detail = ops.get_request("drq_newer", now=500.0)

    assert result.code == "cancelled"
    assert result.request_status == "cancelled"
    assert result.action_status is None
    assert detail is not None
    assert detail.status == "cancelled"
    assert detail.resolved_at == 500.0


def test_cancel_waiting_action_updates_request_and_ticket_atomically(tmp_path: Path) -> None:
    ops = seeded_operations_store(tmp_path)

    result = ops.cancel_action("drq_action", now=500.0)
    request = ops.get_request("drq_action", now=500.0)
    action = ops.get_action("drq_action", now=500.0)

    assert result.code == "cancelled"
    assert request is not None and request.status == "cancelled"
    assert action is not None and action.status == "cancelled"
    assert action.notification_status == "pending"


def test_cancel_ready_action_is_never_runnable(tmp_path: Path) -> None:
    ops, runner = _set_action_state(tmp_path, "ready")

    result = ops.cancel_action("drq_action", now=500.0)

    assert result.code == "cancelled"
    assert runner.claim_next() is None
    action = ops.get_action("drq_action", now=500.0)
    assert action is not None
    assert action.notification_status == "pending"


def test_cancel_ready_action_loses_to_runner_claim(tmp_path: Path) -> None:
    ops, runner = _set_action_state(tmp_path, "ready")
    claim = runner.claim_next()

    result = ops.cancel_action("drq_action", now=500.0)

    assert claim is not None
    assert result.code == "execution_in_progress"
    assert ops.get_action("drq_action").status == DurableActionStatus.CLAIMED.value  # type: ignore[union-attr]


def test_two_operator_cancels_racing_runner_have_one_safe_winner(
    tmp_path: Path,
) -> None:
    _, runner = _set_action_state(tmp_path, "ready")
    operators = [
        DurableOperationsStore(tmp_path / "state.db") for _ in range(2)
    ]
    barrier = threading.Barrier(3)
    cancel_codes: list[str] = []
    claims = []

    def cancel(store: DurableOperationsStore) -> None:
        barrier.wait()
        cancel_codes.append(store.cancel_action("drq_action").code)

    def claim() -> None:
        barrier.wait()
        claims.append(runner.claim_next())

    threads = [
        threading.Thread(target=cancel, args=(store,)) for store in operators
    ] + [threading.Thread(target=claim)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if claims == [None]:
        assert sorted(cancel_codes) == ["already_terminal", "cancelled"]
    else:
        assert claims[0] is not None
        assert cancel_codes == ["execution_in_progress"] * 2


@pytest.mark.parametrize("status", ["claimed", "executing"])
def test_cancel_in_progress_action_is_rejected(tmp_path: Path, status: str) -> None:
    ops, _ = _set_action_state(tmp_path, status, execution_claim="claim-secret")

    result = ops.cancel_action("drq_action", now=500.0)

    assert result.code == "execution_in_progress"
    assert result.ok is False


@pytest.mark.parametrize(
    "status",
    [
        "succeeded",
        "already_applied",
        "declined",
        "expired",
        "cancelled",
        "abandoned",
        "invalidated",
        "failed",
        "unknown",
        "handler_unavailable",
    ],
)
def test_cancel_terminal_action_is_rejected(tmp_path: Path, status: str) -> None:
    ops, _ = _set_action_state(tmp_path, status)

    result = ops.cancel_action("drq_action", now=500.0)

    assert result.code == "already_terminal"
    assert result.action_status == status


def test_cancel_unknown_receipt_is_not_found(tmp_path: Path) -> None:
    result = seeded_operations_store(tmp_path).cancel_request("drq_missing")

    assert result.code == "not_found"
    assert result.ok is False


def test_cancel_rolls_back_request_if_action_compare_and_set_fails(tmp_path: Path) -> None:
    ops = seeded_operations_store(tmp_path)
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            """CREATE TRIGGER reject_action_cancel
               BEFORE UPDATE OF status ON durable_action_tickets
               WHEN NEW.status = 'cancelled'
               BEGIN SELECT RAISE(ABORT, 'test conflict'); END"""
        )

    with pytest.raises(sqlite3.IntegrityError):
        ops.cancel_action("drq_action", now=500.0)

    assert ops.get_request("drq_action").status == "pending_delivery"  # type: ignore[union-attr]
