from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from gateway.durable_actions import PreparedDurableAction
from gateway.durable_operations import (
    DurableActionReconciliationOutcome,
    DurableOperationsStore,
)
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)


def _create_action(store: DurableRequestStore, reference: str, *, action_type="cron.resume"):
    return store.create_with_action(
        DurableRequestSpec(
            kind="approval",
            title="title",
            body="body",
            options=(DurableRequestOption("approve", "yes"), DurableRequestOption("reject", "no")),
            ttl_seconds=1_000,
            reference=reference,
        ),
        action=PreparedDurableAction(
            action_type=action_type,
            action_version=1,
            payload_json='{"job_id":"secret"}',
            fingerprint=f"fingerprint-{reference}",
            idempotency_key=f"key-{reference}",
        ),
        target=DurableRequestTarget("discord", "channel"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id=f"origin-{reference}",
        origin_session_id=f"session-{reference}",
    )


def _unknown_db(tmp_path: Path) -> tuple[Path, str, str]:
    db_path = tmp_path / "state.db"
    store = DurableRequestStore(db_path, wall_clock=lambda: 100.0)
    unknown = _create_action(store, "unknown").request.request_id
    successor = _create_action(store, "successor").request.request_id
    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE durable_requests SET status = 'selected', resolved_at = 100"
        )
        db.execute(
            """UPDATE durable_action_tickets
               SET status = 'unknown', outcome_reason = 'execution_interrupted',
                   completed_at = 100, notification_status = 'delivered', updated_at = 100
               WHERE request_id = ?""",
            (unknown,),
        )
    return db_path, unknown, successor


def test_phase4c_database_upgrades_without_reconciliation_rows(tmp_path: Path) -> None:
    db_path, _, _ = _unknown_db(tmp_path)

    DurableOperationsStore(db_path)
    DurableOperationsStore(db_path)

    with sqlite3.connect(db_path) as db:
        assert db.execute(
            "SELECT count(*) FROM durable_action_reconciliations"
        ).fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM durable_action_tickets").fetchone()[0] == 2


@pytest.mark.parametrize(
    "outcome",
    [
        "confirmed_applied",
        "confirmed_not_applied",
        "abandoned",
    ],
)
def test_unknown_action_reconciles_once(tmp_path: Path, outcome: str) -> None:
    db_path, request_id, _ = _unknown_db(tmp_path)
    ops = DurableOperationsStore(db_path, wall_clock=lambda: 500.0)

    result = ops.reconcile_action(
        request_id,
        outcome=outcome,
        operator_identity="operator-☃",
        note="checked safely",
    )
    detail = ops.get_action(request_id, now=500.0)

    assert result.code == "reconciled"
    assert detail is not None
    assert detail.status == "unknown"
    assert detail.effective_reconciliation == outcome
    with sqlite3.connect(db_path) as db:
        row = db.execute(
            "SELECT outcome, operator_identity, note, created_at "
            "FROM durable_action_reconciliations WHERE request_id = ?",
            (request_id,),
        ).fetchone()
    assert row == (outcome, "operator-☃", "checked safely", 500.0)


def test_superseded_requires_different_same_type_receipt(tmp_path: Path) -> None:
    db_path, request_id, successor = _unknown_db(tmp_path)
    ops = DurableOperationsStore(db_path)

    wrong = ops.reconcile_action(
        request_id,
        outcome="superseded",
        operator_identity="operator",
        superseded_by_request_id=request_id,
    )
    missing = ops.reconcile_action(
        request_id,
        outcome="superseded",
        operator_identity="operator",
        superseded_by_request_id="drq_missing",
    )
    success = ops.reconcile_action(
        request_id,
        outcome="superseded",
        operator_identity="operator",
        superseded_by_request_id=successor,
    )

    assert wrong.code == "invalid_superseding_receipt"
    assert missing.code == "invalid_superseding_receipt"
    assert success.code == "reconciled"


def test_duplicate_and_non_unknown_never_change_first_row(tmp_path: Path) -> None:
    db_path, request_id, successor = _unknown_db(tmp_path)
    ops = DurableOperationsStore(db_path, wall_clock=lambda: 500.0)
    first = ops.reconcile_action(
        request_id, outcome="abandoned", operator_identity="first", note="first"
    )
    duplicate = ops.reconcile_action(
        request_id,
        outcome="superseded",
        operator_identity="second",
        superseded_by_request_id=successor,
    )
    non_unknown = ops.reconcile_action(
        successor, outcome="abandoned", operator_identity="operator"
    )

    assert first.code == "reconciled"
    assert duplicate.code == "already_reconciled"
    assert non_unknown.code == "invalid_state"
    with sqlite3.connect(db_path) as db:
        assert db.execute(
            "SELECT outcome, operator_identity, note FROM durable_action_reconciliations "
            "WHERE request_id = ?",
            (request_id,),
        ).fetchone() == ("abandoned", "first", "first")


@pytest.mark.parametrize(
    "identity,note",
    [
        ("", None),
        ("x" * 129, None),
        ("operator", "x" * 1025),
        ("bad\ud800", None),
        ("operator", "bad\ud800"),
    ],
)
def test_reconciliation_rejects_invalid_identity_and_note(
    tmp_path: Path, identity: str, note: str | None
) -> None:
    db_path, request_id, _ = _unknown_db(tmp_path)

    with pytest.raises(ValueError):
        DurableOperationsStore(db_path).reconcile_action(
            request_id,
            outcome=DurableActionReconciliationOutcome.ABANDONED,
            operator_identity=identity,
            note=note,
        )


def test_concurrent_reconciliation_has_one_immutable_winner(tmp_path: Path) -> None:
    db_path, request_id, _ = _unknown_db(tmp_path)
    barrier = threading.Barrier(2)
    codes: list[str] = []

    def reconcile(identity: str) -> None:
        barrier.wait()
        result = DurableOperationsStore(db_path).reconcile_action(
            request_id, outcome="abandoned", operator_identity=identity
        )
        codes.append(result.code)

    threads = [threading.Thread(target=reconcile, args=(name,)) for name in ("one", "two")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(codes) == ["already_reconciled", "reconciled"]
    with sqlite3.connect(db_path) as db:
        assert db.execute(
            "SELECT count(*) FROM durable_action_reconciliations WHERE request_id = ?",
            (request_id,),
        ).fetchone()[0] == 1


def test_pruning_keeps_unreconciled_unknown_and_removes_linked_evidence_together(
    tmp_path: Path,
) -> None:
    db_path, request_id, _ = _unknown_db(tmp_path)
    store = DurableRequestStore(db_path, wall_clock=lambda: 10_000.0)

    assert store.prune_terminal(10.0) == 0
    DurableOperationsStore(db_path).reconcile_action(
        request_id, outcome="abandoned", operator_identity="operator", now=200.0
    )
    assert store.prune_terminal(10.0) >= 1
    with sqlite3.connect(db_path) as db:
        assert db.execute(
            "SELECT count(*) FROM durable_action_reconciliations WHERE request_id = ?",
            (request_id,),
        ).fetchone()[0] == 0
