"""Behavioral coverage for durable approval of staged memory writes."""

from __future__ import annotations

import json
import os

import pytest


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_SESSION_KEY", "discord:123:thread")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "456")
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")
    monkeypatch.setenv("HERMES_SESSION_ID", "session-1")
    from gateway.durable_requests import reset_durable_request_store_cache

    reset_durable_request_store_cache()
    yield home
    reset_durable_request_store_cache()


def _configure(*, allowed=True, inbox=True):
    from hermes_cli.config import load_config, save_config

    config = load_config()
    config["memory"].update(
        write_approval=True,
        durable_approval=True,
        memory_char_limit=10000,
    )
    config["durable_approvals"] = {
        "enabled": True,
        "allowed_actions": ["memory.apply_pending"] if allowed else [],
    }
    config["durable_requests"].update(
        enabled=True,
        inbox={
            "platform": "discord",
            "chat_id": "123" if inbox else "",
            "thread_id": None,
        },
    )
    save_config(config)


def _stage(content="exact reviewed fact"):
    from tools import write_approval as wa

    return wa.stage_write(
        wa.MEMORY,
        {"action": "add", "target": "memory", "content": content, "old_text": None},
        summary="add to memory",
        origin="background_review",
    )


def test_versioned_record_is_private_fingerprinted_and_ids_are_strict(hermes_home):
    from hermes_cli.config_defaults import DEFAULT_CONFIG
    from tools import write_approval as wa

    assert DEFAULT_CONFIG["memory"]["durable_approval"] is False
    record = _stage("snowman ☃\nsecond line")
    assert record["record_version"] == 2
    assert record["fingerprint_version"] == 1
    assert len(record["baseline_digest"]) == 64
    assert len(record["desired_digest"]) == 64
    assert record["subject_fingerprint"] == wa.memory_pending_fingerprint(record)
    assert wa.get_pending(wa.MEMORY, "../config") is None
    path = hermes_home / "pending" / "memory" / f"{record['id']}.json"
    assert path.stat().st_mode & 0o077 == 0
    assert path.parent.stat().st_mode & 0o077 == 0

    tampered = {**record, "payload": {**record["payload"], "content": "changed"}}
    assert wa.memory_pending_fingerprint(tampered) != record["subject_fingerprint"]


@pytest.mark.parametrize(
    ("allowed", "inbox", "reason"),
    [(False, True, "handler_not_allowed"), (True, False, "inbox_unavailable")],
)
def test_unavailable_durable_path_stays_manual(hermes_home, allowed, inbox, reason):
    from tools import write_approval as wa

    _configure(allowed=allowed, inbox=inbox)
    result = wa.create_memory_durable_approval(_stage())
    assert result == {"status": "pending_manual_review", "reason": reason}
    assert wa.pending_count(wa.MEMORY) == 1


def test_complete_review_creates_one_idempotent_request_with_bounded_payload(hermes_home):
    import sqlite3
    from gateway.durable_actions import decode_durable_action_payload
    from tools import write_approval as wa

    _configure()
    record = _stage()
    first = wa.create_memory_durable_approval(record)
    second = wa.create_memory_durable_approval(record)
    assert first == second
    assert first["status"] == "awaiting_approval", first
    with sqlite3.connect(hermes_home / "state.db") as conn:
        payload_json, fingerprint = conn.execute(
            "SELECT payload_json, fingerprint FROM durable_action_tickets WHERE request_id = ?",
            (first["request_id"],),
        ).fetchone()
    assert decode_durable_action_payload(payload_json) == {"pending_id": record["id"]}
    assert fingerprint == record["subject_fingerprint"]
    assert "exact reviewed fact" not in payload_json
    linked = wa.get_pending(wa.MEMORY, record["id"])
    assert linked["durable_request_id"] == first["request_id"]


def test_oversize_review_falls_back_without_creating_action(hermes_home):
    import sqlite3
    from tools import write_approval as wa

    _configure()
    record = _stage("x" * 5000)
    result = wa.create_memory_durable_approval(record)
    assert result == {"status": "pending_manual_review", "reason": "review_too_large"}
    with sqlite3.connect(hermes_home / "state.db") as conn:
        assert conn.execute("SELECT count(*) FROM durable_action_tickets").fetchone()[0] == 0


def _context(record):
    from gateway.durable_actions import DurableActionExecutionContext, encode_durable_action_payload

    return DurableActionExecutionContext(
        request_id="drq_abcdefghijklmnopqrstuvwxyz",
        action_type="memory.apply_pending",
        action_version=1,
        payload_json=encode_durable_action_payload({"pending_id": record["id"]}),
        fingerprint=record["subject_fingerprint"],
        idempotency_key=f"memory.apply_pending:{record['id']}:{record['subject_fingerprint']}",
    )


def test_handler_applies_exact_change_once_and_detects_already_applied(hermes_home):
    from gateway.durable_actions import DurableActionStatus
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    _configure()
    record = _stage()
    first = wa.execute_durable_memory_pending(_context(record))
    assert first.status is DurableActionStatus.SUCCEEDED
    assert wa.list_pending(wa.MEMORY) == []
    assert wa.get_pending(wa.MEMORY, record["id"])["status"] == "applied"
    store = MemoryStore()
    store.load_from_disk()
    assert store.memory_entries == ["exact reviewed fact"]

    # A stale delivery of the same trusted context cannot duplicate the write.
    assert wa.execute_durable_memory_pending(_context(record)).status is DurableActionStatus.ALREADY_APPLIED
    store.load_from_disk()
    assert store.memory_entries == ["exact reviewed fact"]


def test_handler_invalidates_tamper_and_changed_baseline(hermes_home):
    from gateway.durable_actions import DurableActionStatus
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore
    from utils import atomic_json_write

    _configure()
    tampered = _stage("private canary")
    path = hermes_home / "pending" / "memory" / f"{tampered['id']}.json"
    changed = {**tampered, "payload": {**tampered["payload"], "content": "evil"}}
    atomic_json_write(path, changed, mode=0o600)
    assert wa.execute_durable_memory_pending(_context(tampered)).status is DurableActionStatus.INVALIDATED

    baseline = _stage("approved later")
    store = MemoryStore(); store.load_from_disk()
    assert store.add("memory", "intervening write")["success"]
    assert wa.execute_durable_memory_pending(_context(baseline)).status is DurableActionStatus.INVALIDATED
    store.load_from_disk()
    assert "approved later" not in store.memory_entries


def test_handler_is_statically_registered_and_legacy_records_remain_manual(hermes_home):
    from gateway.durable_actions import lookup_durable_action_handler
    from tools import write_approval as wa

    assert lookup_durable_action_handler("memory.apply_pending", 1) is wa.execute_durable_memory_pending
    legacy = {
        "id": "deadbeef",
        "subsystem": "memory",
        "action": "add",
        "summary": "legacy",
        "origin": "foreground",
        "created_at": 1.0,
        "payload": {"action": "add", "target": "memory", "content": "legacy"},
    }
    assert wa.prepare_durable_memory_pending(legacy) is None


def test_memory_tool_preserves_immediate_manual_and_durable_matrix(hermes_home):
    from hermes_cli.config import load_config, save_config
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore, memory_tool

    config = load_config()
    config["memory"].update(write_approval=False, durable_approval=True)
    save_config(config)
    store = MemoryStore(); store.load_from_disk()
    immediate = json.loads(memory_tool("add", "memory", "immediate", store=store))
    assert immediate["success"] is True and immediate.get("staged") is None

    config = load_config()
    config["memory"].update(write_approval=True, durable_approval=False)
    save_config(config)
    manual = json.loads(memory_tool("add", "user", "manual", store=store))
    assert "review with /memory pending" in manual["message"]

    _configure()
    durable = json.loads(memory_tool("add", "user", "durable canary", store=store))
    assert set(durable) == {
        "success", "staged", "status", "pending_id", "request_id", "action_id"
    }
    assert durable["status"] == "awaiting_approval"
    assert "durable canary" not in json.dumps(durable)


def test_review_body_is_complete_and_not_truncated(hermes_home):
    from gateway.durable_requests import get_durable_request_store
    from tools import write_approval as wa

    _configure()
    content = "first line\nlast line"
    result = wa.create_memory_durable_approval(_stage(content))
    request = get_durable_request_store().get(request_id=result["request_id"])
    assert json.loads(request.body)["operations"][0]["content"] == content
    assert "..." not in request.body


def test_linked_manual_approve_authorizes_same_handler_and_reject_cancels(hermes_home):
    import asyncio
    from gateway.durable_action_runner import DurableActionRunner
    from gateway.durable_actions import DurableActionStatus, DurableActionStore
    from gateway.durable_operations import DurableOperationsStore
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    _configure()
    approved = _stage("manual durable approval")
    receipt = wa.create_memory_durable_approval(approved)
    out = handle_pending_subcommand(wa.MEMORY, ["approve", approved["id"]])
    assert "Authorized 1" in out
    operations = DurableOperationsStore(hermes_home / "state.db")
    assert operations.get_action(receipt["request_id"]).status == DurableActionStatus.READY.value
    assert wa.get_pending(wa.MEMORY, approved["id"]) is not None
    runner = DurableActionRunner(
        DurableActionStore(hermes_home / "state.db"),
        enabled=True,
        allowed_actions={"memory.apply_pending"},
    )
    assert asyncio.run(runner.process_once()) is True
    store = MemoryStore(); store.load_from_disk()
    assert store.memory_entries == ["manual durable approval"]

    rejected = _stage("must never apply")
    rejected_receipt = wa.create_memory_durable_approval(rejected)
    out = handle_pending_subcommand(wa.MEMORY, ["reject", rejected["id"]])
    assert "Rejected 1" in out
    assert operations.get_action(rejected_receipt["request_id"]).status == DurableActionStatus.CANCELLED.value
    assert wa.get_pending(wa.MEMORY, rejected["id"]) is None
    store.load_from_disk()
    assert "must never apply" not in store.memory_entries


def test_expired_linked_request_applies_nothing(hermes_home):
    import sqlite3
    from gateway.durable_requests import get_durable_request_store
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    _configure()
    record = _stage("expired secret")
    receipt = wa.create_memory_durable_approval(record)
    with sqlite3.connect(hermes_home / "state.db") as conn:
        conn.execute(
            "UPDATE durable_requests SET expires_at = 0 WHERE request_id = ?",
            (receipt["request_id"],),
        )
    assert get_durable_request_store().expire_due() == 1
    store = MemoryStore(); store.load_from_disk()
    assert "expired secret" not in store.memory_entries


def test_linked_manual_approve_reports_execution_conflict(hermes_home):
    from gateway.durable_actions import DurableActionStore
    from gateway.durable_operations import DurableOperationsStore
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa

    _configure()
    record = _stage("one execution rail")
    receipt = wa.create_memory_durable_approval(record)
    operations = DurableOperationsStore(hermes_home / "state.db")
    assert operations.authorize_action(receipt["request_id"]).ok
    actions = DurableActionStore(hermes_home / "state.db")
    claim = actions.claim_next()
    assert claim is not None
    assert actions.mark_executing(receipt["request_id"], claim.token)

    out = handle_pending_subcommand(wa.MEMORY, ["approve", record["id"]])
    assert "execution_in_progress" in out
    assert wa.get_pending(wa.MEMORY, record["id"]) is not None


def test_retrying_same_staged_mutation_reuses_pending_identity(hermes_home):
    _configure()
    first = _stage("retry stable")
    second = _stage("retry stable")
    assert first["id"] == second["id"]


def test_tampered_linked_record_cannot_fall_through_to_legacy_apply(hermes_home):
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore
    from utils import atomic_json_write

    _configure()
    record = _stage("approved content")
    wa.create_memory_durable_approval(record)
    path = hermes_home / "pending" / "memory" / f"{record['id']}.json"
    tampered = wa.get_pending(wa.MEMORY, record["id"])
    tampered["payload"]["content"] = "tampered content"
    atomic_json_write(path, tampered, mode=0o600)
    out = handle_pending_subcommand(
        wa.MEMORY, ["approve", record["id"]], memory_store=MemoryStore()
    )
    assert "linked_action_invalid" in out
    store = MemoryStore(); store.load_from_disk()
    assert store.memory_entries == []


def test_manual_authorization_expires_overdue_request(hermes_home):
    import sqlite3
    from gateway.durable_actions import DurableActionStatus
    from gateway.durable_operations import DurableOperationsStore
    from tools import write_approval as wa

    _configure()
    record = _stage("too late")
    receipt = wa.create_memory_durable_approval(record)
    with sqlite3.connect(hermes_home / "state.db") as conn:
        conn.execute(
            "UPDATE durable_requests SET expires_at = 0 WHERE request_id = ?",
            (receipt["request_id"],),
        )
    result = DurableOperationsStore(hermes_home / "state.db").authorize_action(
        receipt["request_id"]
    )
    assert result.code == "expired"
    assert result.action_status == DurableActionStatus.EXPIRED.value


@pytest.mark.parametrize("terminal", ["declined", "expired", "cancelled", "invalidated", "failed"])
def test_pending_list_reconciles_terminal_action_state(hermes_home, terminal):
    import sqlite3
    from tools import write_approval as wa

    _configure()
    record = _stage(f"terminal {terminal}")
    receipt = wa.create_memory_durable_approval(record)
    with sqlite3.connect(hermes_home / "state.db") as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET status = ? WHERE request_id = ?",
            (terminal, receipt["request_id"]),
        )
    assert wa.list_pending(wa.MEMORY) == []
    assert wa.get_pending(wa.MEMORY, record["id"])["status"] == terminal

    from hermes_cli.write_approval_commands import handle_pending_subcommand
    assert "Rejected 1" in handle_pending_subcommand(
        wa.MEMORY, ["reject", record["id"]]
    )
    assert wa.get_pending(wa.MEMORY, record["id"]) is None


def test_tampered_linked_reject_cancels_authoritative_ticket(hermes_home):
    from gateway.durable_actions import DurableActionStatus
    from gateway.durable_operations import DurableOperationsStore
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa
    from utils import atomic_json_write

    _configure()
    record = _stage("reject original")
    receipt = wa.create_memory_durable_approval(record)
    path = hermes_home / "pending" / "memory" / f"{record['id']}.json"
    tampered = wa.get_pending(wa.MEMORY, record["id"])
    tampered["payload"]["content"] = "reject tampered"
    atomic_json_write(path, tampered, mode=0o600)
    out = handle_pending_subcommand(wa.MEMORY, ["reject", record["id"]])
    assert "Rejected 1" in out
    assert DurableOperationsStore(hermes_home / "state.db").get_action(
        receipt["request_id"]
    ).status == DurableActionStatus.CANCELLED.value
    assert wa.get_pending(wa.MEMORY, record["id"]) is None


def test_retry_after_cancel_returns_stable_terminal_receipt(hermes_home):
    from gateway.durable_operations import DurableOperationsStore
    from tools import write_approval as wa

    _configure()
    first = _stage("cancel then retry")
    receipt = wa.create_memory_durable_approval(first)
    assert DurableOperationsStore(hermes_home / "state.db").cancel_request(
        receipt["request_id"]
    ).ok
    retry = _stage("cancel then retry")
    assert retry["id"] == first["id"]
    assert wa.create_memory_durable_approval(retry) == {
        "status": "cancelled",
        "pending_id": first["id"],
        "request_id": receipt["request_id"],
        "action_id": receipt["request_id"],
    }


def test_durable_disabled_keeps_exact_legacy_staged_response(hermes_home):
    from hermes_cli.config import load_config, save_config
    from tools.memory_tool import MemoryStore, memory_tool

    config = load_config()
    config["memory"].update(write_approval=True, durable_approval=False)
    save_config(config)
    store = MemoryStore(); store.load_from_disk()
    result = json.loads(memory_tool("add", "memory", "legacy response", store=store))
    assert set(result) == {"success", "staged", "pending_id", "message"}
    assert "review with /memory pending" in result["message"]


def test_review_is_canonical_json_for_normalized_execution(hermes_home):
    from gateway.durable_requests import get_durable_request_store
    from tools import write_approval as wa

    _configure()
    record = _stage("  Add:\nWith:\n exact value  ")
    receipt = wa.create_memory_durable_approval(record)
    request = get_durable_request_store().get(request_id=receipt["request_id"])
    assert json.loads(request.body) == {
        "target": "memory",
        "operations": [{
            "action": "add",
            "content": "Add:\nWith:\n exact value",
            "old_text": "",
        }],
    }


def test_unknown_diagnostic_reports_only_structural_target_state(hermes_home):
    from gateway.durable_actions import lookup_durable_action_diagnostic
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    _configure()
    record = _stage("diagnostic desired")
    diagnose = lookup_durable_action_diagnostic("memory.apply_pending", 1)
    assert diagnose is wa.diagnose_durable_memory_pending
    assert diagnose(_context(record)).code == "baseline_present"

    store = MemoryStore(); store.load_from_disk()
    assert store.add("memory", "diagnostic desired")["success"]
    desired = diagnose(_context(record))
    assert desired.code == "desired_present"
    assert desired.facts == (("target_state", "desired"),)

    assert store.add("memory", "extra change")["success"]
    assert diagnose(_context(record)).code == "state_changed"


def test_unknown_pending_evidence_requires_explicit_reconciliation(hermes_home):
    import sqlite3
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa

    _configure()
    record = _stage("unknown evidence")
    receipt = wa.create_memory_durable_approval(record)
    with sqlite3.connect(hermes_home / "state.db") as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET status = 'unknown' WHERE request_id = ?",
            (receipt["request_id"],),
        )
    assert [item["id"] for item in wa.list_pending(wa.MEMORY)] == [record["id"]]
    out = handle_pending_subcommand(wa.MEMORY, ["reject", record["id"]])
    assert "reconciliation_required" in out
    assert wa.get_pending(wa.MEMORY, record["id"]) is not None


def test_database_unavailable_fails_linked_manual_commands_closed(hermes_home, monkeypatch):
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    _configure()
    record = _stage("database unavailable")
    wa.create_memory_durable_approval(record)

    def unavailable(*_args, **_kwargs):
        raise wa.MemoryActionLookupUnavailable

    monkeypatch.setattr(wa, "linked_memory_request_id", unavailable)
    monkeypatch.setattr(wa, "memory_action_receipt", unavailable)
    approve = handle_pending_subcommand(
        wa.MEMORY, ["approve", record["id"]], memory_store=MemoryStore()
    )
    reject = handle_pending_subcommand(wa.MEMORY, ["reject", record["id"]])
    assert "linked_action_unavailable" in approve
    assert "linked_action_unavailable" in reject
    assert wa.get_pending(wa.MEMORY, record["id"]) is not None


def test_sqlite_lookup_errors_are_not_reported_as_no_action(hermes_home, monkeypatch):
    import sqlite3
    from tools import write_approval as wa

    _configure()
    record = _stage("database error")
    wa.create_memory_durable_approval(record)

    def unavailable(*_args, **_kwargs):
        raise sqlite3.OperationalError("unavailable")

    monkeypatch.setattr(sqlite3, "connect", unavailable)
    with pytest.raises(wa.MemoryActionLookupUnavailable):
        wa.linked_memory_request_id(record)
    with pytest.raises(wa.MemoryActionLookupUnavailable):
        wa.memory_action_receipt(record["id"])


def test_pending_rewrite_rejects_symlink_target(hermes_home):
    from tools import write_approval as wa

    record = _stage("symlink protected")
    path = hermes_home / "pending" / "memory" / f"{record['id']}.json"
    victim = hermes_home / "victim.json"
    victim.write_text("untouched", encoding="utf-8")
    path.unlink()
    path.symlink_to(victim)
    with pytest.raises(OSError):
        wa._write_pending_record({**record, "status": "cancelled"})
    assert victim.read_text(encoding="utf-8") == "untouched"


def test_pending_writer_uses_portable_fallback_when_posix_capability_is_unavailable(
    hermes_home, monkeypatch
):
    from tools import write_approval as wa

    monkeypatch.delattr(wa.os, "fchmod")
    record = _stage("portable persistence")
    assert wa.get_pending(wa.MEMORY, record["id"])["payload"]["content"] == (
        "portable persistence"
    )


def test_stage_write_does_not_claim_success_when_persistence_fails(
    hermes_home, monkeypatch
):
    from tools import write_approval as wa

    def fail(_record):
        raise OSError("disk unavailable")

    monkeypatch.setattr(wa, "_write_pending_record", fail)
    with pytest.raises(OSError):
        _stage("not persisted")


def test_pending_status_reports_link_lookup_unavailable(hermes_home, monkeypatch):
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa

    record = _stage("bounded status")
    monkeypatch.setattr(wa, "list_pending", lambda *_args, **_kwargs: [record])

    def unavailable(_record):
        raise wa.MemoryActionLookupUnavailable

    monkeypatch.setattr(wa, "linked_memory_request_id", unavailable)
    output = handle_pending_subcommand(wa.MEMORY, ["pending"])
    assert "status unavailable" in output
    assert record["id"] in output


def test_existing_database_without_durable_tables_uses_legacy_manual_path(
    hermes_home
):
    import sqlite3
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    sqlite3.connect(hermes_home / "state.db").close()
    record = _stage("legacy database")
    assert wa.linked_memory_request_id(record) is None
    assert wa.memory_action_receipt(record["id"]) is None
    store = MemoryStore()
    store.load_from_disk()
    output = handle_pending_subcommand(
        wa.MEMORY, ["approve", record["id"]], memory_store=store
    )
    assert "Approved 1" in output
    assert store.memory_entries == ["legacy database"]


@pytest.mark.parametrize(
    "outcome",
    ["confirmed_applied", "confirmed_not_applied", "abandoned", "superseded"],
)
def test_reconciled_unknown_can_be_cleaned_without_applying_memory(
    hermes_home, outcome
):
    import sqlite3
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    from tools import write_approval as wa
    from tools.memory_tool import MemoryStore

    _configure()
    record = _stage(f"reconciled {outcome}")
    receipt = wa.create_memory_durable_approval(record)
    with sqlite3.connect(hermes_home / "state.db") as conn:
        conn.execute(
            "UPDATE durable_action_tickets SET status = 'unknown' WHERE request_id = ?",
            (receipt["request_id"],),
        )
        conn.execute(
            """INSERT INTO durable_action_reconciliations (
                   reconciliation_id, request_id, outcome, operator_identity,
                   note, superseded_by_request_id, created_at
               ) VALUES (?, ?, ?, ?, NULL, ?, 1)""",
            (
                f"drc_{outcome}",
                receipt["request_id"],
                outcome,
                "test-operator",
                "drq_successor" if outcome == "superseded" else None,
            ),
        )
    assert wa.list_pending(wa.MEMORY) == []
    stored = wa.get_pending(wa.MEMORY, record["id"])
    assert stored["status"] == f"reconciled_{outcome}"
    output = handle_pending_subcommand(wa.MEMORY, ["reject", record["id"]])
    assert "reconciliation_required" not in output
    assert wa.get_pending(wa.MEMORY, record["id"]) is None
    store = MemoryStore()
    store.load_from_disk()
    assert store.memory_entries == []
