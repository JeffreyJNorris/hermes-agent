import json
import shutil
from pathlib import Path

import pytest

from gateway.durable_actions import (
    DurableActionExecutionContext,
    DurableActionStatus,
    lookup_durable_action_handler,
)
from tools import write_approval as wa


VALID = "---\nname: demo\ndescription: Does demo tasks.\n---\n\n# Demo\n\nDo it.\n"


def _config(monkeypatch, *, allowed=True):
    config = {
        "skills": {"write_approval": True, "durable_approval": True},
        "durable_approvals": {
            "enabled": True,
            "allowed_actions": ["skill.apply_pending"] if allowed else [],
        },
    }
    monkeypatch.setattr("hermes_cli.config.load_config", lambda: config)
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: config)
    return config


def _context(record):
    prepared = wa.prepare_durable_skill_pending(record)
    assert prepared is not None
    return DurableActionExecutionContext(
        request_id="req_12345678",
        action_type=prepared.action_type,
        action_version=prepared.action_version,
        payload_json=prepared.payload_json,
        fingerprint=prepared.fingerprint,
        idempotency_key=prepared.idempotency_key,
    )


def test_stage_create_uses_private_manifest_and_exact_staged_bytes(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)

    record = wa.stage_skill_write(
        {"action": "create", "name": "demo", "content": VALID},
        summary="create demo", origin="foreground",
    )

    assert "content" not in record.get("payload", {})
    assert record["manifest_version"] == 1
    assert record["operations"][0]["relative_path"] == "demo/SKILL.md"
    staged = tmp_path / "pending" / "skills" / record["id"] / "staged" / "0"
    assert staged.read_bytes() == VALID.encode()
    assert not (staged.stat().st_mode & 0o077)


def test_patch_stages_final_bytes_and_apply_does_not_replay_patch(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    updated = VALID.replace("Do it.", "Do it safely.")

    record = wa.stage_skill_write({
        "action": "patch", "name": "demo", "old_string": "Do it.",
        "new_string": "Do it safely.", "replace_all": False,
    }, summary="patch demo", origin="foreground")
    (skill / "SKILL.md").write_text(VALID.replace("Do it.", "Changed elsewhere."))
    outcome = wa.execute_durable_skill_pending(_context(record))

    assert outcome.status is DurableActionStatus.INVALIDATED
    assert (skill / "SKILL.md").read_text() != updated


def test_supporting_file_patch_targets_requested_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); (skill / "references" / "a.md").write_text("old\n")
    record = wa.stage_skill_write({"action": "patch", "name": "demo", "file_path": "references/a.md", "old_string": "old", "new_string": "new"}, summary="patch", origin="foreground")
    assert record["operations"][0]["relative_path"] == "demo/references/a.md"
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.SUCCEEDED
    assert (skill / "references" / "a.md").read_text() == "new\n"


def test_review_uses_stored_baseline_not_changed_live_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    record = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID.replace("Do it.", "New.")}, summary="edit", origin="foreground")
    expected = wa.skill_pending_diff(record)
    (skill / "SKILL.md").write_text(VALID.replace("Do it.", "Unapproved live change."))
    assert wa.skill_pending_diff(record) == expected


@pytest.mark.parametrize("snapshot", ["baseline/0", "staged/0"])
def test_review_rejects_snapshot_bytes_that_do_not_match_manifest(tmp_path, monkeypatch, snapshot):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True); (skill / "SKILL.md").write_text(VALID)
    record = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID.replace("Do it.", "New.")}, summary="edit", origin="foreground")
    (tmp_path / "pending" / "skills" / record["id"] / snapshot).write_text("tampered")
    assert wa.skill_pending_diff(record) == "(durable review unavailable)"


def test_local_diff_is_complete_when_durable_prompt_exceeds_render_cap(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    content = VALID + ("changed line\n" * 7_000)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": content}, summary="x", origin="foreground")
    review = wa.skill_pending_diff(record)
    assert len(review) > wa.MAX_DURABLE_SKILL_DIFF_CHARS
    assert review.endswith("+changed line\n")
    assert wa.create_skill_durable_approval(record)["reason"] == "invalid_or_oversize_review"


@pytest.mark.parametrize("action", ["write_file", "patch"])
def test_supporting_content_uses_existing_character_limit(tmp_path, monkeypatch, action):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); (skill / "references" / "a").write_text("old")
    payload = ({"action": action, "name": "demo", "file_path": "references/a", "file_content": "x" * 100_001}
               if action == "write_file" else
               {"action": action, "name": "demo", "file_path": "references/a", "old_string": "old", "new_string": "x" * 100_001})
    assert wa.stage_skill_write(payload, summary="x", origin="foreground")["status"] == "pending_manual_review"


def test_non_directory_parent_and_bad_pending_id_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True); (skill / "SKILL.md").write_text(VALID)
    (skill / "references").write_text("not a directory")
    record = wa.stage_skill_write({"action": "write_file", "name": "demo", "file_path": "references/a", "file_content": "x"}, summary="x", origin="foreground")
    assert record["status"] == "pending_manual_review"
    bad = DurableActionExecutionContext("req", "skill.apply_pending", 1, '{"pending_id":"../x"}', "x", "x")
    assert wa.execute_durable_skill_pending(bad).status is DurableActionStatus.FAILED


def test_apply_single_file_once_and_detect_already_applied(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    record = wa.stage_skill_write({
        "action": "write_file", "name": "demo", "file_path": "references/a.md",
        "file_content": "hello\n",
    }, summary="write", origin="foreground")
    context = _context(record)

    first = wa.execute_durable_skill_pending(context)
    second = wa.execute_durable_skill_pending(context)

    assert first.status is DurableActionStatus.SUCCEEDED
    assert second.status is DurableActionStatus.ALREADY_APPLIED
    assert (skill / "references" / "a.md").read_text() == "hello\n"


def test_create_already_applied_requires_no_unreviewed_siblings(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    skill = tmp_path / "skills" / "demo"; skill.mkdir(); (skill / "SKILL.md").write_text(VALID)
    (skill / "unreviewed.txt").write_text("surprise")
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.INVALIDATED


def test_manifest_or_staged_tampering_invalidates(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)
    record = wa.stage_skill_write(
        {"action": "create", "name": "demo", "content": VALID},
        summary="create", origin="foreground",
    )
    context = _context(record)
    staged = tmp_path / "pending" / "skills" / record["id"] / "staged" / "0"
    staged.write_text("tampered")

    assert wa.execute_durable_skill_pending(context).status is DurableActionStatus.INVALIDATED


def test_paths_symlinks_external_and_unsupported_fall_back(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    outside = tmp_path / "outside"
    outside.mkdir()
    (skill / "references").symlink_to(outside, target_is_directory=True)

    assert wa.stage_skill_write({"action": "write_file", "name": "demo", "file_path": "../x", "file_content": "x"}, summary="x", origin="foreground")["status"] == "pending_manual_review"
    assert wa.stage_skill_write({"action": "write_file", "name": "demo", "file_path": "references/x", "file_content": "x"}, summary="x", origin="foreground")["status"] == "pending_manual_review"
    assert wa.stage_skill_write({"action": "rename", "name": "demo"}, summary="x", origin="foreground")["status"] == "pending_manual_review"


def test_remove_file_moves_target_to_journaled_quarantine(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    (skill / "references").mkdir()
    (skill / "references" / "a.md").write_text("a")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a.md"}, summary="remove", origin="foreground")

    outcome = wa.execute_durable_skill_pending(_context(record))

    assert outcome.status is DurableActionStatus.SUCCEEDED
    assert skill.exists() and not (skill / "references" / "a.md").exists()
    journal = json.loads((tmp_path / "skills" / ".transactions" / record["id"] / "journal.json").read_text())
    assert journal["state"] == "COMMITTED"
    assert (tmp_path / "skills" / journal["quarantine_relative_path"]).read_text() == "a"
    assert not (tmp_path / "pending" / "skills" / record["id"]).exists()


def test_handler_is_statically_registered():
    assert lookup_durable_action_handler("skill.apply_pending", 1) is wa.execute_durable_skill_pending


def test_complete_review_creates_idempotent_bounded_action(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_SESSION_KEY", "discord:123")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "456")
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")
    monkeypatch.setenv("HERMES_SESSION_ID", "session-1")
    from gateway.durable_requests import reset_durable_request_store_cache
    from hermes_cli.config import load_config, save_config

    reset_durable_request_store_cache()
    config = load_config()
    config["skills"].update(write_approval=True, durable_approval=True)
    config["durable_approvals"] = {"enabled": True, "allowed_actions": ["skill.apply_pending"]}
    config["durable_requests"].update(enabled=True, inbox={"platform": "discord", "chat_id": "123", "thread_id": None})
    save_config(config)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="create", origin="foreground")
    first = wa.create_skill_durable_approval(record); second = wa.create_skill_durable_approval(record)
    assert first == second and first["status"] == "awaiting_approval"
    assert wa.linked_skill_request_id(wa.get_pending(wa.SKILLS, record["id"])) == first["request_id"]
    reset_durable_request_store_cache()


def test_terminal_request_recreation_reports_terminal_status_without_snapshots(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    shutil.rmtree(tmp_path / "pending" / "skills" / record["id"])
    monkeypatch.setattr(wa, "skill_action_receipt", lambda pending_id: ("request-1", "succeeded", None))
    receipt = wa.create_skill_durable_approval(record)
    assert receipt == {"status": "succeeded", "pending_id": record["id"],
                       "request_id": "request-1", "action_id": "request-1"}


def test_real_skill_manage_builds_request_without_content_in_action(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_SESSION_KEY", "discord:123")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "456")
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")
    monkeypatch.setenv("HERMES_SESSION_ID", "session-1")
    from gateway.durable_requests import reset_durable_request_store_cache
    from hermes_cli.config import load_config, save_config
    from tools.skill_manager_tool import skill_manage
    config = load_config()
    config["skills"].update(write_approval=True, durable_approval=True)
    config["durable_approvals"] = {"enabled": True, "allowed_actions": ["skill.apply_pending"]}
    config["durable_requests"].update(enabled=True, inbox={"platform": "discord", "chat_id": "123", "thread_id": None})
    save_config(config); reset_durable_request_store_cache()

    receipt = json.loads(skill_manage(action="create", name="demo", content=VALID))
    assert receipt.get("status") == "awaiting_approval", receipt
    record = wa.get_pending(wa.SKILLS, receipt["pending_id"])
    assert record["payload"] == {"pending_id": record["id"]}
    assert "Does demo tasks" not in record["payload"].__repr__()
    reset_durable_request_store_cache()


def test_linked_manual_approve_authorizes_and_reject_cancels(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_SESSION_KEY", "discord:123")
    monkeypatch.setenv("HERMES_SESSION_USER_ID", "456")
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")
    monkeypatch.setenv("HERMES_SESSION_ID", "session-1")
    from gateway.durable_requests import reset_durable_request_store_cache
    from gateway.durable_operations import DurableOperationsStore
    from hermes_cli.config import load_config, save_config
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    config = load_config(); config["skills"].update(write_approval=True, durable_approval=True)
    config["durable_approvals"] = {"enabled": True, "allowed_actions": ["skill.apply_pending"]}
    config["durable_requests"].update(enabled=True, inbox={"platform": "discord", "chat_id": "123", "thread_id": None})
    save_config(config); reset_durable_request_store_cache()
    first = wa.stage_skill_write({"action": "create", "name": "one", "content": VALID.replace("name: demo", "name: one")}, summary="one", origin="foreground")
    first_receipt = wa.create_skill_durable_approval(first)
    assert first_receipt.get("status") == "awaiting_approval", first_receipt
    assert "Authorized 1" in handle_pending_subcommand(wa.SKILLS, ["approve", first["id"]])
    assert DurableOperationsStore(tmp_path / "state.db").get_action(first_receipt["request_id"]).status == "ready"
    second = wa.stage_skill_write({"action": "create", "name": "two", "content": VALID.replace("name: demo", "name: two")}, summary="two", origin="foreground")
    second_receipt = wa.create_skill_durable_approval(second)
    assert "Rejected 1" in handle_pending_subcommand(wa.SKILLS, ["reject", second["id"]])
    assert DurableOperationsStore(tmp_path / "state.db").get_action(second_receipt["request_id"]).status == "cancelled"
    reset_durable_request_store_cache()


def test_manual_fallback_manifest_applies_through_exact_handler_and_receipt_has_id(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    from hermes_cli.write_approval_commands import handle_pending_subcommand
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    fallback = wa.create_skill_durable_approval(record)
    assert fallback["pending_id"] == record["id"]
    assert "Approved 1" in handle_pending_subcommand(wa.SKILLS, ["approve", record["id"]])
    assert (tmp_path / "skills" / "demo" / "SKILL.md").read_text() == VALID


def test_root_change_oversize_and_binary_baseline_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    other = tmp_path / "other"; monkeypatch.setenv("HERMES_HOME", str(other))
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.INVALIDATED


def test_parent_swap_after_secure_open_cannot_write_outside_root(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True); (skill / "SKILL.md").write_text(VALID)
    outside = tmp_path / "outside"; outside.mkdir(); (outside / "SKILL.md").write_text("outside")
    record = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID.replace("Do it.", "changed")}, summary="x", origin="foreground")

    def swap(point):
        if point == "parents_opened":
            skill.rename(tmp_path / "skills" / "moved")
            skill.symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(wa, "_skill_mutation_fault", swap)
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.UNKNOWN
    assert (outside / "SKILL.md").read_text() == "outside"


def test_parent_swap_after_secure_open_cannot_quarantine_outside_file(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; references = skill / "references"
    references.mkdir(parents=True); (skill / "SKILL.md").write_text(VALID); (references / "a").write_text("inside")
    outside = tmp_path / "outside"; outside.mkdir(); (outside / "a").write_text("outside")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")

    def swap(point):
        if point == "parents_opened":
            references.rename(skill / "moved-references")
            references.symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(wa, "_skill_mutation_fault", swap)
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.UNKNOWN
    assert (outside / "a").read_text() == "outside"


def test_missing_secure_dirfd_capability_uses_legacy_manual_record(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    monkeypatch.setattr(wa, "_secure_skill_capabilities_available", lambda root: False)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    assert record["status"] == "pending_manual_review"
    assert "manifest_version" not in record
    assert record["payload"]["content"] == VALID


@pytest.mark.parametrize("error", [wa.errno.EPERM, wa.errno.EOPNOTSUPP])
def test_secure_capability_probe_fails_closed_and_cleans_artifacts(tmp_path, monkeypatch, error):
    root = tmp_path / "skills"; root.mkdir()
    monkeypatch.setattr(wa, "_renameat2", lambda *args: (_ for _ in ()).throw(OSError(error, "blocked")))
    assert wa._secure_skill_capabilities_available(root) is False
    assert not list(root.glob(".durable-probe-*"))


def test_secure_capability_probe_proves_actual_skill_filesystem_and_cleans(tmp_path):
    root = tmp_path / "skills"; root.mkdir()
    assert wa._secure_skill_capabilities_available(root) is True
    assert not list(root.glob(".durable-probe-*"))


@pytest.mark.parametrize("fault_point,expected_status", [
    ("parents_opened", DurableActionStatus.INVALIDATED),
    ("entry_validated", DurableActionStatus.UNKNOWN),
])
def test_update_entry_cas_never_overwrites_intervening_content(tmp_path, monkeypatch, fault_point, expected_status):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True); target = skill / "SKILL.md"; target.write_text(VALID)
    record = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID.replace("Do it.", "approved")}, summary="x", origin="foreground")
    monkeypatch.setattr(wa, "_skill_mutation_fault", lambda point: target.write_text("intervening") if point == fault_point else None)
    assert wa.execute_durable_skill_pending(_context(record)).status is expected_status
    assert target.read_text() == "intervening"


def test_create_entry_cas_is_no_replace(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    target = tmp_path / "skills" / "demo"
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")

    def appear(point):
        if point == "entry_validated":
            target.mkdir(); (target / "SKILL.md").write_text("intervening")

    monkeypatch.setattr(wa, "_skill_mutation_fault", appear)
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.INVALIDATED
    assert (target / "SKILL.md").read_text() == "intervening"


def test_remove_entry_cas_restores_intervening_content_and_stays_unknown(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); target = skill / "references" / "a"; target.write_text("baseline")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")
    monkeypatch.setattr(wa, "_skill_mutation_fault", lambda point: target.write_text("intervening") if point == "entry_validated" else None)
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.UNKNOWN
    assert target.read_text() == "intervening"
    assert not (tmp_path / "skills" / ".quarantine" / record["id"]).exists()


def test_remove_closes_first_parent_fd_when_second_open_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); (skill / "references" / "a").write_text("a")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")
    real_open = wa._open_skill_dirfd; captured = []

    def fail_quarantine(root, relative, **kwargs):
        if relative == Path(".quarantine"):
            raise OSError("fault")
        fd = real_open(root, relative, **kwargs)
        if relative == Path("demo/references"):
            captured.append(fd)
        return fd

    monkeypatch.setattr(wa, "_open_skill_dirfd", fail_quarantine)
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.INVALIDATED
    with pytest.raises(OSError):
        wa.os.fstat(captured[0])


def test_cleanup_closes_transaction_fd_when_parent_open_fails(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); (skill / "references" / "a").write_text("a")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.SUCCEEDED
    monkeypatch.setattr(wa, "skill_action_receipt", lambda pending_id: ("req", "succeeded", None))
    real_open = wa._open_skill_dirfd; captured = []; transaction_opens = 0

    def fail_second_transaction_open(root, relative, **kwargs):
        nonlocal transaction_opens
        if relative == Path(".transactions"):
            raise OSError("fault")
        fd = real_open(root, relative, **kwargs)
        if relative == Path(".transactions") / record["id"]:
            transaction_opens += 1
            if transaction_opens == 2:
                captured.append(fd)
        return fd

    monkeypatch.setattr(wa, "_open_skill_dirfd", fail_second_transaction_open)
    assert wa.cleanup_terminal_skill_evidence() == 0
    with pytest.raises(OSError):
        wa.os.fstat(captured[0])
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    skill = tmp_path / "skills" / "binary"; skill.mkdir(parents=True); (skill / "SKILL.md").write_bytes(b"\xff\xfe")
    binary = wa.stage_skill_write({"action": "patch", "name": "binary", "old_string": "x", "new_string": "y"}, summary="x", origin="foreground")
    huge = wa.stage_skill_write({"action": "write_file", "name": "binary", "file_path": "references/x", "file_content": "x" * (wa.MAX_DURABLE_SKILL_FILE_BYTES + 1)}, summary="x", origin="foreground")
    assert binary["status"] == huge["status"] == "pending_manual_review"


def test_absent_remove_without_journal_invalidates_and_security_is_rechecked(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); target = skill / "references" / "a"; target.write_text("a")
    removed = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")
    target.unlink()
    assert wa.execute_durable_skill_pending(_context(removed)).status is DurableActionStatus.INVALIDATED
    edit = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID.replace("Do it.", "changed")}, summary="x", origin="foreground")
    target.write_text("a")
    scan_remove = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")
    monkeypatch.setattr("tools.skill_manager_tool._guard_agent_created_enabled", lambda: True)
    monkeypatch.setattr("tools.skill_manager_tool._security_scan_skill", lambda path: "blocked")
    assert wa.execute_durable_skill_pending(_context(edit)).status is DurableActionStatus.INVALIDATED
    assert (skill / "SKILL.md").read_text() == VALID
    monkeypatch.setattr("tools.skill_manager_tool._security_scan_skill", lambda path: (_ for _ in ()).throw(OSError()))
    assert wa.execute_durable_skill_pending(_context(scan_remove)).status is DurableActionStatus.INVALIDATED
    assert target.read_text() == "a"
    assert not (tmp_path / "skills" / ".transactions" / scan_remove["id"]).exists()


def test_whole_skill_delete_remains_manual_only(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True); (skill / "SKILL.md").write_text(VALID)
    record = wa.stage_skill_write({"action": "delete", "name": "demo", "absorbed_into": ""}, summary="delete", origin="foreground")
    assert record["status"] == "pending_manual_review"
    assert record["payload"]["action"] == "delete"


def test_remove_fault_after_executing_is_unknown_with_journal_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); target = skill / "references" / "a"; target.write_text("a")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="remove", origin="foreground")
    context = _context(record)
    real_rename = wa._renameat2

    def fail_target(source_fd, source, target_fd, destination, flags):
        if source == target.name:
            raise OSError("fault")
        return real_rename(source_fd, source, target_fd, destination, flags)

    monkeypatch.setattr(wa, "_renameat2", fail_target)
    assert wa.execute_durable_skill_pending(context).status is DurableActionStatus.UNKNOWN
    diagnostic = wa.diagnose_durable_skill_pending(context)
    assert diagnostic.code == "journal_evidence"
    assert ("journal_state", "executing") in diagnostic.facts
    assert ("recovery", "confirm_not_applied") in diagnostic.facts
    assert target.exists()


@pytest.mark.parametrize("fault", [
    "prepared", "executing", "renamed", "source_fsynced",
    "quarantine_fsynced", "committed",
])
def test_remove_every_journal_boundary_becomes_unknown_without_replay(tmp_path, monkeypatch, fault):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); target = skill / "references" / "a"; target.write_text("a")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="remove", origin="foreground")
    context = _context(record)
    monkeypatch.setattr(wa, "_skill_journal_fault", lambda point: (_ for _ in ()).throw(OSError(point)) if point == fault else None)
    assert wa.execute_durable_skill_pending(context).status is DurableActionStatus.UNKNOWN
    before = target.exists()
    assert wa.execute_durable_skill_pending(context).status in {DurableActionStatus.UNKNOWN, DurableActionStatus.ALREADY_APPLIED}
    assert target.exists() == before


def test_committed_cleanup_is_bounded_and_unknown_is_retained(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    root = tmp_path / "skills"
    records = []
    for name in ("one", "two"):
        skill = root / name; (skill / "references").mkdir(parents=True)
        (skill / "SKILL.md").write_text(VALID.replace("name: demo", f"name: {name}"))
        (skill / "references" / "a").write_text(name)
        records.append(wa.stage_skill_write({"action": "remove_file", "name": name, "file_path": "references/a"}, summary="x", origin="foreground"))
    assert wa.execute_durable_skill_pending(_context(records[0])).status is DurableActionStatus.SUCCEEDED
    monkeypatch.setattr(wa, "_skill_journal_fault", lambda point: (_ for _ in ()).throw(OSError()) if point == "executing" else None)
    assert wa.execute_durable_skill_pending(_context(records[1])).status is DurableActionStatus.UNKNOWN
    monkeypatch.setattr(wa, "skill_action_receipt", lambda pid: ("req", "succeeded", None) if pid == records[0]["id"] else ("req", "unknown", None))
    assert wa.cleanup_terminal_skill_evidence(limit=1) == 1
    assert not (root / ".quarantine" / records[0]["id"]).exists()
    assert not (root / ".transactions" / records[0]["id"]).exists()
    assert (root / ".transactions" / records[1]["id"]).exists()


@pytest.mark.parametrize("fault,reconciliation,cleaned", [
    ("committed", "confirmed_applied", 1),
    ("prepared", "confirmed_not_applied", 1),
    ("executing", "confirmed_applied", 0),
])
def test_reconciliation_cleans_only_matching_remove_evidence(tmp_path, monkeypatch, fault, reconciliation, cleaned):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; (skill / "references").mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID); (skill / "references" / "a").write_text("a")
    record = wa.stage_skill_write({"action": "remove_file", "name": "demo", "file_path": "references/a"}, summary="x", origin="foreground")
    monkeypatch.setattr(wa, "_skill_journal_fault", lambda point: (_ for _ in ()).throw(OSError()) if point == fault else None)
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.UNKNOWN
    monkeypatch.setattr(wa, "skill_action_receipt", lambda pid: ("req", "unknown", reconciliation))
    assert wa.cleanup_terminal_skill_evidence() == cleaned
    assert (tmp_path / "skills" / ".transactions" / record["id"]).exists() is (not cleaned)
    if cleaned:
        assert wa.get_pending(wa.SKILLS, record["id"]) is None
        assert not wa.list_pending(wa.SKILLS, include_terminal=True)


def test_staging_exception_cleanup_does_not_remove_concurrent_pending_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    concurrent = tmp_path / "pending" / "skills" / "concur01"
    real_write = wa._write_pending_record

    def fail(record):
        if record.get("manifest_version") == 1:
            concurrent.mkdir()
            raise OSError("fault")
        return real_write(record)

    monkeypatch.setattr(wa, "_write_pending_record", fail)
    with pytest.raises(OSError):
        wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    assert concurrent.is_dir()
    assert list((tmp_path / "pending" / "skills").iterdir()) == [concurrent]


def test_terminal_rejection_cleans_staged_bytes_but_unknown_retains_them(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    rejected = wa.stage_skill_write({"action": "create", "name": "rejected", "content": VALID.replace("name: demo", "name: rejected")}, summary="x", origin="foreground")
    unknown = wa.stage_skill_write({"action": "create", "name": "unknown", "content": VALID.replace("name: demo", "name: unknown")}, summary="x", origin="foreground")
    monkeypatch.setattr(wa, "skill_action_receipt", lambda pid: ("req", "declined", None) if pid == rejected["id"] else ("req", "unknown", None))
    wa.list_pending(wa.SKILLS)
    assert not (tmp_path / "pending" / "skills" / rejected["id"]).exists()
    assert (tmp_path / "pending" / "skills" / unknown["id"]).exists()


def test_security_scan_and_protected_provenance_reject_without_staging(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True); (skill / "SKILL.md").write_text(VALID)
    monkeypatch.setattr("tools.skill_manager_tool._guard_agent_created_enabled", lambda: True)
    monkeypatch.setattr("tools.skill_manager_tool._security_scan_skill", lambda path: "blocked")
    rejected = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID.replace("Do it.", "unsafe")}, summary="x", origin="foreground")
    assert rejected == {"status": "rejected", "reason": "security_scan_rejected"}
    assert not list((tmp_path / "pending" / "skills").glob("*.json"))
    monkeypatch.setattr("tools.skill_manager_tool._guard_agent_created_enabled", lambda: False)
    monkeypatch.setattr("tools.skill_usage.is_bundled", lambda name: True)
    protected = wa.stage_skill_write({"action": "edit", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    assert protected == {"status": "rejected", "reason": "protected_target"}
