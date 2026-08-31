import json
from pathlib import Path

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


def test_delete_moves_whole_skill_to_journaled_quarantine(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    (skill / "references").mkdir()
    (skill / "references" / "a.md").write_text("a")
    record = wa.stage_skill_write(
        {"action": "delete", "name": "demo", "absorbed_into": ""},
        summary="delete", origin="foreground",
    )

    outcome = wa.execute_durable_skill_pending(_context(record))

    assert outcome.status is DurableActionStatus.SUCCEEDED
    assert not skill.exists()
    journal = json.loads((tmp_path / "skills" / ".transactions" / record["id"] / "journal.json").read_text())
    assert journal["state"] == "COMMITTED"
    assert (Path(journal["quarantine_relative_path"]) / "SKILL.md").name == "SKILL.md"
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


def test_root_change_oversize_and_binary_baseline_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    record = wa.stage_skill_write({"action": "create", "name": "demo", "content": VALID}, summary="x", origin="foreground")
    other = tmp_path / "other"; monkeypatch.setenv("HERMES_HOME", str(other))
    assert wa.execute_durable_skill_pending(_context(record)).status is DurableActionStatus.INVALIDATED
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    skill = tmp_path / "skills" / "binary"; skill.mkdir(parents=True); (skill / "SKILL.md").write_bytes(b"\xff\xfe")
    binary = wa.stage_skill_write({"action": "patch", "name": "binary", "old_string": "x", "new_string": "y"}, summary="x", origin="foreground")
    huge = wa.stage_skill_write({"action": "write_file", "name": "binary", "file_path": "references/x", "file_content": "x" * (wa.MAX_DURABLE_SKILL_FILE_BYTES + 1)}, summary="x", origin="foreground")
    assert binary["status"] == huge["status"] == "pending_manual_review"


def test_delete_fault_after_executing_is_unknown_with_journal_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path)); _config(monkeypatch)
    skill = tmp_path / "skills" / "demo"; skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(VALID)
    record = wa.stage_skill_write({"action": "delete", "name": "demo", "absorbed_into": ""}, summary="delete", origin="foreground")
    context = _context(record)
    real_replace = wa.os.replace

    def fail_target(source, destination, *args, **kwargs):
        if Path(source) == skill:
            raise OSError("fault")
        return real_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr(wa.os, "replace", fail_target)
    assert wa.execute_durable_skill_pending(context).status is DurableActionStatus.UNKNOWN
    diagnostic = wa.diagnose_durable_skill_pending(context)
    assert diagnostic.code == "journal_evidence"
    assert ("journal_state", "executing") in diagnostic.facts
    assert skill.exists()
