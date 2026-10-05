from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

from gateway.durable_actions import DurableActionStatus
from gateway.durable_requests import get_durable_request_store, reset_durable_request_store_cache
from gateway.session_context import reset_session_vars, set_session_vars
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


@pytest.fixture
def file_profile(tmp_path, monkeypatch):
    from tools import file_tools
    from tools.environments.local import LocalEnvironment
    from tools.file_operations import ShellFileOperations
    from tools.approval_context import set_current_session_key, reset_current_session_key
    import hermes_state

    home = tmp_path / "profile"
    home.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    token = set_hermes_home_override(home)
    reset_durable_request_store_cache()
    reset_session_vars()
    (home / "config.yaml").write_text(f'''terminal:
  backend: local
  cwd: {workspace}
approvals:
  mode: manual
durable_requests:
  enabled: true
  inbox:
    platform: discord
    chat_id: "999999999999999999"
durable_approvals:
  enabled: true
  inline_wait_seconds: 0
  allowed_actions: [file.write, file.patch]
''')
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", home / "state.db")
    db = hermes_state.SessionDB()
    db.create_session("file-session", "discord", session_key="file-session-key",
                      chat_id="123456789012345678", user_id="42")
    db.close()
    set_session_vars(platform="discord", chat_id="123456789012345678", thread_id="123456789012345678",
                     user_id="42", session_key="file-session-key", session_id="file-session", cron_session="")
    session_token = set_current_session_key("file-session-key")
    ops = ShellFileOperations(LocalEnvironment(cwd=str(workspace), timeout=15))
    monkeypatch.setattr(file_tools, "_get_file_ops", lambda task_id="default": ops)
    try:
        yield workspace
    finally:
        reset_current_session_key(session_token)
        reset_session_vars()
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)


def _run(request_id, action_type, *, option_token="1"):
    from gateway.durable_actions import DurableActionStore, _DURABLE_ACTION_HANDLERS
    from gateway.durable_action_runner import DurableActionRunner
    from tools import durable_execution_approval as bridge

    _DURABLE_ACTION_HANDLERS.setdefault((action_type, 1), (bridge.prepare_durable_execution,
                                                       bridge.execute_durable_execution))
    store = get_durable_request_store()
    delivery = store.claim_next_pending(["discord"])
    store.bind_delivery(request_id=delivery.request.request_id, claim_id=delivery.claim_id,
                        platform="discord", message_id="file-review-message")
    if not option_token.isdigit():
        option_token = str(next(index for index, choice in enumerate(delivery.request.options, 1)
                                if choice.id == option_token))
    assert store.resolve(request_id=request_id, option_token=option_token, platform="discord",
                         channel_id="123456789012345678", message_id="file-review-message",
                         actor_id="42").status.value == "resolved"
    runner = DurableActionRunner(DurableActionStore(store.db_path), enabled=True, allowed_actions=[action_type])
    assert asyncio.run(runner.process_once())
    assert not asyncio.run(runner.process_once())
    with sqlite3.connect(store.db_path) as conn:
        status = conn.execute("SELECT status FROM durable_action_tickets WHERE request_id=?", (request_id,)).fetchone()[0]
    return DurableActionStatus(status)


@pytest.mark.parametrize("action", ["file.write", "file.patch"])
def test_protected_file_edit_is_durable_exact_and_only_once(file_profile, action):
    from tools.file_tools import read_file_tool, write_file_tool, patch_tool

    target = file_profile / "AGENTS.md"
    target.write_text("original rules\n")
    read_file_tool(str(target), task_id="file-task")
    if action == "file.write":
        result = json.loads(write_file_tool(str(target), "reviewed rules\n", task_id="file-task"))
    else:
        result = json.loads(patch_tool(path=str(target), old_string="original", new_string="reviewed", task_id="file-task"))
    assert result["status"] == "approval_pending", result
    assert target.read_text() == "original rules\n"
    request = get_durable_request_store().get(request_id=result["request_id"])
    assert [choice.id for choice in request.options] == ["approve", "reject"]
    assert request.target.delivery_channel_id == "123456789012345678"
    assert "reviewed" in request.body
    assert _run(result["request_id"], action) is DurableActionStatus.SUCCEEDED
    assert target.read_text() == "reviewed rules\n"


@pytest.mark.parametrize("change", ["contents", "replace_inode", "symlink", "created"])
def test_deferred_file_target_changes_invalidate_instead_of_overwriting(file_profile, change):
    from tools.file_tools import read_file_tool, write_file_tool

    target = file_profile / "AGENTS.md"
    if change != "created":
        target.write_text("original\n")
        read_file_tool(str(target), task_id="file-task")
    pending = json.loads(write_file_tool(str(target), "approved candidate\n", task_id="file-task"))
    assert pending["status"] == "approval_pending", pending
    if change == "replace_inode":
        replacement = file_profile / "replacement"
        replacement.write_text("external change\n")
        replacement.replace(target)
    elif change == "symlink":
        other = file_profile / "other.md"
        other.write_text("external change\n")
        target.unlink()
        target.symlink_to(other)
    else:
        target.write_text("external change\n")
    assert _run(pending["request_id"], "file.write") is DurableActionStatus.INVALIDATED
    assert target.read_text() == "external change\n"


def test_file_write_approval_survives_loss_of_process_local_read_proof(file_profile):
    from tools import file_tools
    from tools.file_tools_read_tracking import _read_tracker, _read_tracker_lock

    target = file_profile / "AGENTS.md"
    target.write_text("original\n")
    file_tools.read_file_tool(str(target), task_id="file-task")
    pending = json.loads(file_tools.write_file_tool(str(target), "approved\n", task_id="file-task"))
    assert pending["status"] == "approval_pending", pending
    with _read_tracker_lock:
        _read_tracker.pop("file-task", None)
    reset_durable_request_store_cache()
    assert _run(pending["request_id"], "file.write") is DurableActionStatus.SUCCEEDED
    assert target.read_text() == "approved\n"


def test_native_overwrite_read_requirement_is_not_broadened(file_profile):
    from tools import file_tools

    target = file_profile / "AGENTS.md"
    target.write_text("unread original\n")
    pending = json.loads(file_tools.write_file_tool(str(target), "candidate\n", task_id="unread-task"))
    assert pending["status"] == "approval_pending", pending
    assert _run(pending["request_id"], "file.write") is DurableActionStatus.INVALIDATED
    assert target.read_text() == "unread original\n"


def test_native_write_error_is_recorded_as_failure_not_completed(file_profile, monkeypatch):
    from tools import file_tools
    from tools.file_operations_common import WriteResult

    target = file_profile / "AGENTS.md"
    pending = json.loads(file_tools.write_file_tool(str(target), "candidate\n", task_id="file-task"))
    assert pending["status"] == "approval_pending", pending
    ops = file_tools._get_file_ops("file-task")
    monkeypatch.setattr(ops, "write_file", lambda *args, **kwargs: WriteResult(error="Permission denied"))
    assert _run(pending["request_id"], "file.write") is DurableActionStatus.FAILED
    assert not target.exists()


def test_long_instruction_write_keeps_exact_payload_and_delivers_bounded_review(file_profile):
    from tools import file_tools

    target = file_profile / "AGENTS.md"
    secret = "sk-abcdefghijklmnopqrstuvwxyz0123456789"
    content = "Header\n" + secret + "\n" + "Detailed reviewed instructions.\n" * 200
    pending = json.loads(file_tools.write_file_tool(str(target), content, task_id="file-task"))
    assert pending["status"] == "approval_pending", pending
    store = get_durable_request_store()
    request = store.get(request_id=pending["request_id"])
    assert len(request.body) <= 1200
    assert "omitted" in request.body.lower()
    assert "SHA-256" in request.body
    assert secret not in request.body
    with sqlite3.connect(store.db_path) as conn:
        payload = json.loads(conn.execute("SELECT payload_json FROM durable_action_tickets WHERE request_id=?",
                                         (request.request_id,)).fetchone()[0])
    assert payload["arguments"]["content"] == content
    assert _run(pending["request_id"], "file.write") is DurableActionStatus.SUCCEEDED
    assert target.read_text() == content


def test_file_guard_returns_inline_worker_result_without_second_write(file_profile, monkeypatch):
    from tools import file_tools, durable_execution_approval

    calls = []

    def completed(*args, **kwargs):
        calls.append(kwargs)
        return {"approved": False, "status": "execution_completed",
                "tool_result": {"success": True, "verified": True, "files_modified": ["reviewed-path"]}}

    monkeypatch.setattr(durable_execution_approval, "maybe_defer_execution", completed)
    target = file_profile / "AGENTS.md"
    result = json.loads(file_tools.write_file_tool(str(target), "candidate\n"))
    assert result == {"success": True, "verified": True, "files_modified": ["reviewed-path"]}
    assert not target.exists()
    assert calls == [{"allow_session": False, "allow_permanent": False}]


def test_remote_snapshot_uses_backend_not_gateway_stat(file_profile, monkeypatch):
    from tools import file_tools
    import tools.file_tools_read_tracking as tracking

    target = file_profile / "AGENTS.md"
    target.write_text("remote content\n")
    monkeypatch.setattr(file_tools, "_file_ops_uses_host_paths", lambda ops: False)
    monkeypatch.setattr(tracking, "_file_version", lambda path: pytest.fail("gateway file used for remote target"))
    state = file_tools.capture_file_execution_state("file.patch", {"path": str(target), "mode": "replace", "task_id": "remote-task"})
    assert state["host_paths"] is False
    assert state["targets"][0]["exists"] is True
    assert len(state["targets"][0]["version"][-1]) == 64
    target.write_text("changed remotely\n")
    assert not file_tools.restore_file_execution_baseline("file.patch", {"path": str(target), "mode": "replace", "task_id": "remote-task"}, state)


def test_multifile_patch_baseline_tracks_every_target_before_any_edit(file_profile):
    from tools import file_tools

    protected = file_profile / "AGENTS.md"
    plain = file_profile / "notes.md"
    protected.write_text("rules\n")
    plain.write_text("notes\n")
    patch = f"*** Begin Patch\n*** Update File: {protected}\n@@\n-rules\n+reviewed rules\n*** Update File: {plain}\n@@\n-notes\n+reviewed notes\n*** End Patch"
    arguments = {"mode": "patch", "patch": patch, "task_id": "multi-file-task"}
    state = file_tools.capture_file_execution_state("file.patch", arguments)
    assert {target["resolved"] for target in state["targets"]} == {str(protected), str(plain)}
    plain.write_text("someone else's notes\n")
    assert not file_tools.restore_file_execution_baseline("file.patch", arguments, state)
    assert protected.read_text() == "rules\n"


def test_ordinary_write_does_not_capture_a_durable_target(file_profile, monkeypatch):
    from tools import file_tools
    import tools.file_tools_durable as durable

    monkeypatch.setattr(durable, "capture_file_execution_state", lambda *args: pytest.fail("ordinary file staged"))
    target = file_profile / "ordinary.md"
    result = json.loads(file_tools.write_file_tool(str(target), "ordinary content\n"))
    assert not result.get("error"), result
    assert target.read_text() == "ordinary content\n"


@pytest.mark.parametrize("action, choice", [("file.write", "session"), ("file.patch", "always")])
def test_native_ssh_config_review_retains_existing_scoped_controls(file_profile, monkeypatch, action, choice):
    from tools import approval, file_tools
    import agent.file_safety as safety

    # Route the existing SSH-config policy to a temporary home, while executing
    # the actual file gate and shell backend. No real host SSH files are touched.
    monkeypatch.setattr(safety, "_guard_homes", lambda path: {str(file_profile)})
    monkeypatch.setattr(approval, "_presence", lambda callback=None: (None, False, True, False))
    target = file_profile / ".ssh" / "config"
    target.parent.mkdir()
    target.write_text("Host example\n  HostName old.example\n")
    file_tools.read_file_tool(str(target), task_id="ssh-file-task")
    if action == "file.write":
        pending = json.loads(file_tools.write_file_tool(str(target), "Host example\n  HostName reviewed.example\n", task_id="ssh-file-task"))
    else:
        pending = json.loads(file_tools.patch_tool(path=str(target), old_string="old.example", new_string="reviewed.example", task_id="ssh-file-task"))
    assert pending["status"] == "approval_pending", pending
    request = get_durable_request_store().get(request_id=pending["request_id"])
    assert [option.id for option in request.options] == ["approve", "session", "always", "reject"]
    assert _run(pending["request_id"], action, option_token=choice) is DurableActionStatus.SUCCEEDED
    assert "reviewed.example" in target.read_text()
    assert approval.is_approved("file-session-key", "ssh_config_write")
