"""Detach exact Discord tool executions into the existing durable approval outbox.

Native terminal, code and file entry points bind operations. The model cannot supply a
ticket, a destination, a backend override or approval proof. Raw arguments stay
in the private state database; the Discord review and outcome are redacted.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import uuid
import time
import math
from contextlib import contextmanager

_operation = contextvars.ContextVar("durable_execution_operation", default=None)
_consent = contextvars.ContextVar("durable_execution_consent", default=None)
_kernel_process_generation = uuid.uuid4().hex
_SESSION_FIELDS = (
    "platform", "chat_id", "chat_type", "thread_id", "user_id", "scope_id",
    "session_key", "session_id", "ui_session_id", "message_id", "profile",
    "parent_chat_id", "cron_session", "cron_job_id",
)
_ACTION_TYPES = frozenset({"terminal.execute", "execute_code.execute", "file.write", "file.patch"})


def redact_execution_value(value):
    """Redact string leaves before JSON escaping changes token boundaries."""
    from agent.redact import redact_sensitive_text
    if isinstance(value, str):
        return redact_sensitive_text(value, force=True)
    if isinstance(value, dict):
        return {key: redact_execution_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact_execution_value(item) for item in value]
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, default=str).encode()).hexdigest()


def _execution_digest(operation):
    # Guard policy is private approval metadata, not part of the operation the
    # terminal/code entry point reconstructs when the reviewed call is replayed.
    return _digest({key: value for key, value in operation.items() if key != "approval_scope"})


def _approval_scope(operation):
    scope = operation.get("approval_scope")
    if scope is None:
        return {"warnings": [], "allow_session": False, "allow_permanent": False}
    if (not isinstance(scope, dict) or type(scope.get("allow_session")) is not bool
            or type(scope.get("allow_permanent")) is not bool
            or not isinstance(scope.get("warnings"), list)):
        raise ValueError("invalid approval scope")
    for warning in scope["warnings"]:
        if (not isinstance(warning, (list, tuple)) or len(warning) != 3
                or type(warning[0]) is not str or not warning[0]
                or warning[1] is not None and type(warning[1]) is not str
                or type(warning[2]) is not bool):
            raise ValueError("invalid approval warning")
    if ((scope["allow_session"] or scope["allow_permanent"]) and not scope["warnings"]
            or scope["allow_permanent"] and (not scope["allow_session"]
                                               or all(warning[2] for warning in scope["warnings"]))):
        raise ValueError("unsupported approval scope")
    return scope


def _allowed_choices(operation):
    if operation.get("privileged_host"):
        return ("approve",)
    scope = _approval_scope(operation)
    return ("approve",) + (("session",) if scope["allow_session"] else ()) + (("always",) if scope["allow_permanent"] else ())


def _session_values():
    from gateway.session_context import get_session_env
    names = {name: "HERMES_SESSION_" + name.upper() for name in _SESSION_FIELDS}
    names.update(session_key="HERMES_SESSION_KEY", session_id="HERMES_SESSION_ID",
                 ui_session_id="HERMES_UI_SESSION_ID", cron_session="HERMES_CRON_SESSION",
                 cron_job_id="HERMES_CRON_JOB_ID")
    return {key: get_session_env(name, "") for key, name in names.items()}


def _configuration_fingerprint(action_type):
    from tools.terminal_tool import _get_env_config
    from hermes_cli.config import load_config_readonly
    config = {"terminal": _get_env_config(),
              "declared_terminal": load_config_readonly().get("terminal", {})}
    if action_type == "execute_code.execute":
        from tools.code_execution_tool import _load_config
        config["code_execution"] = _load_config()
    return _digest(config)


@contextmanager
def durable_execution_context(action_type, arguments, *, execution_state=None, privileged_host=False):
    """Bind the exact operation around a guard, without exposing it to the model."""
    operation = {
        "action_type": action_type,
        "arguments": dict(arguments),
        "execution_state": execution_state,
        "session_env": _session_values(),
        "configuration_fingerprint": _configuration_fingerprint(action_type),
    }
    if privileged_host:
        operation["privileged_host"] = True
    token = _operation.set(operation)
    try:
        yield
    finally:
        _operation.reset(token)


def prepare_durable_execution(operation):
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload
    if operation.get("action_type") not in _ACTION_TYPES:
        raise ValueError("unsupported durable execution")
    encoded = encode_durable_action_payload(operation)
    fingerprint = hashlib.sha256(encoded.encode()).hexdigest()
    from tools.approval_context import _approval_turn_id, _approval_tool_call_id
    occurrence = [_approval_turn_id.get(), _approval_tool_call_id.get()]
    if not any(occurrence):
        occurrence = [uuid.uuid4().hex]
    return PreparedDurableAction(operation["action_type"], 1, encoded, fingerprint,
                                 _digest([fingerprint, occurrence]))


def _policy_for(action_type):
    from hermes_cli.config import load_config_readonly
    policy = load_config_readonly().get("durable_approvals", {})
    if not isinstance(policy, dict) or type(policy.get("enabled", False)) is not bool:
        raise ValueError("invalid durable approvals configuration")
    allowed = policy.get("allowed_actions", [])
    if not isinstance(allowed, list) or any(type(value) is not str for value in allowed):
        raise ValueError("invalid durable action allowlist")
    return policy if policy.get("enabled") and action_type in allowed else None


def _blocked(message):
    return {"approved": False, "status": "blocked", "message": message}


def durable_execution_active():
    return _consent.get() is not None


def execution_context_matches_consent():
    operation, consent = _operation.get(), _consent.get()
    return (consent is None or operation is None or consent["action_type"] != operation["action_type"]
            or _execution_digest(operation) == consent["operation_digest"])


def defer_prepared_execution(prepared, review_command, description, *, required=False):
    """Stage a prepared trusted operation in the original Discord conversation."""
    from gateway.durable_requests import (
        DurableRequestOption, DurableRequestSpec, get_durable_request_store,
        load_durable_requests_config,
    )
    from tools.durable_request_tool import _current_origin, _delivery_target_for_origin
    from agent.redact import redact_sensitive_text
    from gateway.session_context import get_session_env
    try:
        policy = _policy_for(prepared.action_type)
        if policy is None:
            return _blocked("Durable approval is required for this operation but is not configured.") if required else None
        if get_session_env("HERMES_SESSION_PLATFORM", "").lower() != "discord":
            return _blocked("Durable host/tool approvals require the original Discord conversation; the operation was not run.")
        config = load_durable_requests_config()
        if not config.available or config.target is None:
            return _blocked("Durable execution approvals are unavailable; the command was not run.")
        origin = _current_origin(config.scheduled_responder_mode)
        if origin.origin_type != "session" or not origin.origin_session_id:
            return _blocked("Durable executions require the original Discord conversation; the command was not run.")
        from gateway.durable_actions import decode_durable_action_payload, INLINE_WAIT_OWNER
        payload = decode_durable_action_payload(prepared.payload_json)
        privileged_host = prepared.action_type == "host.sudo" or payload.get("privileged_host") is True
        if privileged_host:
            from tools.host_sudo import host_sudo_actor_allowed
            if not host_sudo_actor_allowed(origin.expected_actor_id):
                return _blocked("Privileged host approval is restricted to the configured owner; the command was not run.")
        ttl = policy.get("execution_ttl_seconds", 7 * 86400)
        if type(ttl) not in (int, float) or not 0 < ttl <= 7 * 86400:
            return _blocked("Invalid durable execution approval lifetime; the command was not run.")
        wait_seconds = inline_wait_seconds(policy)
        if prepared.action_type in {"file.write", "file.patch"}:
            review_command = json.dumps(redact_execution_value(payload["arguments"]), ensure_ascii=False, indent=2)
        review = redact_sensitive_text(review_command, force=True)
        reason = redact_sensitive_text(description, force=True)
        reason = reason.encode("utf-16-le")[:320].decode("utf-16-le", errors="ignore")
        if prepared.action_type in _ACTION_TYPES:
            from gateway.durable_actions import decode_durable_action_payload
            choices = _allowed_choices(decode_durable_action_payload(prepared.payload_json))
            labels = {"approve": "Allow Once", "session": "Allow Session", "always": "Always Allow"}
            options = tuple(DurableRequestOption(choice, labels[choice]) for choice in choices)
            options += (DurableRequestOption("reject", "Deny"),)
        else:
            options = (DurableRequestOption("approve", "Approve Once"), DurableRequestOption("reject", "Deny"))
        header = "**Requested command:**\n```\n"
        suffix = f"\n```\n\n**Why it was flagged:** {reason}"
        if privileged_host:
            import socket
            header = f"**Host:** {socket.gethostname()[:128]}\n\n" + header
        from gateway.interactive import MAX_PROMPT_BODY_LENGTH
        budget = min(850, MAX_PROMPT_BODY_LENGTH - len(header) - len(suffix))
        if len(review.encode("utf-16-le")) // 2 > budget:
            notice = "\n[Remaining content omitted; full review attached. SHA-256: " + prepared.fingerprint + "]"
            review = review.encode("utf-16-le")[:2 * (budget - len(notice))].decode("utf-16-le", errors="ignore") + notice
        body = header + review + suffix
        result = get_durable_request_store().create_with_action(
            DurableRequestSpec(
                kind="tool.durable_approval." + ("host." if privileged_host and prepared.action_type != "host.sudo" else "") + prepared.action_type,
                title="HOST SUDO — elevated privileges" if privileged_host else "Hermes wants to run a command that needs your OK",
                body=body,
                options=options,
                ttl_seconds=min(ttl, config.max_ttl_seconds),
                reference="durable-execution:" + prepared.idempotency_key,
            ),
            action=prepared, target=_delivery_target_for_origin(origin, config.target),
            responder_mode=origin.responder_mode, expected_actor_id=origin.expected_actor_id,
            origin_type=origin.origin_type, origin_id=origin.origin_id,
            origin_session_id=origin.origin_session_id, origin_ui_session_id=origin.origin_ui_session_id,
            origin_profile=origin.origin_profile,
            command_response_allowed=config.command_responses_enabled and config.allow_on_native_delivery,
            text_command_fallback_allowed=config.command_responses_enabled and config.allow_text_delivery_fallback,
            maximum_ttl_seconds=config.max_ttl_seconds,
            terminal_retention_seconds=config.terminal_retention_seconds,
            inline_wait_owner=INLINE_WAIT_OWNER if wait_seconds else None,
            inline_wait_until=time.time() + wait_seconds if wait_seconds else None,
        )
        if wait_seconds:
            completed = _wait_inline(get_durable_request_store(), result.request.request_id, wait_seconds)
            if completed is not None:
                return completed
        return {"approved": False, "status": "approval_pending", "approval_pending": True,
                "request_id": result.request.request_id,
                "message": "The operation is pending approval or execution in this conversation. "
                           "After approval Hermes runs the reviewed operation once and continues here. Do not repeat it."}
    except (ValueError, TypeError):
        return _blocked("Cannot stage durable execution approval; the command was not run.")


def maybe_defer_execution(command, description, *, warnings=None, allow_session=False, allow_permanent=False, required=False):
    """Used only when the existing approval gate has decided to ask a human."""
    operation = _operation.get()
    if operation is None:
        return None
    consent = _consent.get()
    if _reviewed_nested_host_command(operation, consent):
        return {"approved": True, "user_approved": True}
    if consent is not None and consent["action_type"] == operation["action_type"]:
        # Several guard findings may ask about the same command. Consent is
        # scoped to one exact execution, rather than the first guard finding.
        if _execution_digest(operation) != consent["operation_digest"]:
            return {"approved": False, "status": "invalidated",
                    "message": "The reviewed execution context changed; the operation was not run."}
        return {"approved": True, "user_approved": True}
    # CLI/other adapters keep their existing approval semantics.
    if operation["session_env"]["platform"] != "discord":
        return _blocked("Host sudo requires explicit operator approval in Discord.") if required else None
    try:
        if _policy_for(operation["action_type"]) is None:
            return _blocked("Required operator approval is not configured.") if required else None
        operation = dict(operation)
        if warnings is not None:
            operation["approval_scope"] = {
                "warnings": [list(warning) for warning in warnings],
                "allow_session": allow_session, "allow_permanent": allow_permanent,
            }
        _approval_scope(operation)
        prepared = prepare_durable_execution(operation)
        return defer_prepared_execution(prepared, command, description, required=required)
    except (ValueError, TypeError):
        return _blocked("Cannot stage durable execution approval; the command was not run.")


def _reviewed_nested_host_command(operation, consent):
    """A reviewed script's literal host terminal calls share its one-shot consent."""
    if (not consent or consent["action_type"] != "execute_code.execute"
            or operation["action_type"] != "terminal.execute" or not operation.get("privileged_host")):
        return False
    payload = consent.get("payload", {})
    context = consent.get("execution_context")
    if not context or not payload.get("privileged_host"):
        return False
    # Resolve the declared backend now; the parent fingerprint pins it.
    from tools.terminal_tool import _get_env_config
    commands = reviewed_host_terminal_commands(payload["arguments"]["code"], _get_env_config()["env_type"])
    from tools.host_sudo import host_sudo_actor_allowed
    return (operation["arguments"].get("command") in commands
            and operation["session_env"] == payload["session_env"]
            and _policy_for(context.action_type) is not None
            and _configuration_fingerprint(context.action_type) == payload["configuration_fingerprint"]
            and host_sudo_actor_allowed(payload["session_env"]["user_id"])
            and _origin_is_current(context, payload))


def inline_wait_seconds(policy=None):
    if policy is None:
        from hermes_cli.config import load_config_readonly
        policy = load_config_readonly().get("durable_approvals", {})
    value = policy.get("inline_wait_seconds", 300)
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 300:
        raise ValueError("invalid inline approval wait")
    return float(value)


def _wait_inline(store, request_id, seconds):
    """Return the executed result to this caller; never replay the reviewed call."""
    from gateway.durable_actions import INLINE_WAIT_OWNER, decode_durable_action_result
    from contextlib import ExitStack
    from tools.approval_human_wait import human_wait_window, activity_heartbeat
    from tools.interrupt import is_interrupted
    heartbeat = activity_heartbeat("Waiting for tool approval")
    deadline = time.monotonic() + seconds
    try:
        with ExitStack() as human_wait:
            human_wait.enter_context(human_wait_window())
            while True:
                outcome = store.consume_inline_result(request_id, INLINE_WAIT_OWNER)
                if outcome is not None:
                    try:
                        native = decode_durable_action_result(outcome["result_json"]) if outcome.get("result_json") else {
                            "status": "blocked", "error": "The reviewed operation was not executed: " + str(outcome["status"]),
                        }
                    except (TypeError, ValueError):
                        native = {"status": "unknown", "error": "Recorded execution result is unreadable; inspect its receipt before retrying.",
                                  "request_id": request_id}
                    return {"approved": False, "status": "execution_completed", "tool_result": native}
                request = store.get(request_id=request_id)
                if request is not None and request.status.value == "selected":
                    # Execution time is tool work, not time awaiting a human.
                    human_wait.close()
                if time.monotonic() >= deadline or is_interrupted():
                    return None
                heartbeat()
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
    finally:
        store.release_inline_wait(request_id, INLINE_WAIT_OWNER)


def execute_durable_execution(context):
    from gateway.durable_actions import (
        DurableActionOutcome, DurableActionStatus, decode_durable_action_payload,
        encode_durable_action_result,
    )
    from gateway.session_context import set_session_vars
    from tools.approval_context import set_current_session_key, reset_current_session_key
    from agent.redact import redact_sensitive_text
    try:
        payload = decode_durable_action_payload(context.payload_json)
        if (hashlib.sha256(context.payload_json.encode()).hexdigest() != context.fingerprint
                or payload["action_type"] != context.action_type
                or context.action_type not in _ACTION_TYPES):
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="execution_payload_changed")
        if _policy_for(context.action_type) is None:
            return DurableActionOutcome(DurableActionStatus.HANDLER_UNAVAILABLE, reason="execution_not_allowlisted")
        if _configuration_fingerprint(context.action_type) != payload["configuration_fingerprint"]:
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="execution_configuration_changed")
        if payload.get("privileged_host"):
            from tools.host_sudo import host_sudo_actor_allowed
            if not host_sudo_actor_allowed(payload["session_env"]["user_id"]):
                return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="operator_no_longer_allowed")
        if context.action_type == "terminal.execute":
            from tools.host_sudo import requires_host_sudo_approval
            from tools.terminal_tool import _get_env_config
            if requires_host_sudo_approval(payload["arguments"]["command"], _get_env_config()["env_type"]) and payload.get("privileged_host") is not True:
                return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="host_sudo_requires_new_review")
        if context.action_type == "execute_code.execute":
            from tools.terminal_tool import _get_env_config
            if code_requests_host_sudo(payload["arguments"]["code"], _get_env_config()["env_type"]) and payload.get("privileged_host") is not True:
                return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="host_sudo_requires_new_review")
        if not _origin_is_current(context, payload):
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="origin_unavailable")
        tokens = set_session_vars(**payload["session_env"])
        session_token = set_current_session_key(payload["session_env"]["session_key"])
        proof = _consent.set({"operation_digest": _execution_digest(payload), "action_type": context.action_type,
                               "execution_context": context, "payload": payload})
        try:
            if context.action_type == "terminal.execute":
                from tools.terminal_tool import terminal_tool
                result = terminal_tool(**payload["arguments"])
            elif context.action_type in {"file.write", "file.patch"}:
                from tools.file_tools import restore_file_execution_baseline, write_file_tool, patch_tool
                if not restore_file_execution_baseline(context.action_type, payload["arguments"], payload["execution_state"]):
                    return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="file_target_changed")
                handler = write_file_tool if context.action_type == "file.write" else patch_tool
                result = handler(**payload["arguments"])
            else:
                from tools.code_execution_tool import execute_code
                if (capture_code_execution_state(payload["arguments"]) != payload["execution_state"]
                        or not _code_kernels_alive(payload["arguments"])):
                    return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="code_kernel_state_changed")
                result = execute_code(**payload["arguments"])
        finally:
            _consent.reset(proof)
            reset_current_session_key(session_token)
            for token in reversed(tokens):
                token.var.reset(token)
        parsed = json.loads(result)
        if parsed.get("status") not in {"invalidated", "blocked", "approval_pending", "pending_approval", "disabled"}:
            _persist_reviewed_scope(context, payload)
        tool = {"terminal.execute": "terminal", "execute_code.execute": "execute_code",
                "file.write": "write_file", "file.patch": "patch"}[context.action_type]
        output = dict(parsed, tool=tool)
        # Preserve native result fields for the waiting caller, with the same
        # private outbox size limit and credential redaction as deferred results.
        output = redact_execution_value(output)
        for key in ("output", "error", "diff"):
            if isinstance(output.get(key), str):
                output[key] = output[key].encode("utf-8")[-12000:].decode("utf-8", errors="ignore")
        if len(json.dumps(output, ensure_ascii=False).encode()) > 15000:
            output = {"tool": tool, "status": parsed.get("status", "completed"),
                      "exit_code": parsed.get("exit_code"), "success": parsed.get("success"),
                      "output": str(parsed.get("output", parsed.get("error", parsed.get("diff", ""))))[-3000:],
                      "truncated": True}
            output["output"] = redact_sensitive_text(output["output"], force=True)
        status = parsed.get("status")
        failed = status in {"blocked", "error", "approval_pending", "pending_approval", "disabled"} or parsed.get("exit_code") not in (None, 0) or parsed.get("success") is False or bool(parsed.get("error"))
        outcome_status = (DurableActionStatus.INVALIDATED if status == "invalidated" else
                          DurableActionStatus.UNKNOWN if status in {"unknown", "timeout", "interrupted"} or parsed.get("exit_code") in {124, 130} else
                          DurableActionStatus.FAILED if failed else DurableActionStatus.SUCCEEDED)
        return DurableActionOutcome(outcome_status,
                                    reason="execution_failed" if failed else "execution_completed",
                                    result_json=encode_durable_action_result(output))
    except Exception:
        # A generic shell operation may have produced side effects before an exception.
        return DurableActionOutcome(DurableActionStatus.UNKNOWN, reason="execution_exception")


def approved_action_is_executing(context, *, allowed_options=("approve",)):
    """Require the persisted human decision and the exact claimed action."""
    from gateway.durable_requests import get_durable_request_store, DurableRequestStatus
    store = get_durable_request_store()
    request = store.get(request_id=context.request_id)
    if (request is None or request.status is not DurableRequestStatus.SELECTED
            or request.selected_option_id not in allowed_options):
        return False
    import sqlite3
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute("SELECT status, payload_json, fingerprint, action_type, action_version, idempotency_key "
                           "FROM durable_action_tickets WHERE request_id=?", (context.request_id,)).fetchone()
    return row == ("executing", context.payload_json, context.fingerprint, context.action_type,
                   context.action_version, context.idempotency_key)


def _persist_reviewed_scope(context, payload):
    from gateway.durable_requests import get_durable_request_store
    from tools.approval import _persist_choice
    request = get_durable_request_store().get(request_id=context.request_id)
    choice = request.selected_option_id
    if choice not in {"session", "always"}:
        return
    if not _origin_is_current(context, payload):
        # A long-running command can finish after /new. Its reviewed result may
        # be reported, but it must not grant permissions to the replacement.
        return
    if not approved_action_is_executing(context, allowed_options=_allowed_choices(payload)):
        raise ValueError("scope persistence requires its executing human decision")
    _persist_choice(
        payload["session_env"]["session_key"], choice, _approval_scope(payload)["warnings"],
        session_identity=payload["session_env"], durable_request_id=context.request_id,
    )


def _origin_is_current(context, payload):
    from gateway.durable_requests import get_durable_request_store
    from hermes_state import SessionDB
    if not approved_action_is_executing(context, allowed_options=_allowed_choices(payload)):
        return False
    request = get_durable_request_store().get(request_id=context.request_id)
    session_env = payload["session_env"]
    if (request is None or request.origin_type != "session"
            or request.origin_session_id != session_env["session_id"]
            or request.origin_id != session_env["session_key"]
            or request.expected_actor_id != session_env["user_id"]
            or request.target.delivery_channel_id != session_env["chat_id"]):
        return False
    db = SessionDB()
    try:
        session = db.get_session(request.origin_session_id)
    finally:
        db.close()
    return session is not None and session.get("ended_at") is None


def _code_kernels_alive(arguments):
    if arguments.get("reset"):
        return True
    from tools.code_kernel import _REGISTRY as local, _resolve_owner
    from tools.code_kernel_remote import _REGISTRY as remote
    owner = _resolve_owner(arguments.get("task_id"))
    for registry in (local, remote):
        with registry.lock:
            kernels = [kernel for key, kernel in registry.kernels.items() if key[0] == owner]
        for kernel in kernels:
            probe = getattr(kernel, "alive", None) or getattr(kernel, "is_alive", None)
            if probe is not None and not probe():
                return False
    return True


def capture_terminal_execution_state(env, env_type):
    """Pin an existing container; approval cannot move execution to a fresh one."""
    container_id = getattr(env, "_container_id", None)
    if container_id:
        return {"backend": env_type, "container_id": container_id}
    if env_type not in {"local", "ssh"}:
        return {"backend": env_type, "instance": id(env)}
    return None


def reviewed_host_terminal_commands(code, env_type):
    """Extract only literal terminal commands from the exact reviewed script."""
    import ast
    from tools.host_sudo import requires_host_sudo_approval
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return frozenset()
    commands = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
        if name != "terminal":
            continue
        argument = node.args[0] if node.args else next((item.value for item in node.keywords if item.arg == "command"), None)
        if isinstance(argument, ast.Constant) and isinstance(argument.value, str) and requires_host_sudo_approval(argument.value, env_type):
            commands.add(argument.value)
    return frozenset(commands)


def code_requests_host_sudo(code, env_type):
    """Recognize visible direct subprocess elevation before saved code grants."""
    if reviewed_host_terminal_commands(code, env_type):
        return True
    import ast
    from tools.host_sudo import requires_host_sudo_approval
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False
    direct_calls = {"run", "Popen", "call", "check_call", "check_output", "system", "popen"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
        if name not in direct_calls:
            continue
        try:
            command = ast.literal_eval(node.args[0])
        except (ValueError, TypeError):
            continue
        if isinstance(command, (list, tuple)) and all(isinstance(part, str) for part in command):
            import shlex
            command = shlex.join(command)
        if isinstance(command, str) and requires_host_sudo_approval(command, env_type):
            return True
    return False


def capture_code_execution_state(arguments):
    """Pin working directory and existing kernel state before an approval wait."""
    from tools import code_execution_tool
    from tools.terminal_tool import get_session_cwd
    task_id = arguments.get("task_id") or ""
    state = {
        "cwd": code_execution_tool._resolve_child_cwd(
            code_execution_tool._get_execution_mode(), "", task_id=task_id,
        ),
        "terminal_cwd": get_session_cwd(task_id),
        "kernels": [],
    }
    if arguments.get("reset"):
        return state
    from tools.code_kernel import _REGISTRY as local, _resolve_owner
    from tools.code_kernel_remote import _REGISTRY as remote
    owner = _resolve_owner(arguments.get("task_id"))
    result = []
    for name, registry in (("local", local), ("remote", remote)):
        with registry.lock:
            result.extend([name, _kernel_process_generation, repr(key), id(kernel), kernel.execution_count]
                          for key, kernel in registry.kernels.items() if key[0] == owner)
    state["kernels"] = sorted(result, key=repr)
    return state
