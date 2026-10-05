"""Detach exact Discord tool executions into the existing durable approval outbox.

Only the terminal/code entry points bind operations. The model cannot supply a
ticket, a destination, a backend override or approval proof. Raw arguments stay
in the private state database; the Discord review and outcome are redacted.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import uuid
from contextlib import contextmanager

_operation = contextvars.ContextVar("durable_execution_operation", default=None)
_consent = contextvars.ContextVar("durable_execution_consent", default=None)
_kernel_process_generation = uuid.uuid4().hex
_SESSION_FIELDS = (
    "platform", "chat_id", "chat_type", "thread_id", "user_id", "scope_id",
    "session_key", "session_id", "ui_session_id", "message_id", "profile",
    "parent_chat_id", "cron_session", "cron_job_id",
)
_ACTION_TYPES = frozenset({"terminal.execute", "execute_code.execute"})


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, default=str).encode()).hexdigest()


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
def durable_execution_context(action_type, arguments, *, execution_state=None):
    """Bind the exact operation around a guard, without exposing it to the model."""
    operation = {
        "action_type": action_type,
        "arguments": dict(arguments),
        "execution_state": execution_state,
        "session_env": _session_values(),
        "configuration_fingerprint": _configuration_fingerprint(action_type),
    }
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
            or _digest(operation) == consent["operation_digest"])


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
        if prepared.action_type == "host.sudo":
            from tools.host_sudo import host_sudo_actor_allowed
            if not host_sudo_actor_allowed(origin.expected_actor_id):
                return _blocked("Privileged host approval is restricted to the configured owner; the command was not run.")
        ttl = policy.get("execution_ttl_seconds", 7 * 86400)
        if type(ttl) not in (int, float) or not 0 < ttl <= 7 * 86400:
            return _blocked("Invalid durable execution approval lifetime; the command was not run.")
        review = redact_sensitive_text(review_command, force=True)
        reason = redact_sensitive_text(description, force=True)
        body = f"{reason}\n\n```\n{review}\n```\n\nApprove runs this exact operation once. Reject leaves it unexecuted."
        result = get_durable_request_store().create_with_action(
            DurableRequestSpec(
                kind="tool.durable_approval." + prepared.action_type,
                title="Run privileged host command?" if prepared.action_type == "host.sudo" else "Run this operation?",
                body=body,
                options=(DurableRequestOption("approve", "Approve once"),
                         DurableRequestOption("reject", "Reject")),
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
        )
        return {"approved": False, "status": "approval_pending", "approval_pending": True,
                "request_id": result.request.request_id,
                "message": "The operation has not run. A durable approval is pending in this conversation. "
                           "After approval Hermes runs the reviewed operation once and continues here. Do not repeat it."}
    except (ValueError, TypeError):
        return _blocked("Cannot stage durable execution approval; the command was not run.")


def maybe_defer_execution(command, description):
    """Used only when the existing approval gate has decided to ask a human."""
    operation = _operation.get()
    if operation is None:
        return None
    consent = _consent.get()
    if consent is not None and consent["action_type"] == operation["action_type"]:
        # Several guard findings may ask about the same command. Consent is
        # scoped to one exact execution, rather than the first guard finding.
        if _digest(operation) != consent["operation_digest"]:
            return {"approved": False, "status": "invalidated",
                    "message": "The reviewed execution context changed; the operation was not run."}
        return {"approved": True, "user_approved": True}
    # CLI/other adapters keep their existing approval semantics.
    if operation["session_env"]["platform"] != "discord":
        return None
    try:
        if _policy_for(operation["action_type"]) is None:
            return None
        prepared = prepare_durable_execution(operation)
        return defer_prepared_execution(prepared, command, description)
    except (ValueError, TypeError):
        return _blocked("Cannot stage durable execution approval; the command was not run.")


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
        if not _origin_is_current(context, payload):
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="origin_unavailable")
        tokens = set_session_vars(**payload["session_env"])
        session_token = set_current_session_key(payload["session_env"]["session_key"])
        proof = _consent.set({"operation_digest": _digest(payload), "action_type": context.action_type})
        try:
            if context.action_type == "terminal.execute":
                from tools.terminal_tool import terminal_tool
                result = terminal_tool(**payload["arguments"])
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
        output = {"tool": "terminal" if context.action_type == "terminal.execute" else "execute_code",
                  "output": redact_sensitive_text(str(parsed.get("output", parsed.get("error", ""))), force=True),
                  "status": str(parsed.get("status", "completed")), "exit_code": parsed.get("exit_code")}
        for key in ("session_id", "yielded_session_id", "pid", "returncode"):
            if key in parsed:
                output[key] = parsed[key]
        output["output"] = output["output"].encode("utf-8")[-12000:].decode("utf-8", errors="ignore")
        while len(json.dumps(output, ensure_ascii=False).encode()) > 15000:
            output["output"] = output["output"][len(output["output"]) // 2:]
        status = parsed.get("status")
        failed = status in {"blocked", "error", "approval_pending", "pending_approval", "disabled"} or parsed.get("exit_code") not in (None, 0)
        outcome_status = (DurableActionStatus.INVALIDATED if status == "invalidated" else
                          DurableActionStatus.UNKNOWN if status in {"unknown", "timeout", "interrupted"} or parsed.get("exit_code") in {124, 130} else
                          DurableActionStatus.FAILED if failed else DurableActionStatus.SUCCEEDED)
        return DurableActionOutcome(outcome_status,
                                    reason="execution_failed" if failed else "execution_completed",
                                    result_json=encode_durable_action_result(output))
    except Exception:
        # A generic shell operation may have produced side effects before an exception.
        return DurableActionOutcome(DurableActionStatus.UNKNOWN, reason="execution_exception")


def approved_action_is_executing(context):
    """Require the persisted human decision and the exact claimed action."""
    from gateway.durable_requests import get_durable_request_store, DurableRequestStatus
    store = get_durable_request_store()
    request = store.get(request_id=context.request_id)
    if (request is None or request.status is not DurableRequestStatus.SELECTED
            or request.selected_option_id != "approve"):
        return False
    import sqlite3
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute("SELECT status, payload_json, fingerprint, action_type, action_version, idempotency_key "
                           "FROM durable_action_tickets WHERE request_id=?", (context.request_id,)).fetchone()
    return row == ("executing", context.payload_json, context.fingerprint, context.action_type,
                   context.action_version, context.idempotency_key)


def _origin_is_current(context, payload):
    from gateway.durable_requests import get_durable_request_store
    from hermes_state import SessionDB
    if not approved_action_is_executing(context):
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


def capture_code_execution_state(arguments):
    """Pin existing kernel identities/cell counts; new/reset calls need no lost state."""
    if arguments.get("reset"):
        return []
    from tools.code_kernel import _REGISTRY as local, _resolve_owner
    from tools.code_kernel_remote import _REGISTRY as remote
    owner = _resolve_owner(arguments.get("task_id"))
    result = []
    for name, registry in (("local", local), ("remote", remote)):
        with registry.lock:
            result.extend([name, _kernel_process_generation, repr(key), id(kernel), kernel.execution_count]
                          for key, kernel in registry.kernels.items() if key[0] == owner)
    return sorted(result, key=repr)
