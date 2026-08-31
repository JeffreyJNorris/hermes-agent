#!/usr/bin/env python3
"""Write-approval gate + pending store for memory and skill writes.

Background
----------
The agent writes to two persistent stores that survive across sessions:

  * **memory** — MEMORY.md / USER.md, small (~200 char) declarative entries
  * **skills** — SKILL.md + supporting files, potentially huge (10-100 KB)

Both stores are written from two origins:

  * **foreground** — a normal agent turn (user is present / chatting)
  * **background_review** — the self-improvement review fork that runs after a
    turn and autonomously decides what to save (the source of the
    "wrong assumptions" users complained about)

This module lets the user gate those writes per-subsystem with a boolean
``write_approval``:

  * ``false`` (default) — write freely (the pre-gate behaviour)
  * ``true``            — require approval: do not commit the write; either
    prompt inline (memory, interactive CLI only) or **stage** it to a pending
    store and surface it for the user to approve or reject out-of-band

The size asymmetry between memory and skills is real and unavoidable: a memory
entry can be reviewed inline in a chat bubble; a 100 KB SKILL.md cannot. So
the gate stages BOTH to disk, but review affordances differ by subsystem
(see ``hermes_cli`` slash handlers): memory shows full content, skills show
metadata + a one-line gist + a ``diff`` escape hatch (CLI/dashboard/file).

Staging is mandatory for background-origin writes (a daemon thread cannot
block on an interactive prompt) and for gateway sessions (no inline prompt
channel — review happens via ``/memory pending``). Foreground CLI memory
writes prompt inline via the dangerous-command approval callback; skill
writes always stage (too big to eyeball mid-loop).

Pending records live under ``<HERMES_HOME>/pending/{memory,skills}/<id>.json``
so they survive process restarts and can be reviewed from CLI, gateway, or the
web dashboard.
"""

from __future__ import annotations

import json
import hashlib
import logging
import re
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

# Subsystem identifiers
MEMORY = "memory"
SKILLS = "skills"
_SUBSYSTEMS = (MEMORY, SKILLS)

# Config key (per subsystem). A single boolean: the approval gate is OFF by
# default (writes flow freely, the pre-gate behaviour), and ON means stage /
# prompt every write for the user's approval. There is intentionally no third
# "block all writes" state — to disable a subsystem entirely use its own
# enable flag (e.g. ``memory.memory_enabled: false``).
CONFIG_KEY = "write_approval"
DURABLE_CONFIG_KEY = "durable_approval"
_MEMORY_RECORD_VERSION = 2
_MEMORY_FINGERPRINT_VERSION = 1
_PENDING_ID_RE = re.compile(r"^[0-9a-f]{8}$")
_TERMINAL_MEMORY_ACTION_STATUSES = frozenset({
    "declined", "expired", "cancelled", "abandoned", "invalidated", "failed",
    "unknown", "handler_unavailable", "succeeded", "already_applied",
})


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------

def write_approval_enabled(subsystem: str) -> bool:
    """Return whether the approval gate is enabled for ``subsystem``.

    Reads ``<subsystem>.write_approval`` from config.yaml. Defaults to
    ``False`` (gate off — writes flow freely) for any unset / invalid value so
    existing installs keep their current behaviour until the user opts in.
    """
    if subsystem not in _SUBSYSTEMS:
        return False
    try:
        from hermes_cli.config import load_config, cfg_get
        cfg = load_config()
        raw = cfg_get(cfg, subsystem, CONFIG_KEY, default=False)
    except Exception:
        return False
    return _normalize_enabled(raw)


def durable_approval_enabled(subsystem: str) -> bool:
    if subsystem not in _SUBSYSTEMS:
        return False
    try:
        from hermes_cli.config import cfg_get, load_config

        return cfg_get(load_config(), subsystem, DURABLE_CONFIG_KEY, default=False) is True
    except Exception:
        return False


def _normalize_enabled(value: Any) -> bool:
    """Coerce a config value to a bool. Default (unknown) is False (gate off).

    Accepts real bools and the usual truthy/falsey strings. YAML 1.1 parses
    bare ``on``/``off``/``yes``/``no`` as bools already, so the string branch
    is mostly for hand-edited configs.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"on", "true", "yes", "1", "approve", "enabled"}
    return False


# ---------------------------------------------------------------------------
# Pending store (file-backed)
# ---------------------------------------------------------------------------

def _pending_dir(subsystem: str) -> Path:
    return get_hermes_home() / "pending" / subsystem


def _write_pending_record(record: Dict[str, Any]) -> None:
    from utils import atomic_json_write

    directory = _pending_dir(record["subsystem"])
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    atomic_json_write(directory / f"{record['id']}.json", record, mode=0o600)


def _memory_operations(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if payload.get("action") == "batch":
        operations = payload.get("operations")
        return operations if isinstance(operations, list) else []
    return [{
        "action": payload.get("action"),
        "content": payload.get("content"),
        "old_text": payload.get("old_text"),
    }]


def _normalize_memory_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    target = payload.get("target", "memory")
    operations = [{
        "action": operation.get("action"),
        "content": str(operation.get("content") or "").strip(),
        "old_text": str(operation.get("old_text") or "").strip(),
    } for operation in _memory_operations(payload)]
    if payload.get("action") == "batch":
        return {"action": "batch", "target": target, "operations": operations}
    return {"target": target, **operations[0]}


def _memory_record_fields(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    from tools.memory_tool import ENTRY_DELIMITER, load_on_disk_store

    target = payload.get("target", "memory")
    if target not in {"memory", "user"}:
        return None
    operations = _memory_operations(payload)
    if not operations:
        return None
    store = load_on_disk_store()
    path = store._path_for(target)
    with store._file_lock(path):
        if store._reload_target(target) is not None:
            return None
        desired, error = store._plan_batch(target, operations)
        if error is not None or desired is None:
            return None
        baseline = path.read_bytes() if path.exists() else b""
    desired_bytes = (ENTRY_DELIMITER.join(desired) if desired else "").encode("utf-8")
    return {
        "record_version": _MEMORY_RECORD_VERSION,
        "fingerprint_version": _MEMORY_FINGERPRINT_VERSION,
        "profile_identity": str(get_hermes_home().resolve()),
        "logical_target": target,
        "target_path": "USER.md" if target == "user" else "MEMORY.md",
        "baseline_digest": hashlib.sha256(baseline).hexdigest(),
        "desired_digest": hashlib.sha256(desired_bytes).hexdigest(),
    }


def memory_pending_fingerprint(record: Dict[str, Any]) -> str:
    canonical = {
        key: record.get(key)
        for key in (
            "record_version", "fingerprint_version", "profile_identity",
            "action", "logical_target", "target_path", "baseline_digest",
            "desired_digest", "payload",
        )
    }
    encoded = json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stage_write(subsystem: str, payload: Dict[str, Any],
                *, summary: str, origin: str) -> Dict[str, Any]:
    """Persist a pending write and return a short record describing it.

    Args:
        subsystem: ``memory`` or ``skills``.
        payload: the exact kwargs needed to replay the write when approved
            (e.g. ``{"action": "add", "target": "user", "content": "..."}``
            for memory, or the full ``skill_manage`` kwargs for skills).
        summary: a one-line human-readable description shown in pending lists.
            For skills this is the LLM/heuristic gist; for memory it can be the
            entry text itself.
        origin: ``foreground`` or ``background_review`` — recorded for audit.

    Returns a dict with ``id`` and metadata. Best-effort: on disk failure it
    logs and still returns a record (the write is simply lost, which is the
    safe failure for an approval gate — nothing is silently committed).
    """
    if subsystem == MEMORY:
        payload = _normalize_memory_payload(payload)
    fields = _memory_record_fields(payload) if subsystem == MEMORY else None
    if fields is not None:
        identity = json.dumps(
            {"payload": payload, **fields},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        pid = hashlib.sha256(identity).hexdigest()[:8]
    else:
        pid = uuid.uuid4().hex[:8]
    record = {
        "id": pid,
        "subsystem": subsystem,
        "action": payload.get("action", ""),
        "summary": (summary or "").strip(),
        "origin": origin or "foreground",
        "created_at": time.time(),
        "status": "pending",
        "payload": payload,
    }
    if fields is not None:
        record.update(fields)
        record["subject_fingerprint"] = memory_pending_fingerprint(record)
    try:
        _write_pending_record(record)
    except Exception as e:  # pragma: no cover - disk failure path
        logger.error("Failed to stage pending %s write: %s", subsystem, e, exc_info=True)
    return record


def list_pending(
    subsystem: str,
    *,
    include_terminal: bool = False,
) -> List[Dict[str, Any]]:
    """Return all pending records for ``subsystem``, oldest first."""
    d = _pending_dir(subsystem)
    if not d.exists():
        return []
    records: List[Dict[str, Any]] = []
    for p in d.glob("*.json"):
        try:
            record = json.loads(p.read_text(encoding="utf-8"))
            if subsystem == MEMORY and record.get("status", "pending") == "pending":
                action = memory_action_receipt(record.get("id", ""))
                if action is not None and action[1] in _TERMINAL_MEMORY_ACTION_STATUSES:
                    record = {**record, "status": action[1]}
                    try:
                        _write_pending_record(record)
                    except OSError:
                        pass
            status = record.get("status", "pending")
            if status == "pending" or (include_terminal and status != "applied"):
                records.append(record)
        except Exception:
            logger.warning("Skipping unreadable pending record: %s", p)
    records.sort(key=lambda r: r.get("created_at", 0))
    return records


def get_pending(subsystem: str, pending_id: str) -> Optional[Dict[str, Any]]:
    """Return a single pending record by id, or None."""
    if not isinstance(pending_id, str) or _PENDING_ID_RE.fullmatch(pending_id) is None:
        return None
    path = _pending_dir(subsystem) / f"{pending_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def discard_pending(subsystem: str, pending_id: str) -> bool:
    """Delete a pending record. Returns True if it existed."""
    if not isinstance(pending_id, str) or _PENDING_ID_RE.fullmatch(pending_id) is None:
        return False
    path = _pending_dir(subsystem) / f"{pending_id}.json"
    try:
        if path.exists():
            path.unlink()
            return True
    except Exception as e:  # pragma: no cover
        logger.error("Failed to discard pending %s/%s: %s", subsystem, pending_id, e)
    return False


def prepare_durable_memory_pending(record: Dict[str, Any]):
    """Build the fixed bounded action ticket for a versioned pending record."""
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload

    pending_id = record.get("id")
    if (
        record.get("record_version") != _MEMORY_RECORD_VERSION
        or record.get("fingerprint_version") != _MEMORY_FINGERPRINT_VERSION
        or not isinstance(pending_id, str)
        or _PENDING_ID_RE.fullmatch(pending_id) is None
    ):
        return None
    fingerprint = memory_pending_fingerprint(record)
    if record.get("subject_fingerprint") != fingerprint:
        return None
    return PreparedDurableAction(
        action_type="memory.apply_pending",
        action_version=1,
        payload_json=encode_durable_action_payload({"pending_id": pending_id}),
        fingerprint=fingerprint,
        idempotency_key=f"memory.apply_pending:{pending_id}:{fingerprint}",
    )


def _memory_review(record: Dict[str, Any]) -> str:
    return json.dumps(
        {
            "operations": _memory_operations(record["payload"]),
            "target": record["logical_target"],
        },
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def create_memory_durable_approval(record: Dict[str, Any]) -> Dict[str, Any]:
    """Create one trusted request+ticket, or retain the record for manual review."""
    from gateway.durable_requests import (
        DurableRequestError,
        DurableRequestLimitError,
        DurableRequestOption,
        DurableRequestSpec,
        get_durable_request_store,
        load_durable_requests_config,
    )
    from hermes_cli.config import load_config_readonly

    stored = get_pending(MEMORY, str(record.get("id", "")))
    if stored is None or stored.get("status", "pending") != "pending":
        return {"status": "pending_manual_review", "reason": "invalid_record"}
    record = stored
    prepared = prepare_durable_memory_pending(record)
    if prepared is None:
        return {"status": "pending_manual_review", "reason": "invalid_record"}
    try:
        config = load_config_readonly()
        policy = config.get("durable_approvals", {})
        if not isinstance(policy, dict) or policy.get("enabled") is not True:
            return {"status": "pending_manual_review", "reason": "durable_disabled"}
        allowed = policy.get("allowed_actions")
        if not isinstance(allowed, list) or "memory.apply_pending" not in allowed:
            return {"status": "pending_manual_review", "reason": "handler_not_allowed"}
        request_config = load_durable_requests_config()
        if not request_config.available or request_config.target is None:
            return {"status": "pending_manual_review", "reason": "inbox_unavailable"}
        from tools.durable_request_tool import _current_origin
        try:
            origin = _current_origin(request_config.scheduled_responder_mode)
        except ValueError:
            return {"status": "pending_manual_review", "reason": "inbox_unavailable"}
        result = get_durable_request_store().create_with_action(
            DurableRequestSpec(
                kind="tool.durable_approval.memory.apply_pending",
                title="Apply staged memory change?",
                body=_memory_review(record),
                options=(
                    DurableRequestOption(id="approve", label="Approve"),
                    DurableRequestOption(id="reject", label="Reject"),
                ),
                ttl_seconds=request_config.default_ttl_seconds,
                reference=f"memory.apply_pending:{record['id']}",
            ),
            action=prepared,
            target=request_config.target,
            responder_mode=origin.responder_mode,
            expected_actor_id=origin.expected_actor_id,
            origin_type=origin.origin_type,
            origin_id=origin.origin_id,
            origin_session_id=origin.origin_session_id,
            origin_ui_session_id=origin.origin_ui_session_id,
            origin_profile=origin.origin_profile,
            maximum_ttl_seconds=request_config.max_ttl_seconds,
            terminal_retention_seconds=request_config.terminal_retention_seconds,
        )
    except (DurableRequestError, DurableRequestLimitError, ValueError):
        return {"status": "pending_manual_review", "reason": "review_too_large"}
    except (OSError, sqlite3.Error):
        return {"status": "pending_manual_review", "reason": "inbox_unavailable"}
    if result.ticket.status.value != "waiting_for_decision":
        try:
            _write_pending_record({**record, "status": result.ticket.status.value})
        except OSError:
            pass
        return {
            "status": result.ticket.status.value,
            "pending_id": record["id"],
            "request_id": result.request.request_id,
            "action_id": result.request.request_id,
        }
    linked = {**record, "durable_request_id": result.request.request_id,
              "durable_action_id": result.request.request_id}
    try:
        _write_pending_record(linked)
    except OSError:
        logger.warning("Durable memory advisory link update failed")
    return {
        "status": "awaiting_approval",
        "pending_id": record["id"],
        "request_id": result.request.request_id,
        "action_id": result.request.request_id,
    }


def linked_memory_request_id(record: Dict[str, Any]) -> Optional[str]:
    """Resolve and verify the authoritative ticket for a pending record."""
    prepared = prepare_durable_memory_pending(record)
    db_path = get_hermes_home() / "state.db"
    if prepared is None or not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """SELECT request_id, payload_json, fingerprint, idempotency_key
                   FROM durable_action_tickets
                   WHERE action_type = 'memory.apply_pending' AND action_version = 1"""
            ).fetchall()
    except sqlite3.Error:
        return None
    matches = [row[0] for row in rows if row[1:] == (
        prepared.payload_json, prepared.fingerprint, prepared.idempotency_key
    )]
    return matches[0] if len(matches) == 1 else None


def memory_action_exists(pending_id: str) -> bool:
    """Return whether SQLite has any memory ticket for this strict pending ID."""
    return memory_action_receipt(pending_id) is not None


def memory_action_receipt(pending_id: str) -> Optional[tuple[str, str]]:
    """Return the unique authoritative request ID and structural status."""
    from gateway.durable_actions import decode_durable_action_payload

    if _PENDING_ID_RE.fullmatch(pending_id or "") is None:
        return None
    db_path = get_hermes_home() / "state.db"
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            # ponytail: bounded scan avoids a schema migration; add a subject
            # index only if pending-action volume makes this measurable.
            rows = conn.execute(
                """SELECT request_id, status, payload_json FROM durable_action_tickets
                   WHERE action_type = 'memory.apply_pending' AND action_version = 1"""
            ).fetchall()
    except sqlite3.Error:
        return None
    matches = []
    for request_id, status, payload_json in rows:
        try:
            if decode_durable_action_payload(payload_json) == {"pending_id": pending_id}:
                matches.append((request_id, status))
        except (TypeError, ValueError):
            continue
    return matches[0] if len(matches) == 1 else None


def _digest_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes() if path.exists() else b"").hexdigest()


def execute_durable_memory_pending(context):
    """Revalidate and apply one staged memory record through MemoryStore semantics."""
    from gateway.durable_actions import (
        DurableActionOutcome, DurableActionStatus, decode_durable_action_payload,
    )
    from hermes_cli.config import load_config
    from tools.memory_tool import load_on_disk_store

    failed = lambda status, reason: DurableActionOutcome(status, reason=reason)
    try:
        payload = decode_durable_action_payload(context.payload_json)
    except (TypeError, ValueError):
        return failed(DurableActionStatus.FAILED, "invalid_payload")
    if (
        context.action_type != "memory.apply_pending" or context.action_version != 1
        or not isinstance(payload, dict) or set(payload) != {"pending_id"}
        or not isinstance(payload["pending_id"], str)
        or _PENDING_ID_RE.fullmatch(payload["pending_id"]) is None
    ):
        return failed(DurableActionStatus.FAILED, "invalid_payload")
    config = load_config() or {}
    policy = config.get("durable_approvals", {})
    memory_config = config.get("memory", {})
    allowed = policy.get("allowed_actions") if isinstance(policy, dict) else None
    if (
        not isinstance(memory_config, dict) or memory_config.get("write_approval") is not True
        or memory_config.get("durable_approval") is not True
        or not isinstance(policy, dict) or policy.get("enabled") is not True
        or not isinstance(allowed, list) or "memory.apply_pending" not in allowed
    ):
        return failed(DurableActionStatus.INVALIDATED, "permission_revoked")
    record = get_pending(MEMORY, payload["pending_id"])
    if record is None:
        return failed(DurableActionStatus.INVALIDATED, "record_missing")
    prepared = prepare_durable_memory_pending(record)
    if prepared is None or prepared.fingerprint != context.fingerprint:
        return failed(DurableActionStatus.INVALIDATED, "record_changed")
    if record.get("profile_identity") != str(get_hermes_home().resolve()):
        return failed(DurableActionStatus.INVALIDATED, "profile_changed")
    store = load_on_disk_store()
    target = record.get("logical_target")
    if target not in {"memory", "user"} or record.get("target_path") != ("USER.md" if target == "user" else "MEMORY.md"):
        return failed(DurableActionStatus.INVALIDATED, "target_invalid")
    path = store._path_for(target)
    with store._file_lock(path):
        current = _digest_path(path)
        if current == record.get("desired_digest"):
            if record.get("status") != "applied":
                _write_pending_record({**record, "status": "applied"})
            return failed(DurableActionStatus.ALREADY_APPLIED, "desired_present")
        if current != record.get("baseline_digest"):
            return failed(DurableActionStatus.INVALIDATED, "baseline_changed")
        result = store.apply_batch(target, _memory_operations(record["payload"]))
        if not result.get("success"):
            return failed(DurableActionStatus.FAILED, "apply_failed")
        if _digest_path(path) != record.get("desired_digest"):
            return failed(DurableActionStatus.UNKNOWN, "result_unverified")
    _write_pending_record({**record, "status": "applied"})
    return DurableActionOutcome(DurableActionStatus.SUCCEEDED, result_json='{"applied":true}')


def pending_count(subsystem: str) -> int:
    """Cheap count of pending records (for notification badges)."""
    return len(list_pending(subsystem))


# ---------------------------------------------------------------------------
# Write origin
# ---------------------------------------------------------------------------

def current_origin() -> str:
    """Return the active write origin: ``foreground`` or ``background_review``.

    Reuses the skill-provenance ContextVar, which the background review fork
    already sets (see ``agent.background_review`` /
    ``AIAgent._spawn_background_review``). Foreground agent turns leave it at
    the default ``foreground``.
    """
    try:
        from tools.skill_provenance import get_current_write_origin
        return get_current_write_origin()
    except Exception:
        return "foreground"


def is_background() -> bool:
    return current_origin() == "background_review"


# ---------------------------------------------------------------------------
# Gate decision
# ---------------------------------------------------------------------------

class GateDecision:
    """Result of evaluating the write gate for a single write attempt.

    Exactly one of the boolean flags is True:
      * ``allow``  — proceed with the real write (gate off, or an inline
        approval was granted).
      * ``blocked`` — refuse the write (the user denied an inline approval
        prompt). ``message`` explains why; surface it to the agent.
      * ``stage``  — do not write; the caller should stage the payload via
        ``stage_write`` (gate on, and no inline prompt is available — gateway,
        background review, script, or any skill write). ``message`` is the
        user-facing "staged for approval" note.
    """

    __slots__ = ("allow", "blocked", "stage", "message")

    def __init__(self, *, allow=False, blocked=False, stage=False, message=""):
        self.allow = allow
        self.blocked = blocked
        self.stage = stage
        self.message = message


def evaluate_gate(subsystem: str, *, inline_summary: str = "",
                  inline_detail: str = "") -> GateDecision:
    """Decide what to do with a pending write for ``subsystem``.

    Args:
        subsystem: ``memory`` or ``skills``.
        inline_summary: short description used as the inline approval prompt
            header (memory foreground path only).
        inline_detail: full content shown in the inline prompt (memory entries
            are small; skills never take the inline path).

    Decision matrix:
        gate off (default)                    → allow (writes flow freely)
        gate on, memory + interactive CLI     → inline approve/deny prompt
        gate on, memory + gateway/script/bg   → stage
        gate on, skills (any origin)          → stage (too big to review inline)

    Note: there is no config-driven "blocked" outcome — the gate only ever
    delays a write for approval, never silently refuses it. ``blocked`` is
    still produced when the user *actively denies* an inline prompt.
    """
    if not write_approval_enabled(subsystem):
        return GateDecision(allow=True)

    background = is_background()

    # Opting into detached durable approval always stages first. If any
    # durable prerequisite is unavailable, the staged record remains on the
    # existing manual rail instead of falling back to an inline prompt.
    if subsystem == MEMORY and durable_approval_enabled(MEMORY):
        return GateDecision(
            stage=True,
            message="Staged memory write for durable approval.",
        )

    # Skills always stage — a SKILL.md is too large to review inline, and a
    # background skill write happens in a daemon thread with no user present.
    if subsystem == SKILLS or background:
        where = "/skills pending" if subsystem == SKILLS else "/memory pending"
        return GateDecision(
            stage=True,
            message=(
                f"Staged for approval ({subsystem}.write_approval is on). "
                f"Not yet saved — review with {where}."
            ),
        )

    # Memory + foreground: if an interactive approval channel exists (a CLI
    # approval callback registered on this thread), prompt inline — entries
    # are small enough to show in full. Otherwise (gateway, script, batch,
    # no listener) stage instead of forcing a blind deny.
    if _interactive_approval_available():
        granted = _prompt_inline_memory_approval(inline_summary, inline_detail)
        if granted is True:
            return GateDecision(allow=True)
        if granted is False:
            return GateDecision(
                blocked=True,
                message="Memory write denied by user. The change was not saved.",
            )
        # granted is None → prompt failed; fall through to staging.

    return GateDecision(
        stage=True,
        message=(
            "Staged for approval (memory.write_approval is on). "
            "Not yet saved — review with /memory pending."
        ),
    )


def _interactive_approval_available() -> bool:
    """True when a foreground memory write can be approved inline.

    Inline prompting requires a per-thread approval callback registered by the
    interactive CLI (``tools.terminal_tool.set_approval_callback``). Every
    other surface stages instead:

    * **Gateway/API sessions** — the dangerous-command ``/approve`` round-trip
      lives in the pending-approval queue (``submit_pending`` +
      ``_await_gateway_decision``), which ``prompt_dangerous_approval`` never
      reaches; trying to prompt from a gateway session would hit the
      ``input()`` fallback and silently deny. Staging gives the user a real
      review affordance (``/memory pending``) instead.
    * Scripts, cron, and background threads — no user present.
    """
    try:
        from tools.terminal_tool import _get_approval_callback
        return _get_approval_callback() is not None
    except Exception:
        return False


def _prompt_inline_memory_approval(summary: str, detail: str) -> Optional[bool]:
    """Prompt the user inline to approve a memory write.

    Returns True (approved), False (denied), or None (no interactive prompt
    available / prompt failed → caller should stage instead).

    Reuses the per-thread CLI approval callback registered for dangerous
    commands (``tools.terminal_tool.set_approval_callback``). The callback is
    invoked directly — NOT via ``prompt_dangerous_approval`` — because that
    wrapper falls back to ``input()`` (deadlock-prone under prompt_toolkit,
    see #15216) and converts callback errors into a silent deny; here a
    failed prompt must stage the write instead.
    """
    try:
        from tools.terminal_tool import _get_approval_callback
    except Exception:
        return None

    callback = _get_approval_callback()
    if callback is None:
        # No interactive channel on this thread — stage rather than risk the
        # input() fallback (deadlock under prompt_toolkit, EOF-deny in tests).
        return None

    header = summary.strip() or "Save to memory?"
    body = detail.strip()
    description = f"Save to memory: {header}"
    command = body if body else header
    # Invoke the callback directly instead of via prompt_dangerous_approval:
    # that wrapper swallows callback exceptions into "deny", which would
    # silently refuse the write. Direct invocation lets a crashed prompt fall
    # back to staging (the gate only ever delays a write, never drops it).
    try:
        choice = callback(command, description, allow_permanent=False)
    except Exception as e:
        logger.error("Inline memory approval prompt failed: %s", e)
        return None

    if choice in {"once", "session"}:
        return True
    if choice == "deny":
        return False
    # Any other outcome (e.g. timeout that returns "deny" already handled) →
    # treat unknown as no-decision so we stage rather than silently drop.
    return None


# ---------------------------------------------------------------------------
# Skill-specific helpers (gist + diff for the review affordances)
# ---------------------------------------------------------------------------

def skill_gist(action: str, name: str, *, content: str = "",
               file_path: str = "", old_string: str = "",
               new_string: str = "") -> str:
    """Build a one-line human gist for a pending skill write.

    Heuristic, no model call — the gist surfaces enough to decide approve/reject
    in a chat bubble, while the full diff stays behind /skills diff (CLI/
    dashboard/file). For create/edit it pulls the frontmatter ``description:``;
    for patch/write_file it describes the size of the change.
    """
    if action in {"create", "edit"} and content:
        desc = _frontmatter_description(content)
        size = f"{len(content) // 1024 + 1} KB" if len(content) >= 1024 else f"{len(content)} chars"
        verb = "create" if action == "create" else "rewrite"
        if desc:
            return f"{verb} '{name}' — {desc} ({size})"
        return f"{verb} '{name}' ({size})"
    if action == "patch":
        target = file_path or "SKILL.md"
        removed = old_string.count("\n") + 1 if old_string else 0
        added = new_string.count("\n") + 1 if new_string else 0
        return f"patch '{name}' {target} (+{added}/-{removed} lines)"
    if action == "write_file":
        return f"write {file_path} in '{name}'"
    if action == "remove_file":
        return f"remove {file_path} from '{name}'"
    if action == "delete":
        return f"delete skill '{name}'"
    return f"{action} '{name}'"


def _frontmatter_description(content: str) -> str:
    """Extract the ``description:`` value from SKILL.md YAML frontmatter."""
    import re
    m = re.search(r"^description:\s*(.+)$", content, re.MULTILINE)
    if not m:
        return ""
    desc = m.group(1).strip().strip("'\"")
    return desc[:140]


def skill_pending_diff(record: Dict[str, Any]) -> str:
    """Build a full unified diff (or full content) for a staged skill write.

    Used by /skills diff <id> on a surface that can render it (CLI pager, web
    dashboard, or by opening the pending JSON file). For create this is the new
    file content; for edit/patch it is a unified diff against the current
    on-disk skill.
    """
    import difflib
    payload = record.get("payload", {})
    action = payload.get("action", "")
    name = payload.get("name", "")

    if action == "create":
        return (payload.get("content") or "")

    # Resolve current on-disk content for diffable actions.
    try:
        from tools.skill_manager_tool import _find_skill
    except Exception:
        _find_skill = None  # type: ignore

    current = ""
    target_label = "SKILL.md"
    if _find_skill is not None:
        found = _find_skill(name)
        if found:
            base = found["path"]
            if action == "edit":
                p = base / "SKILL.md"
            elif action in {"patch", "write_file"}:
                rel = payload.get("file_path") or "SKILL.md"
                p = base / rel
                target_label = rel
            else:
                p = base / "SKILL.md"
            try:
                if p.exists():
                    current = p.read_text(encoding="utf-8")
            except Exception:
                current = ""

    if action == "edit":
        new = payload.get("content") or ""
    elif action == "patch":
        old_s = payload.get("old_string") or ""
        new_s = payload.get("new_string") or ""
        new = current.replace(old_s, new_s) if current else f"(patch {old_s!r} → {new_s!r})"
    elif action == "write_file":
        new = payload.get("file_content") or ""
    elif action == "remove_file":
        return f"remove file: {payload.get('file_path')} from skill '{name}'"
    elif action == "delete":
        return f"delete skill '{name}'"
    else:
        return f"({action} on '{name}')"

    diff = difflib.unified_diff(
        current.splitlines(keepends=True),
        new.splitlines(keepends=True),
        fromfile=f"a/{target_label}",
        tofile=f"b/{target_label}",
    )
    text = "".join(diff)
    return text or "(no textual change)"
