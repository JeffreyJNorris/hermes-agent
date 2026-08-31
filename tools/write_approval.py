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

import hashlib
import difflib
import json
import logging
import os
import re
import shutil
import sqlite3
import stat
import tempfile
import time
import uuid
from contextlib import contextmanager
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
_SKILL_MANIFEST_VERSION = 1
_SKILL_FINGERPRINT_VERSION = 1
MAX_DURABLE_SKILL_FILES = 64
MAX_DURABLE_SKILL_FILE_BYTES = 1_048_576
MAX_DURABLE_SKILL_TOTAL_BYTES = 2_097_152
MAX_DURABLE_SKILL_DIFF_CHARS = 80_000
_PENDING_ID_RE = re.compile(r"^[0-9a-f]{8}$")
_TERMINAL_MEMORY_ACTION_STATUSES = frozenset({
    "declined", "expired", "cancelled", "abandoned", "invalidated", "failed",
    "handler_unavailable", "succeeded", "already_applied",
})


class MemoryActionLookupUnavailable(RuntimeError):
    """The durable action ledger could not be checked safely."""


class SkillActionLookupUnavailable(RuntimeError):
    """The durable skill action ledger could not be checked safely."""


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


def _secure_dir_fd_supported() -> bool:
    required = (os.open, os.stat, os.unlink, os.mkdir)
    supports_dir_fd = getattr(os, "supports_dir_fd", ())
    return (
        os.name == "posix"
        and hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
        and hasattr(os, "fchmod")
        and all(function in supports_dir_fd for function in required)
    )


def _write_pending_record(record: Dict[str, Any]) -> None:
    subsystem = record.get("subsystem")
    pending_id = record.get("id")
    if (
        subsystem not in _SUBSYSTEMS
        or not isinstance(pending_id, str)
        or _PENDING_ID_RE.fullmatch(pending_id) is None
    ):
        raise ValueError("invalid pending record identity")

    root = get_hermes_home() / "pending"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not _secure_dir_fd_supported():
        _write_pending_record_portable(root, subsystem, pending_id, record)
        return

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root_fd = os.open(root, directory_flags)
    try:
        os.fchmod(root_fd, 0o700)
        try:
            os.mkdir(subsystem, mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        directory_fd = os.open(subsystem, directory_flags, dir_fd=root_fd)
        try:
            os.fchmod(directory_fd, 0o700)
            filename = f"{pending_id}.json"
            try:
                metadata = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                if not stat.S_ISREG(metadata.st_mode):
                    raise OSError("pending record target is not a regular file")

            temporary = f".{pending_id}_{uuid.uuid4().hex}.tmp"
            file_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
            fd = os.open(temporary, file_flags, 0o600, dir_fd=directory_fd)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as stream:
                    json.dump(record, stream, indent=2)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(
                    temporary,
                    filename,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                )
                os.fsync(directory_fd)
            except Exception:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except FileNotFoundError:
                    pass
                raise
        finally:
            os.close(directory_fd)
    finally:
        os.close(root_fd)


def _write_pending_record_portable(
    root: Path,
    subsystem: str,
    pending_id: str,
    record: Dict[str, Any],
) -> None:
    """Best available atomic replacement where directory FDs are unsupported."""
    directory = root / subsystem
    root_metadata = root.lstat()
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise OSError("pending directory is not a regular directory")
    directory.mkdir(exist_ok=True, mode=0o700)
    metadata = directory.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise OSError("pending directory is not a regular directory")
    if directory.resolve().parent != root.resolve():
        raise OSError("pending directory escaped its root")
    if os.name == "posix":
        root.chmod(0o700)
        directory.chmod(0o700)
    path = directory / f"{pending_id}.json"
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError("pending record target is not a regular file")

    fd, temporary = tempfile.mkstemp(
        dir=str(directory), prefix=f".{pending_id}_", suffix=".tmp"
    )
    temporary_path = Path(temporary)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(record, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


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

    Returns a dict with ``id`` and metadata. Persistence failures are raised so
    callers cannot claim that a write was staged when no review record exists.
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
    _write_pending_record(record)
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
                try:
                    action = memory_action_receipt(record.get("id", ""))
                except MemoryActionLookupUnavailable:
                    action = None
                reconciled = action is not None and action[2] is not None
                if action is not None and (
                    action[1] in _TERMINAL_MEMORY_ACTION_STATUSES or reconciled
                ):
                    status = (
                        f"reconciled_{action[2]}" if reconciled else action[1]
                    )
                    record = {**record, "status": status}
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
            if subsystem == SKILLS:
                staged = _skill_pending_path(pending_id)
                if staged.exists() and not staged.is_symlink():
                    shutil.rmtree(staged)
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
            if not _sqlite_table_exists(conn, "durable_action_tickets"):
                return None
            rows = conn.execute(
                """SELECT request_id, payload_json, fingerprint, idempotency_key
                   FROM durable_action_tickets
                   WHERE action_type = 'memory.apply_pending' AND action_version = 1"""
            ).fetchall()
    except sqlite3.Error as exc:
        raise MemoryActionLookupUnavailable from exc
    matches = [row[0] for row in rows if row[1:] == (
        prepared.payload_json, prepared.fingerprint, prepared.idempotency_key
    )]
    return matches[0] if len(matches) == 1 else None


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone() is not None


def memory_action_receipt(
    pending_id: str,
) -> Optional[tuple[str, str, Optional[str]]]:
    """Return the authoritative request, status, and effective reconciliation."""
    from gateway.durable_actions import decode_durable_action_payload

    if _PENDING_ID_RE.fullmatch(pending_id or "") is None:
        return None
    db_path = get_hermes_home() / "state.db"
    if not db_path.exists():
        return None
    try:
        with sqlite3.connect(db_path) as conn:
            if not _sqlite_table_exists(conn, "durable_action_tickets"):
                return None
            # ponytail: bounded scan avoids a schema migration; add a subject
            # index only if pending-action volume makes this measurable.
            if _sqlite_table_exists(conn, "durable_action_reconciliations"):
                rows = conn.execute(
                    """SELECT ticket.request_id, ticket.status, ticket.payload_json,
                              reconciliation.outcome
                       FROM durable_action_tickets AS ticket
                       LEFT JOIN durable_action_reconciliations AS reconciliation
                         ON reconciliation.request_id = ticket.request_id
                       WHERE ticket.action_type = 'memory.apply_pending'
                         AND ticket.action_version = 1"""
                ).fetchall()
            else:
                rows = [(*row, None) for row in conn.execute(
                    """SELECT request_id, status, payload_json
                       FROM durable_action_tickets
                       WHERE action_type = 'memory.apply_pending'
                         AND action_version = 1"""
                ).fetchall()]
    except sqlite3.Error as exc:
        raise MemoryActionLookupUnavailable from exc
    matches = []
    for request_id, status, payload_json, reconciliation in rows:
        try:
            if decode_durable_action_payload(payload_json) == {"pending_id": pending_id}:
                matches.append((request_id, status, reconciliation))
        except (TypeError, ValueError):
            continue
    return matches[0] if len(matches) == 1 else None


def _digest_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes() if path.exists() else b"").hexdigest()


def diagnose_durable_memory_pending(context):
    """Inspect an UNKNOWN memory action without mutating recovery evidence."""
    from gateway.durable_actions import (
        DurableActionDiagnostic,
        decode_durable_action_payload,
    )
    from tools.memory_tool import load_on_disk_store

    try:
        payload = decode_durable_action_payload(context.payload_json)
    except (TypeError, ValueError):
        return DurableActionDiagnostic("invalid_payload")
    if (
        context.action_type != "memory.apply_pending"
        or context.action_version != 1
        or not isinstance(payload, dict)
        or set(payload) != {"pending_id"}
        or not isinstance(payload["pending_id"], str)
        or _PENDING_ID_RE.fullmatch(payload["pending_id"]) is None
    ):
        return DurableActionDiagnostic("invalid_payload")
    record = get_pending(MEMORY, payload["pending_id"])
    if record is None:
        return DurableActionDiagnostic("record_missing")
    prepared = prepare_durable_memory_pending(record)
    if prepared is None or prepared.fingerprint != context.fingerprint:
        return DurableActionDiagnostic("record_changed")
    if record.get("profile_identity") != str(get_hermes_home().resolve()):
        return DurableActionDiagnostic("profile_changed")
    target = record.get("logical_target")
    if (
        target not in {"memory", "user"}
        or record.get("target_path") != ("USER.md" if target == "user" else "MEMORY.md")
    ):
        return DurableActionDiagnostic("target_invalid")
    store = load_on_disk_store()
    path = store._path_for(target)
    with store._file_lock(path):
        current = _digest_path(path)
    state = (
        "desired"
        if current == record.get("desired_digest")
        else "baseline"
        if current == record.get("baseline_digest")
        else "changed"
    )
    code = {
        "desired": "desired_present",
        "baseline": "baseline_present",
        "changed": "state_changed",
    }[state]
    return DurableActionDiagnostic(code, (("target_state", state),))


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
    if record.get("manifest_version") == _SKILL_MANIFEST_VERSION:
        return _durable_skill_review(record) or "(durable review unavailable)"

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


# ---------------------------------------------------------------------------
# Durable skill manifests
# ---------------------------------------------------------------------------

def _skill_digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _skill_manifest_fingerprint(record: Dict[str, Any]) -> str:
    canonical = {key: record.get(key) for key in (
        "manifest_version", "fingerprint_version", "id", "profile_identity",
        "root_identity", "skill_name", "operations",
    )}
    return _skill_digest(json.dumps(canonical, ensure_ascii=False, separators=(",", ":"),
                                    sort_keys=True).encode("utf-8"))


def _skill_pending_path(pending_id: str) -> Path:
    return _pending_dir(SKILLS) / pending_id


def _skill_lstat(path: Path) -> tuple[bool, str, Optional[str]]:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False, "missing", None
    if stat.S_ISLNK(metadata.st_mode):
        return True, "symlink", None
    if stat.S_ISREG(metadata.st_mode):
        return True, "file", _skill_digest(path.read_bytes())
    if stat.S_ISDIR(metadata.st_mode):
        digest = hashlib.sha256()
        try:
            for child in sorted(path.rglob("*")):
                if child.is_symlink():
                    return True, "symlink", None
                if child.is_file():
                    digest.update(child.relative_to(path).as_posix().encode())
                    digest.update(b"\0")
                    digest.update(child.read_bytes())
                    digest.update(b"\0")
        except OSError:
            return True, "other", None
        return True, "directory", digest.hexdigest()
    return True, "other", None


def _skill_components_safe(root: Path, relative: Path, *, allow_missing=True) -> bool:
    current = root
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return allow_missing
        except (NotADirectoryError, OSError):
            return False
        if stat.S_ISLNK(mode) or (hasattr(current, "is_junction") and current.is_junction()):
            return False
        if current != root / relative and not stat.S_ISDIR(mode):
            return False
    return True


def _skill_relative_path(payload: Dict[str, Any], skill_dir: Path) -> Optional[Path]:
    from tools import skill_manager_tool as sm
    action = payload.get("action")
    if action in {"create", "edit"}:
        return skill_dir.relative_to(get_hermes_home() / "skills") / "SKILL.md"
    if action == "patch":
        raw = payload.get("file_path") or "SKILL.md"
        if sm._validate_file_path(raw):
            return None
        return skill_dir.relative_to(get_hermes_home() / "skills") / Path(raw)
    if action in {"write_file", "remove_file"}:
        raw = payload.get("file_path")
        if not isinstance(raw, str) or not raw or "\\" in raw:
            return None
        path = Path(raw)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            return None
        return skill_dir.relative_to(get_hermes_home() / "skills") / path
    if action == "delete":
        return skill_dir.relative_to(get_hermes_home() / "skills")
    return None


def _skill_manual(payload: Dict[str, Any], summary: str, origin: str, reason: str):
    record = stage_write(SKILLS, payload, summary=summary, origin=origin)
    return {**record, "status": "pending_manual_review", "reason": reason}


def _skill_rejected(reason: str) -> Dict[str, Any]:
    return {"status": "rejected", "reason": reason}


def stage_skill_write(payload: Dict[str, Any], *, summary: str, origin: str) -> Dict[str, Any]:
    """Stage exact skill bytes; unsupported shapes remain legacy-manual."""
    from tools import skill_manager_tool as sm

    action, name = payload.get("action"), payload.get("name")
    if action not in {"create", "edit", "patch", "write_file", "remove_file", "delete"}:
        return _skill_manual(payload, summary, origin, "unsupported_operation")
    if not isinstance(name, str) or sm._validate_name(name):
        return _skill_manual(payload, summary, origin, "invalid_target")
    root = get_hermes_home() / "skills"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink():
        return _skill_manual(payload, summary, origin, "unsafe_root")
    existing = sm._find_skill(name)
    if action == "create":
        if existing:
            return _skill_manual(payload, summary, origin, "target_exists")
        category = payload.get("category")
        if sm._validate_category(category):
            return _skill_manual(payload, summary, origin, "invalid_target")
        skill_dir = sm._resolve_skill_dir(name, category)
    else:
        if not existing:
            return _skill_manual(payload, summary, origin, "target_missing")
        skill_dir = existing["path"]
    try:
        skill_rel = skill_dir.relative_to(root)
    except ValueError:
        return _skill_manual(payload, summary, origin, "non_personal_root")
    if not skill_rel.parts or skill_rel.parts[0] in {"_org", ".stock", ".archive"}:
        return _skill_rejected("protected_target")
    if existing:
        try:
            from tools import skill_usage
            if (skill_usage.is_protected_builtin(name) or skill_usage.is_bundled(name)
                    or skill_usage.is_hub_installed(name)):
                return _skill_rejected("protected_target")
        except Exception:
            return _skill_rejected("provenance_unavailable")
    if action == "delete":
        pinned = sm._pinned_guard(name)
        if pinned or sm._org_mirror_write_guard(name, skill_dir, "delete"):
            return _skill_rejected("protected_target")
    if action == "create" and skill_dir.exists():
        return _skill_rejected("target_exists")
    relative = _skill_relative_path(payload, skill_dir)
    if relative is None or not _skill_components_safe(root, relative):
        return _skill_manual(payload, summary, origin, "unsafe_path")
    target = root / relative
    exists, kind, baseline_digest = _skill_lstat(target)

    desired: Optional[bytes] = None
    if action in {"create", "edit"}:
        content = payload.get("content")
        if not isinstance(content, str) or sm._validate_frontmatter(content, new_skill=action == "create"):
            return _skill_manual(payload, summary, origin, "invalid_content")
        desired = content.encode("utf-8")
    elif action == "patch":
        old, new = payload.get("old_string"), payload.get("new_string")
        if kind != "file" or not isinstance(old, str) or not old or not isinstance(new, str):
            return _skill_manual(payload, summary, origin, "invalid_patch")
        try:
            current = target.read_text("utf-8")
        except (UnicodeError, OSError):
            return _skill_manual(payload, summary, origin, "non_text")
        count = current.count(old)
        if not count or (count > 1 and payload.get("replace_all") is not True):
            return _skill_manual(payload, summary, origin, "ambiguous_patch")
        desired = current.replace(old, new, -1 if payload.get("replace_all") else 1).encode()
    elif action == "write_file":
        content = payload.get("file_content")
        if not isinstance(content, str):
            return _skill_manual(payload, summary, origin, "invalid_content")
        desired = content.encode("utf-8")
    elif action == "remove_file" and kind != "file":
        return _skill_manual(payload, summary, origin, "target_missing")
    elif action == "delete" and kind != "directory":
        return _skill_manual(payload, summary, origin, "target_missing")
    if desired is not None and len(desired) > MAX_DURABLE_SKILL_FILE_BYTES:
        return _skill_manual(payload, summary, origin, "review_too_large")
    if desired is not None and relative.name == "SKILL.md":
        try: text = desired.decode("utf-8")
        except UnicodeDecodeError: return _skill_manual(payload, summary, origin, "non_text")
        if sm._validate_frontmatter(text, new_skill=action == "create") or sm._validate_content_size(text):
            return _skill_manual(payload, summary, origin, "invalid_content")
    pending_id = uuid.uuid4().hex[:8]
    directory = _skill_pending_path(pending_id)
    staged_dir = directory / "staged"
    baseline_dir = directory / "baseline"
    staged_dir.mkdir(parents=True, mode=0o700)
    baseline_dir.mkdir(mode=0o700)
    if os.name == "posix":
        directory.chmod(0o700); staged_dir.chmod(0o700)
    operation = {
        "operation": action, "relative_path": relative.as_posix(),
        "baseline_exists": exists, "baseline_type": kind,
        "baseline_digest": baseline_digest, "staged_digest": _skill_digest(desired) if desired is not None else None,
        "staged_bytes": len(desired) if desired is not None else 0, "expected_mode": 0o600,
    }
    if desired is not None:
        fd = os.open(staged_dir / "0", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(desired); stream.flush(); os.fsync(stream.fileno())
    if kind == "file":
        fd = os.open(baseline_dir / "0", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(target.read_bytes()); stream.flush(); os.fsync(stream.fileno())
    record = {
        "id": pending_id, "subsystem": SKILLS, "status": "pending", "summary": summary,
        "origin": origin, "created_at": time.time(), "manifest_version": _SKILL_MANIFEST_VERSION,
        "fingerprint_version": _SKILL_FINGERPRINT_VERSION,
        "profile_identity": str(get_hermes_home().resolve()), "root_identity": str(root.resolve()),
        "skill_name": name, "operations": [operation], "payload": {"pending_id": pending_id},
    }
    record["subject_fingerprint"] = _skill_manifest_fingerprint(record)
    _write_pending_record(record)
    return record


def prepare_durable_skill_pending(record: Dict[str, Any]):
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload
    pending_id = record.get("id")
    if (record.get("manifest_version") != 1 or record.get("fingerprint_version") != 1
            or not isinstance(pending_id, str) or not _PENDING_ID_RE.fullmatch(pending_id)
            or record.get("subject_fingerprint") != _skill_manifest_fingerprint(record)):
        return None
    fingerprint = record["subject_fingerprint"]
    return PreparedDurableAction("skill.apply_pending", 1,
        encode_durable_action_payload({"pending_id": pending_id}), fingerprint,
        f"skill.apply_pending:{pending_id}:{fingerprint}")


def _durable_skill_review(record: Dict[str, Any]) -> Optional[str]:
    operation = record["operations"][0]
    relative = operation["relative_path"]
    action = operation["operation"]
    before = b""
    if operation["baseline_type"] == "file":
        try: before = (_skill_pending_path(record["id"]) / "baseline" / "0").read_bytes()
        except OSError: return None
    after = b""
    if operation["staged_digest"] is not None:
        try: after = (_skill_pending_path(record["id"]) / "staged" / "0").read_bytes()
        except OSError: return None
    try:
        old = before.decode("utf-8"); new = after.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if action == "delete":
        files = []
        try:
            target = get_hermes_home() / "skills" / relative
            for path in sorted(target.rglob("*")):
                if path.is_symlink() or not path.is_file():
                    return None
                data = path.read_bytes()
                if len(data) > MAX_DURABLE_SKILL_FILE_BYTES:
                    return None
                files.append((path.relative_to(root).as_posix(), data.decode("utf-8")))
        except (OSError, UnicodeDecodeError):
            return None
        if len(files) > MAX_DURABLE_SKILL_FILES or sum(len(v.encode()) for _, v in files) > MAX_DURABLE_SKILL_TOTAL_BYTES:
            return None
        chunks = []
        for label, content in files:
            chunks.append(f"# delete {label}\n" + "".join(difflib.unified_diff(
                content.splitlines(keepends=True), [], fromfile=f"a/{label}", tofile="/dev/null")))
        rendered = "".join(chunks)
    else:
        rendered = f"# {action} {relative}\n" + "".join(difflib.unified_diff(
            old.splitlines(keepends=True), new.splitlines(keepends=True),
            fromfile=f"a/{relative}" if before else "/dev/null", tofile=f"b/{relative}" if after else "/dev/null"))
    return rendered if len(rendered) <= MAX_DURABLE_SKILL_DIFF_CHARS else None


def create_skill_durable_approval(record: Dict[str, Any]) -> Dict[str, Any]:
    from gateway.durable_requests import (
        DurableRequestError, DurableRequestLimitError, DurableRequestOption,
        DurableRequestSpec, get_durable_request_store, load_durable_requests_config,
    )
    from hermes_cli.config import load_config_readonly
    stored = get_pending(SKILLS, str(record.get("id", "")))
    prepared = prepare_durable_skill_pending(stored or {})
    review = _durable_skill_review(stored) if stored and prepared else None
    if prepared is None or review is None:
        return {"status": "pending_manual_review", "reason": "invalid_or_oversize_review"}
    try:
        config = load_config_readonly(); policy = config.get("durable_approvals", {})
        if not isinstance(policy, dict) or policy.get("enabled") is not True:
            return {"status": "pending_manual_review", "reason": "durable_disabled"}
        if "skill.apply_pending" not in policy.get("allowed_actions", []):
            return {"status": "pending_manual_review", "reason": "handler_not_allowed"}
        request_config = load_durable_requests_config()
        if not request_config.available or request_config.target is None:
            return {"status": "pending_manual_review", "reason": "inbox_unavailable"}
        from tools.durable_request_tool import _current_origin
        origin = _current_origin(request_config.scheduled_responder_mode)
        result = get_durable_request_store().create_with_action(
            DurableRequestSpec(
                kind="tool.durable_approval.skill.apply_pending",
                title=f"Apply staged skill change to {stored['skill_name']}?",
                body=review,
                options=(DurableRequestOption(id="approve", label="Approve"),
                         DurableRequestOption(id="reject", label="Reject")),
                ttl_seconds=request_config.default_ttl_seconds,
                reference=f"skill.apply_pending:{stored['id']}",
            ), action=prepared, target=request_config.target,
            responder_mode=origin.responder_mode, expected_actor_id=origin.expected_actor_id,
            origin_type=origin.origin_type, origin_id=origin.origin_id,
            origin_session_id=origin.origin_session_id, origin_ui_session_id=origin.origin_ui_session_id,
            origin_profile=origin.origin_profile, maximum_ttl_seconds=request_config.max_ttl_seconds,
            terminal_retention_seconds=request_config.terminal_retention_seconds,
        )
    except (DurableRequestError, DurableRequestLimitError, ValueError):
        return {"status": "pending_manual_review", "reason": "review_too_large"}
    except (OSError, sqlite3.Error):
        return {"status": "pending_manual_review", "reason": "inbox_unavailable"}
    linked = {**stored, "durable_request_id": result.request.request_id,
              "durable_action_id": result.request.request_id}
    try: _write_pending_record(linked)
    except OSError: logger.warning("Durable skill advisory link update failed")
    status = result.ticket.status.value
    return {"status": "awaiting_approval" if status == "waiting_for_decision" else status,
            "pending_id": stored["id"],
            "request_id": result.request.request_id, "action_id": result.request.request_id}


def linked_skill_request_id(record: Dict[str, Any]) -> Optional[str]:
    prepared = prepare_durable_skill_pending(record)
    db_path = get_hermes_home() / "state.db"
    if prepared is None or not db_path.exists(): return None
    try:
        with sqlite3.connect(db_path) as conn:
            if not _sqlite_table_exists(conn, "durable_action_tickets"): return None
            rows = conn.execute("""SELECT request_id,payload_json,fingerprint,idempotency_key
                FROM durable_action_tickets WHERE action_type='skill.apply_pending' AND action_version=1""").fetchall()
    except sqlite3.Error as exc:
        raise SkillActionLookupUnavailable from exc
    matches = [row[0] for row in rows if row[1:] == (prepared.payload_json, prepared.fingerprint, prepared.idempotency_key)]
    return matches[0] if len(matches) == 1 else None


def skill_action_receipt(pending_id: str) -> Optional[tuple[str, str, Optional[str]]]:
    from gateway.durable_actions import decode_durable_action_payload
    if not _PENDING_ID_RE.fullmatch(pending_id or ""): return None
    db_path = get_hermes_home() / "state.db"
    if not db_path.exists(): return None
    try:
        with sqlite3.connect(db_path) as conn:
            if not _sqlite_table_exists(conn, "durable_action_tickets"): return None
            if _sqlite_table_exists(conn, "durable_action_reconciliations"):
                rows = conn.execute("""SELECT t.request_id,t.status,t.payload_json,r.outcome
                    FROM durable_action_tickets t LEFT JOIN durable_action_reconciliations r
                    ON r.request_id=t.request_id WHERE t.action_type='skill.apply_pending' AND t.action_version=1""").fetchall()
            else:
                rows = [(*row, None) for row in conn.execute("""SELECT request_id,status,payload_json
                    FROM durable_action_tickets WHERE action_type='skill.apply_pending' AND action_version=1""").fetchall()]
    except sqlite3.Error as exc:
        raise SkillActionLookupUnavailable from exc
    matches = []
    for request_id, status, encoded, reconciliation in rows:
        try:
            if decode_durable_action_payload(encoded) == {"pending_id": pending_id}:
                matches.append((request_id, status, reconciliation))
        except (TypeError, ValueError): pass
    return matches[0] if len(matches) == 1 else None


@contextmanager
def _skill_write_lock(root: Path):
    lock = root / ".skill-write.lock"
    handle = open(lock, "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            if lock.stat().st_size == 0: handle.write(b"0"); handle.flush()
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        if os.name == "nt":
            import msvcrt
            handle.seek(0); msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def skill_write_lock():
    """One profile-local lock shared by direct, manual, and durable writers."""
    root = get_hermes_home() / "skills"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    return _skill_write_lock(root)


def _atomic_skill_write(target: Path, data: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=target.parent, prefix=".skill-", suffix=".tmp")
    path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.replace(path, target)
        _fsync_dir(target.parent)
        if os.name == "posix": target.chmod(0o600)
    except BaseException:
        path.unlink(missing_ok=True); raise


def _fsync_dir(directory: Path) -> None:
    if os.name != "posix":
        return
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def execute_durable_skill_pending(context):
    from gateway.durable_actions import DurableActionOutcome, DurableActionStatus, decode_durable_action_payload
    from hermes_cli.config import load_config
    failed = lambda status, reason: DurableActionOutcome(status, reason=reason)
    try: payload = decode_durable_action_payload(context.payload_json)
    except (TypeError, ValueError): return failed(DurableActionStatus.FAILED, "invalid_payload")
    if (context.action_type != "skill.apply_pending" or context.action_version != 1
            or not isinstance(payload, dict) or set(payload) != {"pending_id"}
            or not isinstance(payload["pending_id"], str)
            or _PENDING_ID_RE.fullmatch(payload["pending_id"]) is None):
        return failed(DurableActionStatus.FAILED, "invalid_payload")
    config = load_config() or {}; skills = config.get("skills", {}); policy = config.get("durable_approvals", {})
    allowed = policy.get("allowed_actions") if isinstance(policy, dict) else None
    if (not isinstance(skills, dict) or skills.get("write_approval") is not True
            or skills.get("durable_approval") is not True or not isinstance(policy, dict)
            or policy.get("enabled") is not True or not isinstance(allowed, list)
            or "skill.apply_pending" not in allowed):
        return failed(DurableActionStatus.INVALIDATED, "permission_revoked")
    record = get_pending(SKILLS, payload["pending_id"])
    pending_directory = _skill_pending_path(payload["pending_id"])
    manifest_path = _pending_dir(SKILLS) / f"{payload['pending_id']}.json"
    if (pending_directory.is_symlink() or manifest_path.is_symlink()
            or not manifest_path.is_file()):
        return failed(DurableActionStatus.INVALIDATED, "record_changed")
    prepared = prepare_durable_skill_pending(record or {})
    if prepared is None or prepared.fingerprint != context.fingerprint:
        return failed(DurableActionStatus.INVALIDATED, "record_changed")
    root = get_hermes_home() / "skills"
    if root.is_symlink():
        return failed(DurableActionStatus.INVALIDATED, "profile_changed")
    if record.get("profile_identity") != str(get_hermes_home().resolve()) or record.get("root_identity") != str(root.resolve()):
        return failed(DurableActionStatus.INVALIDATED, "profile_changed")
    operation = record["operations"][0]; relative = Path(operation["relative_path"])
    if relative.is_absolute() or ".." in relative.parts or not _skill_components_safe(root, relative):
        return failed(DurableActionStatus.INVALIDATED, "target_invalid")
    target = root / relative
    staged = _skill_pending_path(record["id"]) / "staged" / "0"
    desired = None
    if operation["staged_digest"] is not None:
        _, current_kind, current_digest = _skill_lstat(target)
        if current_kind == "file" and current_digest == operation["staged_digest"]:
            return failed(DurableActionStatus.ALREADY_APPLIED, "desired_present")
        try:
            if not stat.S_ISREG(staged.lstat().st_mode):
                return failed(DurableActionStatus.INVALIDATED, "staged_changed")
            desired = staged.read_bytes()
        except OSError: return failed(DurableActionStatus.INVALIDATED, "staged_missing")
        if _skill_digest(desired) != operation["staged_digest"]:
            return failed(DurableActionStatus.INVALIDATED, "staged_changed")
    with _skill_write_lock(root):
        if not _skill_components_safe(root, relative):
            return failed(DurableActionStatus.INVALIDATED, "target_invalid")
        exists, kind, digest = _skill_lstat(target)
        if desired is not None and kind == "file" and digest == operation["staged_digest"]:
            return failed(DurableActionStatus.ALREADY_APPLIED, "desired_present")
        if operation["operation"] in {"remove_file", "delete"} and not exists:
            return failed(DurableActionStatus.ALREADY_APPLIED, "target_absent")
        if (exists != operation["baseline_exists"] or kind != operation["baseline_type"]
                or digest != operation["baseline_digest"]):
            return failed(DurableActionStatus.INVALIDATED, "baseline_changed")
        try:
            if operation["operation"] in {"delete", "remove_file"}:
                txn = root / ".transactions" / record["id"]
                quarantine = root / ".quarantine" / record["id"]
                for private_root in (txn.parent, quarantine.parent):
                    private_root.mkdir(parents=True, exist_ok=True, mode=0o700)
                    mode = private_root.lstat().st_mode
                    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                        raise OSError("unsafe transaction directory")
                    _fsync_dir(private_root.parent)
                if quarantine.exists() or quarantine.is_symlink():
                    raise OSError("quarantine collision")
                txn.mkdir(exist_ok=True, mode=0o700); _fsync_dir(txn.parent)
                journal = {"journal_version": 1, "state": "PREPARED", "operation_progress": 0,
                           "target_relative_path": relative.as_posix(),
                           "quarantine_relative_path": str(quarantine.relative_to(root))}
                atomic = txn / "journal.json"
                atomic_write = lambda: _atomic_skill_write(atomic, json.dumps(journal, sort_keys=True).encode())
                atomic_write(); journal["state"] = "EXECUTING"; atomic_write()
                os.replace(target, quarantine); journal["operation_progress"] = 1
                _fsync_dir(target.parent); _fsync_dir(quarantine.parent)
                journal["state"] = "COMMITTED"; atomic_write()
            else:
                assert desired is not None
                if operation["operation"] == "create":
                    temporary_dir = target.parent.with_name(f".{target.parent.name}-{record['id']}.tmp")
                    temporary_dir.mkdir(parents=True, exist_ok=False)
                    _atomic_skill_write(temporary_dir / "SKILL.md", desired)
                    os.replace(temporary_dir, target.parent)
                    _fsync_dir(target.parent.parent)
                else:
                    _atomic_skill_write(target, desired)
        except Exception:
            return failed(DurableActionStatus.UNKNOWN, "mutation_unverified")
    _write_pending_record({**record, "status": "applied"})
    staged_directory = _skill_pending_path(record["id"])
    if staged_directory.exists() and not staged_directory.is_symlink():
        shutil.rmtree(staged_directory)
    try:
        from agent.prompt_builder import clear_skills_system_prompt_cache
        clear_skills_system_prompt_cache(clear_snapshot=True)
        from tools.skill_manager_tool import _maybe_debounced_sync_push
        _maybe_debounced_sync_push(record["skill_name"])
    except Exception:
        pass
    return DurableActionOutcome(DurableActionStatus.SUCCEEDED, result_json='{"applied":true}')


def diagnose_durable_skill_pending(context):
    from gateway.durable_actions import DurableActionDiagnostic, decode_durable_action_payload
    try: payload = decode_durable_action_payload(context.payload_json)
    except (TypeError, ValueError): return DurableActionDiagnostic("invalid_payload")
    if not isinstance(payload, dict) or set(payload) != {"pending_id"}:
        return DurableActionDiagnostic("invalid_payload")
    record = get_pending(SKILLS, payload["pending_id"])
    prepared = prepare_durable_skill_pending(record or {})
    if prepared is None or prepared.fingerprint != context.fingerprint:
        return DurableActionDiagnostic("record_changed")
    root = get_hermes_home() / "skills"
    operation = record["operations"][0]
    target = root / operation["relative_path"]
    journal_path = root / ".transactions" / record["id"] / "journal.json"
    if journal_path.exists():
        try: state = json.loads(journal_path.read_text("utf-8"))["state"].lower()
        except (OSError, KeyError, json.JSONDecodeError): return DurableActionDiagnostic("journal_invalid")
        target_state = "present" if target.exists() else "absent"
        return DurableActionDiagnostic("journal_evidence", (("journal_state", state), ("target_state", target_state)))
    exists, kind, digest = _skill_lstat(target)
    if operation["staged_digest"] and kind == "file" and digest == operation["staged_digest"]:
        state = "desired"
    elif (exists == operation["baseline_exists"] and kind == operation["baseline_type"]
          and digest == operation["baseline_digest"]):
        state = "baseline"
    else: state = "changed"
    return DurableActionDiagnostic("target_evidence", (("target_state", state),))
