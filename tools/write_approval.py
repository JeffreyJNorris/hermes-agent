#!/usr/bin/env python3
"""Write-approval gate + pending store for memory and skill writes.

A per-subsystem boolean ``write_approval`` gates the agent's cross-session writes —
**memory** (MEMORY.md / USER.md) and **skills** (SKILL.md + files) — from either
origin (**foreground** turn or **background_review** fork). ``false`` (default)
writes freely; ``true`` never commits directly: it prompts inline (memory,
interactive CLI only) or **stages** the write under
``<HERMES_HOME>/pending/{memory,skills}/<id>.json`` for out-of-band review.
"""

from __future__ import annotations

import hashlib
import difflib
import ctypes
import errno
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
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

# Subsystem identifiers
MEMORY = "memory"
SKILLS = "skills"
_SUBSYSTEMS = (MEMORY, SKILLS)

# Per-subsystem config key. Intentionally a single boolean with no "block all writes"
# state — to disable a subsystem use its own enable flag (e.g. ``memory.memory_enabled``).
CONFIG_KEY = "write_approval"
_TRUTHY_STRINGS = frozenset({"on", "true", "yes", "1", "approve", "enabled"})
DURABLE_CONFIG_KEY = "durable_approval"
_MEMORY_RECORD_VERSION = 2
_MEMORY_FINGERPRINT_VERSION = 1
_SKILL_MANIFEST_VERSION = 1
_SKILL_FINGERPRINT_VERSION = 1
MAX_DURABLE_SKILL_FILES = 64
MAX_DURABLE_SKILL_FILE_BYTES = 1_048_576
MAX_DURABLE_SKILL_TOTAL_BYTES = 2_097_152
MAX_DURABLE_SKILL_DIFF_CHARS = 80_000
MAX_SKILL_JOURNAL_CLEANUPS = 16
_PENDING_ID_RE = re.compile(r"^[0-9a-f]{8}$")
_TERMINAL_MEMORY_ACTION_STATUSES = frozenset({
    "declined", "expired", "cancelled", "abandoned", "invalidated", "failed",
    "handler_unavailable", "succeeded", "already_applied",
})


class MemoryActionLookupUnavailable(RuntimeError):
    """The durable action ledger could not be checked safely."""


class SkillActionLookupUnavailable(RuntimeError):
    """The durable skill action ledger could not be checked safely."""


# --- Config resolution ---

def write_approval_enabled(subsystem: str) -> bool:
    """Read ``<subsystem>.write_approval``; any unset/invalid value means gate off."""
    if subsystem not in _SUBSYSTEMS:
        return False
    try:
        from hermes_cli.config import load_config, cfg_get
        return _normalize_enabled(cfg_get(load_config(), subsystem, CONFIG_KEY, default=False))
    except Exception:
        return False


def durable_approval_enabled(subsystem: str) -> bool:
    if subsystem not in _SUBSYSTEMS:
        return False
    try:
        from hermes_cli.config import cfg_get, load_config

        return cfg_get(load_config(), subsystem, DURABLE_CONFIG_KEY, default=False) is True
    except Exception:
        return False


def _normalize_enabled(value: Any) -> bool:
    """Coerce a config value to bool; unknown → False (gate off). The string branch
    covers hand-edited configs (YAML already parses bare on/off/yes/no)."""
    if isinstance(value, bool):
        return value
    return isinstance(value, str) and value.strip().lower() in _TRUTHY_STRINGS


# --- Pending store (file-backed) ---

def _pending_path(subsystem: str, pending_id: str) -> Path:
    return get_hermes_home() / "pending" / subsystem / f"{pending_id}.json"


def _pending_dir(subsystem: str) -> Path:
    return get_hermes_home() / "pending" / subsystem


def _pending_files(subsystem: str) -> list[Path]:
    directory = _pending_dir(subsystem)
    return list(directory.glob("*.json")) if directory.exists() else []


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


def _plan_memory_entries(
    store, target: str, entries: List[str], operations: List[Dict[str, Any]], *, allow_empty: bool,
) -> Optional[List[str]]:
    """Preview a staged write using the current MemoryStore's validation under its file lock."""
    from tools.memory_tool import ENTRY_DELIMITER, _scan_memory_content

    working = list(entries)
    for index, operation in enumerate(operations, 1):
        action = operation.get("action")
        content = (operation.get("content") or operation.get("new_text") or "").strip()
        if action in {"add", "replace"} and _scan_memory_content(content):
            return None
        error, _ = store._apply_batch_op(
            working, action, content, (operation.get("old_text") or "").strip(),
            f"Operation {index}", operation.get("matched_entry"),
        )
        if error:
            return None
    if entries and not working and not allow_empty:
        return None
    if len(ENTRY_DELIMITER.join(working)) > store._char_limit(target):
        return None
    return working


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

    def _preview(entries, _limit):
        desired = _plan_memory_entries(
            store, target, entries, operations, allow_empty=payload.get("action") != "batch",
        )
        if desired is None:
            return {"success": False}
        return {
            "success": True,
            "desired": desired,
            "baseline": path.read_bytes() if path.exists() else b"",
        }

    preview = store._mutate(target, _preview)
    if not preview.get("success"):
        return None
    desired, baseline = preview["desired"], preview["baseline"]
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
    if subsystem == SKILLS:
        cleanup_terminal_skill_evidence()
    d = _pending_dir(subsystem)
    if not d.exists():
        return []
    records: List[Dict[str, Any]] = []
    for p in _pending_files(subsystem):
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
            elif subsystem == SKILLS and record.get("status", "pending") == "pending":
                try:
                    action = skill_action_receipt(record.get("id", ""))
                except SkillActionLookupUnavailable:
                    action = None
                if action is not None and action[1] != "unknown" and (
                        action[1] in _TERMINAL_MEMORY_ACTION_STATUSES or action[2] is not None):
                    status = f"reconciled_{action[2]}" if action[2] is not None else action[1]
                    record = {**record, "status": status}
                    staged = _skill_pending_path(record["id"])
                    if staged.exists() and not staged.is_symlink():
                        shutil.rmtree(staged)
                    try: _write_pending_record(record)
                    except OSError: pass
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
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def discard_pending(subsystem: str, pending_id: str) -> bool:
    """Delete a pending record. Returns True if it existed."""
    if not isinstance(pending_id, str) or _PENDING_ID_RE.fullmatch(pending_id) is None:
        return False
    path = _pending_dir(subsystem) / f"{pending_id}.json"
    try:
        path = _pending_path(subsystem, pending_id)
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
            command_response_allowed=(
                request_config.command_responses_enabled
                and request_config.allow_on_native_delivery
            ),
            text_command_fallback_allowed=(
                request_config.command_responses_enabled
                and request_config.allow_text_delivery_fallback
            ),
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
    operations = _memory_operations(record["payload"])

    def _apply(entries, _limit):
        current = _digest_path(path)
        if current == record.get("desired_digest"):
            return {"durable_status": "already_applied"}
        if current != record.get("baseline_digest"):
            return {"durable_status": "invalidated"}
        desired = _plan_memory_entries(
            store, target, entries, operations,
            allow_empty=record["payload"].get("action") != "batch",
        )
        if desired is None:
            return {"durable_status": "failed"}
        return desired, "Approved memory write applied."

    result = store._mutate(target, _apply)
    if result.get("durable_status") == "already_applied":
        if record.get("status") != "applied":
            _write_pending_record({**record, "status": "applied"})
        return failed(DurableActionStatus.ALREADY_APPLIED, "desired_present")
    if result.get("durable_status") == "invalidated":
        return failed(DurableActionStatus.INVALIDATED, "baseline_changed")
    if not result.get("success"):
        return failed(DurableActionStatus.FAILED, "apply_failed")
    if _digest_path(path) != record.get("desired_digest"):
        return failed(DurableActionStatus.UNKNOWN, "result_unverified")
    _write_pending_record({**record, "status": "applied"})
    return DurableActionOutcome(DurableActionStatus.SUCCEEDED, result_json='{"applied":true}')


def pending_count(subsystem: str) -> int:
    """Cheap count of pending records (for notification badges)."""
    return len(list_pending(subsystem))


# --- Write origin ---

def current_origin() -> str:
    """``foreground`` or ``background_review`` — reuses the skill-provenance ContextVar
    the background review fork sets; foreground turns leave it at the default."""
    try:
        from tools.skill_provenance import get_current_write_origin
        return get_current_write_origin()
    except Exception:
        return "foreground"


def is_background() -> bool:
    return current_origin() == "background_review"


# --- Gate decision ---

@dataclass(slots=True, kw_only=True)
class GateDecision:
    """Result of evaluating the write gate; exactly one flag is True. ``allow``: do the real write;
    ``blocked``: user denied the inline prompt (``message`` says why); ``stage``: caller must
    ``stage_write`` the payload (``message`` is the user-facing "staged for approval" note)."""

    allow: bool = False
    blocked: bool = False
    stage: bool = False
    message: str = ""


def _staged(subsystem: str) -> GateDecision:
    where = "/skills pending" if subsystem == SKILLS else "/memory pending"
    return GateDecision(stage=True, message=(f"Staged for approval ({subsystem}.write_approval is on). "
                                             f"Not yet saved — review with {where}."))


def evaluate_gate(subsystem: str, *, inline_summary: str = "", inline_detail: str = "") -> GateDecision:
    """Decide what to do with a pending write: gate off → allow; gate on + skills (any origin) or
    background → stage; gate on + memory + foreground → inline prompt when an interactive channel
    exists, else stage. The gate only ever delays a write, never silently refuses it; ``blocked``
    is produced only when the user actively denies the inline prompt."""
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
    """Prompt inline for a memory write: True approved, False denied, None → stage. Uses the per-thread
    CLI approval callback (``tools.terminal_tool.set_approval_callback``) directly, not
    ``prompt_dangerous_approval``: that wrapper falls back to ``input()`` (deadlock-prone under
    prompt_toolkit; silent deny in gateway sessions) and turns callback errors into a deny, whereas
    here a missing channel or failed prompt must stage instead.

    See #15216.
    """
    try:
        from tools.terminal_tool import _get_approval_callback
    except Exception:
        return None
    callback = _get_approval_callback()
    if callback is None:
        return None
    header = summary.strip() or "Save to memory?"
    try:
        from tools.approval_prompt import callback_accepts
        extra = {"title": "Save to memory?"} if callback_accepts(callback, "title") else {}
        choice = callback(detail.strip() or header, f"Save to memory: {header}", allow_permanent=False, **extra)
    except Exception as e:
        logger.error("Inline memory approval prompt failed: %s", e)
        return None
    # unknown outcome → stage rather than drop
    return {"once": True, "session": True, "deny": False}.get(choice)


# --- Skill-specific helpers (gist + diff for the review affordances) ---

_GIST_TEMPLATES = {"write_file": "write {file_path} in '{name}'", "remove_file": "remove {file_path} from '{name}'",
                   "delete": "delete skill '{name}'"}


def skill_gist(action: str, name: str, *, content: str = "", file_path: str = "",
               old_string: str = "", new_string: str = "") -> str:
    """One-line heuristic gist (no model call) for a pending skill write: create/edit use
    the frontmatter ``description:``; patch/write_file describe the size of the change."""
    if action in {"create", "edit"} and content:
        desc = _frontmatter_description(content)
        size = f"{len(content) // 1024 + 1} KB" if len(content) >= 1024 else f"{len(content)} chars"
        return f"{'create' if action == 'create' else 'rewrite'} '{name}'{f' — {desc}' if desc else ''} ({size})"
    if action == "patch":
        removed = old_string.count("\n") + 1 if old_string else 0
        added = new_string.count("\n") + 1 if new_string else 0
        return f"patch '{name}' {file_path or 'SKILL.md'} (+{added}/-{removed} lines)"
    return _GIST_TEMPLATES.get(action, "{action} '{name}'").format(action=action, name=name, file_path=file_path)


def _frontmatter_description(content: str) -> str:
    """Extract the ``description:`` value from SKILL.md YAML frontmatter (≤140 chars)."""
    m = re.search(r"^description:\s*(.+)$", content, re.MULTILINE)
    return m.group(1).strip().strip("'\"")[:140] if m else ""


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
        try:
            return True, "file", _skill_digest(_read_regular_nofollow(path))
        except OSError:
            return True, "other", None
    if stat.S_ISDIR(metadata.st_mode):
        return True, "directory", None
    return True, "other", None


def _read_regular_nofollow(path: Path) -> bytes:
    return _read_regular_nofollow_snapshot(path)[0]


def _read_regular_nofollow_snapshot(path: Path) -> tuple[bytes, tuple[int, int]]:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise OSError("not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise OSError("file identity changed")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(MAX_DURABLE_SKILL_FILE_BYTES + 1)
            if len(data) > MAX_DURABLE_SKILL_FILE_BYTES:
                raise OSError("file exceeds durable skill limit")
            return data, (opened.st_dev, opened.st_ino)
    finally:
        os.close(fd)


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
        if not isinstance(raw, str) or sm._validate_file_path(raw):
            return None
        return skill_dir.relative_to(get_hermes_home() / "skills") / Path(raw)
    if action in {"write_file", "remove_file"}:
        raw = payload.get("file_path")
        if not isinstance(raw, str) or sm._validate_file_path(raw):
            return None
        path = Path(raw)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            return None
        return skill_dir.relative_to(get_hermes_home() / "skills") / path
    return None


def _skill_manual(payload: Dict[str, Any], summary: str, origin: str, reason: str):
    record = stage_write(SKILLS, payload, summary=summary, origin=origin)
    return {**record, "status": "pending_manual_review", "reason": reason}


def _skill_rejected(reason: str) -> Dict[str, Any]:
    return {"status": "rejected", "reason": reason}


def _stage_skill_write_locked(
    payload: Dict[str, Any], *, summary: str, origin: str,
    pending_id: Optional[str] = None, created: Optional[List[Path]] = None,
) -> Dict[str, Any]:
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
    if not _secure_skill_capabilities_available(root):
        return _skill_manual(payload, summary, origin, "secure_mutation_unavailable")
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
        if sm._pinned_guard(name) or sm._org_mirror_write_guard(name, skill_dir, "delete"):
            return _skill_rejected("protected_target")
        return _skill_manual(payload, summary, origin, "unsupported_operation")
    if action == "create" and skill_dir.exists():
        return _skill_rejected("target_exists")
    relative = _skill_relative_path(payload, skill_dir)
    if relative is None or not _skill_components_safe(root, relative):
        return _skill_manual(payload, summary, origin, "unsafe_path")
    target = root / relative
    exists, kind, baseline_digest = _skill_lstat(target)
    baseline_bytes: Optional[bytes] = None
    if kind == "file":
        try:
            baseline_bytes = _read_regular_nofollow(target)
        except OSError:
            return _skill_manual(payload, summary, origin, "unsafe_path")
        baseline_digest = _skill_digest(baseline_bytes)

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
            current = (baseline_bytes or b"").decode("utf-8")
        except UnicodeError:
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
    if desired is not None and len(desired) > MAX_DURABLE_SKILL_FILE_BYTES:
        return _skill_manual(payload, summary, origin, "review_too_large")
    if desired is not None and relative.name == "SKILL.md":
        try: text = desired.decode("utf-8")
        except UnicodeDecodeError: return _skill_manual(payload, summary, origin, "non_text")
        if sm._validate_frontmatter(text, new_skill=action == "create") or sm._validate_content_size(text):
            return _skill_manual(payload, summary, origin, "invalid_content")
    elif desired is not None:
        try:
            supporting_text = desired.decode("utf-8")
        except UnicodeDecodeError:
            return _skill_manual(payload, summary, origin, "non_text")
        if sm._validate_content_size(supporting_text, label=relative.as_posix()):
            return _skill_manual(payload, summary, origin, "invalid_content")
    pending_id = pending_id or uuid.uuid4().hex[:8]
    directory = _skill_pending_path(pending_id)
    for private_root in (get_hermes_home() / "pending", _pending_dir(SKILLS)):
        private_root.mkdir(exist_ok=True, mode=0o700)
        mode = private_root.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise OSError("unsafe pending directory")
        if os.name == "posix": private_root.chmod(0o700)
    directory.mkdir(mode=0o700)
    if created is not None:
        created.append(directory)
    staged_dir = directory / "staged"
    baseline_dir = directory / "baseline"
    staged_dir.mkdir(mode=0o700)
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
            stream.write(baseline_bytes or b""); stream.flush(); os.fsync(stream.fileno())
    if sm._guard_agent_created_enabled():
        review_skill = directory / "review" / name
        try:
            if existing:
                if any(path.is_symlink() for path in skill_dir.rglob("*")):
                    raise OSError("symlink in skill")
                shutil.copytree(skill_dir, review_skill)
            else:
                review_skill.mkdir(parents=True)
            review_target = review_skill / target.relative_to(skill_dir)
            if desired is None:
                if review_target.is_dir():
                    raise OSError("invalid review target")
                review_target.unlink()
            else:
                review_target.parent.mkdir(parents=True, exist_ok=True)
                review_target.write_bytes(desired)
            scan_error = sm._security_scan_skill(review_skill)
        except Exception:
            scan_error = "staged skill could not be scanned safely"
        shutil.rmtree(directory / "review", ignore_errors=True)
        if scan_error:
            shutil.rmtree(directory)
            return _skill_rejected("security_scan_rejected")
    for staged_directory in sorted(
            (path for path in directory.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts), reverse=True):
        _fsync_dir(staged_directory)
    _fsync_dir(directory)
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


def stage_skill_write(payload: Dict[str, Any], *, summary: str, origin: str) -> Dict[str, Any]:
    root = get_hermes_home() / "skills"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    cleanup_terminal_skill_evidence()
    pending = _pending_dir(SKILLS)
    pending_id = uuid.uuid4().hex[:8]
    created: List[Path] = []
    try:
        with _skill_write_lock(root):
            return _stage_skill_write_locked(
                payload, summary=summary, origin=origin,
                pending_id=pending_id, created=created,
            )
    except Exception:
        if pending.is_dir() and not pending.is_symlink():
            for path in created:
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
            _fsync_dir(pending)
        raise


def prepare_durable_skill_pending(record: Dict[str, Any]):
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload
    pending_id = record.get("id")
    operations = record.get("operations")
    operation = operations[0] if isinstance(operations, list) and len(operations) == 1 else None
    relative = Path(operation.get("relative_path", "")) if isinstance(operation, dict) else Path()
    digest_re = re.compile(r"[0-9a-f]{64}")
    baseline_type = operation.get("baseline_type") if isinstance(operation, dict) else None
    staged_digest = operation.get("staged_digest") if isinstance(operation, dict) else None
    baseline_digest = operation.get("baseline_digest") if isinstance(operation, dict) else None
    operation_valid = (
        isinstance(operation, dict)
        and operation.get("operation") in {"create", "edit", "patch", "write_file", "remove_file"}
        and isinstance(operation.get("relative_path"), str)
        and bool(relative.parts) and not relative.is_absolute()
        and not ({"", ".", ".."} & set(relative.parts))
        and type(operation.get("baseline_exists")) is bool
        and baseline_type in {"missing", "file"}
        and operation.get("baseline_exists") is (baseline_type == "file")
        and (operation.get("operation") not in {"edit", "patch", "remove_file"}
             or baseline_type == "file")
        and (operation.get("operation") != "create" or baseline_type == "missing")
        and ((baseline_type == "file" and isinstance(baseline_digest, str)
              and digest_re.fullmatch(baseline_digest))
             or (baseline_type == "missing" and baseline_digest is None))
        and ((staged_digest is None and operation.get("operation") == "remove_file")
             or (isinstance(staged_digest, str) and digest_re.fullmatch(staged_digest)
                 and operation.get("operation") != "remove_file"))
        and type(operation.get("staged_bytes")) is int
        and 0 <= operation.get("staged_bytes", -1) <= MAX_DURABLE_SKILL_FILE_BYTES
        and (staged_digest is not None or operation.get("staged_bytes") == 0)
        and operation.get("expected_mode") == 0o600
    )
    if (record.get("manifest_version") != 1 or record.get("fingerprint_version") != 1
            or not isinstance(pending_id, str) or not _PENDING_ID_RE.fullmatch(pending_id)
            or not isinstance(record.get("skill_name"), str)
            or record["skill_name"] not in relative.parts[:2]
            or not operation_valid
            or record.get("subject_fingerprint") != _skill_manifest_fingerprint(record)):
        return None
    fingerprint = record["subject_fingerprint"]
    return PreparedDurableAction("skill.apply_pending", 1,
        encode_durable_action_payload({"pending_id": pending_id}), fingerprint,
        f"skill.apply_pending:{pending_id}:{fingerprint}")


def execute_manual_skill_pending(record: Dict[str, Any]):
    from gateway.durable_actions import DurableActionExecutionContext, DurableActionOutcome, DurableActionStatus
    prepared = prepare_durable_skill_pending(record)
    if prepared is None:
        return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason="record_changed")
    context = DurableActionExecutionContext(
        request_id=f"manual_{record['id']}", action_type=prepared.action_type,
        action_version=prepared.action_version, payload_json=prepared.payload_json,
        fingerprint=prepared.fingerprint, idempotency_key=prepared.idempotency_key,
    )
    return execute_durable_skill_pending(context, _manual=True)


def _durable_skill_review(record: Dict[str, Any], *, max_chars: Optional[int] = None) -> Optional[str]:
    operation = record["operations"][0]
    relative = operation["relative_path"]
    before = b""
    if operation["baseline_type"] == "file":
        try: before = _read_regular_nofollow(_skill_pending_path(record["id"]) / "baseline" / "0")
        except OSError: return None
        if _skill_digest(before) != operation["baseline_digest"]:
            return None
    after = b""
    if operation["staged_digest"] is not None:
        try: after = _read_regular_nofollow(_skill_pending_path(record["id"]) / "staged" / "0")
        except OSError: return None
        if _skill_digest(after) != operation["staged_digest"]:
            return None
    try:
        old = before.decode("utf-8"); new = after.decode("utf-8")
    except UnicodeDecodeError:
        return None
    rendered = f"# {operation['operation']} {relative}\n" + "".join(difflib.unified_diff(
        old.splitlines(keepends=True), new.splitlines(keepends=True),
        fromfile=f"a/{relative}" if before else "/dev/null",
        tofile=f"b/{relative}" if after else "/dev/null"))
    return rendered if max_chars is None or len(rendered) <= max_chars else None


def create_skill_durable_approval(record: Dict[str, Any]) -> Dict[str, Any]:
    from gateway.durable_requests import (
        DurableRequestError, DurableRequestLimitError, DurableRequestOption,
        DurableRequestSpec, get_durable_request_store, load_durable_requests_config,
    )
    from hermes_cli.config import load_config_readonly
    pending_id = str(record.get("id", ""))
    manual = lambda reason: {"status": "pending_manual_review", "reason": reason,
                             "pending_id": pending_id}
    stored = get_pending(SKILLS, pending_id)
    prepared = prepare_durable_skill_pending(stored or {})
    if prepared is not None:
        try:
            receipt = skill_action_receipt(pending_id)
        except SkillActionLookupUnavailable:
            receipt = None
        if receipt is not None:
            request_id, status, reconciliation = receipt
            if reconciliation is not None:
                status = f"reconciled_{reconciliation}"
            elif status == "waiting_for_decision":
                status = "awaiting_approval"
            return {"status": status, "pending_id": pending_id,
                    "request_id": request_id, "action_id": request_id}
    review = (_durable_skill_review(stored, max_chars=MAX_DURABLE_SKILL_DIFF_CHARS)
              if stored and prepared else None)
    if prepared is None or review is None:
        return manual("invalid_or_oversize_review")
    try:
        config = load_config_readonly(); policy = config.get("durable_approvals", {})
        if not isinstance(policy, dict) or policy.get("enabled") is not True:
            return manual("durable_disabled")
        if "skill.apply_pending" not in policy.get("allowed_actions", []):
            return manual("handler_not_allowed")
        request_config = load_durable_requests_config()
        if not request_config.available or request_config.target is None:
            return manual("inbox_unavailable")
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
            command_response_allowed=(request_config.command_responses_enabled
                                      and request_config.allow_on_native_delivery),
            text_command_fallback_allowed=(request_config.command_responses_enabled
                                           and request_config.allow_text_delivery_fallback),
            origin_session_id=origin.origin_session_id, origin_ui_session_id=origin.origin_ui_session_id,
            origin_profile=origin.origin_profile, maximum_ttl_seconds=request_config.max_ttl_seconds,
            terminal_retention_seconds=request_config.terminal_retention_seconds,
        )
    except (DurableRequestError, DurableRequestLimitError, ValueError):
        return manual("review_too_large")
    except (OSError, sqlite3.Error):
        return manual("inbox_unavailable")
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


def _open_skill_dirfd(root: Path, relative: Path, *, create: bool = False) -> int:
    """Open a verified directory chain without following symlinks."""
    if os.name != "posix":
        raise OSError("secure directory-relative mutation unavailable")
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    current = os.open(root, flags)
    try:
        opened = os.fstat(current)
        named = root.lstat()
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            raise OSError("skill root identity changed")
        for part in relative.parts:
            if part in {"", "."}:
                continue
            try:
                child = os.open(part, flags, dir_fd=current)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, 0o700, dir_fd=current)
                os.fsync(current)
                child = os.open(part, flags, dir_fd=current)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def _dirfd_matches_path(fd: int, path: Path) -> bool:
    try:
        opened = os.fstat(fd)
        named = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(named.st_mode) and (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino)


class _SkillPreconditionError(OSError):
    pass


class _SkillMutationError(OSError):
    pass


def _renameat2(source_fd: int, source: str, target_fd: int, target: str, flags: int) -> None:
    function = getattr(ctypes.CDLL(None, use_errno=True), "renameat2", None)
    if function is None:
        raise OSError(errno.ENOSYS, "renameat2 unavailable")
    function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    function.restype = ctypes.c_int
    if function(source_fd, os.fsencode(source), target_fd, os.fsencode(target), flags) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


def _secure_skill_capabilities_available(root: Path) -> bool:
    if (os.name != "posix" or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY")
            or getattr(ctypes.CDLL(None), "renameat2", None) is None):
        return False
    # Some test/runtime wrappers replace os callables without changing their dir_fd support.
    # The probe below exercises the actual operations on this filesystem instead.
    try:
        root_fd = _open_skill_dirfd(root, Path())
    except OSError:
        return False
    prefix = f".durable-probe-{uuid.uuid4().hex}"
    first, second = f"{prefix}-a", f"{prefix}-b"
    proven = cleanup_ok = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        for name in (first, second):
            fd = os.open(name, flags, 0o600, dir_fd=root_fd)
            try: os.fsync(fd)
            finally: os.close(fd)
        try:
            _renameat2(root_fd, first, root_fd, second, 1)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        else:
            raise OSError("renameat2 no-replace was not enforced")
        _renameat2(root_fd, first, root_fd, second, 2)
        os.fsync(root_fd)
        proven = True
    except (OSError, TypeError):
        pass
    finally:
        cleanup_ok = True
        for name in (first, second):
            try:
                os.unlink(name, dir_fd=root_fd)
            except FileNotFoundError:
                pass
            except OSError:
                cleanup_ok = False
        try: os.fsync(root_fd)
        except OSError: cleanup_ok = False
        os.close(root_fd)
    return proven and cleanup_ok


def _skill_entry_snapshot(parent_fd: int, name: str) -> tuple[bool, str, Optional[str], Optional[tuple[int, int]]]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        return False, "missing", None, None
    try:
        metadata = os.fstat(fd)
        identity = (metadata.st_dev, metadata.st_ino)
        if stat.S_ISDIR(metadata.st_mode):
            return True, "directory", None, identity
        if not stat.S_ISREG(metadata.st_mode):
            return True, "other", None, identity
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(MAX_DURABLE_SKILL_FILE_BYTES + 1)
        if len(data) > MAX_DURABLE_SKILL_FILE_BYTES:
            return True, "other", None, identity
        return True, "file", _skill_digest(data), identity
    finally:
        os.close(fd)


def _write_skill_temp(parent_fd: int, data: bytes) -> str:
    temporary = f".skill-{uuid.uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600, dir_fd=parent_fd)
    try:
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        return temporary
    except BaseException:
        try: os.unlink(temporary, dir_fd=parent_fd)
        except OSError: pass
        raise
    finally:
        os.close(fd)


def _atomic_skill_write_at(parent_fd: int, name: str, data: bytes) -> None:
    temporary = _write_skill_temp(parent_fd, data)
    try:
        os.rename(temporary, name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        os.fsync(parent_fd)
    except BaseException:
        try: os.unlink(temporary, dir_fd=parent_fd)
        except OSError: pass
        raise


def _secure_skill_write(
    root: Path, relative: Path, data: bytes, *, create_parents: bool,
    expected: Optional[tuple[bool, str, Optional[str], Optional[tuple[int, int]]]] = None,
    mutation_fault: bool = True,
) -> None:
    parent_fd = _open_skill_dirfd(root, relative.parent, create=create_parents)
    try:
        if mutation_fault:
            _skill_mutation_fault("parents_opened")
        if expected is None:
            _atomic_skill_write_at(parent_fd, relative.name, data)
        else:
            try:
                observed = _skill_entry_snapshot(parent_fd, relative.name)
            except OSError as exc:
                raise _SkillPreconditionError("target entry unsafe") from exc
            if observed != expected:
                raise _SkillPreconditionError("target entry changed")
            temporary = _write_skill_temp(parent_fd, data)
            exchanged = False
            try:
                _skill_mutation_fault("entry_validated")
                if expected[0]:
                    _renameat2(parent_fd, temporary, parent_fd, relative.name, 2)
                    exchanged = True
                    if _skill_entry_snapshot(parent_fd, temporary) != expected:
                        _renameat2(parent_fd, temporary, parent_fd, relative.name, 2)
                        exchanged = False
                        raise _SkillMutationError("target changed during exchange")
                    os.unlink(temporary, dir_fd=parent_fd)
                else:
                    try:
                        _renameat2(parent_fd, temporary, parent_fd, relative.name, 1)
                    except OSError as exc:
                        if exc.errno == errno.EEXIST:
                            raise _SkillPreconditionError("target appeared") from exc
                        raise
                os.fsync(parent_fd)
            except BaseException:
                if not exchanged:
                    try: os.unlink(temporary, dir_fd=parent_fd)
                    except OSError: pass
                raise
        if not _dirfd_matches_path(parent_fd, root / relative.parent):
            raise _SkillMutationError("target parent identity changed")
    finally:
        os.close(parent_fd)


def _secure_skill_create(root: Path, relative: Path, data: bytes, pending_id: str) -> None:
    skill_relative = relative.parent
    parent_fd = _open_skill_dirfd(root, skill_relative.parent, create=True)
    temporary = f".{skill_relative.name}-{pending_id}.tmp"
    try:
        _skill_mutation_fault("parents_opened")
        try:
            target_exists = _skill_entry_snapshot(parent_fd, skill_relative.name)[0]
        except OSError as exc:
            raise _SkillPreconditionError("target entry unsafe") from exc
        if target_exists:
            raise _SkillPreconditionError("target appeared")
        os.mkdir(temporary, 0o700, dir_fd=parent_fd)
        temporary_fd = os.open(
            temporary,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        try:
            _atomic_skill_write_at(temporary_fd, "SKILL.md", data)
            os.fsync(temporary_fd)
        finally:
            os.close(temporary_fd)
        _skill_mutation_fault("entry_validated")
        try:
            _renameat2(parent_fd, temporary, parent_fd, skill_relative.name, 1)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                raise _SkillPreconditionError("target appeared") from exc
            raise
        os.fsync(parent_fd)
        if not _dirfd_matches_path(parent_fd, root / skill_relative.parent):
            raise _SkillMutationError("target parent identity changed")
    except BaseException:
        try:
            temporary_fd = os.open(
                temporary,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            try:
                os.unlink("SKILL.md", dir_fd=temporary_fd)
                os.fsync(temporary_fd)
            finally:
                os.close(temporary_fd)
            os.rmdir(temporary, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(parent_fd)


def _fsync_dir(directory: Path) -> None:
    if os.name != "posix":
        return
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def _skill_journal_fault(point: str) -> None:
    """Test seam for crash-boundary coverage; production is a no-op."""


def _skill_mutation_fault(point: str) -> None:
    """Test seam for parent-component swap coverage; production is a no-op."""


def cleanup_terminal_skill_evidence(*, limit: int = MAX_SKILL_JOURNAL_CLEANUPS) -> int:
    """Boundedly clean committed quarantine only after trusted terminal success."""
    if type(limit) is not int or not 1 <= limit <= MAX_SKILL_JOURNAL_CLEANUPS:
        raise ValueError("invalid cleanup limit")
    root = get_hermes_home() / "skills"
    transactions = root / ".transactions"
    if not transactions.is_dir() or transactions.is_symlink():
        return 0
    cleaned = 0
    for txn in sorted(transactions.iterdir()):
        if cleaned >= limit or not txn.is_dir() or txn.is_symlink() or not _PENDING_ID_RE.fullmatch(txn.name):
            continue
        journal_path = txn / "journal.json"
        try:
            journal = json.loads(_read_regular_nofollow(journal_path).decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if journal.get("journal_version") != 1 or journal.get("state") not in {
                "PREPARED", "EXECUTING", "COMMITTED", "UNKNOWN", "ROLLBACK_REQUIRED"}:
            continue
        try: receipt = skill_action_receipt(txn.name)
        except SkillActionLookupUnavailable: continue
        if receipt is None or (receipt[1] not in {"succeeded", "already_applied"}
                               and receipt[2] not in {"confirmed_applied", "confirmed_not_applied"}):
            continue
        quarantine = root / ".quarantine" / txn.name
        try:
            if quarantine.is_symlink():
                continue
            record = get_pending(SKILLS, txn.name)
            if prepare_durable_skill_pending(record or {}) is None:
                continue
            operation = record["operations"][0]
            target = root / operation["relative_path"]
            target_exists, target_kind, target_digest = _skill_lstat(target)
            q_exists, q_kind, q_digest = _skill_lstat(quarantine)
            applied_evidence = (
                not target_exists and q_exists
                and q_kind == operation["baseline_type"]
                and q_digest == operation["baseline_digest"]
            )
            not_applied_evidence = (
                target_exists == operation["baseline_exists"]
                and target_kind == operation["baseline_type"]
                and target_digest == operation["baseline_digest"]
                and not q_exists
            )
            if receipt[2] == "confirmed_not_applied":
                if not not_applied_evidence:
                    continue
            elif not applied_evidence:
                continue
            if q_exists:
                quarantine_parent_fd = _open_skill_dirfd(root, Path(".quarantine"))
                try:
                    os.unlink(txn.name, dir_fd=quarantine_parent_fd)
                    os.fsync(quarantine_parent_fd)
                    if not _dirfd_matches_path(quarantine_parent_fd, quarantine.parent):
                        raise OSError("quarantine parent identity changed")
                finally:
                    os.close(quarantine_parent_fd)
            journal["state"] = "CLEANED"
            _secure_skill_write(
                root, journal_path.relative_to(root), json.dumps(journal, sort_keys=True).encode(),
                create_parents=False, mutation_fault=False,
            )
            with ExitStack() as stack:
                txn_fd = _open_skill_dirfd(root, Path(".transactions") / txn.name)
                stack.callback(os.close, txn_fd)
                transactions_fd = _open_skill_dirfd(root, Path(".transactions"))
                stack.callback(os.close, transactions_fd)
                os.unlink("journal.json", dir_fd=txn_fd)
                os.fsync(txn_fd)
                os.rmdir(txn.name, dir_fd=transactions_fd)
                os.fsync(transactions_fd)
                if not _dirfd_matches_path(transactions_fd, transactions):
                    raise OSError("transactions parent identity changed")
            staged = _skill_pending_path(txn.name)
            if staged.is_dir() and not staged.is_symlink():
                shutil.rmtree(staged)
                _fsync_dir(staged.parent)
            manifest = _pending_dir(SKILLS) / f"{txn.name}.json"
            if receipt[2] is not None and manifest.is_file() and not manifest.is_symlink():
                manifest.unlink()
                _fsync_dir(manifest.parent)
            cleaned += 1
        except OSError:
            continue
    return cleaned


def execute_durable_skill_pending(context, *, _manual: bool = False):
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
            or (not _manual and (
            skills.get("durable_approval") is not True or not isinstance(policy, dict)
            or policy.get("enabled") is not True or not isinstance(allowed, list)
            or "skill.apply_pending" not in allowed))):
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
    with _skill_write_lock(root):
        if not _skill_components_safe(root, relative):
            return failed(DurableActionStatus.INVALIDATED, "target_invalid")
        exists, kind, digest = _skill_lstat(target)
        journal_path = root / ".transactions" / record["id"] / "journal.json"
        if operation["operation"] == "remove_file" and journal_path.exists():
            try:
                journal = json.loads(_read_regular_nofollow(journal_path).decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                return failed(DurableActionStatus.UNKNOWN, "journal_invalid")
            if (journal.get("journal_version") != 1
                    or journal.get("target_relative_path") != relative.as_posix()
                    or journal.get("quarantine_relative_path") != f".quarantine/{record['id']}"):
                return failed(DurableActionStatus.UNKNOWN, "journal_invalid")
            journal_state = journal.get("state")
            if journal_state in {"PREPARED", "EXECUTING", "UNKNOWN", "ROLLBACK_REQUIRED"}:
                return failed(DurableActionStatus.UNKNOWN, "journal_incomplete")
            if journal_state in {"COMMITTED", "CLEANED"} and not exists:
                quarantine = root / ".quarantine" / record["id"]
                q_exists, q_kind, q_digest = _skill_lstat(quarantine)
                evidence_matches = (
                    journal_state == "CLEANED" and not q_exists
                    or journal_state == "COMMITTED" and q_exists
                    and q_kind == operation["baseline_type"]
                    and q_digest == operation["baseline_digest"]
                )
                if evidence_matches:
                    return failed(DurableActionStatus.ALREADY_APPLIED, "journal_committed")
                return failed(DurableActionStatus.UNKNOWN, "journal_conflict")
        exact_create = operation["operation"] != "create" or (
            target.parent.is_dir() and not target.parent.is_symlink()
            and sorted(path.name for path in target.parent.iterdir()) == ["SKILL.md"]
        )
        if (operation["staged_digest"] is not None and exact_create
                and kind == "file" and digest == operation["staged_digest"]):
            return failed(DurableActionStatus.ALREADY_APPLIED, "desired_present")
        if operation["operation"] == "create" and target.parent.exists():
            return failed(DurableActionStatus.INVALIDATED, "target_exists")
        if operation["operation"] == "remove_file" and not exists:
            return failed(DurableActionStatus.INVALIDATED, "target_absent")
        if (exists != operation["baseline_exists"] or kind != operation["baseline_type"]
                or digest != operation["baseline_digest"]):
            return failed(DurableActionStatus.INVALIDATED, "baseline_changed")
        if operation["staged_digest"] is not None:
            try:
                desired = _read_regular_nofollow(staged)
            except OSError:
                return failed(DurableActionStatus.INVALIDATED, "staged_missing")
            if _skill_digest(desired) != operation["staged_digest"]:
                return failed(DurableActionStatus.INVALIDATED, "staged_changed")
        from tools import skill_manager_tool as sm
        if sm._guard_agent_created_enabled():
            review_root = Path(tempfile.mkdtemp(dir=_skill_pending_path(record["id"]), prefix="review-"))
            try:
                found = sm._find_skill(record["skill_name"])
                review_skill = review_root / record["skill_name"]
                if found:
                    if any(path.is_symlink() for path in found["path"].rglob("*")):
                        return failed(DurableActionStatus.INVALIDATED, "security_scan_failed")
                    shutil.copytree(found["path"], review_skill)
                    review_target = review_skill / target.relative_to(found["path"])
                else:
                    review_skill.mkdir(); review_target = review_skill / "SKILL.md"
                if desired is None:
                    review_target.unlink()
                else:
                    review_target.parent.mkdir(parents=True, exist_ok=True)
                    review_target.write_bytes(desired)
                if sm._security_scan_skill(review_skill):
                    return failed(DurableActionStatus.INVALIDATED, "security_scan_rejected")
            except Exception:
                return failed(DurableActionStatus.INVALIDATED, "security_scan_failed")
            finally:
                shutil.rmtree(review_root, ignore_errors=True)
        re_exists, re_kind, re_digest = _skill_lstat(target)
        re_identity = None
        if re_kind == "file":
            try:
                re_bytes, re_identity = _read_regular_nofollow_snapshot(target)
            except OSError:
                return failed(DurableActionStatus.INVALIDATED, "baseline_changed")
            re_digest = _skill_digest(re_bytes)
        if (re_exists != operation["baseline_exists"] or re_kind != operation["baseline_type"]
                or re_digest != operation["baseline_digest"]):
            return failed(DurableActionStatus.INVALIDATED, "baseline_changed")
        if operation["operation"] == "create" and target.parent.exists():
            return failed(DurableActionStatus.INVALIDATED, "target_exists")
        if not _secure_skill_capabilities_available(root):
            return failed(DurableActionStatus.INVALIDATED, "secure_mutation_unavailable")
        expected_entry = (re_exists, re_kind, re_digest, re_identity)
        try:
            if operation["operation"] == "remove_file":
                txn = root / ".transactions" / record["id"]
                quarantine = root / ".quarantine" / record["id"]
                try:
                    txn_fd = _open_skill_dirfd(root, Path(".transactions") / record["id"], create=True)
                except OSError as exc:
                    raise _SkillPreconditionError("transaction parent unavailable") from exc
                os.close(txn_fd)
                stack = ExitStack()
                try:
                    target_parent_fd = _open_skill_dirfd(root, relative.parent)
                    stack.callback(os.close, target_parent_fd)
                    quarantine_parent_fd = _open_skill_dirfd(root, Path(".quarantine"), create=True)
                    stack.callback(os.close, quarantine_parent_fd)
                except OSError as exc:
                    stack.close()
                    raise _SkillPreconditionError("secure parent unavailable") from exc
                journal = {"journal_version": 1, "state": "PREPARED", "operation_progress": 0,
                           "target_relative_path": relative.as_posix(),
                           "quarantine_relative_path": str(quarantine.relative_to(root))}
                atomic = txn / "journal.json"
                atomic_write = lambda: _secure_skill_write(
                    root, atomic.relative_to(root), json.dumps(journal, sort_keys=True).encode(),
                    create_parents=False, mutation_fault=False,
                )
                try:
                    try:
                        os.stat(record["id"], dir_fd=quarantine_parent_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        pass
                    else:
                        raise OSError("quarantine collision")
                    if atomic.exists():
                        return failed(DurableActionStatus.UNKNOWN, "journal_incomplete")
                    _skill_mutation_fault("parents_opened")
                    try:
                        observed_entry = _skill_entry_snapshot(target_parent_fd, relative.name)
                    except OSError as exc:
                        raise _SkillPreconditionError("target entry unsafe") from exc
                    if observed_entry != expected_entry:
                        raise _SkillPreconditionError("target entry changed")
                    atomic_write(); _skill_journal_fault("prepared")
                    journal["state"] = "EXECUTING"; atomic_write(); _skill_journal_fault("executing")
                    _skill_mutation_fault("entry_validated")
                    try:
                        _renameat2(target_parent_fd, relative.name, quarantine_parent_fd, record["id"], 1)
                    except OSError as exc:
                        if exc.errno == errno.EEXIST:
                            raise _SkillMutationError("quarantine appeared after journal start") from exc
                        raise
                    journal["operation_progress"] = 1; _skill_journal_fault("renamed")
                    if _skill_entry_snapshot(quarantine_parent_fd, record["id"]) != expected_entry:
                        try:
                            _renameat2(quarantine_parent_fd, record["id"], target_parent_fd, relative.name, 1)
                        except OSError:
                            pass
                        raise _SkillMutationError("moved target identity changed")
                    os.fsync(target_parent_fd); _skill_journal_fault("source_fsynced")
                    os.fsync(quarantine_parent_fd); _skill_journal_fault("quarantine_fsynced")
                    if (not _dirfd_matches_path(target_parent_fd, target.parent)
                            or not _dirfd_matches_path(quarantine_parent_fd, quarantine.parent)):
                        raise OSError("mutation parent identity changed")
                    journal["state"] = "COMMITTED"; atomic_write(); _skill_journal_fault("committed")
                finally:
                    stack.close()
            else:
                assert desired is not None
                if operation["operation"] == "create":
                    _secure_skill_create(root, relative, desired, record["id"])
                else:
                    _secure_skill_write(
                        root, relative, desired,
                        create_parents=operation["operation"] == "write_file",
                        expected=expected_entry,
                    )
        except _SkillPreconditionError:
            return failed(DurableActionStatus.INVALIDATED, "target_changed")
        except Exception:
            return failed(DurableActionStatus.UNKNOWN, "mutation_unverified")
    _write_pending_record({**record, "status": "applied"})
    if pending_directory.is_dir() and not pending_directory.is_symlink():
        shutil.rmtree(pending_directory)
        _fsync_dir(pending_directory.parent)
    try:
        from tools.skill_manager_tool import finalize_skill_mutation
        finalize_skill_mutation(operation["operation"], record["skill_name"], {"success": True})
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
        try:
            journal = json.loads(_read_regular_nofollow(journal_path).decode("utf-8"))
            state = journal["state"].lower()
        except (OSError, UnicodeDecodeError, KeyError, json.JSONDecodeError):
            return DurableActionDiagnostic("journal_invalid")
        target_exists, target_kind, target_digest = _skill_lstat(target)
        target_state = (
            "baseline" if target_exists and target_kind == operation["baseline_type"]
            and target_digest == operation["baseline_digest"]
            else "absent" if not target_exists else "changed"
        )
        quarantine = root / journal.get("quarantine_relative_path", ".invalid")
        q_exists, q_kind, q_digest = _skill_lstat(quarantine)
        quarantine_state = (
            "baseline" if q_exists and q_kind == operation["baseline_type"]
            and q_digest == operation["baseline_digest"] else "absent" if not q_exists else "changed"
        )
        recovery = (
            "confirm_applied" if target_state == "absent" and quarantine_state == "baseline"
            else "confirm_not_applied" if target_state == "baseline" and quarantine_state == "absent"
            else "inspect_conflict"
        )
        return DurableActionDiagnostic("journal_evidence", (
            ("journal_state", state), ("target_state", target_state),
            ("quarantine_state", quarantine_state),
            ("operation_progress", str(journal.get("operation_progress", 0))),
            ("recovery", recovery),
        ))
    exists, kind, digest = _skill_lstat(target)
    if operation["staged_digest"] and kind == "file" and digest == operation["staged_digest"]:
        state = "desired"
    elif (exists == operation["baseline_exists"] and kind == operation["baseline_type"]
          and digest == operation["baseline_digest"]):
        state = "baseline"
    else: state = "changed"
    return DurableActionDiagnostic("target_evidence", (("target_state", state),))
