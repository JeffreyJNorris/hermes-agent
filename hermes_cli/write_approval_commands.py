#!/usr/bin/env python3
"""Shared handlers for the /memory and /skills write-approval subcommands."""

from __future__ import annotations

import json
from typing import List, Optional

from tools import write_approval as wa


def _fmt_state(subsystem: str) -> str:
    on = wa.write_approval_enabled(subsystem)
    return f"{subsystem}.write_approval = {'on' if on else 'off'}"


def _fmt_pending_list(subsystem: str) -> str:
    records = wa.list_pending(subsystem)
    if not records:
        return f"No pending {subsystem} writes."
    lines = [f"Pending {subsystem} writes ({len(records)}):"]
    for r in records:
        origin = r.get("origin", "foreground")
        tag = " [auto]" if origin == "background_review" else ""
        linked = ""
        if subsystem in {wa.MEMORY, wa.SKILLS}:
            try:
                request_id = (wa.linked_memory_request_id(r) if subsystem == wa.MEMORY
                              else wa.linked_skill_request_id(r))
            except (wa.MemoryActionLookupUnavailable, wa.SkillActionLookupUnavailable):
                request_id = None
                linked = " [status unavailable]"
            if request_id:
                try:
                    from gateway.durable_operations import DurableOperationsStore
                    from hermes_constants import get_hermes_home
                    action = DurableOperationsStore(get_hermes_home() / "state.db").get_action(request_id)
                    linked = f" [{action.status}]" if action else ""
                except Exception:
                    pass
        lines.append(f"  {r['id']}{tag}{linked}  {r.get('summary', '')}")
        if subsystem == wa.MEMORY:
            lines.extend(f"      {line}" for line in _matched_entries(r["payload"]))
    lines.append("")
    lines.append(f"Apply: /{subsystem} approve <id>   Reject: /{subsystem} reject <id>")
    if subsystem == wa.SKILLS:
        lines.append("Review full diff: /skills diff <id>")
    return "\n".join(lines)


def handle_pending_subcommand(
    subsystem: str, args: List[str], *, memory_store=None, set_mode_fn=None) -> Optional[str]:
    """Dispatch a /memory or /skills write-approval subcommand.

    ``memory_store`` applies approved memory writes (CLI passes its live store; gateway a freshly
    loaded one); ``set_mode_fn`` persists the write_approval boolean. Returns text for the user,
    or None when the args are not a write-approval subcommand so the caller falls through to its
    other handling (e.g. /skills search).
    """
    if not args:
        return f"{_fmt_state(subsystem)}\n\n" + _fmt_pending_list(subsystem)
    sub, rest = args[0].lower(), args[1:]
    if sub == "pending":
        return _fmt_pending_list(subsystem)
    if sub in {"approve", "apply"}:
        return _approve(subsystem, rest, memory_store)
    if sub in {"reject", "deny", "drop"}:
        return _reject(subsystem, rest)
    if sub == "diff" and subsystem == wa.SKILLS:
        return _diff(rest)
    if sub in {"approval", "mode"}:  # 'mode' kept as a back-compat alias
        return _set_approval(subsystem, rest, set_mode_fn)
    return None  # not ours — caller handles


def _usage(subsystem: str) -> str:
    return f"Usage: /{subsystem} approve|reject <id>  (or 'all')"


def _approve(subsystem: str, rest: List[str], memory_store) -> str:
    if not rest:
        return _usage(subsystem)
    target = rest[0]
    records = wa.list_pending(subsystem)
    if not records:
        return f"No pending {subsystem} writes."
    if target.lower() == "all":
        targets = list(records)
    else:
        rec = wa.get_pending(subsystem, target)
        if not rec:
            return f"No pending {subsystem} write with id '{target}'."
        targets = [rec]

    applied, authorized, failed = 0, 0, []
    overwritten, removed = [], []
    for rec in targets:
        try:
            request_id = (wa.linked_memory_request_id(rec) if subsystem == wa.MEMORY
                          else wa.linked_skill_request_id(rec))
            receipt = (wa.memory_action_receipt(rec.get("id", "")) if subsystem == wa.MEMORY
                       else wa.skill_action_receipt(rec.get("id", "")))
        except (wa.MemoryActionLookupUnavailable, wa.SkillActionLookupUnavailable):
            failed.append(f"{rec['id']}: linked_action_unavailable")
            continue
        if receipt is not None and receipt[1] == "unknown" and receipt[2] is None:
            failed.append(f"{rec['id']}: reconciliation_required")
            continue
        if receipt is not None and receipt[2] is not None:
            wa.discard_pending(subsystem, rec["id"])
            failed.append(f"{rec['id']}: reconciled_{receipt[2]}")
            continue
        if (
            not request_id
            and (
                rec.get("durable_request_id") is not None
                or receipt is not None
            )
        ):
            failed.append(f"{rec['id']}: linked_action_invalid")
            continue
        if request_id:
            from gateway.durable_operations import DurableOperationsStore
            from hermes_constants import get_hermes_home
            result = DurableOperationsStore(get_hermes_home() / "state.db").authorize_action(request_id)
            if result.ok:
                authorized += 1
            else:
                failed.append(f"{rec['id']}: {result.code}")
            continue
        ok, msg, result = _apply_one(subsystem, rec, memory_store)
        if ok:
            wa.discard_pending(subsystem, rec["id"])
            applied += 1
            overwritten.extend(f"  {rec['id']}: {text}" for text in _changed_entries(result, "replaced"))
            removed.extend(f"  {rec['id']}: {text}" for text in _changed_entries(result, "removed"))
        else:
            failed.append(f"{rec['id']}: {msg}")

    out = []
    if authorized:
        out.append(f"Authorized {authorized} linked {subsystem} write(s).")
    out.append(f"Approved {applied} {subsystem} write(s).")
    if overwritten:
        # A memory 'replace' overwrites the WHOLE matched entry (#117952); the approver
        # is the last person who can notice a clause went missing, so show what was lost.
        out.append("Overwrote entire entry (re-add anything you still need):")
        out.extend(overwritten)
    if removed:
        out.append("Removed entry (re-add anything you still need):")
        out.extend(removed)
    if failed:
        out.append("Failed:")
        out.extend(f"  {f}" for f in failed)
    return "\n".join(out)


def _changed_entries(result: dict, kind: str) -> List[str]:
    """Full text of every entry a memory replace overwrote (``kind="replaced"``) or remove
    deleted (``"removed"``), single-op or batch shape."""
    single = result.get(f"{kind}_entry")
    batch = result.get(f"{kind}_entries") or {}
    return ([single] if single else []) + [batch[k] for k in sorted(batch, key=int)]


def _matched_entries(payload) -> List[str]:
    """The full entry each staged memory replace/remove is pinned to: the summary shows only
    the old_text search string, and approval applies to this entry, not to that search."""
    from tools.memory_tool import destructive_ops
    return [f"{op['action']}s entry: {op['matched_entry']}" if op.get("matched_entry")
            else f"{op['action']}: unpinned legacy target \u2014 reject and recreate before approving"
            for op in destructive_ops(payload)]


def _apply_one(subsystem: str, rec, memory_store):
    """``(ok, error, result)`` — *result* is the applier's full payload (empty on exceptions)."""
    payload = rec.get("payload", {})
    try:
        if subsystem == wa.MEMORY:
            if memory_store is None:
                return False, "memory store unavailable", {}
            from tools.memory_tool import apply_memory_pending
            result = apply_memory_pending(payload, memory_store)
        else:
            if rec.get("manifest_version") == 1:
                outcome = wa.execute_manual_skill_pending(rec)
                return outcome.status.value in {"succeeded", "already_applied"}, outcome.reason or "", {}
            from tools.skill_manager_tool import apply_skill_pending
            result = json.loads(apply_skill_pending(payload))
        return bool(result.get("success")), result.get("error", ""), result
    except Exception as e:
        return False, str(e), {}


def _reject(subsystem: str, rest: List[str]) -> str:
    if not rest:
        return _usage(subsystem)
    target = rest[0]
    if target.lower() == "all":
        records = wa.list_pending(subsystem, include_terminal=True)
    else:
        record = wa.get_pending(subsystem, target)
        records = [record] if record is not None else []
    if not records:
        return f"No pending {subsystem} write with id '{target}'."
    rejected, failed = 0, []
    for record in records:
        try:
            request_id = (wa.linked_memory_request_id(record) if subsystem == wa.MEMORY
                          else wa.linked_skill_request_id(record))
            receipt = (wa.memory_action_receipt(record.get("id", "")) if subsystem == wa.MEMORY
                       else wa.skill_action_receipt(record.get("id", "")))
        except (wa.MemoryActionLookupUnavailable, wa.SkillActionLookupUnavailable):
            failed.append(f"{record['id']}: linked_action_unavailable")
            continue
        if receipt is not None and receipt[1] == "unknown" and receipt[2] is None:
            failed.append(f"{record['id']}: reconciliation_required")
            continue
        if receipt is not None and receipt[2] is not None:
            if wa.discard_pending(subsystem, record["id"]):
                rejected += 1
            continue
        if not request_id:
            request_id = receipt[0] if receipt else None
            if not request_id and record.get("durable_request_id") is not None:
                failed.append(f"{record['id']}: linked_action_unavailable")
                continue
        if request_id:
            from gateway.durable_operations import DurableOperationsStore
            from hermes_constants import get_hermes_home
            result = DurableOperationsStore(get_hermes_home() / "state.db").cancel_request(request_id)
            if not result.ok and result.code != "already_terminal":
                failed.append(f"{record['id']}: {result.code}")
                continue
        if wa.discard_pending(subsystem, record["id"]):
            rejected += 1
    out = [f"Rejected {rejected} pending {subsystem} write(s)."]
    if failed:
        out.extend(["Failed:", *(f"  {item}" for item in failed)])
    return "\n".join(out)


def _diff(rest: List[str]) -> str:
    if not rest:
        return "Usage: /skills diff <id>"
    rec = wa.get_pending(wa.SKILLS, rest[0])
    if not rec:
        return f"No pending skill write with id '{rest[0]}'."
    return f"# Pending skill write {rec['id']}: {rec.get('summary', '')}\n\n" + wa.skill_pending_diff(rec)


_APPROVAL_VALUES = {
    **dict.fromkeys(("on", "true", "yes", "1", "enable", "enabled"), True),
    **dict.fromkeys(("off", "false", "no", "0", "disable", "disabled"), False)}


def _set_approval(subsystem: str, rest: List[str], set_mode_fn) -> str:
    """Turn the approval gate on/off for a subsystem."""
    if not rest:
        return (f"{_fmt_state(subsystem)}\n"
                f"Set with: /{subsystem} approval <on|off>")
    arg = rest[0].strip().lower()
    enabled = _APPROVAL_VALUES.get(arg)
    if enabled is None:
        return f"Invalid value '{arg}'. Use: on or off."
    if set_mode_fn is None:
        val = "true" if enabled else "false"
        return (f"To change the {subsystem} approval gate, run:\n"
                f"  hermes config set {subsystem}.write_approval {val}")
    try:
        set_mode_fn(enabled)
    except Exception as e:
        return f"Failed to set {subsystem}.write_approval: {e}"
    return f"{subsystem}.write_approval set to '{'on' if enabled else 'off'}'."
