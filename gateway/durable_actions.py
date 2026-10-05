"""Trusted action values persisted beside durable requests."""

from __future__ import annotations

import json
import math
import re
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from hermes_constants import get_hermes_home
from hermes_state_common import (
    MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES,
    MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH,
    MAX_DURABLE_ACTION_PAYLOAD_BYTES,
    reconcile_durable_action_schema,
)

if TYPE_CHECKING:
    from gateway.durable_requests import DurableRequest


class DurableActionStatus(str, Enum):
    WAITING_FOR_DECISION = "waiting_for_decision"
    READY = "ready"
    CLAIMED = "claimed"
    EXECUTING = "executing"
    SUCCEEDED = "succeeded"
    ALREADY_APPLIED = "already_applied"
    DECLINED = "declined"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    ABANDONED = "abandoned"
    INVALIDATED = "invalidated"
    FAILED = "failed"
    UNKNOWN = "unknown"
    HANDLER_UNAVAILABLE = "handler_unavailable"


class DurableActionNotificationStatus(str, Enum):
    NOT_READY = "not_ready"
    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    DROPPED = "dropped"


_ACTION_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
_STRUCTURAL_REASON_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
_EXECUTION_OUTCOME_STATUSES = frozenset(
    {
        DurableActionStatus.SUCCEEDED,
        DurableActionStatus.ALREADY_APPLIED,
        DurableActionStatus.INVALIDATED,
        DurableActionStatus.FAILED,
        DurableActionStatus.UNKNOWN,
        DurableActionStatus.HANDLER_UNAVAILABLE,
    }
)


def _validate_action_identity(action_type: str, action_version: int) -> None:
    if not isinstance(action_type, str):
        raise TypeError("action_type must be a string")
    if not _ACTION_TYPE_RE.fullmatch(action_type):
        raise ValueError("action_type must be a lowercase dotted name")
    if type(action_version) is not int:
        raise TypeError("action_version must be an integer")
    if action_version < 1:
        raise ValueError("action_version must be positive")


def _validate_nonblank(value: str, field: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    if not value.strip():
        raise ValueError(f"{field} must not be blank")
    _validate_utf8(value, field)


def _validate_utf8(value: str, field: str) -> None:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} must be valid UTF-8") from exc


def _validate_structural_reason(value: str, field: str = "reason") -> None:
    _validate_nonblank(value, field)
    if (
        len(value) > MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH
        or _STRUCTURAL_REASON_RE.fullmatch(value) is None
    ):
        raise ValueError(f"{field} must be a bounded structural token")


def _validate_json_value(value: object) -> None:
    if value is None or type(value) in {str, bool, int}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON numbers must be finite")
        return
    if type(value) is list:
        for item in value:
            _validate_json_value(item)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("JSON object keys must be strings")
            _validate_json_value(item)
        return
    raise TypeError(f"value of type {type(value).__name__} is not JSON")


def _encode_bounded_json(value: object, max_bytes: int) -> str:
    try:
        _validate_json_value(value)
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        byte_length = len(encoded.encode("utf-8"))
    except (RecursionError, UnicodeEncodeError) as exc:
        raise ValueError("value cannot be encoded as canonical JSON") from exc
    if byte_length > max_bytes:
        raise ValueError(f"canonical JSON is too large ({byte_length} bytes)")
    return encoded


def _reject_json_constant(constant: str) -> object:
    raise ValueError(f"invalid JSON constant {constant}")


def _decode_bounded_json(encoded: str, max_bytes: int) -> object:
    if not isinstance(encoded, str):
        raise TypeError("encoded JSON must be a string")
    try:
        byte_length = len(encoded.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("encoded JSON is not valid UTF-8") from exc
    if byte_length > max_bytes:
        raise ValueError(f"canonical JSON is too large ({byte_length} bytes)")
    try:
        value = json.loads(encoded, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("encoded JSON is invalid") from exc
    if _encode_bounded_json(value, max_bytes) != encoded:
        raise ValueError("encoded JSON must be canonical")
    return value


def encode_durable_action_payload(value: object) -> str:
    return _encode_bounded_json(value, MAX_DURABLE_ACTION_PAYLOAD_BYTES)


def decode_durable_action_payload(payload_json: str) -> object:
    return _decode_bounded_json(payload_json, MAX_DURABLE_ACTION_PAYLOAD_BYTES)


def encode_durable_action_result(value: object) -> str:
    return _encode_bounded_json(value, MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES)


def decode_durable_action_result(result_json: str) -> object:
    return _decode_bounded_json(result_json, MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES)


@dataclass(frozen=True)
class PreparedDurableAction:
    action_type: str
    action_version: int
    payload_json: str
    fingerprint: str
    idempotency_key: str

    def __post_init__(self) -> None:
        _validate_action_identity(self.action_type, self.action_version)
        decode_durable_action_payload(self.payload_json)
        _validate_nonblank(self.fingerprint, "fingerprint")
        _validate_nonblank(self.idempotency_key, "idempotency_key")


@dataclass(frozen=True)
class DurableActionExecutionContext:
    request_id: str
    action_type: str
    action_version: int
    payload_json: str
    fingerprint: str
    idempotency_key: str

    def __post_init__(self) -> None:
        _validate_nonblank(self.request_id, "request_id")
        _validate_action_identity(self.action_type, self.action_version)
        decode_durable_action_payload(self.payload_json)
        _validate_nonblank(self.fingerprint, "fingerprint")
        _validate_nonblank(self.idempotency_key, "idempotency_key")


@dataclass(frozen=True)
class DurableActionTicket:
    request_id: str
    action_type: str
    action_version: int
    payload_json: str
    fingerprint: str
    idempotency_key: str
    status: DurableActionStatus
    execution_claim: str | None
    execution_claimed_at: float | None
    execution_started_at: float | None
    outcome_reason: str | None
    outcome_json: str | None
    completed_at: float | None
    notification_status: DurableActionNotificationStatus
    notification_claim: str | None
    notification_claimed_at: float | None
    notification_completed_at: float | None
    created_at: float
    updated_at: float
    notification_reason: str | None = None


@dataclass(frozen=True)
class DurableActionCreateResult:
    request: DurableRequest
    ticket: DurableActionTicket
    created: bool


@dataclass(frozen=True)
class DurableActionClaim:
    context: DurableActionExecutionContext
    token: str
    claimed_at: float


@dataclass(frozen=True)
class DurableActionNotificationClaim:
    request_id: str
    action_type: str
    action_status: DurableActionStatus
    outcome_reason: str | None
    origin_type: str
    origin_id: str
    origin_session_id: str | None
    origin_ui_session_id: str | None
    origin_profile: str | None
    token: str
    tool_result: dict | None = None


def _notification_tool_result(action_type: str, result_json: str | None) -> dict | None:
    if action_type not in {"terminal.execute", "execute_code.execute", "host.sudo"} or not result_json:
        return None
    from agent.redact import redact_sensitive_text

    try:
        result = decode_durable_action_result(result_json)
    except (TypeError, ValueError):
        return None
    if not isinstance(result, dict):
        return None
    return {
        key: (redact_sensitive_text(value, force=True)[:8000] if isinstance(value, str) else value)
        for key, value in result.items()
        if key in {"tool", "output", "exit_code", "status", "error", "session_id"}
        and (value is None or type(value) in {str, int, bool})
    }


@dataclass(frozen=True)
class DurableActionOutcome:
    status: DurableActionStatus
    reason: str | None = None
    result_json: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, DurableActionStatus):
            raise TypeError("status must be a DurableActionStatus")
        if self.status not in _EXECUTION_OUTCOME_STATUSES:
            raise ValueError("status is not an execution outcome")
        if self.reason is not None:
            if not isinstance(self.reason, str):
                raise TypeError("reason must be a string or None")
            _validate_structural_reason(self.reason)
        if self.result_json is not None:
            if not isinstance(self.result_json, str):
                raise TypeError("result_json must be a string or None")
            decode_durable_action_result(self.result_json)


DurableActionPreparer = Callable[..., PreparedDurableAction]
DurableActionHandler = Callable[
    [DurableActionExecutionContext], DurableActionOutcome
]


@dataclass(frozen=True, slots=True)
class DurableActionDiagnostic:
    code: str
    facts: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        token = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
        if token.fullmatch(self.code) is None:
            raise ValueError("diagnostic code must be a bounded structural token")
        if len(self.facts) > 16:
            raise ValueError("diagnostic facts are too large")
        for item in self.facts:
            if (
                type(item) is not tuple
                or len(item) != 2
                or any(
                    type(value) is not str or token.fullmatch(value) is None
                    for value in item
                )
            ):
                raise ValueError("diagnostic facts must be bounded structural tokens")


DurableActionDiagnosticHandler = Callable[
    [DurableActionExecutionContext], DurableActionDiagnostic
]
_DURABLE_ACTION_HANDLERS: dict[
    tuple[str, int], tuple[DurableActionPreparer, DurableActionHandler]
] = {}
_DURABLE_ACTION_DIAGNOSTICS: dict[
    tuple[str, int], DurableActionDiagnosticHandler
] = {}


def register_durable_action_handler(
    action_type: str,
    action_version: int,
    *,
    prepare: DurableActionPreparer,
    execute: DurableActionHandler,
    diagnose_unknown: DurableActionDiagnosticHandler | None = None,
) -> None:
    _validate_action_identity(action_type, action_version)
    if not callable(prepare):
        raise TypeError("prepare must be callable")
    if not callable(execute):
        raise TypeError("execute must be callable")
    if diagnose_unknown is not None and not callable(diagnose_unknown):
        raise TypeError("diagnose_unknown must be callable or None")
    key = (action_type, action_version)
    if key in _DURABLE_ACTION_HANDLERS:
        raise ValueError(f"durable action handler {key!r} is already registered")
    _DURABLE_ACTION_HANDLERS[key] = (prepare, execute)
    if diagnose_unknown is not None:
        _DURABLE_ACTION_DIAGNOSTICS[key] = diagnose_unknown


def _register_cron_resume_builtin(
    *,
    prepare: DurableActionPreparer,
    execute: DurableActionHandler,
    diagnose_unknown: DurableActionDiagnosticHandler | None = None,
) -> None:
    """Register or refresh only Hermes' fixed cron.resume implementation."""
    expected = (
        ("cron.jobs", "prepare_durable_cron_resume"),
        ("cron.jobs", "execute_durable_cron_resume"),
    )
    supplied = tuple(
        (getattr(fn, "__module__", None), getattr(fn, "__qualname__", None))
        for fn in (prepare, execute)
    )
    key = ("cron.resume", 1)
    current = _DURABLE_ACTION_HANDLERS.get(key)
    if supplied != expected or (
        current is not None
        and tuple(
            (getattr(fn, "__module__", None), getattr(fn, "__qualname__", None))
            for fn in current
        )
        != expected
    ):
        raise ValueError(f"durable action handler {key!r} is already registered")
    if diagnose_unknown is not None:
        expected_diagnostic = ("cron.jobs", "diagnose_durable_cron_resume")
        supplied_diagnostic = (
            getattr(diagnose_unknown, "__module__", None),
            getattr(diagnose_unknown, "__qualname__", None),
        )
        current_diagnostic = _DURABLE_ACTION_DIAGNOSTICS.get(key)
        if supplied_diagnostic != expected_diagnostic or (
            current_diagnostic is not None
            and (
                getattr(current_diagnostic, "__module__", None),
                getattr(current_diagnostic, "__qualname__", None),
            )
            != expected_diagnostic
        ):
            raise ValueError(f"durable action diagnostic {key!r} is already registered")
    _DURABLE_ACTION_HANDLERS[key] = (prepare, execute)
    if diagnose_unknown is not None:
        _DURABLE_ACTION_DIAGNOSTICS[key] = diagnose_unknown


def lookup_durable_action_preparer(
    action_type: str,
    action_version: int,
) -> DurableActionPreparer | None:
    try:
        _validate_action_identity(action_type, action_version)
    except (TypeError, ValueError):
        return None
    registered = _DURABLE_ACTION_HANDLERS.get((action_type, action_version))
    return registered[0] if registered is not None else None


def lookup_durable_action_handler(
    action_type: str,
    action_version: int,
) -> DurableActionHandler | None:
    try:
        _validate_action_identity(action_type, action_version)
    except (TypeError, ValueError):
        return None
    registered = _DURABLE_ACTION_HANDLERS.get((action_type, action_version))
    return registered[1] if registered is not None else None


def lookup_durable_action_diagnostic(
    action_type: str,
    action_version: int,
) -> DurableActionDiagnosticHandler | None:
    try:
        _validate_action_identity(action_type, action_version)
    except (TypeError, ValueError):
        return None
    return _DURABLE_ACTION_DIAGNOSTICS.get((action_type, action_version))


class DurableActionStore:
    """SQLite persistence for execution claims and immutable outcomes."""

    def __init__(
        self,
        db_path: str | Path | None = None,
        *,
        wall_clock=time.time,
    ) -> None:
        self._db_path = Path(db_path or Path(get_hermes_home()) / "state.db")
        self._wall_clock = wall_clock
        self._lock = threading.RLock()
        conn = self._connect()
        try:
            reconcile_durable_action_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, timeout=10.0)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 10000")
            from hermes_state import apply_wal_with_fallback

            apply_wal_with_fallback(conn, db_label="state.db (durable_actions)")
            return conn
        except Exception:
            conn.close()
            raise

    @contextmanager
    def _transaction(self):
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _now(self) -> float:
        now = float(self._wall_clock())
        if not math.isfinite(now):
            raise ValueError("wall clock must be finite")
        return now

    def claim_next(self) -> DurableActionClaim | None:
        token = secrets.token_urlsafe(24)
        with self._lock, self._transaction() as conn:
            row = conn.execute(
                """SELECT request_id, action_type, action_version, payload_json,
                          fingerprint, idempotency_key
                   FROM durable_action_tickets
                   WHERE status = ?
                   ORDER BY created_at, request_id
                   LIMIT 1""",
                (DurableActionStatus.READY.value,),
            ).fetchone()
            if row is None:
                return None
            context = DurableActionExecutionContext(
                request_id=row["request_id"],
                action_type=row["action_type"],
                action_version=int(row["action_version"]),
                payload_json=row["payload_json"],
                fingerprint=row["fingerprint"],
                idempotency_key=row["idempotency_key"],
            )
            now = self._now()
            changed = conn.execute(
                """UPDATE durable_action_tickets
                   SET status = ?, execution_claim = ?, execution_claimed_at = ?,
                       updated_at = ?
                   WHERE request_id = ? AND status = ?""",
                (
                    DurableActionStatus.CLAIMED.value,
                    token,
                    now,
                    now,
                    context.request_id,
                    DurableActionStatus.READY.value,
                ),
            ).rowcount
            if changed != 1:
                return None
        return DurableActionClaim(context=context, token=token, claimed_at=now)

    def mark_executing(self, request_id: str, token: str) -> bool:
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET status = ?, execution_claimed_at = ?,
                       execution_started_at = ?, updated_at = ?
                   WHERE request_id = ? AND status = ? AND execution_claim = ?""",
                (
                    DurableActionStatus.EXECUTING.value,
                    now,
                    now,
                    now,
                    request_id,
                    DurableActionStatus.CLAIMED.value,
                    token,
                ),
            ).rowcount == 1

    def renew_execution_claim(self, request_id: str, token: str) -> bool:
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET execution_claimed_at = ?, updated_at = ?
                   WHERE request_id = ? AND status = ? AND execution_claim = ?""",
                (
                    now,
                    now,
                    request_id,
                    DurableActionStatus.EXECUTING.value,
                    token,
                ),
            ).rowcount == 1

    def complete(
        self,
        request_id: str,
        token: str,
        outcome: DurableActionOutcome,
    ) -> bool:
        if not isinstance(outcome, DurableActionOutcome):
            raise TypeError("outcome must be a DurableActionOutcome")
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET status = ?, execution_claim = NULL,
                       execution_claimed_at = NULL, outcome_reason = ?,
                       outcome_json = ?, completed_at = ?,
                       notification_status = ?, updated_at = ?
                   WHERE request_id = ? AND status = ? AND execution_claim = ?""",
                (
                    outcome.status.value,
                    outcome.reason,
                    outcome.result_json,
                    now,
                    DurableActionNotificationStatus.PENDING.value,
                    now,
                    request_id,
                    DurableActionStatus.EXECUTING.value,
                    token,
                ),
            ).rowcount == 1

    def mark_handler_unavailable(
        self,
        request_id: str,
        token: str,
        *,
        reason: str,
    ) -> bool:
        outcome = DurableActionOutcome(
            DurableActionStatus.HANDLER_UNAVAILABLE,
            reason=reason,
        )
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET status = ?, execution_claim = NULL,
                       execution_claimed_at = NULL, outcome_reason = ?,
                       outcome_json = NULL, completed_at = ?,
                       notification_status = ?, updated_at = ?
                   WHERE request_id = ? AND status = ? AND execution_claim = ?""",
                (
                    outcome.status.value,
                    outcome.reason,
                    now,
                    DurableActionNotificationStatus.PENDING.value,
                    now,
                    request_id,
                    DurableActionStatus.CLAIMED.value,
                    token,
                ),
            ).rowcount == 1

    def recover_stale(self, stale_after_seconds: float) -> tuple[int, int]:
        """Recover stale work with two observations for executing tickets.

        The first stale observation only moves ``updated_at``; UNKNOWN requires
        that marker to remain stale for another interval without a heartbeat.
        """
        stale_after_seconds = float(stale_after_seconds)
        if not math.isfinite(stale_after_seconds) or stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be finite and positive")
        with self._lock, self._transaction() as conn:
            now = self._now()
            cutoff = now - stale_after_seconds
            claimed = conn.execute(
                """UPDATE durable_action_tickets
                   SET status = ?, execution_claim = NULL,
                       execution_claimed_at = NULL, updated_at = ?
                   WHERE status = ? AND execution_started_at IS NULL
                     AND execution_claimed_at <= ?""",
                (
                    DurableActionStatus.READY.value,
                    now,
                    DurableActionStatus.CLAIMED.value,
                    cutoff,
                ),
            ).rowcount
            executing = conn.execute(
                """UPDATE durable_action_tickets
                   SET status = ?, execution_claim = NULL,
                       execution_claimed_at = NULL,
                       outcome_reason = 'execution_interrupted',
                       outcome_json = NULL, completed_at = ?,
                       notification_status = ?, updated_at = ?
                   WHERE status = ?
                     AND execution_claimed_at <= ?
                     AND updated_at != execution_claimed_at
                     AND updated_at <= ?""",
                (
                    DurableActionStatus.UNKNOWN.value,
                    now,
                    DurableActionNotificationStatus.PENDING.value,
                    now,
                    DurableActionStatus.EXECUTING.value,
                    cutoff,
                    cutoff,
                ),
            ).rowcount
            conn.execute(
                """UPDATE durable_action_tickets
                   SET updated_at = ?
                   WHERE status = ? AND execution_claimed_at <= ?
                     AND updated_at = execution_claimed_at""",
                (
                    now,
                    DurableActionStatus.EXECUTING.value,
                    cutoff,
                ),
            )
        return claimed, executing

    def claim_next_notification(self) -> DurableActionNotificationClaim | None:
        token = secrets.token_urlsafe(24)
        with self._lock, self._transaction() as conn:
            row = conn.execute(
                """SELECT ticket.request_id, ticket.action_type, ticket.status,
                          ticket.outcome_reason, ticket.outcome_json, request.origin_type,
                          request.origin_id, request.origin_session_id,
                          request.origin_ui_session_id, request.origin_profile
                   FROM durable_action_tickets AS ticket
                   JOIN durable_requests AS request
                     ON request.request_id = ticket.request_id
                   WHERE ticket.notification_status = ?
                   ORDER BY ticket.completed_at, ticket.request_id
                   LIMIT 1""",
                (DurableActionNotificationStatus.PENDING.value,),
            ).fetchone()
            if row is None:
                return None
            now = self._now()
            changed = conn.execute(
                """UPDATE durable_action_tickets
                   SET notification_status = ?, notification_claim = ?,
                       notification_claimed_at = ?, notification_reason = NULL,
                       updated_at = ?
                   WHERE request_id = ? AND notification_status = ?""",
                (
                    DurableActionNotificationStatus.CLAIMED.value,
                    token,
                    now,
                    now,
                    row["request_id"],
                    DurableActionNotificationStatus.PENDING.value,
                ),
            ).rowcount
            if changed != 1:
                return None
        return DurableActionNotificationClaim(
            request_id=row["request_id"],
            action_type=row["action_type"],
            action_status=DurableActionStatus(row["status"]),
            outcome_reason=row["outcome_reason"],
            origin_type=row["origin_type"],
            origin_id=row["origin_id"],
            origin_session_id=row["origin_session_id"],
            origin_ui_session_id=row["origin_ui_session_id"],
            origin_profile=row["origin_profile"],
            token=token,
            tool_result=_notification_tool_result(row["action_type"], row["outcome_json"]),
        )

    def recover_stale_notifications(self, stale_after_seconds: float) -> int:
        stale_after_seconds = float(stale_after_seconds)
        if not math.isfinite(stale_after_seconds) or stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be finite and positive")
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET notification_status = ?, notification_claim = NULL,
                       notification_claimed_at = NULL,
                       notification_reason = NULL, updated_at = ?
                   WHERE notification_status = ? AND notification_claimed_at <= ?""",
                (
                    DurableActionNotificationStatus.PENDING.value,
                    now,
                    DurableActionNotificationStatus.CLAIMED.value,
                    now - stale_after_seconds,
                ),
            ).rowcount

    def complete_notification(self, request_id: str, token: str) -> bool:
        return self._finish_notification(
            request_id,
            token,
            status=DurableActionNotificationStatus.DELIVERED,
        )

    def drop_notification(self, request_id: str, token: str, *, reason: str) -> bool:
        _validate_structural_reason(reason)
        return self._finish_notification(
            request_id,
            token,
            status=DurableActionNotificationStatus.DROPPED,
            reason=reason,
        )

    def _finish_notification(
        self,
        request_id: str,
        token: str,
        *,
        status: DurableActionNotificationStatus,
        reason: str | None = None,
    ) -> bool:
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET notification_status = ?, notification_claim = NULL,
                       notification_claimed_at = NULL,
                       notification_completed_at = ?, notification_reason = ?,
                       updated_at = ?
                   WHERE request_id = ? AND notification_status = ?
                     AND notification_claim = ?""",
                (
                    status.value,
                    now,
                    reason,
                    now,
                    request_id,
                    DurableActionNotificationStatus.CLAIMED.value,
                    token,
                ),
            ).rowcount == 1

    def release_notification(self, request_id: str, token: str) -> bool:
        with self._lock, self._transaction() as conn:
            now = self._now()
            return conn.execute(
                """UPDATE durable_action_tickets
                   SET notification_status = ?, notification_claim = NULL,
                       notification_claimed_at = NULL,
                       notification_reason = NULL, updated_at = ?
                   WHERE request_id = ? AND notification_status = ?
                     AND notification_claim = ?""",
                (
                    DurableActionNotificationStatus.PENDING.value,
                    now,
                    request_id,
                    DurableActionNotificationStatus.CLAIMED.value,
                    token,
                ),
            ).rowcount == 1


__all__ = [
    "DurableActionCreateResult",
    "DurableActionDiagnostic",
    "DurableActionDiagnosticHandler",
    "DurableActionClaim",
    "DurableActionNotificationStatus",
    "DurableActionNotificationClaim",
    "DurableActionOutcome",
    "DurableActionExecutionContext",
    "DurableActionHandler",
    "DurableActionPreparer",
    "DurableActionStatus",
    "DurableActionStore",
    "DurableActionTicket",
    "MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES",
    "MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH",
    "MAX_DURABLE_ACTION_PAYLOAD_BYTES",
    "PreparedDurableAction",
    "decode_durable_action_payload",
    "decode_durable_action_result",
    "encode_durable_action_payload",
    "encode_durable_action_result",
    "lookup_durable_action_handler",
    "lookup_durable_action_diagnostic",
    "lookup_durable_action_preparer",
    "register_durable_action_handler",
]


# Eager fixed-handler bootstrap. This is deliberately after every registry type
# and function so importing either module first is circular-safe.
from cron import jobs as _cron_jobs  # noqa: E402,F401
from tools import write_approval as _write_approval  # noqa: E402,F401
from tools import durable_execution_approval as _execution_approval  # noqa: E402
from tools import host_sudo as _host_sudo  # noqa: E402

_register_cron_resume_builtin(
    prepare=_cron_jobs.prepare_durable_cron_resume,
    execute=_cron_jobs.execute_durable_cron_resume,
    diagnose_unknown=_cron_jobs.diagnose_durable_cron_resume,
)

register_durable_action_handler(
    "memory.apply_pending",
    1,
    prepare=_write_approval.prepare_durable_memory_pending,
    execute=_write_approval.execute_durable_memory_pending,
    diagnose_unknown=_write_approval.diagnose_durable_memory_pending,
)

register_durable_action_handler(
    "skill.apply_pending",
    1,
    prepare=_write_approval.prepare_durable_skill_pending,
    execute=_write_approval.execute_durable_skill_pending,
    diagnose_unknown=_write_approval.diagnose_durable_skill_pending,
)

for _action_type in ("terminal.execute", "execute_code.execute"):
    register_durable_action_handler(
        _action_type,
        1,
        prepare=_execution_approval.prepare_durable_execution,
        execute=_execution_approval.execute_durable_execution,
    )

register_durable_action_handler(
    "host.sudo",
    1,
    prepare=_host_sudo.prepare_host_sudo,
    execute=_host_sudo.execute_host_sudo,
)
