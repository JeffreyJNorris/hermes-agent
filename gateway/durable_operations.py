"""Trusted local projections and mutations for durable ledgers."""

from __future__ import annotations

import math
import re
import secrets
import sqlite3
import time
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from enum import Enum
from threading import Lock
from typing import Generic, TypeVar

from gateway.durable_actions import (
    DurableActionDiagnostic,
    DurableActionExecutionContext,
    DurableActionNotificationStatus,
    DurableActionStatus,
    lookup_durable_action_diagnostic,
    lookup_durable_action_handler,
)
from gateway.durable_requests import (
    DurableRequestStatus,
    DurableResolveStatus,
    DurableWakeStatus,
)
from hermes_state_common import reconcile_durable_action_schema


_MAX_PAGE_SIZE = 100
_STRUCTURAL_REASON_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,255}$")
_REQUEST_STATUSES = frozenset(status.value for status in DurableRequestStatus)
_ACTION_STATUSES = frozenset(status.value for status in DurableActionStatus)
_ACTIONABLE_STATUSES = frozenset(
    {
        DurableActionStatus.WAITING_FOR_DECISION.value,
        DurableActionStatus.READY.value,
        DurableActionStatus.CLAIMED.value,
        DurableActionStatus.EXECUTING.value,
        DurableActionStatus.UNKNOWN.value,
    }
)
_NOTIFICATION_STATUSES = frozenset(
    status.value for status in DurableActionNotificationStatus
)
_DELIVERY_EVENTS = frozenset(
    {"native_to_text_fallback", "text_delivery_failed", "text_delivery_unknown"}
)
_MAX_EVENT_INCREMENT = 1_000_000
_DURABLE_EVENT_COUNTS: Counter[tuple[str, str]] = Counter()
_DURABLE_EVENT_LOCK = Lock()
_ACTION_AGE_SQL = """CASE
    WHEN ticket.status IN ('claimed','executing')
        THEN COALESCE(ticket.execution_claimed_at, ticket.updated_at, ticket.created_at)
    WHEN ticket.status = 'ready'
        THEN COALESCE(ticket.updated_at, ticket.created_at)
    WHEN ticket.status = 'unknown' OR ticket.notification_status IN ('pending','claimed')
        THEN COALESCE(ticket.completed_at, ticket.updated_at, ticket.created_at)
    ELSE ticket.created_at
END"""


class DurableActionReconciliationOutcome(str, Enum):
    CONFIRMED_APPLIED = "confirmed_applied"
    CONFIRMED_NOT_APPLIED = "confirmed_not_applied"
    ABANDONED = "abandoned"
    SUPERSEDED = "superseded"


_RECONCILIATION_OUTCOMES = frozenset(
    outcome.value for outcome in DurableActionReconciliationOutcome
)


@dataclass(frozen=True, slots=True)
class DurableRequestSummary:
    request_id: str
    kind: str
    status: str
    origin_type: str
    platform: str
    delivery_state: str
    delivery_mode: str
    resolution_source: str | None
    wake_status: str
    created_at: float
    expires_at: float
    resolved_at: float | None
    pending_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class DurableRequestDetail(DurableRequestSummary):
    terminal_reason: str | None
    delivery_attempts: int
    wake_attempts: int
    action_status: str | None


@dataclass(frozen=True, slots=True)
class DurableActionSummary:
    request_id: str
    action_type: str
    action_version: int
    status: str
    created_at: float
    execution_claimed_at: float | None
    completed_at: float | None
    notification_status: str
    effective_reconciliation: str | None
    actionable_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class DurableActionDetail(DurableActionSummary):
    outcome_reason: str | None
    notification_reason: str | None
    execution_started_at: float | None
    request_status: str
    delivery_state: str
    diagnostic: DurableActionDiagnostic | None


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class OperatorPage(Generic[T]):
    items: tuple[T, ...]
    next_offset: int | None


@dataclass(frozen=True, slots=True)
class DurableOperatorResult:
    ok: bool
    code: str
    request_id: str
    request_status: str | None = None
    action_status: str | None = None


@dataclass(frozen=True, slots=True)
class DurableHealthDatum:
    name: str
    value: int | float
    attributes: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class DurableHealthResult:
    severity: str
    code: str
    counts: tuple[tuple[str, int], ...]


def record_durable_command_response(status: DurableResolveStatus) -> None:
    if not isinstance(status, DurableResolveStatus):
        return
    with _DURABLE_EVENT_LOCK:
        _DURABLE_EVENT_COUNTS[("command_response", status.value)] += 1


def record_durable_multi_select_event(event: str) -> None:
    if event not in {"submissions", "invalid_sets"}:
        raise ValueError("unknown multi-select event")
    with _DURABLE_EVENT_LOCK:
        _DURABLE_EVENT_COUNTS[("multi_select", event)] += 1


def record_durable_delivery_event(event: str, *, count: int = 1) -> None:
    if (
        event not in _DELIVERY_EVENTS
        or type(count) is not int
        or not 1 <= count <= _MAX_EVENT_INCREMENT
    ):
        return
    with _DURABLE_EVENT_LOCK:
        _DURABLE_EVENT_COUNTS[("delivery", event)] += count


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


def _age(now: float, timestamp: object) -> float:
    return max(0.0, now - float(timestamp))


def _structural_reason(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if _STRUCTURAL_REASON_RE.fullmatch(text) else "redacted"


def _delivery_state(row: sqlite3.Row) -> str:
    status = row["request_status"]
    if status == DurableRequestStatus.PENDING_DELIVERY.value:
        return "claimed" if row["delivery_claimed_at"] is not None else "pending"
    if status == DurableRequestStatus.UNSUPPORTED.value:
        return "unsupported"
    if status == DurableRequestStatus.DELIVERY_FAILED.value:
        return "failed"
    if status == DurableRequestStatus.DELIVERY_UNKNOWN.value:
        return "unknown"
    return "delivered"


class DurableOperationsStore:
    """Bounded, redacted operator access to a local ``state.db``."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        wall_clock=time.time,
        reconcile_schema: bool = True,
    ) -> None:
        self._db_path = Path(db_path)
        self._wall_clock = wall_clock
        if reconcile_schema and self._db_path.exists():
            with closing(self._write_connection()) as conn:
                reconcile_durable_action_schema(conn)
                conn.commit()

    def _now(self, now: float | None) -> float:
        value = float(self._wall_clock() if now is None else now)
        if not math.isfinite(value):
            raise ValueError("wall clock must be finite")
        return value

    def _read_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            f"{self._db_path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=10.0,
        )
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only = ON")
            conn.execute("PRAGMA busy_timeout = 10000")
            return conn
        except Exception:
            conn.close()
            raise

    def _write_connection(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, timeout=10.0)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 10000")
            from hermes_state import apply_wal_with_fallback

            apply_wal_with_fallback(conn, db_label="state.db (durable_operations)")
            return conn
        except Exception:
            conn.close()
            raise

    @staticmethod
    def _page_values(limit: int, offset: int) -> tuple[int, int]:
        if type(limit) is not int:
            raise TypeError("limit must be an integer")
        if not 1 <= limit <= _MAX_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {_MAX_PAGE_SIZE}")
        if type(offset) is not int:
            raise TypeError("offset must be an integer")
        if offset < 0:
            raise ValueError("offset must not be negative")
        return limit, offset

    def list_requests(
        self,
        *,
        status: str | None = None,
        origin_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
        now: float | None = None,
    ) -> OperatorPage[DurableRequestSummary]:
        limit, offset = self._page_values(limit, offset)
        if status is not None and status not in _REQUEST_STATUSES:
            raise ValueError("unsupported request status")
        if origin_type is not None:
            if not isinstance(origin_type, str):
                raise TypeError("origin_type must be a string")
            if not origin_type:
                raise ValueError("origin_type must not be blank")
        current_time = self._now(now)
        where: list[str] = []
        params: list[object] = []
        if status is not None:
            where.append("request.status = ?")
            params.append(status)
        if origin_type is not None:
            where.append("request.origin_type = ?")
            params.append(origin_type)
        predicate = f"WHERE {' AND '.join(where)}" if where else ""
        with closing(self._read_connection()) as conn:
            rows = conn.execute(
                f"""SELECT request.request_id, request.kind,
                           request.status AS request_status, request.origin_type,
                           request.platform, request.delivery_claimed_at,
                           request.delivery_mode, request.resolution_source,
                           request.wake_status, request.created_at,
                           request.expires_at, request.resolved_at
                    FROM durable_requests AS request
                    {predicate}
                    ORDER BY request.created_at DESC, request.request_id DESC
                    LIMIT ? OFFSET ?""",
                (*params, limit + 1, offset),
            ).fetchall()
        items = tuple(self._request_summary(row, current_time) for row in rows[:limit])
        return OperatorPage(
            items=items,
            next_offset=offset + limit if len(rows) > limit else None,
        )

    def get_request(
        self,
        request_id: str,
        *,
        now: float | None = None,
    ) -> DurableRequestDetail | None:
        current_time = self._now(now)
        with closing(self._read_connection()) as conn:
            row = conn.execute(
                """SELECT request.request_id, request.kind,
                          request.status AS request_status, request.origin_type,
                          request.platform, request.delivery_claimed_at,
                          request.delivery_mode, request.resolution_source,
                          request.wake_status, request.created_at,
                          request.expires_at, request.resolved_at,
                          request.terminal_reason, request.delivery_attempts,
                          request.wake_attempts, ticket.status AS action_status
                   FROM durable_requests AS request
                   LEFT JOIN durable_action_tickets AS ticket
                     ON ticket.request_id = request.request_id
                   WHERE request.request_id = ?""",
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        summary = self._request_summary(row, current_time)
        return DurableRequestDetail(
            **{field: getattr(summary, field) for field in summary.__slots__},
            terminal_reason=_structural_reason(row["terminal_reason"]),
            delivery_attempts=int(row["delivery_attempts"] or 0),
            wake_attempts=int(row["wake_attempts"] or 0),
            action_status=row["action_status"],
        )

    def list_actions(
        self,
        *,
        status: str | None = None,
        action_type: str | None = None,
        minimum_age_seconds: float | None = None,
        limit: int = 50,
        offset: int = 0,
        now: float | None = None,
    ) -> OperatorPage[DurableActionSummary]:
        limit, offset = self._page_values(limit, offset)
        if status is not None and status not in _ACTION_STATUSES:
            raise ValueError("unsupported action status")
        if action_type is not None:
            if not isinstance(action_type, str):
                raise TypeError("action_type must be a string")
            if not action_type:
                raise ValueError("action_type must not be blank")
        current_time = self._now(now)
        if minimum_age_seconds is not None:
            minimum_age_seconds = float(minimum_age_seconds)
            if not math.isfinite(minimum_age_seconds) or minimum_age_seconds < 0:
                raise ValueError("minimum_age_seconds must be finite and non-negative")
        where: list[str] = []
        params: list[object] = []
        if status is not None:
            where.append("ticket.status = ?")
            params.append(status)
        if action_type is not None:
            where.append("ticket.action_type = ?")
            params.append(action_type)
        if minimum_age_seconds is not None:
            where.append(f"{_ACTION_AGE_SQL} <= ?")
            params.append(current_time - minimum_age_seconds)
        predicate = f"WHERE {' AND '.join(where)}" if where else ""
        with closing(self._read_connection()) as conn:
            rows = conn.execute(
                f"""SELECT ticket.request_id, ticket.action_type,
                           ticket.action_version, ticket.status,
                           ticket.created_at, ticket.updated_at,
                           ticket.execution_claimed_at,
                           ticket.completed_at, ticket.notification_status,
                           reconciliation.outcome AS effective_reconciliation
                    FROM durable_action_tickets AS ticket
                    LEFT JOIN durable_action_reconciliations AS reconciliation
                      ON reconciliation.request_id = ticket.request_id
                    {predicate}
                    ORDER BY ticket.created_at DESC, ticket.request_id DESC
                    LIMIT ? OFFSET ?""",
                (*params, limit + 1, offset),
            ).fetchall()
        items = tuple(self._action_summary(row, current_time) for row in rows[:limit])
        return OperatorPage(
            items=items,
            next_offset=offset + limit if len(rows) > limit else None,
        )

    def inspect_action(
        self,
        request_id: str,
        *,
        diagnose: bool = False,
        now: float | None = None,
    ) -> DurableActionDetail | None:
        current_time = self._now(now)
        with closing(self._read_connection()) as conn:
            row = conn.execute(
                """SELECT ticket.request_id, ticket.action_type,
                          ticket.action_version, ticket.status,
                          ticket.created_at, ticket.updated_at,
                          ticket.execution_claimed_at,
                          ticket.execution_started_at, ticket.completed_at,
                          ticket.notification_status, ticket.outcome_reason,
                          ticket.notification_reason,
                          ticket.payload_json, ticket.fingerprint,
                          ticket.idempotency_key,
                          request.status AS request_status,
                          request.delivery_claimed_at,
                          reconciliation.outcome AS effective_reconciliation
                   FROM durable_action_tickets AS ticket
                   JOIN durable_requests AS request
                     ON request.request_id = ticket.request_id
                   LEFT JOIN durable_action_reconciliations AS reconciliation
                     ON reconciliation.request_id = ticket.request_id
                   WHERE ticket.request_id = ?""",
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        summary = self._action_summary(row, current_time)
        diagnostic = None
        if diagnose:
            if row["status"] != DurableActionStatus.UNKNOWN.value:
                diagnostic = DurableActionDiagnostic("not_unknown")
            else:
                handler = lookup_durable_action_diagnostic(
                    row["action_type"], int(row["action_version"])
                )
                if handler is None:
                    diagnostic = DurableActionDiagnostic("handler_unavailable")
                else:
                    try:
                        context = DurableActionExecutionContext(
                            request_id=row["request_id"],
                            action_type=row["action_type"],
                            action_version=int(row["action_version"]),
                            payload_json=row["payload_json"],
                            fingerprint=row["fingerprint"],
                            idempotency_key=row["idempotency_key"],
                        )
                        diagnostic = handler(context)
                        if not isinstance(diagnostic, DurableActionDiagnostic):
                            raise TypeError("diagnostic returned an invalid result")
                    except Exception as exc:
                        diagnostic = DurableActionDiagnostic(
                            "diagnostic_failed",
                            (("exception", type(exc).__name__),),
                        )
        return DurableActionDetail(
            **{field: getattr(summary, field) for field in summary.__slots__},
            outcome_reason=_structural_reason(row["outcome_reason"]),
            notification_reason=_structural_reason(row["notification_reason"]),
            execution_started_at=_optional_float(row["execution_started_at"]),
            request_status=row["request_status"],
            delivery_state=_delivery_state(row),
            diagnostic=diagnostic,
        )

    def get_action(
        self,
        request_id: str,
        *,
        now: float | None = None,
    ) -> DurableActionDetail | None:
        return self.inspect_action(request_id, now=now)

    def cancel_request(
        self,
        request_id: str,
        *,
        now: float | None = None,
    ) -> DurableOperatorResult:
        current_time = self._now(now)
        with closing(self._write_connection()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """SELECT request.status AS request_status,
                              ticket.status AS action_status,
                              ticket.execution_claim
                       FROM durable_requests AS request
                       LEFT JOIN durable_action_tickets AS ticket
                         ON ticket.request_id = request.request_id
                       WHERE request.request_id = ?""",
                    (request_id,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return DurableOperatorResult(False, "not_found", request_id)
                if row["action_status"] is not None:
                    result = self._cancel_action_locked(
                        conn, request_id, row, current_time
                    )
                else:
                    result = self._cancel_generic_request_locked(
                        conn, request_id, row["request_status"], current_time
                    )
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    def authorize_action(
        self,
        request_id: str,
        *,
        now: float | None = None,
    ) -> DurableOperatorResult:
        """Authorize a waiting action from the trusted local operator rail."""
        current_time = self._now(now)
        with closing(self._write_connection()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """SELECT request.status AS request_status,
                              request.expires_at,
                              ticket.status AS action_status,
                              ticket.execution_claim
                       FROM durable_action_tickets AS ticket
                       JOIN durable_requests AS request
                         ON request.request_id = ticket.request_id
                       WHERE ticket.request_id = ?""",
                    (request_id,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return DurableOperatorResult(False, "not_found", request_id)
                action_status = row["action_status"]
                if (
                    float(row["expires_at"]) <= current_time
                    and action_status == DurableActionStatus.WAITING_FOR_DECISION.value
                ):
                    conn.execute(
                        """UPDATE durable_requests
                           SET status = 'expired', terminal_reason = 'deadline_elapsed',
                               resolved_at = ?, updated_at = ?, delivery_claim = NULL,
                               delivery_claimed_at = NULL
                           WHERE request_id = ?""",
                        (current_time, current_time, request_id),
                    )
                    conn.execute(
                        """UPDATE durable_action_tickets
                           SET status = 'expired', outcome_reason = 'deadline_elapsed',
                               completed_at = ?, notification_status = 'pending',
                               updated_at = ? WHERE request_id = ?""",
                        (current_time, current_time, request_id),
                    )
                    conn.commit()
                    return DurableOperatorResult(
                        False, "expired", request_id,
                        request_status=DurableRequestStatus.EXPIRED.value,
                        action_status=DurableActionStatus.EXPIRED.value,
                    )
                if action_status in {
                    DurableActionStatus.CLAIMED.value,
                    DurableActionStatus.EXECUTING.value,
                } or row["execution_claim"] is not None:
                    conn.commit()
                    return DurableOperatorResult(
                        False, "execution_in_progress", request_id,
                        request_status=row["request_status"], action_status=action_status,
                    )
                if action_status == DurableActionStatus.READY.value:
                    conn.commit()
                    return DurableOperatorResult(
                        True, "already_authorized", request_id,
                        request_status=row["request_status"], action_status=action_status,
                    )
                if action_status != DurableActionStatus.WAITING_FOR_DECISION.value:
                    conn.commit()
                    return DurableOperatorResult(
                        False, "already_terminal", request_id,
                        request_status=row["request_status"], action_status=action_status,
                    )
                request_changed = conn.execute(
                    """UPDATE durable_requests
                       SET status = 'selected', selected_option_id = 'approve',
                           resolved_by_actor_id = 'operator:manual',
                           terminal_reason = NULL, resolved_at = ?, updated_at = ?,
                           delivery_claim = NULL, delivery_claimed_at = NULL
                       WHERE request_id = ? AND status IN ('pending_delivery', 'open')""",
                    (current_time, current_time, request_id),
                ).rowcount
                action_changed = conn.execute(
                    """UPDATE durable_action_tickets
                       SET status = 'ready', outcome_reason = NULL,
                           completed_at = NULL, updated_at = ?
                       WHERE request_id = ? AND status = 'waiting_for_decision'""",
                    (current_time, request_id),
                ).rowcount
                if request_changed != 1 or action_changed != 1:
                    conn.rollback()
                    return DurableOperatorResult(False, "concurrent_update", request_id)
                conn.commit()
                return DurableOperatorResult(
                    True, "authorized", request_id,
                    request_status=DurableRequestStatus.SELECTED.value,
                    action_status=DurableActionStatus.READY.value,
                )
            except Exception:
                conn.rollback()
                raise

    def reconcile_action(
        self,
        request_id: str,
        *,
        outcome: DurableActionReconciliationOutcome | str,
        operator_identity: str,
        note: str | None = None,
        superseded_by_request_id: str | None = None,
        now: float | None = None,
    ) -> DurableOperatorResult:
        try:
            outcome_value = DurableActionReconciliationOutcome(outcome).value
        except (TypeError, ValueError) as exc:
            raise ValueError("unsupported reconciliation outcome") from exc
        self._validate_reconciliation_text(
            operator_identity, field="operator_identity", maximum=128, required=True
        )
        if note is not None:
            self._validate_reconciliation_text(
                note, field="note", maximum=1024, required=False
            )
        if outcome_value == DurableActionReconciliationOutcome.SUPERSEDED.value:
            if not superseded_by_request_id or superseded_by_request_id == request_id:
                return DurableOperatorResult(
                    False, "invalid_superseding_receipt", request_id
                )
        elif superseded_by_request_id is not None:
            raise ValueError("superseding receipt requires superseded outcome")
        current_time = self._now(now)
        with closing(self._write_connection()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """SELECT ticket.status AS action_status, ticket.action_type,
                              request.status AS request_status,
                              reconciliation.outcome AS existing_outcome
                       FROM durable_action_tickets AS ticket
                       LEFT JOIN durable_requests AS request
                         ON request.request_id = ticket.request_id
                       LEFT JOIN durable_action_reconciliations AS reconciliation
                         ON reconciliation.request_id = ticket.request_id
                       WHERE ticket.request_id = ?""",
                    (request_id,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return DurableOperatorResult(False, "not_found", request_id)
                if row["existing_outcome"] is not None:
                    conn.commit()
                    return DurableOperatorResult(
                        False,
                        "already_reconciled",
                        request_id,
                        request_status=row["request_status"],
                        action_status=row["action_status"],
                    )
                if row["action_status"] != DurableActionStatus.UNKNOWN.value:
                    conn.commit()
                    return DurableOperatorResult(
                        False,
                        "invalid_state",
                        request_id,
                        request_status=row["request_status"],
                        action_status=row["action_status"],
                    )
                if outcome_value == DurableActionReconciliationOutcome.SUPERSEDED.value:
                    successor = conn.execute(
                        """SELECT action_type FROM durable_action_tickets
                           WHERE request_id = ?""",
                        (superseded_by_request_id,),
                    ).fetchone()
                    if successor is None or successor["action_type"] != row["action_type"]:
                        conn.commit()
                        return DurableOperatorResult(
                            False,
                            "invalid_superseding_receipt",
                            request_id,
                            request_status=row["request_status"],
                            action_status=row["action_status"],
                        )
                conn.execute(
                    """INSERT INTO durable_action_reconciliations (
                           reconciliation_id, request_id, outcome,
                           operator_identity, note, superseded_by_request_id, created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        f"drc_{secrets.token_urlsafe(18)}",
                        request_id,
                        outcome_value,
                        operator_identity,
                        note,
                        superseded_by_request_id,
                        current_time,
                    ),
                )
                conn.commit()
                return DurableOperatorResult(
                    True,
                    "reconciled",
                    request_id,
                    request_status=row["request_status"],
                    action_status=row["action_status"],
                )
            except Exception:
                conn.rollback()
                raise

    def collect_health_data(
        self,
        *,
        now: float | None = None,
    ) -> tuple[DurableHealthDatum, ...]:
        from gateway.durable_action_runner import DEFAULT_STALE_CLAIM_SECONDS
        from gateway.durable_requests import (
            DELIVERY_CLAIM_STALE_SECONDS,
            WAKE_CLAIM_STALE_SECONDS,
        )

        current_time = self._now(now)
        metrics: dict[
            tuple[str, tuple[tuple[str, str], ...]], DurableHealthDatum
        ] = {}

        def add(name: str, value: object, **attributes: object) -> None:
            labels = tuple(
                sorted(
                    (key, self._bounded_health_label(item))
                    for key, item in attributes.items()
                )
            )
            key = (name, labels)
            number = (
                float(value)
                if name == "hermes_durable_oldest_backlog_seconds"
                else int(value)
            )
            previous = metrics.get(key)
            metrics[key] = DurableHealthDatum(
                name,
                number + (previous.value if previous is not None else 0),
                labels,
            )

        with closing(self._read_connection()) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(durable_requests)")}
            if "selection_mode" in columns:
                active = conn.execute(
                    "SELECT COUNT(*) FROM durable_requests WHERE selection_mode = 'multi' "
                    "AND status IN ('pending_delivery', 'open')"
                ).fetchone()[0]
                stuck = conn.execute(
                    "SELECT COUNT(*) FROM durable_requests WHERE selection_mode = 'multi' "
                    "AND status IN ('pending_delivery', 'open') AND expires_at <= ?",
                    (current_time,),
                ).fetchone()[0]
                add("hermes_durable_multi_select_active", active)
                add("hermes_durable_multi_select_stuck", stuck)
            for row in conn.execute(
                """SELECT status, platform, COUNT(*) AS count
                   FROM durable_requests GROUP BY status, platform
                   ORDER BY status, platform"""
            ):
                add(
                    "hermes_durable_requests",
                    row["count"],
                    status=(row["status"] if row["status"] in _REQUEST_STATUSES else "unknown"),
                    platform=row["platform"],
                )
            for row in conn.execute(
                """SELECT status, action_type, COUNT(*) AS count
                   FROM durable_action_tickets GROUP BY status, action_type
                   ORDER BY status, action_type"""
            ):
                add(
                    "hermes_durable_actions",
                    row["count"],
                    status=(row["status"] if row["status"] in _ACTION_STATUSES else "unknown"),
                    action_type=row["action_type"],
                )
            for row in conn.execute(
                """SELECT ticket.notification_status AS status, request.origin_type,
                          COUNT(*) AS count
                   FROM durable_action_tickets AS ticket
                   JOIN durable_requests AS request ON request.request_id = ticket.request_id
                   GROUP BY ticket.notification_status, request.origin_type
                   ORDER BY ticket.notification_status, request.origin_type"""
            ):
                add(
                    "hermes_durable_notifications",
                    row["count"],
                    status=(
                        row["status"]
                        if row["status"] in _NOTIFICATION_STATUSES
                        else "unknown"
                    ),
                    origin_type=row["origin_type"],
                )
            stale_queries = (
                (
                    "delivery",
                    "SELECT COUNT(*) FROM durable_requests WHERE status = 'pending_delivery' "
                    "AND delivery_claimed_at IS NOT NULL AND delivery_claimed_at <= ?",
                    current_time - DELIVERY_CLAIM_STALE_SECONDS,
                ),
                (
                    "wake",
                    "SELECT COUNT(*) FROM durable_requests WHERE wake_status = 'claimed' "
                    "AND wake_claimed_at IS NOT NULL AND wake_claimed_at <= ?",
                    current_time - WAKE_CLAIM_STALE_SECONDS,
                ),
                (
                    "execution",
                    "SELECT COUNT(*) FROM durable_action_tickets WHERE status IN ('claimed','executing') "
                    "AND execution_claimed_at IS NOT NULL AND execution_claimed_at <= ?",
                    current_time - DEFAULT_STALE_CLAIM_SECONDS,
                ),
                (
                    "notification",
                    "SELECT COUNT(*) FROM durable_action_tickets WHERE notification_status = 'claimed' "
                    "AND notification_claimed_at IS NOT NULL AND notification_claimed_at <= ?",
                    current_time - DEFAULT_STALE_CLAIM_SECONDS,
                ),
            )
            for claim_type, sql, cutoff in stale_queries:
                count = int(conn.execute(sql, (cutoff,)).fetchone()[0])
                if count:
                    add(
                        "hermes_durable_stale_claims",
                        count,
                        claim_type=claim_type,
                    )
            for row in conn.execute(
                """SELECT ticket.action_type,
                          CASE WHEN reconciliation.request_id IS NULL THEN 'false' ELSE 'true' END AS reconciled,
                          COUNT(*) AS count
                   FROM durable_action_tickets AS ticket
                   LEFT JOIN durable_action_reconciliations AS reconciliation
                     ON reconciliation.request_id = ticket.request_id
                   WHERE ticket.status = 'unknown'
                   GROUP BY ticket.action_type, reconciled
                   ORDER BY ticket.action_type, reconciled"""
            ):
                add(
                    "hermes_durable_unknown_actions",
                    row["count"],
                    action_type=row["action_type"],
                    reconciled=row["reconciled"],
                )
            for row in conn.execute(
                """SELECT ticket.action_type, reconciliation.outcome, COUNT(*) AS count
                   FROM durable_action_reconciliations AS reconciliation
                   JOIN durable_action_tickets AS ticket
                     ON ticket.request_id = reconciliation.request_id
                   GROUP BY ticket.action_type, reconciliation.outcome
                   ORDER BY ticket.action_type, reconciliation.outcome"""
            ):
                add(
                    "hermes_durable_reconciliations",
                    row["count"],
                    action_type=row["action_type"],
                    outcome=(
                        row["outcome"]
                        if row["outcome"] in _RECONCILIATION_OUTCOMES
                        else "unknown"
                    ),
                )
            backlog_queries = (
                (
                    "delivery",
                    "SELECT MIN(created_at) FROM durable_requests WHERE status = 'pending_delivery'",
                ),
                (
                    "text_command_delivery",
                    "SELECT MIN(created_at) FROM durable_requests "
                    "WHERE status = 'pending_delivery' AND delivery_mode = 'text_command'",
                ),
                (
                    "wake",
                    "SELECT MIN(COALESCE(wake_claimed_at, resolved_at, updated_at)) "
                    "FROM durable_requests WHERE wake_status IN ('pending','claimed')",
                ),
                (
                    "action",
                    "SELECT MIN(COALESCE(execution_claimed_at, updated_at, created_at)) "
                    "FROM durable_action_tickets WHERE status IN ('ready','claimed','executing')",
                ),
                (
                    "notification",
                    "SELECT MIN(COALESCE(notification_claimed_at, completed_at, updated_at)) "
                    "FROM durable_action_tickets WHERE notification_status IN ('pending','claimed')",
                ),
                (
                    "unknown",
                    "SELECT MIN(ticket.completed_at) FROM durable_action_tickets AS ticket "
                    "LEFT JOIN durable_action_reconciliations AS reconciliation "
                    "ON reconciliation.request_id = ticket.request_id "
                    "WHERE ticket.status = 'unknown' AND reconciliation.request_id IS NULL",
                ),
            )
            for queue_type, sql in backlog_queries:
                timestamp = conn.execute(sql).fetchone()[0]
                if timestamp is not None:
                    add(
                        "hermes_durable_oldest_backlog_seconds",
                        _age(current_time, timestamp),
                        queue_type=queue_type,
                    )
        with _DURABLE_EVENT_LOCK:
            event_counts = tuple(_DURABLE_EVENT_COUNTS.items())
        for (event_type, value), count in event_counts:
            if event_type == "command_response":
                add("hermes_durable_command_responses", count, result=value)
            elif event_type == "multi_select":
                add(f"hermes_durable_multi_select_{value}", count)
            else:
                add("hermes_durable_delivery_events", count, event=value)
        return tuple(metrics.values())

    def check_health(
        self,
        *,
        requests_enabled: bool = False,
        actions_enabled: bool = False,
        allowed_actions: object = (),
        request_worker_alive: bool | None = None,
        action_worker_alive: bool | None = None,
        now: float | None = None,
    ) -> DurableHealthResult:
        from gateway.durable_action_runner import DEFAULT_STALE_CLAIM_SECONDS
        from gateway.durable_requests import (
            DELIVERY_CLAIM_STALE_SECONDS,
            WAKE_CLAIM_STALE_SECONDS,
        )

        if (
            not isinstance(allowed_actions, (list, tuple))
            or any(type(item) is not str for item in allowed_actions)
        ):
            allowed: tuple[str, ...] = ()
            invalid_allowlist = True
        else:
            allowed = tuple(allowed_actions)
            invalid_allowlist = len(set(allowed)) != len(allowed)
        missing_workers = int(
            requests_enabled and request_worker_alive is not True
        ) + int(actions_enabled and action_worker_alive is not True)
        worker_unknown = (
            requests_enabled and request_worker_alive is None
        ) or (actions_enabled and action_worker_alive is None)
        configured_handlers = {(action_type, 1) for action_type in allowed}
        configured_missing_handlers = sum(
            lookup_durable_action_handler(*key) is None
            for key in configured_handlers
        )

        def uninitialized_result() -> DurableHealthResult:
            counts = {}
            if configured_missing_handlers:
                counts["missing_handlers"] = configured_missing_handlers
            if invalid_allowlist:
                counts["invalid_allowlist"] = 1
            if missing_workers:
                counts["workers"] = missing_workers
            if invalid_allowlist or configured_missing_handlers:
                code = "invalid_allowlist"
            elif missing_workers:
                code = (
                    "worker_confirmation_unavailable"
                    if worker_unknown
                    else "worker_unavailable"
                )
            else:
                return DurableHealthResult("ok", "not_initialized", ())
            return DurableHealthResult("warning", code, tuple(sorted(counts.items())))

        if not self._db_path.exists():
            return uninitialized_result()
        current_time = self._now(now)
        backlog_cutoff = current_time - 300.0
        counts: dict[str, int] = {}
        try:
            with closing(self._read_connection()) as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                durable_tables = {
                    "durable_requests",
                    "durable_action_tickets",
                    "durable_action_reconciliations",
                }
                if not (tables & durable_tables):
                    return uninitialized_result()
                if not durable_tables <= tables:
                    return DurableHealthResult("critical", "impossible_state", ())
                queries = {
                    "pending_requests": (
                        "SELECT COUNT(*) FROM durable_requests "
                        "WHERE status = 'pending_delivery' AND created_at <= ?",
                        (backlog_cutoff,),
                    ),
                    "pending_text_command_requests": (
                        "SELECT COUNT(*) FROM durable_requests "
                        "WHERE status = 'pending_delivery' AND delivery_mode = 'text_command' "
                        "AND created_at <= ?",
                        (backlog_cutoff,),
                    ),
                    "ready_actions": (
                        "SELECT COUNT(*) FROM durable_action_tickets "
                        "WHERE status = 'ready' AND updated_at <= ?",
                        (backlog_cutoff,),
                    ),
                    "pending_notifications": (
                        "SELECT COUNT(*) FROM durable_action_tickets "
                        "WHERE notification_status = 'pending' "
                        "AND COALESCE(completed_at, updated_at) <= ?",
                        (backlog_cutoff,),
                    ),
                    "stale_delivery_claims": (
                        "SELECT COUNT(*) FROM durable_requests "
                        "WHERE status = 'pending_delivery' AND delivery_claimed_at <= ?",
                        (current_time - DELIVERY_CLAIM_STALE_SECONDS,),
                    ),
                    "stale_wake_claims": (
                        "SELECT COUNT(*) FROM durable_requests "
                        "WHERE wake_status = 'claimed' AND wake_claimed_at <= ?",
                        (current_time - WAKE_CLAIM_STALE_SECONDS,),
                    ),
                    "stale_action_claims": (
                        "SELECT COUNT(*) FROM durable_action_tickets "
                        "WHERE status IN ('claimed','executing') "
                        "AND execution_claimed_at <= ?",
                        (current_time - DEFAULT_STALE_CLAIM_SECONDS,),
                    ),
                    "stale_notification_claims": (
                        "SELECT COUNT(*) FROM durable_action_tickets "
                        "WHERE notification_status = 'claimed' AND notification_claimed_at <= ?",
                        (current_time - DEFAULT_STALE_CLAIM_SECONDS,),
                    ),
                    "unreconciled_unknown": (
                        "SELECT COUNT(*) FROM durable_action_tickets AS ticket "
                        "LEFT JOIN durable_action_reconciliations AS reconciliation "
                        "ON reconciliation.request_id = ticket.request_id "
                        "WHERE ticket.status = 'unknown' AND reconciliation.request_id IS NULL",
                        (),
                    ),
                    "orphan_actions": (
                        "SELECT COUNT(*) FROM durable_action_tickets AS ticket "
                        "LEFT JOIN durable_requests AS request ON request.request_id = ticket.request_id "
                        "WHERE request.request_id IS NULL",
                        (),
                    ),
                }
                for name, (sql, params) in queries.items():
                    value = int(conn.execute(sql, params).fetchone()[0])
                    if value:
                        counts[name] = value
                actionable_handlers = conn.execute(
                    """SELECT DISTINCT action_type, action_version
                       FROM durable_action_tickets
                       WHERE status IN ('ready','claimed','executing')"""
                ).fetchall()
        except (OSError, sqlite3.Error):
            return DurableHealthResult("critical", "database_unavailable", ())

        if counts.get("orphan_actions"):
            return DurableHealthResult(
                "critical", "impossible_state", tuple(sorted(counts.items()))
            )
        if counts.get("unreconciled_unknown"):
            return DurableHealthResult(
                "critical", "unreconciled_unknown", tuple(sorted(counts.items()))
            )

        handler_keys = configured_handlers | {
            (row["action_type"], int(row["action_version"]))
            for row in actionable_handlers
        }
        missing_handlers = sum(
            lookup_durable_action_handler(*key) is None for key in handler_keys
        )
        if missing_handlers:
            counts["missing_handlers"] = missing_handlers
        if invalid_allowlist:
            counts["invalid_allowlist"] = 1
        if missing_workers:
            counts["workers"] = missing_workers

        backlog_keys = {
            "pending_requests",
            "pending_text_command_requests",
            "ready_actions",
            "pending_notifications",
            "stale_delivery_claims",
            "stale_wake_claims",
            "stale_action_claims",
            "stale_notification_claims",
        }
        if any(counts.get(key) for key in backlog_keys):
            code = "actionable_backlog"
        elif invalid_allowlist or missing_handlers:
            code = "invalid_allowlist"
        elif missing_workers:
            code = (
                "worker_confirmation_unavailable"
                if worker_unknown
                else "worker_unavailable"
            )
        else:
            return DurableHealthResult("ok", "healthy", tuple(sorted(counts.items())))
        return DurableHealthResult("warning", code, tuple(sorted(counts.items())))

    @staticmethod
    def _bounded_health_label(value: object) -> str:
        text = str(value or "unknown")
        return (
            text
            if len(text) <= 128
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", text) is not None
            else "unknown"
        )

    @staticmethod
    def _validate_reconciliation_text(
        value: str,
        *,
        field: str,
        maximum: int,
        required: bool,
    ) -> None:
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string")
        if required and not value.strip():
            raise ValueError(f"{field} must not be blank")
        if len(value) > maximum:
            raise ValueError(f"{field} is too long")
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError(f"{field} must be valid UTF-8") from exc

    def cancel_action(
        self,
        request_id: str,
        *,
        now: float | None = None,
    ) -> DurableOperatorResult:
        current_time = self._now(now)
        with closing(self._write_connection()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """SELECT request.status AS request_status, ticket.status AS action_status,
                              ticket.execution_claim
                       FROM durable_action_tickets AS ticket
                       JOIN durable_requests AS request
                         ON request.request_id = ticket.request_id
                       WHERE ticket.request_id = ?""",
                    (request_id,),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return DurableOperatorResult(False, "not_found", request_id)
                result = self._cancel_action_locked(conn, request_id, row, current_time)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _cancel_generic_request_locked(
        conn: sqlite3.Connection,
        request_id: str,
        request_status: str,
        now: float,
    ) -> DurableOperatorResult:
        active = (
            DurableRequestStatus.PENDING_DELIVERY.value,
            DurableRequestStatus.OPEN.value,
        )
        if request_status not in active:
            return DurableOperatorResult(
                False,
                "already_terminal",
                request_id,
                request_status=request_status,
            )
        changed = conn.execute(
            """UPDATE durable_requests
               SET status = 'cancelled', terminal_reason = 'cancelled_by_operator',
                   updated_at = ?, resolved_at = ?, delivery_claim = NULL,
                   delivery_claimed_at = NULL,
                   wake_status = CASE
                       WHEN wake_requested = 1 AND wake_status = 'not_requested'
                           THEN 'pending'
                       ELSE wake_status
                   END,
                   wake_claim = NULL, wake_claimed_at = NULL, wake_reason = NULL
               WHERE request_id = ? AND status IN (?, ?)""",
            (now, now, request_id, *active),
        ).rowcount
        if changed != 1:
            return DurableOperatorResult(
                False,
                "concurrent_update",
                request_id,
                request_status=request_status,
            )
        return DurableOperatorResult(
            True,
            "cancelled",
            request_id,
            request_status=DurableRequestStatus.CANCELLED.value,
        )

    def _cancel_action_locked(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        row: sqlite3.Row,
        now: float,
    ) -> DurableOperatorResult:
        request_status = row["request_status"]
        action_status = row["action_status"]
        if action_status in {
            DurableActionStatus.CLAIMED.value,
            DurableActionStatus.EXECUTING.value,
        } or (
            action_status == DurableActionStatus.READY.value
            and row["execution_claim"] is not None
        ):
            return DurableOperatorResult(
                False,
                "execution_in_progress",
                request_id,
                request_status=request_status,
                action_status=action_status,
            )
        if action_status not in {
            DurableActionStatus.WAITING_FOR_DECISION.value,
            DurableActionStatus.READY.value,
        }:
            return DurableOperatorResult(
                False,
                "already_terminal",
                request_id,
                request_status=request_status,
                action_status=action_status,
            )
        changed = conn.execute(
            """UPDATE durable_action_tickets
               SET status = 'cancelled', execution_claim = NULL,
                   execution_claimed_at = NULL,
                   outcome_reason = 'cancelled_by_operator', outcome_json = NULL,
                   completed_at = ?, notification_status = 'pending',
                   notification_claim = NULL, notification_claimed_at = NULL,
                   notification_reason = NULL, updated_at = ?
               WHERE request_id = ? AND status = ? AND execution_claim IS NULL""",
            (now, now, request_id, action_status),
        ).rowcount
        if changed != 1:
            conn.rollback()
            return DurableOperatorResult(
                False,
                "concurrent_update",
                request_id,
                request_status=request_status,
                action_status=action_status,
            )
        if action_status == DurableActionStatus.WAITING_FOR_DECISION.value:
            request_result = self._cancel_generic_request_locked(
                conn, request_id, request_status, now
            )
            if not request_result.ok:
                conn.rollback()
                return DurableOperatorResult(
                    False,
                    "concurrent_update",
                    request_id,
                    request_status=request_status,
                    action_status=action_status,
                )
            request_status = DurableRequestStatus.CANCELLED.value
        return DurableOperatorResult(
            True,
            "cancelled",
            request_id,
            request_status=request_status,
            action_status=DurableActionStatus.CANCELLED.value,
        )

    @staticmethod
    def _request_summary(row: sqlite3.Row, now: float) -> DurableRequestSummary:
        status = row["request_status"]
        return DurableRequestSummary(
            request_id=row["request_id"],
            kind=row["kind"],
            status=status,
            origin_type=row["origin_type"],
            platform=row["platform"],
            delivery_state=_delivery_state(row),
            delivery_mode=row["delivery_mode"],
            resolution_source=row["resolution_source"],
            wake_status=row["wake_status"] or DurableWakeStatus.NOT_REQUESTED.value,
            created_at=float(row["created_at"]),
            expires_at=float(row["expires_at"]),
            resolved_at=_optional_float(row["resolved_at"]),
            pending_age_seconds=(
                _age(now, row["created_at"])
                if status
                in {
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    DurableRequestStatus.OPEN.value,
                }
                else None
            ),
        )

    @staticmethod
    def _action_summary(row: sqlite3.Row, now: float) -> DurableActionSummary:
        status = row["status"]
        notification_status = row["notification_status"]
        actionable = status in _ACTIONABLE_STATUSES or notification_status in {
            "pending",
            "claimed",
        }
        if status in {"claimed", "executing"}:
            actionable_since = row["execution_claimed_at"] or row["updated_at"]
        elif status == "ready":
            actionable_since = row["updated_at"]
        elif status == "unknown" or notification_status in {"pending", "claimed"}:
            actionable_since = row["completed_at"] or row["updated_at"]
        else:
            actionable_since = row["created_at"]
        return DurableActionSummary(
            request_id=row["request_id"],
            action_type=row["action_type"],
            action_version=int(row["action_version"]),
            status=status,
            created_at=float(row["created_at"]),
            execution_claimed_at=_optional_float(row["execution_claimed_at"]),
            completed_at=_optional_float(row["completed_at"]),
            notification_status=notification_status,
            effective_reconciliation=row["effective_reconciliation"],
            actionable_age_seconds=(
                _age(now, actionable_since)
                if actionable
                else None
            ),
        )


__all__ = [
    "DurableActionDetail",
    "DurableActionReconciliationOutcome",
    "DurableActionSummary",
    "DurableHealthDatum",
    "DurableHealthResult",
    "DurableOperationsStore",
    "DurableOperatorResult",
    "DurableRequestDetail",
    "DurableRequestSummary",
    "OperatorPage",
]
