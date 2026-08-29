"""Trusted local projections and mutations for durable ledgers."""

from __future__ import annotations

import math
import re
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

from gateway.durable_actions import DurableActionStatus
from gateway.durable_requests import DurableRequestStatus, DurableWakeStatus


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


@dataclass(frozen=True, slots=True)
class DurableRequestSummary:
    request_id: str
    kind: str
    status: str
    origin_type: str
    platform: str
    delivery_state: str
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

    def __init__(self, db_path: str | Path, *, wall_clock=time.time) -> None:
        self._db_path = Path(db_path)
        self._wall_clock = wall_clock

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
        if origin_type is not None and not origin_type:
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
        if action_type is not None and not action_type:
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
            where.append("ticket.created_at <= ?")
            params.append(current_time - minimum_age_seconds)
        predicate = f"WHERE {' AND '.join(where)}" if where else ""
        with closing(self._read_connection()) as conn:
            rows = conn.execute(
                f"""SELECT ticket.request_id, ticket.action_type,
                           ticket.action_version, ticket.status,
                           ticket.created_at, ticket.execution_claimed_at,
                           ticket.completed_at, ticket.notification_status
                    FROM durable_action_tickets AS ticket
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

    def get_action(
        self,
        request_id: str,
        *,
        now: float | None = None,
    ) -> DurableActionDetail | None:
        current_time = self._now(now)
        with closing(self._read_connection()) as conn:
            row = conn.execute(
                """SELECT ticket.request_id, ticket.action_type,
                          ticket.action_version, ticket.status,
                          ticket.created_at, ticket.execution_claimed_at,
                          ticket.execution_started_at, ticket.completed_at,
                          ticket.notification_status, ticket.outcome_reason,
                          ticket.notification_reason,
                          request.status AS request_status,
                          request.delivery_claimed_at
                   FROM durable_action_tickets AS ticket
                   JOIN durable_requests AS request
                     ON request.request_id = ticket.request_id
                   WHERE ticket.request_id = ?""",
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        summary = self._action_summary(row, current_time)
        return DurableActionDetail(
            **{field: getattr(summary, field) for field in summary.__slots__},
            outcome_reason=_structural_reason(row["outcome_reason"]),
            notification_reason=_structural_reason(row["notification_reason"]),
            execution_started_at=_optional_float(row["execution_started_at"]),
            request_status=row["request_status"],
            delivery_state=_delivery_state(row),
        )

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
        return DurableActionSummary(
            request_id=row["request_id"],
            action_type=row["action_type"],
            action_version=int(row["action_version"]),
            status=status,
            created_at=float(row["created_at"]),
            execution_claimed_at=_optional_float(row["execution_claimed_at"]),
            completed_at=_optional_float(row["completed_at"]),
            notification_status=notification_status,
            effective_reconciliation=None,
            actionable_age_seconds=(
                _age(
                    now,
                    row["execution_claimed_at"]
                    or row["completed_at"]
                    or row["created_at"],
                )
                if actionable
                else None
            ),
        )


__all__ = [
    "DurableActionDetail",
    "DurableActionSummary",
    "DurableOperationsStore",
    "DurableOperatorResult",
    "DurableRequestDetail",
    "DurableRequestSummary",
    "OperatorPage",
]
