"""Trusted action values persisted beside durable requests."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from hermes_state_common import (
    MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES,
    MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH,
    MAX_DURABLE_ACTION_PAYLOAD_BYTES,
)


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


@dataclass(frozen=True)
class DurableActionOutcome:
    status: DurableActionStatus
    reason: str | None = None
    result_json: str | None = None


__all__ = [
    "DurableActionNotificationStatus",
    "DurableActionOutcome",
    "DurableActionStatus",
    "DurableActionTicket",
    "MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES",
    "MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH",
    "MAX_DURABLE_ACTION_PAYLOAD_BYTES",
]
