"""Trusted action values persisted beside durable requests."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Callable

from hermes_state_common import (
    MAX_DURABLE_ACTION_OUTCOME_JSON_BYTES,
    MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH,
    MAX_DURABLE_ACTION_PAYLOAD_BYTES,
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


@dataclass(frozen=True)
class DurableActionCreateResult:
    request: DurableRequest
    ticket: DurableActionTicket
    created: bool


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
            _validate_utf8(self.reason, "reason")
            if len(self.reason) > MAX_DURABLE_ACTION_OUTCOME_REASON_LENGTH:
                raise ValueError("reason is too long")
        if self.result_json is not None:
            if not isinstance(self.result_json, str):
                raise TypeError("result_json must be a string or None")
            decode_durable_action_result(self.result_json)


DurableActionPreparer = Callable[..., PreparedDurableAction]
DurableActionHandler = Callable[
    [DurableActionExecutionContext], DurableActionOutcome
]
_DURABLE_ACTION_HANDLERS: dict[
    tuple[str, int], tuple[DurableActionPreparer, DurableActionHandler]
] = {}


def register_durable_action_handler(
    action_type: str,
    action_version: int,
    *,
    prepare: DurableActionPreparer,
    execute: DurableActionHandler,
) -> None:
    _validate_action_identity(action_type, action_version)
    if not callable(prepare):
        raise TypeError("prepare must be callable")
    if not callable(execute):
        raise TypeError("execute must be callable")
    key = (action_type, action_version)
    if key in _DURABLE_ACTION_HANDLERS:
        raise ValueError(f"durable action handler {key!r} is already registered")
    _DURABLE_ACTION_HANDLERS[key] = (prepare, execute)


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


__all__ = [
    "DurableActionCreateResult",
    "DurableActionNotificationStatus",
    "DurableActionOutcome",
    "DurableActionExecutionContext",
    "DurableActionHandler",
    "DurableActionPreparer",
    "DurableActionStatus",
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
    "lookup_durable_action_preparer",
    "register_durable_action_handler",
]
