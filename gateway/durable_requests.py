"""Persisted one-shot finite-choice requests.

This module is intentionally separate from :mod:`gateway.interactive`.  The
interactive registry is a live-turn primitive backed by process-local waiters;
durable requests are detached records backed by ``state.db``.  A provider
message or component view is only a presentation of the database row.

Request display text is caller-controlled.  Callers must provide pre-redacted,
display-safe title, body, and option labels.  This module logs structural data
only and never logs request content at ordinary log levels.
"""

from __future__ import annotations

import json
import logging
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
from typing import Iterable, Iterator, Mapping

from gateway.durable_actions import (
    DurableActionCreateResult,
    DurableActionNotificationStatus,
    DurableActionStatus,
    DurableActionTicket,
    PreparedDurableAction,
)
from gateway.interactive import (
    MAX_PROMPT_BODY_LENGTH,
    MAX_PROMPT_KIND_LENGTH,
    MAX_PROMPT_OPTION_ID_LENGTH,
    MAX_PROMPT_OPTION_LABEL_LENGTH,
    MAX_PROMPT_OPTIONS,
    MAX_MULTI_SELECT_OPTIONS,
    validate_selection_bounds,
    MAX_PROMPT_TITLE_LENGTH,
    MAX_PROVIDER_COMPONENT_ID_LENGTH,
    MAX_RENDERED_PROMPT_LENGTH,
    PromptStyle,
)
from hermes_constants import get_hermes_home
from hermes_state_common import reconcile_durable_action_schema

logger = logging.getLogger(__name__)

DURABLE_REQUEST_ID_RANDOM_BYTES = 24
MIN_DURABLE_REQUEST_ID_LENGTH = 20
MAX_DURABLE_REQUEST_ID_LENGTH = 96
MAX_DURABLE_REFERENCE_LENGTH = 256
MIN_DURABLE_OPTIONS = 2
DEFAULT_DURABLE_REQUEST_TTL_SECONDS = 24 * 60 * 60.0
MAX_DURABLE_REQUEST_TTL_SECONDS = 7 * 24 * 60 * 60.0
DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS = 7 * 24 * 60 * 60.0
MAX_ACTIVE_DURABLE_REQUESTS = 1_000
MAX_TERMINAL_DURABLE_REQUESTS = 4_096
DELIVERY_CLAIM_STALE_SECONDS = 5 * 60.0
MAX_DELIVERY_BATCH = 10
WAKE_CLAIM_STALE_SECONDS = 5 * 60.0
MAX_WAKE_BATCH = 10

_REQUEST_ID_RE = re.compile(r"^drq_[A-Za-z0-9_-]+$")
_OPTION_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
_COMPONENT_ID_PREFIX = "dr"


def _supports_execution_approval_scopes(action_type: str, kind: str) -> bool:
    """Scope choices belong only to regular native tool approval paths."""
    return (action_type in {"terminal.execute", "execute_code.execute", "file.write", "file.patch"}
            and kind == "tool.durable_approval." + action_type)


class DurableRequestStatus(str, Enum):
    PENDING_DELIVERY = "pending_delivery"
    OPEN = "open"
    SELECTED = "selected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNSUPPORTED = "unsupported"
    DELIVERY_FAILED = "delivery_failed"
    DELIVERY_UNKNOWN = "delivery_unknown"


class DurableRequestDeliveryMode(str, Enum):
    NATIVE = "native"
    TEXT_COMMAND = "text_command"


class DurableRequestResolutionSource(str, Enum):
    NATIVE = "native"
    COMMAND = "command"


class DurableResponderMode(str, Enum):
    INITIATOR = "initiator"
    ANY_AUTHORIZED = "any_authorized"


class DurableWakeStatus(str, Enum):
    NOT_REQUESTED = "not_requested"
    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    DROPPED = "dropped"


class DurableResolveStatus(str, Enum):
    RESOLVED = "resolved"
    ALREADY_RESOLVED = "already_resolved"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNSUPPORTED = "unsupported"
    DELIVERY_FAILED = "delivery_failed"
    DELIVERY_UNKNOWN = "delivery_unknown"
    NOT_FOUND = "not_found"
    INVALID_OPTION = "invalid_option"
    SCOPE_MISMATCH = "scope_mismatch"
    WRONG_CHANNEL = "wrong_channel"
    WRONG_ACTOR = "wrong_actor"
    COMMAND_DISABLED = "command_disabled"
    NOT_DELIVERED = "not_delivered"


_TERMINAL_STATUSES = frozenset(
    {
        DurableRequestStatus.SELECTED,
        DurableRequestStatus.EXPIRED,
        DurableRequestStatus.CANCELLED,
        DurableRequestStatus.UNSUPPORTED,
        DurableRequestStatus.DELIVERY_FAILED,
        DurableRequestStatus.DELIVERY_UNKNOWN,
    }
)
_ACTIVE_STATUSES = frozenset(
    {
        DurableRequestStatus.PENDING_DELIVERY,
        DurableRequestStatus.OPEN,
    }
)
_TERMINAL_ACTION_STATUSES = tuple(
    status.value
    for status in DurableActionStatus
    if status
    not in {
        DurableActionStatus.WAITING_FOR_DECISION,
        DurableActionStatus.READY,
        DurableActionStatus.CLAIMED,
        DurableActionStatus.EXECUTING,
    }
)


@dataclass(frozen=True)
class DurableRequestOption:
    id: str
    label: str
    style: PromptStyle = PromptStyle.SECONDARY


@dataclass(frozen=True)
class DurableRequestSpec:
    kind: str
    title: str
    body: str
    options: tuple[DurableRequestOption, ...]
    ttl_seconds: float = DEFAULT_DURABLE_REQUEST_TTL_SECONDS
    reference: str | None = None
    selection_mode: str = "single"
    min_selections: int = 1
    max_selections: int = 1


@dataclass(frozen=True)
class DurableRequestTarget:
    platform: str
    chat_id: str
    thread_id: str | None = None

    @property
    def delivery_channel_id(self) -> str:
        return self.thread_id or self.chat_id


@dataclass(frozen=True)
class DurableRequest:
    request_id: str
    kind: str
    title: str
    body: str
    options: tuple[DurableRequestOption, ...]
    status: DurableRequestStatus
    created_at: float
    updated_at: float
    expires_at: float
    target: DurableRequestTarget
    responder_mode: DurableResponderMode
    expected_actor_id: str | None
    origin_type: str
    origin_id: str
    reference: str | None
    wake_requested: bool = False
    wake_status: DurableWakeStatus = DurableWakeStatus.NOT_REQUESTED
    origin_session_id: str | None = None
    origin_ui_session_id: str | None = None
    origin_profile: str | None = None
    message_id: str | None = None
    selected_option_id: str | None = None
    resolved_by_actor_id: str | None = None
    resolved_at: float | None = None
    terminal_reason: str | None = None
    delivery_attempts: int = 0
    delivery_claim: str | None = None
    delivery_claimed_at: float | None = None
    delivered_at: float | None = None
    wake_attempts: int = 0
    wake_claim: str | None = None
    wake_claimed_at: float | None = None
    wake_delivered_at: float | None = None
    wake_reason: str | None = None
    delivery_mode: DurableRequestDeliveryMode = DurableRequestDeliveryMode.NATIVE
    command_response_allowed: bool = False
    text_command_fallback_allowed: bool = False
    resolution_source: DurableRequestResolutionSource | None = None
    selection_mode: str = "single"
    min_selections: int = 1
    max_selections: int = 1
    selected_option_ids: tuple[str, ...] = ()

    @property
    def delivery_channel_id(self) -> str:
        return self.target.delivery_channel_id

    @property
    def selected_option(self) -> DurableRequestOption | None:
        if self.selected_option_id is None:
            return None
        return next(
            (option for option in self.options if option.id == self.selected_option_id),
            None,
        )


@dataclass(frozen=True)
class DurableCreateResult:
    request: DurableRequest
    created: bool


@dataclass(frozen=True)
class _ValidatedCreate:
    spec: DurableRequestSpec
    target: DurableRequestTarget
    responder_mode: DurableResponderMode
    actor: str | None
    origin_type: str
    origin_id: str
    delivery_mode: DurableRequestDeliveryMode
    command_response_allowed: bool
    text_command_fallback_allowed: bool
    wake_on_terminal: bool
    origin_session_id: str | None
    origin_ui_session_id: str | None
    origin_profile: str | None
    terminal_retention_seconds: float


@dataclass(frozen=True)
class DurableDeliveryClaim:
    request: DurableRequest
    claim_id: str


@dataclass(frozen=True)
class DurableWakeClaim:
    request: DurableRequest
    claim_id: str


@dataclass(frozen=True)
class DurableResolveResult:
    status: DurableResolveStatus
    request: DurableRequest | None = None

    def __post_init__(self) -> None:
        if self.status is DurableResolveStatus.RESOLVED:
            if self.request is None:
                raise ValueError("RESOLVED results require a request")
        elif self.request is not None:
            raise ValueError("Only RESOLVED results may contain a request")


@dataclass(frozen=True)
class DurableRequestsConfig:
    enabled: bool
    target: DurableRequestTarget | None
    default_ttl_seconds: float
    max_ttl_seconds: float
    terminal_retention_seconds: float
    scheduled_responder_mode: DurableResponderMode
    reason: str | None = None
    command_responses_enabled: bool = False
    allow_on_native_delivery: bool = True
    allow_text_delivery_fallback: bool = True

    @property
    def available(self) -> bool:
        return self.enabled and self.target is not None and self.reason is None


class DurableRequestError(ValueError):
    """Raised for invalid caller input or invalid durable-request configuration."""


class DurableRequestLimitError(RuntimeError):
    """Raised when the active durable-request capacity has been reached."""


def _require_text(
    value: object,
    *,
    field: str,
    maximum: int,
    single_line: bool = False,
) -> str:
    if not isinstance(value, str):
        raise DurableRequestError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise DurableRequestError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise DurableRequestError(f"{field} exceeds {maximum} characters")
    if single_line and ("\n" in normalized or "\r" in normalized):
        raise DurableRequestError(f"{field} must be a single line")
    return normalized


def _normalize_optional_text(
    value: object,
    *,
    field: str,
    maximum: int,
    single_line: bool = False,
) -> str | None:
    if value is None:
        return None
    return _require_text(
        value,
        field=field,
        maximum=maximum,
        single_line=single_line,
    )


def _validate_request_id(request_id: object) -> str:
    value = _require_text(
        request_id,
        field="request_id",
        maximum=MAX_DURABLE_REQUEST_ID_LENGTH,
        single_line=True,
    )
    if (
        len(value) < MIN_DURABLE_REQUEST_ID_LENGTH
        or _REQUEST_ID_RE.fullmatch(value) is None
    ):
        raise DurableRequestError("request_id is not a valid durable request token")
    return value


def _validate_ttl(value: object, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DurableRequestError("ttl_seconds must be a number")
    ttl = float(value)
    if not math.isfinite(ttl) or ttl <= 0:
        raise DurableRequestError("ttl_seconds must be positive")
    if ttl > maximum:
        raise DurableRequestError(
            f"ttl_seconds exceeds the maximum of {int(maximum)} seconds"
        )
    return ttl


def _selection_instruction(request: DurableRequest | DurableRequestSpec) -> str:
    if request.selection_mode == "multi":
        return f"Choose {request.min_selections}–{request.max_selections} options:"
    return "Choose one option:"


def render_durable_request_content(request: DurableRequest | DurableRequestSpec) -> str:
    """Render the complete question and option set as ordinary message text."""

    lines = [f"**{request.title}**", "", request.body, "", _selection_instruction(request)]
    lines.extend(
        f"{index}. {option.label}"
        for index, option in enumerate(request.options, start=1)
    )
    if isinstance(request, DurableRequest) and request.command_response_allowed:
        lines.extend(
            (
                "",
                f"Alternative: /request respond {request.request_id} "
                + ("<numbers, comma-separated>" if request.selection_mode == "multi" else "<number>"),
            )
        )
    return "\n".join(lines)


def render_durable_request_text_command_content(request: DurableRequest) -> str:
    """Render a complete command-driven durable request without option IDs."""

    lines = [
        f"**{request.title}**",
        "",
        request.body,
        "",
        f"Request: {request.request_id}",
        "",
        _selection_instruction(request),
    ]
    lines.extend(
        f"{index}. {option.label}"
        for index, option in enumerate(request.options, start=1)
    )
    lines.extend(
        (
            "",
            "Respond in this channel with:",
            f"/request respond {request.request_id} "
            + ("<numbers, comma-separated>" if request.selection_mode == "multi" else "<number>"),
        )
    )
    return "\n".join(lines)


def make_durable_component_id(request_id: str, slot: int | str) -> str:
    validated_id = _validate_request_id(request_id)
    token = str(slot)
    if not token.isdigit() or int(token) <= 0:
        raise DurableRequestError("option slot must be a positive integer")
    custom_id = f"{_COMPONENT_ID_PREFIX}:{validated_id}:{token}"
    if len(custom_id) > MAX_PROVIDER_COMPONENT_ID_LENGTH:
        raise DurableRequestError("durable request component identifier is too long")
    return custom_id


def make_durable_multi_select_component_id(request_id: str) -> str:
    custom_id = f"drm:{_validate_request_id(request_id)}"
    if len(custom_id) > MAX_PROVIDER_COMPONENT_ID_LENGTH:
        raise DurableRequestError("durable request component identifier is too long")
    return custom_id


def parse_durable_multi_select_component_id(custom_id: object) -> str | None:
    if not isinstance(custom_id, str) or not custom_id.startswith("drm:"):
        return None
    try:
        request_id = _validate_request_id(custom_id[4:])
        return request_id if len(custom_id) <= MAX_PROVIDER_COMPONENT_ID_LENGTH else None
    except DurableRequestError:
        return None


def validate_durable_request_spec(
    spec: DurableRequestSpec,
    *,
    maximum_ttl_seconds: float = MAX_DURABLE_REQUEST_TTL_SECONDS,
) -> DurableRequestSpec:
    if not isinstance(spec, DurableRequestSpec):
        raise DurableRequestError("spec must be a DurableRequestSpec")

    kind = _require_text(
        spec.kind,
        field="kind",
        maximum=MAX_PROMPT_KIND_LENGTH,
        single_line=True,
    )
    title = _require_text(
        spec.title,
        field="title",
        maximum=MAX_PROMPT_TITLE_LENGTH,
    )
    body = _require_text(
        spec.body,
        field="body",
        maximum=MAX_PROMPT_BODY_LENGTH,
    )
    reference = _normalize_optional_text(
        spec.reference,
        field="reference",
        maximum=MAX_DURABLE_REFERENCE_LENGTH,
        single_line=True,
    )
    ttl = _validate_ttl(spec.ttl_seconds, maximum=maximum_ttl_seconds)

    if not isinstance(spec.options, tuple):
        raise DurableRequestError("options must be a tuple")
    if spec.selection_mode not in ("single", "multi"):
        raise DurableRequestError("selection_mode must be single or multi")
    if spec.selection_mode == "single":
        if (
            type(spec.min_selections) is not int
            or type(spec.max_selections) is not int
            or (spec.min_selections, spec.max_selections) != (1, 1)
        ):
            raise DurableRequestError("single requests require min_selections=max_selections=1")
        option_limit = MAX_PROMPT_OPTIONS
    else:
        try:
            validate_selection_bounds(len(spec.options), spec.min_selections, spec.max_selections)
        except ValueError as exc:
            raise DurableRequestError(str(exc)) from exc
        option_limit = MAX_MULTI_SELECT_OPTIONS
    if not MIN_DURABLE_OPTIONS <= len(spec.options) <= option_limit:
        raise DurableRequestError(
            f"options must contain between {MIN_DURABLE_OPTIONS} and "
            f"{option_limit} choices"
        )

    validated_options: list[DurableRequestOption] = []
    option_ids: set[str] = set()
    for option in spec.options:
        if not isinstance(option, DurableRequestOption):
            raise DurableRequestError("options must contain DurableRequestOption values")
        option_id = _require_text(
            option.id,
            field="option id",
            maximum=MAX_PROMPT_OPTION_ID_LENGTH,
            single_line=True,
        )
        if _OPTION_ID_RE.fullmatch(option_id) is None:
            raise DurableRequestError("option id contains unsupported characters")
        if option_id in option_ids:
            raise DurableRequestError("option ids must be unique")
        option_ids.add(option_id)
        label = _require_text(
            option.label,
            field="option label",
            maximum=MAX_PROMPT_OPTION_LABEL_LENGTH,
        )
        if not isinstance(option.style, PromptStyle):
            raise DurableRequestError("option style is not supported")
        validated_options.append(
            DurableRequestOption(id=option_id, label=label, style=option.style)
        )

    validated = DurableRequestSpec(
        kind=kind,
        title=title,
        body=body,
        options=tuple(validated_options),
        ttl_seconds=ttl,
        reference=reference,
        selection_mode=spec.selection_mode,
        min_selections=spec.min_selections,
        max_selections=spec.max_selections,
    )
    if len(render_durable_request_content(validated)) > MAX_RENDERED_PROMPT_LENGTH:
        raise DurableRequestError("rendered durable request exceeds delivery limits")

    # Validate provider IDs up-front for every option.  The canonical option id
    # never appears in the component identifier.
    sample_id = _new_request_id()
    if validated.selection_mode == "multi":
        make_durable_multi_select_component_id(sample_id)
    else:
        for slot in range(1, len(validated.options) + 1):
            make_durable_component_id(sample_id, slot)
    return validated


def _new_request_id() -> str:
    return "drq_" + secrets.token_urlsafe(DURABLE_REQUEST_ID_RANDOM_BYTES)


def _new_claim_id() -> str:
    return secrets.token_urlsafe(24)


def _serialize_options(options: tuple[DurableRequestOption, ...]) -> str:
    return json.dumps(
        [
            {"id": option.id, "label": option.label, "style": option.style.value}
            for option in options
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _deserialize_options(raw: str) -> tuple[DurableRequestOption, ...]:
    values = json.loads(raw)
    if not isinstance(values, list):
        raise ValueError("stored durable request options are invalid")
    return tuple(
        DurableRequestOption(
            id=str(value["id"]),
            label=str(value["label"]),
            style=PromptStyle(value["style"]),
        )
        for value in values
    )


class DurableRequestStore:
    """Thread-safe SQLite-backed durable request ledger."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        wall_clock=time.time,
        max_active: int = MAX_ACTIVE_DURABLE_REQUESTS,
        max_terminal: int = MAX_TERMINAL_DURABLE_REQUESTS,
    ) -> None:
        self._db_path = Path(db_path)
        self._wall_clock = wall_clock
        self._max_active = max_active
        self._max_terminal = max_terminal
        self._lock = threading.RLock()
        self._selection_counts = {"submissions": 0, "invalid_sets": 0}
        self._ensure_schema()

    def _record_multi_select(self, event: str) -> None:
        from gateway.durable_operations import record_durable_multi_select_event

        self._selection_counts[event] += 1
        record_durable_multi_select_event(event)

    def selection_metrics(self) -> dict[str, int]:
        with self._lock:
            return dict(self._selection_counts)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 10000")
        from hermes_state import apply_wal_with_fallback

        apply_wal_with_fallback(conn, db_label="state.db (durable_requests)")
        return conn

    @contextmanager
    def _transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            if immediate:
                conn.execute("BEGIN IMMEDIATE")
            else:
                conn.execute("BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_DURABLE_SCHEMA_SQL)
                reconcile_durable_action_schema(conn)
                existing_columns = {
                    row["name"]
                    for row in conn.execute(
                        "PRAGMA table_info(durable_requests)"
                    ).fetchall()
                }
                for column, ddl in _DURABLE_COLUMN_DDL.items():
                    if column not in existing_columns:
                        conn.execute(
                            f"ALTER TABLE durable_requests ADD COLUMN {column} {ddl}"
                        )
                conn.execute(
                    """CREATE INDEX IF NOT EXISTS idx_durable_requests_wake
                       ON durable_requests(wake_status, resolved_at, request_id)"""
                )
                conn.commit()
            finally:
                conn.close()

    def create(
        self,
        spec: DurableRequestSpec,
        *,
        target: DurableRequestTarget,
        responder_mode: DurableResponderMode,
        expected_actor_id: str | None,
        origin_type: str,
        origin_id: str,
        command_response_allowed: bool = False,
        text_command_fallback_allowed: bool = False,
        wake_on_terminal: bool = False,
        origin_session_id: str | None = None,
        origin_ui_session_id: str | None = None,
        origin_profile: str | None = None,
        maximum_ttl_seconds: float = MAX_DURABLE_REQUEST_TTL_SECONDS,
        terminal_retention_seconds: float = (
            DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS
        ),
    ) -> DurableCreateResult:
        values = self._validate_create_inputs(
            spec,
            target=target,
            responder_mode=responder_mode,
            expected_actor_id=expected_actor_id,
            origin_type=origin_type,
            origin_id=origin_id,
            command_response_allowed=command_response_allowed,
            text_command_fallback_allowed=text_command_fallback_allowed,
            wake_on_terminal=wake_on_terminal,
            origin_session_id=origin_session_id,
            origin_ui_session_id=origin_ui_session_id,
            origin_profile=origin_profile,
            maximum_ttl_seconds=maximum_ttl_seconds,
            terminal_retention_seconds=terminal_retention_seconds,
            retain_origin=False,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            self._prepare_create_locked(conn, values=values, now=now)
            if values.spec.reference is not None:
                existing = self._find_reference_locked(conn, values)
                if existing is not None:
                    return DurableCreateResult(
                        request=self._row_to_request(existing),
                        created=False,
                    )
            self._require_create_capacity_locked(conn)
            try:
                row = self._insert_request_locked(conn, values=values, now=now)
            except sqlite3.IntegrityError:
                if values.spec.reference is None:
                    raise
                existing = self._find_reference_locked(conn, values)
                if existing is None:
                    raise
                return DurableCreateResult(
                    request=self._row_to_request(existing),
                    created=False,
                )
            result = DurableCreateResult(
                request=self._row_to_request(row),
                created=True,
            )
        self._log_registered(result.request)
        return result

    def create_with_action(
        self,
        spec: DurableRequestSpec,
        *,
        action: PreparedDurableAction,
        target: DurableRequestTarget,
        responder_mode: DurableResponderMode,
        expected_actor_id: str | None,
        origin_type: str,
        origin_id: str,
        command_response_allowed: bool = False,
        text_command_fallback_allowed: bool = False,
        origin_session_id: str | None = None,
        origin_ui_session_id: str | None = None,
        origin_profile: str | None = None,
        maximum_ttl_seconds: float = MAX_DURABLE_REQUEST_TTL_SECONDS,
        terminal_retention_seconds: float = (
            DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS
        ),
        inline_wait_owner: str | None = None,
        inline_wait_until: float | None = None,
    ) -> DurableActionCreateResult:
        if not isinstance(action, PreparedDurableAction):
            raise DurableRequestError("action must be a PreparedDurableAction")
        values = self._validate_create_inputs(
            spec,
            target=target,
            responder_mode=responder_mode,
            expected_actor_id=expected_actor_id,
            origin_type=origin_type,
            origin_id=origin_id,
            command_response_allowed=command_response_allowed,
            text_command_fallback_allowed=text_command_fallback_allowed,
            wake_on_terminal=False,
            origin_session_id=origin_session_id,
            origin_ui_session_id=origin_ui_session_id,
            origin_profile=origin_profile,
            maximum_ttl_seconds=maximum_ttl_seconds,
            terminal_retention_seconds=terminal_retention_seconds,
            retain_origin=True,
        )
        choices = tuple(option.id for option in values.spec.options)
        allowed_choices = {("approve", "reject")}
        if _supports_execution_approval_scopes(action.action_type, values.spec.kind):
            allowed_choices.update({("approve", "session", "reject"),
                                    ("approve", "session", "always", "reject")})
        if values.spec.selection_mode != "single" or choices not in allowed_choices:
            raise DurableRequestError(
                "action approvals require approve and reject options; scoped choices are limited to native tool executions"
            )

        now = float(self._wall_clock())
        if inline_wait_owner is not None or inline_wait_until is not None:
            if (type(inline_wait_owner) is not str or not inline_wait_owner.strip()
                    or len(inline_wait_owner) > 128
                    or type(inline_wait_until) not in (int, float)
                    or not math.isfinite(inline_wait_until)
                    or not now < inline_wait_until <= now + MAX_DURABLE_REQUEST_TTL_SECONDS):
                raise DurableRequestError("inline result wait requires a bounded owner lease")
        with self._lock, self._transaction(immediate=True) as conn:
            self._prepare_create_locked(conn, values=values, now=now)
            if values.spec.reference is not None:
                existing_request = self._find_reference_locked(conn, values)
                if existing_request is not None:
                    existing_ticket = conn.execute(
                        "SELECT * FROM durable_action_tickets WHERE request_id = ?",
                        (existing_request["request_id"],),
                    ).fetchone()
                    if existing_request["selection_mode"] != "single" or existing_ticket is None or not self._ticket_matches_action(
                        existing_ticket,
                        action,
                    ):
                        raise DurableRequestError(
                            "reference conflicts with the prepared durable action"
                        )
                    return DurableActionCreateResult(
                        request=self._row_to_request(existing_request),
                        ticket=self._row_to_action_ticket(existing_ticket),
                        created=False,
                    )

            existing_ticket = conn.execute(
                """SELECT * FROM durable_action_tickets
                   WHERE action_type = ? AND idempotency_key = ?""",
                (action.action_type, action.idempotency_key),
            ).fetchone()
            if existing_ticket is not None:
                if not self._ticket_matches_action(existing_ticket, action):
                    raise DurableRequestError(
                        "idempotency key conflicts with the prepared durable action"
                    )
                existing_request = conn.execute(
                    "SELECT * FROM durable_requests WHERE request_id = ?",
                    (existing_ticket["request_id"],),
                ).fetchone()
                if existing_request is None or existing_request["selection_mode"] != "single":
                    raise DurableRequestError(
                        "durable action ticket has no matching request"
                    )
                return DurableActionCreateResult(
                    request=self._row_to_request(existing_request),
                    ticket=self._row_to_action_ticket(existing_ticket),
                    created=False,
                )

            self._require_create_capacity_locked(conn)
            request_row = self._insert_request_locked(conn, values=values, now=now)
            conn.execute(
                """INSERT INTO durable_action_tickets (
                       request_id, action_type, action_version, payload_json,
                       fingerprint, idempotency_key, status,
                       notification_status, created_at, updated_at,
                       inline_wait_owner, inline_wait_until
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    request_row["request_id"],
                    action.action_type,
                    action.action_version,
                    action.payload_json,
                    action.fingerprint,
                    action.idempotency_key,
                    DurableActionStatus.WAITING_FOR_DECISION.value,
                    DurableActionNotificationStatus.NOT_READY.value,
                    now,
                    now,
                    inline_wait_owner,
                    inline_wait_until,
                ),
            )
            ticket_row = conn.execute(
                "SELECT * FROM durable_action_tickets WHERE request_id = ?",
                (request_row["request_id"],),
            ).fetchone()
            assert ticket_row is not None
            result = DurableActionCreateResult(
                request=self._row_to_request(request_row),
                ticket=self._row_to_action_ticket(ticket_row),
                created=True,
            )
        self._log_registered(result.request)
        return result

    def consume_inline_result(self, request_id: str, owner: str) -> dict | None:
        """Atomically give a terminal outcome to its original active tool turn."""
        if type(owner) is not str or not owner.strip() or len(owner) > 128:
            raise DurableRequestError("inline wait owner is invalid")
        with self._lock, self._transaction(immediate=True) as conn:
            now = float(self._wall_clock())
            row = conn.execute(
                "SELECT status, outcome_json, outcome_reason FROM durable_action_tickets "
                "WHERE request_id=? AND inline_wait_owner=? AND inline_wait_until>? "
                "AND notification_status=?",
                (request_id, owner, now, DurableActionNotificationStatus.PENDING.value),
            ).fetchone()
            if row is None or row["status"] not in _TERMINAL_ACTION_STATUSES:
                return None
            changed = conn.execute(
                "UPDATE durable_action_tickets SET notification_status=?, notification_completed_at=?, "
                "notification_reason='consumed_inline', inline_wait_owner=NULL, inline_wait_until=NULL, updated_at=? "
                "WHERE request_id=? AND inline_wait_owner=? AND notification_status=?",
                (DurableActionNotificationStatus.DELIVERED.value, now, now, request_id, owner,
                 DurableActionNotificationStatus.PENDING.value),
            ).rowcount
            if changed != 1:
                return None
            return {"status": row["status"], "result_json": row["outcome_json"], "reason": row["outcome_reason"]}

    def release_inline_wait(self, request_id: str, owner: str) -> bool:
        """Leave delivery to the durable notifier when the active turn detaches."""
        if type(owner) is not str or not owner.strip() or len(owner) > 128:
            raise DurableRequestError("inline wait owner is invalid")
        with self._lock, self._transaction(immediate=True) as conn:
            return conn.execute(
                "UPDATE durable_action_tickets SET inline_wait_owner=NULL, inline_wait_until=NULL, updated_at=? "
                "WHERE request_id=? AND inline_wait_owner=?",
                (float(self._wall_clock()), request_id, owner),
            ).rowcount == 1

    @staticmethod
    def _ticket_matches_action(
        row: sqlite3.Row,
        action: PreparedDurableAction,
    ) -> bool:
        return (
            row["action_type"] == action.action_type
            and int(row["action_version"]) == action.action_version
            and row["payload_json"] == action.payload_json
            and row["fingerprint"] == action.fingerprint
            and row["idempotency_key"] == action.idempotency_key
        )

    def _validate_create_inputs(
        self,
        spec: DurableRequestSpec,
        *,
        target: DurableRequestTarget,
        responder_mode: DurableResponderMode,
        expected_actor_id: str | None,
        origin_type: str,
        origin_id: str,
        command_response_allowed: bool,
        text_command_fallback_allowed: bool,
        wake_on_terminal: bool,
        origin_session_id: str | None,
        origin_ui_session_id: str | None,
        origin_profile: str | None,
        maximum_ttl_seconds: float,
        terminal_retention_seconds: float,
        retain_origin: bool,
    ) -> _ValidatedCreate:
        validated = validate_durable_request_spec(
            spec,
            maximum_ttl_seconds=maximum_ttl_seconds,
        )
        validated_target = _validate_target(target)
        if not isinstance(responder_mode, DurableResponderMode):
            raise DurableRequestError("responder_mode is not supported")
        actor = _normalize_optional_text(
            expected_actor_id,
            field="expected_actor_id",
            maximum=512,
            single_line=True,
        )
        if responder_mode is DurableResponderMode.INITIATOR and actor is None:
            raise DurableRequestError("initiator responder mode requires an actor")
        origin_type_value = _require_text(
            origin_type,
            field="origin_type",
            maximum=64,
            single_line=True,
        )
        origin_id_value = _require_text(
            origin_id,
            field="origin_id",
            maximum=512,
            single_line=True,
        )
        if not isinstance(wake_on_terminal, bool):
            raise DurableRequestError("wake_on_terminal must be a boolean")
        if not isinstance(command_response_allowed, bool):
            raise DurableRequestError("command_response_allowed must be a boolean")
        if not isinstance(text_command_fallback_allowed, bool):
            raise DurableRequestError(
                "text_command_fallback_allowed must be a boolean"
            )
        origin_session_value = _normalize_optional_text(
            origin_session_id,
            field="origin_session_id",
            maximum=512,
            single_line=True,
        )
        origin_ui_session_value = _normalize_optional_text(
            origin_ui_session_id,
            field="origin_ui_session_id",
            maximum=512,
            single_line=True,
        )
        origin_profile_value = _normalize_optional_text(
            origin_profile,
            field="origin_profile",
            maximum=128,
            single_line=True,
        )
        if (
            (wake_on_terminal or retain_origin)
            and origin_type_value == "session"
            and origin_session_value is None
        ):
            raise DurableRequestError(
                "session wake-up requires a durable origin session id"
            )
        if not wake_on_terminal and not retain_origin:
            origin_session_value = None
            origin_ui_session_value = None
            origin_profile_value = None
        if terminal_retention_seconds <= 0:
            raise DurableRequestError("terminal_retention_seconds must be positive")
        return _ValidatedCreate(
            spec=validated,
            target=validated_target,
            responder_mode=responder_mode,
            actor=actor,
            origin_type=origin_type_value,
            origin_id=origin_id_value,
            delivery_mode=DurableRequestDeliveryMode.NATIVE,
            command_response_allowed=command_response_allowed,
            text_command_fallback_allowed=text_command_fallback_allowed,
            wake_on_terminal=wake_on_terminal,
            origin_session_id=origin_session_value,
            origin_ui_session_id=origin_ui_session_value,
            origin_profile=origin_profile_value,
            terminal_retention_seconds=float(terminal_retention_seconds),
        )

    def _prepare_create_locked(
        self,
        conn: sqlite3.Connection,
        *,
        values: _ValidatedCreate,
        now: float,
    ) -> None:
        self._expire_due_locked(conn, now)
        self._prune_locked(
            conn,
            now=now,
            retention_seconds=values.terminal_retention_seconds,
        )

    @staticmethod
    def _find_reference_locked(
        conn: sqlite3.Connection,
        values: _ValidatedCreate,
    ) -> sqlite3.Row | None:
        return conn.execute(
            """SELECT * FROM durable_requests
               WHERE origin_type = ? AND origin_id = ? AND reference = ?""",
            (values.origin_type, values.origin_id, values.spec.reference),
        ).fetchone()

    def _require_create_capacity_locked(self, conn: sqlite3.Connection) -> None:
        active_count = conn.execute(
            """SELECT COUNT(*) FROM durable_requests
               WHERE status IN (?, ?)""",
            (
                DurableRequestStatus.PENDING_DELIVERY.value,
                DurableRequestStatus.OPEN.value,
            ),
        ).fetchone()[0]
        if int(active_count) >= self._max_active:
            raise DurableRequestLimitError("too many active durable requests")

    @staticmethod
    def _insert_request_locked(
        conn: sqlite3.Connection,
        *,
        values: _ValidatedCreate,
        now: float,
    ) -> sqlite3.Row:
        request_id = _new_request_id()
        conn.execute(
            """INSERT INTO durable_requests (
                   request_id, kind, title, body, options_json, status,
                   created_at, updated_at, expires_at,
                   platform, chat_id, thread_id,
                   responder_mode, expected_actor_id,
                   origin_type, origin_id, reference,
                   delivery_mode, command_response_allowed,
                   text_command_fallback_allowed,
                   wake_requested, wake_status,
                   origin_session_id, origin_ui_session_id, origin_profile,
                   selection_mode, min_selections, max_selections,
                   delivery_attempts, wake_attempts
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)""",
            (
                request_id,
                values.spec.kind,
                values.spec.title,
                values.spec.body,
                _serialize_options(values.spec.options),
                DurableRequestStatus.PENDING_DELIVERY.value,
                now,
                now,
                now + values.spec.ttl_seconds,
                values.target.platform,
                values.target.chat_id,
                values.target.thread_id,
                values.responder_mode.value,
                values.actor,
                values.origin_type,
                values.origin_id,
                values.spec.reference,
                values.delivery_mode.value,
                1 if values.command_response_allowed else 0,
                1 if values.text_command_fallback_allowed else 0,
                1 if values.wake_on_terminal else 0,
                DurableWakeStatus.NOT_REQUESTED.value,
                values.origin_session_id,
                values.origin_ui_session_id,
                values.origin_profile,
                values.spec.selection_mode,
                values.spec.min_selections,
                values.spec.max_selections,
            ),
        )
        row = conn.execute(
            "SELECT * FROM durable_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        assert row is not None
        return row

    @staticmethod
    def _log_registered(request: DurableRequest) -> None:
        logger.info(
            "Durable request registered",
            extra={
                "request_id": request.request_id,
                "platform": request.target.platform,
                "state": DurableRequestStatus.PENDING_DELIVERY.value,
            },
        )

    @staticmethod
    def _row_to_action_ticket(row: sqlite3.Row) -> DurableActionTicket:
        return DurableActionTicket(
            request_id=row["request_id"],
            action_type=row["action_type"],
            action_version=int(row["action_version"]),
            payload_json=row["payload_json"],
            fingerprint=row["fingerprint"],
            idempotency_key=row["idempotency_key"],
            status=DurableActionStatus(row["status"]),
            execution_claim=row["execution_claim"],
            execution_claimed_at=(
                None
                if row["execution_claimed_at"] is None
                else float(row["execution_claimed_at"])
            ),
            execution_started_at=(
                None
                if row["execution_started_at"] is None
                else float(row["execution_started_at"])
            ),
            outcome_reason=row["outcome_reason"],
            outcome_json=row["outcome_json"],
            completed_at=(
                None if row["completed_at"] is None else float(row["completed_at"])
            ),
            notification_status=DurableActionNotificationStatus(
                row["notification_status"]
            ),
            notification_claim=row["notification_claim"],
            notification_claimed_at=(
                None
                if row["notification_claimed_at"] is None
                else float(row["notification_claimed_at"])
            ),
            notification_completed_at=(
                None
                if row["notification_completed_at"] is None
                else float(row["notification_completed_at"])
            ),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            notification_reason=row["notification_reason"],
        )

    def get(
        self,
        *,
        request_id: str | None = None,
        origin_type: str | None = None,
        origin_id: str | None = None,
        reference: str | None = None,
    ) -> DurableRequest | None:
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = self._find_row_locked(
                conn,
                request_id=request_id,
                origin_type=origin_type,
                origin_id=origin_id,
                reference=reference,
            )
            if row is None:
                return None
            row = self._expire_row_if_due_locked(conn, row, now)
            return self._row_to_request(row)

    def cancel(
        self,
        *,
        origin_type: str,
        origin_id: str,
        request_id: str | None = None,
        reference: str | None = None,
        reason: str = "cancelled_by_origin",
    ) -> DurableRequest | None:
        reason_value = _require_text(
            reason,
            field="reason",
            maximum=256,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = self._find_row_locked(
                conn,
                request_id=request_id,
                origin_type=origin_type,
                origin_id=origin_id,
                reference=reference,
                require_origin=True,
            )
            if row is None:
                return None
            row = self._expire_row_if_due_locked(conn, row, now)
            status = DurableRequestStatus(row["status"])
            if status in _ACTIVE_STATUSES:
                conn.execute(
                    """UPDATE durable_requests
                       SET status = ?, terminal_reason = ?, updated_at = ?,
                           resolved_at = ?, delivery_claim = NULL,
                           delivery_claimed_at = NULL,
                           wake_status = CASE
                               WHEN wake_requested = 1 AND wake_status = ? THEN ?
                               ELSE wake_status
                           END,
                           wake_claim = NULL, wake_claimed_at = NULL,
                           wake_reason = NULL
                       WHERE request_id = ?""",
                    (
                        DurableRequestStatus.CANCELLED.value,
                        reason_value,
                        now,
                        now,
                        DurableWakeStatus.NOT_REQUESTED.value,
                        DurableWakeStatus.PENDING.value,
                        row["request_id"],
                    ),
                )
                self._transition_action_for_terminal_request_locked(
                    conn,
                    request_id=row["request_id"],
                    request_status=DurableRequestStatus.CANCELLED,
                    now=now,
                )
                row = conn.execute(
                    "SELECT * FROM durable_requests WHERE request_id = ?",
                    (row["request_id"],),
                ).fetchone()
                assert row is not None
            return self._row_to_request(row)

    def claim_next_pending(
        self,
        available_platforms: Iterable[str],
    ) -> DurableDeliveryClaim | None:
        platforms = tuple(sorted({str(value) for value in available_platforms if value}))
        if not platforms:
            return None
        now = float(self._wall_clock())
        placeholders = ",".join("?" for _ in platforms)
        with self._lock, self._transaction(immediate=True) as conn:
            self._expire_due_locked(conn, now)
            row = conn.execute(
                f"""SELECT * FROM durable_requests
                    WHERE status = ?
                      AND delivery_claim IS NULL
                      AND platform IN ({placeholders})
                    ORDER BY created_at, request_id
                    LIMIT 1""",
                (DurableRequestStatus.PENDING_DELIVERY.value, *platforms),
            ).fetchone()
            if row is None:
                return None
            claim_id = _new_claim_id()
            updated = conn.execute(
                """UPDATE durable_requests
                   SET delivery_claim = ?, delivery_claimed_at = ?,
                       delivery_attempts = delivery_attempts + 1,
                       updated_at = ?
                   WHERE request_id = ? AND status = ?
                     AND delivery_claim IS NULL""",
                (
                    claim_id,
                    now,
                    now,
                    row["request_id"],
                    DurableRequestStatus.PENDING_DELIVERY.value,
                ),
            ).rowcount
            if updated != 1:
                return None
            claimed = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (row["request_id"],),
            ).fetchone()
            assert claimed is not None
            return DurableDeliveryClaim(
                request=self._row_to_request(claimed),
                claim_id=claim_id,
            )

    def release_delivery_claim(
        self,
        *,
        request_id: str,
        claim_id: str,
    ) -> bool:
        """Release a claim when no provider send was attempted.

        Safe pre-send failures such as a disconnected adapter or a transient
        channel lookup may be retried later.  Ambiguous failures after the
        provider send call must instead transition to ``DELIVERY_UNKNOWN``.
        """

        rid = _validate_request_id(request_id)
        claim = _require_text(
            claim_id,
            field="claim_id",
            maximum=256,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return False
            row = self._expire_row_if_due_locked(conn, row, now)
            if (
                row["status"] != DurableRequestStatus.PENDING_DELIVERY.value
                or row["delivery_claim"] != claim
            ):
                return False
            updated = conn.execute(
                """UPDATE durable_requests
                   SET delivery_claim = NULL, delivery_claimed_at = NULL,
                       updated_at = ?
                   WHERE request_id = ? AND status = ?
                     AND delivery_claim = ?""",
                (
                    now,
                    rid,
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    claim,
                ),
            ).rowcount
            return updated == 1

    def switch_claim_to_text_command(
        self,
        *,
        request_id: str,
        claim_id: str,
    ) -> DurableRequest | None:
        """Change an actively claimed native delivery to command text."""

        rid = _validate_request_id(request_id)
        claim = _require_text(
            claim_id,
            field="claim_id",
            maximum=256,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return None
            row = self._expire_row_if_due_locked(conn, row, now)
            if (
                row["status"] != DurableRequestStatus.PENDING_DELIVERY.value
                or row["delivery_claim"] != claim
                or row["delivery_mode"] != DurableRequestDeliveryMode.NATIVE.value
                or not bool(row["text_command_fallback_allowed"])
            ):
                return None
            conn.execute(
                """UPDATE durable_requests
                   SET delivery_mode = ?, command_response_allowed = 1,
                       updated_at = ?
                   WHERE request_id = ? AND status = ?
                     AND delivery_claim = ?""",
                (
                    DurableRequestDeliveryMode.TEXT_COMMAND.value,
                    now,
                    rid,
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    claim,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            assert updated is not None
            return self._row_to_request(updated)

    def bind_delivery(
        self,
        *,
        request_id: str,
        claim_id: str,
        platform: str,
        message_id: str,
    ) -> DurableRequest | None:
        rid = _validate_request_id(request_id)
        claim = _require_text(
            claim_id,
            field="claim_id",
            maximum=256,
            single_line=True,
        )
        platform_value = _require_text(
            platform,
            field="platform",
            maximum=64,
            single_line=True,
        )
        message = _require_text(
            message_id,
            field="message_id",
            maximum=512,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return None
            row = self._expire_row_if_due_locked(conn, row, now)
            status = DurableRequestStatus(row["status"])
            if row["platform"] != platform_value:
                return None

            if status is DurableRequestStatus.PENDING_DELIVERY:
                if row["delivery_claim"] != claim:
                    return None
                next_status = DurableRequestStatus.OPEN
                conn.execute(
                    """UPDATE durable_requests
                       SET status = ?, message_id = ?, delivered_at = ?, updated_at = ?,
                           delivery_claim = NULL, delivery_claimed_at = NULL
                       WHERE request_id = ?""",
                    (next_status.value, message, now, now, rid),
                )
            elif status in {
                DurableRequestStatus.EXPIRED,
                DurableRequestStatus.CANCELLED,
            }:
                # The authoritative terminal transition may win while the
                # provider send is in flight. The returned message id is still
                # valuable: persist it so restart reconciliation can restore a
                # stale-response view without reopening the request.
                existing_message = row["message_id"]
                if existing_message not in (None, message):
                    return None
                if row["delivery_claim"] not in (None, claim):
                    return None
                next_status = status
                conn.execute(
                    """UPDATE durable_requests
                       SET message_id = COALESCE(message_id, ?),
                           delivered_at = COALESCE(delivered_at, ?), updated_at = ?,
                           delivery_claim = NULL, delivery_claimed_at = NULL
                       WHERE request_id = ?""",
                    (message, now, now, rid),
                )
            else:
                return None

            bound = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            assert bound is not None
            logger.info(
                "Durable request delivery bound",
                extra={
                    "request_id": rid,
                    "platform": platform_value,
                    "state": next_status.value,
                },
            )
            return self._row_to_request(bound)

    def fail_delivery(
        self,
        *,
        request_id: str,
        claim_id: str,
        status: DurableRequestStatus,
        reason: str,
    ) -> DurableRequest | None:
        if status not in {
            DurableRequestStatus.UNSUPPORTED,
            DurableRequestStatus.DELIVERY_FAILED,
            DurableRequestStatus.DELIVERY_UNKNOWN,
        }:
            raise DurableRequestError("invalid delivery terminal status")
        rid = _validate_request_id(request_id)
        claim = _require_text(
            claim_id,
            field="claim_id",
            maximum=256,
            single_line=True,
        )
        reason_value = _require_text(
            reason,
            field="reason",
            maximum=256,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return None
            row = self._expire_row_if_due_locked(conn, row, now)
            if (
                row["status"] != DurableRequestStatus.PENDING_DELIVERY.value
                or row["delivery_claim"] != claim
            ):
                return None
            conn.execute(
                """UPDATE durable_requests
                   SET status = ?, terminal_reason = ?, resolved_at = ?,
                       updated_at = ?, delivery_claim = NULL,
                       delivery_claimed_at = NULL,
                       wake_status = CASE
                           WHEN wake_requested = 1 AND wake_status = ? THEN ?
                           ELSE wake_status
                       END,
                       wake_claim = NULL, wake_claimed_at = NULL,
                       wake_reason = NULL
                   WHERE request_id = ?""",
                (
                    status.value,
                    reason_value,
                    now,
                    now,
                    DurableWakeStatus.NOT_REQUESTED.value,
                    DurableWakeStatus.PENDING.value,
                    rid,
                ),
            )
            self._transition_action_for_terminal_request_locked(
                conn,
                request_id=rid,
                request_status=status,
                now=now,
            )
            terminal = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            assert terminal is not None
            return self._row_to_request(terminal)

    def recover_stale_delivery_claims(
        self,
        *,
        stale_after_seconds: float = DELIVERY_CLAIM_STALE_SECONDS,
        delivery_mode: DurableRequestDeliveryMode | None = None,
    ) -> int:
        if stale_after_seconds <= 0:
            raise DurableRequestError("stale_after_seconds must be positive")
        if delivery_mode is not None and not isinstance(
            delivery_mode, DurableRequestDeliveryMode
        ):
            raise DurableRequestError("invalid delivery mode")
        now = float(self._wall_clock())
        cutoff = now - float(stale_after_seconds)
        mode_clause = "" if delivery_mode is None else " AND delivery_mode = ?"
        mode_params: tuple[object, ...] = (
            () if delivery_mode is None else (delivery_mode.value,)
        )
        with self._lock, self._transaction(immediate=True) as conn:
            self._expire_due_locked(conn, now)
            request_ids = [
                row["request_id"]
                for row in conn.execute(
                    f"""SELECT request_id FROM durable_requests
                       WHERE status = ? AND delivery_claim IS NOT NULL
                         AND delivery_claimed_at IS NOT NULL
                         AND delivery_claimed_at <= ?{mode_clause}""",
                    (DurableRequestStatus.PENDING_DELIVERY.value, cutoff, *mode_params),
                )
            ]
            updated = conn.execute(
                f"""UPDATE durable_requests
                   SET status = ?, terminal_reason = ?, resolved_at = ?,
                       updated_at = ?, delivery_claim = NULL,
                       delivery_claimed_at = NULL,
                       wake_status = CASE
                           WHEN wake_requested = 1 AND wake_status = ? THEN ?
                           ELSE wake_status
                       END,
                       wake_claim = NULL, wake_claimed_at = NULL,
                       wake_reason = NULL
                   WHERE status = ? AND delivery_claim IS NOT NULL
                     AND delivery_claimed_at IS NOT NULL
                     AND delivery_claimed_at <= ?{mode_clause}""",
                (
                    DurableRequestStatus.DELIVERY_UNKNOWN.value,
                    "provider_send_outcome_unknown",
                    now,
                    now,
                    DurableWakeStatus.NOT_REQUESTED.value,
                    DurableWakeStatus.PENDING.value,
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    cutoff,
                    *mode_params,
                ),
            ).rowcount
            for request_id in request_ids:
                self._transition_action_for_terminal_request_locked(
                    conn,
                    request_id=request_id,
                    request_status=DurableRequestStatus.DELIVERY_UNKNOWN,
                    now=now,
                )
            return int(updated)

    def claim_next_wake(self) -> DurableWakeClaim | None:
        """Claim one terminal wake event for delivery.

        The request row is the outbox record.  A claim changes only wake
        delivery metadata; it never changes the authoritative request outcome.
        """

        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            self._expire_due_locked(conn, now)
            row = conn.execute(
                """SELECT * FROM durable_requests
                   WHERE wake_status = ? AND status IN (?, ?, ?, ?, ?, ?)
                   ORDER BY wake_attempts,
                            COALESCE(resolved_at, updated_at), request_id
                   LIMIT 1""",
                (
                    DurableWakeStatus.PENDING.value,
                    DurableRequestStatus.SELECTED.value,
                    DurableRequestStatus.EXPIRED.value,
                    DurableRequestStatus.CANCELLED.value,
                    DurableRequestStatus.UNSUPPORTED.value,
                    DurableRequestStatus.DELIVERY_FAILED.value,
                    DurableRequestStatus.DELIVERY_UNKNOWN.value,
                ),
            ).fetchone()
            if row is None:
                return None
            claim_id = _new_claim_id()
            updated = conn.execute(
                """UPDATE durable_requests
                   SET wake_status = ?, wake_claim = ?, wake_claimed_at = ?,
                       wake_attempts = wake_attempts + 1, updated_at = ?
                   WHERE request_id = ? AND wake_status = ?""",
                (
                    DurableWakeStatus.CLAIMED.value,
                    claim_id,
                    now,
                    now,
                    row["request_id"],
                    DurableWakeStatus.PENDING.value,
                ),
            ).rowcount
            if updated != 1:
                return None
            claimed = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (row["request_id"],),
            ).fetchone()
            assert claimed is not None
            return DurableWakeClaim(
                request=self._row_to_request(claimed),
                claim_id=claim_id,
            )

    def release_wake_claim(self, *, request_id: str, claim_id: str) -> bool:
        """Release a wake claim after a retryable routing failure."""

        rid = _validate_request_id(request_id)
        claim = _require_text(
            claim_id,
            field="claim_id",
            maximum=256,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            updated = conn.execute(
                """UPDATE durable_requests
                   SET wake_status = ?, wake_claim = NULL,
                       wake_claimed_at = NULL, updated_at = ?
                   WHERE request_id = ? AND wake_status = ?
                     AND wake_claim = ?""",
                (
                    DurableWakeStatus.PENDING.value,
                    now,
                    rid,
                    DurableWakeStatus.CLAIMED.value,
                    claim,
                ),
            ).rowcount
            return updated == 1

    def complete_wake(self, *, request_id: str, claim_id: str) -> bool:
        """Acknowledge a wake only after a fresh turn was accepted."""

        return self._finish_wake(
            request_id=request_id,
            claim_id=claim_id,
            status=DurableWakeStatus.DELIVERED,
            reason=None,
        )

    def drop_wake(
        self,
        *,
        request_id: str,
        claim_id: str,
        reason: str,
    ) -> bool:
        """Terminally drop an undeliverable wake without altering the result."""

        reason_value = _require_text(
            reason,
            field="reason",
            maximum=256,
            single_line=True,
        )
        return self._finish_wake(
            request_id=request_id,
            claim_id=claim_id,
            status=DurableWakeStatus.DROPPED,
            reason=reason_value,
        )

    def _finish_wake(
        self,
        *,
        request_id: str,
        claim_id: str,
        status: DurableWakeStatus,
        reason: str | None,
    ) -> bool:
        if status not in {DurableWakeStatus.DELIVERED, DurableWakeStatus.DROPPED}:
            raise DurableRequestError("invalid terminal wake status")
        rid = _validate_request_id(request_id)
        claim = _require_text(
            claim_id,
            field="claim_id",
            maximum=256,
            single_line=True,
        )
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            updated = conn.execute(
                """UPDATE durable_requests
                   SET wake_status = ?, wake_claim = NULL,
                       wake_claimed_at = NULL, wake_delivered_at = ?,
                       wake_reason = ?, updated_at = ?
                   WHERE request_id = ? AND wake_status = ?
                     AND wake_claim = ?""",
                (
                    status.value,
                    now if status is DurableWakeStatus.DELIVERED else None,
                    reason,
                    now,
                    rid,
                    DurableWakeStatus.CLAIMED.value,
                    claim,
                ),
            ).rowcount
            return updated == 1

    def recover_stale_wake_claims(
        self,
        *,
        stale_after_seconds: float = WAKE_CLAIM_STALE_SECONDS,
    ) -> int:
        if stale_after_seconds <= 0:
            raise DurableRequestError("stale_after_seconds must be positive")
        now = float(self._wall_clock())
        cutoff = now - float(stale_after_seconds)
        with self._lock, self._transaction(immediate=True) as conn:
            updated = conn.execute(
                """UPDATE durable_requests
                   SET wake_status = ?, wake_claim = NULL,
                       wake_claimed_at = NULL, updated_at = ?
                   WHERE wake_status = ? AND wake_claim IS NOT NULL
                     AND wake_claimed_at IS NOT NULL
                     AND wake_claimed_at <= ?""",
                (
                    DurableWakeStatus.PENDING.value,
                    now,
                    DurableWakeStatus.CLAIMED.value,
                    cutoff,
                ),
            ).rowcount
            return int(updated)

    def list_open(self, available_platforms: Iterable[str]) -> list[DurableRequest]:
        platforms = tuple(sorted({str(value) for value in available_platforms if value}))
        if not platforms:
            return []
        now = float(self._wall_clock())
        placeholders = ",".join("?" for _ in platforms)
        with self._lock, self._transaction(immediate=True) as conn:
            self._expire_due_locked(conn, now)
            rows = conn.execute(
                f"""SELECT * FROM durable_requests
                    WHERE status = ? AND platform IN ({placeholders})
                    ORDER BY created_at, request_id""",
                (DurableRequestStatus.OPEN.value, *platforms),
            ).fetchall()
            return [self._row_to_request(row) for row in rows]

    def list_bound_requests(
        self,
        available_platforms: Iterable[str],
    ) -> list[DurableRequest]:
        """Return rows whose exact provider message may still receive clicks.

        Terminal selected/expired/cancelled rows remain restorable during
        retention so a stale provider component gets an accurate response even
        after a gateway restart.
        """
        platforms = tuple(
            sorted({str(value) for value in available_platforms if value})
        )
        if not platforms:
            return []
        now = float(self._wall_clock())
        placeholders = ",".join("?" for _ in platforms)
        statuses = (
            DurableRequestStatus.OPEN.value,
            DurableRequestStatus.SELECTED.value,
            DurableRequestStatus.EXPIRED.value,
            DurableRequestStatus.CANCELLED.value,
        )
        status_placeholders = ",".join("?" for _ in statuses)
        with self._lock, self._transaction(immediate=True) as conn:
            self._expire_due_locked(conn, now)
            rows = conn.execute(
                f"""SELECT * FROM durable_requests
                    WHERE message_id IS NOT NULL
                      AND status IN ({status_placeholders})
                      AND platform IN ({placeholders})
                    ORDER BY created_at, request_id""",
                (*statuses, *platforms),
            ).fetchall()
            return [self._row_to_request(row) for row in rows]

    def resolve(
        self,
        *,
        request_id: str,
        option_token: str,
        platform: str,
        channel_id: str,
        message_id: str | None,
        actor_id: str | None,
    ) -> DurableResolveResult:
        return self._resolve_native(
            request_id=request_id, option_tokens=(str(option_token),),
            platform=platform, channel_id=channel_id, message_id=message_id,
            actor_id=actor_id, selection_mode="single",
        )

    def resolve_multi_select(
        self, *, request_id: str, option_tokens: tuple[str, ...] | list[str],
        platform: str, channel_id: str, message_id: str | None,
        actor_id: str | None,
    ) -> DurableResolveResult:
        return self._resolve_native(
            request_id=request_id, option_tokens=option_tokens,
            platform=platform, channel_id=channel_id, message_id=message_id,
            actor_id=actor_id, selection_mode="multi",
        )

    def _resolve_native(
        self, *, request_id: str, option_tokens: tuple[str, ...] | list[str],
        platform: str, channel_id: str, message_id: str | None,
        actor_id: str | None, selection_mode: str,
    ) -> DurableResolveResult:
        try:
            rid = _validate_request_id(request_id)
        except DurableRequestError:
            return DurableResolveResult(DurableResolveStatus.NOT_FOUND)
        with self._lock, self._transaction(immediate=True) as conn:
            now = float(self._wall_clock())
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return DurableResolveResult(DurableResolveStatus.NOT_FOUND)
            row = self._expire_row_if_due_locked(conn, row, now)
            status = DurableRequestStatus(row["status"])
            terminal_result = _resolve_status_for_terminal(status)
            if terminal_result is not None:
                return DurableResolveResult(terminal_result)
            if status is not DurableRequestStatus.OPEN:
                return DurableResolveResult(DurableResolveStatus.NOT_DELIVERED)

            actual_message_id = None if message_id is None else str(message_id)
            actual_actor_id = None if actor_id is None else str(actor_id)
            if (
                str(platform) != row["platform"]
                or str(channel_id) != (row["thread_id"] or row["chat_id"])
                or actual_message_id != row["message_id"]
                or not actual_actor_id
            ):
                return DurableResolveResult(DurableResolveStatus.SCOPE_MISMATCH)
            if (
                row["responder_mode"] == DurableResponderMode.INITIATOR.value
                and actual_actor_id != row["expected_actor_id"]
            ):
                return DurableResolveResult(DurableResolveStatus.SCOPE_MISMATCH)

            if row["selection_mode"] != selection_mode or row["delivery_mode"] != "native":
                return DurableResolveResult(DurableResolveStatus.INVALID_OPTION)
            if not isinstance(option_tokens, (tuple, list)) or any(
                not isinstance(token, str) or re.fullmatch(r"[0-9]+", token) is None
                or len(token) > (2 if selection_mode == "multi" else 128)
                or (selection_mode == "multi" and token != str(int(token)))
                for token in option_tokens
            ):
                if row["selection_mode"] == "multi":
                    self._record_multi_select("submissions")
                    self._record_multi_select("invalid_sets")
                return DurableResolveResult(DurableResolveStatus.INVALID_OPTION)
            return self._resolve_selected_slots_locked(
                conn, row=row, slots=tuple(int(token) for token in option_tokens),
                actor_id=actual_actor_id,
                source=DurableRequestResolutionSource.NATIVE, now=now,
            )

    def resolve_command(
        self,
        *,
        request_id: str,
        option_slots: tuple[int, ...],
        platform: str,
        channel_id: str,
        actor_id: str | None,
    ) -> DurableResolveResult:
        try:
            rid = _validate_request_id(request_id)
        except DurableRequestError:
            return DurableResolveResult(DurableResolveStatus.NOT_FOUND)
        with self._lock, self._transaction(immediate=True) as conn:
            now = float(self._wall_clock())
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return DurableResolveResult(DurableResolveStatus.NOT_FOUND)
            row = self._expire_row_if_due_locked(conn, row, now)
            if str(platform) != row["platform"]:
                return DurableResolveResult(DurableResolveStatus.SCOPE_MISMATCH)
            if str(channel_id) != (row["thread_id"] or row["chat_id"]):
                return DurableResolveResult(DurableResolveStatus.WRONG_CHANNEL)

            actual_actor_id = None if actor_id is None else str(actor_id)
            if not actual_actor_id or (
                row["responder_mode"] == DurableResponderMode.INITIATOR.value
                and actual_actor_id != row["expected_actor_id"]
            ):
                return DurableResolveResult(DurableResolveStatus.WRONG_ACTOR)

            status = DurableRequestStatus(row["status"])
            terminal_result = _resolve_status_for_terminal(status)
            if terminal_result is not None:
                return DurableResolveResult(terminal_result)
            if status is not DurableRequestStatus.OPEN:
                return DurableResolveResult(DurableResolveStatus.NOT_DELIVERED)
            if not bool(row["command_response_allowed"]):
                return DurableResolveResult(DurableResolveStatus.COMMAND_DISABLED)
            return self._resolve_selected_slots_locked(
                conn, row=row, slots=option_slots, actor_id=actual_actor_id,
                source=DurableRequestResolutionSource.COMMAND, now=now,
            )

    def _resolve_selected_slots_locked(
        self,
        conn: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        slots: tuple[int, ...],
        actor_id: str,
        source: DurableRequestResolutionSource,
        now: float,
    ) -> DurableResolveResult:
        options = _deserialize_options(row["options_json"])
        if row["selection_mode"] == "multi":
            self._record_multi_select("submissions")
        if (
            not isinstance(slots, tuple)
            or not row["min_selections"] <= len(slots) <= row["max_selections"]
            or any(type(slot) is not int or not 1 <= slot <= len(options) for slot in slots)
            or len(set(slots)) != len(slots)
        ):
            if row["selection_mode"] == "multi":
                self._record_multi_select("invalid_sets")
            return DurableResolveResult(DurableResolveStatus.INVALID_OPTION)
        selected_ids = tuple(options[slot - 1].id for slot in sorted(slots))
        selected_option_id = selected_ids[0] if row["selection_mode"] == "single" else None
        selected_json = json.dumps(selected_ids) if row["selection_mode"] == "multi" else None
        request_id = row["request_id"]
        updated = conn.execute(
            """UPDATE durable_requests
               SET status = ?, selected_option_id = ?, selected_option_ids_json = ?,
                   resolved_by_actor_id = ?, resolved_at = ?, updated_at = ?,
                   terminal_reason = NULL, resolution_source = ?,
                   wake_status = CASE
                       WHEN wake_requested = 1 AND wake_status = ? THEN ?
                       ELSE wake_status
                   END,
                   wake_claim = NULL, wake_claimed_at = NULL,
                   wake_reason = NULL
               WHERE request_id = ? AND status = ?""",
            (
                DurableRequestStatus.SELECTED.value,
                selected_option_id,
                selected_json,
                actor_id,
                now,
                now,
                source.value,
                DurableWakeStatus.NOT_REQUESTED.value,
                DurableWakeStatus.PENDING.value,
                request_id,
                DurableRequestStatus.OPEN.value,
            ),
        ).rowcount
        if updated != 1:
            current = conn.execute(
                "SELECT status FROM durable_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if current is None:
                return DurableResolveResult(DurableResolveStatus.NOT_FOUND)
            mapped = _resolve_status_for_terminal(DurableRequestStatus(current["status"]))
            return DurableResolveResult(mapped or DurableResolveStatus.NOT_DELIVERED)

        self._transition_action_for_terminal_request_locked(
            conn,
            request_id=request_id,
            request_status=DurableRequestStatus.SELECTED,
            selected_option_id=selected_option_id,
            now=now,
        )
        resolved = conn.execute(
            "SELECT * FROM durable_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        assert resolved is not None
        request = self._row_to_request(resolved)
        logger.info(
            "Durable request resolved",
            extra={
                "request_id": request_id,
                "platform": row["platform"],
                "resolution_status": DurableResolveStatus.RESOLVED.value,
            },
        )
        return DurableResolveResult(DurableResolveStatus.RESOLVED, request=request)

    def mark_open_failed(
        self,
        *,
        request_id: str,
        message_id: str,
        reason: str,
    ) -> DurableRequest | None:
        rid = _validate_request_id(request_id)
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            if row is None:
                return None
            row = self._expire_row_if_due_locked(conn, row, now)
            if (
                row["status"] != DurableRequestStatus.OPEN.value
                or row["message_id"] != str(message_id)
            ):
                return None
            conn.execute(
                """UPDATE durable_requests
                   SET status = ?, terminal_reason = ?, resolved_at = ?,
                       updated_at = ?,
                       wake_status = CASE
                           WHEN wake_requested = 1 AND wake_status = ? THEN ?
                           ELSE wake_status
                       END,
                       wake_claim = NULL, wake_claimed_at = NULL,
                       wake_reason = NULL
                   WHERE request_id = ?""",
                (
                    DurableRequestStatus.DELIVERY_FAILED.value,
                    _require_text(
                        reason,
                        field="reason",
                        maximum=256,
                        single_line=True,
                    ),
                    now,
                    now,
                    DurableWakeStatus.NOT_REQUESTED.value,
                    DurableWakeStatus.PENDING.value,
                    rid,
                ),
            )
            self._transition_action_for_terminal_request_locked(
                conn,
                request_id=rid,
                request_status=DurableRequestStatus.DELIVERY_FAILED,
                now=now,
            )
            terminal = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()
            assert terminal is not None
            return self._row_to_request(terminal)

    def expire_due(self) -> int:
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            return self._expire_due_locked(conn, now)

    def prune_terminal(self, retention_seconds: float) -> int:
        if retention_seconds <= 0:
            raise DurableRequestError("retention_seconds must be positive")
        now = float(self._wall_clock())
        with self._lock, self._transaction(immediate=True) as conn:
            return self._prune_locked(
                conn,
                now=now,
                retention_seconds=float(retention_seconds),
            )

    def count_by_status(self) -> Mapping[DurableRequestStatus, int]:
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT status, COUNT(*) AS count FROM durable_requests GROUP BY status"
                ).fetchall()
            finally:
                conn.close()
        return {DurableRequestStatus(row["status"]): int(row["count"]) for row in rows}

    def _find_row_locked(
        self,
        conn: sqlite3.Connection,
        *,
        request_id: str | None,
        origin_type: str | None,
        origin_id: str | None,
        reference: str | None,
        require_origin: bool = False,
    ) -> sqlite3.Row | None:
        if request_id is not None:
            rid = _validate_request_id(request_id)
            if require_origin or origin_type is not None or origin_id is not None:
                if not origin_type or not origin_id:
                    raise DurableRequestError("origin scope is required")
                return conn.execute(
                    """SELECT * FROM durable_requests
                       WHERE request_id = ? AND origin_type = ? AND origin_id = ?""",
                    (rid, str(origin_type), str(origin_id)),
                ).fetchone()
            return conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (rid,),
            ).fetchone()

        if reference is None:
            raise DurableRequestError("request_id or reference is required")
        if not origin_type or not origin_id:
            raise DurableRequestError("reference lookup requires origin scope")
        reference_value = _require_text(
            reference,
            field="reference",
            maximum=MAX_DURABLE_REFERENCE_LENGTH,
            single_line=True,
        )
        return conn.execute(
            """SELECT * FROM durable_requests
               WHERE origin_type = ? AND origin_id = ? AND reference = ?""",
            (str(origin_type), str(origin_id), reference_value),
        ).fetchone()

    def _expire_row_if_due_locked(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        now: float,
    ) -> sqlite3.Row:
        if (
            DurableRequestStatus(row["status"]) in _ACTIVE_STATUSES
            and float(row["expires_at"]) <= now
        ):
            updated = conn.execute(
                """UPDATE durable_requests
                   SET status = ?, terminal_reason = ?, resolved_at = ?,
                       updated_at = ?, delivery_claim = NULL,
                       delivery_claimed_at = NULL,
                       wake_status = CASE
                           WHEN wake_requested = 1 AND wake_status = ? THEN ?
                           ELSE wake_status
                       END,
                       wake_claim = NULL, wake_claimed_at = NULL,
                       wake_reason = NULL
                   WHERE request_id = ? AND status IN (?, ?)""",
                (
                    DurableRequestStatus.EXPIRED.value,
                    "deadline_elapsed",
                    now,
                    now,
                    DurableWakeStatus.NOT_REQUESTED.value,
                    DurableWakeStatus.PENDING.value,
                    row["request_id"],
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    DurableRequestStatus.OPEN.value,
                ),
            ).rowcount
            if updated == 1:
                self._transition_action_for_terminal_request_locked(
                    conn,
                    request_id=row["request_id"],
                    request_status=DurableRequestStatus.EXPIRED,
                    now=now,
                )
            refreshed = conn.execute(
                "SELECT * FROM durable_requests WHERE request_id = ?",
                (row["request_id"],),
            ).fetchone()
            assert refreshed is not None
            return refreshed
        return row

    def _expire_due_locked(self, conn: sqlite3.Connection, now: float) -> int:
        request_ids = [
            row["request_id"]
            for row in conn.execute(
                """SELECT request_id FROM durable_requests
                   WHERE status IN (?, ?) AND expires_at <= ?""",
                (
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    DurableRequestStatus.OPEN.value,
                    now,
                ),
            )
        ]
        updated = int(
            conn.execute(
                """UPDATE durable_requests
                   SET status = ?, terminal_reason = ?, resolved_at = ?,
                       updated_at = ?, delivery_claim = NULL,
                       delivery_claimed_at = NULL,
                       wake_status = CASE
                           WHEN wake_requested = 1 AND wake_status = ? THEN ?
                           ELSE wake_status
                       END,
                       wake_claim = NULL, wake_claimed_at = NULL,
                       wake_reason = NULL
                   WHERE status IN (?, ?) AND expires_at <= ?""",
                (
                    DurableRequestStatus.EXPIRED.value,
                    "deadline_elapsed",
                    now,
                    now,
                    DurableWakeStatus.NOT_REQUESTED.value,
                    DurableWakeStatus.PENDING.value,
                    DurableRequestStatus.PENDING_DELIVERY.value,
                    DurableRequestStatus.OPEN.value,
                    now,
                ),
            ).rowcount
        )
        for request_id in request_ids:
            self._transition_action_for_terminal_request_locked(
                conn,
                request_id=request_id,
                request_status=DurableRequestStatus.EXPIRED,
                now=now,
            )
        return updated

    @staticmethod
    def _transition_action_for_terminal_request_locked(
        conn: sqlite3.Connection,
        *,
        request_id: str,
        request_status: DurableRequestStatus,
        now: float,
        selected_option_id: str | None = None,
    ) -> None:
        if request_status is DurableRequestStatus.SELECTED:
            ticket = conn.execute(
                """SELECT a.action_type, r.kind FROM durable_action_tickets a
                   JOIN durable_requests r ON r.request_id=a.request_id
                   WHERE a.request_id=?""", (request_id,),
            ).fetchone()
            scoped_approval = (selected_option_id in {"session", "always"}
                               and ticket is not None
                               and _supports_execution_approval_scopes(ticket[0], ticket[1]))
            action_status, reason = {
                "approve": (DurableActionStatus.READY, None),
                "reject": (DurableActionStatus.DECLINED, "rejected"),
            }.get(
                selected_option_id,
                (DurableActionStatus.READY, None) if scoped_approval else
                (DurableActionStatus.ABANDONED, "invalid_decision"),
            )
        else:
            action_status, reason = {
                DurableRequestStatus.EXPIRED: (
                    DurableActionStatus.EXPIRED,
                    "deadline_elapsed",
                ),
                DurableRequestStatus.CANCELLED: (
                    DurableActionStatus.CANCELLED,
                    "cancelled_by_origin",
                ),
            }.get(
                request_status,
                (DurableActionStatus.ABANDONED, request_status.value),
            )
        completed_at = None if action_status is DurableActionStatus.READY else now
        notification_status = (
            DurableActionNotificationStatus.NOT_READY
            if action_status is DurableActionStatus.READY
            else DurableActionNotificationStatus.PENDING
        )

        updated = conn.execute(
            """UPDATE durable_action_tickets
               SET status = ?, outcome_reason = ?, completed_at = ?,
                   notification_status = ?, updated_at = ?
               WHERE request_id = ? AND status = ?""",
            (
                action_status.value,
                reason,
                completed_at,
                notification_status.value,
                now,
                request_id,
                DurableActionStatus.WAITING_FOR_DECISION.value,
            ),
        ).rowcount
        if updated == 1:
            conn.execute(
                """UPDATE durable_requests
                   SET terminal_reason = ?, wake_requested = 0,
                       wake_status = ?, wake_claim = NULL,
                       wake_claimed_at = NULL, wake_reason = NULL
                   WHERE request_id = ?""",
                (
                    reason,
                    DurableWakeStatus.NOT_REQUESTED.value,
                    request_id,
                ),
            )

    def _prune_locked(
        self,
        conn: sqlite3.Connection,
        *,
        now: float,
        retention_seconds: float,
    ) -> int:
        terminal_values = tuple(status.value for status in _TERMINAL_STATUSES)
        placeholders = ",".join("?" for _ in terminal_values)
        action_placeholders = ",".join("?" for _ in _TERMINAL_ACTION_STATUSES)
        cutoff = now - retention_seconds
        eligible_action = f"""(
            NOT EXISTS (
                SELECT 1 FROM durable_action_tickets AS ticket
                WHERE ticket.request_id = durable_requests.request_id
            )
            OR EXISTS (
                SELECT 1 FROM durable_action_tickets AS ticket
                WHERE ticket.request_id = durable_requests.request_id
                  AND (
                      (ticket.status IN ({action_placeholders}) AND ticket.status != 'unknown')
                      OR (
                          ticket.status = 'unknown'
                          AND ticket.completed_at <= ?
                          AND EXISTS (
                          SELECT 1 FROM durable_action_reconciliations AS reconciliation
                          WHERE reconciliation.request_id = ticket.request_id
                          )
                      )
                  )
                  AND ticket.notification_status IN (?, ?)
                  AND ticket.completed_at IS NOT NULL
            )
        )"""
        action_retention_due = """(
            NOT EXISTS (
                SELECT 1 FROM durable_action_tickets AS ticket
                WHERE ticket.request_id = durable_requests.request_id
            )
            OR EXISTS (
                SELECT 1 FROM durable_action_tickets AS ticket
                WHERE ticket.request_id = durable_requests.request_id
                  AND ticket.completed_at <= ?
            )
        )"""
        action_params = (
            *_TERMINAL_ACTION_STATUSES,
            cutoff,
            DurableActionNotificationStatus.DELIVERED.value,
            DurableActionNotificationStatus.DROPPED.value,
        )
        age_ids = [
            row["request_id"]
            for row in conn.execute(
                f"""SELECT request_id FROM durable_requests
                    WHERE status IN ({placeholders})
                      AND wake_status NOT IN (?, ?)
                      AND COALESCE(resolved_at, updated_at) <= ?
                      AND {eligible_action}
                      AND {action_retention_due}""",
                (
                    *terminal_values,
                    DurableWakeStatus.PENDING.value,
                    DurableWakeStatus.CLAIMED.value,
                    cutoff,
                    *action_params,
                    cutoff,
                ),
            )
        ]
        conn.executemany(
            "DELETE FROM durable_action_reconciliations WHERE request_id = ?",
            ((request_id,) for request_id in age_ids),
        )
        conn.executemany(
            "DELETE FROM durable_action_tickets WHERE request_id = ?",
            ((request_id,) for request_id in age_ids),
        )
        conn.executemany(
            "DELETE FROM durable_requests WHERE request_id = ?",
            ((request_id,) for request_id in age_ids),
        )
        terminal_count = conn.execute(
            f"""SELECT COUNT(*) FROM durable_requests
                WHERE status IN ({placeholders})
                  AND wake_status NOT IN (?, ?)""",
            (
                *terminal_values,
                DurableWakeStatus.PENDING.value,
                DurableWakeStatus.CLAIMED.value,
            ),
        ).fetchone()[0]
        excess = int(terminal_count) - self._max_terminal
        overflow_ids: list[str] = []
        if excess > 0:
            overflow_ids = [
                row["request_id"]
                for row in conn.execute(
                    f"""SELECT request_id FROM durable_requests
                        WHERE status IN ({placeholders})
                          AND wake_status NOT IN (?, ?)
                          AND {eligible_action}
                        ORDER BY COALESCE(resolved_at, updated_at), request_id
                        LIMIT ?""",
                (
                    *terminal_values,
                    DurableWakeStatus.PENDING.value,
                    DurableWakeStatus.CLAIMED.value,
                    *action_params,
                    excess,
                ),
                )
            ]
            conn.executemany(
                "DELETE FROM durable_action_reconciliations WHERE request_id = ?",
                ((request_id,) for request_id in overflow_ids),
            )
            conn.executemany(
                "DELETE FROM durable_action_tickets WHERE request_id = ?",
                ((request_id,) for request_id in overflow_ids),
            )
            conn.executemany(
                "DELETE FROM durable_requests WHERE request_id = ?",
                ((request_id,) for request_id in overflow_ids),
            )
        return len(age_ids) + len(overflow_ids)

    @staticmethod
    def _row_to_request(row: sqlite3.Row) -> DurableRequest:
        return DurableRequest(
            request_id=row["request_id"],
            kind=row["kind"],
            title=row["title"],
            body=row["body"],
            options=_deserialize_options(row["options_json"]),
            status=DurableRequestStatus(row["status"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            expires_at=float(row["expires_at"]),
            target=DurableRequestTarget(
                platform=row["platform"],
                chat_id=row["chat_id"],
                thread_id=row["thread_id"],
            ),
            responder_mode=DurableResponderMode(row["responder_mode"]),
            expected_actor_id=row["expected_actor_id"],
            origin_type=row["origin_type"],
            origin_id=row["origin_id"],
            reference=row["reference"],
            delivery_mode=DurableRequestDeliveryMode(row["delivery_mode"]),
            command_response_allowed=bool(row["command_response_allowed"]),
            text_command_fallback_allowed=bool(
                row["text_command_fallback_allowed"]
            ),
            resolution_source=(
                None
                if row["resolution_source"] is None
                else DurableRequestResolutionSource(row["resolution_source"])
            ),
            wake_requested=bool(row["wake_requested"]),
            wake_status=DurableWakeStatus(row["wake_status"]),
            origin_session_id=row["origin_session_id"],
            origin_ui_session_id=row["origin_ui_session_id"],
            origin_profile=row["origin_profile"],
            message_id=row["message_id"],
            selected_option_id=row["selected_option_id"],
            selection_mode=row["selection_mode"],
            min_selections=int(row["min_selections"]),
            max_selections=int(row["max_selections"]),
            selected_option_ids=(
                tuple(json.loads(row["selected_option_ids_json"]))
                if row["selected_option_ids_json"] is not None else ()
            ),
            resolved_by_actor_id=row["resolved_by_actor_id"],
            resolved_at=(
                None if row["resolved_at"] is None else float(row["resolved_at"])
            ),
            terminal_reason=row["terminal_reason"],
            delivery_attempts=int(row["delivery_attempts"] or 0),
            delivery_claim=row["delivery_claim"],
            delivery_claimed_at=(
                None
                if row["delivery_claimed_at"] is None
                else float(row["delivery_claimed_at"])
            ),
            delivered_at=(
                None if row["delivered_at"] is None else float(row["delivered_at"])
            ),
            wake_attempts=int(row["wake_attempts"] or 0),
            wake_claim=row["wake_claim"],
            wake_claimed_at=(
                None
                if row["wake_claimed_at"] is None
                else float(row["wake_claimed_at"])
            ),
            wake_delivered_at=(
                None
                if row["wake_delivered_at"] is None
                else float(row["wake_delivered_at"])
            ),
            wake_reason=row["wake_reason"],
        )


def _validate_target(target: DurableRequestTarget) -> DurableRequestTarget:
    if not isinstance(target, DurableRequestTarget):
        raise DurableRequestError("target must be a DurableRequestTarget")
    platform = _require_text(
        target.platform,
        field="platform",
        maximum=64,
        single_line=True,
    ).lower()
    chat_id = _require_text(
        target.chat_id,
        field="chat_id",
        maximum=512,
        single_line=True,
    )
    thread_id = _normalize_optional_text(
        target.thread_id,
        field="thread_id",
        maximum=512,
        single_line=True,
    )
    return DurableRequestTarget(
        platform=platform,
        chat_id=chat_id,
        thread_id=thread_id,
    )


def _resolve_status_for_terminal(
    status: DurableRequestStatus,
) -> DurableResolveStatus | None:
    return {
        DurableRequestStatus.SELECTED: DurableResolveStatus.ALREADY_RESOLVED,
        DurableRequestStatus.EXPIRED: DurableResolveStatus.EXPIRED,
        DurableRequestStatus.CANCELLED: DurableResolveStatus.CANCELLED,
        DurableRequestStatus.UNSUPPORTED: DurableResolveStatus.UNSUPPORTED,
        DurableRequestStatus.DELIVERY_FAILED: DurableResolveStatus.DELIVERY_FAILED,
        DurableRequestStatus.DELIVERY_UNKNOWN: DurableResolveStatus.DELIVERY_UNKNOWN,
    }.get(status)


_DURABLE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS durable_requests (
    request_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    options_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    platform TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    thread_id TEXT,
    message_id TEXT,
    responder_mode TEXT NOT NULL,
    expected_actor_id TEXT,
    origin_type TEXT NOT NULL,
    origin_id TEXT NOT NULL,
    reference TEXT,
    delivery_mode TEXT NOT NULL DEFAULT 'native',
    command_response_allowed INTEGER NOT NULL DEFAULT 0,
    text_command_fallback_allowed INTEGER NOT NULL DEFAULT 0,
    resolution_source TEXT,
    selected_option_id TEXT,
    selection_mode TEXT NOT NULL DEFAULT 'single',
    min_selections INTEGER NOT NULL DEFAULT 1,
    max_selections INTEGER NOT NULL DEFAULT 1,
    selected_option_ids_json TEXT,
    resolved_by_actor_id TEXT,
    resolved_at REAL,
    terminal_reason TEXT,
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    delivery_claim TEXT,
    delivery_claimed_at REAL,
    delivered_at REAL,
    wake_requested INTEGER NOT NULL DEFAULT 0,
    wake_status TEXT NOT NULL DEFAULT 'not_requested',
    wake_attempts INTEGER NOT NULL DEFAULT 0,
    wake_claim TEXT,
    wake_claimed_at REAL,
    wake_delivered_at REAL,
    wake_reason TEXT,
    origin_session_id TEXT,
    origin_ui_session_id TEXT,
    origin_profile TEXT
);
CREATE INDEX IF NOT EXISTS idx_durable_requests_status_expiry
    ON durable_requests(status, expires_at);
CREATE INDEX IF NOT EXISTS idx_durable_requests_delivery
    ON durable_requests(status, delivery_claimed_at);
CREATE INDEX IF NOT EXISTS idx_durable_requests_platform
    ON durable_requests(platform, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_durable_requests_origin_reference
    ON durable_requests(origin_type, origin_id, reference)
    WHERE reference IS NOT NULL;
"""


_DURABLE_COLUMN_DDL = {
    "selection_mode": "TEXT NOT NULL DEFAULT 'single'",
    "min_selections": "INTEGER NOT NULL DEFAULT 1",
    "max_selections": "INTEGER NOT NULL DEFAULT 1",
    "selected_option_ids_json": "TEXT",
    "delivery_mode": "TEXT NOT NULL DEFAULT 'native'",
    "command_response_allowed": "INTEGER NOT NULL DEFAULT 0",
    "text_command_fallback_allowed": "INTEGER NOT NULL DEFAULT 0",
    "resolution_source": "TEXT",
    "wake_requested": "INTEGER NOT NULL DEFAULT 0",
    "wake_status": "TEXT NOT NULL DEFAULT 'not_requested'",
    "wake_attempts": "INTEGER NOT NULL DEFAULT 0",
    "wake_claim": "TEXT",
    "wake_claimed_at": "REAL",
    "wake_delivered_at": "REAL",
    "wake_reason": "TEXT",
    "origin_session_id": "TEXT",
    "origin_ui_session_id": "TEXT",
    "origin_profile": "TEXT",
}


_STORE_CACHE: dict[Path, DurableRequestStore] = {}
_STORE_CACHE_LOCK = threading.Lock()


def get_durable_request_store() -> DurableRequestStore:
    path = Path(get_hermes_home()) / "state.db"
    with _STORE_CACHE_LOCK:
        store = _STORE_CACHE.get(path)
        if store is None:
            store = DurableRequestStore(path)
            _STORE_CACHE[path] = store
        return store


def reset_durable_request_store_cache() -> None:
    """Clear module-level store wrappers. Intended for tests only."""

    with _STORE_CACHE_LOCK:
        _STORE_CACHE.clear()


def load_durable_requests_config() -> DurableRequestsConfig:
    """Load and validate the active profile's durable-request configuration."""

    try:
        from hermes_cli.config import load_config_readonly

        raw_config = load_config_readonly()
    except Exception as exc:
        return DurableRequestsConfig(
            enabled=False,
            target=None,
            default_ttl_seconds=DEFAULT_DURABLE_REQUEST_TTL_SECONDS,
            max_ttl_seconds=MAX_DURABLE_REQUEST_TTL_SECONDS,
            terminal_retention_seconds=DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS,
            scheduled_responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            reason=f"configuration_unavailable:{type(exc).__name__}",
        )

    raw = raw_config.get("durable_requests", {}) if isinstance(raw_config, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    from utils import is_truthy_value

    enabled = is_truthy_value(raw.get("enabled", False), default=False)

    try:
        command_responses = raw.get("command_responses", {})
        if not isinstance(command_responses, dict):
            raise DurableRequestError("command_responses must be a mapping")
        command_responses_enabled = is_truthy_value(
            command_responses.get("enabled", False), default=False
        )
        allow_on_native_delivery = is_truthy_value(
            command_responses.get("allow_on_native_delivery", True), default=True
        )
        allow_text_delivery_fallback = is_truthy_value(
            command_responses.get("allow_text_delivery_fallback", True), default=True
        )
        max_ttl = _validate_config_seconds(
            raw.get("max_ttl_seconds", MAX_DURABLE_REQUEST_TTL_SECONDS),
            field="max_ttl_seconds",
            maximum=MAX_DURABLE_REQUEST_TTL_SECONDS,
        )
        default_ttl = _validate_config_seconds(
            raw.get("default_ttl_seconds", DEFAULT_DURABLE_REQUEST_TTL_SECONDS),
            field="default_ttl_seconds",
            maximum=max_ttl,
        )
        retention = _validate_config_seconds(
            raw.get(
                "terminal_retention_seconds",
                DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS,
            ),
            field="terminal_retention_seconds",
            maximum=365 * 24 * 60 * 60.0,
        )
        responder_mode = DurableResponderMode(
            str(
                raw.get(
                    "scheduled_responder_policy",
                    DurableResponderMode.ANY_AUTHORIZED.value,
                )
            )
        )
        if responder_mode is not DurableResponderMode.ANY_AUTHORIZED:
            raise DurableRequestError(
                "scheduled_responder_policy must be any_authorized"
            )

        inbox = raw.get("inbox", {})
        if not isinstance(inbox, dict):
            raise DurableRequestError("inbox must be a mapping")
        platform = str(inbox.get("platform", "")).strip().lower()
        chat_id = str(inbox.get("chat_id", "")).strip()
        thread_raw = inbox.get("thread_id")
        thread_id = None if thread_raw in (None, "") else str(thread_raw).strip()
        target = None
        if enabled:
            if platform != "discord":
                raise DurableRequestError(
                    "phase-one durable requests require a Discord inbox"
                )
            if not chat_id:
                raise DurableRequestError("durable request inbox chat_id is required")
            if not chat_id.isdigit() or int(chat_id) <= 0:
                raise DurableRequestError(
                    "durable request inbox chat_id must be a Discord channel id"
                )
            if thread_id is not None and (
                not thread_id.isdigit() or int(thread_id) <= 0
            ):
                raise DurableRequestError(
                    "durable request inbox thread_id must be a Discord channel id"
                )
            target = _validate_target(
                DurableRequestTarget(
                    platform=platform,
                    chat_id=chat_id,
                    thread_id=thread_id,
                )
            )
        return DurableRequestsConfig(
            enabled=enabled,
            target=target,
            default_ttl_seconds=default_ttl,
            max_ttl_seconds=max_ttl,
            terminal_retention_seconds=retention,
            scheduled_responder_mode=responder_mode,
            command_responses_enabled=command_responses_enabled,
            allow_on_native_delivery=allow_on_native_delivery,
            allow_text_delivery_fallback=allow_text_delivery_fallback,
            reason=None if enabled else "disabled",
        )
    except (DurableRequestError, ValueError) as exc:
        return DurableRequestsConfig(
            enabled=enabled,
            target=None,
            default_ttl_seconds=DEFAULT_DURABLE_REQUEST_TTL_SECONDS,
            max_ttl_seconds=MAX_DURABLE_REQUEST_TTL_SECONDS,
            terminal_retention_seconds=DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS,
            scheduled_responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            reason=str(exc),
        )


def _validate_config_seconds(value: object, *, field: str, maximum: float) -> float:
    try:
        result = _validate_ttl(value, maximum=maximum)
    except DurableRequestError as exc:
        raise DurableRequestError(str(exc).replace("ttl_seconds", field)) from exc
    return result


def create_durable_request(
    spec: DurableRequestSpec,
    *,
    target: DurableRequestTarget,
    responder_mode: DurableResponderMode,
    expected_actor_id: str | None,
    origin_type: str,
    origin_id: str,
    command_response_allowed: bool = False,
    text_command_fallback_allowed: bool = False,
    wake_on_terminal: bool = False,
    origin_session_id: str | None = None,
    origin_ui_session_id: str | None = None,
    origin_profile: str | None = None,
    maximum_ttl_seconds: float = MAX_DURABLE_REQUEST_TTL_SECONDS,
    terminal_retention_seconds: float = (
        DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS
    ),
) -> DurableCreateResult:
    return get_durable_request_store().create(
        spec,
        target=target,
        responder_mode=responder_mode,
        expected_actor_id=expected_actor_id,
        origin_type=origin_type,
        origin_id=origin_id,
        command_response_allowed=command_response_allowed,
        text_command_fallback_allowed=text_command_fallback_allowed,
        wake_on_terminal=wake_on_terminal,
        origin_session_id=origin_session_id,
        origin_ui_session_id=origin_ui_session_id,
        origin_profile=origin_profile,
        maximum_ttl_seconds=maximum_ttl_seconds,
        terminal_retention_seconds=terminal_retention_seconds,
    )


def get_durable_request(
    *,
    request_id: str | None = None,
    origin_type: str | None = None,
    origin_id: str | None = None,
    reference: str | None = None,
) -> DurableRequest | None:
    return get_durable_request_store().get(
        request_id=request_id,
        origin_type=origin_type,
        origin_id=origin_id,
        reference=reference,
    )


def cancel_durable_request(
    *,
    origin_type: str,
    origin_id: str,
    request_id: str | None = None,
    reference: str | None = None,
) -> DurableRequest | None:
    return get_durable_request_store().cancel(
        origin_type=origin_type,
        origin_id=origin_id,
        request_id=request_id,
        reference=reference,
    )


def resolve_durable_request(
    *,
    request_id: str,
    option_token: str,
    platform: str,
    channel_id: str,
    message_id: str | None,
    actor_id: str | None,
) -> DurableResolveResult:
    return get_durable_request_store().resolve(
        request_id=request_id,
        option_token=option_token,
        platform=platform,
        channel_id=channel_id,
        message_id=message_id,
        actor_id=actor_id,
    )


def resolve_durable_multi_select_request(
    *, request_id: str, option_tokens: tuple[str, ...] | list[str],
    platform: str, channel_id: str, message_id: str | None,
    actor_id: str | None,
) -> DurableResolveResult:
    return get_durable_request_store().resolve_multi_select(
        request_id=request_id, option_tokens=option_tokens, platform=platform,
        channel_id=channel_id, message_id=message_id, actor_id=actor_id,
    )


def resolve_durable_request_command(
    *,
    request_id: str,
    option_slots: tuple[int, ...],
    platform: str,
    channel_id: str,
    actor_id: str | None,
) -> DurableResolveResult:
    return get_durable_request_store().resolve_command(
        request_id=request_id,
        option_slots=option_slots,
        platform=platform,
        channel_id=channel_id,
        actor_id=actor_id,
    )


__all__ = [
    "make_durable_multi_select_component_id",
    "parse_durable_multi_select_component_id",
    "resolve_durable_multi_select_request",
    "DEFAULT_DURABLE_REQUEST_TTL_SECONDS",
    "DEFAULT_DURABLE_TERMINAL_RETENTION_SECONDS",
    "DELIVERY_CLAIM_STALE_SECONDS",
    "DurableCreateResult",
    "DurableDeliveryClaim",
    "DurableWakeClaim",
    "DurableRequest",
    "DurableRequestDeliveryMode",
    "DurableRequestError",
    "DurableRequestLimitError",
    "DurableRequestOption",
    "DurableRequestResolutionSource",
    "DurableRequestSpec",
    "DurableRequestStatus",
    "DurableRequestStore",
    "DurableRequestTarget",
    "DurableRequestsConfig",
    "DurableResponderMode",
    "DurableWakeStatus",
    "DurableResolveResult",
    "DurableResolveStatus",
    "MAX_ACTIVE_DURABLE_REQUESTS",
    "MAX_DELIVERY_BATCH",
    "MAX_WAKE_BATCH",
    "MAX_DURABLE_REQUEST_TTL_SECONDS",
    "MAX_DURABLE_REFERENCE_LENGTH",
    "MIN_DURABLE_OPTIONS",
    "WAKE_CLAIM_STALE_SECONDS",
    "cancel_durable_request",
    "create_durable_request",
    "get_durable_request",
    "get_durable_request_store",
    "load_durable_requests_config",
    "make_durable_component_id",
    "render_durable_request_content",
    "render_durable_request_text_command_content",
    "reset_durable_request_store_cache",
    "resolve_durable_request",
    "resolve_durable_request_command",
    "validate_durable_request_spec",
]
