"""Model-facing persisted finite-choice request tool.

The tool creates and queries detached user requests in the configured inbox.
It is not an authorization primitive and cannot satisfy terminal or application
approval checks.  Delivery target and responder policy are trusted operator /
gateway inputs, never model-provided fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from gateway.durable_requests import (
    MAX_DURABLE_REFERENCE_LENGTH,
    MIN_DURABLE_OPTIONS,
    DurableRequest,
    DurableRequestError,
    DurableRequestLimitError,
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStatus,
    DurableResponderMode,
    DurableWakeStatus,
    cancel_durable_request,
    create_durable_request,
    get_durable_request,
    load_durable_requests_config,
)
from gateway.interactive import (
    MAX_PROMPT_BODY_LENGTH,
    MAX_PROMPT_OPTION_LABEL_LENGTH,
    MAX_PROMPT_OPTIONS,
    MAX_PROMPT_TITLE_LENGTH,
)
from gateway.session_context import get_session_env
from tools.registry import registry, tool_error, tool_result

_TOOL_KIND = "tool.durable_request"


@dataclass(frozen=True)
class _Origin:
    origin_type: str
    origin_id: str
    responder_mode: DurableResponderMode
    expected_actor_id: str | None
    requires_reference: bool
    origin_session_id: str | None = None
    origin_ui_session_id: str | None = None
    origin_profile: str | None = None


def check_durable_request_requirements() -> bool:
    """Expose the tool only when an operator configured a valid inbox."""

    return load_durable_requests_config().available


def durable_request_tool(
    *,
    action: Any,
    title: Any = None,
    question: Any = None,
    options: Any = None,
    reference: Any = None,
    expires_in_seconds: Any = None,
    wake_on_terminal: Any = False,
    request_id: Any = None,
) -> str:
    try:
        action_value = _normalize_action(action)
        config = load_durable_requests_config()
        if not config.available or config.target is None:
            return tool_error(
                "durable requests are not configured"
                if not config.reason or config.reason == "disabled"
                else f"durable requests are unavailable: {config.reason}"
            )
        origin = _current_origin(config.scheduled_responder_mode)

        if action_value == "post":
            wake_requested = _normalize_bool(
                wake_on_terminal,
                field="wake_on_terminal",
            )
            normalized_reference = _normalize_optional_text(
                reference,
                field="reference",
                maximum=MAX_DURABLE_REFERENCE_LENGTH,
                single_line=True,
            )
            if origin.requires_reference and normalized_reference is None:
                return tool_error("reference is required for scheduled requests")
            normalized_title = _normalize_text(
                title,
                field="title",
                maximum=MAX_PROMPT_TITLE_LENGTH,
            )
            normalized_question = _normalize_text(
                question,
                field="question",
                maximum=MAX_PROMPT_BODY_LENGTH,
            )
            labels = _normalize_options(options)
            ttl = _normalize_ttl(
                expires_in_seconds,
                default=config.default_ttl_seconds,
                maximum=config.max_ttl_seconds,
            )
            result = create_durable_request(
                DurableRequestSpec(
                    kind=_TOOL_KIND,
                    title=normalized_title,
                    body=normalized_question,
                    options=tuple(
                        DurableRequestOption(
                            id=f"option_{index}",
                            label=label,
                        )
                        for index, label in enumerate(labels, start=1)
                    ),
                    ttl_seconds=ttl,
                    reference=normalized_reference,
                ),
                target=config.target,
                responder_mode=origin.responder_mode,
                expected_actor_id=origin.expected_actor_id,
                origin_type=origin.origin_type,
                origin_id=origin.origin_id,
                command_response_allowed=(
                    config.command_responses_enabled
                    and config.allow_on_native_delivery
                ),
                text_command_fallback_allowed=(
                    config.command_responses_enabled
                    and config.allow_text_delivery_fallback
                ),
                wake_on_terminal=wake_requested,
                origin_session_id=origin.origin_session_id,
                origin_ui_session_id=origin.origin_ui_session_id,
                origin_profile=origin.origin_profile,
                maximum_ttl_seconds=config.max_ttl_seconds,
                terminal_retention_seconds=config.terminal_retention_seconds,
            )
            payload = _serialize_request(result.request)
            payload["created"] = result.created
            return tool_result(payload)

        lookup = _normalize_lookup(request_id=request_id, reference=reference)
        if action_value == "status":
            request = get_durable_request(
                request_id=lookup[0],
                origin_type=origin.origin_type,
                origin_id=origin.origin_id,
                reference=lookup[1],
            )
            if request is None:
                return tool_result(status="not_found")
            return tool_result(_serialize_request(request))

        request = cancel_durable_request(
            origin_type=origin.origin_type,
            origin_id=origin.origin_id,
            request_id=lookup[0],
            reference=lookup[1],
        )
        if request is None:
            return tool_result(status="not_found")
        return tool_result(_serialize_request(request))
    except (DurableRequestError, DurableRequestLimitError, ValueError) as exc:
        return tool_error(str(exc))


def _normalize_action(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("action must be a string")
    action = value.strip().lower()
    if action not in {"post", "status", "cancel"}:
        raise ValueError("action must be post, status, or cancel")
    return action


def _normalize_text(
    value: Any,
    *,
    field: str,
    maximum: int,
    single_line: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise ValueError(f"{field} exceeds the maximum length of {maximum}")
    if single_line and ("\n" in normalized or "\r" in normalized):
        raise ValueError(f"{field} must be a single line")
    return normalized


def _normalize_optional_text(
    value: Any,
    *,
    field: str,
    maximum: int,
    single_line: bool = False,
) -> str | None:
    if value is None:
        return None
    return _normalize_text(
        value,
        field=field,
        maximum=maximum,
        single_line=single_line,
    )


def _normalize_options(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("options must be an array of strings")
    if not MIN_DURABLE_OPTIONS <= len(value) <= MAX_PROMPT_OPTIONS:
        raise ValueError(
            f"options must contain between {MIN_DURABLE_OPTIONS} and "
            f"{MAX_PROMPT_OPTIONS} choices"
        )
    labels: list[str] = []
    normalized_labels: set[str] = set()
    for raw_label in value:
        label = _normalize_text(
            raw_label,
            field="option label",
            maximum=MAX_PROMPT_OPTION_LABEL_LENGTH,
        )
        key = " ".join(label.split()).casefold()
        if key in normalized_labels:
            raise ValueError("options must have unique labels")
        normalized_labels.add(key)
        labels.append(label)
    return tuple(labels)


def _normalize_ttl(value: Any, *, default: float, maximum: float) -> float:
    if value is None:
        return float(default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("expires_in_seconds must be a number")
    ttl = float(value)
    if ttl <= 0:
        raise ValueError("expires_in_seconds must be positive")
    if ttl > maximum:
        raise ValueError(
            f"expires_in_seconds exceeds the configured maximum of {int(maximum)}"
        )
    return ttl


def _normalize_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field} must be a boolean")


def _normalize_lookup(
    *,
    request_id: Any,
    reference: Any,
) -> tuple[str | None, str | None]:
    normalized_id = _normalize_optional_text(
        request_id,
        field="request_id",
        maximum=96,
        single_line=True,
    )
    normalized_reference = _normalize_optional_text(
        reference,
        field="reference",
        maximum=MAX_DURABLE_REFERENCE_LENGTH,
        single_line=True,
    )
    if (normalized_id is None) == (normalized_reference is None):
        raise ValueError("provide exactly one of request_id or reference")
    return normalized_id, normalized_reference


def _current_origin(
    scheduled_responder_mode: DurableResponderMode,
) -> _Origin:
    if get_session_env("HERMES_CRON_SESSION", "") == "1":
        job_id = get_session_env("HERMES_CRON_JOB_ID", "").strip()
        if not job_id:
            raise ValueError("durable requests require a trusted cron job id")
        return _Origin(
            origin_type="cron",
            origin_id=job_id,
            responder_mode=scheduled_responder_mode,
            expected_actor_id=None,
            requires_reference=True,
            origin_profile=(
                get_session_env("HERMES_SESSION_PROFILE", "").strip() or None
            ),
        )

    session_key = get_session_env("HERMES_SESSION_KEY", "").strip()
    actor_id = get_session_env("HERMES_SESSION_USER_ID", "").strip()
    platform = get_session_env("HERMES_SESSION_PLATFORM", "").strip().lower()
    session_id = get_session_env("HERMES_SESSION_ID", "").strip()
    ui_session_id = get_session_env("HERMES_UI_SESSION_ID", "").strip()
    profile = get_session_env("HERMES_SESSION_PROFILE", "").strip()
    if not session_key or not actor_id or platform != "discord":
        raise ValueError(
            "durable requests require an active Discord gateway session or cron job"
        )
    return _Origin(
        origin_type="session",
        origin_id=session_key,
        responder_mode=DurableResponderMode.INITIATOR,
        expected_actor_id=actor_id,
        requires_reference=False,
        origin_session_id=session_id or None,
        origin_ui_session_id=ui_session_id or None,
        origin_profile=profile or None,
    )


def _serialize_request(request: DurableRequest) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "request_id": request.request_id,
        "status": request.status.value,
        "expires_at": _format_timestamp(request.expires_at),
    }
    if request.reference is not None:
        payload["reference"] = request.reference

    if not request.wake_requested:
        payload["wake"] = "not_requested"
    elif request.status in {
        DurableRequestStatus.PENDING_DELIVERY,
        DurableRequestStatus.OPEN,
    }:
        payload["wake"] = "requested"
    elif request.wake_status in {
        DurableWakeStatus.PENDING,
        DurableWakeStatus.CLAIMED,
    }:
        payload["wake"] = "pending"
    else:
        payload["wake"] = request.wake_status.value
    if request.wake_status is DurableWakeStatus.DROPPED and request.wake_reason:
        payload["wake_reason"] = request.wake_reason

    if request.status is DurableRequestStatus.PENDING_DELIVERY:
        payload["status"] = "pending"
        payload["delivery"] = "queued"
    elif request.status is DurableRequestStatus.OPEN:
        payload["status"] = "pending"
        payload["delivery"] = "delivered"
    elif request.status is DurableRequestStatus.SELECTED:
        option = request.selected_option
        if option is None:
            raise ValueError("selected durable request has no stored option")
        index = next(
            index
            for index, candidate in enumerate(request.options, start=1)
            if candidate.id == option.id
        )
        payload.update(
            selected_index=index,
            selected_label=option.label,
            resolved_at=_format_timestamp(request.resolved_at),
        )
    elif request.resolved_at is not None:
        payload["resolved_at"] = _format_timestamp(request.resolved_at)

    if request.terminal_reason:
        payload["reason"] = request.terminal_reason
    return payload


def _format_timestamp(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


DURABLE_REQUEST_SCHEMA = {
    "name": "durable_request",
    "description": (
        "Post, inspect, or cancel a persisted finite-choice request in the "
        "operator-configured Approvals & Requests inbox. Posting returns "
        "immediately and the request can be answered after this turn or a "
        "gateway restart. Scheduled jobs should use a stable reference, then "
        "either poll status or opt into a fresh terminal wake turn. This is "
        "user input, NOT a security "
        "authorization token: never use it to bypass terminal, execution, "
        "slash-command, or application approval controls."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["post", "status", "cancel"],
            },
            "title": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_PROMPT_TITLE_LENGTH,
                "description": "Required for post. Display-safe short heading.",
            },
            "question": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_PROMPT_BODY_LENGTH,
                "description": "Required for post. Pre-redacted decision context.",
            },
            "options": {
                "type": "array",
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_PROMPT_OPTION_LABEL_LENGTH,
                },
                "minItems": MIN_DURABLE_OPTIONS,
                "maxItems": MAX_PROMPT_OPTIONS,
                "description": "Required for post. Complete one-choice option set.",
            },
            "reference": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_DURABLE_REFERENCE_LENGTH,
                "description": (
                    "Origin-scoped idempotency key. Required for scheduled "
                    "requests; accepted for post, status, and cancel."
                ),
            },
            "expires_in_seconds": {
                "type": "number",
                "exclusiveMinimum": 0,
                "description": (
                    "Optional post lifetime, bounded by operator configuration."
                ),
            },
            "wake_on_terminal": {
                "type": "boolean",
                "description": (
                    "Optional for post. When true, a terminal result starts a "
                    "fresh turn for the originating conversation or schedule. "
                    "The result remains user input, not an authorization token."
                ),
            },
            "request_id": {
                "type": "string",
                "description": "Request receipt used by status or cancel.",
            },
        },
        "required": ["action"],
    },
}


registry.register(
    name="durable_request",
    toolset="durable_request",
    schema=DURABLE_REQUEST_SCHEMA,
    handler=lambda args, **_: durable_request_tool(
        action=args.get("action"),
        title=args.get("title"),
        question=args.get("question"),
        options=args.get("options"),
        reference=args.get("reference"),
        expires_in_seconds=args.get("expires_in_seconds"),
        wake_on_terminal=args.get("wake_on_terminal", False),
        request_id=args.get("request_id"),
    ),
    check_fn=check_durable_request_requirements,
    emoji="📥",
)


__all__ = [
    "DURABLE_REQUEST_SCHEMA",
    "check_durable_request_requirements",
    "durable_request_tool",
]
