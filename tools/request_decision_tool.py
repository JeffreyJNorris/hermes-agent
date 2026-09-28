"""Model-facing finite-choice decision tool.

This is a narrow wrapper over :mod:`gateway.interactive_decisions`.  The
model supplies display-safe text and a complete set of labels; the gateway
continues to own scope, delivery, timeout, replay protection, and resolution.
"""

from __future__ import annotations

from typing import Any

from gateway.interactive import (
    MAX_PROMPT_BODY_LENGTH,
    MAX_PROMPT_OPTION_LABEL_LENGTH,
    MAX_PROMPT_OPTIONS,
    MAX_PROMPT_TITLE_LENGTH,
)
from gateway.interactive_decisions import (
    DecisionOption,
    DecisionRequest,
    DecisionStatus,
    request_decision,
)
from tools.registry import registry, tool_error, tool_result

_TOOL_KIND = "tool.request_decision"
_MIN_OPTIONS = 2


def _normalize_text(value: Any, *, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")

    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if len(normalized) > maximum:
        raise ValueError(f"{field} exceeds the maximum length of {maximum}")
    return normalized


def _normalize_options(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("options must be an array of strings")
    if not _MIN_OPTIONS <= len(value) <= MAX_PROMPT_OPTIONS:
        raise ValueError(
            f"options must contain between {_MIN_OPTIONS} and "
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
        key = label.casefold()
        if key in normalized_labels:
            raise ValueError("options must have unique labels")
        normalized_labels.add(key)
        labels.append(label)

    return tuple(labels)


def request_decision_tool(
    *,
    title: Any,
    question: Any,
    options: Any,
) -> str:
    """Ask the active gateway user to select one option and return JSON."""

    try:
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
    except ValueError as exc:
        return tool_error(str(exc))

    try:
        result = request_decision(
            DecisionRequest(
                kind=_TOOL_KIND,
                title=normalized_title,
                body=normalized_question,
                options=tuple(
                    DecisionOption(value=index, label=label)
                    for index, label in enumerate(labels, start=1)
                ),
            )
        )
    except (TypeError, ValueError, RuntimeError) as exc:
        return tool_error(str(exc))

    if result.status is DecisionStatus.SELECTED:
        selected_index = result.value
        if (
            not isinstance(selected_index, int)
            or isinstance(selected_index, bool)
            or not 1 <= selected_index <= len(labels)
        ):
            return tool_error("interactive decision returned an invalid selection")
        return tool_result(
            status=DecisionStatus.SELECTED.value,
            selected_index=selected_index,
            selected_label=labels[selected_index - 1],
        )

    payload: dict[str, str] = {"status": result.status.value}
    if result.reason:
        payload["reason"] = result.reason
    return tool_result(payload)


REQUEST_DECISION_SCHEMA = {
    "name": "request_decision",
    "description": (
        "Ask the current user to select exactly one option from a complete "
        "finite set, then wait for the result. Use this only when a user "
        "decision is required before continuing. Use clarify for open-ended "
        "questions or multi-select input. Do not use this to bypass terminal, "
        "execution, slash-command, or other security approval flows."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "title": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_PROMPT_TITLE_LENGTH,
                "description": (
                    "A short display heading. Do not include credentials, "
                    "tokens, or other secrets."
                ),
            },
            "question": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_PROMPT_BODY_LENGTH,
                "description": (
                    "The question or decision context. Put selectable answers "
                    "only in options; do not duplicate them in this text."
                ),
            },
            "options": {
                "type": "array",
                "items": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_PROMPT_OPTION_LABEL_LENGTH,
                },
                "minItems": _MIN_OPTIONS,
                "maxItems": MAX_PROMPT_OPTIONS,
                "description": (
                    "The complete set of choices. The user must select exactly "
                    "one; there is no implicit free-form or Other response."
                ),
            },
        },
        "required": ["title", "question", "options"],
    },
}


registry.register(
    name="request_decision",
    toolset="decision",
    schema=REQUEST_DECISION_SCHEMA,
    handler=lambda args, **_: request_decision_tool(
        title=args.get("title"),
        question=args.get("question"),
        options=args.get("options"),
    ),
    emoji="❓",
)


__all__ = [
    "REQUEST_DECISION_SCHEMA",
    "request_decision_tool",
]
