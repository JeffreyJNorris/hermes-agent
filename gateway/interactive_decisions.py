"""Typed domain helpers for provider-neutral interactive prompts.

The low-level interactive prompt callback returns canonical option identifiers.
This module keeps those low-level identifiers out of domain code by
mapping each prompt option to an arbitrary typed value.  It also provides a
small confirmation helper for the common two-choice case.

Display text remains caller-controlled.  Titles, bodies, and labels must be
pre-redacted and safe to show to the scoped user.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Generic, TypeVar

from gateway.interactive import (
    InteractivePromptSpec,
    InteractiveMultiSelectSpec,
    MultiSelectOutcome,
    validate_selection_bounds,
    PromptOption,
    PromptOutcome,
    PromptOutcomeStatus,
    PromptStyle,
)

ValueT = TypeVar("ValueT")

SyncPromptCallback = Callable[[InteractivePromptSpec], PromptOutcome]
AsyncPromptCallback = Callable[[InteractivePromptSpec], Awaitable[PromptOutcome]]

_OPTION_ID_PREFIX = "decision"


class DecisionStatus(str, Enum):
    SELECTED = "selected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNSUPPORTED = "unsupported"
    DELIVERY_FAILED = "delivery_failed"


@dataclass(frozen=True)
class DecisionOption(Generic[ValueT]):
    value: ValueT
    label: str
    style: PromptStyle = PromptStyle.SECONDARY


@dataclass(frozen=True)
class DecisionRequest(Generic[ValueT]):
    kind: str
    title: str
    body: str
    options: tuple[DecisionOption[ValueT], ...]
    timeout_seconds: float = 60.0


@dataclass(frozen=True)
class DecisionResult(Generic[ValueT]):
    status: DecisionStatus
    value: ValueT | None = None
    reason: str | None = None


class ConfirmationStatus(str, Enum):
    CONFIRMED = "confirmed"
    DECLINED = "declined"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNSUPPORTED = "unsupported"
    DELIVERY_FAILED = "delivery_failed"


@dataclass(frozen=True)
class ConfirmationRequest:
    kind: str
    title: str
    body: str
    timeout_seconds: float = 60.0
    confirm_label: str = "Confirm"
    decline_label: str = "Cancel"
    destructive: bool = False


@dataclass(frozen=True)
class ConfirmationResult:
    status: ConfirmationStatus
    reason: str | None = None


def request_decision(
    request: DecisionRequest[ValueT],
    *,
    callback: SyncPromptCallback | None = None,
) -> DecisionResult[ValueT]:
    """Synchronously request one typed value from a finite choice set.

    When ``callback`` is omitted, the callback bound to the active gateway turn
    is used.  Calls outside an active gateway request therefore fail explicitly
    instead of falling back to ambient transport state.
    """

    prompt, values_by_option_id = _build_prompt(request)
    prompt_callback = _resolve_sync_callback(callback)
    outcome = prompt_callback(prompt)
    return _map_decision_outcome(outcome, values_by_option_id)


async def request_decision_async(
    request: DecisionRequest[ValueT],
    *,
    callback: AsyncPromptCallback,
) -> DecisionResult[ValueT]:
    """Asynchronously request one typed value using gateway-owned async code."""

    if not callable(callback):
        raise TypeError("interactive prompt callback must be callable")
    prompt, values_by_option_id = _build_prompt(request)
    outcome = await callback(prompt)
    return _map_decision_outcome(outcome, values_by_option_id)


def request_confirmation(
    request: ConfirmationRequest,
    *,
    callback: SyncPromptCallback | None = None,
) -> ConfirmationResult:
    """Synchronously request a typed confirm-or-decline decision."""

    decision = request_decision(
        _confirmation_decision_request(request),
        callback=callback,
    )
    return _map_confirmation_result(decision)


async def request_confirmation_async(
    request: ConfirmationRequest,
    *,
    callback: AsyncPromptCallback,
) -> ConfirmationResult:
    """Asynchronously request a typed confirm-or-decline decision."""

    decision = await request_decision_async(
        _confirmation_decision_request(request),
        callback=callback,
    )
    return _map_confirmation_result(decision)


def _build_prompt(
    request: DecisionRequest[ValueT],
) -> tuple[InteractivePromptSpec, dict[str, ValueT]]:
    if not isinstance(request, DecisionRequest):
        raise TypeError("request must be a DecisionRequest")

    prompt_options: list[PromptOption] = []
    values_by_option_id: dict[str, ValueT] = {}
    for index, option in enumerate(request.options, start=1):
        if not isinstance(option, DecisionOption):
            raise TypeError("request options must be DecisionOption values")
        option_id = f"{_OPTION_ID_PREFIX}_{index}"
        prompt_options.append(
            PromptOption(
                id=option_id,
                label=option.label,
                style=option.style,
            )
        )
        values_by_option_id[option_id] = option.value

    return (
        InteractivePromptSpec(
            kind=request.kind,
            title=request.title,
            body=request.body,
            options=tuple(prompt_options),
            timeout_seconds=request.timeout_seconds,
        ),
        values_by_option_id,
    )


def _resolve_sync_callback(
    callback: SyncPromptCallback | None,
) -> SyncPromptCallback:
    if callback is None:
        from tools.interactive_prompt_context import (
            require_interactive_prompt_callback,
        )

        callback = require_interactive_prompt_callback()
    if not callable(callback):
        raise TypeError("interactive prompt callback must be callable")
    return callback


def _map_decision_outcome(
    outcome: PromptOutcome,
    values_by_option_id: dict[str, ValueT],
) -> DecisionResult[ValueT]:
    if not isinstance(outcome, PromptOutcome):
        raise TypeError("interactive prompt callback must return PromptOutcome")

    status = DecisionStatus(outcome.status.value)
    if outcome.status is not PromptOutcomeStatus.SELECTED:
        return DecisionResult(status=status, reason=outcome.reason)

    assert outcome.response is not None
    try:
        value = values_by_option_id[outcome.response.option_id]
    except KeyError as exc:
        raise RuntimeError(
            "interactive prompt returned an option outside the decision request"
        ) from exc
    return DecisionResult(status=DecisionStatus.SELECTED, value=value)


def _confirmation_decision_request(
    request: ConfirmationRequest,
) -> DecisionRequest[bool]:
    if not isinstance(request, ConfirmationRequest):
        raise TypeError("request must be a ConfirmationRequest")
    if not isinstance(request.destructive, bool):
        raise TypeError("destructive must be a bool")

    confirm_style = (
        PromptStyle.DANGER if request.destructive else PromptStyle.PRIMARY
    )
    return DecisionRequest(
        kind=request.kind,
        title=request.title,
        body=request.body,
        options=(
            DecisionOption(True, request.confirm_label, confirm_style),
            DecisionOption(False, request.decline_label, PromptStyle.SECONDARY),
        ),
        timeout_seconds=request.timeout_seconds,
    )


def _map_confirmation_result(
    decision: DecisionResult[bool],
) -> ConfirmationResult:
    if decision.status is DecisionStatus.SELECTED:
        if decision.value is True:
            return ConfirmationResult(ConfirmationStatus.CONFIRMED)
        if decision.value is False:
            return ConfirmationResult(ConfirmationStatus.DECLINED)
        raise RuntimeError("confirmation returned an invalid selected value")
    return ConfirmationResult(
        status=ConfirmationStatus(decision.status.value),
        reason=decision.reason,
    )


__all__ = [
    "AsyncPromptCallback",
    "ConfirmationRequest",
    "ConfirmationResult",
    "ConfirmationStatus",
    "DecisionOption",
    "DecisionRequest",
    "DecisionResult",
    "DecisionStatus",
    "SyncPromptCallback",
    "request_confirmation",
    "request_confirmation_async",
    "request_decision",
    "request_decision_async",
]


@dataclass(frozen=True)
class MultiDecisionOption(Generic[ValueT]):
    value: ValueT
    label: str
    style: PromptStyle = PromptStyle.SECONDARY


@dataclass(frozen=True)
class MultiDecisionRequest(Generic[ValueT]):
    kind: str
    title: str
    body: str
    options: tuple[MultiDecisionOption[ValueT], ...]
    min_selections: int
    max_selections: int
    timeout_seconds: float = 60.0


@dataclass(frozen=True)
class MultiDecisionResult(Generic[ValueT]):
    status: DecisionStatus
    values: tuple[ValueT, ...] = ()
    reason: str | None = None


def _build_multi_prompt(request: MultiDecisionRequest[ValueT]):
    if not isinstance(request, MultiDecisionRequest):
        raise TypeError("request must be a MultiDecisionRequest")
    validate_selection_bounds(len(request.options), request.min_selections, request.max_selections)
    options = []
    values = {}
    for index, option in enumerate(request.options, start=1):
        if not isinstance(option, MultiDecisionOption):
            raise TypeError("request options must be MultiDecisionOption values")
        option_id = f"decision_{index}"
        options.append(PromptOption(option_id, option.label, option.style))
        values[option_id] = option.value
    return InteractiveMultiSelectSpec(
        kind=request.kind, title=request.title, body=request.body, options=tuple(options),
        min_selections=request.min_selections, max_selections=request.max_selections,
        timeout_seconds=request.timeout_seconds,
    ), values


def _map_multi_outcome(outcome: MultiSelectOutcome, values: dict[str, ValueT],
                       request: MultiDecisionRequest[ValueT]) -> MultiDecisionResult[ValueT]:
    if not isinstance(outcome, MultiSelectOutcome):
        raise TypeError("multi-select callback must return MultiSelectOutcome")
    status = DecisionStatus(outcome.status.value)
    if outcome.status is not PromptOutcomeStatus.SELECTED:
        return MultiDecisionResult(status=status, reason=outcome.reason)
    ids = outcome.response.option_ids
    if (not request.min_selections <= len(ids) <= request.max_selections
        or len(set(ids)) != len(ids) or any(option_id not in values for option_id in ids)):
        raise RuntimeError("multi-select returned an invalid selection")
    selected = set(ids)
    return MultiDecisionResult(status=status, values=tuple(value for key, value in values.items() if key in selected))


def request_multi_decision(request: MultiDecisionRequest[ValueT], *, callback=None) -> MultiDecisionResult[ValueT]:
    prompt, values = _build_multi_prompt(request)
    if callback is None:
        from tools.interactive_prompt_context import require_interactive_multi_select_callback
        callback = require_interactive_multi_select_callback()
    if not callable(callback):
        raise TypeError("multi-select callback must be callable")
    return _map_multi_outcome(callback(prompt), values, request)


async def request_multi_decision_async(request: MultiDecisionRequest[ValueT], *, callback) -> MultiDecisionResult[ValueT]:
    if not callable(callback):
        raise TypeError("multi-select callback must be callable")
    prompt, values = _build_multi_prompt(request)
    return _map_multi_outcome(await callback(prompt), values, request)


__all__ += ["MultiDecisionOption", "MultiDecisionRequest", "MultiDecisionResult",
            "request_multi_decision", "request_multi_decision_async"]
