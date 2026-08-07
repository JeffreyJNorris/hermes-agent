from __future__ import annotations

from dataclasses import dataclass

import pytest

from gateway.interactive import (
    PromptOutcome,
    PromptOutcomeStatus,
    PromptResponse,
    PromptStyle,
)
from gateway.interactive_decisions import (
    ConfirmationRequest,
    ConfirmationStatus,
    DecisionOption,
    DecisionRequest,
    DecisionResult,
    DecisionStatus,
    request_confirmation,
    request_confirmation_async,
    request_decision,
    request_decision_async,
)
from tools.interactive_prompt_context import (
    bind_interactive_prompt_callback,
    reset_interactive_prompt_callback,
)


def _selected(prompt, index: int = 0) -> PromptOutcome:
    return PromptOutcome(
        PromptOutcomeStatus.SELECTED,
        response=PromptResponse(
            prompt_id="registered-prompt",
            option_id=prompt.options[index].id,
            platform="discord",
            chat_id="chat",
            thread_id=None,
            message_id="message",
            actor_id="actor",
        ),
    )


@dataclass(frozen=True)
class _Target:
    name: str


def test_request_decision_maps_internal_option_to_typed_value() -> None:
    staging = _Target("staging")
    production = _Target("production")
    captured = []

    def callback(prompt):
        captured.append(prompt)
        return _selected(prompt, 1)

    result = request_decision(
        DecisionRequest(
            kind="deployment.environment",
            title="Choose environment",
            body="Select the reviewed target.",
            options=(
                DecisionOption(staging, "Staging"),
                DecisionOption(production, "Production", PromptStyle.DANGER),
            ),
            timeout_seconds=90,
        ),
        callback=callback,
    )

    assert result == DecisionResult(
        status=DecisionStatus.SELECTED,
        value=production,
    )
    assert len(captured) == 1
    prompt = captured[0]
    assert prompt.kind == "deployment.environment"
    assert prompt.timeout_seconds == 90
    assert [option.label for option in prompt.options] == ["Staging", "Production"]
    assert prompt.options[1].style is PromptStyle.DANGER
    assert len({option.id for option in prompt.options}) == 2
    assert all("staging" not in option.id for option in prompt.options)
    assert all("production" not in option.id for option in prompt.options)


@pytest.mark.parametrize(
    ("outcome_status", "expected_status"),
    [
        (PromptOutcomeStatus.EXPIRED, DecisionStatus.EXPIRED),
        (PromptOutcomeStatus.CANCELLED, DecisionStatus.CANCELLED),
        (PromptOutcomeStatus.UNSUPPORTED, DecisionStatus.UNSUPPORTED),
        (PromptOutcomeStatus.DELIVERY_FAILED, DecisionStatus.DELIVERY_FAILED),
    ],
)
def test_request_decision_preserves_terminal_status_and_reason(
    outcome_status: PromptOutcomeStatus,
    expected_status: DecisionStatus,
) -> None:
    result = request_decision(
        DecisionRequest(
            kind="test.decision",
            title="Choose",
            body="Choose one.",
            options=(DecisionOption("yes", "Yes"),),
        ),
        callback=lambda _prompt: PromptOutcome(
            outcome_status,
            reason="terminal_reason",
        ),
    )

    assert result.status is expected_status
    assert result.value is None
    assert result.reason == "terminal_reason"


def test_request_decision_rejects_response_outside_request() -> None:
    def callback(_prompt):
        return PromptOutcome(
            PromptOutcomeStatus.SELECTED,
            response=PromptResponse(
                prompt_id="registered-prompt",
                option_id="forged-option",
                platform="discord",
                chat_id="chat",
                thread_id=None,
                message_id="message",
                actor_id="actor",
            ),
        )

    with pytest.raises(RuntimeError, match="outside the decision request"):
        request_decision(
            DecisionRequest(
                kind="test.decision",
                title="Choose",
                body="Choose one.",
                options=(DecisionOption("yes", "Yes"),),
            ),
            callback=callback,
        )


def test_request_decision_rejects_invalid_callback_result() -> None:
    with pytest.raises(TypeError, match="return PromptOutcome"):
        request_decision(
            DecisionRequest(
                kind="test.decision",
                title="Choose",
                body="Choose one.",
                options=(DecisionOption("yes", "Yes"),),
            ),
            callback=lambda _prompt: object(),
        )


def test_request_decision_uses_active_gateway_callback() -> None:
    seen = []

    def callback(prompt):
        seen.append(prompt)
        return _selected(prompt)

    token = bind_interactive_prompt_callback(callback)
    try:
        result = request_decision(
            DecisionRequest(
                kind="test.context",
                title="Choose",
                body="Choose one.",
                options=(DecisionOption(42, "Forty two"),),
            )
        )
    finally:
        reset_interactive_prompt_callback(token)

    assert result.status is DecisionStatus.SELECTED
    assert result.value == 42
    assert len(seen) == 1


def test_request_decision_requires_gateway_context_without_callback() -> None:
    with pytest.raises(RuntimeError, match="outside an active gateway request"):
        request_decision(
            DecisionRequest(
                kind="test.context",
                title="Choose",
                body="Choose one.",
                options=(DecisionOption(42, "Forty two"),),
            )
        )


def test_request_confirmation_maps_confirm_and_styles_destructive_action() -> None:
    captured = []

    def callback(prompt):
        captured.append(prompt)
        return _selected(prompt, 0)

    result = request_confirmation(
        ConfirmationRequest(
            kind="workspace.delete",
            title="Delete workspace?",
            body="This action cannot be undone.",
            confirm_label="Delete",
            decline_label="Keep workspace",
            destructive=True,
        ),
        callback=callback,
    )

    assert result.status is ConfirmationStatus.CONFIRMED
    assert result.reason is None
    assert captured[0].options[0].label == "Delete"
    assert captured[0].options[0].style is PromptStyle.DANGER
    assert captured[0].options[1].style is PromptStyle.SECONDARY


def test_request_confirmation_requires_boolean_destructive_flag() -> None:
    request = ConfirmationRequest(
        kind="test.confirm",
        title="Continue?",
        body="Choose whether to continue.",
        destructive="yes",  # type: ignore[arg-type]
    )

    with pytest.raises(TypeError, match="destructive must be a bool"):
        request_confirmation(
            request,
            callback=lambda prompt: _selected(prompt),
        )


def test_request_confirmation_maps_explicit_decline() -> None:
    result = request_confirmation(
        ConfirmationRequest(
            kind="test.confirm",
            title="Continue?",
            body="Choose whether to continue.",
        ),
        callback=lambda prompt: _selected(prompt, 1),
    )

    assert result.status is ConfirmationStatus.DECLINED


@pytest.mark.parametrize(
    ("outcome_status", "expected_status"),
    [
        (PromptOutcomeStatus.EXPIRED, ConfirmationStatus.EXPIRED),
        (PromptOutcomeStatus.CANCELLED, ConfirmationStatus.CANCELLED),
        (PromptOutcomeStatus.UNSUPPORTED, ConfirmationStatus.UNSUPPORTED),
        (PromptOutcomeStatus.DELIVERY_FAILED, ConfirmationStatus.DELIVERY_FAILED),
    ],
)
def test_request_confirmation_preserves_non_selection_status(
    outcome_status: PromptOutcomeStatus,
    expected_status: ConfirmationStatus,
) -> None:
    result = request_confirmation(
        ConfirmationRequest(
            kind="test.confirm",
            title="Continue?",
            body="Choose whether to continue.",
        ),
        callback=lambda _prompt: PromptOutcome(
            outcome_status,
            reason="terminal_reason",
        ),
    )

    assert result.status is expected_status
    assert result.reason == "terminal_reason"


@pytest.mark.asyncio
async def test_request_decision_async_maps_typed_value() -> None:
    async def callback(prompt):
        return _selected(prompt, 1)

    result = await request_decision_async(
        DecisionRequest(
            kind="test.async",
            title="Choose",
            body="Choose one.",
            options=(
                DecisionOption("first", "First"),
                DecisionOption("second", "Second"),
            ),
        ),
        callback=callback,
    )

    assert result.status is DecisionStatus.SELECTED
    assert result.value == "second"


@pytest.mark.asyncio
async def test_request_confirmation_async_maps_decline() -> None:
    async def callback(prompt):
        return _selected(prompt, 1)

    result = await request_confirmation_async(
        ConfirmationRequest(
            kind="test.async.confirm",
            title="Continue?",
            body="Choose whether to continue.",
        ),
        callback=callback,
    )

    assert result.status is ConfirmationStatus.DECLINED
