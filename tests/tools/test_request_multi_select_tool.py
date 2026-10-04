from __future__ import annotations

import json

import pytest

from gateway.interactive import MultiSelectOutcome, MultiSelectResponse, PromptOutcomeStatus
from tools.interactive_prompt_context import (
    bind_interactive_multi_select_callback, reset_interactive_multi_select_callback,
    get_interactive_multi_select_callback,
)
from tools.request_multi_select_tool import request_multi_select_tool


def ask(**changes):
    args = dict(title="Choose", question="Which utilities?", options=["Electric", "Gas", "Water"],
                min_selections=1, max_selections=2)
    args.update(changes)
    return json.loads(request_multi_select_tool(**args))


def test_context_scoped_result_is_canonical_indices_and_labels():
    captured = []
    def callback(spec):
        captured.append(spec)
        return MultiSelectOutcome(PromptOutcomeStatus.SELECTED, MultiSelectResponse(
            prompt_id="private-token", option_ids=("decision_3", "decision_1"), platform="discord",
            chat_id="chat", thread_id=None, message_id="message", actor_id="actor",
        ))
    token = bind_interactive_multi_select_callback(callback)
    try:
        result = ask()
    finally:
        reset_interactive_multi_select_callback(token)
    assert result == dict(status="selected", selected_indices=[1, 3], selected_labels=["Electric", "Water"])
    assert [option.id for option in captured[0].options] == ["decision_1", "decision_2", "decision_3"]
    assert get_interactive_multi_select_callback() is None
    assert "error" in ask()


@pytest.mark.parametrize("changes", [dict(options=["Only"]), dict(options=["A", "a"]),
    dict(options=["A", ""]), dict(options=["A", 2]), dict(options="A,B"),
    dict(min_selections=True), dict(max_selections=0), dict(min_selections=3),
    dict(max_selections=4), dict(title=""), dict(question="")])
def test_invalid_input_does_not_invoke_callback(changes):
    def callback(spec):
        raise AssertionError("invalid input reached gateway")
    token = bind_interactive_multi_select_callback(callback)
    try:
        assert "error" in ask(**changes)
    finally:
        reset_interactive_multi_select_callback(token)


@pytest.mark.parametrize("status", ["expired", "cancelled", "unsupported", "delivery_failed"])
def test_normal_lifecycle_status(status):
    token = bind_interactive_multi_select_callback(lambda spec: MultiSelectOutcome(PromptOutcomeStatus(status), reason="bounded_reason"))
    try:
        assert ask() == dict(status=status, reason="bounded_reason")
    finally:
        reset_interactive_multi_select_callback(token)


def test_callback_error_does_not_return_private_exception_text():
    def callback(spec):
        raise RuntimeError("private selection or provider payload")
    token = bind_interactive_multi_select_callback(callback)
    try:
        result = ask()
    finally:
        reset_interactive_multi_select_callback(token)
    assert "error" in result and "private" not in str(result)
    assert "RuntimeError" in str(result)
