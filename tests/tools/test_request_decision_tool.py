from __future__ import annotations

import json

import pytest

from agent.tool_dispatch_helpers import _NEVER_PARALLEL_TOOLS
from gateway.interactive import (
    MAX_PROMPT_BODY_LENGTH,
    MAX_PROMPT_OPTION_LABEL_LENGTH,
    MAX_PROMPT_OPTIONS,
    MAX_PROMPT_TITLE_LENGTH,
    PromptOutcome,
    PromptOutcomeStatus,
    PromptResponse,
)
from tools.interactive_prompt_context import (
    bind_interactive_prompt_callback,
    reset_interactive_prompt_callback,
)
from tools.registry import registry
from tools.request_decision_tool import (
    REQUEST_DECISION_SCHEMA,
    request_decision_tool,
)
from tools.tool_search import is_deferrable_tool_name
from toolsets import TOOLSETS, _HERMES_CORE_TOOLS, resolve_toolset


def _selected(prompt, index: int) -> PromptOutcome:
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


def test_tool_registers_with_decision_toolset() -> None:
    entry = registry.get_entry("request_decision")

    assert entry is not None
    assert entry.toolset == "decision"
    assert entry.schema is REQUEST_DECISION_SCHEMA
    assert entry.is_async is False


def test_schema_matches_core_prompt_limits() -> None:
    parameters = REQUEST_DECISION_SCHEMA["parameters"]
    properties = parameters["properties"]

    assert parameters["required"] == ["title", "question", "options"]
    assert properties["title"]["maxLength"] == MAX_PROMPT_TITLE_LENGTH
    assert properties["question"]["maxLength"] == MAX_PROMPT_BODY_LENGTH
    assert properties["options"]["minItems"] == 2
    assert properties["options"]["maxItems"] == MAX_PROMPT_OPTIONS
    assert (
        properties["options"]["items"]["maxLength"]
        == MAX_PROMPT_OPTION_LABEL_LENGTH
    )
    assert "timeout" not in properties
    assert "actor" not in properties
    assert "style" not in properties


def test_selected_response_uses_one_based_index_and_full_label() -> None:
    captured = []

    def callback(prompt):
        captured.append(prompt)
        return _selected(prompt, 1)

    token = bind_interactive_prompt_callback(callback)
    try:
        payload = json.loads(
            request_decision_tool(
                title="  Choose environment  ",
                question="  Select the reviewed deployment target.  ",
                options=["  Staging  ", "Production"],
            )
        )
    finally:
        reset_interactive_prompt_callback(token)

    assert payload == {
        "status": "selected",
        "selected_index": 2,
        "selected_label": "Production",
    }
    assert len(captured) == 1
    prompt = captured[0]
    assert prompt.kind == "tool.request_decision"
    assert prompt.title == "Choose environment"
    assert prompt.body == "Select the reviewed deployment target."
    assert [option.label for option in prompt.options] == ["Staging", "Production"]
    assert [option.id for option in prompt.options] == ["decision_1", "decision_2"]
    labels = ["Staging", "Production"]
    assert all(
        label not in option.id
        for label, option in zip(labels, prompt.options)
    )


def test_registry_dispatch_invokes_the_registered_handler() -> None:
    token = bind_interactive_prompt_callback(lambda prompt: _selected(prompt, 0))
    try:
        payload = json.loads(
            registry.dispatch(
                "request_decision",
                {
                    "title": "Choose",
                    "question": "Choose one.",
                    "options": ["First", "Second"],
                },
            )
        )
    finally:
        reset_interactive_prompt_callback(token)

    assert payload == {
        "status": "selected",
        "selected_index": 1,
        "selected_label": "First",
    }


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (PromptOutcomeStatus.EXPIRED, "deadline_elapsed"),
        (PromptOutcomeStatus.CANCELLED, "session_cleared"),
        (PromptOutcomeStatus.UNSUPPORTED, "text_fallback_busy"),
        (PromptOutcomeStatus.DELIVERY_FAILED, "missing_message_id"),
    ],
)
def test_terminal_outcomes_are_normal_tool_results(
    status: PromptOutcomeStatus,
    reason: str,
) -> None:
    token = bind_interactive_prompt_callback(
        lambda _prompt: PromptOutcome(status, reason=reason)
    )
    try:
        payload = json.loads(
            request_decision_tool(
                title="Choose",
                question="Choose one.",
                options=["First", "Second"],
            )
        )
    finally:
        reset_interactive_prompt_callback(token)

    assert payload == {"status": status.value, "reason": reason}
    assert "error" not in payload


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"title": ""}, "title must not be empty"),
        ({"title": 42}, "title must be a string"),
        ({"question": "   "}, "question must not be empty"),
        ({"question": None}, "question must be a string"),
        ({"options": "First, Second"}, "options must be an array of strings"),
        ({"options": ["Only one"]}, "options must contain between 2 and 5 choices"),
        (
            {"options": [str(index) for index in range(MAX_PROMPT_OPTIONS + 1)]},
            "options must contain between 2 and 5 choices",
        ),
        ({"options": ["First", ""]}, "option label must not be empty"),
        ({"options": ["First", 2]}, "option label must be a string"),
        ({"options": [" First ", "first"]}, "options must have unique labels"),
        (
            {"title": "x" * (MAX_PROMPT_TITLE_LENGTH + 1)},
            f"title exceeds the maximum length of {MAX_PROMPT_TITLE_LENGTH}",
        ),
        (
            {"question": "x" * (MAX_PROMPT_BODY_LENGTH + 1)},
            f"question exceeds the maximum length of {MAX_PROMPT_BODY_LENGTH}",
        ),
        (
            {
                "options": [
                    "First",
                    "x" * (MAX_PROMPT_OPTION_LABEL_LENGTH + 1),
                ]
            },
            (
                "option label exceeds the maximum length of "
                f"{MAX_PROMPT_OPTION_LABEL_LENGTH}"
            ),
        ),
    ],
)
def test_invalid_arguments_return_tool_error(
    overrides: dict[str, object],
    message: str,
) -> None:
    arguments: dict[str, object] = {
        "title": "Choose",
        "question": "Choose one.",
        "options": ["First", "Second"],
    }
    arguments.update(overrides)

    payload = json.loads(request_decision_tool(**arguments))

    assert payload == {"error": message}


def test_missing_gateway_context_returns_tool_error() -> None:
    payload = json.loads(
        request_decision_tool(
            title="Choose",
            question="Choose one.",
            options=["First", "Second"],
        )
    )

    assert payload == {
        "error": (
            "interactive prompts are unavailable outside an active gateway request"
        )
    }


def test_selected_response_does_not_expose_transport_or_registry_identifiers() -> None:
    token = bind_interactive_prompt_callback(lambda prompt: _selected(prompt, 0))
    try:
        payload = json.loads(
            request_decision_tool(
                title="Choose",
                question="Choose one.",
                options=["First", "Second"],
            )
        )
    finally:
        reset_interactive_prompt_callback(token)

    assert set(payload) == {"status", "selected_index", "selected_label"}
    serialized = json.dumps(payload)
    for forbidden in (
        "prompt_id",
        "message_id",
        "platform",
        "channel_id",
        "actor_id",
        "decision_1",
    ):
        assert forbidden not in serialized


def test_tool_is_exposed_only_by_the_discord_platform_preset() -> None:
    assert TOOLSETS["decision"]["tools"] == ["request_decision"]
    assert "request_decision" in resolve_toolset("hermes-discord")
    assert "request_decision" not in _HERMES_CORE_TOOLS

    for preset in (
        "hermes-cli",
        "hermes-cron",
        "hermes-telegram",
        "hermes-whatsapp",
        "hermes-slack",
        "hermes-acp",
        "hermes-api-server",
    ):
        assert "request_decision" not in resolve_toolset(preset), preset


def test_tool_is_never_parallelized() -> None:
    assert "request_decision" in _NEVER_PARALLEL_TOOLS


def test_tool_stays_visible_when_progressive_tool_search_is_enabled() -> None:
    assert not is_deferrable_tool_name("request_decision")


def test_model_schema_keeps_decision_tool_eager(monkeypatch) -> None:
    from model_tools import _compute_tool_definitions
    from tools.tool_search import ToolSearchConfig

    monkeypatch.setattr(
        "tools.tool_search.load_config",
        lambda: ToolSearchConfig(
            enabled="on",
            threshold_pct=5.0,
            search_default_limit=5,
            max_search_limit=20,
        ),
    )

    definitions = _compute_tool_definitions(
        enabled_toolsets=["decision"],
        quiet_mode=True,
    )
    names = {definition["function"]["name"] for definition in definitions}

    assert "request_decision" in names
    assert "tool_call" not in names
