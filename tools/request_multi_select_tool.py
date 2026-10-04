"""One complete, bounded selection set from the active gateway user."""
from __future__ import annotations

from copy import deepcopy

from gateway.interactive import (
    MAX_MULTI_SELECT_OPTIONS, MAX_PROMPT_TITLE_LENGTH, MAX_PROMPT_BODY_LENGTH,
    MAX_PROMPT_OPTION_LABEL_LENGTH, validate_selection_bounds,
)
from gateway.interactive_decisions import (
    DecisionStatus, MultiDecisionOption, MultiDecisionRequest, request_multi_decision,
)
from tools.request_decision_tool import (
    REQUEST_DECISION_SCHEMA, _normalize_text,
)
from tools.registry import registry, tool_error, tool_result


def request_multi_select_tool(*, title, question, options, min_selections, max_selections) -> str:
    try:
        title = _normalize_text(title, field="title", maximum=MAX_PROMPT_TITLE_LENGTH)
        question = _normalize_text(question, field="question", maximum=MAX_PROMPT_BODY_LENGTH)
        if not isinstance(options, list):
            raise ValueError("options must be an array of strings")
        validate_selection_bounds(len(options), min_selections, max_selections)
        labels = tuple(_normalize_text(label, field="option label", maximum=MAX_PROMPT_OPTION_LABEL_LENGTH) for label in options)
        if len({label.casefold() for label in labels}) != len(labels):
            raise ValueError("options must have unique labels")
    except (ValueError, TypeError) as exc:
        return tool_error(str(exc))
    try:
        result = request_multi_decision(MultiDecisionRequest(
            kind="tool.request_multi_select", title=title, body=question,
            options=tuple(MultiDecisionOption(index, label) for index, label in enumerate(labels, 1)),
            min_selections=min_selections, max_selections=max_selections,
        ))
    except Exception as exc:
        return tool_error(f"multi-select request failed ({type(exc).__name__})")
    if result.status is DecisionStatus.SELECTED:
        return tool_result(status="selected", selected_indices=list(result.values),
                           selected_labels=[labels[index - 1] for index in result.values])
    payload = {"status": result.status.value}
    if result.reason:
        payload["reason"] = result.reason
    return tool_result(payload)


REQUEST_MULTI_SELECT_SCHEMA = deepcopy(REQUEST_DECISION_SCHEMA)
REQUEST_MULTI_SELECT_SCHEMA.update(
    name="request_multi_select",
    description=("Ask the current user to submit one complete set of choices and wait for it. "
                 "Selection bounds are enforced by the gateway. Use only when a finite selection "
                 "is needed before continuing. This grants no application mutation authority "
                 "and cannot bypass security approvals."),
)
_parameters = REQUEST_MULTI_SELECT_SCHEMA["parameters"]
_parameters["properties"]["options"].update(
    maxItems=MAX_MULTI_SELECT_OPTIONS,
    description="The complete set of choices. Submit one bounded set; no partial selection or free-form answer.",
)
for _name in ("min_selections", "max_selections"):
    _parameters["properties"][_name] = {"type": "integer", "minimum": 1, "maximum": MAX_MULTI_SELECT_OPTIONS}
_parameters["required"] += ["min_selections", "max_selections"]

registry.register(
    name="request_multi_select", toolset="multi_select", schema=REQUEST_MULTI_SELECT_SCHEMA,
    handler=lambda args, **_: request_multi_select_tool(**{
        name: args.get(name) for name in ("title", "question", "options", "min_selections", "max_selections")
    }), emoji="☑️",
)
