# Interactive decisions

Status: implemented

Hermes interactive prompts are a low-level, provider-neutral primitive. The
registry owns prompt identity, scope validation, deadlines, replay handling,
and terminal resolution. Adapters only render prompts and relay interaction
metadata.

Most domain code should use the typed helpers in
`gateway.interactive_decisions` rather than constructing option identifiers or
interpreting `PromptOutcome` directly.

## Typed finite choices

```python
from dataclasses import dataclass

from gateway.interactive_decisions import (
    DecisionOption,
    DecisionRequest,
    DecisionStatus,
    request_decision,
)


@dataclass(frozen=True)
class Environment:
    name: str


result = request_decision(
    DecisionRequest(
        kind="deployment.environment",
        title="Choose an environment",
        body="Select the reviewed deployment target.",
        options=(
            DecisionOption(Environment("staging"), "Staging"),
            DecisionOption(Environment("production"), "Production"),
        ),
        timeout_seconds=120,
    )
)

if result.status is DecisionStatus.SELECTED:
    deploy(result.value)
```

The helper generates internal option identifiers and maps the selected option
back to the caller's original value. Domain code does not need to compare raw
option strings or inspect provider message metadata.

## Confirmations

```python
from gateway.interactive_decisions import (
    ConfirmationRequest,
    ConfirmationStatus,
    request_confirmation,
)

result = request_confirmation(
    ConfirmationRequest(
        kind="workspace.delete",
        title="Delete workspace?",
        body="This permanently removes the selected workspace.",
        confirm_label="Delete",
        decline_label="Keep workspace",
        destructive=True,
    )
)

if result.status is ConfirmationStatus.CONFIRMED:
    delete_workspace()
```

A declined confirmation is distinct from expiry, cancellation, unsupported
transport, and delivery failure. `destructive=True` changes only the visual
style of the affirmative option; trusted actor and delivery scope continue to come
from the gateway.

## Model-facing finite choices

The Discord agent exposes the provider-neutral primitive through the
`request_decision` tool. The model supplies only display text and the complete
set of two to five labels:

```json
{
  "title": "Choose deployment target",
  "question": "Which reviewed environment should I deploy to?",
  "options": ["Staging", "Production", "Cancel"]
}
```

The tool fixes the prompt kind and timeout and does not accept actor, platform,
channel, message, option identifier, style, or delivery-mode fields. The
gateway derives all trusted scope from the active request.

A selection returns a one-based index and the full canonical label:

```json
{
  "status": "selected",
  "selected_index": 2,
  "selected_label": "Production"
}
```

Expiry, cancellation, unsupported delivery, and delivery failure are normal
status results. Invalid arguments or use outside an active gateway request use
the standard `{"error": "..."}` tool envelope. Prompt, message, channel, actor,
and internal option identifiers are never returned.

Use `clarify` instead when the answer may be open-ended, outside the offered
choices, or multi-select. `request_decision` is not an alternative to terminal,
execution, slash-command, or other security approval flows.

## Callback availability

The synchronous helpers use the callback bound to the active gateway turn.
They raise `RuntimeError` outside that context. Tests and other explicit hosts
may inject a callback with the `callback=` argument.

Gateway-owned asynchronous code can use the async variants with
`InteractivePromptBridge.request_async`:

```python
result = await request_confirmation_async(
    request,
    callback=bridge.request_async,
)
```

Do not call the synchronous callback from the gateway event-loop thread.

## Boundaries

The typed layer does not mutate application state. Callers apply a selected
value only after receiving a terminal result. Existing execution approval,
clarify, slash confirmation, model picker, and choice picker implementations
remain independent.

Titles, bodies, and labels are caller-controlled display content. They must be
pre-redacted and safe to show to the scoped user.

## Durable requests

For questions that must outlive the current turn or gateway process, use the
separate persisted request subsystem described in
[`durable-requests.md`](durable-requests.md). Do not extend the in-memory prompt
registry with long-lived waiters.
