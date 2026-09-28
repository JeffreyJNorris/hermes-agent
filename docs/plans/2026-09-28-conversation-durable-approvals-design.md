# Conversation-local durable approvals — design

Status: approved for implementation on 2026-09-28.

## Outcome

A durable request created during a live Discord conversation appears in that
conversation's exact channel or thread. Cron-origin requests keep using the
configured durable-request inbox. The existing Approve/Reject controls, command
response path, SQLite authority, and action execution semantics stay unchanged.
Memory approvals show a complete human-readable review instead of raw JSON.

## Routing

The gateway already binds the actual Discord `CHAT_ID`, `THREAD_ID`, actor, and
session key into task-local trusted context. The durable-request origin helper
will capture an optional delivery target from that context. A Discord session
must have a valid numeric physical channel ID; when a thread ID is present, it
must equal that physical channel ID. Missing or inconsistent scope fails closed
instead of falling back to the inbox. The model cannot supply or override any
of these values.

The generic durable-request tool and the two trusted staged-write approval
creators use the origin's target for Discord sessions. Cron origins retain the
configured inbox target. The target is persisted in the existing request row
at creation, so config changes and restart cannot redirect an active request.
Existing rows retain their persisted target and need no migration.

Delivery failure does not create a second presentation in `#approvals`.
Definite pre-send failures follow the existing retry policy; ambiguous sends
remain `delivery_unknown`. The operator/manual-review rail handles an
unavailable originating channel. Native controls still validate the bound
message; commands still require the request receipt, exact physical channel,
actual actor, and open state.

## Review presentation

The memory request body uses deterministic plain text: target file, numbered
operations, and the complete content being added or replacing an entry. For
replace/remove, show the complete pinned entry being changed when available;
never imply a substring-only edit. No option ID, action payload JSON, actor,
session, or provider identifier appears in the review body. Nothing is
truncated. A review too large for the transport stays on manual review.

The generic Discord durable-request view and its buttons remain unchanged.
Skill approvals continue to show their complete deterministic diff; the
predefined paged skill workflow remains Phase 5C work.

## Verification and rollout

Use focused tests for parent channel, existing thread, auto-created thread,
cron inbox, malformed/missing scope, restart persistence, native/command exact
scope, and memory review of add/replace/remove/batch. Keep existing single
request, action, wake, and command regression tests green. No live canary is
approved until the rendered review and bound channel are inspected.

Deploy as an immutable release after tests and review. Start with one harmless
conversation-local memory canary, then one staged skill canary; verify request
and action state in SQLite before and after each interaction. Keep the
scheduled inbox behavior as a separate check. Restore approval feature gates
and remove canary content after acceptance. The GPT-6 Luna default is a
separate live config change and is not part of this code branch.
