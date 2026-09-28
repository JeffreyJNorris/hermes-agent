# Durable requests

Status: implemented

Durable requests are persisted, one-shot finite-choice questions for work that
cannot depend on a live agent turn. They are intended for schedules and other
detached workflows where the user may respond after the originating worker,
session, or gateway process has ended.

This subsystem is separate from `gateway.interactive`:

- `request_decision` is conversation-bound, synchronous, and in memory.
- `durable_request` returns immediately and stores authoritative state in the
  active profile's `state.db`.

A Discord message or button view is only a presentation of the database row.
Session reset, adapter disconnect, and gateway shutdown do not cancel a durable
request.

## Configuration

Durable requests are disabled until an operator configures an inbox:

```yaml
durable_requests:
  enabled: true
  inbox:
    platform: discord
    chat_id: "123456789012345678"
    thread_id: null
  default_ttl_seconds: 86400
  max_ttl_seconds: 604800
  terminal_retention_seconds: 604800
  scheduled_responder_policy: any_authorized
  command_responses:
    enabled: false
    allow_on_native_delivery: true
    allow_text_delivery_fallback: true
```

Current delivery support uses a Discord inbox only. The model cannot choose the platform,
channel, thread, responder policy, or actor scope. Disabling the feature pauses
outbox delivery and restart reconciliation without deleting persisted rows; they
can resume if the same profile is enabled again.

For a request created during a Discord conversation, the initiating actor is
the only permitted responder after the adapter's normal authorization check.
For a scheduled request, `any_authorized` means any user accepted by the
Discord adapter's existing user/role authorization settings. It does not mean
anyone who can see the channel.

Explicit request commands are disabled by default. When
`durable_requests.command_responses.enabled` is true, new requests snapshot
whether command responses are allowed; changing the configuration does not
widen an existing request. `allow_on_native_delivery` adds
`/request respond <request-id> <number>` as an alternative to native buttons.
`allow_text_delivery_fallback` permits one command-driven text presentation
only when native delivery returns the structural `unsupported` result. It
never falls back after a failed or ambiguous send. Historical rows migrate as
native delivery with command responses and text fallback disabled.

## Tool contract

The `durable_request` tool is exposed to the Discord and cron presets when the
configured inbox is valid. It supports `post`, `status`, and `cancel`.

Post a request:

```json
{
  "action": "post",
  "title": "Approve weekly release",
  "question": "The release checks passed. What should happen next?",
  "options": ["Approve", "Reject"],
  "reference": "weekly-release-2026-W32",
  "expires_in_seconds": 86400,
  "wake_on_terminal": true
}
```

`wake_on_terminal` is optional and defaults to `false`. Poll-only callers keep
the original behavior. When enabled, the request still returns immediately;
its eventual terminal result is delivered to the trusted origin as a new turn.

Posting is durable before the tool returns. A queued response looks like:

```json
{
  "request_id": "drq_...",
  "status": "pending",
  "delivery": "queued",
  "reference": "weekly-release-2026-W32",
  "expires_at": "2026-08-09T03:00:00Z",
  "wake": "requested",
  "created": true
}
```

A scheduled request requires `reference`. The reference is scoped to the
scheduler-owned cron job ID, so retrying the same job/reference returns the
existing record instead of creating a duplicate message.

Query by receipt or origin-scoped reference:

```json
{"action": "status", "request_id": "drq_..."}
```

```json
{"action": "status", "reference": "weekly-release-2026-W32"}
```

A resolved request returns the one-based option index and canonical label:

```json
{
  "request_id": "drq_...",
  "status": "selected",
  "selected_index": 1,
  "selected_label": "Approve",
  "resolved_at": "2026-08-08T04:31:20Z",
  "expires_at": "2026-08-09T03:00:00Z",
  "wake": "delivered"
}
```

Wake state is intentionally structural:

```text
not_requested
requested
pending
delivered
dropped
```

`dropped` means the result remains persisted but its original session or job
was no longer a safe continuation target. The response may include a bounded
structural `wake_reason`; it never exposes session IDs, provider message IDs,
actors, delivery claims, or channel routing.

`delivered` means the fresh conversation turn or cron execution was accepted
by its runtime. It does not assert that the follow-up agent run completed or
that any downstream operation succeeded.

Cancel by receipt or reference:

```json
{"action": "cancel", "request_id": "drq_..."}
```

Provider message IDs, channel IDs, actor IDs, delivery claims, and canonical
option IDs are not returned to the model.

## Lifecycle and delivery safety

Persisted states are:

```text
pending_delivery -> open -> selected
        |             |      expired
        |             `----> cancelled
        |----> unsupported
        |----> delivery_failed
        `----> delivery_unknown
```

The bridge claims a pending row before provider delivery and binds the exact
provider message ID only after a successful send. Safe failures before the
provider send, such as a disconnected adapter or rate-limited channel lookup,
release the claim for a later retry.

If Hermes cannot know whether the provider created a message, the request
becomes `delivery_unknown`. It is never resent automatically because doing so
could create two valid-looking requests.

On gateway restart, Hermes restores persistent Discord views for the exact
stored message IDs. The database rechecks expiry, platform, channel, message,
option slot, and responder scope in one write transaction. Resolution commits
before any best-effort Discord edit.

## Terminal wake-up

Wake delivery is a second persisted outbox on the same authoritative request
row. Every terminal transition atomically records both the request outcome and,
when requested, `wake_status=pending`. The worker then claims, dispatches, and
acknowledges that wake without changing or reopening the request result.

Terminal outcomes that can wake an origin are:

```text
selected
expired
cancelled
unsupported
delivery_failed
delivery_unknown
```

Pending and claimed wakes are protected from terminal-retention pruning. A
gateway restart recovers stale wake claims and resumes delivery. Disabling the
durable-request feature pauses both provider delivery and wake delivery without
deleting their rows.

### Conversation origins

A live Discord post records the exact physical gateway session that created
it. A terminal result is injected as a fresh internal turn in the original
conversation. Hermes follows verified compression lineage, but it fails closed
after `/new`, reset, an unknown session, or another non-compression session
boundary. A replacement conversation is never selected merely because it now
occupies the same channel.

The injected event includes the request receipt, reference, terminal status,
display-safe question, and selected label or terminal reason. It states that
the result is persisted user input rather than authorization. Provider message,
channel, and responder identifiers are omitted.

Adapter acceptance is the wake acknowledgement boundary. As with the existing
detached-completion rail, a process death after adapter acceptance but before
the SQLite acknowledgement can cause an at-least-once replay. Consequential
follow-up must therefore re-check current state and remain idempotent.

### Scheduled origins

A cron-origin wake starts a new execution for the scheduler-owned job ID. It
does not resume the original Python stack. The continuation uses the job's
current model, toolsets, working directory, profile, and delivery settings,
while removing the schedule's data-collection script and `context_from`
replay. Its prompt combines the current job prompt with the durable terminal
event.

The continuation is schedule-neutral:

- It does not advance `next_run_at`.
- It does not consume a finite dispatch or repeat count.
- It does not re-enable a paused or disabled job.
- It does not alter the completed state of a retained one-shot.
- It receives its own execution-ledger row with source `durable_request`.

Acceptance is idempotent by durable request ID. Hermes records the continuation
execution before executor submission, favoring at-most-once automatic execution
over an unsafe duplicate after an ambiguous crash. A definitely failed submit
removes its unstarted reservation and remains retryable. If Hermes cannot prove
that reservation cleanup succeeded, it does not create a second execution; the
execution ledger remains the audit record. A paused, removed, or non-agent
origin is terminally dropped; the durable result remains queryable.

## Scheduled workflow patterns

Polling remains supported and is the default:

1. Query a stable occurrence reference.
2. Post when no record exists.
3. Finish while the request is pending.
4. On a later run, inspect the selected or terminal result.
5. Revalidate current application state before performing any mutation.

The result may be observed more than once, so downstream scheduled operations
must be idempotent.

For an event-driven schedule, set `wake_on_terminal=true`. The original run can
finish after posting. Hermes later starts one fresh, schedule-neutral
continuation when the request terminates. The continuation must query or trust
only the authoritative request result, revalidate current application state,
and retain all normal approval gates before mutation.

## Trusted durable action tickets

A generic `durable_request` remains persisted user input only. It cannot name
an action, attach a payload, or execute application code, even when an option is
labelled `Approve`. Trusted durable actions are created only inside a domain
tool that owns the fixed action type, version, payload schema, prompt,
destination, responder scope, fingerprint, and idempotency key. No generic
model-facing durable-action tool exists.

The request and its separate `durable_action_tickets` row are committed in one
SQLite transaction before provider delivery. The request owns the human
decision; the ticket owns execution and notification state. The initial pilot
is `cron.resume` version 1 and is disabled by default:

```yaml
durable_approvals:
  enabled: false
  allowed_actions: []
```

To opt in, set `enabled: true` and add `cron.resume` to `allowed_actions`.
This changes only model-initiated resume calls. Explicit CLI, API, and operator
resume paths keep their existing immediate semantics. The existing durable
request inbox is reused.

### Ticket lifecycle and crash boundary

```text
waiting_for_decision
  approve -> ready -> claimed -> executing
                              -> succeeded | already_applied | invalidated
                              -> failed | unknown
  reject  -> declined
  expiry  -> expired
  cancel  -> cancelled
  delivery terminal failure -> abandoned
```

Only the canonical internal `approve` option makes a ticket ready. Display
labels are not authority. Before mutation, the registered in-process handler
reloads current state, rechecks the allowlist and policy, and compares the
stored fingerprint. A removed or changed job becomes `invalidated` without a
mutation. The trusted `cron.resume` preparer emits exactly one inert data field,
`job_id`; it never emits an executable selector, callback, command, Python
import path, or credential. Bounded JSON used by future trusted handlers is
data only and cannot select a handler; the separate, fixed action type and
version select a statically registered implementation.

Execution is at most once by default. A stale `claimed` ticket whose handler
never started may return to `ready`. Hermes records `executing` before entering
application code. A stale execution, an unexpected handler exception, or an
invalid handler return becomes `unknown` and is never replayed automatically.
`failed` is reserved for a handler's bounded, definite failure result. Handler
code runs outside the SQLite transaction, and a terminal outcome is persisted
before notification.

For `unknown`, an operator must inspect the current domain state and structural
ticket outcome before acting. Do not edit the SQLite row or reset it to
`ready`. Reconcile through the normal operator surface; request a new trusted
approval only after the target has a new, unambiguous state/fingerprint.

### Local operator operations

The trusted local `hermes durable` command reads bounded, redacted projections
from the active profile's `state.db`. Operators can list and inspect requests
and actions, safely cancel an active request, run a read-only diagnostic for an
`unknown` action, record one immutable reconciliation, and check structural
health. This command is not registered as a model tool, slash command, ACP
operation, or API-server tool.

An `unknown` action remains `unknown` after reconciliation. The separate
reconciliation classifies the evidence as `confirmed_applied`,
`confirmed_not_applied`, `abandoned`, or `superseded`; it never invokes or
retries the handler. `superseded` links a different existing receipt of the
same action type and does not assert that the newer action executed.

Health, authenticated detailed readiness, and optional OTLP gauges reuse the
same read-only ledger aggregation. Public `/health` remains a shallow liveness
probe and does not open the durable database. Unreconciled UNKNOWN evidence is
critical and is retained beyond normal terminal pruning; after reconciliation,
the request, action, and reconciliation are pruned together only when the
normal age, wake, and notification conditions permit it.

See [Durable operations runbook](../operations/durable-operations.md) for exact
commands, cancellation outcomes, health severity, metrics, retention, and
rollback guidance.

### Completion notification and privacy

An attached action suppresses the ordinary request-terminal wake. Hermes sends
one fresh completion event only after the action is terminal. Rejection,
expiry, cancellation, abandonment, invalidation, failure, unavailable handlers,
and unknown outcomes are described structurally. `unknown` always says the
outcome is ambiguous and must be inspected before retry; it never promises that
no action was applied. Notification failure never changes the action outcome.
Session notifications follow the Phase 4B lineage checks, so `/new` drops them
rather than injecting into a replacement session. Cron notifications use a
schedule-neutral execution with source `durable_action` and the request ID as
its trigger ID.

Model-facing tool responses and formatted completion text expose only bounded
structural fields such as request ID, action type, terminal status, and reason.
They omit payloads, fingerprints, idempotency keys, actor/profile/provider
identifiers, routing, claims, credentials, and raw exceptions. The internal
routing envelope necessarily carries the verified session/profile address, but
still omits action payloads, fingerprints, actor/provider identifiers, claims,
credentials, and raw exceptions. Ordinary structural logs omit all of those
private values.

## Security and privacy

A durable request is user input, not an authorization token. It does not
replace terminal approval, execution approval, slash confirmation, or any
application-specific security gate. Trusted application code must independently
revalidate sensitive actions.

Titles, bodies, and labels are stored in `state.db` until terminal retention
prunes the row. Callers must provide pre-redacted, display-safe content. Hermes
disables Discord mention expansion and logs structural fields rather than
request content at ordinary log levels.

This phase deliberately excludes serialized callbacks, arbitrary continuation
payloads or action selection, text fallback, multiselect, forms, modals, and
arbitrary provider components. Automatic application mutation is limited to
explicitly allowlisted, statically registered trusted handlers.
