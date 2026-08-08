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
```

Phase one supports a Discord inbox only. The model cannot choose the platform,
channel, thread, responder policy, or actor scope. Disabling the feature pauses
outbox delivery and restart reconciliation without deleting persisted rows; they
can resume if the same profile is enabled again.

For a request created during a Discord conversation, the initiating actor is
the only permitted responder after the adapter's normal authorization check.
For a scheduled request, `any_authorized` means any user accepted by the
Discord adapter's existing user/role authorization settings. It does not mean
anyone who can see the channel.

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
  "expires_in_seconds": 86400
}
```

Posting is durable before the tool returns. A queued response looks like:

```json
{
  "request_id": "drq_...",
  "status": "pending",
  "delivery": "queued",
  "reference": "weekly-release-2026-W32",
  "expires_at": "2026-08-09T03:00:00Z",
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
  "expires_at": "2026-08-09T03:00:00Z"
}
```

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

## Scheduled workflow pattern

Phase one uses polling rather than attempting to resume an old Python stack:

1. Query a stable occurrence reference.
2. Post when no record exists.
3. Finish while the request is pending.
4. On a later run, inspect the selected or terminal result.
5. Revalidate current application state before performing any mutation.

The result may be observed more than once, so downstream scheduled operations
must be idempotent.

## Security and privacy

A durable request is user input, not an authorization token. It does not
replace terminal approval, execution approval, slash confirmation, or any
application-specific security gate. Trusted application code must independently
revalidate sensitive actions.

Titles, bodies, and labels are stored in `state.db` until terminal retention
prunes the row. Callers must provide pre-redacted, display-safe content. Hermes
disables Discord mention expansion and logs structural fields rather than
request content at ordinary log levels.

Phase one deliberately excludes text fallback, automatic continuation,
multiselect, forms, modals, and arbitrary provider components.
