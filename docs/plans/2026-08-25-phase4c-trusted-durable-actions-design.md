# Phase 4C Trusted Durable Actions Design

## Goal

Allow trusted Hermes application code to attach one narrowly defined,
resumable action to a durable binary approval. A human decision can make that
action eligible to run, but the action executes only after trusted code reloads
and revalidates current state.

Generic durable requests remain persisted user input and never become an
authorization mechanism.

## Scope

The first and only Phase 4C handler is `cron.resume` version 1. It is disabled
by default and requires an explicit allowlist entry. No generic model-facing
durable-action tool is added.

When durable approvals are enabled and `cron.resume` is allowlisted, every
model-initiated `cronjob(action="resume")` uses durable approval. Explicit
operator CLI, API, dashboard, and console resume paths remain immediate. When
the feature is disabled, model resume keeps its legacy immediate behavior. An
enabled feature without the `cron.resume` allowlist entry fails closed.

## Architecture

The profile's existing `state.db` gains a separate
`durable_action_tickets` table keyed by durable request ID. Trusted application
code creates the durable request and ticket in one SQLite transaction through
the existing durable-request store. The generic `durable_request` tool cannot
attach a ticket.

The ticket stores a fixed action type and version, bounded canonical JSON,
fingerprint, idempotency key, execution state, bounded outcome, and completion
notification state. It never stores commands, prompts as executable content,
callbacks, Python import paths, credentials, or provider identifiers.

A static in-process registry maps `(action_type, action_version)` to trusted
prepare and execute functions. Unknown handlers and versions fail closed.

A separate profile-scoped runner claims ready tickets, executes registered
handlers outside database transactions, persists the outcome, and delivers a
fresh completion notification through the existing Phase 4B origin-routing
machinery.

## Persistence and state transitions

The action lifecycle is:

```
waiting_for_decision
  approve          -> ready
  reject           -> declined
  expiry           -> expired
  cancellation     -> cancelled
  delivery failure -> abandoned

ready -> claimed -> executing

executing -> succeeded | already_applied | invalidated | failed | unknown
```

Handler lookup failure produces `handler_unavailable` without invoking
application code.

Request resolution and the corresponding ticket transition happen atomically.
Only the canonical server-side option ID `approve` makes a ticket ready; labels
are presentation only. Every other terminal request outcome makes the ticket
permanently non-runnable.

The table has a unique `(action_type, idempotency_key)` constraint. Repeating
the same trusted action returns its existing ticket, including a terminal
ticket. Rejecting, expiring, cancelling, invalidating, or failing an unchanged
action does not silently create another approval. A materially changed action
has a new fingerprint and idempotency key and may require fresh approval.

Request pruning cannot remove a request while its ticket or completion
notification remains active. Ticket and request retention are reconciled
together. Existing Phase 4A/4B rows gain no tickets and retain
`wake_requested=0`/`wake_status=not_requested` behavior.

## Approval and notification behavior

Trusted approvals are binary and use fixed internal option IDs `approve` and
`reject`. Trusted callers may choose display labels, but neither labels nor the
model select the action handler.

Ticketed requests suppress the ordinary Phase 4B terminal wake. Rejection,
expiry, cancellation, abandonment, or a completed execution creates exactly
one action-completion notification. Approval does not notify until the trusted
action itself reaches a terminal state.

Completion routing inherits Phase 4B guarantees:

- a live-conversation notification follows verified session lineage;
- `/new` or deletion drops it instead of entering a replacement session;
- cron delivery is a schedule-neutral fresh execution;
- notification failure never changes the action outcome.

The model-facing event includes only request ID, action type, canonical
decision, terminal action status, and a bounded structural reason or result.
It excludes payload JSON, fingerprints, physical or logical session IDs, cron
job IDs, profile names, Discord/provider identifiers, actors, claims,
credentials, and raw exceptions.

## Runner and crash recovery

The runner atomically claims one ready ticket and records `claimed`. It then
records `executing` before entering trusted application code. Handler execution
runs without a SQLite transaction or lock held.

A stale `claimed` ticket whose execution never started can safely return to
`ready`. A stale `executing` ticket becomes `unknown` and is never retried
automatically. This provides at-most-once automatic execution and fails closed
across the ambiguous crash window.

Repeated scans, resolver replays, duplicate Discord interactions, and restarts
cannot create a second execution. Persisted successful outcomes are immutable.

## `cron.resume` version 1

The persisted payload contains only the canonical job ID. Preparation loads
the current job and computes a canonical fingerprint over the job definition,
including schedule, repeat settings, prompt/script/no-agent behavior, model and
provider settings, work directory, toolsets, delivery/origin configuration,
and other fields that affect what resumes.

The paused/enabled runtime state is evaluated separately from the definition
fingerprint:

- unchanged definition and still paused: resume through the existing scheduler
  path and return `succeeded`;
- unchanged definition and already resumed: return `already_applied`;
- changed definition, removed job, or revoked permission: return `invalidated`
  without mutation.

Reloading, fingerprint comparison, and the resume mutation occur under the
existing jobs lock to avoid a check-then-mutate race. The existing scheduler
notification path is reused after a successful update.

The idempotency key is derived from `cron.resume`, the canonical job ID, and
the definition fingerprint. The fingerprint is stored as a digest; secret
material is neither included nor logged.

## Configuration

The default configuration is:

```yaml
durable_approvals:
  enabled: false
  allowed_actions: []
```

The pilot also requires a valid enabled durable-request inbox. Enabling durable
approvals does not create another Discord channel or delivery mechanism.

## Validation

Implementation follows test-driven development using `scripts/run_tests.sh`.
Release-blocking coverage includes:

- atomic request/ticket creation and every coupled terminal transition;
- generic durable-request isolation and canonical approval IDs;
- idempotency, claim concurrency, pruning, schema upgrade, and payload bounds;
- duplicate/unknown handler and version failures and disabled-action behavior;
- unchanged, already-applied, changed, removed, and unauthorized cron jobs;
- stale claim recovery, ambiguous execution to `unknown`, and no automatic
  retry;
- one completion event, session-reset isolation, cron schedule neutrality, and
  notification failure independence;
- privacy boundaries and independence from execution/terminal approvals;
- existing Phase 4A, Phase 4B, Discord control, cron scheduler, and execution
  ledger regressions.

No changes are planned for the Discord adapter, interactive prompt base,
`request_decision`, the generic `durable_request` tool, or message tools.
