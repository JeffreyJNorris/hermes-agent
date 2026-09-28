# Durable operations runbook

Use `hermes durable` on the gateway host to inspect and safely manage the
active profile's durable request and trusted-action ledgers. These are local
operator commands. They are not model tools, slash commands, or remote API
operations.

SQLite remains authoritative. Do not edit `state.db` directly, reset an action
to `ready`, or invoke an action handler while repairing a row.

## Inspect requests and actions

List requests, newest first:

```bash
hermes durable requests list
hermes durable requests list --status open --origin-type session
hermes durable requests list --limit 25 --offset 25 --json
```

List actions:

```bash
hermes durable actions list
hermes durable actions list --status unknown
hermes durable actions list --action-type cron.resume --minimum-age-seconds 300
```

Lists default to 50 rows and accept 1 through 100. Pagination is stable by
creation time and receipt. `next_offset` in JSON identifies the next page when
one exists.

Inspect one receipt:

```bash
hermes durable requests inspect drq_RECEIPT
hermes durable actions inspect drq_RECEIPT
hermes durable actions inspect drq_RECEIPT --diagnose --json
```

`--diagnose` is read-only. It runs only after the ledger read has closed and
only for an `unknown` action with a registered diagnostic callback. The current
`cron.resume` diagnostic reports bounded structural facts about whether the job
exists, whether its state and definition match, and whether execution-ledger
evidence exists. It does not run the action or expose the job prompt or command.
Unknown action types, missing diagnostics, and diagnostic failures return a
structural diagnostic code.

Request and action output intentionally omits prompt bodies and options,
payload and result JSON, fingerprints, idempotency keys, claim tokens, provider
message/channel/thread identifiers, origin/session/profile identifiers, actor
identifiers, credentials, and raw exceptions. Reconciliation notes and local
operator identity are stored as evidence but are not emitted by list, inspect,
health, metrics, or ordinary logs.

## Cancel safely

Cancellation defaults to no in an interactive terminal. Supply `--yes` for an
intentional non-interactive or JSON operation:

```bash
hermes durable requests cancel drq_RECEIPT
hermes durable requests cancel drq_RECEIPT --yes --json
```

The request command also handles action-backed receipts:

| Current record | Result |
|---|---|
| Generic `pending_delivery` or `open` request | Request becomes `cancelled` |
| Action `waiting_for_decision` | Request and action become `cancelled` in one transaction |
| Unclaimed action `ready` | Action becomes `cancelled` and its completion notification becomes pending |
| Claimed `ready`, `claimed`, or `executing` action | Rejected as execution in progress |
| `unknown` or any terminal request/action | Rejected as already terminal |
| Concurrent runner claim | Compare-and-set loses safely and reports a conflict |

Cancellation never calls an action handler. An action cancellation is persisted
before its completion notification is dispatched.

## Reconcile an UNKNOWN action

`unknown` means Hermes crossed the execution boundary but cannot prove whether
the application mutation completed. It does not mean the action failed or was
not applied. Never automatically retry an `unknown` action.

First inspect the ledger and current application state:

```bash
hermes durable actions inspect drq_RECEIPT --diagnose
```

Then record exactly one operator conclusion:

```bash
hermes durable actions reconcile drq_RECEIPT \
  --outcome confirmed_applied \
  --note "verified current job state" \
  --yes
```

Available outcomes are:

| Outcome | Meaning |
|---|---|
| `confirmed_applied` | External evidence confirms the intended mutation took effect |
| `confirmed_not_applied` | External evidence confirms it did not take effect |
| `abandoned` | The operator is closing the ambiguity without claiming applied or not applied |
| `superseded` | A different receipt of the same action type now represents the follow-up work |

`superseded` requires the other receipt explicitly:

```bash
hermes durable actions reconcile drq_OLD \
  --outcome superseded \
  --superseded-by drq_NEW \
  --yes
```

The superseding receipt must exist, differ from the original, and have the same
action type. This outcome does not claim that the newer action ran.

Reconciliation is append-only and immutable: only `unknown` actions qualify,
and concurrent operators produce one winner. The action itself remains
`unknown`; subsequent inspection exposes its `effective_reconciliation`.
Reconciliation never calls, retries, or queues the handler. If new execution is
needed, establish a fresh unambiguous target state and request a new trusted
approval.

The local OS account name is stored as the operator identity. `--note` is
optional and limited to 1,024 characters. JSON and non-interactive
reconciliation require `--yes` and never prompt.

## JSON and exit status

Add `--json` to any leaf command. JSON writes one bounded object to stdout with
a stable `code`; list results contain `items` and `next_offset`. Human output is
reserved for invocations without `--json`.

| Exit | Meaning |
|---:|---|
| 0 | Success, or durable health is OK |
| 1 | Durable health warning |
| 2 | Invalid command/input or confirmation required |
| 3 | Receipt not found |
| 4 | State or concurrency conflict |
| 5 | Database/subsystem failure or critical health |

Stable mutation codes include `cancelled`, `reconciled`, `not_found`,
`already_terminal`, `already_reconciled`, `execution_in_progress`,
`concurrent_update`, `invalid_state`, and `invalid_superseding_receipt`.

## Health and readiness

Run either local health surface:

```bash
hermes durable doctor
hermes durable doctor --json
hermes doctor
```

These checks are read-only. `hermes doctor --fix` does not cancel or reconcile
durable operations.

Health severity is:

| Severity | Conditions |
|---|---|
| `ok` | Ledger is absent/not initialized or readable, with no detected actionable backlog or structural problem |
| `warning` | Five-minute-old pending delivery, ready action, or pending notification; stale recoverable delivery/wake/action/notification claim; invalid allowlist or missing handler; enabled feature whose worker is stopped or cannot be confirmed |
| `critical` | Database unavailable/corrupt, incomplete/orphaned durable schema state, or an unreconciled `unknown` action |

An offline CLI cannot inspect the gateway's asyncio tasks. When a durable
feature is enabled it reports worker confirmation unavailable instead of
claiming the worker is live. The authenticated `/health/detailed` endpoint uses
the gateway's actual durable worker tasks and includes a bounded
`checks.durable` object. Warning or critical durable health degrades the
detailed readiness result. Public `/health` remains shallow and does not query
the durable database.

Historical terminal rows are healthy. A reconciled action remains `unknown` in
the action ledger but no longer triggers the unreconciled-UNKNOWN critical
condition.

## Metrics

When gateway OTLP health export is enabled, Hermes exports SQLite snapshot
gauges. Snapshot failure is fail-open: the durable gauges are omitted without
stopping the gateway or the other health metrics.

| Gauge | Labels |
|---|---|
| `hermes_durable_requests` | `status`, `platform` |
| `hermes_durable_actions` | `status`, `action_type` |
| `hermes_durable_notifications` | `status`, `origin_type` |
| `hermes_durable_stale_claims` | `claim_type` |
| `hermes_durable_unknown_actions` | `action_type`, `reconciled` |
| `hermes_durable_reconciliations` | `action_type`, `outcome` |
| `hermes_durable_oldest_backlog_seconds` | `queue_type` |

These are current gauges, not transition counters. Labels are bounded and
content-free; receipts, notes, actors, payloads, fingerprints, provider IDs,
and raw errors are never labels.

## Retention

The configured `durable_requests.terminal_retention_seconds` controls normal
terminal pruning. Pending/claimed wake delivery and pending/claimed action
notifications protect their records from pruning. Unreconciled `unknown`
actions are retained regardless of the normal age or terminal-row cap.

After reconciliation, an `unknown` action becomes eligible only after the
request and completed action cross the retention boundary and notification is
`delivered` or `dropped`. Hermes then removes the linked request, action ticket,
and reconciliation evidence together.

## Rollback

Before rolling back a gateway that has processed durable operations, stop the
gateway and take a consistent backup of the active profile's `state.db` and its
WAL/SHM files. Disable `durable_requests.enabled` and
`durable_approvals.enabled` if the older runtime must not process queued work.
Preserve the database: Phase 4D adds reconciliation storage without rewriting
existing request or action rows, and direct deletion would discard the evidence
needed to resolve ambiguous execution.

After restoring the Phase 4D runtime, run `hermes durable doctor` and inspect
any `unknown` receipts before re-enabling processing. Never make an older
runtime compatible by deleting tables or editing durable states by hand.
