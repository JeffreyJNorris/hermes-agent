# Phase 4D Durable Operations Design

## Goal

Provide trusted local inspection, safe cancellation, conservative UNKNOWN
reconciliation, and structural health visibility for durable requests and
actions without direct SQLite edits or any new model-facing surface.

Phase 4D is based on promoted commit
`fa37ed778b58f856c39a752937d046980c8b7b94`. Phase 4C.2 does not begin until
4D is promoted.

## Boundaries

- SQLite remains authoritative; Discord remains presentation only.
- Generic `durable_request` remains non-executable.
- Operator commands are trusted local CLI commands, never model or slash tools.
- Reconciliation records an operator conclusion and never invokes or retries a
  handler.
- Application diagnostics run after database transactions and connections
  close.
- Schema changes are additive and idempotent for Phase 4A through 4C databases.
- No new CLI, component, or telemetry framework is introduced.
- Metrics are authoritative SQLite snapshot gauges and ages only. Phase 4D
  does not add transition counters.

## Architecture

`gateway/durable_operations.py` is the trusted operator facade. It owns
immutable redacted projections, bounded list/lookups, cancellation
compare-and-set operations, append-only reconciliation, and durable-health
calculations. It reuses the current request/action stores and transition rules
instead of exposing raw rows or duplicating handler execution.

Thin consumers are:

- `hermes durable ...` for local parsing, confirmation, formatting, and exits;
- authenticated readiness and local doctor for health;
- the existing gateway-health exporter for optional OTLP snapshot gauges.

The existing request ID is also the action receipt. No surrogate action ID is
added. Optional handler diagnostics are read-only registry callbacks invoked
only after ledger reads have closed their SQLite connection.

## Repository conventions

- Root parser: `hermes_cli._parser.build_top_level_parser()` and
  `hermes_cli.main.main()`.
- Group parser: `hermes_cli/subcommands/<group>.py` with nested argparse
  subparsers and a lazy `cmd_*` implementation adapter.
- Human output and confirmation: `hermes_cli.cli_output`; destructive commands
  default to no and accept `-y/--yes`.
- JSON output: direct bounded dict/list JSON, with no human chatter on stdout.
- Runtime health: `gateway.readiness.collect_runtime_readiness()` and
  authenticated `/health/detailed`; public `/health` stays unchanged.
- Local health: `hermes_cli.doctor.run_doctor()`.
- Metrics: `agent.monitoring.gateway_health.GatewayMetric`, merged and exported
  through `gateway_health_export`; no additional dependency.
- Schema: `hermes_state_common.py` plus the existing durable-store schema
  reconciler.

## State-transition matrix

| Ledger | From | Event | To / result |
|---|---|---|---|
| Request | new | create | `pending_delivery` |
| Request | `pending_delivery` | provider bind | `open` |
| Request | active | selection | `selected` |
| Request | active | deadline/cancel | `expired` / `cancelled` |
| Request | pending/open | delivery terminal | `unsupported`, `delivery_failed`, or `delivery_unknown` |
| Wake | `not_requested` | terminal wake requested | `pending` |
| Wake | `pending` | worker claim | `claimed` |
| Wake | `claimed` | accepted/unsafe origin | `delivered` / `dropped` |
| Action | new | atomic request/action create | `waiting_for_decision` |
| Action | waiting | canonical approve/reject | `ready` / `declined` |
| Action | waiting | expiry/cancel/delivery terminal | `expired`, `cancelled`, or `abandoned` |
| Action | `ready` | runner claim/start | `claimed` then `executing` |
| Action | `executing` | handler outcome | `succeeded`, `already_applied`, `invalidated`, `failed`, or `unknown` |
| Action | `claimed` | unavailable handler | `handler_unavailable` |
| Notification | `not_ready` | action terminal | `pending` |
| Notification | `pending` | worker claim | `claimed` |
| Notification | `claimed` | accepted/unsafe origin | `delivered` / `dropped` |
| Reconciliation | none | UNKNOWN operator classification | one immutable reconciliation row |

Stale delivery, wake, execution, and notification claim checks import the
existing centralized five-minute thresholds. A stale pre-execution action
claim may return to `ready`; stale `executing` becomes `unknown` and is never
automatically retried.

## Read projections and pagination

Frozen request and action summary/detail dataclasses expose only stable
operator fields. Lists support the specified state/type/origin/age filters,
use a bounded `--limit`, and use deterministic offset pagination ordered by
`created_at DESC, request_id DESC`. Unknown receipts return stable `not_found`.

Age fields use one injected wall clock and cover pending delivery, ready work,
claims, notifications, and UNKNOWN actions. Database connections close before
any handler-specific diagnostic callback. New indexes are added only when
`EXPLAIN QUERY PLAN` demonstrates a need.

## Privacy and redaction matrix

| Safe by default | Omitted by default and from logs/metrics |
|---|---|
| Request/action receipt | Prompt title, body, options, selected label |
| Kind, action type/version | Action payload/result JSON and fingerprint |
| Request/action states | Provider channel, thread, message, and payload |
| Origin type | Origin ID, session IDs, profile, actor IDs |
| Delivery/wake/notification state | Idempotency keys and claim tokens |
| Timestamps, bounded ages/attempts | Cron prompt, command, PID, raw errors |
| Bounded structural reason/outcome | Reconciliation note/operator identity outside trusted inspect |
| Reconciliation outcome | Any user-controlled content as a metric label |

Local reconciliation inspect may show the bounded operator identity and note;
lists, ordinary logs, health, and metrics never do. Exception classes may be
logged structurally, but raw exception messages may not.

## Local CLI

```text
hermes durable requests list [filters] [--limit N] [--offset N] [--json]
hermes durable requests inspect REQUEST_ID [--json]
hermes durable requests cancel REQUEST_ID [-y|--yes] [--json]

hermes durable actions list [filters] [--limit N] [--offset N] [--json]
hermes durable actions inspect REQUEST_ID [--diagnose] [--json]
hermes durable actions reconcile REQUEST_ID --outcome OUTCOME [options] [-y|--yes] [--json]

hermes durable doctor [--json]
```

Cancellation rules:

- An open generic request uses the existing request cancellation transaction.
- `waiting_for_decision` cancels its linked request and action atomically.
- `ready` cancels only through compare-and-set when there is no active claim.
- `claimed`, `executing`, `unknown`, and terminal actions reject cancellation.
- A concurrent runner claim returns `concurrent_update`; it is never raced.

Human destructive operations show a structural preview and use the existing
default-no prompt. JSON or non-interactive destructive use requires `--yes`.
Stable JSON codes distinguish `not_found`, `already_terminal`, `invalid_state`,
`execution_in_progress`, `concurrent_update`, `feature_disabled`,
`database_unavailable`, `handler_unavailable`, and
`reconciliation_required`. Process exits are local constants: 0 success,
1 warning, 2 invalid input/usage, 3 not found, 4 conflict, 5 subsystem failure.

The command is registered only in the local CLI parser and built-in command
set. It is absent from model tools, platform presets, slash commands, ACP, and
API-server tool discovery.

## UNKNOWN reconciliation

Add `durable_action_reconciliations` with one row per action receipt:

```text
reconciliation_id
request_id
outcome
operator_identity
note
superseded_by_request_id
created_at
```

Allowed outcomes are `confirmed_applied`, `confirmed_not_applied`, `abandoned`,
and `superseded`. `superseded` requires a different existing action receipt of
the same action type. The local OS account supplies a bounded operator identity;
an optional note is separately bounded and never logged.

Only `unknown` actions may be reconciled. The action remains `unknown`; its
projection adds the immutable effective classification. A unique constraint on
`request_id` arbitrates concurrent operators. Reconciliation never calls a
handler, never changes an action to `ready`, and never claims whether a
superseding action ran.

Pruning retains unreconciled UNKNOWN rows and reconciliation evidence for the
documented terminal retention. Linked request, ticket, and reconciliation rows
are removed together only after the retention and notification conditions hold.

## Read-only handler diagnostics

Handler registration may provide an optional `diagnose_unknown` callback.
Existing registrations remain source-compatible. Unknown types/versions and
diagnostic failures produce bounded structural results without changing state.

`cron.resume` version 1 reports only:

- target job exists;
- target is currently paused or enabled;
- current definition fingerprint matches or differs;
- structural execution-ledger evidence exists.

It never returns job prompt/command content, mutates the job, chooses a
reconciliation outcome, or runs while a ledger connection is open.

## Metrics inventory

The existing periodic exporter receives authoritative snapshot gauges:

| Metric | Labels |
|---|---|
| `hermes_durable_requests` | status, platform |
| `hermes_durable_actions` | status, action_type |
| `hermes_durable_notifications` | status, origin_type |
| `hermes_durable_stale_claims` | claim_type |
| `hermes_durable_unknown_actions` | action_type, reconciled |
| `hermes_durable_reconciliations` | action_type, outcome |
| `hermes_durable_oldest_backlog_seconds` | queue_type |

Snapshots use read-only/query-only SQLite connections and close before export.
There are no IDs, notes, actors, payloads, or arbitrary text labels. No
in-memory or synthetic transition counter is added.

## Health severity policy

| Severity | Conditions |
|---|---|
| OK | Database readable; configured handlers available; no actionable backlog |
| Warning / degraded | Overdue delivery, ready action, or notification; stale recoverable claim; invalid allowlist; enabled subsystem without a confirmed live worker |
| Critical | Database unavailable/corrupt, impossible state, or unreconciled UNKNOWN |

Historical terminal rows are never unhealthy. One pure durable-health
collector feeds `hermes durable doctor`, the existing doctor section,
authenticated detailed readiness, and monitoring snapshots. Public `/health`
remains shallow. Gateway callers supply actual supervised worker task state;
offline CLI inspection reports inability to confirm a worker rather than
inventing liveness.

## Migration plan

1. Increment the central schema version.
2. Add reconciliation table and unique/FK indexes with `IF NOT EXISTS` to the
   shared durable schema.
3. Invoke the same reconciler from central initialization and direct durable
   stores.
4. Preserve every existing request/action row and create no reconciliation
   rows during upgrade.
5. Extend pruning to keep UNKNOWN/reconciliation evidence through retention.
6. Verify fresh databases, Phase 4A/4B/4C fixtures, repeated initialization,
   concurrent reconciliation uniqueness, and row preservation.

No action-status rewrite, surrogate action identifier, payload migration, or
provider schema change is required.

## Testing and promotion

Implementation uses test-driven development and small compiling commits:

1. projections and queries;
2. local list/inspect/cancel CLI;
3. reconciliation schema/API/command;
4. read-only diagnostics;
5. gauges and health integration;
6. migration, privacy, concurrency, fault, and discovery tests;
7. operator documentation.

The focused gate includes Phase 4C regressions, schema upgrades, deterministic
pagination, cancellation races, duplicate reconciliation, zero handler calls
during reconciliation, connection closure before diagnostics, privacy
canaries, tool-discovery exclusion, health severity, and bounded metric labels.
The full repository and exported-runtime focused suites follow.

Live acceptance fault-injects one UNKNOWN action, inspects it through the local
CLI, runs read-only diagnostics, reconciles it without invoking a handler, and
confirms health/metrics update structurally. The immutable Phase 4D runtime is
promoted separately with Phase 4C retained as rollback material.
