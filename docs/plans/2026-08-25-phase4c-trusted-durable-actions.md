# Phase 4C Trusted Durable Actions Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add disabled-by-default trusted durable approval tickets with one allowlisted `cron.resume` handler, at-most-once execution, current-state revalidation, and durable completion notification.

**Architecture:** Persist action tickets beside durable requests in the existing profile `state.db`; couple request and ticket transitions in one transaction; execute registered handlers through a separate supervised runner; reuse Phase 4B lineage and cron continuation routing for final notifications. The generic durable-request tool and Discord transport remain unchanged.

**Tech Stack:** Python standard library, SQLite, asyncio, existing Hermes gateway/cron infrastructure, pytest through `scripts/run_tests.sh`.

---

## Ground rules

- Work only in `/path/to/hermes-worktree` on `feature/discord-interactive-ui`.
- Do not touch unrelated local worktrees, the production runtime, or backup directories.
- Use `scripts/run_tests.sh`; do not call pytest directly.
- Follow red-green-refactor for every behavior change.
- Keep the model-facing `durable_request` tool pure; it must have no action parameters.
- Do not add dependencies, a generic action tool, serialized callbacks, or Discord adapter changes.

## Task 1: Add the action-ticket schema and value types

**Files:**

- Create: `gateway/durable_actions.py`
- Modify: `hermes_state_common.py`
- Modify: `gateway/durable_requests.py`
- Create: `tests/gateway/test_durable_actions.py`

**Step 1: Write failing schema reconciliation tests**

Add tests that initialize a fresh state database through both supported paths:

1. the central state schema;
2. `DurableRequestStore` directly.

Assert that `durable_action_tickets` exists with:

- `request_id` primary key;
- action type/version, bounded payload JSON, fingerprint, and idempotency key;
- status and execution claim timestamps;
- bounded outcome fields;
- completion-notification state and timestamps;
- created/updated timestamps;
- a unique `(action_type, idempotency_key)` index;
- an index supporting runnable-action and pending-notification scans.

Also initialize a Phase 4B-style database and assert reconciliation preserves
existing durable requests without creating tickets.

Run:

```bash
./scripts/run_tests.sh tests/gateway/test_durable_actions.py -q
```

Expected: fail because the table and action types do not exist.

**Step 2: Add the minimum schema and types**

In `gateway/durable_actions.py`, define:

- `DurableActionStatus` with the approved lifecycle values;
- `DurableActionNotificationStatus` with `not_ready`, `pending`, `claimed`,
  `delivered`, and `dropped`;
- frozen action ticket/outcome dataclasses used by persistence and tests.

Define one shared `DURABLE_ACTION_SCHEMA_SQL` constant in the lower-level
`hermes_state_common.py`; include it in the central schema and import it from
`gateway/durable_requests.py` for `DurableRequestStore._ensure_schema()`. This
keeps the two initialization paths from drifting without making state-schema
code import the gateway layer. Do not rely on SQLite foreign-key enforcement;
pruning will protect the relationship explicitly.

**Step 3: Run the focused test**

```bash
./scripts/run_tests.sh tests/gateway/test_durable_actions.py -q
```

Expected: pass.

**Step 4: Commit**

```bash
git add gateway/durable_actions.py gateway/durable_requests.py hermes_state_common.py tests/gateway/test_durable_actions.py
git commit -m "feat(durable-actions): add action ticket schema"
```

## Task 2: Add the trusted handler registry and bounded serialization

**Files:**

- Modify: `gateway/durable_actions.py`
- Modify: `tests/gateway/test_durable_actions.py`

**Step 1: Write failing registry and validation tests**

Cover:

- registration and exact `(action_type, version)` lookup;
- duplicate registration rejection;
- unknown type/version returning no executable handler;
- canonical JSON payload acceptance;
- rejection of non-JSON values, oversized payloads/results, invalid action
  names, blank fingerprints, and blank idempotency keys;
- persisted data cannot specify an import path or callback;
- handler exceptions are converted later into bounded structural outcomes, not
  stored raw exceptions.

Run the focused file and observe failure.

**Step 2: Implement the static registry**

Add frozen `PreparedDurableAction`, `DurableActionExecutionContext`, and
`DurableActionOutcome` types plus:

- `register_durable_action_handler(...)`;
- exact-version lookup;
- canonical bounded JSON encoding/decoding helpers.

Use module-local dictionaries and standard-library `json`; no factory or
plugin loader.

**Step 3: Run and commit**

```bash
./scripts/run_tests.sh tests/gateway/test_durable_actions.py -q
git add gateway/durable_actions.py tests/gateway/test_durable_actions.py
git commit -m "feat(durable-actions): add trusted handler registry"
```

## Task 3: Create requests and tickets atomically

**Files:**

- Modify: `gateway/durable_requests.py`
- Modify: `gateway/durable_actions.py`
- Modify: `tests/gateway/test_durable_actions.py`
- Modify: `tests/gateway/test_durable_requests.py`

**Step 1: Write failing atomic-creation tests**

Test a trusted store API that receives validated request data plus a prepared
action and:

- creates the request and ticket in one transaction;
- always uses fixed internal option IDs `approve` and `reject`;
- preserves origin session/profile routing data while setting ordinary request
  wake to not requested;
- returns the existing request/ticket for the same action type and idempotency
  key, including terminal tickets;
- rolls back the request if ticket insertion fails;
- rolls back the ticket if request insertion fails;
- keeps ordinary `DurableRequestStore.create()` and the generic tool ticket-free.

Include a reference collision test that cannot return a request belonging to a
different action.

**Step 2: Refactor one internal request insert seam**

Extract only the minimum private helper needed for `create()` and trusted
creation to share validation and insertion inside the caller's existing
`BEGIN IMMEDIATE` transaction. Add `create_with_action(...)` on the store; do
not perform a public request create followed by a separate insert.

The trusted API accepts prepared data only. It does not accept origin identity,
profile, platform scope, action type, or handler version from model-controlled
JSON.

**Step 3: Run regression tests**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_requests.py \
  tests/tools/test_durable_request_tool.py \
  -q
```

Expected: pass, including proof that the generic tool cannot attach actions.

**Step 4: Commit**

```bash
git add gateway/durable_actions.py gateway/durable_requests.py tests/gateway/test_durable_actions.py tests/gateway/test_durable_requests.py
git commit -m "feat(durable-actions): create approvals atomically"
```

## Task 4: Couple every request terminal transition to the ticket

**Files:**

- Modify: `gateway/durable_requests.py`
- Modify: `gateway/durable_actions.py`
- Modify: `tests/gateway/test_durable_actions.py`
- Modify: `tests/gateway/test_durable_request_lifecycle.py`

**Step 1: Write a terminal-transition matrix test**

For ticketed requests assert, in the same transaction:

- canonical `approve` -> request selected and action `ready`;
- canonical `reject` -> `declined` plus notification pending;
- expiry -> `expired` plus notification pending;
- origin cancellation -> `cancelled` plus notification pending;
- delivery terminal failure, unsupported delivery, missing provider message,
  stale ambiguous delivery, and open failure -> `abandoned` plus notification
  pending;
- duplicate resolution/cancellation does not change or requeue the ticket;
- display labels named Approve cannot run an action unless their internal ID is
  canonical `approve`;
- all ticketed request rows retain ordinary wake `not_requested`.

Exercise the actual store entry points: `resolve`, `cancel`, `fail_delivery`,
`mark_open_failed`, stale-delivery reconciliation, and both expiry paths.

**Step 2: Add one shared locked transition helper**

Implement a private helper invoked by every terminal mutation site while the
existing durable-request transaction is held. It updates a ticket only when one
exists and never infers executable state in a background scan.

Protect pruning so an active ticket or undelivered action notification keeps
its parent request. Prune a terminal ticket and parent only when both satisfy
retention rules.

**Step 3: Run lifecycle regressions and commit**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_request_lifecycle.py \
  tests/gateway/test_durable_requests.py \
  -q
git add gateway/durable_actions.py gateway/durable_requests.py tests/gateway/test_durable_actions.py tests/gateway/test_durable_request_lifecycle.py
git commit -m "feat(durable-actions): couple request outcomes"
```

## Task 5: Implement claims, crash recovery, and immutable outcomes

**Files:**

- Modify: `gateway/durable_actions.py`
- Create: `gateway/durable_action_runner.py`
- Create: `tests/gateway/test_durable_action_runner.py`

**Step 1: Write failing ledger tests**

Test:

- exactly one claimant wins for a ready action;
- claim records a random token and timestamp;
- matching claimant marks execution started before handler entry;
- wrong/stale tokens cannot mutate a ticket;
- stale `claimed` without execution start returns to `ready`;
- stale `executing` becomes `unknown` and notification pending;
- `unknown` is never automatically claimed again;
- success and other terminal outcomes cannot be overwritten;
- disabled or unavailable handlers become `handler_unavailable` without a
  handler call;
- handler exceptions become bounded `failed` outcomes without raw exception
  persistence;
- repeated scans cannot execute a ticket twice;
- handler execution occurs outside a database transaction.

**Step 2: Implement the minimum store and runner**

Keep SQL persistence methods in `gateway/durable_actions.py`. Add a small
async runner in `gateway/durable_action_runner.py` that:

1. recovers stale claims;
2. claims one ready row;
3. checks configuration allowlisting and exact handler version;
4. marks execution started;
5. invokes the handler outside SQLite locks, using `asyncio.to_thread` for a
   synchronous handler;
6. persists one bounded terminal outcome.

Do not introduce variable retry policies or an execution framework.

**Step 3: Run and commit**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_action_runner.py \
  -q
git add gateway/durable_actions.py gateway/durable_action_runner.py tests/gateway/test_durable_action_runner.py
git commit -m "feat(durable-actions): execute tickets at most once"
```

## Task 6: Add the locked `cron.resume` handler

**Files:**

- Modify: `cron/jobs.py`
- Modify: `cron/scheduler.py`
- Modify: `gateway/durable_actions.py`
- Create: `tests/cron/test_durable_cron_resume.py`
- Modify: `tests/cron/test_scheduler.py`

**Step 1: Write failing preparation and execution tests**

Cover:

- preparation accepts only a canonical existing paused job ID;
- payload contains only `job_id`;
- fingerprint is deterministic and changes when any execution-relevant job
  definition changes;
- secret material is not persisted or logged;
- unchanged paused job resumes and returns `succeeded`;
- unchanged already-resumed job returns `already_applied`;
- modified job, removed job, revoked policy, and invalid current configuration
  return `invalidated` without mutation;
- one-shot scheduling errors return a bounded failure;
- fingerprint check and mutation are protected by one jobs lock;
- successful execution uses the existing provider-job change notification;
- repeated execution is idempotent.

**Step 2: Implement canonical fingerprinting and conditional resume**

In `cron/jobs.py`, add the minimum helpers to:

- select and canonicalize execution-relevant job definition fields;
- hash the canonical JSON with `hashlib.sha256`;
- resume only if the loaded canonical job matches the expected fingerprint,
  all under one `_jobs_lock()` acquisition.

Avoid calling existing helpers that reacquire the jobs lock from inside this
critical section; reuse their underlying load/save and next-run logic.

Register `cron.resume` version 1 with prepare/execute functions. The handler
must retrieve current trusted configuration at execution time.

**Step 3: Run cron tests and commit**

```bash
./scripts/run_tests.sh \
  tests/cron/test_durable_cron_resume.py \
  tests/cron/test_scheduler.py \
  tests/tools/test_cronjob_tools.py \
  -q
git add cron/jobs.py cron/scheduler.py gateway/durable_actions.py tests/cron/test_durable_cron_resume.py tests/cron/test_scheduler.py
git commit -m "feat(cron): add trusted durable resume handler"
```

## Task 7: Gate only model-initiated resume calls

**Files:**

- Modify: `hermes_cli/config_defaults.py`
- Modify: `tools/cronjob_tools.py`
- Modify: `tests/tools/test_cronjob_tools.py`
- Modify: `tests/cron/test_durable_request_context.py`
- Create: `tests/tools/test_durable_cron_approval.py`

**Step 1: Write failing configuration and routing tests**

Assert:

- defaults are `durable_approvals.enabled: false` and `allowed_actions: []`;
- disabled feature preserves immediate model resume;
- enabled and allowlisted model resume returns `approval_pending` with request
  ID, action type, expiry, and no sensitive routing/fingerprint/payload data;
- enabled without allowlist fails closed and does not resume;
- missing durable-request inbox/context fails closed with a normal tool error;
- CLI/operator calls still use immediate `resume_job`;
- the model cannot supply action type, version, payload, fingerprint, origin,
  responder policy, or handler name;
- duplicate unchanged resume returns the existing ticket and `created: false`.

**Step 2: Add a model-tool-only wrapper**

Keep the existing Python `cronjob()` function as the immediate domain API so
CLI/API/dashboard callers remain unchanged. Change only the registered
model-tool handler for `action="resume"` to:

1. inspect trusted origin/profile context;
2. read `durable_approvals` and durable-request inbox configuration;
3. call the registered `cron.resume` prepare function;
4. atomically create the binary durable approval and ticket;
5. return a redacted `approval_pending` result.

All other cron actions delegate unchanged.

**Step 3: Run and commit**

```bash
./scripts/run_tests.sh \
  tests/tools/test_durable_cron_approval.py \
  tests/tools/test_cronjob_tools.py \
  tests/cron/test_durable_request_context.py \
  -q
git add hermes_cli/config_defaults.py tools/cronjob_tools.py tests/tools/test_durable_cron_approval.py tests/tools/test_cronjob_tools.py tests/cron/test_durable_request_context.py
git commit -m "feat(cron): require durable approval for model resume"
```

## Task 8: Deliver exactly one bounded completion notification

**Files:**

- Modify: `gateway/durable_actions.py`
- Modify: `gateway/durable_action_runner.py`
- Modify: `gateway/run.py`
- Modify: `cron/scheduler.py`
- Create: `tests/gateway/test_durable_action_notifications.py`
- Modify: `tests/gateway/test_durable_request_wake_routing.py`
- Modify: `tests/cron/test_durable_request_continuation.py`
- Modify: `tests/cron/test_execution_ledger.py`

**Step 1: Write failing notification tests**

Cover:

- approve does not send an ordinary request-terminal wake;
- success sends one completion turn after outcome persistence;
- reject, expire, cancel, abandon, invalidation, failure, unknown, and handler
  unavailable each send an accurate no-action/result notification;
- model-facing event contains only approved bounded fields;
- `/new` or deleted session drops notification with a structural reason;
- valid compression lineage receives it;
- notification failure/retry cannot alter the action outcome;
- duplicate scans and interactions do not create a second completion turn;
- cron completion reserves one execution with `source="durable_action"` and
  `trigger_id=request_id`;
- cron notification uses current job configuration, replays no script or
  `context_from`, and does not alter `next_run_at`, enabled state, or repeat
  counters.

**Step 2: Reuse Phase 4B routing with a bounded action event**

Add action-notification claim/ack/drop methods to the ticket store. Extend the
runner to dispatch pending notifications after action processing.

In `gateway/run.py`, start and stop one supervised action runner per gateway,
iterate the same profile scopes used by the durable-request worker, and route
session notifications through the existing lineage classifier and injection
path.

Add a sibling cron enqueue function that shares the existing execution-ledger
reservation behavior while setting `source="durable_action"`. It must create a
schedule-neutral fresh turn and use only the bounded completion event.

**Step 3: Run and commit**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_action_notifications.py \
  tests/gateway/test_durable_action_runner.py \
  tests/gateway/test_durable_request_wake_routing.py \
  tests/cron/test_durable_request_continuation.py \
  tests/cron/test_execution_ledger.py \
  -q
git add gateway/durable_actions.py gateway/durable_action_runner.py gateway/run.py cron/scheduler.py tests/gateway/test_durable_action_notifications.py tests/gateway/test_durable_action_runner.py tests/gateway/test_durable_request_wake_routing.py tests/cron/test_durable_request_continuation.py tests/cron/test_execution_ledger.py
git commit -m "feat(durable-actions): deliver completion notifications"
```

## Task 9: Add end-to-end security and upgrade coverage

**Files:**

- Create: `tests/gateway/test_durable_action_e2e.py`
- Modify: `tests/gateway/test_durable_action_notifications.py`
- Modify: `tests/gateway/test_durable_actions.py`
- Modify: `docs/design/durable-requests.md`

**Step 1: Add real-import temporary-profile tests**

Use temporary `HERMES_HOME` profiles and real imports to prove:

- Phase 4B database upgrade preserves rows and wakes none of them;
- generic durable requests never create or trigger action tickets;
- a generic option labelled Approve cannot trigger `cron.resume`;
- durable approval is independent of terminal/execution approval mechanisms;
- no arbitrary action name or stored import path can execute;
- action payload, fingerprint, actor/provider IDs, claims, credentials, and raw
  exceptions do not appear in tool responses, completion events, or structural
  logs;
- a complete approved `cron.resume` path creates, resolves, revalidates,
  executes once, persists outcome, and notifies once;
- changed state invalidates the same path without mutation;
- feature disable pauses ready-action processing without deleting tickets, and
  re-enable resumes it once.

**Step 2: Document the shipped contract**

Update `docs/design/durable-requests.md` with:

- generic-request versus trusted-action boundary;
- ticket state machine and crash guarantees;
- notification timing and privacy contract;
- disabled-by-default `cron.resume` pilot configuration;
- operator recovery guidance for `unknown`.

**Step 3: Run focused Phase 4C and commit**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_action_runner.py \
  tests/gateway/test_durable_action_notifications.py \
  tests/gateway/test_durable_action_e2e.py \
  tests/cron/test_durable_cron_resume.py \
  tests/tools/test_durable_cron_approval.py \
  -q
git add tests/gateway/test_durable_action_e2e.py tests/gateway/test_durable_action_notifications.py tests/gateway/test_durable_actions.py docs/design/durable-requests.md
git commit -m "test(durable-actions): cover trusted approval boundary"
```

## Task 10: Run regression suites and review the final diff

**Files:** None expected unless a test exposes a defect.

**Step 1: Run the complete durable/Discord/cron regression set**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_action_runner.py \
  tests/gateway/test_durable_action_notifications.py \
  tests/gateway/test_durable_action_e2e.py \
  tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_request_lifecycle.py \
  tests/gateway/test_durable_request_bridge.py \
  tests/gateway/test_durable_request_wake.py \
  tests/gateway/test_durable_request_wake_routing.py \
  tests/gateway/test_discord_durable_requests.py \
  tests/gateway/test_discord_component_auth.py \
  tests/gateway/test_discord_exec_approval_content.py \
  tests/tools/test_durable_request_tool.py \
  tests/tools/test_request_decision_tool.py \
  tests/tools/test_cronjob_tools.py \
  tests/tools/test_durable_cron_approval.py \
  tests/cron/test_durable_cron_resume.py \
  tests/cron/test_durable_request_continuation.py \
  tests/cron/test_execution_ledger.py \
  tests/cron/test_scheduler.py \
  -q
```

Expected: all pass with no skips caused by missing runtime dependencies.

**Step 2: Run the complete repository suite**

```bash
./scripts/run_tests.sh
```

Record any pre-existing environmental failures separately; do not weaken or
skip tests to obtain a green result.

**Step 3: Inspect scope and history**

```bash
git diff --check
git status --short
git log --oneline --decorate -12
```

Confirm `local backup directory` remains untracked and untouched,
the old installation and production runtime were not modified, and no personal
GWS content, credentials, databases, caches, virtualenvs, or runtime state are
tracked.

**Step 4: Request code review before promotion**

Use the `superpowers:requesting-code-review` skill. Address verified findings
with test-first fixes, rerun affected suites, and leave the development branch
writable and available. Do not promote, archive, upload, push, or open a PR
without a separate explicit request.
