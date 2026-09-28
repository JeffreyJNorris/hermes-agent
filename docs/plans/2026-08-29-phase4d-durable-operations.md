# Phase 4D Durable Operations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add trusted local inspection, cancellation, UNKNOWN reconciliation, diagnostics, and structural health for durable requests/actions without direct SQLite edits or handler replay.

**Architecture:** A new `gateway.durable_operations` facade returns immutable redacted projections and owns operator-only compare-and-set mutations. Thin local CLI, readiness, doctor, and monitoring consumers reuse that facade; the existing request/action ledgers remain authoritative and Discord/model surfaces remain unchanged.

**Tech Stack:** Python 3.11, stdlib `argparse`, dataclasses, SQLite, existing Hermes CLI output helpers, readiness probes, and OTLP gateway-health gauges.

---

## Baseline and working agreement

- Worktree: `/path/to/hermes-worktree`
- Branch: `feature/durable-operations-4d`
- Base: `fa37ed778b58f856c39a752937d046980c8b7b94`
- Approved design: `docs/plans/2026-08-29-phase4d-durable-operations-design.md`
- Baseline: 27 files, 445 tests passed, 0 failed.
- Use `./scripts/run_tests.sh`, never direct pytest, for recorded gates.
- Follow red-green TDD for every behavior change.
- Do not touch Phase 4C.2/4C.3, Discord components, or model tool schemas.

### Task 1: Immutable operator projections and bounded reads

**Files:**
- Create: `gateway/durable_operations.py`
- Create: `tests/gateway/test_durable_operations.py`

**Step 1: Write failing projection tests**

Create fixtures using a temporary `state.db` initialized through
`DurableRequestStore`. Insert generic and action-backed requests through the
real stores. Assert that summaries are frozen, deterministically ordered by
`created_at DESC, request_id DESC`, filter correctly, and contain none of the
sensitive source columns.

```python
def test_request_list_is_bounded_stable_and_redacted(tmp_path):
    store = seeded_operations_store(tmp_path)
    page = store.list_requests(limit=1, offset=0, now=500.0)
    assert len(page.items) == 1
    assert page.items[0].request_id == "drq_newer"
    assert not hasattr(page.items[0], "body")
    assert page.next_offset == 1


def test_action_lookup_by_request_omits_execution_material(tmp_path):
    detail = seeded_operations_store(tmp_path).get_action("drq_action", now=500.0)
    assert detail.action_type == "cron.resume"
    for name in ("payload_json", "fingerprint", "idempotency_key", "execution_claim"):
        assert not hasattr(detail, name)
```

Also cover unknown receipt, request status/origin filters, action status/type/age
filters, `limit` bounds, negative offset rejection, equal-timestamp ordering,
one injected wall clock for all age fields, and Phase 4A/B rows without tickets.

**Step 2: Run the tests and verify RED**

```bash
./scripts/run_tests.sh tests/gateway/test_durable_operations.py -q
```

Expected: failure because `gateway.durable_operations` does not exist.

**Step 3: Implement the minimal read facade**

Define only the approved fields:

```python
@dataclass(frozen=True, slots=True)
class DurableRequestSummary:
    request_id: str
    kind: str
    status: str
    origin_type: str
    platform: str
    delivery_state: str
    wake_status: str
    created_at: float
    expires_at: float
    resolved_at: float | None
    pending_age_seconds: float | None


@dataclass(frozen=True, slots=True)
class DurableActionSummary:
    request_id: str
    action_type: str
    action_version: int
    status: str
    created_at: float
    execution_claimed_at: float | None
    completed_at: float | None
    notification_status: str
    effective_reconciliation: str | None
    actionable_age_seconds: float | None


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class OperatorPage(Generic[T]):
    items: tuple[T, ...]
    next_offset: int | None
```

Use explicit column lists, `contextlib.closing`, `mode=ro`/`query_only` for read
calls, default limit 50, maximum 100, and no raw-row public API. Details extend
summaries only with bounded structural reasons, attempts, and linked states.

**Step 4: Verify GREEN and adjacent schema tests**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_operations.py \
  tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_actions.py -q
```

Expected: all pass.

**Step 5: Commit**

```bash
git add gateway/durable_operations.py tests/gateway/test_durable_operations.py
git commit -m "feat(durable): add operator read projections"
```

### Task 2: Safe operator cancellation with compare-and-set

**Files:**
- Modify: `gateway/durable_operations.py`
- Modify: `tests/gateway/test_durable_operations.py`

**Step 1: Write failing cancellation tests**

Cover open generic request cancellation, action-backed
`waiting_for_decision`, unclaimed `ready`, concurrently claimed `ready`,
`executing`, `unknown`, and every terminal state.

```python
def test_cancel_ready_action_loses_to_runner_claim(tmp_path):
    ops, action_store = seeded_ready_action(tmp_path)
    claim = action_store.claim_next({"cron.resume"})
    result = ops.cancel_action("drq_ready", now=500.0)
    assert result.code == "execution_in_progress"
    assert action_store.get("drq_ready").status is DurableActionStatus.CLAIMED
```

Assert cancelled actions set notification pending, never become runnable, and
request/action changes occur in one transaction where both are active.

**Step 2: Verify RED**

```bash
./scripts/run_tests.sh tests/gateway/test_durable_operations.py -q
```

Expected: missing cancellation APIs.

**Step 3: Implement one transaction per operation**

Add a frozen structural result:

```python
@dataclass(frozen=True, slots=True)
class DurableOperatorResult:
    ok: bool
    code: str
    request_id: str
    request_status: str | None = None
    action_status: str | None = None
```

Use `BEGIN IMMEDIATE` and SQL `WHERE status = ... AND execution_claim IS NULL`
compare-and-set conditions. Do not call a handler. Return stable codes:
`not_found`, `already_terminal`, `execution_in_progress`,
`concurrent_update`, or `cancelled`.

**Step 4: Verify GREEN and runner concurrency**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_operations.py \
  tests/gateway/test_durable_action_runner.py \
  tests/gateway/test_durable_action_e2e.py -q
```

**Step 5: Commit**

```bash
git add gateway/durable_operations.py tests/gateway/test_durable_operations.py
git commit -m "feat(durable): add safe operator cancellation"
```

### Task 3: Trusted local CLI list, inspect, and cancel

**Files:**
- Create: `hermes_cli/subcommands/durable.py`
- Create: `hermes_cli/durable.py`
- Modify: `hermes_cli/main.py`
- Create: `tests/hermes_cli/test_durable_cli.py`
- Modify: relevant preset/tool-discovery tests under `tests/tools/` and `tests/gateway/`

**Step 1: Write failing parser and command tests**

Use a standalone argparse parser with an injected sentinel to assert the full
nested command tree. Use `capsys` and `json.loads` to prove JSON stdout is clean.
Monkeypatch `builtins.input` to prove destructive operations default to no and
`--yes` bypasses prompting.

```python
def test_json_cancel_requires_yes_without_prompt(parser, monkeypatch, capsys):
    args = parser.parse_args(["durable", "requests", "cancel", "drq_x", "--json"])
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("must not prompt"))
    assert durable_command(args) == EXIT_INVALID
    assert json.loads(capsys.readouterr().out)["code"] == "confirmation_required"
```

Add discovery assertions that no command/function named durable operator
controls appears in model tools, Discord/CLI/cron/ACP/API-server presets, or
slash command registration.

**Step 2: Verify RED**

```bash
./scripts/run_tests.sh tests/hermes_cli/test_durable_cli.py -q
```

**Step 3: Implement the thin CLI**

Register `durable` in `_BUILTIN_SUBCOMMANDS`, import/build its parser in
`main()`, and add a lazy `cmd_durable` adapter. Keep all database behavior in
`gateway.durable_operations`.

Use local exit constants:

```python
EXIT_OK = 0
EXIT_WARNING = 1
EXIT_INVALID = 2
EXIT_NOT_FOUND = 3
EXIT_CONFLICT = 4
EXIT_FAILURE = 5
```

Human output uses `print_header`, `print_info`, `print_warning`, and
`print_error`. JSON output is a bounded raw dict/list with stable `code`.

**Step 4: Verify GREEN and local-only registration**

```bash
./scripts/run_tests.sh \
  tests/hermes_cli/test_durable_cli.py \
  tests/hermes_cli/test_approvals_suggest.py \
  tests/tools/test_durable_request_tool.py \
  tests/tools/test_request_decision_tool.py -q
```

**Step 5: Commit**

```bash
git add hermes_cli/main.py hermes_cli/durable.py hermes_cli/subcommands/durable.py \
  tests/hermes_cli/test_durable_cli.py tests/tools tests/gateway
git commit -m "feat(cli): add durable operator inspection"
```

### Task 4: Additive UNKNOWN reconciliation ledger

**Files:**
- Modify: `hermes_state_common.py`
- Modify: `gateway/durable_operations.py`
- Modify: `gateway/durable_requests.py`
- Create: `tests/gateway/test_durable_reconciliation.py`
- Modify: `tests/gateway/test_durable_actions.py`

**Step 1: Write failing fresh/legacy migration tests**

Cover central schema initialization, direct durable-store initialization,
Phase 4A/B/4C fixtures, a second idempotent open, row preservation, bounded
UTF-8 identity/note values, allowed outcomes, foreign receipts, and unique
concurrent reconciliation.

```python
def test_phase4c_database_upgrades_without_reconciliation_rows(phase4c_db):
    DurableOperationsStore(phase4c_db)
    with sqlite3.connect(phase4c_db) as db:
        assert db.execute(
            "SELECT count(*) FROM durable_action_reconciliations"
        ).fetchone()[0] == 0
```

**Step 2: Verify RED**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_reconciliation.py \
  tests/gateway/test_durable_actions.py -q
```

**Step 3: Add the minimal schema**

Increment `SCHEMA_VERSION` and append this table to
`DURABLE_ACTION_SCHEMA_SQL` through the existing reconciler:

```sql
CREATE TABLE IF NOT EXISTS durable_action_reconciliations (
    reconciliation_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    outcome TEXT NOT NULL,
    operator_identity TEXT NOT NULL CHECK(length(operator_identity) <= 128),
    note TEXT CHECK(note IS NULL OR length(note) <= 1024),
    superseded_by_request_id TEXT,
    created_at REAL NOT NULL,
    FOREIGN KEY(request_id) REFERENCES durable_action_tickets(request_id)
);
```

Do not backfill rows or rewrite action status.

**Step 4: Implement atomic reconciliation and projection**

```python
class DurableActionReconciliationOutcome(str, Enum):
    CONFIRMED_APPLIED = "confirmed_applied"
    CONFIRMED_NOT_APPLIED = "confirmed_not_applied"
    ABANDONED = "abandoned"
    SUPERSEDED = "superseded"
```

In one `BEGIN IMMEDIATE` transaction, require current action status `unknown`,
validate any superseding receipt is different and has the same action type,
then insert once. Derive bounded operator identity from `getpass.getuser()` at
the CLI boundary. Duplicate or non-UNKNOWN reconciliation never changes the
first row.

Extend pruning so linked reconciliation evidence is removed only together with
its retained request/ticket after the normal retention boundary; unreconciled
UNKNOWN remains protected.

**Step 5: Verify GREEN, idempotency, and pruning**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_reconciliation.py \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_requests.py -q
```

**Step 6: Commit**

```bash
git add hermes_state_common.py gateway/durable_operations.py \
  gateway/durable_requests.py tests/gateway/test_durable_reconciliation.py \
  tests/gateway/test_durable_actions.py
git commit -m "feat(durable): add unknown reconciliation ledger"
```

### Task 5: Reconciliation CLI and read-only handler diagnostics

**Files:**
- Modify: `gateway/durable_actions.py`
- Modify: `gateway/durable_operations.py`
- Modify: `cron/jobs.py`
- Modify: `hermes_cli/subcommands/durable.py`
- Modify: `hermes_cli/durable.py`
- Modify: `tests/gateway/test_durable_reconciliation.py`
- Create: `tests/cron/test_durable_cron_diagnostics.py`
- Modify: `tests/hermes_cli/test_durable_cli.py`

**Step 1: Write failing reconciliation CLI tests**

Require a structural preview and explicit acknowledgement. Test every outcome,
bounded optional note, duplicate/non-UNKNOWN rejection, missing superseding
receipt, JSON `--yes`, and zero handler invocations.

**Step 2: Write failing diagnostics tests**

Add a fake diagnostic hook that records whether the store connection is closed.
Test missing handler/version, callback exception-class redaction, and no state
mutation. For `cron.resume`, cover target missing, paused/enabled, fingerprint
match/difference, and absence of prompt/command content.

```python
def test_diagnostic_runs_after_connection_closes(tmp_path):
    detail = DurableOperationsStore(tmp_path / "state.db").inspect_action(
        "drq_unknown", diagnose=True
    )
    assert detail.diagnostic.code == "state_matches"
    assert connection_probe.closed is True
```

**Step 3: Verify RED**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_reconciliation.py \
  tests/cron/test_durable_cron_diagnostics.py \
  tests/hermes_cli/test_durable_cli.py -q
```

**Step 4: Extend the registry source-compatibly**

Add an optional diagnostic registry entry without changing existing handler
call signatures:

```python
DurableActionDiagnosticHandler = Callable[
    [DurableActionExecutionContext], DurableActionDiagnostic
]

def register_durable_action_handler(
    action_type, action_version, *, prepare, execute, diagnose_unknown=None
): ...
```

Validate diagnostic values as bounded structural keys/values. Lookup returns
`None` for unknown type/version. The operator facade loads and closes its row,
then invokes the hook.

**Step 5: Implement `cron.resume` diagnostics and CLI reconcile**

Reuse current job loading, definition fingerprinting, and jobs lock, but never
call `resume_job_if_unchanged` or `_save_jobs_unlocked`. Render only structural
facts. Reconciliation persists the operator decision after confirmation and
does not call the diagnostic automatically.

**Step 6: Verify GREEN and handler non-invocation**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_reconciliation.py \
  tests/cron/test_durable_cron_diagnostics.py \
  tests/cron/test_durable_cron_resume.py \
  tests/hermes_cli/test_durable_cli.py -q
```

**Step 7: Commit**

```bash
git add gateway/durable_actions.py gateway/durable_operations.py cron/jobs.py \
  hermes_cli/durable.py hermes_cli/subcommands/durable.py tests
git commit -m "feat(durable): reconcile unknown actions safely"
```

### Task 6: Durable health projection and snapshot gauges

**Files:**
- Create: `agent/monitoring/durable_health.py`
- Modify: `agent/monitoring/gateway_health_export.py`
- Create: `tests/monitoring/test_durable_health.py`
- Modify: `tests/monitoring/test_cron_health_export.py`

**Step 1: Write failing health snapshot tests**

Seed every request/action/notification/claim/reconciliation state. Assert exact
counts, oldest age, centralized five-minute stale thresholds, bounded labels,
read-only/query-only connection use, and complete absence of canary content,
receipts, actors, provider IDs, payloads, fingerprints, and notes.

```python
def test_snapshot_contains_no_high_cardinality_or_content_labels(tmp_path):
    snapshot = build_durable_health_snapshot(db_path=seeded_db(tmp_path), now=500.0)
    forbidden = " ".join(str(metric.attributes) for metric in snapshot.metrics)
    for canary in ("drq_", "PHASE4D-CANARY", "actor-", "fingerprint-"):
        assert canary not in forbidden
```

**Step 2: Verify RED**

```bash
./scripts/run_tests.sh tests/monitoring/test_durable_health.py -q
```

**Step 3: Implement SQLite-derived gauges**

`build_durable_health_snapshot()` delegates ledger aggregation to the operator
facade and converts it to existing `GatewayMetric` objects. Emit only:

```text
hermes_durable_requests
hermes_durable_actions
hermes_durable_notifications
hermes_durable_stale_claims
hermes_durable_unknown_actions
hermes_durable_reconciliations
hermes_durable_oldest_backlog_seconds
```

Use only approved enum/type labels. Do not add counters or a dependency.

**Step 4: Merge and register every gauge**

Extend `_read_runtime_snapshot()` and the exporter `metric_names` allowlist.
Retain fail-open behavior and log only exception class for snapshot failures.

**Step 5: Verify GREEN and exporter completeness**

```bash
./scripts/run_tests.sh \
  tests/monitoring/test_durable_health.py \
  tests/monitoring/test_cron_health_export.py \
  tests/monitoring/test_gateway_health_export.py -q
```

**Step 6: Commit**

```bash
git add agent/monitoring/durable_health.py agent/monitoring/gateway_health_export.py \
  tests/monitoring
git commit -m "feat(monitoring): expose durable health gauges"
```

### Task 7: Readiness and doctor integration

**Files:**
- Modify: `gateway/readiness.py`
- Modify: `gateway/platforms/api_server.py`
- Modify: `gateway/run.py`
- Modify: `hermes_cli/doctor.py`
- Modify: `hermes_cli/durable.py`
- Modify: `tests/gateway/test_readiness.py`
- Modify: `tests/gateway/test_api_server.py`
- Create or modify: `tests/hermes_cli/test_doctor_durable.py`
- Modify: `tests/hermes_cli/test_durable_cli.py`

**Step 1: Write failing severity tests**

Cover OK, old pending/ready/notification warning, stale recoverable claim,
invalid allowlist/missing registration, enabled feature with missing worker,
unreconciled UNKNOWN critical, reconciled UNKNOWN no longer critical, DB
unavailable/corrupt critical, and historical terminals healthy.

Assert public `/health` performs no durable DB probe while authenticated
`/health/detailed` includes one bounded durable check.

**Step 2: Verify RED**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_readiness.py \
  tests/gateway/test_api_server.py \
  tests/hermes_cli/test_doctor_durable.py \
  tests/hermes_cli/test_durable_cli.py -q
```

**Step 3: Add one shared health result**

Define a frozen structural health result in the operator facade:

```python
@dataclass(frozen=True, slots=True)
class DurableHealthResult:
    severity: str  # ok | warning | critical
    code: str
    counts: tuple[tuple[str, int], ...]
```

The gateway supplies actual `_durable_request_task` and
`_durable_action_task` liveness to detailed readiness. Offline CLI uses runtime
status evidence and reports worker confirmation unavailable rather than
inventing liveness.

**Step 4: Reuse it in all three surfaces**

- Add `checks["durable"]` only to authenticated detailed readiness.
- Add a read-only `◆ Durable operations` section to existing doctor.
- Render the same result from `hermes durable doctor`, including JSON and exit
  0/1/5 for OK/warning/critical or unavailable.

No `--fix` path may reconcile or cancel anything.

**Step 5: Verify GREEN**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_readiness.py \
  tests/gateway/test_api_server.py \
  tests/hermes_cli/test_doctor_durable.py \
  tests/hermes_cli/test_durable_cli.py -q
```

**Step 6: Commit**

```bash
git add gateway/readiness.py gateway/platforms/api_server.py gateway/run.py \
  hermes_cli/doctor.py hermes_cli/durable.py tests
git commit -m "feat(durable): add operator health checks"
```

### Task 8: Migration, privacy, hostile-input, and fault matrix

**Files:**
- Modify: `tests/gateway/test_durable_operations.py`
- Modify: `tests/gateway/test_durable_reconciliation.py`
- Modify: `tests/hermes_cli/test_durable_cli.py`
- Modify: `tests/monitoring/test_durable_health.py`
- Modify: existing Phase 4C migration/privacy/fault tests where coverage belongs

**Step 1: Add release-blocking cross-cutting tests**

Add explicit tests for:

- Phase 4A/B/C upgrade and repeated schema reconciliation;
- database unavailable/locked/corrupt stable results;
- two cancellation operators versus runner claim;
- two reconciliation operators, one immutable winner;
- crash before reconciliation commit versus after commit;
- diagnostic exception and proof of no state mutation;
- retention of unreconciled/reconciled UNKNOWN evidence;
- operator identity/note UTF-8 and length boundaries;
- SQL wildcard/status/type hostile input rejection;
- all privacy canaries across `record.msg`, `record.args`, and
  `record.__dict__`;
- CLI receipt-only output and no raw DB row serialization;
- model/slash/preset discovery absence;
- no provider-specific or Discord component changes.

**Step 2: Run tests and fix only demonstrated gaps**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_operations.py \
  tests/gateway/test_durable_reconciliation.py \
  tests/hermes_cli/test_durable_cli.py \
  tests/monitoring/test_durable_health.py \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_action_runner.py \
  tests/gateway/test_durable_action_notifications.py \
  tests/gateway/test_durable_action_e2e.py -q
```

Expected: all pass with no skipped dependency coverage.

**Step 3: Commit**

```bash
git add tests gateway hermes_cli agent hermes_state_common.py
git commit -m "test(durable): cover phase 4d safety boundaries"
```

### Task 9: Operator documentation and runbook

**Files:**
- Modify: `docs/design/durable-requests.md`
- Create: `docs/operations/durable-operations.md`
- Create: `docs/testing/phase4d-live-acceptance.md`

**Step 1: Document only implemented behavior**

Document command examples, JSON/exit contracts, privacy omissions,
cancellation matrix, UNKNOWN meanings, all reconciliation outcomes, explicit
no-retry warning, health severity, metric names/labels, retention, and rollback.

The live guide must create a disposable UNKNOWN through test-only fault
injection, inspect/diagnose it, reconcile it, prove zero handler invocations,
and verify health/gauges move from unresolved to reconciled.

**Step 2: Verify examples against parser help**

```bash
.venv/bin/python -m hermes_cli.main durable --help
.venv/bin/python -m hermes_cli.main durable requests --help
.venv/bin/python -m hermes_cli.main durable actions --help
git diff --check
```

**Step 3: Commit**

```bash
git add docs
git commit -m "docs(durable): add phase 4d operator runbook"
```

### Task 10: Focused, full, exported-runtime, and live gates

**Files:**
- No source changes unless a gate demonstrates a defect.

**Step 1: Run the complete Phase 4D focused matrix**

```bash
./scripts/run_tests.sh \
  tests/gateway/test_durable_operations.py \
  tests/gateway/test_durable_reconciliation.py \
  tests/hermes_cli/test_durable_cli.py \
  tests/hermes_cli/test_doctor_durable.py \
  tests/monitoring/test_durable_health.py \
  tests/monitoring/test_cron_health_export.py \
  tests/gateway/test_readiness.py \
  tests/gateway/test_api_server.py \
  tests/cron/test_durable_cron_diagnostics.py \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_action_runner.py \
  tests/gateway/test_durable_action_notifications.py \
  tests/gateway/test_durable_action_e2e.py \
  tests/cron/test_durable_cron_resume.py \
  tests/tools/test_durable_cron_approval.py \
  tests/gateway/test_discord_durable_requests.py \
  tests/tools/test_durable_request_tool.py \
  tests/tools/test_request_decision_tool.py -q
```

**Step 2: Run the complete repository suite**

```bash
./scripts/run_tests.sh
```

Record exact pass/fail/skip counts. Investigate failures against the
`fa37ed778b` baseline; do not conceal unrelated environment failures.

**Step 3: Review and verify the branch**

Run the requesting-code-review and verification-before-completion skills.
Inspect `git diff fa37ed778b...HEAD`, `git diff --check`, schema migration,
privacy labels, and absence of Phase 4C.2/4C.3 or provider-specific changes.

**Step 4: Prepare the immutable release**

Use the existing runtime-ops workflow with Python 3.11.16 / SQLite 3.53.1.
Run the focused matrix again inside the exported source, upstream Discord
tests, dependency/import checks, archive/GWS exclusions, database quick checks,
and stock/custom skill permission checks.

**Step 5: Promote and execute live acceptance**

Back up config, service definition, `state.db`, cron execution DB, and jobs.
Activate exactly one gateway. Verify Discord connection, then:

1. Create one test-only UNKNOWN action without a real side effect.
2. List and inspect it locally without raw SQLite.
3. Run its read-only diagnostic.
4. Verify critical health and UNKNOWN gauge.
5. Reconcile it as `abandoned` or the fault fixture's known outcome.
6. Verify no handler invocation, immutable action `unknown`, one reconciliation,
   updated effective classification, and non-critical health.
7. Re-run generic durable request, cron approval, execution approval, clarify,
   and Discord component smoke checks.

**Step 6: Finalize or roll back**

Finalize only after all live checks pass. On any failure, stop the new gateway,
restore the previous runtime pointer/config, start exactly one old gateway, and
leave the failed release intact for diagnosis.

**Step 7: Report**

Report promoted commit/runtime, test counts, service/Discord/SQLite status,
live UNKNOWN reconciliation result, backup/rollback path, GWS exclusion, and
confirmation that `feature/durable-memory-approval-4c2` has not started.
