# Phase 4D live acceptance

This procedure validates UNKNOWN inspection and reconciliation against a
disposable local `state.db`. It does not start a gateway, contact a platform,
or modify an existing Hermes home. Never run the fault-injection step against a
real profile.

## Prepare an isolated home

Run from the repository root with the development virtual environment ready.

```bash
export PHASE4D_PY="$PWD/.venv/bin/python"
export HERMES_HOME="$(mktemp -d /tmp/hermes-phase4d.XXXXXX)"
test -x "$PHASE4D_PY"
case "$HERMES_HOME" in
  /tmp/hermes-phase4d.*) ;;
  *) echo "refusing non-test HERMES_HOME" >&2; exit 1 ;;
esac
printf '{}\n' > "$HERMES_HOME/config.yaml"
printf 'Disposable home: %s\n' "$HERMES_HOME"
```

Keep every command below in this shell. Do not substitute `~/.hermes`, a
profile directory, or a copied production database.

## Inject one test-only UNKNOWN

The following script creates a synthetic action with no external target. It
registers an execution canary, then directly changes only the disposable
fixture to the post-crash UNKNOWN shape. Direct SQL is fault injection for this
test only; it is not an operator recovery procedure.

```bash
REQUEST_ID="$($PHASE4D_PY - <<'PY'
import os
import sqlite3
from pathlib import Path

from gateway.durable_actions import (
    DurableActionOutcome,
    DurableActionStatus,
    PreparedDurableAction,
    register_durable_action_handler,
)
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)

home = Path(os.environ["HERMES_HOME"])
canary = home / "HANDLER_RAN"

def execute(_context):
    canary.write_text("unexpected\n", encoding="utf-8")
    return DurableActionOutcome(DurableActionStatus.SUCCEEDED)

register_durable_action_handler(
    "acceptance.noop",
    1,
    prepare=lambda: None,
    execute=execute,
)
store = DurableRequestStore(home / "state.db")
created = store.create_with_action(
    DurableRequestSpec(
        kind="approval",
        title="Phase 4D acceptance",
        body="Disposable no-op action",
        options=(
            DurableRequestOption("approve", "Approve"),
            DurableRequestOption("reject", "Reject"),
        ),
        ttl_seconds=3600,
        reference="phase4d-live-acceptance",
    ),
    action=PreparedDurableAction(
        action_type="acceptance.noop",
        action_version=1,
        payload_json="{}",
        fingerprint="phase4d-live-acceptance",
        idempotency_key="phase4d-live-acceptance",
    ),
    target=DurableRequestTarget("acceptance", "disposable"),
    responder_mode=DurableResponderMode.ANY_AUTHORIZED,
    expected_actor_id=None,
    origin_type="acceptance",
    origin_id="phase4d-live-acceptance",
)
request_id = created.request.request_id
with sqlite3.connect(home / "state.db") as db:
    db.execute(
        "UPDATE durable_requests SET status='selected', resolved_at=strftime('%s','now') "
        "WHERE request_id=?",
        (request_id,),
    )
    db.execute(
        "UPDATE durable_action_tickets SET status='unknown', "
        "outcome_reason='test_fault_injection', completed_at=strftime('%s','now'), "
        "notification_status='delivered', updated_at=strftime('%s','now') "
        "WHERE request_id=?",
        (request_id,),
    )
assert not canary.exists()
print(request_id)
PY
)"
test -n "$REQUEST_ID"
test ! -e "$HERMES_HOME/HANDLER_RAN"
printf 'Receipt: %s\n' "$REQUEST_ID"
```

The canary must remain absent for the rest of the procedure. Its presence is a
hard failure: stop and retain the disposable directory for investigation.

## Capture the unresolved state

Inspect through the trusted local CLI. The diagnostic is expected to report
`handler_unavailable` because the synthetic test handler is not registered in
this new CLI process; it must not attempt execution.

```bash
$PHASE4D_PY -m hermes_cli.main durable actions inspect \
  "$REQUEST_ID" --diagnose --json
$PHASE4D_PY -m hermes_cli.main durable doctor --json
test ! -e "$HERMES_HOME/HANDLER_RAN"
```

The action inspection must show `status: "unknown"`,
`effective_reconciliation: null`, and diagnostic code
`handler_unavailable`. Doctor must exit 5 and report severity `critical` with
code `unreconciled_unknown`.

Capture the authoritative SQLite gauge snapshot:

```bash
$PHASE4D_PY - <<'PY'
import json
import os
from dataclasses import asdict
from pathlib import Path

from gateway.durable_operations import DurableOperationsStore

store = DurableOperationsStore(Path(os.environ["HERMES_HOME"]) / "state.db")
rows = [
    asdict(row)
    for row in store.collect_health_data()
    if row.name in {
        "hermes_durable_unknown_actions",
        "hermes_durable_reconciliations",
    }
]
print(json.dumps(rows, indent=2, sort_keys=True))
assert any(
    row["name"] == "hermes_durable_unknown_actions"
    and ["reconciled", "false"] in [list(item) for item in row["attributes"]]
    and row["value"] == 1
    for row in rows
)
PY
```

Before the mutation, make an online SQLite backup. This is also the rollback
point for the acceptance run.

```bash
$PHASE4D_PY - <<'PY'
import os
import sqlite3
from pathlib import Path

home = Path(os.environ["HERMES_HOME"])
with sqlite3.connect(home / "state.db") as source:
    with sqlite3.connect(home / "state.before-reconcile.db") as backup:
        source.backup(backup)
PY
```

## Reconcile without retrying

Choose the test-only conclusion `abandoned`. Reconciliation records operator
evidence; it does not run or retry the action.

```bash
$PHASE4D_PY -m hermes_cli.main durable actions reconcile "$REQUEST_ID" \
  --outcome abandoned \
  --note phase4d-live-acceptance \
  --yes --json
test ! -e "$HERMES_HOME/HANDLER_RAN"
$PHASE4D_PY -m hermes_cli.main durable actions inspect "$REQUEST_ID" --json
$PHASE4D_PY -m hermes_cli.main durable doctor --json
```

Reconcile must return code `reconciled`. The action remains `unknown`, while
`effective_reconciliation` becomes `abandoned`. Doctor must now exit 0 with
severity `ok`; reconciliation does not claim that the original action ran.

Verify the gauge transition:

```bash
$PHASE4D_PY - <<'PY'
import os
from pathlib import Path

from gateway.durable_operations import DurableOperationsStore

rows = DurableOperationsStore(
    Path(os.environ["HERMES_HOME"]) / "state.db"
).collect_health_data()

def matches(row, name, **labels):
    return row.name == name and dict(row.attributes).items() >= labels.items()

assert any(
    matches(row, "hermes_durable_unknown_actions", reconciled="true")
    and row.value == 1
    for row in rows
)
assert not any(
    matches(row, "hermes_durable_unknown_actions", reconciled="false")
    and row.value
    for row in rows
)
assert any(
    matches(row, "hermes_durable_reconciliations", outcome="abandoned")
    and row.value == 1
    for row in rows
)
PY
test ! -e "$HERMES_HOME/HANDLER_RAN"
```

The periodic OTLP exporter emits these same snapshot rows when configured.
This acceptance uses the local collector so it requires no collector endpoint
and sends no telemetry.

## Roll back and clean up

Restore the backup with SQLite's backup API, then confirm the unresolved state
returns and the canary is still absent:

```bash
$PHASE4D_PY - <<'PY'
import os
import sqlite3
from pathlib import Path

home = Path(os.environ["HERMES_HOME"])
with sqlite3.connect(home / "state.before-reconcile.db") as backup:
    with sqlite3.connect(home / "state.db") as target:
        backup.backup(target)
PY
$PHASE4D_PY -m hermes_cli.main durable doctor --json
test ! -e "$HERMES_HOME/HANDLER_RAN"
```

Doctor must again exit 5 with `unreconciled_unknown`. This proves the backup is
a usable rollback point. The normal production rollback is the same principle:
stop every process using the selected profile, restore a consistent pre-change
SQLite backup, and restart the prior runtime. Never delete reconciliation rows
or edit a live database by hand.

After recording the results, remove only the guarded disposable directory:

```bash
case "$HERMES_HOME" in
  /tmp/hermes-phase4d.*) rm -rf -- "$HERMES_HOME" ;;
  *) echo "refusing cleanup outside disposable home" >&2; exit 1 ;;
esac
unset HERMES_HOME PHASE4D_PY REQUEST_ID
```
