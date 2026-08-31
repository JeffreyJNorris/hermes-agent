# Phase 5A Durable Request Command Responses Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let authorized users resolve durable requests with `/request respond <request-id> <slot>`, including a safe command-only delivery fallback for adapters that explicitly reject native durable controls.

**Architecture:** Extend the existing SQLite durable-request ledger with persisted delivery/response capabilities and a separate command-resolution transaction. Reuse the current gateway command registry, action transition, wake outbox, delivery bridge, and Discord authorization paths; add one narrow adapter method for ambiguity-safe unsplit text delivery.

**Tech Stack:** Python 3.11, SQLite, asyncio, Discord.py, pytest through `scripts/run_tests.sh`, existing Hermes metrics/health projections.

---

### Task 1: Persist command-response capabilities and structural modes

**Files:**
- Modify: `gateway/durable_requests.py`
- Modify: `hermes_state_common.py`
- Modify: `hermes_cli/config_defaults.py`
- Test: `tests/gateway/test_durable_requests.py`
- Test: `tests/gateway/test_durable_request_wake.py`
- Test: `tests/gateway/test_durable_actions.py`

**Step 1: Write failing model, configuration, and migration tests**

Add focused tests proving:

```python
def test_command_responses_default_disabled(): ...
def test_create_snapshots_command_response_permission(): ...
def test_existing_rows_default_to_native_and_command_disabled(): ...
def test_phase4_schema_reconciliation_adds_phase5a_columns(): ...
def test_create_with_action_preserves_command_permission(): ...
```

Assert exact defaults:

```python
DurableRequestDeliveryMode.NATIVE
command_response_allowed is False
resolution_source is None
```

**Step 2: Run the failing tests**

Run:

```bash
scripts/run_tests.sh \
  tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_request_wake.py \
  tests/gateway/test_durable_actions.py
```

Expected: failures for missing enums, fields, configuration, and columns.

**Step 3: Add the minimal persistence model**

In `gateway/durable_requests.py`, add:

```python
class DurableRequestDeliveryMode(str, Enum):
    NATIVE = "native"
    TEXT_COMMAND = "text_command"


class DurableRequestResolutionSource(str, Enum):
    NATIVE = "native"
    COMMAND = "command"
```

Extend `DurableRequest`, `_ValidatedCreate`, and `DurableRequestsConfig` with
typed fields. Add `command_response_allowed: bool = False` to `create()`,
`create_with_action()`, and the `create_durable_request()` facade; callers pass
the already-loaded effective configuration rather than reloading it in the
store.

Add the columns to `_DURABLE_SCHEMA_SQL` and `hermes_state_common.SCHEMA_SQL`:

```sql
delivery_mode TEXT NOT NULL DEFAULT 'native',
command_response_allowed INTEGER NOT NULL DEFAULT 0,
resolution_source TEXT
```

Rename `_DURABLE_WAKE_COLUMN_DDL` to `_DURABLE_COLUMN_DDL` and include the
three new columns so old Phase 4 databases reconcile idempotently.

Load the nested default-disabled configuration:

```python
command_responses = raw.get("command_responses", {})
enabled = is_truthy_value(command_responses.get("enabled", False), default=False)
allow_on_native_delivery = is_truthy_value(
    command_responses.get("allow_on_native_delivery", True), default=True
)
allow_text_delivery_fallback = is_truthy_value(
    command_responses.get("allow_text_delivery_fallback", True), default=True
)
```

Add matching defaults under `hermes_cli/config_defaults.py`.

**Step 4: Run migration and model tests**

Run the Step 2 command. Expected: all pass.

**Step 5: Commit**

```bash
git add gateway/durable_requests.py hermes_state_common.py \
  hermes_cli/config_defaults.py tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_request_wake.py tests/gateway/test_durable_actions.py
git commit -m "feat(durable): persist command response capability"
```

### Task 2: Add the command-resolution transaction

**Files:**
- Modify: `gateway/durable_requests.py`
- Test: `tests/gateway/test_durable_requests.py`
- Test: `tests/gateway/test_durable_actions.py`
- Test: `tests/gateway/test_durable_request_wake.py`

**Step 1: Write failing command-resolution tests**

Cover:

```python
def test_resolve_command_rejects_invalid_id_before_connect(): ...
def test_resolve_command_requires_one_positive_slot(): ...
def test_resolve_command_checks_platform_channel_and_actor(): ...
def test_resolve_command_rejects_disabled_request(): ...
def test_resolve_command_maps_slot_and_records_source(): ...
def test_command_and_native_resolution_race_has_one_winner(): ...
def test_command_resolution_queues_generic_wake(): ...
def test_command_approval_readies_action_atomically(): ...
def test_command_rejection_declines_action_atomically(): ...
```

Use separate structural statuses for wrong channel and wrong actor internally,
plus `COMMAND_DISABLED`; native `resolve()` retains its existing result shape.

**Step 2: Verify failures**

Run the three test files from Task 1. Expected: failures for missing
`resolve_command()` and result statuses.

**Step 3: Extract one shared terminal-write helper**

Move only the option lookup, compare-and-set update, action transition, and
resolved-row load from `DurableRequestStore.resolve()` into a private locked
helper. Parameters include the validated row, selected slot, actor, resolution
source, and current time. Native resolution calls it only after exact message
binding passes.

Add:

```python
def resolve_command(
    self,
    *,
    request_id: str,
    option_slots: tuple[int, ...],
    platform: str,
    channel_id: str,
    actor_id: str | None,
) -> DurableResolveResult:
    ...
```

Validate request format before opening SQLite. Inside one immediate transaction:

1. load and expire;
2. require `OPEN` and `command_response_allowed=1`;
3. compare platform and exact `thread_id or chat_id`;
4. require an actor and apply `INITIATOR` policy;
5. require exactly one positive slot;
6. run the shared terminal helper with `COMMAND` source;
7. commit before returning.

Add the public `resolve_durable_request_command()` facade and export it.

**Step 4: Run focused transaction tests**

Run:

```bash
scripts/run_tests.sh \
  tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_request_wake.py
```

Expected: all pass, including the native/command race.

**Step 5: Commit**

```bash
git add gateway/durable_requests.py tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_actions.py tests/gateway/test_durable_request_wake.py
git commit -m "feat(durable): resolve requests by command"
```

### Task 3: Render and deliver the safe text-command fallback

**Files:**
- Modify: `gateway/durable_requests.py`
- Modify: `gateway/platforms/base.py`
- Modify: `gateway/durable_request_bridge.py`
- Modify: `plugins/platforms/discord/adapter.py`
- Test: `tests/gateway/test_durable_request_bridge.py`
- Test: `tests/gateway/test_durable_request_adapter_contract.py`
- Test: `tests/gateway/test_discord_durable_requests.py`

**Step 1: Write failing delivery tests**

Add tests for:

```python
def test_native_content_shows_alternative_only_when_allowed(): ...
def test_text_command_render_includes_full_receipt_and_every_option(): ...
def test_text_command_render_refuses_transport_overflow(): ...
def test_explicit_native_unsupported_falls_back_once(): ...
def test_native_success_never_sends_text_fallback(): ...
def test_ambiguous_native_failure_never_falls_back(): ...
def test_text_success_requires_and_binds_message_id(): ...
def test_ambiguous_text_send_is_not_retried_after_restart(): ...
def test_text_command_rows_skip_native_view_restore(): ...
def test_discord_text_delivery_is_unsplit_and_disables_mentions(): ...
```

**Step 2: Run tests to verify failure**

Run:

```bash
scripts/run_tests.sh \
  tests/gateway/test_durable_request_bridge.py \
  tests/gateway/test_durable_request_adapter_contract.py \
  tests/gateway/test_discord_durable_requests.py
```

Expected: failures for missing renderer, adapter method, and delivery mode
transition.

**Step 3: Add central rendering and one adapter contract**

Add `render_durable_request_command_content()` in
`gateway/durable_requests.py`. It emits the full title/body/receipt/options and
`/request respond <receipt> <number>` instruction without canonical option IDs.
Extend native rendering with one alternative line only when allowed.

In `BasePlatformAdapter`, add a non-abstract optional method:

```python
async def send_durable_request_text(
    self,
    chat_id: str,
    request: DurableRequest,
    metadata: Optional[Dict[str, Any]] = None,
) -> SendResult:
    return SendResult(success=False, error="Unsupported", error_kind="unsupported")
```

In Discord, resolve the channel before the send, preflight with `utf16_len`,
send once with `AllowedMentions.none()`, return the exact message ID, and map
any exception after `channel.send()` begins to `delivery_unknown`. Do not call
ordinary `send()` or `_send_with_retry()`.

**Step 4: Add the bridge transition**

On only native `error_kind == "unsupported"` and persisted fallback permission:

1. atomically change the claimed row to `TEXT_COMMAND` while preserving claim;
2. call `send_durable_request_text()` once;
3. bind the returned message ID through existing `bind_delivery()`;
4. classify missing ID/exception/ambiguous result as `DELIVERY_UNKNOWN`;
5. classify explicit pre-send failures with existing terminal rules.

Skip `restore_durable_request()` for bound text-command rows. Existing stale
claim recovery remains the only restart behavior for an unbound attempted send.

**Step 5: Run delivery tests**

Run the Step 2 command. Expected: all pass.

**Step 6: Commit**

```bash
git add gateway/durable_requests.py gateway/platforms/base.py \
  gateway/durable_request_bridge.py plugins/platforms/discord/adapter.py \
  tests/gateway/test_durable_request_bridge.py \
  tests/gateway/test_durable_request_adapter_contract.py \
  tests/gateway/test_discord_durable_requests.py
git commit -m "feat(durable): add text command delivery fallback"
```

### Task 4: Parse and route `/request respond`

**Files:**
- Modify: `hermes_cli/commands.py`
- Modify: `gateway/slash_commands.py`
- Modify: `gateway/run.py`
- Test: `tests/gateway/test_slash_access_dispatch.py`
- Test: `tests/gateway/test_interactive_prompt_base.py`
- Test: `tests/gateway/test_durable_request_commands.py`

**Step 1: Add failing parser and precedence tests**

Create `tests/gateway/test_durable_request_commands.py` for:

```python
def test_exact_command_resolves(): ...
def test_invalid_receipt_is_rejected_before_store_lookup(): ...
def test_malformed_respond_is_consumed_as_usage(): ...
def test_slot_zero_labels_and_lists_are_rejected(): ...
def test_wrong_scope_messages_do_not_leak_expected_values(): ...
def test_command_disabled_message_is_bounded(): ...
def test_duplicate_and_terminal_results_map_safely(): ...
def test_approve_route_wins_over_request_parser(): ...
def test_bare_number_approve_and_chat_are_unconsumed(): ...
def test_request_command_dispatches_while_agent_busy(): ...
```

Also assert a strict maximum raw argument length before regex parsing.

**Step 2: Verify failures**

Run:

```bash
scripts/run_tests.sh \
  tests/gateway/test_durable_request_commands.py \
  tests/gateway/test_slash_access_dispatch.py \
  tests/gateway/test_interactive_prompt_base.py
```

Expected: missing command and handler failures.

**Step 3: Register and implement the command handler**

Add one `CommandDef`:

```python
CommandDef(
    "request",
    "Respond to a durable request by receipt",
    "Session",
    args_hint="respond <request-id> <number>",
    gateway_only=True,
    busy_policy="dispatch",
)
```

In `gateway/slash_commands.py`, add a compiled strict parser reusing the exact
request ID token format and a `_handle_request_command()` method. Parse only the
`respond` subcommand in Phase 5A. Convert the slot to a one-item tuple and call
`resolve_durable_request_command()` using actual source platform, exact
`thread_id or chat_id`, and actor ID.

Map structural results to the specified bounded messages. Never include raw
command text, actor IDs, canonical option IDs, or expected scope in logs.

**Step 4: Place the route at the required precedence**

In `gateway/run.py`, let existing execution approvals and trusted command
handling retain first refusal, then dispatch recognized `/request respond`
before ephemeral text fallback and clarify interception. Apply the existing
`_check_slash_access()` policy before the handler. Add `request` to the busy
dispatch table so it never reaches the model or queues behind a turn.

Do not add a new invocation rate limiter: this checkout has no general command
rate limiter to reuse. Keep the command length and strict grammar bounds local.

**Step 5: Run command and regression tests**

Run the Step 2 command. Expected: all pass.

**Step 6: Commit**

```bash
git add hermes_cli/commands.py gateway/slash_commands.py gateway/run.py \
  tests/gateway/test_durable_request_commands.py \
  tests/gateway/test_slash_access_dispatch.py \
  tests/gateway/test_interactive_prompt_base.py
git commit -m "feat(gateway): route durable request responses"
```

### Task 5: Verify Discord slash integration and best-effort presentation updates

**Files:**
- Modify: `plugins/platforms/discord/adapter.py`
- Test: `tests/gateway/test_discord_slash_commands.py`
- Test: `tests/gateway/test_discord_slash_auth.py`
- Test: `tests/gateway/test_discord_durable_requests.py`

**Step 1: Write failing integration tests**

Cover:

```python
def test_request_auto_slash_command_is_registered(): ...
def test_request_slash_runs_authorization_before_dispatch(): ...
def test_request_slash_uses_actual_thread_and_actor(): ...
def test_command_success_commits_before_original_message_edit(): ...
def test_missing_or_failed_original_message_edit_does_not_change_result(): ...
```

**Step 2: Run the failing tests**

```bash
scripts/run_tests.sh \
  tests/gateway/test_discord_slash_commands.py \
  tests/gateway/test_discord_slash_auth.py \
  tests/gateway/test_discord_durable_requests.py
```

**Step 3: Reuse automatic slash registration**

Do not create an `app_commands.Group`. Let `_build_auto_slash_command()` expose
the registered gateway command as `/request` with bounded free-text args and
route it through `_run_simple_slash()`, which already authenticates and builds
actual source metadata.

Add only the smallest helper needed to best-effort fetch/edit the bound request
message after the core handler returns success. Disable its controls or replace
the view when found. Catch lookup/edit exceptions after resolution and log only
request receipt plus exception class.

**Step 4: Run Discord tests**

Run the Step 2 command. Expected: all pass.

**Step 5: Commit**

```bash
git add plugins/platforms/discord/adapter.py \
  tests/gateway/test_discord_slash_commands.py \
  tests/gateway/test_discord_slash_auth.py \
  tests/gateway/test_discord_durable_requests.py
git commit -m "feat(discord): integrate durable request commands"
```

### Task 6: Add structural operations and privacy visibility

**Files:**
- Modify: `gateway/durable_operations.py`
- Modify: `agent/monitoring/durable_health.py`
- Modify: `agent/monitoring/gateway_health_export.py`
- Test: `tests/gateway/test_durable_operations.py`
- Test: `tests/monitoring/test_durable_health.py`
- Test: `tests/hermes_cli/test_doctor_durable.py`
- Test: `tests/gateway/test_durable_request_commands.py`

**Step 1: Write failing operations/privacy tests**

Test that:

- operator request detail exposes delivery mode and resolution source only;
- health reports text-command pending backlog structurally;
- command attempts use bounded result labels;
- no request receipt is a metric label;
- logs omit raw commands, actor IDs, option labels, payloads, and provider data;
- user errors never reveal expected actor/channel/message/action/session.

**Step 2: Verify failures**

```bash
scripts/run_tests.sh \
  tests/gateway/test_durable_operations.py \
  tests/monitoring/test_durable_health.py \
  tests/hermes_cli/test_doctor_durable.py \
  tests/gateway/test_durable_request_commands.py
```

**Step 3: Add bounded structural fields**

Extend `DurableRequestSummary`/`Detail` with delivery mode and optional
resolution source. Add a `text_command_delivery` backlog type derived from
`status='pending_delivery' AND delivery_mode='text_command'`. Reuse the existing
bounded label helper; never include request ID, option, actor, or content in
metric attributes.

Count command outcomes at the gateway service boundary by
`DurableResolveStatus.value`; do not persist a new event table for counters.

**Step 4: Run tests and commit**

Run the Step 2 command, then:

```bash
git add gateway/durable_operations.py agent/monitoring/durable_health.py \
  agent/monitoring/gateway_health_export.py \
  tests/gateway/test_durable_operations.py \
  tests/monitoring/test_durable_health.py \
  tests/hermes_cli/test_doctor_durable.py \
  tests/gateway/test_durable_request_commands.py
git commit -m "feat(durable): expose command response health"
```

### Task 7: Complete migration, fault, and regression verification

**Files:**
- Modify: `docs/configuration.md` or the existing durable-request configuration documentation discovered during implementation
- Create: `docs/runbooks/phase5a-durable-request-command-responses.md`
- Test: existing Phase 5A-focused files only

**Step 1: Add remaining release-gate and fault tests**

Ensure coverage for interruption:

- before native send;
- after structural unsupported classification;
- before and after text send;
- before message-ID binding;
- after command-resolution commit and before message edit.

Each test asserts authoritative state, retry safety, duplicate-presentation risk,
and whether the outcome is unknown.

**Step 2: Run the complete focused suite**

```bash
scripts/run_tests.sh \
  tests/gateway/test_durable_requests.py \
  tests/gateway/test_durable_request_bridge.py \
  tests/gateway/test_durable_request_adapter_contract.py \
  tests/gateway/test_discord_durable_requests.py \
  tests/gateway/test_durable_request_commands.py \
  tests/gateway/test_durable_actions.py \
  tests/gateway/test_durable_request_wake.py \
  tests/gateway/test_slash_access_dispatch.py \
  tests/gateway/test_discord_slash_commands.py \
  tests/gateway/test_discord_slash_auth.py \
  tests/gateway/test_interactive_prompt_base.py \
  tests/gateway/test_durable_operations.py \
  tests/monitoring/test_durable_health.py \
  tests/hermes_cli/test_doctor_durable.py
```

Expected: zero failures.

**Step 3: Run compile and diff checks**

```bash
python3 -m compileall -q gateway hermes_cli plugins/platforms/discord agent/monitoring
git diff --check
git status --short
```

Expected: compile and diff checks succeed; only intentional files are changed.

**Step 4: Write the runbook**

Document configuration, migration defaults, native and fallback live checks,
wrong-channel/actor checks, ambiguous-send expectations, inspection commands,
and rollback to the prior immutable release. State explicitly that Phase 5B
slot lists are unsupported.

**Step 5: Commit**

```bash
git add docs tests
git commit -m "docs(durable): add command response runbook"
```

**Step 6: Final verification**

Re-run the complete focused suite and compile/diff checks after the documentation
commit. Record exact test counts and any pre-existing warnings. Do not promote,
push, or open a pull request without separate user authorization.
