# Phase 5B–5D interaction requirements

Status: Phase 5B implemented and verified locally; see its
[implementation record](../plans/2026-10-02-phase5b-atomic-multi-select.md).
Phase 5C and 5D requirements are captured; implementation plans remain pending.
Production remains on Phase 5A.

Source: the Phase 5 implementation specification supplied for this project.
Phase 5A has its own [design](../plans/2026-08-31-phase5a-durable-request-command-responses-design.md),
[plan](../plans/2026-08-31-phase5a-durable-request-command-responses.md),
and [runbook](../runbooks/phase5a-durable-request-command-responses.md).
This document records the remaining requirements without deciding their
implementation tasks, commit boundaries, or upstream reconciliation strategy.

## Sequence and release boundaries

```text
Promoted Phase 4D, 4C.2, 4C.3 → Phase 5A → 5B → 5C → 5D
```

Develop 5B, 5C, and 5D on separate branches, each based on its previously
promoted predecessor. Each must be independently testable, promotable, and
reversible. Persisted schema changes must be additive and existing active and
terminal records must remain readable. Suggested branch names are
`feature/interactive-multi-select`, `feature/staged-skill-review-workflow`,
and `feature/structured-input-modals`.

### Invariants shared by all three phases

- The registry or SQLite ledger is authoritative. Provider messages and
  controls only present that state. Commit a resolution or transition before
  editing a provider message. A failed edit cannot reopen anything, and
  provider messages are never scanned to infer business state.
- Run general adapter authorization first. Then validate generated identifier,
  expected platform, actual delivery channel, actual actor, responder policy,
  deadline, lifecycle state, server-side option/action mapping, and current
  workflow revision where applicable. Native interactions also validate the
  exact bound provider message.
- The model cannot choose platform, channel/thread, provider message or actor
  IDs, responder policy, prompt/request/action/workflow IDs, provider component
  JSON or custom IDs, handler type/version, durable action payload, or
  execution/reconciliation claims.
- Provider IDs carry generated tokens and short slots only: `drm:<request-token>`
  for durable multi-select, `ipm:<prompt-token>` for ephemeral multi-select,
  `sw:<workflow-token>:<revision>:<action-slot>` for workflow controls,
  `sf:<prompt-token>:open` for a form opener, and
  `sf:<prompt-token>:<nonce>` for its modal. Field keys, canonical option IDs,
  action types, filenames, and application values stay server-side.
- Ordinary logs may include generated receipt, platform, transition, resolution
  source, bounded reason, handler type, exception class, count, and duration.
  They must exclude prompt bodies, labels, submitted values, skill diffs,
  memory content, action payload JSON, fingerprints, actor/session IDs,
  provider payloads, and raw command or modal text. Metrics must use bounded
  structural labels, never receipts or user content.
- Use explicit typed models and narrow adapter methods. Do not add arbitrary
  component trees, provider JSON, model-authored layouts or workflow steps,
  persisted Python callbacks, dynamic handler imports, generic renderers, or
  a provider-independent DOM.
- Preserve existing single-choice decisions, native and text prompts, durable
  requests and wakes, binary trusted actions, approval mechanisms, clarify,
  pickers, Discord REST tools, cron semantics, and `send_message_tool.py`.

## Phase 5B — atomic multi-select

### Scope and meaning

A user submits a **complete set** of options once. The registry validates the
set and records one terminal response; it stores no partial selection, toggle
state, or separate submit step. Add ephemeral prompts (sync and async), Discord
native select, strict ephemeral text fallback, generic durable requests,
restart-safe status/wake, request-ID command selection, and model-facing
ephemeral and durable creation. Keep trusted durable actions binary and
single-select. Quorum, ranking, ordered preferences, and partial submissions
are outside this phase.

### Models and validation

Keep existing single-choice types unchanged. Add separate typed models for a
multi-select spec (`InteractiveMultiSelectSpec`), registered prompt
(`InteractiveMultiSelectPrompt`), response (`MultiSelectResponse`), outcome
(`MultiSelectOutcome`), handle, and typed caller request/result
(`MultiDecisionOption`, `MultiDecisionRequest`, `MultiDecisionResult`). The
spec contains kind, title, body, options,
`min_selections`, `max_selections`, and timeout. The response carries canonical
option IDs and trusted platform/chat/thread/message/actor metadata. Caller
values stay behind server-generated IDs such as `decision_1`; they never enter
provider payloads. Order selected canonical IDs by original option ordinal,
regardless of provider submission order.

Use named limits: 2–10 options, `min_selections >= 1`, and
`min <= max <= min(option_count, 10)`. Reject duplicate option IDs, empty
labels, unknown styles, invalid bounds, duplicate or unknown submitted slots,
sets outside bounds, and any presentation that would omit an option. Canonical
labels and IDs remain intact; provider display labels alone may be truncated.

### Ephemeral resolution and delivery

Provide narrow `register_multi_select_prompt`, `resolve_multi_select_prompt`,
and `wait_for_multi_select` facades, plus sync callback and async gateway
request paths (`_interactive_multi_select_callback_sync` and
`request_multi_select_async`). Trusted active-request context supplies the
scope. Never run the sync callback on the gateway loop. Cancellation,
session/platform cleanup, and shutdown unblock only affected waiters.

Under the registry lock: find the active entry or tombstone, recheck monotonic
deadline, delivery mode/source, platform, actual channel, native message ID,
actor, slot uniqueness/mapping, and count bounds; then record one canonically
ordered terminal response, remove indexes, create a tombstone, and signal the
waiter. Invoke any synchronous resolver **outside** the lock. Late attempts
cannot resolve.

For the one active ephemeral text-fallback prompt in a session, render every
numbered option and the allowed selection count. Accept one centrally chosen,
strict numeric-set grammar (for example `1,3` and `1, 3`); reject prose,
ranges, `all`, `none`, and duplicates. Consume only recognized numeric-set
syntax so unrelated text reaches its existing route.

Discord uses one native select control with an opaque prompt-token custom ID
and numeric slot option values. Full labels remain in message content;
provider option labels may be display-truncated. `_component_check_auth()`
runs before registry resolution with actual channel/message/actor IDs.
Resolve before disabling controls; edit failure does not change the result.
The registry owns timeout.

### Durable contract

Add `selection_mode TEXT NOT NULL DEFAULT 'single'`,
`min_selections INTEGER NOT NULL DEFAULT 1`,
`max_selections INTEGER NOT NULL DEFAULT 1`, and
`selected_option_ids_json TEXT`. Historical rows remain single-select. Keep
`selected_option_id` for single responses; multi-select writes a canonical,
duplicate-free JSON array in the terminal transaction and leaves the scalar
null. No trusted action ticket may link to a multi-select request.

Extend Phase 5A command syntax to a strict slot list such as
`/request respond drq_... 1,3,4`. Single requests still require exactly one
slot; multi requests enforce persisted bounds and reject duplicates, labels,
or canonical IDs. Native and command races yield one terminal set. Existing
platform/channel/actor/receipt checks apply.

Extend the existing `durable_request` post action with `selection_mode`,
`min_selections`, and `max_selections`; default mode is single with min=max=1
and reject conflicting bounds. Multi status returns one-based
`selected_indices` and corresponding `selected_labels`; keep the existing
single-select response shape. A multi terminal wake may show bounded labels
in canonical order, without option IDs or executable actions.
Trusted action creation must still require the canonical single-select
`approve`/`reject` options.

Add model-facing `request_multi_select` only where the gateway callback is
available. It accepts title, question, options, and selection bounds; returns
selected indices/labels or normal lifecycle statuses (`expired`, `cancelled`,
`unsupported`, `delivery_failed`). It is never parallel, does not replace
clarify, and grants no application mutation authority.

### 5B promotion gate

- Native and text ephemeral prompts resolve one complete set exactly once.
- Durable native multi-select survives restart; command selection resolves by
  request ID; wake/status preserve canonical option order.
- Duplicate, unknown, and out-of-range slots fail; bounds are server-side;
  provider ordering cannot change the result.
- Trusted actions cannot attach to multi-select; existing single-select
  shapes and behavior remain intact; no partial selection is persisted.
- Metrics count submissions and invalid sets, health exposes active/stuck
  prompts, and no selection content becomes a metric label.

## Phase 5C — staged skill-change review workflow v1

### Scope and source

Implement exactly one durable workflow for a trusted staged personal-skill
write: summary, every full-diff review page in sequence, then final approve or
reject, with back navigation and restart-safe progress. Link it exactly to the
existing `skill.apply_pending` action ticket and a binary approve/reject
durable request. The model cannot create a workflow. No generic workflow API,
graph, loops, branching, multi-party review, partial file approval, or other
action type.

### Identity, rendering, and storage

Add `durable_skill_review_workflows` with generated `swf_` ID, unique linked
request ID, version, status, step, page/page count, revision,
`rendered_revision`, review digest, renderer version, timestamps, and terminal
reason. Do not duplicate skill content, diff, action payload, message ID, or
responder scope in this row. Those stay in the staged manifest, request, and
action records. Keep active workflows out of pruning.

Derive pages from the already staged and fingerprinted manifest. The digest
covers manifest fingerprint, renderer version, deterministic pagination policy,
and ordered rendered pages. Use named limits of at most 20 pages, 50 KiB total,
and 2.5 KiB per page, adjusted only for verified provider-safe budgets. Split
deterministically at line boundaries, keep file headers with their first hunk
where possible, preserve file order and **every** changed line, and never
truncate or substitute a summary. Oversized changes use the existing manual
review path.

The workflow states are active, approved, rejected, expired, cancelled, and
invalidated; steps are summary, review(page N), and confirm. Summary permits
next/reject. Review permits next/back/reject. Confirm permits
back/approve/reject. Approval is impossible before confirm. Every successful
transition increments revision. Native controls include that revision and
stale controls return `STALE_STEP` without mutation.

### Authority and transitions

Create the staged request, trusted action, and workflow atomically with stable
idempotency, then deliver the presentation. A narrow transition service
(`transition_skill_review_workflow`) accepts workflow ID, expected revision
for native controls, action,
platform/channel/message/actor, and source. Native source validates exact
bound request message; command source maps a request ID to the workflow and
acts on its current step. Both enforce existing actor policy and exact channel.

Before every forward transition and final approval, reload the staged
manifest, verify its fingerprint, recompute review digest, and compare it to
the stored digest. Promptly validate on back navigation too. A mismatch
atomically invalidates the workflow and action and makes the request terminally
unavailable with reason `review_subject_changed`.

Final approval is one transaction: validate workflow/step/revision/digest,
open request and waiting action; resolve canonical `approve`, make action
ready, mark workflow approved, increment revision, create action work, commit.
Rejection from any active step atomically resolves canonical `reject`, rejects
the action, and marks the workflow rejected. Adapter callbacks never perform
the skill mutation. Concurrent manual operator and Discord decisions use
compare-and-set; exactly one wins. Manual approval/rejection closes or
supersedes the workflow and request, preserves history, and uses the same
durable action handler without requiring UI navigation.

Commit each transition before provider rendering. The presentation worker
edits the authoritative current step and advances `rendered_revision` only
after success. Edit failure leaves `rendered_revision < revision`; retry and
health reporting must expose this backlog. Startup reconciliation rechecks
deadline/digest, restores the current native view, renders the current step,
and applies existing missing-message delivery semantics.

Discord uses one dedicated review view, opaque action slots, complete current
step content, disabled mentions, general authorization first, actual metadata,
and transition-before-edit. Summary shows change metadata and Review/Reject;
pages show complete diff with Back/Next/Reject; confirmation shows
Back/Approve/Reject.

Extend the request command group with `/request step <request-id> next|back|approve|reject`.
It accepts only the linked request ID, never a raw workflow ID or revision.
Commands cannot skip steps; approve works only at confirm. They render the
new authoritative step, possibly as a new reply if text delivery cannot edit
the original message. Historical presentations do not become authoritative.
Trusted config `skills.durable_review_workflow: true` selects this workflow;
when disabled, existing one-shot or manual review remains available.

### 5C promotion gate

- Summary, sequential complete diff pages, back navigation, final-only
  approval, and rejection from every step work.
- Restart restores the exact step; old-revision controls and command shortcuts
  cannot change or skip state.
- A changed manifest invalidates approval; oversized reviews remain manual.
- Final approval executes the linked action once; provider edit failure cannot
  lose a committed transition.
- Manual operator action safely supersedes the workflow, and generic durable
  requests cannot create workflows.
- Operator inspection shows linked workflow state, stale rendering, and trusted
  cancellation; no generic engine or model-facing workflow tool is introduced.

## Phase 5D — ephemeral structured input and Discord modals

### Scope and data contract

Add a provider-neutral, one-shot structured-input primitive for live
conversations. Initial fields are `short_text` and `long_text` only. No
passwords, secrets, uploads, date/numeric widgets, checkboxes, selects, rich
text, conditional fields, or arbitrary validation scripts. Arbitrary submitted
text has **no durable storage** in this release.

Add typed field spec (`key`, `label`, kind, required, min/max length,
placeholder), prompt spec (kind, title, body, fields, timeout), ordered
key/value submission with trusted scope metadata, and outcomes `submitted`,
`expired`, `cancelled`, `unsupported`, and `delivery_failed`. Caller field keys
are application values; provider field IDs contain only numeric slots.
The public types are `StructuredInputFieldSpec`, `StructuredInputSpec`,
`StructuredFieldValue`, `StructuredInputSubmission`, and
`StructuredInputOutcome`.

Use named policy limits: 1–5 fields; key pattern
`[a-z][a-z0-9_]{0,63}`; labels/placeholders at most 100 characters; short
text default max 200 and absolute max 500; long text default max 1,000 and
absolute max 2,000; at most 4,000 submitted characters total. Reject duplicate
keys, invalid kinds or lengths, content that cannot fit, missing required
fields, unknown/duplicate slots, oversize values, NUL, and disallowed control
characters. Normalize CRLF/CR to LF. Preserve leading/trailing whitespace;
only use `value.strip() == ""` to decide whether a required field is empty.
Return values in field-spec order.

### Nonce and registry lifecycle

After prompt delivery and exact message binding, an authorized actor clicks
Open form. The registry issues a cryptographically random nonce bound to the
prompt, actor, and original message; its lifetime is the lesser of remaining
prompt lifetime and five minutes. A new nonce invalidates the old one. The
prompt remains delivered while a form is open; no separate public workflow
state is needed.

The opener must validate active/delivered state, monotonic deadline, platform,
actual channel, bound message, expected actor, and absence of terminal result.
The narrow APIs are `issue_structured_input_form` and
`submit_structured_input`. Submission must additionally validate nonce
existence, expiry, and actor binding, then field slots/content. Under the lock,
store one terminal outcome,
remove active indexes, clear nonce, make a privacy-safe tombstone, and signal
the waiter. Invoke the synchronous resolver outside the lock. A reused or
replaced nonce returns an accurate stale result; resolver exceptions cannot
reopen the form.

Tombstones retain only prompt ID, terminal status, and terminal timestamp:
never values, keys, labels, body, resolver, nonce, actor, or provider payload.
Once the waiting caller receives the result, the registry retains no submitted
values.

### Adapter, gateway, and tool

Add an optional `send_structured_input` adapter method whose default is
explicit unsupported. Success means a delivered opener message with provider
message ID. The adapter does not receive an application callback or mutate
application state. Keep the complete question in normal message content.
There is no text fallback.

Discord sends one mention-disabled Open form button with
`sf:<prompt-token>:open`. General component authorization runs first; the
registry issues nonce using actual channel/message/actor before Discord shows
the modal. The modal ID is `sf:<prompt-token>:<nonce>` and field IDs are
`f:1`, `f:2`, etc. Do not silently omit fields or place caller keys/values in
custom IDs or logs. Authenticate modal submission, resolve in the registry,
acknowledge, then best-effort edit/disable the original opener. Edit failure
cannot erase a successful submission or invoke the resolver twice.

Gateway sync callback and async request paths
(`_structured_input_callback_sync` and `request_structured_input_async`)
share delivery and derive scope from the trusted active request. Bind the
initiating actor in the first release. Forbid sync callback on the gateway
loop. Session cleanup cancels
forms, adapter disconnect cancels platform forms, and shutdown cancels all.
Multiple native forms can coexist because opener, nonce, and actor are exact.
Unsupported delivery returns `unsupported`; ambiguous attempted delivery
returns `delivery_failed`; success requires exact message binding.

Add model-facing `request_structured_input` only to supported active gateway
conversations, not cron. It accepts title, question, and bounded field specs;
returns a JSON string with caller-keyed values or normal lifecycle status.
It is never parallel and accepts no timeout, scope, actor, or provider
configuration. Its description prohibits credentials, tokens, recovery codes,
and private keys. Do not claim Hermes can detect every secret entered into
caller-controlled text. Callers must minimize personal data and use
display-safe prompt text. Session history may retain the tool result under
normal Hermes behavior; ordinary logs and metrics must not contain values.

### 5D promotion gate

- A live Discord form opens and submits once; wrong actors cannot open or
  submit; reopening invalidates the previous modal.
- Prompt deadline, nonce deadline, cancellation, cleanup, and restart-free
  ephemeral lifecycle behave as specified. Stale submissions cannot overwrite
  a newer outcome.
- Required/optional fields, slot validation, line endings, content preservation,
  per-field/total limits, and ordering are enforced server-side.
- Submission commits before opener edit; edit failure leaves the result
  intact. No submitted values remain in tombstones, logs, or metrics.
- Tool discovery is gateway-only, never parallel, absent from cron, and does
  not return prompt/provider IDs. Existing interaction paths remain intact.
- No durable arbitrary-text persistence is introduced.

## Fault boundaries and release evidence

Each later implementation plan must cover the fault boundaries named in the
source specification, including set arrival/terminal commit/UI disable/wake
and native-command races (5B); creation, transition commit, provider edit,
approval/action claim, reconciliation, and restart at every step (5C); opener
delivery/binding, nonce issue/modal open, submission commit/edit, and cleanup
(5D). At each boundary, state whether retry is safe, presentation can
duplicate, user action can repeat, outcome becomes unknown, or operator help
is needed. Phase-specific release tests and live promotion checks above are
required before advancing to the next branch.

## Deliberately deferred beyond Phase 5

Ranked voting, toggle drafts, quorum/multi-party approval, arbitrary or
model-authored workflow graphs, password/secret collection, form uploads,
generic execution from form values, plugin component definitions, generic UI
trees, cross-host workflow coordination, and automatic retry of ambiguous
application mutations remain out of scope.

Durable structured text needs a separate approved storage/retention decision
covering plaintext at rest, existing encryption, keys, deletion, backups,
operator inspection, audit, wake content, origin-scoped retrieval, and profile
isolation. Any later implementation uses a separate bounded-lifetime table,
explicit configuration, and a trusted typed handler for values used by
mutations. Do not put arbitrary text in `durable_requests` or structural wakes,
attach it directly to trusted action execution, or invent new encryption when
the repository already has an accepted facility. Without an accepted policy,
durable structured input remains unsupported.
