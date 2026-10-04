# Conversation-local durable approvals implementation plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Put live Discord durable requests and staged approvals in their originating channel/thread, keep cron requests in the configured inbox, and render memory reviews as complete readable text.

**Architecture:** Extend the existing trusted durable-request origin with a validated physical Discord delivery target. Use one narrow target selector at all three creation call sites; persist the chosen target in the existing request row. Replace only the memory review body renderer; leave Discord controls, storage schema, action payloads, and terminal transactions unchanged.

**Tech Stack:** Python, SQLite, Discord adapter, pytest via `scripts/run_tests.sh`.

---

Baseline: `scripts/run_tests.sh tests/tools/test_durable_request_tool.py tests/tools/test_durable_memory_approval.py tests/tools/test_durable_skill_approval.py -q` passed 110 tests on the branch before implementation.

### Task 1: Capture a trusted conversation delivery target

**Files:**
- Modify: `tools/durable_request_tool.py` (`_Origin`, `_current_origin`)
- Test: `tests/tools/test_durable_request_tool.py`

**Step 1: Write failing tests.** Bind a live Discord session with numeric `chat_id` and matching `thread_id`, then assert the origin target is that physical thread. Repeat for a parent channel with no thread. Assert missing, nonnumeric, zero, or mismatched thread scope has no usable target. Assert cron still has no origin target. Existing status/cancel tests must not require a delivery target.

**Step 2: Run the focused file and observe failure.** `scripts/run_tests.sh tests/tools/test_durable_request_tool.py -q`.

**Step 3: Implement the minimum.** Add `delivery_target: DurableRequestTarget | None = None` to `_Origin`. In `_current_origin`, after existing actor/session validation, read `HERMES_SESSION_CHAT_ID` and `HERMES_SESSION_THREAD_ID` from `get_session_env`. Accept positive decimal Discord IDs; when thread ID is present, require it to equal the physical chat ID. Construct `DurableRequestTarget("discord", chat_id, thread_id_or_none)` only from those trusted values. Add one `_delivery_target_for_origin(origin, configured_inbox)` helper: return the origin target for `session`, the configured inbox for `cron`, and never silently substitute the inbox for a session missing its target.

**Step 4: Run the focused file and require green.**

**Step 5: Commit.** `git add tools/durable_request_tool.py tests/tools/test_durable_request_tool.py && git commit -m "feat(durable): bind live requests to trusted Discord origin"`.

### Task 2: Route generic durable requests by origin

**Files:**
- Modify: `tools/durable_request_tool.py` (`post` creation only)
- Test: `tests/tools/test_durable_request_tool.py`

**Step 1: Write failing tests.** A session-created request in a parent channel targets that channel; one created in a thread targets that thread; a cron-created request still targets the configured inbox; a session with missing/invalid channel scope returns a normal tool error and creates no row. Verify the model tool schema still accepts no destination fields.

**Step 2: Run focused tests and observe failure.**

**Step 3: Implement.** On `post` only, select the target with `_delivery_target_for_origin`. If it is `None`, return a bounded unavailable error before creation. Leave `status` and `cancel` origin authorization unchanged. Pass the chosen target into `create_durable_request` and change nothing in SQLite or the adapter.

**Step 4: Run focused tests and require green.**

**Step 5: Commit.** `git add tools/durable_request_tool.py tests/tools/test_durable_request_tool.py && git commit -m "feat(durable): deliver live requests in the originating conversation"`.

### Task 3: Route trusted staged-write approvals by origin

**Files:**
- Modify: `tools/write_approval.py` (`create_memory_durable_approval`, `create_skill_durable_approval`)
- Test: `tests/tools/test_durable_memory_approval.py`
- Test: `tests/tools/test_durable_skill_approval.py`

**Step 1: Write failing tests.** Both memory and skill approvals use the bound live channel/thread, while cron origins retain the inbox. Missing live channel scope leaves the staged record on manual review and creates no request/action ticket. Existing linked action idempotency and actor-binding tests remain valid. Update test session fixtures to bind actual numeric channel IDs.

**Step 2: Run both focused files and observe failure.**

**Step 3: Implement.** Import the shared target selector next to `_current_origin` in each creator. Resolve the target before `create_with_action`; if absent, return the existing `pending_manual_review`/`inbox_unavailable` shape. Pass the target in place of `request_config.target`. Do not alter action preparation, fingerprints, payload JSON, or execution handlers.

**Step 4: Run both focused files and require green.**

**Step 5: Commit.** `git add tools/write_approval.py tests/tools/test_durable_memory_approval.py tests/tools/test_durable_skill_approval.py && git commit -m "feat(approval): keep live staged reviews in their conversation"`.

### Task 4: Render a complete readable memory review

**Files:**
- Modify: `tools/write_approval.py` (`_memory_review`)
- Test: `tests/tools/test_durable_memory_approval.py`

**Step 1: Write failing tests.** Assert an add request body names `MEMORY.md` or `USER.md`, says `Add`, includes every content line, and does not start with `{`. Cover replace and remove as whole-entry operations with the complete pinned prior entry; cover ordered batch operations and long text without ellipsis. An unreviewable or oversized body must keep the staged record on manual review instead of truncating. No action payload or option ID appears in the body.

**Step 2: Run the focused file and observe failure.**

**Step 3: Implement.** Build a list of deterministic plain-text lines from the normalized staged payload: target heading, one-based operation heading, full prior entry for replace/remove, and full new entry for add/replace. Preserve embedded newlines. Reject missing pinned prior content rather than inventing a summary. Let existing bounded request-spec validation route oversized review to manual review. Never mutate the pending payload in the renderer.

**Step 4: Run the focused file and require green.**

**Step 5: Commit.** `git add tools/write_approval.py tests/tools/test_durable_memory_approval.py && git commit -m "fix(memory): show readable complete durable review"`.

### Task 5: Regression, docs, and live gate

**Files:**
- Modify: `docs/design/durable-requests.md`
- Modify: `docs/runbooks/phase5a-durable-request-command-responses.md`
- Test: `tests/gateway/test_durable_requests.py`
- Test: `tests/gateway/test_durable_request_commands.py`
- Test: `tests/gateway/test_discord_durable_requests.py`

**Step 1: Add/update tests for persisted exact target after config change/restart, wrong-channel command rejection, native bound-message enforcement, and no fallback presentation after ambiguous send.** Reuse current fixtures rather than adding a generic UI layer.

**Step 2: Run the six focused request/approval test files, then the affected gateway request/Discord files.** Use `scripts/run_tests.sh` and require zero failures.

**Step 3: Update docs with the conversation-versus-cron routing rule, missing-channel manual rail, and complete review presentation.** Historical rows remain at their persisted target.

**Step 4: Check `git diff --check`, compile/import affected modules, inspect the public diff for secrets, and get independent security review before deployment.**

**Step 5: Commit docs/tests, then deploy to an immutable release only after review.** Recreate a harmless memory canary in a live Discord thread and verify message location/body before approving; separately verify a cron request stays in `#approvals`. Test staged skill review location. Do not promote to fork `main` until live acceptance and rollback checks pass.
