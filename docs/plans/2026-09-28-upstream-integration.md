# Phase 4/5A Upstream Integration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring the audited Phase 4 through Phase 5A feature stack onto the fork's current upstream `main`, preserving upstream behavior and the durable security invariants.

**Architecture:** Merge the published, sanitized feature branch into a new branch based on `fork/main`. Resolve conflicts in the current upstream modules, then verify the combined gateway, Discord, durable ledger, cron, trusted actions, and operator paths. Keep `fork/main` and the published feature branch intact until the integration passes.

**Tech Stack:** Git, Python 3.14, SQLite, Hermes gateway and Discord adapter, the repository's pinned `scripts/run_tests.sh` environment.

---

### Task 1: Establish baseline and merge state

**Files:** No source changes expected.

1. In an isolated worktree at `fork/main`, confirm a clean `git status` and the exact fork and feature heads.
2. Run `scripts/run_tests.sh tests/gateway/test_discord_slash_commands.py tests/gateway/test_interactive_prompt_base.py` as the upstream baseline. Record any existing failures before integration.
3. Merge `feature/durable-request-command-response` with `git merge --no-commit --no-ff`. Do not choose a whole-file side for a conflicted file merely to make the merge finish.
4. Record `git diff --name-only --diff-filter=U` and group conflicts by owner: gateway routing/lifecycle, Discord, cron/trusted actions, CLI/config/schema, tests/docs.

### Task 2: Reconcile state, cron, and trusted actions

**Files:** `gateway/durable_*.py`, `hermes_state_common.py`, `cron/*.py`, `tools/cronjob_tools.py`, `tools/write_approval.py`, `tools/durable_request_tool.py`, relevant tests.

1. Retain additive Phase 4/5A tables and idempotent migration paths while preserving current upstream schema and cron behavior.
2. Reconnect binary action creation, execution claims, staged memory/skill approval, and terminal wake to current upstream APIs. Keep application handlers outside ledger locks and never retry ambiguous mutations.
3. Verify focused durable, action, cron, memory, and skill tests through `scripts/run_tests.sh -j 4` and fix only demonstrated integration failures.

### Task 3: Reconnect gateway and Discord entry points

**Files:** `gateway/run*.py`, `gateway/slash_commands*.py`, `gateway/platforms/base.py`, `plugins/platforms/discord/adapter.py`, relevant tests.

1. Put Phase 4/5A routing in the modules that now own inbound messages, busy handling, startup, shutdown, and slash commands. Preserve authorization before prompt-specific checks and the command/approval/clarify precedence.
2. Restore exact native message binding, opaque component IDs, structural unsupported-vs-unknown delivery handling, and commit-before-edit behavior.
3. Verify the Discord command cap/registration, native callbacks, fallback delivery, disconnect/restart cleanup, and focused gateway/Discord tests.

### Task 4: Reconcile CLI, configuration, and operations

**Files:** `hermes_cli/*.py`, `agent/monitoring/*.py`, tool discovery/config files, relevant tests and docs.

1. Preserve current upstream CLI mixins and config migrations while adding the operator inspection, health, and command-response configuration from the custom stack.
2. Ensure model-facing tools remain gated and cannot set scope, provider IDs, trusted action payloads, or handler versions.
3. Verify CLI, monitoring, config, and toolset tests.

### Task 5: Final gates and publication

**Files:** Any files changed by demonstrated failures; no speculative refactors.

1. Run `python -m compileall -q gateway hermes_cli tools plugins/platforms/discord agent/monitoring`, `ruff check .`, `git diff --check`, and the focused regression sets identified above.
2. Run broader tests where the conflict map or focused failures show risk. Compare combined behavior with the Phase 5A release gates, including races, restart, native/command scope, and privacy.
3. Scan every newly reachable outgoing commit and tracked tree for credentials and private local data before any public push. Triage scanner findings without printing values.
4. Commit the reconciled merge, publish the verified integration branch to the fork, and keep `fork/main` unchanged until promotion is explicitly chosen.
