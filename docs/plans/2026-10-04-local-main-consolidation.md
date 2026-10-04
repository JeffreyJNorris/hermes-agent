# Local main consolidation — 2026-10-04

Consolidate the local fork's functional work on `main`, preserving commit
authorship and the exact source changes in the deployed Phase 5B release.
Deployment remains paused; this operation does not restart the gateway or
change its live configuration.

## Included work

- Fork base `2588f718f3`, including upstream integration, Discord interactive
  UI, durable operations, request command responses, and multi-select foundations.
- Commit `52fd9fca7d`: all 41 changed source files verified against the deployed
  `phase5b-ad310ee5e376a295` release manifest. This captures the previously
  uncommitted Phase 5B work without changing its original worktree.
- Merge `5262dc0ca9`: conversation-local durable approvals from
  `feature/conversation-durable-approvals`, with the request tool regression
  updated to check refusal in the wrong channel and resolution in the origin.
- Merge `feature/discord-status-obsidian`: one editable Discord activity line,
  emoji-free routine status, deletion after successful final delivery, and
  path-first Obsidian vault lookup through the existing skill and filesystem.

The status feature was adapted to the newer gateway modules. Integration fixes
preserve the platform-specific installer defaults, snapshot delivery outcomes
before a queued successor can clear an interrupt, and clean the first turn's
status after accepted queued final delivery. Refused deliveries, failed turns,
and connector authorization declines preserve status breadcrumbs.

## Branch audit

Older Discord UI, durable operations, request command, and SQLite runtime branches
have no missing functional patches relative to the fork base. The private Phase
5A history differs from its sanitized counterpart only in documentation with
personal paths; its implementation is already included. Backup source files in
the old development worktree remain local artifacts.

The previous local `main` (`1be70d6354`) is an ancestor of the fork base. Preserve
it as `backup/main-before-consolidation-20261004` before promoting the verified
integration commit. Keep existing feature branches and other worktrees intact.

## Validation

Run the canonical `scripts/run_tests.sh` across durable approvals, interactive
requests, queued delivery, progress, Discord adapters, warnings, and Obsidian.
Also run Ruff on changed Python files and `git diff --check` before committing.

The real delivery regressions use temporary Hermes homes and the base adapter
pipeline, including successful and refused first/terminal queued responses.
The callback timeout regression disables the unrelated delivery ledger through
its temporary config so SQLite scheduling cannot consume its one-second limit.

Final validation: 65 files passed 1,113 tests. After correcting the queued
refusal fixture and adding a real connector-decline callback regression, the
two affected files passed all 42 tests with retries disabled. This covers 1,114
distinct passing tests. Ruff and diff checks passed.
