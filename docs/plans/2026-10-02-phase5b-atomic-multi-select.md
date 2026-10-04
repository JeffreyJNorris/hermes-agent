# Phase 5B: atomic multi-select

Base: promoted Phase 5A `2588f718f33dba23d35a7648758b48b342b138ab`.
Scope: `docs/design/phase5b-5d-requirements.md`, Phase 5B only.
Local implementation and tests; no commits, pushes, promotion, configuration changes, or restart.

1. Add separate multi-select models and narrow registry facades, reusing the authoritative lifecycle and shared strict slot grammar. Preserve single-select models and behavior.
2. Extend bridge and context-bound typed helpers; add the bounded model tool and session visibility/parallel restrictions.
3. Add Discord Select delivery and callbacks with authorization before resolution and edits after commit.
4. Add additive durable schema, canonical set transactions, strict request commands, status and restart-safe wakes. Reject multi-select trusted actions.
5. Verify invalid sets, bounds, auth, lifecycle cleanup, replay/races, delivery/edit failures, restart, status/wake ordering, bounded metrics and health. Run existing single-choice and durable regression files through scripts/run_tests.sh.
6. Record concrete local verification and remaining promotion gate; leave production unchanged.

## Local verification

Implementation complete locally; production remains on the promoted Phase 5A base.
Final focused gate: **206 passed, 0 failed**, across 13 files, using the isolated
`.venv/bin/python` and `scripts/run_tests.sh --file-retries 0`.
Each file used its own `/dev/shm` basetemp. Slow disk SQLite DDL made the original
baseline time out; its 46 tests passed on temporary memory-backed storage.
Durable regression suites also passed during implementation: 273 tests across
nine file runs (some overlap with the final gate).

Verified requirements:

- Typed synchronous/asynchronous sets; strict text grammar; full option rendering;
  count/uniqueness/mapping validation; canonical ordering; actor/channel/message scope;
  replay, deadline, cancellation and scoped cleanup.
- Native Discord Select; exact authorization; commit before UI edit; failed edits
  leave the terminal result intact; early provider timers cannot expire the registry.
- Additive historical-row defaults; canonical JSON sets with null scalar;
  restart restoration; native/command race; transaction rollback; binary action guard;
  status and wake order; failed wake admission retries the committed set.
- Callback-dependent tool discovery; eager sequential dispatch; cron absence;
  bounded live/SQLite health metrics; no selection labels in diagnostic logs.
- Held-lock regressions reproduced the stale-clock bug before fixes, then passed
  after clock reads moved inside registry locks and acquired SQLite transactions.

Final gate logs: `/tmp/hermes-5b-verification-1hrhnjrk/` (temporary local evidence).
Provider transport was simulated in tests; no live Discord messages were sent.
No commits, pushes, promotion, production schema/config changes, or restart occurred.
Phase 5C and Phase 5D remain pending.
