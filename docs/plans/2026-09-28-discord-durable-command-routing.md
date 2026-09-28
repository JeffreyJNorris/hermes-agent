# Discord Durable-Request Command Routing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Keep explicit `/request` responses in their actual Discord channel even when that channel requires a mention and normally auto-threads messages.

**Architecture:** The Discord adapter already authenticates inbound messages before `_handle_message`, and the gateway already parses `/request` and validates the durable request. Add only a narrow adapter routing exception for the exact command word. Preserve the existing core resolver and all other Discord gates.

**Tech Stack:** Python 3.14, discord.py adapter, pytest through `scripts/run_tests.sh`.

---

### Task 1: Reproduce the live routing failure

**Files:**
- Modify: `tests/gateway/test_discord_free_response.py`

1. Add an async test using the existing adapter and fake channel fixture with `DISCORD_REQUIRE_MENTION=true` and `DISCORD_AUTO_THREAD=true`. Send an unmentioned `/request respond drq_... 1`. Assert `_handle_message` dispatches it inline with `source.chat_id` equal to the parent channel and never calls `_auto_create_thread`.
2. Run `scripts/run_tests.sh tests/gateway/test_discord_free_response.py -k durable_request -j 1`; expect failure because the message is ignored.
3. Add a second case with an explicit bot mention. Assert the same inline source and no auto-thread; expect failure because a thread is created.

### Task 2: Make the narrow adapter change

**Files:**
- Modify: `plugins/platforms/discord/adapter.py` in `_handle_message`
- Test: `tests/gateway/test_discord_free_response.py`

1. After mention stripping, identify only the exact `/request` command word from `normalized_content` (not `/requestfoo`).
2. Let that command pass the mention gate after normal adapter authorization.
3. Include that command in the existing `skip_thread` condition so `SessionSource` retains the physical channel ID.
4. Run the focused test; expect both new cases to pass. Keep malformed `/request` routed to the gateway's existing usage response.
5. Run existing mention-gate, auto-thread, slash-command, and durable-request command tests with `scripts/run_tests.sh`.
6. Compile the changed module, run `git diff --check`, and commit the code and tests.

### Task 3: Deploy and accept in Discord

**Files:**
- No application file edits; immutable release under `/home/jeff/hermes/runtime/releases/`

1. Build the tested commit into a new release-specific Python environment. Verify imports and dependencies.
2. Take a quiescent private backup of the active profile's SQLite database and config, then atomically move `runtime/current` to the new release. Preserve `runtime/previous` for rollback.
3. Confirm one gateway process, Discord connected, and a healthy structural request ledger.
4. In `#approvals`, repeat `/request respond` for the already-selected receipt without a mention. Expect “This request has already been answered” in the same channel, with no new thread. Also verify a wrong-channel command remains unavailable and no second decision is recorded.
5. If deployment or live validation fails, stop the gateway, inspect active durable rows, and follow the Phase 5A rollback runbook before switching back. Never roll back by rewriting the ledger.
