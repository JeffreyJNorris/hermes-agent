# Discord Status Progress Implementation Plan

**Goal:** Show one editable, emoji-free activity line in Discord and remove it
after the final answer is delivered successfully.

**Architecture:** Extend the existing gateway progress mode with `status`.
Reuse friendly tool verbs, progress throttling, thread routing, and post-delivery
cleanup. Implement the Discord adapter's existing deletion interface. No model
tools, prompt changes, or extra inference requests are needed.

**Tech stack:** Python, asyncio, discord.py, pytest.

**Completed:** Status rendering, Discord thread edits/deletion, configuration,
documentation, and successful-delivery cleanup are implemented. The final
focused gateway regression run passed all 151 tests; Ruff and diff checks pass.

1. Add failing behavioral tests for the status callback and real progress
   sender: latest-line replacement, duplicate suppression, retaining one message
   across narration resets, thread routing, cancellation flush, and no extra
   messages on edit failure. Run with `scripts/run_tests.sh`.
2. Implement the status branches in `gateway/run.py`, preserving accumulated
   rendering for existing modes. Reuse `agent.display.get_tool_verb` with a
   generic fallback; do not interpolate arguments or tool-provided previews.
3. Independently add and test `DiscordAdapter.delete_message` through the
   adapter's existing channel and partial-message APIs.
4. Independently wire mode normalization, Discord defaults, `/verbose`, and
   user documentation. Existing explicit mode overrides remain effective.
5. Exercise `_run_agent` with temporary `HERMES_HOME` and controlled agent events
   to prove one progress message survives mid-turn narration and is deleted only
   after final delivery; failed runs preserve their status. Run focused tests,
   lint changed Python, and review the diff.
