# Local Discord brief cards

The email and security brief presentation integration lives in the standalone repository `/home/jeff/projects/hermes-discord-cards`, on local `main` at `9c88f479456a54d7638305173c7e5c6b682197bd`. Its code, regression tests and non-secret deployment snapshots are versioned there. It is installed as `~/.hermes/plugins/discord-cards`; it does not vendor the integration into Hermes core or add a model tool.

The plugin registers the delivery-only `discord-card` platform against existing native plugin APIs. It uses the existing Discord bot credential, validates Components V2, verifies message readback and records private per-channel delivery receipts. Email and security renderers produce cards and equivalent Obsidian Markdown from structured input. Security reports preserve narrative and can continue in numbered messages. Stable delivery keys update the same date's messages, with pending-send reconciliation and verified removal of obsolete generated parts.

## Versioned setup

The standalone repository's `deployment/` directory contains the owner-maintained workflow prose, card templates, authoring skill/references, bounded public security collector, webhook workflow transform, shared Markdown formatter, Google helper integration, host launcher, native security job definition and a webhook route example. The route's authentication secret is replaced with a placeholder. Credentials, generated reports, task/calendar records and private delivery state are excluded.

The live vault remains the editing surface. The versioned files are reviewed snapshots; applying a snapshot requires comparing it with owner edits. Card authoring source is stored once in the standalone repository and copied into the mounted skill's `scripts/` directory for Docker access.

The owner configuration enables `discord-cards` and `discord-card`, restricts local card reads to `Hermes/Daily Briefs/Cards` and `Hermes/Security Briefs/Cards`, and uses `/home/jeff/.local/state/hermes-discord-cards` for private receipts. These settings belong in `config.yaml`; bot credentials remain in the private credential store.

Native Inbox Zero webhook delivery uses `discord-card` and retains its existing authentication and scoped tools. Native security job `fdfa0420634c` runs at `0 7 * * *` in `Australia/Melbourne`, delivers to `discord-card:1503048211122622585` and follows the current inference configuration. The agent returns a small validated file-reference envelope; the plugin handles the normal cron wrapper without changing global wrapper settings. No second bot or daemon is required.

## Core approval-context fix

The production runtime previously carried an uncommitted fix in `tools/approval.py`. It is now tracked alongside regression coverage in `tests/tools/test_webhook_approval_mode.py`. Webhook and Microsoft Graph webhook sessions have no approval reply surface, so an inherited `HERMES_EXEC_ASK` flag must not make them wait for a human response. They resolve from the configured unattended policy. The API server retains its explicit approval bridge.

This is a generic session-context correction; the card plugin does not alter approval policy. A configured unattended denial still blocks an operation. Tests exercise both configured approve/deny behavior against a temporary real `HERMES_HOME`, with inherited gateway flags and dangerous-command, combined-command and plugin-tool guard paths.

The live immutable runtime already contains the exact same approval implementation. Committing it does not require replacing the runtime or restarting the gateway.

## Verification

The card integration's 77 regression tests pass. The security job completed a real research/render/delivery run on 5 October 2026; two Discord messages were independently read back and matched the generated components. The Obsidian report matched the complete rendered narrative, passed formatting checks and retained owner editability. Private readback evidence is `/home/jeff/.local/state/hermes-discord-cards/native-security-verification.json`.

Run card tests from the standalone repository with:

```bash
PYTHONPATH=/home/jeff/hermes/runtime/current/source /home/jeff/hermes/runtime/current/venv/bin/python -m unittest discover -s tests
```

Hermes approval tests run from this main worktree using a test environment with pytest and the normal runtime dependencies. The production runtime's virtual environment is not modified for testing. Existing Discord/Obsidian feature work is already included in local main through merge `ce79e53c51`.
