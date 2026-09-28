# Discord durable-request command routing fix

## Observed failure

In the live `#approvals` inbox, an unmentioned `/request respond` message can be
ignored by Discord's mention gate. Mentioning the bot admits the message, but
the adapter auto-threads it before the gateway resolves the command. The
gateway then supplies the new thread ID to the durable-request service, which
correctly rejects it as the wrong delivery channel. The request itself remains
selected from the earlier successful command; no second decision was recorded.

## Design

Treat the exact `/request` command prefix as a Discord control message after
normal adapter authorization but before the mention and auto-thread gates.
Route it in the channel where the user sent it, without creating a thread.
The existing gateway parser consumes malformed `/request` forms and the core
service still validates the generated receipt, actual platform, exact channel,
actor policy, deadline, state, and server-side option slot. Other messages and
commands retain their current mention and auto-thread behavior.

This is narrower than changing channel-wide `require_mention` or
`auto_thread` settings, and avoids adding a new Discord slash-command surface.
No database, durable-request, or provider identifier schema changes are needed.

## Verification

Add regression tests for unmentioned and mentioned `/request respond` in a
mention-required, auto-thread-enabled channel: both must reach the gateway
without creating a thread. An unrelated unmentioned message must still be
ignored, and an unrelated mentioned message must still auto-thread. Existing
core wrong-channel and duplicate-result tests remain authoritative.

After focused tests, build an immutable release, back up the live profile,
restart the gateway once, and repeat the command in `#approvals`. Keep the
existing request's ledger status unchanged; an already-selected receipt should
produce the duplicate acknowledgement rather than a wrong-channel result.
