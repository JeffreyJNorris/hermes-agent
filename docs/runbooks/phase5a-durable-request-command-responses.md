# Phase 5A durable request command responses

SQLite is authoritative. Commands and Discord controls only submit a response;
a failed presentation edit cannot reopen or change a request.

## Enablement

Configure the active profile and restart the gateway:

```yaml
durable_requests:
  enabled: true
  command_responses:
    enabled: true
    allow_on_native_delivery: true
    allow_text_delivery_fallback: true
```

`durable_requests.command_responses.enabled` affects only newly created
requests. The other two settings permit native requests to advertise the
command and permit explicitly unsupported native delivery to use one text
presentation, respectively. Existing and migrated rows remain
command-disabled; `delivery_mode` defaults to `native`.

## Command contract

Answer in the request's exact delivery channel:

```text
/request respond drq_RECEIPT 2
```

For live Discord requests this is the originating channel or thread. For
scheduled requests it is the configured inbox. The request's persisted target
does not change if configuration later changes. A missing live channel is not
silently redirected to the inbox.

The responder must already pass normal Hermes command authorization and the
request's responder policy. Phase 5A accepts exactly one positive numeric slot.
Labels, canonical option IDs, bare numbers, `approve`, ranges, and lists such as
`1,3` are not responses. A malformed `/request respond` is consumed and returns
usage instead of reaching the model. Phase 5B owns slot lists.

The command validates the generated receipt, actual platform, exact delivery
channel, actual actor, deadline, open state, persisted command permission, and
server-side slot mapping. Native controls additionally validate the bound
provider message ID. One transaction records the terminal decision, linked
action transition, resolution source, and wake work before returning success.

## Live checks

1. Enable command responses and create a new durable request in Discord.
2. Confirm the native message includes the full receipt and alternative command.
3. Submit its numeric slot and confirm `Response recorded.`
4. Create several requests in the same channel and answer each by its receipt.
5. From another channel, confirm the response is unavailable; with an
   unauthorized actor, confirm it is denied. Do not expect the reply to reveal
   the configured channel, actor, message, action, session, or option ID.
6. With an adapter that structurally returns `unsupported`, confirm exactly one
   complete text presentation is sent, bound, and answerable. A native success
   must not create a text presentation.
7. Force an ambiguous native send and confirm the request becomes
   `delivery_unknown` with no fallback. Force an ambiguous text send and confirm
   restart does not resend it.

Do not classify exception text as unsupported. The adapter must return the
bounded structural result.

## Inspection and health

```bash
hermes durable requests inspect drq_RECEIPT
hermes durable requests list --status open
hermes durable doctor
hermes durable doctor --json
hermes doctor
```

Inspection exposes structural `delivery_mode` and `resolution_source`, not
content, command permission, or scope. Health reports a
`text_command_delivery` backlog. Metrics use bounded result/event labels only;
receipts never become labels. Follow the general procedures in
[`docs/operations/durable-operations.md`](../operations/durable-operations.md)
for cancellation and UNKNOWN handling.

## Fault boundaries

| Boundary | Authoritative state | Retry and duplicate expectation | Operator action |
|---|---|---|---|
| Before native send | Claimed `pending_delivery` | A definite pre-send failure releases the claim; retry is safe and no presentation exists | Investigate only if health reports a stale backlog |
| Native returns structural `unsupported` | Claim atomically changes to `text_command` before text send | One text attempt is allowed; native success never enters this path | Verify adapter capability/configuration if unexpected |
| Before text send | Claimed `pending_delivery` in `text_command` mode | A definite pre-send failure may retry; no presentation exists | Investigate repeated delivery failures |
| After text send, before message-ID binding | Provider outcome may be ambiguous | Becomes `delivery_unknown`; never resend automatically because a presentation may exist | Inspect provider and ledger; do not reset the row |
| After message-ID binding | `open`, exact message ID stored | Restart does not resend; text presentations need no persistent native view | None unless the provider message is missing |
| After command commit | Request is terminal; linked action/wake work is committed | Repeating the command reports already answered; no application code runs under the request transaction | Treat SQLite as final even if acknowledgement fails |
| Before optional original-message edit | Same terminal state | Phase 5A does not perform this optional edit; stale controls cannot win a second resolution | None; inspect the terminal row if presentation looks stale |
| Native and command race | First compare-and-set winner is terminal | Exactly one decision wins; the loser is already answered | None |
| Restart with stale text claim | Claim age proves bind never completed, not whether send completed | Reconciles to `delivery_unknown`; no automatic resend | Inspect provider and ledger |

At ambiguous boundaries, a duplicate presentation is possible only from the
provider completing an unacknowledged send; Hermes does not create another one.
User action may be repeated safely, but cannot produce a second terminal choice.

## Rollback

1. Set `durable_requests.command_responses.enabled: false` to stop granting the
   capability to new requests. Existing records keep their persisted policy.
2. Stop the Phase 5A gateway and verify that no second gateway process is using
   the profile. Keep the gateway stopped through inventory, cleanup, backup,
   binary or symlink rollback, and the old-runtime start. Run only the trusted
   local `hermes durable` CLI against this quiescent ledger.
3. Inventory both active states from offset zero with the maximum page size:

   ```bash
   hermes durable requests list --status pending_delivery --limit 100 --offset 0 --json
   hermes durable requests list --status open --limit 100 --offset 0 --json
   ```

   For each command, repeat it with `--offset NEXT_OFFSET` using the exact
   `next_offset` returned by the previous page. Continue until `next_offset` is
   absent. The ledger permits up to 1,000 active rows while the default page
   contains only 50, so the first page is not a complete rollback inventory.

   For every `pending_delivery` row whose `delivery_mode` is `text_command`,
   cancel it with the supported operator command:

   ```bash
   hermes durable requests cancel drq_RECEIPT --yes
   ```

   Cancel each `open` text-command request too. A pre-5A runtime cannot answer
   it by `/request respond`. If an open request must be preserved, set
   `durable_requests.enabled: false` and keep durable processing disabled on the
   old runtime; the request is intentionally stranded until Phase 5A is
   restored. This pauses every durable request, not only text-command rows.
   Do not clear claims or change states with ad hoc SQL.
4. Repeat both complete paginated scans from offset zero, again following every
   `next_offset` until absent. Require zero `pending_delivery` text-command rows.
   Also require zero `open` text-command rows unless
   `durable_requests.enabled: false` will remain in effect on the old runtime.
   Do not proceed based on an earlier page or pre-cleanup inventory.
5. With the gateway still stopped, take a consistent backup of the active
   profile's `state.db` and its WAL/SHM files.
6. Perform the normal immutable binary or symlink rollback while preserving
   service singleton ownership. The additive `delivery_mode`,
   `command_response_allowed`, `text_command_fallback_allowed`, and
   `resolution_source` columns may remain; do not drop or rewrite them.
7. Start exactly one old-runtime gateway. If any open text-command request was
   intentionally stranded, confirm durable processing remains disabled and
   re-enable it only after restoring Phase 5A.

Terminal `delivery_unknown` rows remain authoritative evidence and must not be
retried or rewritten during rollback. Follow the general durable-operations
backup and UNKNOWN guidance; provider messages are never a source of business
state.

Phase 5A deliberately defers multi-slot syntax, atomic multi-select, ranked or
partial selections, and multi-select action approvals to later work. Do not
encode those behaviors in command text or provider identifiers.
