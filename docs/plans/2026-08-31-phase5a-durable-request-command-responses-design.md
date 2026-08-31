# Phase 5A Durable Request Command Responses Design

## Goal

Allow an authorized user to answer a durable request by its generated receipt
and one-based numeric option slot while preserving native button behavior and
the durable ledger as the sole authority.

## Chosen Approach

Extend the existing durable-request store, bridge, gateway command registry,
and Discord adapter directly. Add no generic command framework, component
model, callback registry, or provider-independent UI tree.

The two heavier alternatives are rejected:

- A hand-built Discord slash-command subgroup duplicates the current automatic
  command registration without improving the provider-neutral service.
- A generic fallback renderer or component abstraction creates speculative
  surface that Phase 5A does not need.

## Persistence

Add three backward-compatible columns to both durable-request schema sources
and the store's idempotent reconciliation:

```text
delivery_mode TEXT NOT NULL DEFAULT 'native'
command_response_allowed INTEGER NOT NULL DEFAULT 0
resolution_source TEXT
```

Existing rows remain native and command-disabled. Request creation snapshots
the effective configuration, so later configuration changes cannot widen an
active request's response surface.

Typed enums represent `native`/`text_command` delivery and
`native`/`command` resolution. Operator projections may expose these structural
values; model-facing results do not.

## Resolution

Add a sibling `DurableRequestStore.resolve_command()` API instead of weakening
the native `resolve()` message-binding contract. It validates the request-token
format before lookup, then checks expiry, lifecycle, command permission,
platform, exact delivery channel, actor/responder policy, and exactly one
positive numeric slot.

The command and native paths share the smallest existing terminal-write helper:
slot-to-option mapping, compare-and-set resolution, linked action transition,
wake creation, and terminal logging. The transaction records
`resolution_source='command'` and commits before any acknowledgement or
provider-message edit. Application handlers continue to run outside the lock.

## Delivery

The bridge attempts native delivery first. Only a structural
`error_kind='unsupported'` may enter text-command fallback; exceptions,
definite send failures, and ambiguous results never do.

Add one optional adapter method for a single, unsplit, mention-disabled durable
text send. Its default returns unsupported. Discord implements it without the
ordinary retry/splitting path and classifies a post-attempt exception as
delivery-unknown. The bridge sets `delivery_mode='text_command'`, sends the
central deterministic rendering, and binds the exact returned message ID.
Stale text-delivery claims converge to the existing unknown state and are not
resent automatically.

Native content appends the alternative command only when the persisted flag is
enabled. Text content includes the full receipt and every option; it fails
before send if it exceeds the transport budget.

## Command Routing

Register the provider-neutral `request` gateway command with the current
automatic Discord slash registration and busy-dispatch convention. One strict
parser handles both text and structured slash events:

```text
/request respond drq_<token> <positive-slot>
```

Malformed `request respond` forms are consumed and receive bounded usage text.
Unrelated `/request` input, bare numbers, approval words, and normal chat retain
their current routes. Existing approval and trusted administrative handlers run
first. The early command route explicitly applies current gateway authorization
and slash-access checks; there is no existing invocation rate limiter to reuse,
so Phase 5A does not invent one under that name.

User messages map structural results without revealing expected scope,
canonical option IDs, action linkage, or provider identifiers. Original-message
updates are best effort after commit.

## Configuration and Operations

Add disabled-by-default settings under `durable_requests.command_responses`:

```yaml
enabled: false
allow_on_native_delivery: true
allow_text_delivery_fallback: true
```

Metrics use bounded structural result, delivery mode, and failure reason
dimensions only. Health and operator inspection add text-command backlog and
delivery/resolution mode visibility without request IDs or content labels.

## Verification

Use focused TDD around:

- additive migration from historical durable schemas;
- strict parsing and pre-lookup receipt validation;
- command scope, actor, expiry, lifecycle, slot, race, wake, and action coupling;
- unsupported-only fallback and ambiguous-send non-retry behavior;
- restart restoration for native and text-command rows;
- command precedence, busy dispatch, slash access, and bounded responses;
- Discord mention suppression and resolve-before-edit ordering;
- privacy-safe logs, metrics, health, and user-facing errors;
- unchanged native resolution and existing approval/chat behavior.

Phase 5B slot lists, manual Discord subcommand groups, and generic UI rendering
remain deferred.
