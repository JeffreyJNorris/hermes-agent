"""Minimal at-most-once runner for trusted durable actions."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from gateway.durable_actions import (
    DurableActionOutcome,
    DurableActionStatus,
    DurableActionStore,
    lookup_durable_action_handler,
)

DEFAULT_STALE_CLAIM_SECONDS = 5 * 60.0


class DurableActionRunner:
    def __init__(
        self,
        store: DurableActionStore,
        *,
        enabled: bool,
        allowed_actions: Iterable[str] = (),
        stale_claim_seconds: float = DEFAULT_STALE_CLAIM_SECONDS,
    ) -> None:
        self._store = store
        self._enabled = enabled
        self._allowed_actions = frozenset(allowed_actions)
        self._stale_claim_seconds = stale_claim_seconds

    async def process_once(self) -> bool:
        if not self._enabled:
            return False
        self._store.recover_stale(self._stale_claim_seconds)
        claim = self._store.claim_next()
        if claim is None:
            return False
        context = claim.context
        if context.action_type not in self._allowed_actions:
            return self._store.mark_handler_unavailable(
                context.request_id,
                claim.token,
                reason="action_not_allowlisted",
            )
        handler = lookup_durable_action_handler(
            context.action_type, context.action_version
        )
        if handler is None:
            return self._store.mark_handler_unavailable(
                context.request_id,
                claim.token,
                reason="handler_unavailable",
            )
        if not self._store.mark_executing(context.request_id, claim.token):
            return False
        try:
            outcome = await asyncio.to_thread(handler, context)
        except Exception:
            outcome = DurableActionOutcome(
                DurableActionStatus.FAILED,
                reason="handler_exception",
            )
        if not isinstance(outcome, DurableActionOutcome):
            outcome = DurableActionOutcome(
                DurableActionStatus.FAILED,
                reason="invalid_handler_outcome",
            )
        return self._store.complete(context.request_id, claim.token, outcome)


__all__ = ["DEFAULT_STALE_CLAIM_SECONDS", "DurableActionRunner"]
