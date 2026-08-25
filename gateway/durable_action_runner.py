"""Minimal at-most-once runner for trusted durable actions."""

from __future__ import annotations

import asyncio
import math
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
        stale_claim_seconds = float(stale_claim_seconds)
        if not math.isfinite(stale_claim_seconds) or stale_claim_seconds <= 0:
            raise ValueError("stale_claim_seconds must be finite and positive")
        self._store = store
        self._enabled = enabled
        self._allowed_actions = frozenset(allowed_actions)
        self._stale_claim_seconds = stale_claim_seconds
        self._heartbeat_seconds = stale_claim_seconds / 3

    async def process_once(self) -> bool:
        if not self._enabled:
            return False
        await asyncio.to_thread(self._store.recover_stale, self._stale_claim_seconds)
        claim = await asyncio.to_thread(self._store.claim_next)
        if claim is None:
            return False
        context = claim.context
        if context.action_type not in self._allowed_actions:
            return await asyncio.to_thread(
                self._store.mark_handler_unavailable,
                context.request_id,
                claim.token,
                reason="action_not_allowlisted",
            )
        handler = lookup_durable_action_handler(
            context.action_type, context.action_version
        )
        if handler is None:
            return await asyncio.to_thread(
                self._store.mark_handler_unavailable,
                context.request_id,
                claim.token,
                reason="handler_unavailable",
            )
        if not await asyncio.to_thread(
            self._store.mark_executing, context.request_id, claim.token
        ):
            return False
        execution = asyncio.create_task(
            self._execute_and_complete(handler, context, claim.token)
        )
        try:
            return await asyncio.shield(execution)
        except asyncio.CancelledError:
            while not execution.done():
                try:
                    await asyncio.shield(execution)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if execution.done() and not execution.cancelled():
                execution.exception()
            raise asyncio.CancelledError

    async def _execute_and_complete(self, handler, context, token: str) -> bool:
        handler_task = asyncio.create_task(asyncio.to_thread(handler, context))
        while not handler_task.done():
            try:
                await asyncio.wait_for(
                    asyncio.shield(handler_task),
                    timeout=self._heartbeat_seconds,
                )
            except TimeoutError:
                try:
                    await asyncio.to_thread(
                        self._store.renew_execution_claim,
                        context.request_id,
                        token,
                    )
                except Exception:
                    continue
            except Exception:
                break
        try:
            outcome = await handler_task
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
        return await asyncio.to_thread(
            self._store.complete,
            context.request_id,
            token,
            outcome,
        )


__all__ = ["DEFAULT_STALE_CLAIM_SECONDS", "DurableActionRunner"]
