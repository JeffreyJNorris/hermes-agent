"""Minimal at-most-once runner for trusted durable actions."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable

from gateway.durable_actions import (
    DurableActionOutcome,
    DurableActionStatus,
    DurableActionStore,
    lookup_durable_action_handler,
)

DEFAULT_STALE_CLAIM_SECONDS = 5 * 60.0


class DurableActionNotificationDisposition(str, Enum):
    DELIVERED = "delivered"
    RETRY = "retry"
    DROPPED = "dropped"


@dataclass(frozen=True)
class DurableActionNotificationResult:
    disposition: DurableActionNotificationDisposition
    reason: str | None = None


DurableActionNotificationHandler = Callable[
    [object], Awaitable[DurableActionNotificationResult]
]


class DurableActionRunner:
    def __init__(
        self,
        store: DurableActionStore,
        *,
        enabled: bool,
        allowed_actions: Iterable[str] = (),
        stale_claim_seconds: float = DEFAULT_STALE_CLAIM_SECONDS,
        notification_handler: DurableActionNotificationHandler | None = None,
        completion_handler: Callable[[object, str], Awaitable[None]] | None = None,
    ) -> None:
        stale_claim_seconds = float(stale_claim_seconds)
        if not math.isfinite(stale_claim_seconds) or stale_claim_seconds <= 0:
            raise ValueError("stale_claim_seconds must be finite and positive")
        self._store = store
        self._enabled = enabled
        self._allowed_actions = frozenset(allowed_actions)
        self._stale_claim_seconds = stale_claim_seconds
        self._heartbeat_seconds = stale_claim_seconds / 3
        self._notification_handler = notification_handler
        self._completion_handler = completion_handler

    async def process_once(self) -> bool:
        if not self._enabled:
            return False
        await asyncio.to_thread(self._store.recover_stale, self._stale_claim_seconds)
        await asyncio.to_thread(
            self._store.recover_stale_notifications, self._stale_claim_seconds
        )
        action_processed = await self._process_action()
        notification_processed = await self._dispatch_notification()
        return action_processed or notification_processed

    async def _process_action(self) -> bool:
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
        execution = asyncio.create_task(
            self._start_execute_and_complete(handler, context, claim.token)
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

    async def _start_execute_and_complete(self, handler, context, token: str) -> bool:
        if not await asyncio.to_thread(
            self._store.mark_executing,
            context.request_id,
            token,
        ):
            return False
        return await self._execute_and_complete(handler, context, token)

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
                DurableActionStatus.UNKNOWN,
                reason="handler_exception",
            )
        if not isinstance(outcome, DurableActionOutcome):
            outcome = DurableActionOutcome(
                DurableActionStatus.UNKNOWN,
                reason="invalid_handler_outcome",
            )
        completed = await asyncio.to_thread(
            self._store.complete,
            context.request_id,
            token,
            outcome,
        )
        if completed and self._completion_handler is not None:
            try:
                await self._completion_handler(context, outcome.status.value)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Presentation cannot change an already recorded execution.
                pass
        return completed

    async def _dispatch_notification(self) -> bool:
        if self._notification_handler is None:
            return False
        claim = await asyncio.to_thread(self._store.claim_next_notification)
        if claim is None:
            return False
        try:
            result = await self._notification_handler(claim)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.to_thread(
                self._store.release_notification, claim.request_id, claim.token
            )
            return False
        if result.disposition is DurableActionNotificationDisposition.DELIVERED:
            return await asyncio.to_thread(
                self._store.complete_notification, claim.request_id, claim.token
            )
        if result.disposition is DurableActionNotificationDisposition.DROPPED:
            return await asyncio.to_thread(
                self._store.drop_notification,
                claim.request_id,
                claim.token,
                reason=result.reason or "origin_unavailable",
            )
        await asyncio.to_thread(
            self._store.release_notification, claim.request_id, claim.token
        )
        return False


__all__ = [
    "DEFAULT_STALE_CLAIM_SECONDS",
    "DurableActionNotificationDisposition",
    "DurableActionNotificationResult",
    "DurableActionRunner",
]
