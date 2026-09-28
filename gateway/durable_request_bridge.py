"""Delivery and restart reconciliation for persisted durable requests."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping

from gateway.durable_requests import (
    DELIVERY_CLAIM_STALE_SECONDS,
    MAX_DELIVERY_BATCH,
    MAX_WAKE_BATCH,
    WAKE_CLAIM_STALE_SECONDS,
    DurableRequest,
    DurableRequestDeliveryMode,
    DurableRequestStatus,
    DurableWakeClaim,
    get_durable_request_store,
    load_durable_requests_config,
)

logger = logging.getLogger(__name__)


class DurableWakeDisposition(str, Enum):
    DELIVERED = "delivered"
    RETRY = "retry"
    DROPPED = "dropped"


@dataclass(frozen=True)
class DurableWakeDispatchResult:
    disposition: DurableWakeDisposition
    reason: str | None = None


DurableWakeHandler = Callable[
    [DurableWakeClaim], Awaitable[DurableWakeDispatchResult]
]


class DurableRequestBridge:
    """Process one profile's durable request outbox and open-message views.

    The database is authoritative.  This bridge never manufactures terminal
    state and never automatically retries an ambiguous provider send.
    """

    def __init__(self) -> None:
        self._restored: set[tuple[int, str, str]] = set()
        self._wake_handler: DurableWakeHandler | None = None

    def set_wake_handler(self, handler: DurableWakeHandler | None) -> None:
        """Set the profile-scoped terminal wake dispatcher."""

        self._wake_handler = handler

    async def process_once(self, adapters: Mapping[Any, Any]) -> None:
        config = load_durable_requests_config()
        if not config.available:
            return

        store = get_durable_request_store()
        adapters_by_platform = _adapters_by_platform(adapters)
        platforms = tuple(adapters_by_platform)

        store.expire_due()
        text_unknown = store.recover_stale_delivery_claims(
            stale_after_seconds=DELIVERY_CLAIM_STALE_SECONDS,
            delivery_mode=DurableRequestDeliveryMode.TEXT_COMMAND,
        )
        if text_unknown:
            from gateway.durable_operations import record_durable_delivery_event

            record_durable_delivery_event(
                "text_delivery_unknown", count=text_unknown
            )
        store.recover_stale_delivery_claims(
            stale_after_seconds=DELIVERY_CLAIM_STALE_SECONDS
        )
        store.recover_stale_wake_claims(
            stale_after_seconds=WAKE_CLAIM_STALE_SECONDS
        )

        if platforms:
            await self._restore_bound_requests(
                store=store,
                adapters_by_platform=adapters_by_platform,
            )
            await self._deliver_pending_requests(
                store=store,
                adapters_by_platform=adapters_by_platform,
            )
        if self._wake_handler is not None:
            await self._deliver_terminal_wakes(
                store=store,
                wake_handler=self._wake_handler,
            )
        store.prune_terminal(config.terminal_retention_seconds)

    async def _deliver_terminal_wakes(
        self,
        *,
        store,
        wake_handler: DurableWakeHandler,
    ) -> None:
        for _ in range(MAX_WAKE_BATCH):
            claim = store.claim_next_wake()
            if claim is None:
                return
            try:
                result = await wake_handler(claim)
            except asyncio.CancelledError:
                # The target may have accepted the new turn. Keep the claim so
                # stale-claim recovery makes the ambiguity visible on restart.
                raise
            except Exception as exc:
                store.release_wake_claim(
                    request_id=claim.request.request_id,
                    claim_id=claim.claim_id,
                )
                logger.warning(
                    "Durable request wake dispatch raised",
                    extra={
                        "request_id": claim.request.request_id,
                        "origin_type": claim.request.origin_type,
                        "exception_class": type(exc).__name__,
                    },
                )
                return

            if result.disposition is DurableWakeDisposition.DELIVERED:
                store.complete_wake(
                    request_id=claim.request.request_id,
                    claim_id=claim.claim_id,
                )
                continue
            if result.disposition is DurableWakeDisposition.DROPPED:
                store.drop_wake(
                    request_id=claim.request.request_id,
                    claim_id=claim.claim_id,
                    reason=result.reason or "origin_unavailable",
                )
                continue
            store.release_wake_claim(
                request_id=claim.request.request_id,
                claim_id=claim.claim_id,
            )
            # Avoid a tight retry loop within the same worker pass.
            return

    async def _restore_bound_requests(
        self,
        *,
        store,
        adapters_by_platform: Mapping[str, Any],
    ) -> None:
        bound_requests = store.list_bound_requests(adapters_by_platform)
        live_keys: set[tuple[int, str, str]] = set()
        for request in bound_requests:
            if request.delivery_mode is DurableRequestDeliveryMode.TEXT_COMMAND:
                continue
            adapter = adapters_by_platform.get(request.target.platform)
            if adapter is None or request.message_id is None:
                continue
            key = (id(adapter), request.request_id, request.message_id)
            live_keys.add(key)
            if key in self._restored:
                continue

            restore = getattr(adapter, "restore_durable_request", None)
            if restore is None:
                store.mark_open_failed(
                    request_id=request.request_id,
                    message_id=request.message_id,
                    reason="persistent_view_restore_unsupported",
                )
                continue

            try:
                result = await restore(request=request)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Durable request restore failed",
                    extra={
                        "request_id": request.request_id,
                        "platform": request.target.platform,
                        "exception_class": type(exc).__name__,
                    },
                )
                # A thrown restore may be a transient provider failure.  Keep
                # the request OPEN and retry reconciliation on a later pass.
                continue

            if result.success:
                self._restored.add(key)
                continue

            permanent_reasons = {
                "not_found": "provider_message_missing",
                "unsupported": "persistent_view_restore_unsupported",
                "forbidden": "provider_message_unavailable",
                "bad_format": "persistent_view_restore_failed",
            }
            permanent_reason = permanent_reasons.get(result.error_kind or "")
            if permanent_reason is not None:
                if request.status is DurableRequestStatus.OPEN:
                    store.mark_open_failed(
                        request_id=request.request_id,
                        message_id=request.message_id,
                        reason=permanent_reason,
                    )
                else:
                    self._restored.add(key)
                continue

            logger.debug(
                "Durable request restore deferred",
                extra={
                    "request_id": request.request_id,
                    "platform": request.target.platform,
                    "error_kind": result.error_kind or "unknown",
                },
            )

        # Bound memory while preserving successful registrations for currently
        # open rows.  New adapter instances naturally get new keys after a
        # reconnect and therefore re-register their views.
        self._restored.intersection_update(live_keys)

    async def _deliver_pending_requests(
        self,
        *,
        store,
        adapters_by_platform: Mapping[str, Any],
    ) -> None:
        for _ in range(MAX_DELIVERY_BATCH):
            claim = store.claim_next_pending(adapters_by_platform)
            if claim is None:
                return
            request = claim.request
            adapter = adapters_by_platform.get(request.target.platform)
            if adapter is None:
                # The adapter disappeared after the claim and no provider call
                # was attempted. Release the claim so a reconnect can retry.
                store.release_delivery_claim(
                    request_id=request.request_id,
                    claim_id=claim.claim_id,
                )
                return

            if request.delivery_mode is DurableRequestDeliveryMode.TEXT_COMMAND:
                keep_processing = await self._deliver_text_request(
                    store=store,
                    adapter=adapter,
                    request=request,
                    claim_id=claim.claim_id,
                )
                if not keep_processing:
                    return
                continue

            send = getattr(adapter, "send_durable_request", None)
            if send is None:
                store.fail_delivery(
                    request_id=request.request_id,
                    claim_id=claim.claim_id,
                    status=DurableRequestStatus.UNSUPPORTED,
                    reason="adapter_unsupported",
                )
                continue

            try:
                result = await send(
                    chat_id=request.target.chat_id,
                    request=request,
                    metadata={"thread_id": request.target.thread_id},
                )
            except asyncio.CancelledError:
                # The provider call may have completed.  Leave the claim for
                # stale-claim recovery, which converges to DELIVERY_UNKNOWN and
                # prevents an unsafe duplicate send after restart.
                raise
            except Exception as exc:
                store.fail_delivery(
                    request_id=request.request_id,
                    claim_id=claim.claim_id,
                    status=DurableRequestStatus.DELIVERY_UNKNOWN,
                    reason="provider_send_outcome_unknown",
                )
                logger.warning(
                    "Durable request delivery raised",
                    extra={
                        "request_id": request.request_id,
                        "platform": request.target.platform,
                        "exception_class": type(exc).__name__,
                    },
                )
                continue

            if result.success:
                if not result.message_id:
                    store.fail_delivery(
                        request_id=request.request_id,
                        claim_id=claim.claim_id,
                        status=DurableRequestStatus.DELIVERY_UNKNOWN,
                        reason="missing_message_id",
                    )
                    continue
                bound = store.bind_delivery(
                    request_id=request.request_id,
                    claim_id=claim.claim_id,
                    platform=request.target.platform,
                    message_id=str(result.message_id),
                )
                if bound is not None:
                    self._restored.add(
                        (id(adapter), bound.request_id, str(result.message_id))
                    )
                continue

            if result.error_kind in {"transient", "rate_limited"}:
                # The Discord adapter uses these only before channel.send().
                # The provider did not receive a message, so a later retry is
                # safe. Stop this batch to avoid an immediate retry loop.
                store.release_delivery_claim(
                    request_id=request.request_id,
                    claim_id=claim.claim_id,
                )
                return
            if (
                result.error_kind == "unsupported"
                and request.text_command_fallback_allowed
            ):
                text_request = store.switch_claim_to_text_command(
                    request_id=request.request_id,
                    claim_id=claim.claim_id,
                )
                if text_request is None:
                    continue
                from gateway.durable_operations import record_durable_delivery_event

                record_durable_delivery_event("native_to_text_fallback")
                keep_processing = await self._deliver_text_request(
                    store=store,
                    adapter=adapter,
                    request=text_request,
                    claim_id=claim.claim_id,
                )
                if not keep_processing:
                    return
                continue
            if result.error_kind == "unsupported":
                terminal_status = DurableRequestStatus.UNSUPPORTED
                reason = "adapter_unsupported"
            elif result.error_kind == "delivery_unknown":
                terminal_status = DurableRequestStatus.DELIVERY_UNKNOWN
                reason = "provider_send_outcome_unknown"
            else:
                terminal_status = DurableRequestStatus.DELIVERY_FAILED
                reason = f"provider_delivery_{result.error_kind or 'failed'}"
            store.fail_delivery(
                request_id=request.request_id,
                claim_id=claim.claim_id,
                status=terminal_status,
                reason=reason,
            )

    async def _deliver_text_request(
        self,
        *,
        store,
        adapter: Any,
        request: DurableRequest,
        claim_id: str,
    ) -> bool:
        send = getattr(adapter, "send_durable_request_text", None)
        if send is None:
            store.fail_delivery(
                request_id=request.request_id,
                claim_id=claim_id,
                status=DurableRequestStatus.UNSUPPORTED,
                reason="adapter_text_unsupported",
            )
            return True
        try:
            result = await send(
                chat_id=request.target.chat_id,
                request=request,
                metadata={"thread_id": request.target.thread_id},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            terminal = store.fail_delivery(
                request_id=request.request_id,
                claim_id=claim_id,
                status=DurableRequestStatus.DELIVERY_UNKNOWN,
                reason="provider_text_send_outcome_unknown",
            )
            if terminal is not None:
                from gateway.durable_operations import record_durable_delivery_event

                record_durable_delivery_event("text_delivery_unknown")
            logger.warning(
                "Durable request text delivery raised",
                extra={
                    "request_id": request.request_id,
                    "platform": request.target.platform,
                    "exception_class": type(exc).__name__,
                },
            )
            return True

        if result.success:
            if not result.message_id:
                terminal = store.fail_delivery(
                    request_id=request.request_id,
                    claim_id=claim_id,
                    status=DurableRequestStatus.DELIVERY_UNKNOWN,
                    reason="missing_text_message_id",
                )
                if terminal is not None:
                    from gateway.durable_operations import record_durable_delivery_event

                    record_durable_delivery_event("text_delivery_unknown")
                return True
            store.bind_delivery(
                request_id=request.request_id,
                claim_id=claim_id,
                platform=request.target.platform,
                message_id=str(result.message_id),
            )
            return True

        if result.error_kind in {"transient", "rate_limited"}:
            store.release_delivery_claim(
                request_id=request.request_id,
                claim_id=claim_id,
            )
            return False
        if result.error_kind == "unsupported":
            status = DurableRequestStatus.UNSUPPORTED
            reason = "adapter_text_unsupported"
        elif result.error_kind == "delivery_unknown":
            status = DurableRequestStatus.DELIVERY_UNKNOWN
            reason = "provider_text_send_outcome_unknown"
        else:
            status = DurableRequestStatus.DELIVERY_FAILED
            reason = f"provider_text_delivery_{result.error_kind or 'failed'}"
        terminal = store.fail_delivery(
            request_id=request.request_id,
            claim_id=claim_id,
            status=status,
            reason=reason,
        )
        if terminal is not None and status in {
            DurableRequestStatus.DELIVERY_FAILED,
            DurableRequestStatus.DELIVERY_UNKNOWN,
        }:
            from gateway.durable_operations import record_durable_delivery_event

            record_durable_delivery_event(
                "text_delivery_unknown"
                if status is DurableRequestStatus.DELIVERY_UNKNOWN
                else "text_delivery_failed"
            )
        return True


def _adapters_by_platform(adapters: Mapping[Any, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, adapter in adapters.items():
        value = getattr(key, "value", key)
        platform = str(value).strip().lower()
        if platform and adapter is not None:
            result[platform] = adapter
    return result


__all__ = [
    "DurableRequestBridge",
    "DurableWakeDispatchResult",
    "DurableWakeDisposition",
    "DurableWakeHandler",
]
