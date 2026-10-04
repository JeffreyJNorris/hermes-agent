"""Gateway-owned delivery bridge for provider-neutral interactive prompts.

Domain tools call :meth:`InteractivePromptBridge.request_sync` from their worker
thread. Gateway-owned asynchronous code uses
:meth:`InteractivePromptBridge.request_async`. Both paths return a terminal
:class:`~gateway.interactive.PromptOutcome`. Adapters only render the
registered prompt and report provider metadata; all state and decision
validity remain in :mod:`gateway.interactive`.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from agent.async_utils import consume_detached_task_result
from gateway.interactive import (
    InteractivePromptSpec,
    InteractiveMultiSelectSpec,
    MultiSelectHandle,
    MultiSelectOutcome,
    MultiSelectResponse,
    register_multi_select_prompt,
    PromptHandle,
    PromptOutcome,
    PromptOutcomeStatus,
    PromptResponse,
    PromptScope,
    activate_prompt_text_fallback,
    bind_prompt_delivery,
    cancel_prompt,
    expire_prompt,
    register_prompt,
    render_text_fallback,
    wait_for_prompt,
)

logger = logging.getLogger(__name__)

# Added to gateway.platforms.base.SEND_ERROR_KINDS by the integration patch.
UNSUPPORTED_SEND_ERROR_KIND = "unsupported"


class _SendResultLike(Protocol):
    success: bool
    message_id: str | None
    error: str | None
    error_kind: str | None


class _AdapterLike(Protocol):
    async def send_interactive_prompt(
        self,
        chat_id: str,
        prompt: Any,
        metadata: dict | None = None,
    ) -> _SendResultLike: ...

    async def send_interactive_multi_select_prompt(
        self, chat_id: str, prompt: Any, metadata: dict | None = None,
    ) -> _SendResultLike: ...

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict | None = None,
    ) -> _SendResultLike: ...


FallbackSender = Callable[
    [str, str, dict | None],
    Awaitable[_SendResultLike] | _SendResultLike,
]


@dataclass(frozen=True)
class _DeliveryDecision:
    wait_for_resolution: bool
    outcome: PromptOutcome | None = None


class InteractivePromptBridge:
    """Register, deliver, bind, and wait using trusted gateway scope.

    One bridge is constructed for an active gateway request. Its scope is
    derived by the gateway from the inbound event, never from domain-tool input.
    """

    def __init__(
        self,
        *,
        adapter: _AdapterLike,
        scope: PromptScope,
        event_loop: asyncio.AbstractEventLoop,
        metadata: dict | None = None,
        fallback_sender: FallbackSender | None = None,
        event_loop_thread_id: int | None = None,
    ) -> None:
        if adapter is None:
            raise ValueError("adapter is required")
        if not isinstance(scope, PromptScope):
            raise TypeError("scope must be a PromptScope")
        if not isinstance(event_loop, asyncio.AbstractEventLoop):
            raise TypeError("event_loop must be an asyncio event loop")
        if fallback_sender is not None and not callable(fallback_sender):
            raise TypeError("fallback_sender must be callable")

        self._adapter = adapter
        self._scope = scope
        self._event_loop = event_loop
        self._metadata = dict(metadata or {})
        if scope.thread_id is not None:
            self._metadata.setdefault("thread_id", scope.thread_id)
        self._fallback_sender = fallback_sender
        self._event_loop_thread_id = event_loop_thread_id

    @staticmethod
    def _outcome_type(handle):
        return MultiSelectOutcome if isinstance(handle, MultiSelectHandle) else PromptOutcome

    def request_multi_select_sync(self, prompt: InteractiveMultiSelectSpec, *,
                                  resolver: Callable[[MultiSelectResponse], None] | None = None) -> MultiSelectOutcome:
        if not isinstance(prompt, InteractiveMultiSelectSpec):
            raise TypeError("prompt must be an InteractiveMultiSelectSpec")
        return self.request_sync(prompt, resolver=resolver)

    async def request_multi_select_async(self, prompt: InteractiveMultiSelectSpec, *,
                                        resolver: Callable[[MultiSelectResponse], None] | None = None) -> MultiSelectOutcome:
        if not isinstance(prompt, InteractiveMultiSelectSpec):
            raise TypeError("prompt must be an InteractiveMultiSelectSpec")
        return await self.request_async(prompt, resolver=resolver)

    def request_sync(
        self,
        prompt: InteractivePromptSpec,
        *,
        resolver: Callable[[PromptResponse], None] | None = None,
    ) -> PromptOutcome:
        """Synchronous domain callback; never call it on the gateway loop.

        Registration happens in this worker thread. Delivery is scheduled onto
        the gateway event loop, then this worker blocks on the registry handle.
        """
        self._assert_not_event_loop_thread()
        register = register_multi_select_prompt if isinstance(prompt, InteractiveMultiSelectSpec) else register_prompt
        handle = register(prompt, scope=self._scope, resolver=resolver)

        if self._event_loop.is_closed() or not self._event_loop.is_running():
            cancel_prompt(handle.prompt.prompt_id, reason="gateway_loop_unavailable")
            return self._outcome_type(handle)(
                PromptOutcomeStatus.DELIVERY_FAILED,
                reason="gateway_loop_unavailable",
            )

        delivery_future: concurrent.futures.Future[_DeliveryDecision] | None = None
        try:
            delivery_future = asyncio.run_coroutine_threadsafe(
                self._deliver_registered(handle),
                self._event_loop,
            )
            remaining = max(0.0, handle.deadline - time.monotonic())
            decision = delivery_future.result(timeout=remaining)
        except concurrent.futures.TimeoutError:
            # The provider send may be stuck. Cancel the coroutine and expire via
            # the registry; a response that won the race remains authoritative.
            if delivery_future is not None:
                delivery_future.cancel()
            expire_prompt(handle.prompt.prompt_id)
            return wait_for_prompt(handle)
        except Exception as exc:
            if delivery_future is not None:
                delivery_future.cancel()
            if cancel_prompt(
                handle.prompt.prompt_id,
                reason="delivery_bridge_failed",
            ):
                logger.error(
                    "Interactive prompt delivery bridge failed (%s)",
                    type(exc).__name__,
                    extra={
                        "prompt_id": handle.prompt.prompt_id,
                        "platform": self._scope.platform,
                        "exception_class": type(exc).__name__,
                    },
                )
                return self._outcome_type(handle)(
                    PromptOutcomeStatus.DELIVERY_FAILED,
                    reason="delivery_bridge_failed",
                )
            # Cleanup, expiry, or a valid response may have won the race.
            return wait_for_prompt(handle)

        if not decision.wait_for_resolution:
            assert decision.outcome is not None
            return decision.outcome
        return wait_for_prompt(handle)

    async def request_async(
        self,
        prompt: InteractivePromptSpec,
        *,
        resolver: Callable[[PromptResponse], None] | None = None,
    ) -> PromptOutcome:
        """Non-blocking bridge for gateway-owned asynchronous callers.

        Cancelling the caller cancels only this prompt and re-raises
        ``CancelledError``. A response that already resolved remains
        authoritative.
        """
        if asyncio.get_running_loop() is not self._event_loop:
            raise RuntimeError(
                "interactive prompt async request must run on the gateway event loop"
            )
        register = register_multi_select_prompt if isinstance(prompt, InteractiveMultiSelectSpec) else register_prompt
        handle = register(prompt, scope=self._scope, resolver=resolver)
        prompt_id = handle.prompt.prompt_id
        remaining = max(0.0, handle.deadline - time.monotonic())
        delivery_task = asyncio.create_task(self._deliver_registered(handle))
        try:
            done, _pending = await asyncio.wait({delivery_task}, timeout=remaining)
            if delivery_task not in done:
                delivery_task.cancel()
                delivery_task.add_done_callback(consume_detached_task_result)
                expire_prompt(prompt_id)
                # Let the adapter observe cancellation without waiting for one
                # that suppresses it; the registry deadline stays authoritative.
                await asyncio.sleep(0)
                return await asyncio.to_thread(wait_for_prompt, handle)

            try:
                decision = delivery_task.result()
            except Exception as exc:
                if cancel_prompt(
                    prompt_id,
                    reason="delivery_bridge_failed",
                ):
                    logger.error(
                        "Interactive prompt async delivery bridge failed (%s)",
                        type(exc).__name__,
                        extra={
                            "prompt_id": prompt_id,
                            "platform": self._scope.platform,
                            "exception_class": type(exc).__name__,
                        },
                    )
                    return self._outcome_type(handle)(
                        PromptOutcomeStatus.DELIVERY_FAILED,
                        reason="delivery_bridge_failed",
                    )
                return await asyncio.to_thread(wait_for_prompt, handle)

            if not decision.wait_for_resolution:
                assert decision.outcome is not None
                return decision.outcome
            return await asyncio.to_thread(wait_for_prompt, handle)
        except asyncio.CancelledError:
            # Registry state is authoritative. If a valid response won this race,
            # cancel_prompt() is a no-op and the recorded decision stays terminal.
            cancel_prompt(prompt_id, reason="async_request_cancelled")
            if not delivery_task.done():
                delivery_task.cancel()
            delivery_task.add_done_callback(consume_detached_task_result)
            raise

    def _assert_not_event_loop_thread(self) -> None:
        if (
            self._event_loop_thread_id is not None
            and threading.get_ident() == self._event_loop_thread_id
        ):
            raise RuntimeError(
                "interactive prompt synchronous callback cannot run on the "
                "gateway event-loop thread"
            )
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is self._event_loop:
            raise RuntimeError(
                "interactive prompt synchronous callback cannot run on the "
                "gateway event-loop thread"
            )

    async def _deliver_registered(
        self,
        handle: PromptHandle,
    ) -> _DeliveryDecision:
        prompt_id = handle.prompt.prompt_id
        try:
            sender = (self._adapter.send_interactive_multi_select_prompt
                      if isinstance(handle, MultiSelectHandle)
                      else self._adapter.send_interactive_prompt)
            native_result = await sender(
                chat_id=self._scope.chat_id,
                prompt=handle.prompt,
                metadata=self._metadata or None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return self._delivery_failure(
                handle,
                reason="native_delivery_failed",
                exc=exc,
            )

        if not self._looks_like_send_result(native_result):
            return self._delivery_failure(
                handle,
                reason="invalid_native_delivery_result",
            )

        if native_result.success:
            message_id = getattr(native_result, "message_id", None)
            if not message_id:
                return self._delivery_failure(
                    handle,
                    reason="missing_message_id",
                )
            if not bind_prompt_delivery(
                prompt_id=prompt_id,
                platform=self._scope.platform,
                message_id=str(message_id),
            ):
                # Expiration/session cleanup may have beaten delivery binding.
                if not cancel_prompt(
                    prompt_id,
                    reason="delivery_binding_failed",
                ):
                    return _DeliveryDecision(
                        wait_for_resolution=False,
                        outcome=wait_for_prompt(handle),
                    )
                return _DeliveryDecision(
                    wait_for_resolution=False,
                    outcome=self._outcome_type(handle)(
                        PromptOutcomeStatus.DELIVERY_FAILED,
                        reason="delivery_binding_failed",
                    ),
                )
            return _DeliveryDecision(wait_for_resolution=True)

        if getattr(native_result, "error_kind", None) != UNSUPPORTED_SEND_ERROR_KIND:
            # An attempted or ambiguous native failure can mean the provider
            # created a message without returning its identity. Never create a
            # duplicate fallback in this case.
            return self._delivery_failure(
                handle,
                reason="native_delivery_failed",
            )

        # Explicitly unsupported optional feature: reserve the single session
        # fallback slot before sending any ordinary text.
        if not activate_prompt_text_fallback(prompt_id):
            if not cancel_prompt(prompt_id, reason="text_fallback_busy"):
                return _DeliveryDecision(
                    wait_for_resolution=False,
                    outcome=wait_for_prompt(handle),
                )
            return _DeliveryDecision(
                wait_for_resolution=False,
                outcome=self._outcome_type(handle)(
                    PromptOutcomeStatus.UNSUPPORTED,
                    reason="text_fallback_busy",
                ),
            )

        try:
            fallback_result = await self._send_fallback(
                self._scope.chat_id,
                render_text_fallback(handle.prompt),
                self._metadata or None,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return self._delivery_failure(
                handle,
                reason="text_fallback_delivery_failed",
                exc=exc,
            )

        if not self._looks_like_send_result(fallback_result):
            return self._delivery_failure(
                handle,
                reason="invalid_text_fallback_result",
            )
        if fallback_result.success:
            return _DeliveryDecision(wait_for_resolution=True)

        if getattr(fallback_result, "error_kind", None) == UNSUPPORTED_SEND_ERROR_KIND:
            deadline_decision = self._deadline_decision(handle)
            if deadline_decision is not None:
                return deadline_decision
            if not cancel_prompt(prompt_id, reason="text_fallback_unsupported"):
                return _DeliveryDecision(
                    wait_for_resolution=False,
                    outcome=wait_for_prompt(handle),
                )
            return _DeliveryDecision(
                wait_for_resolution=False,
                outcome=self._outcome_type(handle)(
                    PromptOutcomeStatus.UNSUPPORTED,
                    reason="text_fallback_unsupported",
                ),
            )

        return self._delivery_failure(
            handle,
            reason="text_fallback_delivery_failed",
        )

    def _delivery_failure(
        self,
        handle: PromptHandle,
        *,
        reason: str,
        exc: Exception | None = None,
    ) -> _DeliveryDecision:
        prompt_id = handle.prompt.prompt_id
        deadline_decision = self._deadline_decision(handle)
        if deadline_decision is not None:
            return deadline_decision
        if not cancel_prompt(prompt_id, reason=reason):
            return _DeliveryDecision(
                wait_for_resolution=False,
                outcome=wait_for_prompt(handle),
            )
        if exc is not None:
            logger.error(
                "Interactive prompt delivery failed (%s)",
                type(exc).__name__,
                extra={
                    "prompt_id": prompt_id,
                    "platform": self._scope.platform,
                    "delivery_mode": (
                        "text" if reason.startswith("text_fallback") else "native"
                    ),
                    "exception_class": type(exc).__name__,
                },
            )
        else:
            logger.warning(
                "Interactive prompt delivery failed",
                extra={
                    "prompt_id": prompt_id,
                    "platform": self._scope.platform,
                    "delivery_mode": (
                        "text" if reason.startswith("text_fallback") else "native"
                    ),
                    "resolution_status": PromptOutcomeStatus.DELIVERY_FAILED.value,
                },
            )
        return _DeliveryDecision(
            wait_for_resolution=False,
            outcome=self._outcome_type(handle)(
                PromptOutcomeStatus.DELIVERY_FAILED,
                reason=reason,
            ),
        )

    @staticmethod
    def _deadline_decision(handle: PromptHandle) -> _DeliveryDecision | None:
        """Let the registry deadline win over a late infrastructure result."""
        if time.monotonic() < handle.deadline:
            return None
        expire_prompt(handle.prompt.prompt_id)
        return _DeliveryDecision(
            wait_for_resolution=False,
            outcome=wait_for_prompt(handle),
        )

    async def _send_fallback(
        self,
        chat_id: str,
        content: str,
        metadata: dict | None,
    ) -> _SendResultLike:
        if self._fallback_sender is None:
            return await self._adapter.send(
                chat_id=chat_id,
                content=content,
                metadata=metadata,
            )
        result = self._fallback_sender(chat_id, content, metadata)
        if inspect.isawaitable(result):
            return await result
        return result

    @staticmethod
    def _looks_like_send_result(result: object) -> bool:
        return result is not None and isinstance(getattr(result, "success", None), bool)


__all__ = [
    "FallbackSender",
    "InteractivePromptBridge",
    "UNSUPPORTED_SEND_ERROR_KIND",
]
