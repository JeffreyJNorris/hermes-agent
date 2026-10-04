"""Atomic multi-select delivery within the initiating gateway turn."""

from gateway.interactive import MultiSelectOutcome, PromptOutcomeStatus, PromptScope
from gateway.interactive_bridge import InteractivePromptBridge


class MultiSelectTurnMixin:
    def _interactive_multi_select_bridge(self):
        ctx = self._ctx
        if (ctx._status_adapter is None or ctx._loop_for_step is None
                or not callable(ctx._run_still_current) or not ctx._run_still_current()):
            return None
        source = ctx.source
        if source is None or not ctx.session_key or not source.chat_id or not source.user_id:
            return None
        return InteractivePromptBridge(
            adapter=ctx._status_adapter,
            scope=PromptScope(
                session_key=str(ctx.session_key), platform=source.platform.value,
                chat_id=str(source.chat_id),
                thread_id=str(source.thread_id) if source.thread_id is not None else None,
                expected_actor_id=str(source.user_id),
            ),
            event_loop=ctx._loop_for_step,
            metadata=ctx._status_thread_metadata,
        )

    def _interactive_multi_select_callback_sync(self, prompt):
        bridge = self._interactive_multi_select_bridge()
        if bridge is None:
            return MultiSelectOutcome(PromptOutcomeStatus.UNSUPPORTED, reason="trusted_scope_unavailable")
        return bridge.request_multi_select_sync(prompt)

    async def request_multi_select_async(self, prompt):
        bridge = self._interactive_multi_select_bridge()
        if bridge is None:
            return MultiSelectOutcome(PromptOutcomeStatus.UNSUPPORTED, reason="trusted_scope_unavailable")
        return await bridge.request_multi_select_async(prompt)

    def _multi_select_callback_for_turn(self):
        return (self._interactive_multi_select_callback_sync
                if self._interactive_multi_select_bridge() is not None else None)
