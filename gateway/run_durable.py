"""Durable request and trusted-action workers for the gateway."""

import asyncio
import logging
from pathlib import Path
from typing import Dict

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter
from hermes_constants import get_hermes_home

logger = logging.getLogger("gateway.run")


class GatewayDurableMixin:
    def _start_durable_request_worker(self) -> None:
        """Start the profile-aware durable outbox/reconciliation loop."""
        existing = getattr(self, "_durable_request_task", None)
        if existing is not None and not existing.done():
            return
        self._durable_request_task = self._spawn_supervised(
            self._run_durable_request_worker,
            "durable_request_worker",
            on_spawn=lambda task: setattr(self, "_durable_request_task", task),
        )

    def _start_durable_action_worker(self) -> None:
        """Start the profile-aware trusted-action runner."""
        existing = getattr(self, "_durable_action_task", None)
        if existing is not None and not existing.done():
            return
        self._durable_action_task = self._spawn_supervised(
            self._run_durable_action_worker,
            "durable_action_worker",
            on_spawn=lambda task: setattr(self, "_durable_action_task", task),
        )

    async def _run_durable_action_worker(self) -> None:
        from gateway.durable_action_runner import DurableActionRunner
        from gateway.durable_actions import DurableActionStore
        from gateway.run import _async_profile_runtime_scope

        try:
            while self._running and not self._shutdown_event.is_set():
                profiles: list[tuple[str, Path, Dict[Platform, BasePlatformAdapter]]] = [
                    (
                        "primary",
                        Path(getattr(self, "_durable_request_home", get_hermes_home())),
                        dict(self.adapters),
                    )
                ]
                if getattr(self.config, "multiplex_profiles", False):
                    try:
                        from hermes_cli.profiles import get_profile_dir

                        profiles.extend(
                            (name, Path(get_profile_dir(name)), dict(self._adapters_for_profile(name)))
                            for name, adapters in list(
                                getattr(self, "_profile_adapters", {}).items()
                            )
                        )
                    except Exception:
                        logger.debug(
                            "Could not enumerate durable-action profile scopes",
                            exc_info=True,
                        )

                for profile_name, profile_home, adapters in profiles:
                    async with _async_profile_runtime_scope(profile_home):
                        try:
                            from hermes_cli.config import load_config_readonly

                            raw = load_config_readonly().get("durable_approvals", {})
                            enabled = isinstance(raw, dict) and raw.get("enabled") is True
                            allowed = raw.get("allowed_actions", []) if isinstance(raw, dict) else []
                            if not isinstance(allowed, list) or not all(
                                isinstance(item, str) for item in allowed
                            ):
                                enabled, allowed = False, []

                            async def _notify(
                                claim,
                                _profile=profile_name,
                                _adapters=adapters,
                            ):
                                return await self._dispatch_durable_action_notification(
                                    claim,
                                    profile_name=_profile,
                                    adapters=_adapters,
                                )

                            action_runner = DurableActionRunner(
                                DurableActionStore(profile_home / "state.db"),
                                enabled=enabled,
                                allowed_actions=allowed,
                                notification_handler=_notify,
                            )
                            await action_runner.process_once()
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.warning(
                                "Durable action worker pass failed",
                                extra={
                                    "exception_class": type(exc).__name__,
                                },
                            )

                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
        finally:
            self._durable_action_task = None

    async def _run_durable_request_worker(self) -> None:
        from gateway.durable_request_bridge import DurableRequestBridge
        from gateway.run import _async_profile_runtime_scope

        try:
            while self._running and not self._shutdown_event.is_set():
                profiles: list[tuple[str, Path, Dict[Platform, BasePlatformAdapter]]] = [
                    (
                        "primary",
                        Path(getattr(self, "_durable_request_home", get_hermes_home())),
                        dict(self.adapters),
                    )
                ]
                if getattr(self.config, "multiplex_profiles", False):
                    try:
                        from hermes_cli.profiles import get_profile_dir

                        profiles.extend(
                            (name, Path(get_profile_dir(name)), dict(self._adapters_for_profile(name)))
                            for name, adapters in list(
                                getattr(self, "_profile_adapters", {}).items()
                            )
                        )
                    except Exception:
                        logger.debug(
                            "Could not enumerate durable-request profile scopes",
                            exc_info=True,
                        )

                for profile_name, profile_home, adapters in profiles:
                    async with _async_profile_runtime_scope(profile_home):
                        try:
                            bridge = self._durable_request_bridges.get(profile_name)
                            if bridge is None:
                                bridge = DurableRequestBridge()
                                self._durable_request_bridges[profile_name] = bridge

                            async def _wake_handler(
                                claim,
                                _profile=profile_name,
                                _adapters=adapters,
                            ):
                                return await self._dispatch_durable_request_wake(
                                    claim,
                                    profile_name=_profile,
                                    adapters=_adapters,
                                )

                            set_wake_handler = getattr(
                                bridge,
                                "set_wake_handler",
                                None,
                            )
                            if callable(set_wake_handler):
                                set_wake_handler(_wake_handler)
                            await bridge.process_once(adapters)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.warning(
                                "Durable request worker pass failed",
                                extra={
                                    "profile": profile_name,
                                    "exception_class": type(exc).__name__,
                                },
                            )

                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            self._durable_request_task = None

    async def _dispatch_durable_request_wake(
        self,
        claim,
        *,
        profile_name: str,
        adapters: Dict[Platform, BasePlatformAdapter],
    ):
        """Route one claimed durable terminal result to its trusted origin."""

        from gateway.durable_request_bridge import (
            DurableWakeDispatchResult,
            DurableWakeDisposition,
        )

        request = claim.request
        selected_option = request.selected_option
        selected_index = None
        if selected_option is not None:
            selected_index = next(
                index
                for index, option in enumerate(request.options, start=1)
                if option.id == selected_option.id
            )

        selected = [
            (index, option.label)
            for index, option in enumerate(request.options, start=1)
            if option.id in request.selected_option_ids
        ]
        multi_selection = {
            "selected_indices": [index for index, _ in selected],
            "selected_labels": [label for _, label in selected],
        } if request.selection_mode == "multi" else {}

        if request.origin_type == "cron":
            try:
                from cron.scheduler import enqueue_durable_request_continuation

                outcome, reason = await asyncio.to_thread(
                    enqueue_durable_request_continuation,
                    job_id=request.origin_id,
                    request_id=request.request_id,
                    reference=request.reference,
                    status=request.status.value,
                    title=request.title,
                    question=request.body,
                    selected_index=selected_index,
                    selected_label=(
                        None if selected_option is None else selected_option.label
                    ),
                    terminal_reason=request.terminal_reason,
                    **multi_selection,
                    adapters=adapters,
                    loop=asyncio.get_running_loop(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Durable cron continuation dispatch failed",
                    extra={
                        "request_id": request.request_id,
                        "exception_class": type(exc).__name__,
                    },
                )
                return DurableWakeDispatchResult(
                    DurableWakeDisposition.RETRY,
                    "cron_continuation_error",
                )
            if outcome == "accepted":
                return DurableWakeDispatchResult(DurableWakeDisposition.DELIVERED)
            if outcome == "dropped":
                return DurableWakeDispatchResult(
                    DurableWakeDisposition.DROPPED,
                    reason or "origin_job_unavailable",
                )
            return DurableWakeDispatchResult(
                DurableWakeDisposition.RETRY,
                reason or "cron_continuation_deferred",
            )

        if request.origin_type != "session":
            return DurableWakeDispatchResult(
                DurableWakeDisposition.DROPPED,
                "unsupported_origin_type",
            )
        parent_session_id = str(request.origin_session_id or "").strip()
        if not parent_session_id:
            return DurableWakeDispatchResult(
                DurableWakeDisposition.DROPPED,
                "origin_session_missing",
            )

        verdict = await self._classify_completion_target(parent_session_id)
        if verdict == "terminal":
            return DurableWakeDispatchResult(
                DurableWakeDisposition.DROPPED,
                "origin_session_ended",
            )
        if verdict == "retry":
            return DurableWakeDispatchResult(
                DurableWakeDisposition.RETRY,
                "origin_session_unavailable",
            )

        event_profile = request.origin_profile
        if not event_profile and profile_name != "primary":
            event_profile = profile_name
        evt = {
            "type": "durable_request_terminal",
            "request_id": request.request_id,
            "session_key": request.origin_id,
            "parent_session_id": parent_session_id,
            "origin_session_id": parent_session_id,
            "origin_ui_session_id": request.origin_ui_session_id or "",
            "profile": event_profile or "",
            "reference": request.reference or "",
            "status": request.status.value,
            "title": request.title,
            "question": request.body,
            "selected_index": selected_index,
            "selected_label": (
                None if selected_option is None else selected_option.label
            ),
            **multi_selection,
            "terminal_reason": request.terminal_reason or "",
            "message_id": f"durable-request:{request.request_id}",
        }
        source = await asyncio.to_thread(self._build_process_event_source, evt)
        if source is None:
            return DurableWakeDispatchResult(
                DurableWakeDisposition.DROPPED,
                "origin_route_missing",
            )
        if self._delivery_adapter_for(source) is None:
            return DurableWakeDispatchResult(
                DurableWakeDisposition.RETRY,
                "origin_adapter_unavailable",
            )
        from gateway.run import _format_gateway_process_notification
        synth_text = _format_gateway_process_notification(evt)
        if not synth_text:
            return DurableWakeDispatchResult(
                DurableWakeDisposition.DROPPED,
                "wake_format_failed",
            )
        delivered = await self._inject_watch_notification(synth_text, evt)
        if delivered is True:
            return DurableWakeDispatchResult(DurableWakeDisposition.DELIVERED)
        if delivered is False:
            return DurableWakeDispatchResult(
                DurableWakeDisposition.RETRY,
                "origin_delivery_failed",
            )
        return DurableWakeDispatchResult(
            DurableWakeDisposition.RETRY,
            "origin_adapter_unavailable",
        )

    async def _dispatch_durable_action_notification(
        self,
        claim,
        *,
        profile_name: str,
        adapters: Dict[Platform, BasePlatformAdapter],
    ):
        """Route one bounded trusted-action outcome to its verified origin."""
        from gateway.durable_action_runner import (
            DurableActionNotificationDisposition,
            DurableActionNotificationResult,
        )

        if claim.origin_type == "cron":
            try:
                from cron.scheduler import enqueue_durable_action_continuation

                outcome, reason = await asyncio.to_thread(
                    enqueue_durable_action_continuation,
                    job_id=claim.origin_id,
                    request_id=claim.request_id,
                    action_type=claim.action_type,
                    action_status=claim.action_status.value,
                    outcome_reason=claim.outcome_reason,
                    adapters=adapters,
                    loop=asyncio.get_running_loop(),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Durable action cron notification failed",
                    extra={
                        "request_id": claim.request_id,
                        "exception_class": type(exc).__name__,
                    },
                )
                return DurableActionNotificationResult(
                    DurableActionNotificationDisposition.RETRY,
                    "cron_continuation_error",
                )
            return DurableActionNotificationResult(
                {
                    "accepted": DurableActionNotificationDisposition.DELIVERED,
                    "dropped": DurableActionNotificationDisposition.DROPPED,
                }.get(outcome, DurableActionNotificationDisposition.RETRY),
                reason,
            )

        if claim.origin_type != "session":
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.DROPPED,
                "unsupported_origin_type",
            )
        parent_session_id = str(claim.origin_session_id or "").strip()
        if not parent_session_id:
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.DROPPED,
                "origin_session_missing",
            )
        verdict = await self._classify_completion_target(parent_session_id)
        if verdict == "terminal":
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.DROPPED,
                "origin_session_ended",
            )
        if verdict == "retry":
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.RETRY,
                "origin_session_unavailable",
            )

        event_profile = claim.origin_profile
        if not event_profile and profile_name != "primary":
            event_profile = profile_name
        evt = {
            "type": "durable_action_terminal",
            "request_id": claim.request_id,
            "action_type": claim.action_type,
            "action_status": claim.action_status.value,
            "outcome_reason": claim.outcome_reason or "",
            "session_key": claim.origin_id,
            "parent_session_id": parent_session_id,
            "origin_session_id": parent_session_id,
            "origin_ui_session_id": claim.origin_ui_session_id or "",
            "profile": event_profile or "",
            "message_id": f"durable-action:{claim.request_id}",
        }
        source = await asyncio.to_thread(self._build_process_event_source, evt)
        if source is None:
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.DROPPED,
                "origin_route_missing",
            )
        if self._delivery_adapter_for(source) is None:
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.RETRY,
                "origin_adapter_unavailable",
            )
        from gateway.run import _format_gateway_process_notification
        synth_text = _format_gateway_process_notification(evt)
        if not synth_text:
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.DROPPED,
                "notification_format_failed",
            )
        delivered = await self._inject_watch_notification(synth_text, evt)
        if delivered is True:
            return DurableActionNotificationResult(
                DurableActionNotificationDisposition.DELIVERED
            )
        return DurableActionNotificationResult(
            DurableActionNotificationDisposition.RETRY,
            "origin_delivery_failed"
            if delivered is False
            else "origin_adapter_unavailable",
        )
