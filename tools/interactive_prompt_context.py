"""Per-turn access to the gateway-owned synchronous prompt callback.

The callback is deliberately context-bound. A domain tool outside an active
gateway request cannot reach provider adapters or prompt registry internals.
Common typed choice and confirmation helpers live in
``gateway.interactive_decisions``.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Callable

from gateway.interactive import InteractivePromptSpec, PromptOutcome, InteractiveMultiSelectSpec, MultiSelectOutcome

InteractivePromptCallback = Callable[[InteractivePromptSpec], PromptOutcome]

_INTERACTIVE_PROMPT_CALLBACK: ContextVar[InteractivePromptCallback | None] = ContextVar(
    "hermes_interactive_prompt_callback",
    default=None,
)


def bind_interactive_prompt_callback(
    callback: InteractivePromptCallback,
) -> Token[InteractivePromptCallback | None]:
    if not callable(callback):
        raise TypeError("interactive prompt callback must be callable")
    return _INTERACTIVE_PROMPT_CALLBACK.set(callback)


def reset_interactive_prompt_callback(
    token: Token[InteractivePromptCallback | None],
) -> None:
    _INTERACTIVE_PROMPT_CALLBACK.reset(token)


def get_interactive_prompt_callback() -> InteractivePromptCallback | None:
    return _INTERACTIVE_PROMPT_CALLBACK.get()


def require_interactive_prompt_callback() -> InteractivePromptCallback:
    callback = get_interactive_prompt_callback()
    if callback is None:
        raise RuntimeError(
            "interactive prompts are unavailable outside an active gateway request"
        )
    return callback


__all__ = [
    "InteractivePromptCallback",
    "bind_interactive_prompt_callback",
    "get_interactive_prompt_callback",
    "require_interactive_prompt_callback",
    "reset_interactive_prompt_callback",
]


InteractiveMultiSelectCallback = Callable[[InteractiveMultiSelectSpec], MultiSelectOutcome]
_INTERACTIVE_MULTI_SELECT_CALLBACK: ContextVar[InteractiveMultiSelectCallback | None] = ContextVar(
    "hermes_interactive_multi_select_callback", default=None,
)


def bind_interactive_multi_select_callback(callback: InteractiveMultiSelectCallback | None):
    if callback is not None and not callable(callback):
        raise TypeError("interactive multi-select callback must be callable")
    return _INTERACTIVE_MULTI_SELECT_CALLBACK.set(callback)


def reset_interactive_multi_select_callback(token) -> None:
    _INTERACTIVE_MULTI_SELECT_CALLBACK.reset(token)


def get_interactive_multi_select_callback() -> InteractiveMultiSelectCallback | None:
    return _INTERACTIVE_MULTI_SELECT_CALLBACK.get()


def require_interactive_multi_select_callback() -> InteractiveMultiSelectCallback:
    callback = get_interactive_multi_select_callback()
    if callback is None:
        raise RuntimeError("multi-select is unavailable outside an active gateway request")
    return callback


__all__ += ["InteractiveMultiSelectCallback", "bind_interactive_multi_select_callback",
            "reset_interactive_multi_select_callback", "get_interactive_multi_select_callback",
            "require_interactive_multi_select_callback"]
