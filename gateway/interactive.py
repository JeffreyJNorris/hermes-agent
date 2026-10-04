"""Provider-neutral one-shot finite-choice interactive prompts.

The registry in this module is the authoritative source of prompt state. A
provider message, view, button, edited label, or fallback message is only a
presentation of registry state.

Prompt display text is caller-controlled. Callers MUST provide pre-redacted,
display-safe title/body text. This module deliberately logs structural fields
only and never logs prompt bodies or option labels at ordinary log levels.
"""

from __future__ import annotations

import logging
import math
import re
import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Callable

logger = logging.getLogger(__name__)

# Phase-one limits. The option maximum intentionally fits Discord's five-button
# action-row budget. Adapters may impose stricter limits but must fail delivery
# instead of silently removing options.
PROMPT_ID_RANDOM_BYTES = 32
MIN_PROMPT_ID_LENGTH = 32
MAX_PROMPT_ID_LENGTH = 128
MAX_PROMPT_KIND_LENGTH = 64
MAX_PROMPT_TITLE_LENGTH = 256
MAX_PROMPT_BODY_LENGTH = 1_200
MAX_PROMPT_OPTIONS = 5
MIN_MULTI_SELECT_OPTIONS = 2
MAX_MULTI_SELECT_OPTIONS = 10
MAX_PROMPT_OPTION_ID_LENGTH = 128
MAX_PROMPT_OPTION_LABEL_LENGTH = 200
MAX_PROMPT_SCOPE_VALUE_LENGTH = 512
MAX_PROMPT_TIMEOUT_SECONDS = 3_600.0
MAX_RENDERED_PROMPT_LENGTH = 2_000
MAX_PROVIDER_COMPONENT_ID_LENGTH = 100

TOMBSTONE_TTL_SECONDS = 300.0
MAX_TOMBSTONES = 2_048

_PROMPT_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class PromptStyle(str, Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    SUCCESS = "success"
    DANGER = "danger"


@dataclass(frozen=True)
class PromptOption:
    id: str
    label: str
    style: PromptStyle = PromptStyle.SECONDARY


@dataclass(frozen=True)
class InteractivePromptSpec:
    kind: str
    title: str
    body: str
    options: tuple[PromptOption, ...]
    timeout_seconds: float


@dataclass(frozen=True)
class InteractivePrompt:
    prompt_id: str
    kind: str
    title: str
    body: str
    options: tuple[PromptOption, ...]
    timeout_seconds: float


@dataclass(frozen=True)
class InteractiveMultiSelectSpec(InteractivePromptSpec):
    min_selections: int
    max_selections: int


@dataclass(frozen=True)
class InteractiveMultiSelectPrompt(InteractivePrompt):
    min_selections: int
    max_selections: int


@dataclass(frozen=True)
class MultiSelectResponse:
    prompt_id: str
    option_ids: tuple[str, ...]
    platform: str
    chat_id: str
    thread_id: str | None
    message_id: str | None
    actor_id: str | None


@dataclass(frozen=True)
class PromptScope:
    session_key: str
    platform: str
    chat_id: str
    thread_id: str | None
    expected_actor_id: str | None = None

    @property
    def delivery_channel_id(self) -> str:
        return self.thread_id or self.chat_id


@dataclass(frozen=True)
class PromptResponse:
    prompt_id: str
    option_id: str
    platform: str
    chat_id: str
    thread_id: str | None
    message_id: str | None
    actor_id: str | None


class PromptOutcomeStatus(str, Enum):
    SELECTED = "selected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    UNSUPPORTED = "unsupported"
    DELIVERY_FAILED = "delivery_failed"


@dataclass(frozen=True)
class PromptOutcome:
    status: PromptOutcomeStatus
    response: PromptResponse | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.status is PromptOutcomeStatus.SELECTED:
            if self.response is None:
                raise ValueError("SELECTED outcomes require a response")
        elif self.response is not None:
            raise ValueError("Only SELECTED outcomes may contain a response")


@dataclass(frozen=True)
class MultiSelectOutcome(PromptOutcome):
    response: MultiSelectResponse | None = None


class ResolveStatus(str, Enum):
    RESOLVED = "resolved"
    ALREADY_RESOLVED = "already_resolved"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    NOT_FOUND = "not_found"
    INVALID_OPTION = "invalid_option"
    SCOPE_MISMATCH = "scope_mismatch"
    NOT_DELIVERED = "not_delivered"
    WRONG_DELIVERY_MODE = "wrong_delivery_mode"


@dataclass(frozen=True)
class ResolveResult:
    status: ResolveStatus
    response: PromptResponse | MultiSelectResponse | None = None

    def __post_init__(self) -> None:
        if self.status is ResolveStatus.RESOLVED:
            if self.response is None:
                raise ValueError("RESOLVED results require a response")
        elif self.response is not None:
            raise ValueError("Only RESOLVED results may contain a response")


class _PromptState(str, Enum):
    REGISTERED = "registered"
    DELIVERED_NATIVE = "delivered_native"
    AWAITING_TEXT = "awaiting_text"
    RESOLVED = "resolved"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


@dataclass
class _HandleState:
    event: threading.Event
    outcome: PromptOutcome | MultiSelectOutcome | None = None


class PromptHandle:
    """Opaque handle used by the registry and gateway bridge.

    ``prompt`` is exposed only so the bridge can deliver the centrally generated
    identifier and validated prompt to an adapter. Callers should otherwise
    treat this type as opaque.
    """

    __slots__ = (
        "prompt",
        "deadline",
        "_entry_identity",
        "_registry_identity",
        "_state",
    )

    def __init__(
        self,
        *,
        prompt: InteractivePrompt,
        deadline: float,
        entry_identity: object,
        registry_identity: object,
        state: _HandleState,
    ) -> None:
        self.prompt = prompt
        self.deadline = deadline
        self._entry_identity = entry_identity
        self._registry_identity = registry_identity
        self._state = state

    def __repr__(self) -> str:
        return (
            f"PromptHandle(prompt_id={self.prompt.prompt_id!r}, "
            f"deadline={self.deadline!r})"
        )


class MultiSelectHandle(PromptHandle):
    """Opaque multi-select handle sharing the registry lifecycle."""
    __slots__ = ()


@dataclass
class _PromptEntry:
    prompt: InteractivePrompt | InteractiveMultiSelectPrompt
    scope: PromptScope
    deadline: float
    state: _PromptState
    entry_identity: object
    handle_state: _HandleState
    slot_to_option_id: dict[str, str]
    resolver: Callable[[PromptResponse], None] | None
    bound_platform: str | None = None
    message_id: str | None = None


@dataclass(frozen=True)
class _Tombstone:
    status: PromptOutcomeStatus
    terminal_timestamp: float


def _require_text(
    value: object,
    *,
    field: str,
    maximum: int,
    single_line: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    if not value.strip():
        raise ValueError(f"{field} must not be empty")
    if len(value) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    if single_line and ("\n" in value or "\r" in value):
        raise ValueError(f"{field} must be a single line")
    return value


def _validate_prompt_id(prompt_id: object) -> str:
    value = _require_text(
        prompt_id,
        field="prompt_id",
        maximum=MAX_PROMPT_ID_LENGTH,
        single_line=True,
    )
    if len(value) < MIN_PROMPT_ID_LENGTH or _PROMPT_ID_RE.fullmatch(value) is None:
        raise ValueError("prompt_id is not a valid URL-safe token")
    return value


def _normalize_choice(value: str) -> str:
    return " ".join(value.strip().split()).casefold()


def render_text_fallback(prompt: InteractivePrompt) -> str:
    """Serialize a prompt deterministically without dropping any option."""
    instruction = "Reply with one option:"
    if isinstance(prompt, InteractiveMultiSelectPrompt):
        instruction = (
            f"Reply with {prompt.min_selections}–{prompt.max_selections} option numbers "
            "separated by commas:"
        )
    lines = [prompt.title, "", prompt.body, "", instruction]
    lines.extend(
        f"{slot}. {option.label}"
        for slot, option in enumerate(prompt.options, start=1)
    )
    return "\n".join(lines)


def render_native_prompt_content(prompt: InteractivePrompt) -> str:
    """Return substantive ordinary-message content for a native prompt."""
    lines = [f"**{prompt.title}**", "", prompt.body, "", "Options:"]
    lines.extend(
        f"{slot}. {option.label}"
        for slot, option in enumerate(prompt.options, start=1)
    )
    return "\n".join(lines)


def make_component_id(prompt_id: str, slot: int | str) -> str:
    """Build ``ip:<prompt_id>:<slot>`` without exposing application values."""
    valid_prompt_id = _validate_prompt_id(prompt_id)
    slot_text = str(slot)
    if not slot_text.isascii() or not slot_text.isdigit() or int(slot_text) <= 0:
        raise ValueError("slot must be a positive integer")
    component_id = f"ip:{valid_prompt_id}:{slot_text}"
    if len(component_id) > MAX_PROVIDER_COMPONENT_ID_LENGTH:
        raise ValueError("interactive prompt component ID exceeds provider limit")
    return component_id


def parse_component_id(component_id: str) -> tuple[str, str] | None:
    """Parse an exact ``ip:<prompt_id>:<slot>`` component identifier."""
    if not isinstance(component_id, str):
        return None
    if len(component_id) > MAX_PROVIDER_COMPONENT_ID_LENGTH:
        return None
    parts = component_id.split(":")
    if len(parts) != 3 or parts[0] != "ip":
        return None
    prompt_id, slot = parts[1], parts[2]
    try:
        _validate_prompt_id(prompt_id)
    except ValueError:
        return None
    if not slot.isascii() or not slot.isdigit() or int(slot) <= 0:
        return None
    return prompt_id, slot


def validate_selection_bounds(option_count: int, minimum: int, maximum: int) -> None:
    if not MIN_MULTI_SELECT_OPTIONS <= option_count <= MAX_MULTI_SELECT_OPTIONS:
        raise ValueError("multi-select requires between 2 and 10 options")
    if any(isinstance(value, bool) or not isinstance(value, int)
           for value in (minimum, maximum)):
        raise ValueError("selection bounds must be integers")
    if not 1 <= minimum <= maximum <= min(option_count, MAX_MULTI_SELECT_OPTIONS):
        raise ValueError("invalid selection bounds")


def parse_selection_slots(text: str) -> tuple[str, ...] | None:
    """Recognize only ASCII numeric slots separated by commas, retaining duplicates."""
    if not isinstance(text, str):
        return None
    candidate = text.strip()
    if re.fullmatch(r"[0-9]+(?:, *[0-9]+)*", candidate) is None:
        return None
    # Avoid converting unbounded inbound integers. At most ten short slot values
    # can resolve; long values remain recognizable but invalid server-side.
    return tuple(part.strip().lstrip("0") or "0" for part in candidate.split(","))


def make_multi_select_component_id(prompt_id: str) -> str:
    component_id = f"ipm:{_validate_prompt_id(prompt_id)}"
    if len(component_id) > MAX_PROVIDER_COMPONENT_ID_LENGTH:
        raise ValueError("multi-select component ID exceeds provider limit")
    return component_id


def parse_multi_select_component_id(component_id: str) -> str | None:
    if not isinstance(component_id, str) or not component_id.startswith("ipm:"):
        return None
    if len(component_id) > MAX_PROVIDER_COMPONENT_ID_LENGTH:
        return None
    try:
        return _validate_prompt_id(component_id[4:])
    except ValueError:
        return None


class InteractivePromptRegistry:
    """Thread-safe in-memory authority for one-shot finite-choice prompts."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        id_factory: Callable[[], str] | None = None,
        tombstone_ttl_seconds: float = TOMBSTONE_TTL_SECONDS,
        max_tombstones: int = MAX_TOMBSTONES,
    ) -> None:
        if not callable(clock):
            raise TypeError("clock must be callable")
        if id_factory is not None and not callable(id_factory):
            raise TypeError("id_factory must be callable")
        if (
            not isinstance(tombstone_ttl_seconds, (int, float))
            or isinstance(tombstone_ttl_seconds, bool)
            or not math.isfinite(float(tombstone_ttl_seconds))
            or float(tombstone_ttl_seconds) <= 0
        ):
            raise ValueError("tombstone_ttl_seconds must be positive and finite")
        if (
            isinstance(max_tombstones, bool)
            or not isinstance(max_tombstones, int)
            or max_tombstones <= 0
        ):
            raise ValueError("max_tombstones must be a positive integer")

        self._clock = clock
        self._id_factory = id_factory or (
            lambda: secrets.token_urlsafe(PROMPT_ID_RANDOM_BYTES)
        )
        self._tombstone_ttl_seconds = float(tombstone_ttl_seconds)
        self._max_tombstones = max_tombstones
        self._registry_identity = object()
        self._lock = threading.RLock()
        self._active: dict[str, _PromptEntry] = {}
        self._session_index: dict[str, set[str]] = {}
        self._platform_index: dict[str, set[str]] = {}
        self._text_by_session: dict[str, str] = {}
        self._tombstones: OrderedDict[str, _Tombstone] = OrderedDict()
        self._multi_submissions = 0
        self._multi_invalid_sets = 0

    def register(
        self,
        prompt: InteractivePromptSpec,
        *,
        scope: PromptScope,
        resolver: Callable[[PromptResponse], None] | None = None,
    ) -> PromptHandle:
        """Validate and publish a prompt before any delivery attempt."""
        self._validate_spec(prompt)
        self._validate_scope(scope)
        if resolver is not None and not callable(resolver):
            raise ValueError("resolver must be callable")

        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            prompt_id = self._new_unique_prompt_id_locked()
            multi = isinstance(prompt, InteractiveMultiSelectSpec)
            prompt_type = InteractiveMultiSelectPrompt if multi else InteractivePrompt
            bounds = ({"min_selections": prompt.min_selections,
                       "max_selections": prompt.max_selections} if multi else {})
            registered = prompt_type(
                **bounds,
                prompt_id=prompt_id,
                kind=prompt.kind,
                title=prompt.title,
                body=prompt.body,
                options=tuple(prompt.options),
                timeout_seconds=float(prompt.timeout_seconds),
            )

            # Prove that every option can be represented before the entry is
            # visible. No adapter may silently trim the option set later.
            if multi:
                make_multi_select_component_id(prompt_id)
            else:
                for slot in range(1, len(registered.options) + 1):
                    make_component_id(prompt_id, slot)
            if len(render_text_fallback(registered)) > MAX_RENDERED_PROMPT_LENGTH:
                raise ValueError("rendered text fallback exceeds delivery budget")
            if len(render_native_prompt_content(registered)) > MAX_RENDERED_PROMPT_LENGTH:
                raise ValueError("rendered native prompt exceeds delivery budget")

            deadline = now + registered.timeout_seconds
            entry_identity = object()
            handle_state = _HandleState(event=threading.Event())
            entry = _PromptEntry(
                prompt=registered,
                scope=scope,
                deadline=deadline,
                state=_PromptState.REGISTERED,
                entry_identity=entry_identity,
                handle_state=handle_state,
                slot_to_option_id={
                    str(slot): option.id
                    for slot, option in enumerate(registered.options, start=1)
                },
                resolver=resolver,
            )
            self._active[prompt_id] = entry
            self._session_index.setdefault(scope.session_key, set()).add(prompt_id)
            self._platform_index.setdefault(scope.platform, set()).add(prompt_id)
            handle_type = MultiSelectHandle if multi else PromptHandle
            handle = handle_type(
                prompt=registered,
                deadline=deadline,
                entry_identity=entry_identity,
                registry_identity=self._registry_identity,
                state=handle_state,
            )

        logger.debug(
            "Interactive prompt registered",
            extra={
                "prompt_id": prompt_id,
                "platform": scope.platform,
                "state": _PromptState.REGISTERED.value,
            },
        )
        return handle

    def bind_delivery(
        self,
        *,
        prompt_id: str,
        platform: str,
        message_id: str,
    ) -> bool:
        """Bind exact native delivery metadata to a REGISTERED prompt."""
        if not all(isinstance(value, str) for value in (prompt_id, platform, message_id)):
            return False
        if not prompt_id or not platform or not message_id:
            return False

        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            entry = self._active.get(prompt_id)
            if entry is None:
                return False
            if now >= entry.deadline:
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.EXPIRED,
                        reason="deadline_elapsed",
                    ),
                    now,
                )
                return False
            if entry.state is _PromptState.DELIVERED_NATIVE:
                return (
                    entry.bound_platform == platform
                    and entry.message_id == message_id
                )
            if entry.state is not _PromptState.REGISTERED:
                return False
            if entry.scope.platform != platform:
                return False
            entry.bound_platform = platform
            entry.message_id = message_id
            entry.state = _PromptState.DELIVERED_NATIVE

        logger.debug(
            "Interactive prompt delivery bound",
            extra={
                "prompt_id": prompt_id,
                "platform": platform,
                "state": _PromptState.DELIVERED_NATIVE.value,
            },
        )
        return True

    def activate_text_fallback(self, prompt_id: str) -> bool:
        """Atomically reserve the session's sole text-fallback slot."""
        if not isinstance(prompt_id, str):
            return False
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            entry = self._active.get(prompt_id)
            if entry is None:
                return False
            if now >= entry.deadline:
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.EXPIRED,
                        reason="deadline_elapsed",
                    ),
                    now,
                )
                return False
            if entry.state is not _PromptState.REGISTERED:
                return False

            session_key = entry.scope.session_key
            occupied_id = self._text_by_session.get(session_key)
            if occupied_id is not None:
                occupied = self._active.get(occupied_id)
                if occupied is None:
                    self._text_by_session.pop(session_key, None)
                elif now >= occupied.deadline:
                    self._terminalize_locked(
                        occupied,
                        PromptOutcome(
                            PromptOutcomeStatus.EXPIRED,
                            reason="deadline_elapsed",
                        ),
                        now,
                    )
                else:
                    return False

            entry.state = _PromptState.AWAITING_TEXT
            entry.bound_platform = None
            entry.message_id = None
            self._text_by_session[session_key] = prompt_id

        logger.debug(
            "Interactive prompt text fallback activated",
            extra={
                "prompt_id": prompt_id,
                "platform": entry.scope.platform,
                "state": _PromptState.AWAITING_TEXT.value,
            },
        )
        return True

    def register_multi_select_prompt(self, prompt: InteractiveMultiSelectSpec, *,
                                     scope: PromptScope, resolver=None) -> MultiSelectHandle:
        if not isinstance(prompt, InteractiveMultiSelectSpec):
            raise TypeError("prompt must be an InteractiveMultiSelectSpec")
        return self.register(prompt, scope=scope, resolver=resolver)

    def resolve(self, *, prompt_id: str, option_token: str, platform: str,
                channel_id: str, message_id: str | None, actor_id: str | None,
                source: str) -> ResolveResult:
        if not isinstance(option_token, str):
            return ResolveResult(ResolveStatus.SCOPE_MISMATCH)
        return self._resolve(prompt_id=prompt_id, option_tokens=(option_token,), multi=False,
                             platform=platform, channel_id=channel_id, message_id=message_id,
                             actor_id=actor_id, source=source)

    def resolve_multi_select_prompt(self, *, option_tokens: tuple[str, ...],
                                    **metadata) -> ResolveResult:
        return self._resolve(option_tokens=option_tokens, multi=True, **metadata)

    def wait_for_multi_select(self, handle: MultiSelectHandle) -> MultiSelectOutcome:
        if not isinstance(handle, MultiSelectHandle):
            raise TypeError("handle must be a MultiSelectHandle")
        return self.wait(handle)

    def _resolve(
        self,
        *,
        prompt_id: str,
        option_tokens: tuple[str, ...],
        multi: bool,
        platform: str,
        channel_id: str,
        message_id: str | None,
        actor_id: str | None,
        source: str,
    ) -> ResolveResult:
        """Validate and atomically store a native or text response."""
        if source not in {"native", "text"}:
            raise ValueError("source must be 'native' or 'text'")
        if not all(
            isinstance(value, str)
            for value in (prompt_id, platform, channel_id)
        ):
            return ResolveResult(ResolveStatus.SCOPE_MISMATCH)

        callback: Callable[[PromptResponse], None] | None = None
        response: PromptResponse | None = None
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            if multi:
                self._multi_submissions += 1
            entry = self._active.get(prompt_id)
            if entry is None:
                return ResolveResult(self._lookup_terminal_status_locked(prompt_id))

            if now >= entry.deadline:
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.EXPIRED,
                        reason="deadline_elapsed",
                    ),
                    now,
                )
                return ResolveResult(ResolveStatus.EXPIRED)

            if source == "native":
                if entry.state is _PromptState.REGISTERED:
                    return ResolveResult(ResolveStatus.NOT_DELIVERED)
                if entry.state is not _PromptState.DELIVERED_NATIVE:
                    return ResolveResult(ResolveStatus.WRONG_DELIVERY_MODE)
            elif entry.state is not _PromptState.AWAITING_TEXT:
                return ResolveResult(ResolveStatus.WRONG_DELIVERY_MODE)

            if entry.scope.platform != platform:
                return ResolveResult(ResolveStatus.SCOPE_MISMATCH)
            if entry.scope.delivery_channel_id != channel_id:
                return ResolveResult(ResolveStatus.SCOPE_MISMATCH)
            if source == "native" and (
                entry.bound_platform != platform
                or entry.message_id != message_id
            ):
                return ResolveResult(ResolveStatus.SCOPE_MISMATCH)
            if (
                entry.scope.expected_actor_id is not None
                and entry.scope.expected_actor_id != actor_id
            ):
                return ResolveResult(ResolveStatus.SCOPE_MISMATCH)

            is_multi = isinstance(entry.prompt, InteractiveMultiSelectPrompt)
            if is_multi != multi:
                return ResolveResult(ResolveStatus.WRONG_DELIVERY_MODE)
            minimum = entry.prompt.min_selections if is_multi else 1
            maximum = entry.prompt.max_selections if is_multi else 1
            if (not isinstance(option_tokens, (tuple, list))
                or not minimum <= len(option_tokens) <= maximum
                or any(not isinstance(slot, str) for slot in option_tokens)
                or len(set(option_tokens)) != len(option_tokens)
                or any(slot not in entry.slot_to_option_id for slot in option_tokens)):
                if is_multi:
                    self._multi_invalid_sets += 1
                return ResolveResult(ResolveStatus.INVALID_OPTION)
            selected = set(option_tokens)
            option_ids = tuple(option_id for slot, option_id in entry.slot_to_option_id.items()
                               if slot in selected)
            response_type = MultiSelectResponse if is_multi else PromptResponse
            selection = {"option_ids": option_ids} if is_multi else {"option_id": option_ids[0]}
            response = response_type(
                **selection,
                prompt_id=prompt_id,
                platform=platform,
                chat_id=entry.scope.chat_id,
                thread_id=entry.scope.thread_id,
                message_id=message_id if source == "native" else None,
                actor_id=actor_id,
            )
            callback = entry.resolver
            self._terminalize_locked(
                entry,
                (MultiSelectOutcome if is_multi else PromptOutcome)(
                    PromptOutcomeStatus.SELECTED,
                    response=response,
                ),
                now,
            )

        # Application code is deliberately invoked after releasing the lock.
        if callback is not None and response is not None:
            try:
                callback(response)
            except Exception as exc:
                logger.error(
                    "Interactive prompt resolver failed (%s)",
                    type(exc).__name__,
                    extra={
                        "prompt_id": response.prompt_id,
                        "exception_class": type(exc).__name__,
                    },
                )

        logger.debug(
            "Interactive prompt resolved",
            extra={
                "prompt_id": prompt_id,
                "platform": platform,
                "resolution_status": ResolveStatus.RESOLVED.value,
                "delivery_mode": source,
            },
        )
        return ResolveResult(ResolveStatus.RESOLVED, response=response)

    def resolve_text_for_session(
        self,
        *,
        session_key: str,
        text: str,
        platform: str,
        channel_id: str,
        actor_id: str | None,
    ) -> ResolveResult | None:
        """Resolve recognized selection syntax for a session fallback.

        ``None`` means the inbound text is unrelated conversational content and
        must continue to clarify or normal dispatch. Numeric input is always
        recognized selection syntax, even if the slot is invalid.
        """
        if not isinstance(text, str):
            return None
        candidate = text.strip()
        if not candidate or candidate.startswith(("/", "!")):
            return None

        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            prompt_id = self._text_by_session.get(session_key)
            if prompt_id is None:
                return None
            entry = self._active.get(prompt_id)
            if entry is None or entry.state is not _PromptState.AWAITING_TEXT:
                self._text_by_session.pop(session_key, None)
                return None

            if isinstance(entry.prompt, InteractiveMultiSelectPrompt):
                slots = parse_selection_slots(candidate)
                if slots is None:
                    return None
            elif candidate.isascii() and candidate.isdigit():
                option_token = str(int(candidate))
            else:
                normalized = _normalize_choice(candidate)
                matches: set[str] = set()
                for slot, option in enumerate(entry.prompt.options, start=1):
                    if normalized in {
                        _normalize_choice(option.label),
                        _normalize_choice(option.id),
                    }:
                        matches.add(str(slot))
                if not matches:
                    return None
                if len(matches) != 1:
                    return ResolveResult(ResolveStatus.INVALID_OPTION)
                option_token = next(iter(matches))

        if isinstance(entry.prompt, InteractiveMultiSelectPrompt):
            return self.resolve_multi_select_prompt(
                prompt_id=prompt_id, option_tokens=slots, platform=platform,
                channel_id=channel_id, message_id=None, actor_id=actor_id, source="text",
            )

        return self.resolve(
            prompt_id=prompt_id,
            option_token=option_token,
            platform=platform,
            channel_id=channel_id,
            message_id=None,
            actor_id=actor_id,
            source="text",
        )

    def wait(self, handle: PromptHandle) -> PromptOutcome:
        """Block for the remaining duration and expire through the registry."""
        if not isinstance(handle, PromptHandle):
            raise TypeError("handle must be a PromptHandle")
        if handle._registry_identity is not self._registry_identity:
            raise ValueError("handle does not belong to this registry")

        # Validate the live entry identity before waiting. A terminal handle is
        # already safe because its immutable outcome was stored before signal.
        if handle._state.outcome is None:
            with self._lock:
                entry = self._active.get(handle.prompt.prompt_id)
                if (
                    entry is None
                    or entry.entry_identity is not handle._entry_identity
                    or entry.handle_state is not handle._state
                ):
                    raise ValueError("handle does not identify an active prompt")

        while True:
            outcome = handle._state.outcome
            if outcome is not None:
                return outcome

            remaining = handle.deadline - self._clock()
            if remaining <= 0:
                self.expire(handle.prompt.prompt_id)
                continue

            handle._state.event.wait(remaining)
            outcome = handle._state.outcome
            if outcome is not None:
                return outcome
            # A false wait return means the authoritative deadline may have
            # elapsed. Expire through the registry; never fabricate an outcome.
            self.expire(handle.prompt.prompt_id)

    def cancel(self, prompt_id: str, *, reason: str = "cancelled") -> bool:
        if not isinstance(reason, str) or not reason:
            reason = "cancelled"
        if not isinstance(prompt_id, str):
            return False
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            entry = self._active.get(prompt_id)
            if entry is None:
                return False
            self._terminalize_locked(
                entry,
                PromptOutcome(PromptOutcomeStatus.CANCELLED, reason=reason),
                now,
            )
        logger.debug(
            "Interactive prompt cancelled",
            extra={
                "prompt_id": prompt_id,
                "platform": entry.scope.platform,
                "state": _PromptState.CANCELLED.value,
            },
        )
        return True

    def expire(self, prompt_id: str, *, only_if_due: bool = False) -> bool:
        if not isinstance(prompt_id, str):
            return False
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            entry = self._active.get(prompt_id)
            if entry is None:
                return False
            if only_if_due and now < entry.deadline:
                return False
            self._terminalize_locked(
                entry,
                PromptOutcome(
                    PromptOutcomeStatus.EXPIRED,
                    reason="deadline_elapsed",
                ),
                now,
            )
        logger.debug(
            "Interactive prompt expired",
            extra={
                "prompt_id": prompt_id,
                "platform": entry.scope.platform,
                "state": _PromptState.EXPIRED.value,
            },
        )
        return True

    def clear_session(self, session_key: str) -> int:
        return self._clear_index(
            index=self._session_index,
            key=session_key,
            reason="session_cleared",
        )

    def clear_platform(self, platform: str) -> int:
        return self._clear_index(
            index=self._platform_index,
            key=platform,
            reason="platform_disconnected",
        )

    def clear_all(self) -> int:
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            entries = list(self._active.values())
            for entry in entries:
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.CANCELLED,
                        reason="gateway_shutdown",
                    ),
                    now,
                )
            return len(entries)

    def remaining_seconds(self, prompt_id: str) -> float | None:
        """Return authoritative remaining duration for provider view timeout."""
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            entry = self._active.get(prompt_id)
            if entry is None:
                return None
            if now >= entry.deadline:
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.EXPIRED,
                        reason="deadline_elapsed",
                    ),
                    now,
                )
                return None
            return entry.deadline - now

    def has_text_fallback_for_session(self, session_key: str) -> bool:
        """Return whether a live text-fallback prompt owns this session slot.

        This narrow query supports the base adapter's active-session bypass. It
        exposes no prompt content or option mapping. The gateway interceptor
        still performs the actual resolution with provider-supplied platform,
        channel, and actor metadata.
        """
        if not isinstance(session_key, str) or not session_key:
            return False
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            prompt_id = self._text_by_session.get(session_key)
            if prompt_id is None:
                return False
            entry = self._active.get(prompt_id)
            if entry is None or entry.state is not _PromptState.AWAITING_TEXT:
                self._text_by_session.pop(session_key, None)
                return False
            if now >= entry.deadline:
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.EXPIRED,
                        reason="deadline_elapsed",
                    ),
                    now,
                )
                return False
            return True

    def selection_metrics(self) -> dict[str, int]:
        """Content-free process counters and live multi-select lifecycle gauges."""
        with self._lock:
            now = self._clock()
            entries = [entry for entry in self._active.values()
                       if isinstance(entry.prompt, InteractiveMultiSelectPrompt)]
            return {"submissions": self._multi_submissions,
                    "invalid_sets": self._multi_invalid_sets,
                    "active": len(entries),
                    "stuck": sum(now >= entry.deadline for entry in entries)}

    def _validate_spec(self, prompt: InteractivePromptSpec) -> None:
        if not isinstance(prompt, InteractivePromptSpec):
            raise ValueError("prompt must be an InteractivePromptSpec")
        _require_text(
            prompt.kind,
            field="kind",
            maximum=MAX_PROMPT_KIND_LENGTH,
            single_line=True,
        )
        _require_text(
            prompt.title,
            field="title",
            maximum=MAX_PROMPT_TITLE_LENGTH,
            single_line=True,
        )
        _require_text(
            prompt.body,
            field="body",
            maximum=MAX_PROMPT_BODY_LENGTH,
        )
        if not isinstance(prompt.options, tuple):
            raise ValueError("options must be a tuple")
        if not prompt.options:
            raise ValueError("at least one option is required")
        multi = isinstance(prompt, InteractiveMultiSelectSpec)
        if multi:
            validate_selection_bounds(len(prompt.options), prompt.min_selections, prompt.max_selections)
        if len(prompt.options) > (MAX_MULTI_SELECT_OPTIONS if multi else MAX_PROMPT_OPTIONS):
            raise ValueError(
                f"option count exceeds phase-one maximum of {MAX_PROMPT_OPTIONS}"
            )

        seen_ids: set[str] = set()
        for option in prompt.options:
            if not isinstance(option, PromptOption):
                raise ValueError("every option must be a PromptOption")
            option_id = _require_text(
                option.id,
                field="option id",
                maximum=MAX_PROMPT_OPTION_ID_LENGTH,
                single_line=True,
            )
            _require_text(
                option.label,
                field="option label",
                maximum=MAX_PROMPT_OPTION_LABEL_LENGTH,
                single_line=True,
            )
            if not isinstance(option.style, PromptStyle):
                raise ValueError("unknown prompt option style")
            if option_id in seen_ids:
                raise ValueError("duplicate option id")
            seen_ids.add(option_id)

        timeout = prompt.timeout_seconds
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError("timeout_seconds must be numeric")
        timeout = float(timeout)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        if timeout > MAX_PROMPT_TIMEOUT_SECONDS:
            raise ValueError(
                f"timeout_seconds exceeds {MAX_PROMPT_TIMEOUT_SECONDS:g}"
            )

    def _validate_scope(self, scope: PromptScope) -> None:
        if not isinstance(scope, PromptScope):
            raise ValueError("scope must be a PromptScope")
        _require_text(
            scope.session_key,
            field="session_key",
            maximum=MAX_PROMPT_SCOPE_VALUE_LENGTH,
            single_line=True,
        )
        _require_text(
            scope.platform,
            field="platform",
            maximum=MAX_PROMPT_SCOPE_VALUE_LENGTH,
            single_line=True,
        )
        _require_text(
            scope.chat_id,
            field="chat_id",
            maximum=MAX_PROMPT_SCOPE_VALUE_LENGTH,
            single_line=True,
        )
        for field, value in (
            ("thread_id", scope.thread_id),
            ("expected_actor_id", scope.expected_actor_id),
        ):
            if value is not None:
                _require_text(
                    value,
                    field=field,
                    maximum=MAX_PROMPT_SCOPE_VALUE_LENGTH,
                    single_line=True,
                )

    def _new_unique_prompt_id_locked(self) -> str:
        for _ in range(32):
            candidate = self._id_factory()
            try:
                prompt_id = _validate_prompt_id(candidate)
            except ValueError as exc:
                raise ValueError("id_factory returned a malformed prompt ID") from exc
            if prompt_id not in self._active and prompt_id not in self._tombstones:
                return prompt_id
        raise RuntimeError("could not generate a unique interactive prompt ID")

    def _clear_index(
        self,
        *,
        index: dict[str, set[str]],
        key: str,
        reason: str,
    ) -> int:
        if not isinstance(key, str):
            return 0
        with self._lock:
            now = self._clock()
            self._prune_tombstones_locked(now)
            prompt_ids = tuple(index.get(key, ()))
            count = 0
            for prompt_id in prompt_ids:
                entry = self._active.get(prompt_id)
                if entry is None:
                    continue
                self._terminalize_locked(
                    entry,
                    PromptOutcome(
                        PromptOutcomeStatus.CANCELLED,
                        reason=reason,
                    ),
                    now,
                )
                count += 1
            return count

    def _terminalize_locked(
        self,
        entry: _PromptEntry,
        outcome: PromptOutcome,
        now: float,
    ) -> None:
        if isinstance(entry.prompt, InteractiveMultiSelectPrompt) and not isinstance(outcome, MultiSelectOutcome):
            outcome = MultiSelectOutcome(outcome.status, reason=outcome.reason)
        prompt_id = entry.prompt.prompt_id
        if prompt_id not in self._active:
            return
        if outcome.status is PromptOutcomeStatus.SELECTED:
            entry.state = _PromptState.RESOLVED
        elif outcome.status is PromptOutcomeStatus.EXPIRED:
            entry.state = _PromptState.EXPIRED
        else:
            entry.state = _PromptState.CANCELLED

        self._active.pop(prompt_id, None)
        self._discard_index_value(
            self._session_index,
            entry.scope.session_key,
            prompt_id,
        )
        self._discard_index_value(
            self._platform_index,
            entry.scope.platform,
            prompt_id,
        )
        if self._text_by_session.get(entry.scope.session_key) == prompt_id:
            self._text_by_session.pop(entry.scope.session_key, None)

        self._tombstones[prompt_id] = _Tombstone(
            status=outcome.status,
            terminal_timestamp=now,
        )
        self._tombstones.move_to_end(prompt_id)
        self._prune_tombstones_locked(now)

        # Store before signal so a woken waiter cannot observe no result.
        entry.handle_state.outcome = outcome
        entry.handle_state.event.set()

    @staticmethod
    def _discard_index_value(
        index: dict[str, set[str]],
        key: str,
        prompt_id: str,
    ) -> None:
        values = index.get(key)
        if values is None:
            return
        values.discard(prompt_id)
        if not values:
            index.pop(key, None)

    def _lookup_terminal_status_locked(self, prompt_id: str) -> ResolveStatus:
        tombstone = self._tombstones.get(prompt_id)
        if tombstone is None:
            return ResolveStatus.NOT_FOUND
        if tombstone.status is PromptOutcomeStatus.SELECTED:
            return ResolveStatus.ALREADY_RESOLVED
        if tombstone.status is PromptOutcomeStatus.EXPIRED:
            return ResolveStatus.EXPIRED
        return ResolveStatus.CANCELLED

    def _prune_tombstones_locked(self, now: float) -> None:
        cutoff = now - self._tombstone_ttl_seconds
        while self._tombstones:
            first_prompt_id = next(iter(self._tombstones))
            first = self._tombstones[first_prompt_id]
            if first.terminal_timestamp > cutoff:
                break
            self._tombstones.popitem(last=False)
        while len(self._tombstones) > self._max_tombstones:
            self._tombstones.popitem(last=False)


_registry = InteractivePromptRegistry()


def register_prompt(
    prompt: InteractivePromptSpec,
    *,
    scope: PromptScope,
    resolver: Callable[[PromptResponse], None] | None = None,
) -> PromptHandle:
    return _registry.register(prompt, scope=scope, resolver=resolver)


def interactive_multi_select_metrics() -> dict[str, int]:
    return _registry.selection_metrics()


def register_multi_select_prompt(prompt: InteractiveMultiSelectSpec, *, scope: PromptScope,
                                 resolver=None) -> MultiSelectHandle:
    return _registry.register_multi_select_prompt(prompt, scope=scope, resolver=resolver)


def resolve_multi_select_prompt(*, option_tokens: tuple[str, ...], **metadata) -> ResolveResult:
    return _registry.resolve_multi_select_prompt(option_tokens=option_tokens, **metadata)


def wait_for_multi_select(handle: MultiSelectHandle) -> MultiSelectOutcome:
    return _registry.wait_for_multi_select(handle)


def bind_prompt_delivery(
    *,
    prompt_id: str,
    platform: str,
    message_id: str,
) -> bool:
    return _registry.bind_delivery(
        prompt_id=prompt_id,
        platform=platform,
        message_id=message_id,
    )


def activate_prompt_text_fallback(prompt_id: str) -> bool:
    return _registry.activate_text_fallback(prompt_id)


def resolve_prompt(
    *,
    prompt_id: str,
    option_token: str,
    platform: str,
    channel_id: str,
    message_id: str | None,
    actor_id: str | None,
    source: str,
) -> ResolveResult:
    return _registry.resolve(
        prompt_id=prompt_id,
        option_token=option_token,
        platform=platform,
        channel_id=channel_id,
        message_id=message_id,
        actor_id=actor_id,
        source=source,
    )


def resolve_text_fallback_for_session(
    *,
    session_key: str,
    text: str,
    platform: str,
    channel_id: str,
    actor_id: str | None,
) -> ResolveResult | None:
    return _registry.resolve_text_for_session(
        session_key=session_key,
        text=text,
        platform=platform,
        channel_id=channel_id,
        actor_id=actor_id,
    )


def wait_for_prompt(handle: PromptHandle) -> PromptOutcome:
    return _registry.wait(handle)


def cancel_prompt(prompt_id: str, *, reason: str = "cancelled") -> bool:
    return _registry.cancel(prompt_id, reason=reason)


def expire_prompt(prompt_id: str, *, only_if_due: bool = False) -> bool:
    return _registry.expire(prompt_id, only_if_due=only_if_due)


def remaining_prompt_seconds(prompt_id: str) -> float | None:
    return _registry.remaining_seconds(prompt_id)


def has_active_text_fallback(session_key: str) -> bool:
    """Narrow facade used to bypass active-session queuing for text replies."""
    return _registry.has_text_fallback_for_session(session_key)


def clear_interactive_session(session_key: str) -> int:
    return _registry.clear_session(session_key)


def clear_interactive_platform(platform: str) -> int:
    return _registry.clear_platform(platform)


def clear_all_interactive_prompts() -> int:
    return _registry.clear_all()


__all__ = [
    "InteractiveMultiSelectSpec", "InteractiveMultiSelectPrompt", "MultiSelectResponse",
    "MultiSelectOutcome", "MultiSelectHandle", "MIN_MULTI_SELECT_OPTIONS", "MAX_MULTI_SELECT_OPTIONS",
    "validate_selection_bounds", "parse_selection_slots", "make_multi_select_component_id",
    "parse_multi_select_component_id", "register_multi_select_prompt",
    "resolve_multi_select_prompt", "wait_for_multi_select", "interactive_multi_select_metrics",
    "InteractivePrompt",
    "InteractivePromptRegistry",
    "InteractivePromptSpec",
    "MAX_PROMPT_OPTIONS",
    "MAX_PROVIDER_COMPONENT_ID_LENGTH",
    "MAX_RENDERED_PROMPT_LENGTH",
    "PromptHandle",
    "PromptOption",
    "PromptOutcome",
    "PromptOutcomeStatus",
    "PromptResponse",
    "PromptScope",
    "PromptStyle",
    "ResolveResult",
    "ResolveStatus",
    "activate_prompt_text_fallback",
    "bind_prompt_delivery",
    "cancel_prompt",
    "clear_all_interactive_prompts",
    "clear_interactive_platform",
    "clear_interactive_session",
    "expire_prompt",
    "has_active_text_fallback",
    "make_component_id",
    "parse_component_id",
    "register_prompt",
    "remaining_prompt_seconds",
    "render_native_prompt_content",
    "render_text_fallback",
    "resolve_prompt",
    "resolve_text_fallback_for_session",
    "wait_for_prompt",
]
