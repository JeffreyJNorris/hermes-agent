from __future__ import annotations

import json
from pathlib import Path

import pytest

from agent.tool_dispatch_helpers import _NEVER_PARALLEL_TOOLS
from gateway.durable_requests import (
    DurableRequestStatus,
    DurableResponderMode,
    DurableWakeStatus,
    get_durable_request_store,
    reset_durable_request_store_cache,
)
from gateway.session_context import (
    clear_session_vars,
    reset_session_vars,
    set_session_vars,
)
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from tools.durable_request_tool import (
    DURABLE_REQUEST_SCHEMA,
    check_durable_request_requirements,
    durable_request_tool,
)
from tools.registry import registry
from tools.tool_search import is_deferrable_tool_name
from toolsets import TOOLSETS, _HERMES_CORE_TOOLS, resolve_toolset


@pytest.fixture
def configured_home(tmp_path: Path):
    (tmp_path / "config.yaml").write_text(
        """durable_requests:
  enabled: true
  inbox:
    platform: discord
    chat_id: "123456789012345678"
    thread_id: "123456789012345679"
  default_ttl_seconds: 3600
  max_ttl_seconds: 7200
  terminal_retention_seconds: 86400
  scheduled_responder_policy: any_authorized
  command_responses:
    enabled: true
    allow_on_native_delivery: false
    allow_text_delivery_fallback: true
""",
        encoding="utf-8",
    )
    token = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    reset_session_vars()
    try:
        yield tmp_path
    finally:
        reset_session_vars()
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)


def _bind_live_session(
    *,
    session_key: str = "discord:guild:user",
    actor: str = "42",
    session_id: str = "",
    ui_session_id: str = "",
    profile: str = "",
    chat_id: str = "987654321098765432",
    thread_id: str = "",
):
    return set_session_vars(
        platform="discord",
        session_key=session_key,
        session_id=session_id,
        ui_session_id=ui_session_id,
        profile=profile,
        user_id=actor,
        chat_id=chat_id,
        thread_id=thread_id,
        cron_session="",
        cron_job_id="",
    )


def _bind_cron(*, job_id: str = "job-17", profile: str = ""):
    return set_session_vars(
        platform="",
        session_key="",
        user_id="",
        profile=profile,
        cron_session="1",
        cron_job_id=job_id,
        async_delivery=False,
    )


def test_tool_registers_with_narrow_toolset() -> None:
    entry = registry.get_entry("durable_request")

    assert entry is not None
    assert entry.toolset == "durable_request"
    assert entry.schema is DURABLE_REQUEST_SCHEMA
    assert entry.is_async is False
    assert entry.check_fn is check_durable_request_requirements


def test_schema_does_not_expose_delivery_or_authorization_scope() -> None:
    properties = DURABLE_REQUEST_SCHEMA["parameters"]["properties"]

    assert DURABLE_REQUEST_SCHEMA["parameters"]["required"] == ["action"]
    for forbidden in (
        "platform",
        "chat_id",
        "thread_id",
        "actor_id",
        "message_id",
        "style",
        "command",
        "callback",
    ):
        assert forbidden not in properties


def test_tool_is_exposed_to_discord_and_cron_only() -> None:
    assert "durable_request" in resolve_toolset("hermes-discord")
    assert "durable_request" in resolve_toolset("hermes-cron")
    assert "durable_request" not in resolve_toolset("hermes-cli")
    assert "durable_request" not in resolve_toolset("hermes-telegram")
    assert "durable_request" not in _HERMES_CORE_TOOLS
    assert TOOLSETS["durable_request"]["tools"] == ["durable_request"]


def test_tool_is_never_parallel_and_not_deferred() -> None:
    assert "durable_request" in _NEVER_PARALLEL_TOOLS
    assert is_deferrable_tool_name("durable_request") is False


def test_live_post_derives_target_origin_and_actor_from_trusted_context(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session()
    try:
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Approve release",
                question="Should the reviewed release continue?",
                options=["Approve", "Reject"],
                reference="release-42",
            )
        )
    finally:
        clear_session_vars(tokens)

    request = get_durable_request_store().get(request_id=payload["request_id"])
    assert payload["status"] == "pending"
    assert payload["delivery"] == "queued"
    assert payload["created"] is True
    assert request is not None
    assert request.target.platform == "discord"
    assert request.target.chat_id == "987654321098765432"
    assert request.target.thread_id is None
    assert request.origin_type == "session"
    assert request.origin_id == "discord:guild:user"
    assert request.responder_mode is DurableResponderMode.INITIATOR
    assert request.expected_actor_id == "42"
    assert request.command_response_allowed is False
    assert request.text_command_fallback_allowed is True

    serialized = json.dumps(payload)
    for forbidden in (
        "123456789012345678",
        "123456789012345679",
        "discord:guild:user",
        "actor_id",
        "message_id",
        "option_1",
    ):
        assert forbidden not in serialized


def test_cron_requires_reference_and_is_idempotent(configured_home: Path) -> None:
    tokens = _bind_cron()
    try:
        missing = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
            )
        )
        first = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
                reference="weekly-2026-W32",
            )
        )
        second = json.loads(
            durable_request_tool(
                action="post",
                title="Changed display text is ignored by idempotency",
                question="Continue now?",
                options=["Yes", "No"],
                reference="weekly-2026-W32",
            )
        )
    finally:
        clear_session_vars(tokens)

    assert missing == {"error": "reference is required for scheduled requests"}
    assert first["created"] is True
    assert second["created"] is False
    assert second["request_id"] == first["request_id"]
    request = get_durable_request_store().get(request_id=first["request_id"])
    assert request.origin_type == "cron"
    assert request.origin_id == "job-17"
    assert request.responder_mode is DurableResponderMode.ANY_AUTHORIZED
    assert request.expected_actor_id is None
    assert request.target.chat_id == "123456789012345678"
    assert request.target.thread_id == "123456789012345679"


def test_live_thread_post_targets_exact_thread(configured_home: Path) -> None:
    tokens = _bind_live_session(
        chat_id="987654321098765433", thread_id="987654321098765433"
    )
    try:
        payload = json.loads(durable_request_tool(
            action="post", title="Review", question="Continue?", options=["Yes", "No"]
        ))
    finally:
        clear_session_vars(tokens)
    request = get_durable_request_store().get(request_id=payload["request_id"])
    assert request.target.chat_id == "987654321098765433"
    assert request.target.thread_id == "987654321098765433"


@pytest.mark.parametrize(
    ("chat_id", "thread_id"),
    [("", ""), ("0", ""), ("not-a-channel", ""),
     ("987654321098765432", "987654321098765433")],
)
def test_live_post_without_valid_channel_never_falls_back_to_inbox(
    configured_home: Path, chat_id: str, thread_id: str
) -> None:
    tokens = _bind_live_session(chat_id=chat_id, thread_id=thread_id)
    try:
        payload = json.loads(durable_request_tool(
            action="post", title="Review", question="Continue?", options=["Yes", "No"]
        ))
    finally:
        clear_session_vars(tokens)
    assert "error" in payload
    assert get_durable_request_store().claim_next_pending(["discord"]) is None


def test_status_and_cancel_do_not_require_current_delivery_channel(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session()
    try:
        posted = json.loads(durable_request_tool(
            action="post", title="Review", question="Continue?", options=["Yes", "No"]
        ))
    finally:
        clear_session_vars(tokens)

    tokens = _bind_live_session(chat_id="")
    try:
        status = json.loads(durable_request_tool(action="status", request_id=posted["request_id"]))
        cancelled = json.loads(durable_request_tool(action="cancel", request_id=posted["request_id"]))
    finally:
        clear_session_vars(tokens)
    assert status["status"] == "pending"
    assert cancelled["status"] == "cancelled"


def test_status_returns_selected_index_and_full_label(configured_home: Path) -> None:
    tokens = _bind_live_session()
    try:
        posted = json.loads(
            durable_request_tool(
                action="post",
                title="Target",
                question="Choose target.",
                options=["Staging", "Production"],
                reference="target",
            )
        )
        store = get_durable_request_store()
        claim = store.claim_next_pending(["discord"])
        assert claim is not None
        bound = store.bind_delivery(
            request_id=posted["request_id"],
            claim_id=claim.claim_id,
            platform="discord",
            message_id="message-1",
        )
        assert bound is not None
        result = store.resolve(
            request_id=posted["request_id"],
            option_token="2",
            platform="discord",
            channel_id="987654321098765432",
            message_id="message-1",
            actor_id="42",
        )
        assert result.request is not None

        payload = json.loads(
            durable_request_tool(action="status", reference="target")
        )
    finally:
        clear_session_vars(tokens)

    assert payload["status"] == "selected"
    assert payload["selected_index"] == 2
    assert payload["selected_label"] == "Production"
    assert "resolved_at" in payload


def test_lookup_and_cancel_are_origin_scoped(configured_home: Path) -> None:
    first_tokens = _bind_live_session(session_key="session-a", actor="1")
    try:
        posted = json.loads(
            durable_request_tool(
                action="post",
                title="Choose",
                question="Choose one.",
                options=["A", "B"],
                reference="same",
            )
        )
    finally:
        clear_session_vars(first_tokens)

    second_tokens = _bind_live_session(session_key="session-b", actor="2")
    try:
        hidden = json.loads(
            durable_request_tool(action="status", request_id=posted["request_id"])
        )
        cancelled = json.loads(
            durable_request_tool(action="cancel", request_id=posted["request_id"])
        )
    finally:
        clear_session_vars(second_tokens)

    assert hidden == {"status": "not_found"}
    assert cancelled == {"status": "not_found"}
    assert (
        get_durable_request_store().get(request_id=posted["request_id"]).status
        is DurableRequestStatus.PENDING_DELIVERY
    )


def test_cancel_returns_terminal_normal_result(configured_home: Path) -> None:
    tokens = _bind_live_session()
    try:
        posted = json.loads(
            durable_request_tool(
                action="post",
                title="Choose",
                question="Choose one.",
                options=["A", "B"],
            )
        )
        payload = json.loads(
            durable_request_tool(
                action="cancel",
                request_id=posted["request_id"],
            )
        )
    finally:
        clear_session_vars(tokens)

    assert payload["status"] == "cancelled"
    assert payload["reason"] == "cancelled_by_origin"
    assert "error" not in payload


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"action": "bad"}, "action must be post, status, or cancel"),
        (
            {
                "action": "post",
                "title": "T",
                "question": "Q",
                "options": ["only"],
            },
            "options must contain between 2 and 5 choices",
        ),
        (
            {
                "action": "post",
                "title": "T",
                "question": "Q",
                "options": [" First ", "first"],
            },
            "options must have unique labels",
        ),
        (
            {"action": "status"},
            "provide exactly one of request_id or reference",
        ),
        (
            {
                "action": "status",
                "request_id": "drq_x",
                "reference": "both",
            },
            "provide exactly one of request_id or reference",
        ),
    ],
)
def test_invalid_arguments_return_tool_error(
    configured_home: Path,
    arguments: dict[str, object],
    message: str,
) -> None:
    tokens = _bind_live_session()
    try:
        payload = json.loads(durable_request_tool(**arguments))
    finally:
        clear_session_vars(tokens)

    assert payload == {"error": message}


def test_disabled_configuration_hides_and_rejects_tool(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        "durable_requests:\n  enabled: false\n",
        encoding="utf-8",
    )
    token = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    try:
        assert check_durable_request_requirements() is False
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Choose",
                question="Choose one.",
                options=["A", "B"],
            )
        )
    finally:
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)

    assert payload == {"error": "durable requests are not configured"}


def test_invalid_discord_inbox_hides_tool(tmp_path: Path) -> None:
    (tmp_path / "config.yaml").write_text(
        """durable_requests:
  enabled: true
  inbox:
    platform: discord
    chat_id: not-a-channel-id
""",
        encoding="utf-8",
    )
    token = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    try:
        assert check_durable_request_requirements() is False
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Choose",
                question="Choose one.",
                options=["A", "B"],
            )
        )
    finally:
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)

    assert payload == {
        "error": (
            "durable requests are unavailable: durable request inbox chat_id "
            "must be a Discord channel id"
        )
    }


def test_live_wake_post_captures_trusted_session_lineage(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session(
        session_id="physical-session",
        ui_session_id="ui-session",
        profile="operations",
    )
    try:
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
                reference="wake-live",
                wake_on_terminal=True,
            )
        )
    finally:
        clear_session_vars(tokens)

    request = get_durable_request_store().get(request_id=payload["request_id"])
    assert payload["wake"] == "requested"
    assert request is not None
    assert request.wake_requested is True
    assert request.wake_status is DurableWakeStatus.NOT_REQUESTED
    assert request.origin_session_id == "physical-session"
    assert request.origin_ui_session_id == "ui-session"
    assert request.origin_profile == "operations"


def test_poll_only_post_does_not_persist_wake_routing_metadata(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session(
        session_id="physical-session",
        ui_session_id="ui-session",
        profile="operations",
    )
    try:
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
                reference="poll-only",
            )
        )
    finally:
        clear_session_vars(tokens)

    request = get_durable_request_store().get(request_id=payload["request_id"])
    assert payload["wake"] == "not_requested"
    assert request is not None
    assert request.origin_session_id is None
    assert request.origin_ui_session_id is None
    assert request.origin_profile is None


def test_live_wake_post_fails_closed_without_physical_session_id(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session()
    try:
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
                wake_on_terminal=True,
            )
        )
    finally:
        clear_session_vars(tokens)

    assert payload == {
        "error": "session wake-up requires a durable origin session id"
    }


def test_cron_wake_post_uses_scheduler_owned_origin_and_profile(
    configured_home: Path,
) -> None:
    tokens = _bind_cron(profile="operations")
    try:
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
                reference="wake-cron",
                wake_on_terminal=True,
            )
        )
    finally:
        clear_session_vars(tokens)

    request = get_durable_request_store().get(request_id=payload["request_id"])
    assert payload["wake"] == "requested"
    assert request is not None
    assert request.origin_type == "cron"
    assert request.origin_id == "job-17"
    assert request.origin_profile == "operations"
    assert request.origin_session_id is None


def test_wake_status_is_structural_and_hides_routing_metadata(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session(session_id="physical-session")
    try:
        posted = json.loads(
            durable_request_tool(
                action="post",
                title="Target",
                question="Choose target.",
                options=["Staging", "Production"],
                reference="wake-status",
                wake_on_terminal=True,
            )
        )
        store = get_durable_request_store()
        claim = store.claim_next_pending(["discord"])
        assert claim is not None
        assert store.bind_delivery(
            request_id=posted["request_id"],
            claim_id=claim.claim_id,
            platform="discord",
            message_id="message-1",
        ) is not None
        assert store.resolve(
            request_id=posted["request_id"],
            option_token="2",
            platform="discord",
            channel_id="987654321098765432",
            message_id="message-1",
            actor_id="42",
        ).request is not None

        pending = json.loads(
            durable_request_tool(action="status", reference="wake-status")
        )
        wake_claim = store.claim_next_wake()
        assert wake_claim is not None
        assert store.complete_wake(
            request_id=posted["request_id"],
            claim_id=wake_claim.claim_id,
        )
        delivered = json.loads(
            durable_request_tool(action="status", reference="wake-status")
        )
    finally:
        clear_session_vars(tokens)

    assert pending["wake"] == "pending"
    assert delivered["wake"] == "delivered"
    serialized = json.dumps(delivered)
    for forbidden in (
        "physical-session",
        "message-1",
        "123456789012345679",
        "wake_claim",
        "origin_session_id",
    ):
        assert forbidden not in serialized


def test_invalid_wake_flag_uses_standard_tool_error(
    configured_home: Path,
) -> None:
    tokens = _bind_live_session(session_id="physical-session")
    try:
        payload = json.loads(
            durable_request_tool(
                action="post",
                title="Release",
                question="Continue?",
                options=["Approve", "Reject"],
                wake_on_terminal="yes",
            )
        )
    finally:
        clear_session_vars(tokens)

    assert payload == {"error": "wake_on_terminal must be a boolean"}


def test_registry_multi_post_and_status_keep_canonical_set(configured_home: Path) -> None:
    tokens = _bind_live_session()
    try:
        posted = json.loads(registry.dispatch("durable_request", {
            "action": "post", "title": "Bills", "question": "Choose available bills.",
            "options": ["Electricity", "Gas", "Water", "Internet", "Other", "Waste"],
            "selection_mode": "multi", "min_selections": 2, "max_selections": 4,
        }))
        store = get_durable_request_store()
        claim = store.claim_next_pending(["discord"])
        assert claim is not None and claim.request.max_selections == 4
        assert store.bind_delivery(request_id=posted["request_id"], claim_id=claim.claim_id,
                                   platform="discord", message_id="multi-message") is not None
        wrong_channel = store.resolve_multi_select(
            request_id=posted["request_id"], option_tokens=("6", "2"),
            platform="discord", channel_id="123456789012345679",
            message_id="multi-message", actor_id="42",
        )
        assert wrong_channel.request is None
        assert wrong_channel.status.value == "scope_mismatch"
        selected = store.resolve_multi_select(request_id=posted["request_id"], option_tokens=("6", "2"),
            platform="discord", channel_id=claim.request.target.chat_id, message_id="multi-message", actor_id="42")
        assert selected.request is not None
        payload = json.loads(registry.dispatch("durable_request", {"action": "status", "request_id": posted["request_id"]}))
    finally:
        clear_session_vars(tokens)
    assert payload["selected_indices"] == [2, 6]
    assert payload["selected_labels"] == ["Gas", "Waste"]
    assert "selected_index" not in payload and "option_ids" not in payload


@pytest.mark.parametrize("policy", [
    {"selection_mode": "single", "max_selections": 2},
    {"selection_mode": "multi", "min_selections": 0},
    {"selection_mode": "multi", "min_selections": True},
    {"selection_mode": "multi", "max_selections": 3},
    {"selection_mode": []},
])
def test_registry_rejects_conflicting_selection_policy(configured_home: Path, policy) -> None:
    tokens = _bind_live_session()
    try:
        result = json.loads(registry.dispatch("durable_request", {
            "action": "post", "title": "Choose", "question": "Which?", "options": ["A", "B"], **policy,
        }))
        assert "error" in result
        assert get_durable_request_store().count_by_status().get(DurableRequestStatus.PENDING_DELIVERY, 0) == 0
    finally:
        clear_session_vars(tokens)
