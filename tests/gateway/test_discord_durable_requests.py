from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from gateway.interactive import InteractivePrompt, PromptOption
from gateway.durable_requests import (
    DurableRequest,
    DurableRequestOption,
    DurableRequestStatus,
    DurableRequestTarget,
    DurableResponderMode,
    DurableResolveResult,
    DurableResolveStatus,
    render_durable_request_content,
    render_durable_request_text_command_content,
)


class _FakeView:
    def __init__(self, *, timeout=None):
        self.timeout = timeout
        self.children = []
        self.stopped = False

    def add_item(self, item):
        self.children.append(item)

    def remove_item(self, item):
        if item in self.children:
            self.children.remove(item)

    def stop(self):
        self.stopped = True


class _FakeButton:
    def __init__(self, *, label=None, style=None, custom_id=None, **_):
        self.label = label
        self.style = style
        self.custom_id = custom_id
        self.disabled = False
        self.callback = None


class _FakeSelect:
    def __init__(self, *_, **kwargs):
        self.options = kwargs.get("options", [])
        self.custom_id = kwargs.get("custom_id")
        self.min_values = kwargs.get("min_values", 1)
        self.max_values = kwargs.get("max_values", 1)
        self.disabled = False
        self.values = []
        self.callback = None


def _button_decorator(**kwargs):
    def decorate(function):
        function.__button_kwargs__ = kwargs
        return function

    return decorate


class _FakeAllowedMentions:
    sentinel = object()

    @classmethod
    def none(cls):
        return cls.sentinel


class _FakeColor:
    @staticmethod
    def orange():
        return "orange"

    @staticmethod
    def blue():
        return "blue"

    @staticmethod
    def dark_grey():
        return "dark_grey"

    @staticmethod
    def green():
        return "green"

    @staticmethod
    def greyple():
        return "greyple"

    @staticmethod
    def purple():
        return "purple"

    @staticmethod
    def red():
        return "red"


def _fake_discord():
    styles = SimpleNamespace(
        primary="primary",
        secondary="secondary",
        success="success",
        danger="danger",
        green="green",
        red="red",
        blurple="blurple",
        grey="grey",
    )
    ui = SimpleNamespace(
        View=_FakeView,
        Button=_FakeButton,
        Select=_FakeSelect,
        button=_button_decorator,
    )
    return SimpleNamespace(
        ui=ui,
        ButtonStyle=styles,
        Color=_FakeColor,
        SelectOption=lambda **kwargs: SimpleNamespace(**kwargs),
        AllowedMentions=_FakeAllowedMentions,
        Interaction=object,
        Embed=lambda **kwargs: SimpleNamespace(**kwargs),
        File=lambda fp, *, filename: SimpleNamespace(fp=fp, filename=filename),
    )


@pytest.fixture
def discord_adapter_module(monkeypatch):
    import plugins.platforms.discord.adapter as module

    view_names = (
        "DurableRequestView",
        "InteractivePromptView",
        "InteractiveMultiSelectView",
        "ExecApprovalView",
        "SlashConfirmView",
        "UpdatePromptView",
        "ModelPickerView",
        "ClarifyChoiceView",
        "ChoicePickerView",
    )
    missing = object()
    original_views = {
        name: getattr(module, name, missing)
        for name in view_names
    }
    fake = _fake_discord()
    monkeypatch.setattr(module, "discord", fake)
    monkeypatch.setattr(module, "DISCORD_AVAILABLE", True)
    module._define_discord_view_classes()
    try:
        yield module
    finally:
        for name, original in original_views.items():
            if original is missing:
                if hasattr(module, name):
                    delattr(module, name)
            else:
                setattr(module, name, original)


def _request(*, message_id: str | None = None) -> DurableRequest:
    return DurableRequest(
        request_id="drq_abcdefghijklmnopqrstuvwxyz123456",
        kind="tool.durable_request",
        title="Approve release",
        body="Should the reviewed release continue?",
        options=(
            DurableRequestOption("canonical-approve", "Approve"),
            DurableRequestOption("canonical-reject", "Reject"),
        ),
        status=(
            DurableRequestStatus.OPEN
            if message_id is not None
            else DurableRequestStatus.PENDING_DELIVERY
        ),
        created_at=1.0,
        updated_at=1.0,
        expires_at=10_000.0,
        target=DurableRequestTarget("discord", "10", "11"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="cron",
        origin_id="job-1",
        reference="release",
        message_id=message_id,
    )


def _tool_request(action="terminal.execute", *, message_id=None):
    host = action in {"host.sudo", "host.terminal.execute", "host.execute_code.execute"}
    choices = [("approve", "Approve Once" if host else "Allow Once")]
    if not host and action not in {"file.write", "file.patch"}:
        choices.extend((("session", "Allow Session"), ("always", "Always Allow")))
    choices.append(("reject", "Deny"))
    return replace(
        _request(message_id=message_id),
        kind="tool.durable_approval." + action,
        body="**Requested command:**\n```\nid -u\n```\n\n**Why it was flagged:** elevated operation",
        options=tuple(DurableRequestOption(value, label) for value, label in choices),
        command_response_allowed=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["terminal.execute", "execute_code.execute", "host.sudo", "host.terminal.execute", "host.execute_code.execute", "file.write", "file.patch"])
async def test_durable_tool_approval_preserves_compact_controls_and_distinct_host_card(
    discord_adapter_module, monkeypatch, action,
):
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    monkeypatch.setattr(module, "get_durable_request_store", lambda: _Store(None))
    request = _tool_request(action)

    result = await adapter.send_durable_request("10", request, {"thread_id": "11"})

    assert result.success
    sent = channel.sent[0]
    assert "**Requested command:**" in sent["content"]
    assert "id -u" in sent["content"]
    assert "**Why it was flagged:**" in sent["content"]
    assert "<t:10000:F>" in sent["content"]
    assert "Choose one option" not in sent["content"]
    assert "1. " not in sent["content"]
    assert "/request respond" not in sent["content"]
    assert "1 minute" not in sent["content"]
    assert all(ord(char) < 128 for char in sent["content"])
    view = sent["view"]
    assert view.timeout is None
    if action in {"host.sudo", "host.terminal.execute", "host.execute_code.execute"}:
        assert sent["embed"].title == "HOST SUDO - elevated privileges"
        assert sent["embed"].color == "red"
        assert [button.label for button in view.children] == ["Approve Once", "Deny"]
        assert [button.style for button in view.children] == ["green", "red"]
        assert "Operator approval is required for every host sudo operation." in sent["content"]
    elif action in {"file.write", "file.patch"}:
        assert sent["embed"].title == "Hermes wants to edit files that need your OK"
        assert sent["embed"].color == "orange"
        assert [button.label for button in view.children] == ["Allow Once", "Deny"]
        assert [button.style for button in view.children] == ["green", "red"]
    else:
        assert sent["embed"].title == "Hermes wants to run a command that needs your OK"
        assert sent["embed"].color == "orange"
        assert [button.label for button in view.children] == ["Allow Once", "Allow Session", "Always Allow", "Deny"]
        assert [button.style for button in view.children] == ["green", "grey", "blurple", "red"]


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["terminal.execute", "execute_code.execute", "host.sudo", "host.terminal.execute", "host.execute_code.execute", "file.write", "file.patch"])
async def test_durable_tool_approval_restore_retains_controls_and_original_message(
    discord_adapter_module, monkeypatch, action,
):
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    monkeypatch.setattr(module, "get_durable_request_store", lambda: _Store(None))

    result = await adapter.restore_durable_request(_tool_request(action, message_id="99"))

    assert result.success
    view, message_id = adapter._client.views[0]
    assert view.timeout is None
    assert message_id == 99
    assert view._message is channel.message
    expected = (["Approve Once", "Deny"] if action in {"host.sudo", "host.terminal.execute", "host.execute_code.execute"}
                else ["Allow Once", "Deny"] if action in {"file.write", "file.patch"}
                else ["Allow Once", "Allow Session", "Always Allow", "Deny"])
    assert [button.label for button in view.children] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("slot, status", [
    (1, "Approved once"), (2, "Allowed for this conversation"),
    (3, "Always allowed for matching rules"), (4, "Denied"),
])
async def test_durable_tool_button_updates_status_after_authoritative_resolution(
    discord_adapter_module, monkeypatch, slot, status,
):
    module = discord_adapter_module
    request = _tool_request(message_id="99")
    store = _Store(DurableResolveResult(DurableResolveStatus.RESOLVED, request=request))
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: True)
    view = module.DurableRequestView(request=request, allowed_user_ids={"42"}, store=store)
    interaction = _interaction([])

    await view.children[slot - 1].callback(interaction)

    assert store.calls[0]["option_token"] == str(slot)
    assert status in interaction.response.edits[0]["embed"].description
    assert all(child.disabled for child in view.children)
    assert view.stopped
    if slot != 4:
        assert "queued for execution" in interaction.response.edits[0]["embed"].description


def test_host_sudo_ui_rejects_persistent_permission_options(discord_adapter_module):
    request = replace(
        _tool_request("host.sudo"),
        options=(DurableRequestOption("approve", "Approve Once"),
                 DurableRequestOption("always", "Always Allow")),
    )
    with pytest.raises(ValueError, match="host sudo"):
        discord_adapter_module.DurableRequestView(request=request, allowed_user_ids={"42"}, store=_Store(None))


@pytest.mark.parametrize("action", ["file.write", "file.patch"])
def test_file_approval_card_retains_scopes_when_native_gate_allows_them(discord_adapter_module, action):
    request = replace(_tool_request(action), options=_tool_request("terminal.execute").options)
    view = discord_adapter_module.DurableRequestView(request=request, allowed_user_ids={"42"}, store=_Store(None))
    assert [button.label for button in view.children] == ["Allow Once", "Allow Session", "Always Allow", "Deny"]
    assert view.timeout is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["terminal.execute", "host.sudo"])
@pytest.mark.parametrize("status, description", [
    ("succeeded", "Reviewed operation completed."),
    ("failed", "Operation failed. Partial changes may have occurred; do not repeat it automatically."),
    ("unknown", "Execution outcome is unknown. Partial changes may have occurred; do not repeat it automatically."),
    ("invalidated", "Approval invalidated because the reviewed context changed. Operation was not run."),
    ("expired", "Approval expired. Operation was not run."),
    ("cancelled", "Approval cancelled. Operation was not run."),
    ("declined", "Denied. Operation was not run."),
    ("handler_unavailable", "Execution handler unavailable. Operation was not run."),
])
async def test_durable_tool_completion_edits_original_card_with_actual_outcome(
    discord_adapter_module, action, status, description,
):
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)

    result = await adapter.update_durable_execution_result(_tool_request(action, message_id="99"), status)

    assert result.success
    assert result.message_id == "99"
    assert channel.sent == []
    assert len(channel.message.edits) == 1
    edit = channel.message.edits[0]
    assert edit["view"] is None
    assert edit["embed"].description == description
    assert all(ord(char) < 128 for char in description)
    if action == "host.sudo":
        assert edit["embed"].title == "HOST SUDO - elevated privileges"


@pytest.mark.asyncio
async def test_durable_tool_completion_rejects_unknown_status_and_generic_request(
    discord_adapter_module,
):
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    for request, status in [(_tool_request(message_id="99"), "arbitrary user text"),
                            (_request(message_id="99"), "succeeded")]:
        result = await adapter.update_durable_execution_result(request, status)
        assert not result.success
        assert result.error_kind == "bad_format"
    assert channel.message.edits == []


@pytest.mark.asyncio
async def test_fast_execution_result_cannot_be_overwritten_by_queued_decision_card(
    discord_adapter_module, monkeypatch,
):
    import asyncio

    module = discord_adapter_module
    request = _tool_request(message_id="99")
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    store = _Store(DurableResolveResult(DurableResolveStatus.RESOLVED, request=request))
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: True)
    view = module.DurableRequestView(request=request, allowed_user_ids={"42"}, store=store)
    interaction = _interaction([])
    editing_decision = asyncio.Event()
    release_decision = asyncio.Event()

    async def delayed_decision_edit(**kwargs):
        editing_decision.set()
        await release_decision.wait()
        interaction.response.edits.append(kwargs)

    interaction.response.edit_message = delayed_decision_edit
    decision = asyncio.create_task(view.children[0].callback(interaction))
    await editing_decision.wait()
    completion = asyncio.create_task(adapter.update_durable_execution_result(request, "succeeded"))
    await asyncio.sleep(0)
    assert not completion.done()
    assert channel.message.edits == []
    release_decision.set()
    await decision
    assert (await completion).success
    assert "queued for execution" in interaction.response.edits[0]["embed"].description
    assert channel.message.edits[-1]["embed"].description == "Reviewed operation completed."


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["file.write", "file.patch", "terminal.execute", "execute_code.execute", "host.sudo"])
async def test_omitted_operation_attaches_full_redacted_verified_review(
    discord_adapter_module, monkeypatch, tmp_path, action,
):
    import hashlib
    import json
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload
    from gateway.durable_requests import DurableRequestSpec, DurableRequestStore

    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    secret = "sk-abcdefghijklmnopqrstuvwxyz0123456789"
    content = "Reviewed action detail\n" * 120 + secret
    arguments = ({"argv": ["/usr/bin/printf", content], "timeout": 30}
                 if action == "host.sudo" else {"content": content, "path": "AGENTS.md"})
    payload = arguments if action == "host.sudo" else {"action_type": action, "arguments": arguments,
                                                         "session_env": {"private_marker": "PRIVATE_CONTEXT"}}
    encoded = encode_durable_action_payload(payload)
    fingerprint = hashlib.sha256(encoded.encode()).hexdigest()
    store = DurableRequestStore(tmp_path / "review.db", wall_clock=lambda: 1.0)
    template = _tool_request(action)
    request = store.create_with_action(
        DurableRequestSpec(kind=template.kind, title="Reviewed operation", options=template.options,
                           body=f"Preview\n[Remaining content omitted; exact operation SHA-256: {fingerprint}]",
                           ttl_seconds=1000),
        action=PreparedDurableAction(action, 1, encoded, fingerprint, "attachment-review"),
        target=template.target, responder_mode=template.responder_mode, expected_actor_id=None,
        origin_type="cron", origin_id="attachment-test",
    ).request
    monkeypatch.setattr(module, "get_durable_request_store", lambda: store)

    result = await adapter.send_durable_request("10", request, {"thread_id": "11"})

    assert result.success, result.error
    sent = channel.sent[0]
    assert "Full reviewed operation is attached." in sent["content"]
    assert sent["allowed_mentions"] is _FakeAllowedMentions.sentinel
    attachment = sent["file"]
    assert attachment.filename == "reviewed-operation.txt"
    full_review = attachment.fp.getvalue().decode()
    assert fingerprint in full_review
    assert "Reviewed action detail" in full_review
    assert secret not in full_review
    assert "PRIVATE_CONTEXT" not in full_review
    assert full_review.count("Reviewed action detail") == 120


@pytest.mark.asyncio
async def test_omitted_review_cannot_deliver_a_tampered_payload(discord_adapter_module, monkeypatch, tmp_path):
    import hashlib
    import sqlite3
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload
    from gateway.durable_requests import DurableRequestSpec, DurableRequestStore

    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter.platform = module.Platform.DISCORD
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    store = DurableRequestStore(tmp_path / "review.db", wall_clock=lambda: 1.0)
    template = _tool_request("file.write")
    encoded = encode_durable_action_payload({"arguments": {"path": "AGENTS.md", "content": "reviewed"}})
    request = store.create_with_action(
        DurableRequestSpec(kind=template.kind, title="Reviewed operation", options=template.options,
                           body="[Remaining content omitted; inspect attachment]", ttl_seconds=1000),
        action=PreparedDurableAction("file.write", 1, encoded, hashlib.sha256(encoded.encode()).hexdigest(), "tamper-test"),
        target=template.target, responder_mode=template.responder_mode, expected_actor_id=None,
        origin_type="cron", origin_id="attachment-test",
    ).request
    with sqlite3.connect(store.db_path) as connection:
        connection.execute("UPDATE durable_action_tickets SET payload_json=? WHERE request_id=?",
                           ('{"arguments":{"content":"unreviewed"}}', request.request_id))
    monkeypatch.setattr(module, "get_durable_request_store", lambda: store)

    result = await adapter.send_durable_request("10", request, {"thread_id": "11"})

    assert not result.success
    assert result.error_kind == "bad_format"
    assert channel.sent == []


def test_native_alternative_is_visible_only_when_command_response_allowed() -> None:
    request = _request()

    assert "/request respond" not in render_durable_request_content(request)
    content = render_durable_request_content(
        replace(request, command_response_allowed=True)
    )

    assert (
        f"Alternative: /request respond {request.request_id} <number>" in content
    )


def test_text_command_render_includes_full_receipt_and_every_option() -> None:
    request = replace(_request(), command_response_allowed=True)

    content = render_durable_request_text_command_content(request)

    assert content == (
        "**Approve release**\n\n"
        "Should the reviewed release continue?\n\n"
        f"Request: {request.request_id}\n\n"
        "Choose one option:\n"
        "1. Approve\n"
        "2. Reject\n\n"
        "Respond in this channel with:\n"
        f"/request respond {request.request_id} <number>"
    )
    assert "canonical-" not in content


class _Message:
    def __init__(self, message_id=99):
        self.id = message_id
        self.edits = []

    async def edit(self, **kwargs):
        self.edits.append(kwargs)


class _Channel:
    def __init__(self):
        self.sent = []
        self.message = _Message()

    async def send(self, **kwargs):
        self.sent.append(kwargs)
        return self.message

    async def fetch_message(self, message_id):
        assert message_id == self.message.id
        return self.message


class _Client:
    def __init__(self, channel):
        self.channel = channel
        self.views = []

    def get_channel(self, channel_id):
        assert channel_id == 11
        return self.channel

    async def fetch_channel(self, channel_id):
        return self.channel

    def add_view(self, view, *, message_id):
        self.views.append((view, message_id))


class _Store:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def resolve(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


@pytest.mark.asyncio
async def test_send_uses_substantive_content_mentions_off_and_opaque_slots(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    store = _Store(DurableResolveResult(DurableResolveStatus.NOT_DELIVERED))
    monkeypatch.setattr(module, "get_durable_request_store", lambda: store)

    result = await adapter.send_durable_request(
        chat_id="10",
        request=_request(),
        metadata={"thread_id": "11"},
    )

    assert result.success is True
    assert result.message_id == "99"
    sent = channel.sent[0]
    assert "Approve release" in sent["content"]
    assert "Should the reviewed release continue?" in sent["content"]
    assert "1. Approve" in sent["content"]
    assert "2. Reject" in sent["content"]
    assert sent["allowed_mentions"] is _FakeAllowedMentions.sentinel
    view = sent["view"]
    assert view.timeout is None
    assert [button.custom_id.rsplit(":", 1)[-1] for button in view.children] == [
        "1",
        "2",
    ]
    assert all("canonical-" not in button.custom_id for button in view.children)


@pytest.mark.asyncio
async def test_send_text_uses_one_unsplit_message_mentions_off_and_exact_id(
    discord_adapter_module,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter.MAX_MESSAGE_LENGTH = 2000
    request = replace(_request(), command_response_allowed=True)

    result = await adapter.send_durable_request_text(
        chat_id="10",
        request=request,
        metadata={"thread_id": "11"},
    )

    assert result.success is True
    assert result.message_id == "99"
    assert len(channel.sent) == 1
    assert channel.sent[0] == {
        "content": render_durable_request_text_command_content(request),
        "allowed_mentions": _FakeAllowedMentions.sentinel,
    }


@pytest.mark.asyncio
async def test_send_text_rejects_complete_render_over_transport_limit(
    discord_adapter_module,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter.MAX_MESSAGE_LENGTH = 20

    result = await adapter.send_durable_request_text(
        chat_id="10",
        request=replace(_request(), command_response_allowed=True),
        metadata={"thread_id": "11"},
    )

    assert result.success is False
    assert result.error_kind == "too_long"
    assert channel.sent == []


@pytest.mark.asyncio
async def test_restore_fetches_exact_message_and_registers_persistent_view(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    client = _Client(channel)
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = client
    adapter._allowed_user_ids = set()
    adapter._allowed_role_ids = set()
    monkeypatch.setattr(
        module,
        "get_durable_request_store",
        lambda: _Store(DurableResolveResult(DurableResolveStatus.EXPIRED)),
    )

    result = await adapter.restore_durable_request(_request(message_id="99"))

    assert result.success is True
    assert result.message_id == "99"
    assert len(client.views) == 1
    view, message_id = client.views[0]
    assert message_id == 99
    assert view.timeout is None
    assert view._message is channel.message


class _Response:
    def __init__(self, events, *, fail_edit=False):
        self.events = events
        self.fail_edit = fail_edit
        self.messages = []
        self.deferred = False
        self.edits = []

    def is_done(self):
        return False

    async def edit_message(self, **kwargs):
        self.events.append("edit")
        self.edits.append(kwargs)
        if self.fail_edit:
            raise RuntimeError("edit failed")

    async def send_message(self, message, *, ephemeral=False):
        self.messages.append((message, ephemeral))

    async def defer(self):
        self.deferred = True


class _Followup:
    async def send(self, *_args, **_kwargs):
        return None


def _interaction(events, *, fail_edit=False):
    return SimpleNamespace(
        channel_id=11,
        message=SimpleNamespace(id=99),
        user=SimpleNamespace(id=42, roles=[]),
        response=_Response(events, fail_edit=fail_edit),
        followup=_Followup(),
    )


@pytest.mark.asyncio
async def test_auth_runs_before_resolution_and_resolution_before_edit(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    events = []
    request = _request(message_id="99")

    def auth(*_args, **_kwargs):
        events.append("auth")
        return True

    class OrderedStore(_Store):
        def resolve(self, **kwargs):
            events.append("resolve")
            return super().resolve(**kwargs)

    store = OrderedStore(
        DurableResolveResult(DurableResolveStatus.RESOLVED, request=request)
    )
    monkeypatch.setattr(module, "_component_check_auth", auth)
    view = module.DurableRequestView(
        request=request,
        allowed_user_ids=set(),
        store=store,
    )

    await view.children[0].callback(_interaction(events))

    assert events == ["auth", "resolve", "edit"]
    assert store.calls == [
        {
            "request_id": request.request_id,
            "option_token": "1",
            "platform": "discord",
            "channel_id": "11",
            "message_id": "99",
            "actor_id": "42",
        }
    ]
    assert all(child.disabled for child in view.children)


@pytest.mark.asyncio
async def test_edit_failure_does_not_repeat_or_erase_resolution(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    events = []
    request = _request(message_id="99")
    store = _Store(
        DurableResolveResult(DurableResolveStatus.RESOLVED, request=request)
    )
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: True)
    view = module.DurableRequestView(
        request=request,
        allowed_user_ids=set(),
        store=store,
    )
    interaction = _interaction(events, fail_edit=True)

    await view.children[0].callback(interaction)

    assert len(store.calls) == 1
    assert interaction.response.deferred is True
    assert view.stopped is False


@pytest.mark.asyncio
async def test_stale_terminal_result_is_reported_ephemerally(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    request = _request(message_id="99")
    store = _Store(DurableResolveResult(DurableResolveStatus.EXPIRED))
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: True)
    view = module.DurableRequestView(
        request=request,
        allowed_user_ids=set(),
        store=store,
    )
    interaction = _interaction([])

    await view.children[0].callback(interaction)

    assert interaction.response.messages == [("This request has expired.", True)]


@pytest.mark.asyncio
async def test_existing_ephemeral_prompt_delivery_contract_is_unchanged(
    discord_adapter_module,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    prompt = InteractivePrompt(
        prompt_id="abcdefghijklmnopqrstuvwxyz1234567890",
        kind="test",
        title="Choose",
        body="Choose one.",
        options=(PromptOption("a", "A"), PromptOption("b", "B")),
        timeout_seconds=60,
    )

    result = await adapter.send_interactive_prompt(
        chat_id="10",
        prompt=prompt,
        metadata={"thread_id": "11"},
    )

    assert result.success is True
    assert result.message_id == "99"
    assert channel.sent[0]["view"].timeout == 60


@pytest.mark.asyncio
async def test_button_label_truncation_does_not_change_canonical_option(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = set()
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    full_label = "L" * 200
    request = replace(
        _request(),
        options=(
            DurableRequestOption("canonical-long", full_label),
            DurableRequestOption("canonical-other", "Other"),
        ),
    )
    monkeypatch.setattr(
        module,
        "get_durable_request_store",
        lambda: _Store(DurableResolveResult(DurableResolveStatus.NOT_DELIVERED)),
    )

    result = await adapter.send_durable_request(
        chat_id="10",
        request=request,
        metadata={"thread_id": "11"},
    )

    assert result.success is True
    first_button = channel.sent[0]["view"].children[0]
    assert len(first_button.label) <= module._DISCORD_BUTTON_LABEL_LIMIT
    assert request.options[0].label == full_label
    assert "canonical-long" not in first_button.custom_id


@pytest.mark.asyncio
async def test_adapter_rejects_excess_options_without_sending(
    discord_adapter_module,
) -> None:
    module = discord_adapter_module
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = set()
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    request = replace(
        _request(),
        options=tuple(
            DurableRequestOption(f"option-{index}", f"Option {index}")
            for index in range(6)
        ),
    )

    result = await adapter.send_durable_request(
        chat_id="10",
        request=request,
        metadata={"thread_id": "11"},
    )

    assert result.success is False
    assert result.error_kind == "bad_format"
    assert channel.sent == []


@pytest.mark.asyncio
async def test_failed_general_authorization_never_reaches_store(
    discord_adapter_module,
    monkeypatch,
) -> None:
    module = discord_adapter_module
    request = _request(message_id="99")
    store = _Store(DurableResolveResult(DurableResolveStatus.NOT_DELIVERED))
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: False)
    view = module.DurableRequestView(
        request=request,
        allowed_user_ids=set(),
        store=store,
    )
    interaction = _interaction([])

    await view.children[0].callback(interaction)

    assert store.calls == []
    assert interaction.response.messages == [
        ("You are not authorized to use this control.", True)
    ]


@pytest.mark.asyncio
async def test_multi_native_real_registry_commits_set_before_failed_edit_and_rejects_scope(
    discord_adapter_module, monkeypatch,
) -> None:
    import gateway.interactive as interactive

    module = discord_adapter_module
    clock = [1_000.0]
    registry = interactive.InteractivePromptRegistry(clock=lambda: clock[0])
    monkeypatch.setattr(interactive, "_registry", registry)
    handle = registry.register_multi_select_prompt(
        interactive.InteractiveMultiSelectSpec(
            kind="test.multi", title="Choose bills", body="Select the utility categories.",
            options=tuple(PromptOption(f"private-{slot}", "L" * 120 if slot == 1 else f"Label {slot}") for slot in range(1, 7)),
            min_selections=2, max_selections=4, timeout_seconds=60,
        ),
        scope=interactive.PromptScope("session", "discord", "10", "11", "42"),
    )
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    result = await adapter.send_interactive_multi_select_prompt("10", handle.prompt, {"thread_id": "11"})
    assert result.success and result.message_id == "99"
    assert registry.bind_delivery(prompt_id=handle.prompt.prompt_id, platform="discord", message_id="99")
    view = channel.sent[0]["view"]
    assert len(view.children) == 1
    control = view.children[0]
    assert control.custom_id == f"ipm:{handle.prompt.prompt_id}"
    assert (control.min_values, control.max_values) == (2, 4)
    assert [option.value for option in control.options] == [str(slot) for slot in range(1, 7)]
    assert len(control.options[0].label) <= module._DISCORD_SELECT_FIELD_LIMIT
    assert handle.prompt.options[0].label in channel.sent[0]["content"]
    assert channel.sent[0]["allowed_mentions"] is _FakeAllowedMentions.sentinel

    def interaction(values=("3", "1"), custom_id=None):
        value = _interaction([], fail_edit=True)
        value.data = {"custom_id": custom_id or control.custom_id, "values": list(values)}
        return value

    monkeypatch.setattr(module, "_component_check_auth", lambda *_: False)
    denied = interaction()
    await control.callback(denied)
    assert "not authorized" in denied.response.messages[0][0]
    assert not control.disabled
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: True)
    for field, bad_value in (("channel_id", 12), ("message", SimpleNamespace(id=100)), ("user", SimpleNamespace(id=43))):
        mismatch = interaction()
        setattr(mismatch, field, bad_value)
        await control.callback(mismatch)
        assert "does not match" in mismatch.response.messages[0][0]
        assert not control.disabled
    for values in (("1", "1"), ("1",), ("1", "7"), ("1", "2", "3", "4", "5")):
        invalid = interaction(values)
        await control.callback(invalid)
        assert "invalid" in invalid.response.messages[0][0]
        assert not control.disabled
    wrong_id = interaction(custom_id="ipm:" + "z" * 32)
    await control.callback(wrong_id)
    assert "invalid" in wrong_id.response.messages[0][0]
    assert not control.disabled

    selected = interaction()
    await control.callback(selected)
    outcome = registry.wait_for_multi_select(handle)
    assert outcome.response.option_ids == ("private-1", "private-3")
    assert control.disabled and selected.response.deferred
    repeated = interaction(("2", "4"))
    await control.callback(repeated)
    assert "already answered" in repeated.response.messages[0][0]
    assert registry.wait_for_multi_select(handle).response.option_ids == ("private-1", "private-3")
    # Single entry point cannot silently change a multi prompt into buttons.
    assert not (await adapter.send_interactive_prompt("10", handle.prompt)).success
    expiring = registry.register_multi_select_prompt(
        interactive.InteractiveMultiSelectSpec(
            kind="test.multi", title="Choose", body="Choose categories.",
            options=handle.prompt.options, min_selections=1, max_selections=2, timeout_seconds=5,
        ),
        scope=interactive.PromptScope("session", "discord", "10", "11", "42"),
    )
    expiring_view = module.InteractiveMultiSelectView(expiring.prompt, {"42"})
    expiring_view._message = _Message()
    # A provider timeout firing early must not change authoritative state.
    await expiring_view.on_timeout()
    assert not expiring_view.children[0].disabled and not expiring_view._message.edits
    clock[0] += 6
    await expiring_view.on_timeout()
    assert registry.wait_for_multi_select(expiring).status is interactive.PromptOutcomeStatus.EXPIRED
    assert expiring_view.children[0].disabled and expiring_view.stopped
    assert len(expiring_view._message.edits) == 1
    await expiring_view.on_timeout()
    assert len(expiring_view._message.edits) == 1


@pytest.mark.asyncio
async def test_multi_durable_native_restore_and_command_race_preserve_sqlite_set(
    discord_adapter_module, monkeypatch, tmp_path,
) -> None:
    import asyncio
    from gateway.durable_requests import DurableRequestSpec, DurableRequestStore

    module = discord_adapter_module
    database = tmp_path / "state.db"
    store = DurableRequestStore(database, wall_clock=lambda: 1_000.0)
    request = store.create(
        DurableRequestSpec(
            kind="test.multi", title="Choose utilities", body="Choose categories.",
            options=tuple(DurableRequestOption(f"private-{slot}", f"Label {slot}") for slot in range(1, 7)),
            ttl_seconds=60, selection_mode="multi", min_selections=2, max_selections=4,
        ),
        target=DurableRequestTarget("discord", "10", "11"),
        responder_mode=DurableResponderMode.INITIATOR, expected_actor_id="42",
        origin_type="session", origin_id="session", command_response_allowed=True,
    ).request
    claim = store.claim_next_pending(["discord"])
    request = store.bind_delivery(request_id=request.request_id, claim_id=claim.claim_id, platform="discord", message_id="99")
    # Reopen the ledger, then register the exact stored native message.
    restarted = DurableRequestStore(database, wall_clock=lambda: 1_000.0)
    monkeypatch.setattr(module, "get_durable_request_store", lambda: restarted)
    monkeypatch.setattr(module, "_component_check_auth", lambda *_: True)
    channel = _Channel()
    adapter = object.__new__(module.DiscordAdapter)
    adapter._client = _Client(channel)
    adapter._allowed_user_ids = {"42"}
    adapter._allowed_role_ids = set()
    adapter.MAX_MESSAGE_LENGTH = 2000
    assert (await adapter.restore_durable_request(request)).success
    view = adapter._client.views[0][0]
    control = view.children[0]
    assert control.custom_id.startswith("drm:") and "private" not in control.custom_id
    assert len(control.options) == 6 and view.timeout is None
    native = _interaction([], fail_edit=True)
    native.data = {"custom_id": control.custom_id, "values": ["3", "1"]}

    command_result, _ = await asyncio.gather(
        asyncio.to_thread(restarted.resolve_command, request_id=request.request_id, option_slots=(4, 2), platform="discord", channel_id="11", actor_id="42"),
        control.callback(native),
    )
    persisted = restarted.get(request_id=request.request_id)
    assert persisted.selected_option_id is None
    assert persisted.selected_option_ids in (("private-1", "private-3"), ("private-2", "private-4"))
    if command_result.status is DurableResolveStatus.RESOLVED:
        assert native.response.messages == [("This request was already answered.", True)]
    else:
        assert command_result.status is DurableResolveStatus.ALREADY_RESOLVED
        assert native.response.deferred and control.disabled and not view.stopped
    repeat = _interaction([])
    repeat.data = native.data
    await control.callback(repeat)
    assert repeat.response.messages == [("This request was already answered.", True)]
