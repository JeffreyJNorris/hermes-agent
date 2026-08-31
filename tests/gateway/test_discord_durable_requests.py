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
    )


@pytest.fixture
def discord_adapter_module(monkeypatch):
    import plugins.platforms.discord.adapter as module

    view_names = (
        "DurableRequestView",
        "InteractivePromptView",
        "ExecApprovalView",
        "SlashConfirmView",
        "UpdatePromptView",
        "ModelPickerView",
        "ClarifyChoiceView",
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

    def is_done(self):
        return False

    async def edit_message(self, **kwargs):
        self.events.append("edit")
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
