from __future__ import annotations

import pytest

from gateway.durable_requests import DurableRequest
from gateway.platforms.base import BasePlatformAdapter, SendResult


class _StubAdapter(BasePlatformAdapter):
    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, *_args, **_kwargs) -> SendResult:
        return SendResult(success=True)

    async def get_chat_info(self, *_args, **_kwargs):
        return None


@pytest.mark.asyncio
async def test_durable_request_methods_are_optional_and_explicitly_unsupported() -> None:
    adapter = object.__new__(_StubAdapter)

    send = await adapter.send_durable_request(
        chat_id="chat",
        request=object(),  # type: ignore[arg-type]
    )
    text = await adapter.send_durable_request_text(
        chat_id="chat",
        request=object(),  # type: ignore[arg-type]
    )
    restore = await adapter.restore_durable_request(
        request=object(),  # type: ignore[arg-type]
    )
    multi = await adapter.send_interactive_multi_select_prompt(
        chat_id="chat", prompt=object(),  # type: ignore[arg-type]
    )

    assert send.success is False
    assert send.error_kind == "unsupported"
    assert text.success is False
    assert text.error_kind == "unsupported"
    assert restore.success is False
    assert restore.error_kind == "unsupported"
    assert multi.success is False
    assert multi.error_kind == "unsupported"
    assert "send_interactive_multi_select_prompt" not in _StubAdapter.__abstractmethods__
    assert "send_durable_request" not in _StubAdapter.__abstractmethods__
    assert "send_durable_request_text" not in _StubAdapter.__abstractmethods__
    assert "restore_durable_request" not in _StubAdapter.__abstractmethods__
