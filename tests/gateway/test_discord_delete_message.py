"""Discord progress cleanup deletes messages without fetching their contents."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import discord
import pytest

from gateway.config import PlatformConfig
from plugins.platforms.discord.adapter import DiscordAdapter


class NotFound(Exception):
    pass


class Forbidden(Exception):
    pass


@pytest.fixture(autouse=True)
def discord_errors(monkeypatch):
    # Gateway conftest stubs the optional SDK, including its exception names.
    monkeypatch.setattr(discord, "NotFound", NotFound)
    monkeypatch.setattr(discord, "Forbidden", Forbidden)


def connected_adapter(*, cached=True):
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="***"))
    message = SimpleNamespace(delete=AsyncMock())
    channel = SimpleNamespace(get_partial_message=Mock(return_value=message))
    adapter._client = SimpleNamespace(
        get_channel=Mock(return_value=channel if cached else None),
        fetch_channel=AsyncMock(return_value=channel),
    )
    adapter._last_overflow_preview[("555", "42")] = "old preview"
    adapter._last_overflow_preview[("555", "43")] = "another preview"
    return adapter, channel, message


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [True, False])
async def test_deletes_progress_message_and_clears_only_its_preview(cached):
    adapter, channel, message = connected_adapter(cached=cached)

    assert await adapter.delete_message("555", "42") is True

    channel.get_partial_message.assert_called_once_with(42)
    message.delete.assert_awaited_once_with()
    assert ("555", "42") not in adapter._last_overflow_preview
    assert adapter._last_overflow_preview[("555", "43")] == "another preview"
    if cached:
        adapter._client.fetch_channel.assert_not_awaited()
    else:
        adapter._client.fetch_channel.assert_awaited_once_with(555)


@pytest.mark.asyncio
async def test_already_deleted_message_is_successful_cleanup():
    adapter, channel, message = connected_adapter()
    message.delete.side_effect = NotFound("Unknown Message")

    assert await adapter.delete_message("555", "42") is True
    assert ("555", "42") not in adapter._last_overflow_preview


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["forbidden", "network"])
async def test_failed_deletion_returns_false_and_preserves_preview(failure, caplog):
    adapter, channel, message = connected_adapter()
    message.delete.side_effect = (
        Forbidden("Missing Permissions")
        if failure == "forbidden" else OSError("connection lost")
    )

    assert await adapter.delete_message("555", "42") is False
    assert adapter._last_overflow_preview[("555", "42")] == "old preview"
    assert "42" in caplog.text


@pytest.mark.asyncio
async def test_disconnected_adapter_cannot_delete():
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="***"))
    assert await adapter.delete_message("555", "42") is False


@pytest.mark.asyncio
async def test_channel_fetch_failure_does_not_claim_deletion():
    adapter, channel, message = connected_adapter(cached=False)
    adapter._client.fetch_channel.side_effect = Forbidden("Missing Access")

    assert await adapter.delete_message("555", "42") is False
    message.delete.assert_not_awaited()
    assert adapter._last_overflow_preview[("555", "42")] == "old preview"
