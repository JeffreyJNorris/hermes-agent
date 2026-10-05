import pytest

from gateway.session_context import reset_session_vars, set_session_vars


@pytest.mark.parametrize('enabled, inline, platform, expected', [
    (True, 300, 'discord', 360),
    (True, 120, 'discord', 180),
    (True, 0, 'discord', 120),
    (False, 300, 'discord', 120),
    (True, 300, 'webhook', 120),
    (True, True, 'discord', 120),
    (True, float('nan'), 'discord', 120),
    (True, 301, 'discord', 120),
])
def test_human_wait_ceiling_covers_only_configured_discord_inline_wait(monkeypatch, enabled, inline, platform, expected):
    from hermes_cli import config
    from tools import approval_context
    from tools.approval_human_wait import human_wait_ceiling
    monkeypatch.setattr(approval_context, '_get_approval_timeout', lambda: 60)
    monkeypatch.setattr(config, 'load_config_readonly', lambda: {
        'durable_approvals': {'enabled': enabled, 'inline_wait_seconds': inline},
    })
    set_session_vars(platform=platform)
    try:
        assert human_wait_ceiling() == expected
    finally:
        reset_session_vars()
