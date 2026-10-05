"""Webhook sessions resolve approval from real unattended config, not a leaked ask flag."""

import pytest

import hermes_cli.config as config
from gateway.session_context import clear_session_vars, reset_session_vars, set_session_vars
from tools import approval


@pytest.fixture
def webhook_context(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_EXEC_ASK", "1")
    monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
    monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
    monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)
    monkeypatch.delenv("HERMES_SINGLE_QUERY_SESSION", raising=False)
    monkeypatch.setattr(approval, "_YOLO_MODE_FROZEN", False)
    reset_session_vars()
    config._LOAD_CONFIG_CACHE.clear()
    yield home
    reset_session_vars()
    config._LOAD_CONFIG_CACHE.clear()


@pytest.mark.parametrize("platform", ["webhook", "msgraph_webhook"])
@pytest.mark.parametrize("mode", ["deny", "approve"])
@pytest.mark.parametrize("guard", ["dangerous_command", "combined_command", "plugin_tool"])
def test_webhook_ask_flag_honors_unattended_config(webhook_context, platform, mode, guard):
    (webhook_context / "config.yaml").write_text(
        f"approvals:\n  mode: manual\n  unattended_mode: {mode}\n"
        "command_allowlist: []\nsecurity:\n  tirith_enabled: false\n",
        encoding="utf-8",
    )
    tokens = set_session_vars(platform=platform, session_id="webhook-approval-regression",
                              cron_session="")
    try:
        if guard == "dangerous_command":
            result = approval.check_dangerous_command("rm -rf /tmp/webhook-example", "local")
        elif guard == "combined_command":
            result = approval.check_all_command_guards("rm -rf /tmp/webhook-example", "local")
        else:
            result = approval.request_tool_approval("write_file", "sensitive write",
                                                    rule_key="webhook-test")
    finally:
        clear_session_vars(tokens)

    assert result["approved"] is (mode == "approve")
    assert not result.get("approval_required")
    if mode == "deny":
        assert "blocked" in result["message"].lower()
        assert "unattended" in result["message"].lower()


def test_api_server_retains_ask_presence(webhook_context):
    tokens = set_session_vars(platform="api_server", cron_session="")
    try:
        assert approval._presence()[3] is True
    finally:
        clear_session_vars(tokens)
