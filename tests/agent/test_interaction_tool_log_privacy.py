"""Tool history keeps interaction content; ordinary diagnostic logs omit it."""
from types import SimpleNamespace
import logging

import pytest

from agent import tool_executor as executor


@pytest.mark.parametrize("tool", ["request_multi_select", "durable_request"])
@pytest.mark.parametrize("is_error", [False, True])
def test_interaction_results_are_not_logged_even_in_verbose_mode(monkeypatch, caplog, tool, is_error):
    caplog.set_level(logging.DEBUG)
    private = '{"selected_labels":["PRIVATE-label"],"question":"PRIVATE-body"}'
    agent = SimpleNamespace(
        verbose_logging=True,
        _append_guardrail_observation=lambda name, args, result, **kwargs: result,
        _record_file_mutation_result=lambda *args, **kwargs: None,
        _touch_activity=lambda message: None,
        _subdirectory_hints=SimpleNamespace(check_tool_call=lambda *args: None),
        _tool_result_content_for_active_model=lambda name, result: result,
    )
    monkeypatch.setattr(executor, "get_active_env", lambda task_id: None)
    monkeypatch.setattr(executor, "maybe_persist_tool_result", lambda **kwargs: kwargs["content"])
    monkeypatch.setattr(executor, "_record_persisted_path_for_stub", lambda *args: None)
    monkeypatch.setattr(executor, "_flush_session_db_after_tool_progress", lambda *args, **kwargs: False)
    messages = []
    executor._commit_tool_result(agent, messages, executor._ToolCallRef(tool, {}, "task", "call", []), private,
        budget=None, tool_duration=1.0, is_error=is_error, blocked=False,
        effect_disposition=None, observed=True, success_log_chars=len(private))
    assert "PRIVATE" not in caplog.text
    assert tool in caplog.text
    assert private in str(messages)
