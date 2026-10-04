from types import SimpleNamespace

import gateway.interactive as interactive
from agent.monitoring import gateway_health_export as exporter
from gateway.interactive import InteractivePromptRegistry, InteractiveMultiSelectSpec, PromptOption, PromptScope


def test_runtime_exports_registered_content_free_selection_counts_and_gauges(monkeypatch):
    now = [1.0]
    registry = InteractivePromptRegistry(clock=lambda: now[0])
    monkeypatch.setattr(interactive, "_registry", registry)
    monkeypatch.setattr(exporter, "_read_gateway_snapshot", lambda config: SimpleNamespace(metrics=[], events=[]))
    monkeypatch.setattr(exporter, "_read_cron_snapshot", lambda: SimpleNamespace(metrics=[]))
    monkeypatch.setattr(exporter, "_read_durable_snapshot", lambda: SimpleNamespace(metrics=[]))
    monkeypatch.setattr(exporter, "_read_background_work_count", lambda: 0)
    monkeypatch.setattr(exporter, "_read_background_delegations_count", lambda: 0)
    handle = registry.register_multi_select_prompt(InteractiveMultiSelectSpec(
        kind="private-kind", title="private-title", body="private-body",
        options=(PromptOption("private-id1", "private-label1"), PromptOption("private-id2", "private-label2")),
        min_selections=1, max_selections=2, timeout_seconds=2,
    ), scope=PromptScope("private-session", "discord", "private-channel", None, "private-actor"))
    registry.bind_delivery(prompt_id=handle.prompt.prompt_id, platform="discord", message_id="private-message")
    registry.resolve_multi_select_prompt(prompt_id=handle.prompt.prompt_id, option_tokens=("1", "1"),
        platform="discord", channel_id="private-channel", message_id="private-message", actor_id="private-actor", source="native")
    now[0] = 4.0
    metrics = [metric for metric in exporter._read_runtime_snapshot({}).metrics
               if metric.name.startswith("hermes_interactive_multi_select_")]
    assert {metric.name.removeprefix("hermes_interactive_multi_select_"): metric.value for metric in metrics} == {
        "submissions": 1, "invalid_sets": 1, "active": 1, "stuck": 1,
    }
    assert all(metric.attributes == {} and metric.name in exporter._OBSERVABLE_METRIC_NAMES for metric in metrics)
    assert "private" not in repr(metrics) and handle.prompt.prompt_id not in repr(metrics)
