from __future__ import annotations

from gateway.durable_operations import DurableHealthResult


def test_main_doctor_renders_durable_warning_without_fixing(monkeypatch, capsys):
    from hermes_cli import doctor

    monkeypatch.setattr(
        "hermes_cli.durable.durable_health_result",
        lambda: DurableHealthResult(
            "warning", "worker_confirmation_unavailable", (("workers", 1),)
        ),
    )
    issues = []

    doctor._check_durable_operations(issues)

    output = capsys.readouterr().out
    assert "Durable operations" in output
    assert "worker confirmation unavailable" in output
    assert issues == ["Inspect durable operations: hermes durable doctor"]
