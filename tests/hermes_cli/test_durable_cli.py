from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pytest

from gateway.durable_actions import PreparedDurableAction
from gateway.durable_operations import DurableOperationsStore
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)
from hermes_cli.durable import EXIT_INVALID, EXIT_OK, EXIT_WARNING, durable_command
from hermes_cli.subcommands.durable import build_durable_parser


def _parser(handler=durable_command) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    build_durable_parser(subparsers, cmd_durable=handler)
    return parser


def _seed_home(tmp_path: Path) -> str:
    result = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 100.0).create(
        DurableRequestSpec(
            kind="test.release",
            title="PHASE4D-CANARY title",
            body="PHASE4D-CANARY body",
            options=(
                DurableRequestOption("approve", "PHASE4D-CANARY approve"),
                DurableRequestOption("reject", "PHASE4D-CANARY reject"),
            ),
            ttl_seconds=1_000,
            reference="cli-test",
        ),
        target=DurableRequestTarget("discord", "channel-secret", "thread-secret"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="origin-secret",
    )
    return result.request.request_id


def _seed_unknown_home(tmp_path: Path) -> str:
    store = DurableRequestStore(tmp_path / "state.db", wall_clock=lambda: 100.0)
    result = store.create_with_action(
        DurableRequestSpec(
            kind="approval",
            title="PHASE4D-CANARY title",
            body="PHASE4D-CANARY body",
            options=(DurableRequestOption("approve", "yes"), DurableRequestOption("reject", "no")),
            ttl_seconds=1_000,
            reference="unknown-cli",
        ),
        action=PreparedDurableAction(
            action_type="cron.resume",
            action_version=1,
            payload_json='{"job_id":"secret"}',
            fingerprint="secret-fingerprint",
            idempotency_key="secret-key",
        ),
        target=DurableRequestTarget("discord", "secret-channel"),
        responder_mode=DurableResponderMode.ANY_AUTHORIZED,
        expected_actor_id=None,
        origin_type="session",
        origin_id="secret-origin",
        origin_session_id="secret-session",
    )
    with sqlite3.connect(tmp_path / "state.db") as db:
        db.execute(
            "UPDATE durable_requests SET status = 'selected', resolved_at = 100 "
            "WHERE request_id = ?",
            (result.request.request_id,),
        )
        db.execute(
            "UPDATE durable_action_tickets SET status = 'unknown', completed_at = 100, "
            "notification_status = 'delivered' WHERE request_id = ?",
            (result.request.request_id,),
        )
    return result.request.request_id


def test_parser_wires_complete_durable_tree() -> None:
    sentinel = object()
    parser = _parser(lambda args: sentinel)

    request_list = parser.parse_args(
        [
            "durable",
            "requests",
            "list",
            "--status",
            "open",
            "--origin-type",
            "session",
            "--limit",
            "7",
            "--offset",
            "2",
            "--json",
        ]
    )
    request_inspect = parser.parse_args(
        ["durable", "requests", "inspect", "drq_x", "--json"]
    )
    request_cancel = parser.parse_args(
        ["durable", "requests", "cancel", "drq_x", "--yes", "--json"]
    )
    action_list = parser.parse_args(
        [
            "durable",
            "actions",
            "list",
            "--status",
            "ready",
            "--action-type",
            "cron.resume",
            "--minimum-age-seconds",
            "300",
        ]
    )
    action_inspect = parser.parse_args(
        ["durable", "actions", "inspect", "drq_x", "--json"]
    )

    assert request_list.durable_group == "requests"
    assert request_list.durable_action == "list"
    assert request_list.limit == 7 and request_list.offset == 2
    assert request_inspect.request_id == "drq_x"
    assert request_cancel.yes is True
    assert action_list.action_type == "cron.resume"
    assert action_list.minimum_age_seconds == 300.0
    assert action_inspect.func(action_inspect) is sentinel


def test_json_list_is_clean_bounded_and_redacted(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    request_id = _seed_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    args = _parser().parse_args(["durable", "requests", "list", "--json"])

    assert durable_command(args) == EXIT_OK
    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert output.err == ""
    assert payload["code"] == "ok"
    assert payload["items"][0]["request_id"] == request_id
    assert payload["next_offset"] is None
    assert "PHASE4D-CANARY" not in output.out
    for forbidden in ("body", "title", "options", "origin_id", "chat_id"):
        assert forbidden not in payload["items"][0]


def test_json_cancel_requires_yes_without_prompt(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _seed_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    args = _parser().parse_args(
        ["durable", "requests", "cancel", "drq_x", "--json"]
    )
    monkeypatch.setattr(
        "builtins.input", lambda _: pytest.fail("must not prompt")
    )

    assert durable_command(args) == EXIT_INVALID
    assert json.loads(capsys.readouterr().out)["code"] == "confirmation_required"


def test_human_cancel_defaults_to_no(tmp_path: Path, monkeypatch, capsys) -> None:
    request_id = _seed_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "")
    args = _parser().parse_args(
        ["durable", "requests", "cancel", request_id]
    )

    assert durable_command(args) == EXIT_INVALID
    assert "cancelled" not in capsys.readouterr().out.lower()


def test_yes_bypasses_prompt_and_cancels(tmp_path: Path, monkeypatch, capsys) -> None:
    request_id = _seed_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        "builtins.input", lambda _: pytest.fail("--yes must bypass prompt")
    )
    args = _parser().parse_args(
        ["durable", "requests", "cancel", request_id, "--yes", "--json"]
    )

    assert durable_command(args) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "action_status": None,
        "code": "cancelled",
        "ok": True,
        "request_id": request_id,
        "request_status": "cancelled",
    }


def test_operator_command_is_absent_from_model_and_slash_surfaces() -> None:
    from hermes_cli.commands import (
        COMMAND_REGISTRY,
        GATEWAY_KNOWN_COMMANDS,
        resolve_command,
        slack_subcommand_map,
        telegram_bot_commands,
    )
    from tools.registry import registry
    from toolsets import TOOLSETS, _HERMES_CORE_TOOLS, resolve_toolset

    assert registry.get_entry("durable") is None
    assert "durable" not in _HERMES_CORE_TOOLS
    for preset in TOOLSETS:
        assert "durable" not in resolve_toolset(preset), preset
    assert resolve_command("durable") is None
    assert "durable" not in GATEWAY_KNOWN_COMMANDS
    assert "durable" not in {command.name for command in COMMAND_REGISTRY}
    assert "durable" not in {name for name, _ in telegram_bot_commands()}
    assert "durable" not in slack_subcommand_map()


def test_json_reconcile_requires_yes_without_prompt(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    request_id = _seed_unknown_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        "builtins.input", lambda _: pytest.fail("must not prompt")
    )
    args = _parser().parse_args(
        [
            "durable",
            "actions",
            "reconcile",
            request_id,
            "--outcome",
            "abandoned",
            "--json",
        ]
    )

    assert durable_command(args) == EXIT_INVALID
    assert json.loads(capsys.readouterr().out)["code"] == "confirmation_required"


def test_human_superseded_confirmation_names_successor(monkeypatch, capsys) -> None:
    class FakeStore:
        def __init__(self, _path) -> None:
            pass

        def get_action(self, _request_id):
            return type("Detail", (), {"status": "unknown"})()

    monkeypatch.setattr("hermes_cli.durable.DurableOperationsStore", FakeStore)
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(
        "hermes_cli.durable.prompt_yes_no", lambda *_args, **_kwargs: False
    )
    args = _parser().parse_args(
        [
            "durable",
            "actions",
            "reconcile",
            "drq_old",
            "--outcome",
            "superseded",
            "--superseded-by",
            "drq_new",
        ]
    )

    assert durable_command(args) == EXIT_INVALID
    assert "drq_new" in capsys.readouterr().out


def test_json_reconcile_records_local_identity_without_handler(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    request_id = _seed_unknown_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr("hermes_cli.durable.getpass.getuser", lambda: "local-user")
    monkeypatch.setattr(
        "gateway.durable_actions.lookup_durable_action_handler",
        lambda *_: pytest.fail("reconciliation must not look up a handler"),
    )
    args = _parser().parse_args(
        [
            "durable",
            "actions",
            "reconcile",
            request_id,
            "--outcome",
            "confirmed_not_applied",
            "--note",
            "operator checked",
            "--yes",
            "--json",
        ]
    )

    assert durable_command(args) == EXIT_OK
    output = capsys.readouterr().out
    assert json.loads(output)["code"] == "reconciled"
    assert "PHASE4D-CANARY" not in output
    with sqlite3.connect(tmp_path / "state.db") as db:
        assert db.execute(
            "SELECT outcome, operator_identity, note FROM durable_action_reconciliations"
        ).fetchone() == ("confirmed_not_applied", "local-user", "operator checked")


def test_json_action_diagnostic_never_serializes_execution_material(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    request_id = _seed_unknown_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    args = _parser().parse_args(
        ["durable", "actions", "inspect", request_id, "--diagnose", "--json"]
    )

    assert durable_command(args) == EXIT_OK
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["request_id"] == request_id
    assert payload["diagnostic"]["code"] in {
        "handler_unavailable",
        "target_missing",
    }
    for canary in ("secret", "fingerprint", "idempotency", "payload_json"):
        assert canary not in output


def test_json_list_reports_corrupt_database_by_exception_class_only(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    (tmp_path / "state.db").write_bytes(b"PHASE4D-CANARY corrupt database")
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    args = _parser().parse_args(["durable", "requests", "list", "--json"])

    assert durable_command(args) == 5
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload == {"code": "database_unavailable", "error": "DatabaseError"}
    assert "PHASE4D-CANARY" not in output


def test_json_mutation_reports_locked_database_by_exception_class_only(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    request_id = _seed_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        DurableOperationsStore,
        "_write_connection",
        lambda _self: (_ for _ in ()).throw(
            sqlite3.OperationalError("database is locked PHASE4D-CANARY")
        ),
    )
    args = _parser().parse_args(
        ["durable", "requests", "cancel", request_id, "--yes", "--json"]
    )

    assert durable_command(args) == 5
    output = capsys.readouterr().out
    assert json.loads(output) == {
        "code": "database_unavailable",
        "error": "OperationalError",
    }
    assert "PHASE4D-CANARY" not in output


def test_durable_doctor_json_uses_shared_health_result(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    _seed_home(tmp_path)
    monkeypatch.setattr("hermes_cli.durable.get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        "hermes_cli.durable.load_config_readonly",
        lambda: {"durable_requests": {"enabled": False}},
    )
    args = _parser().parse_args(["durable", "doctor", "--json"])

    assert durable_command(args) == EXIT_WARNING
    payload = json.loads(capsys.readouterr().out)
    assert payload["severity"] == "warning"
    assert payload["code"] == "actionable_backlog"
