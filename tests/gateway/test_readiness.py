from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from gateway.readiness import collect_runtime_readiness
from gateway.durable_actions import PreparedDurableAction
from gateway.durable_operations import DurableOperationsStore
from gateway.durable_requests import (
    DurableRequestOption,
    DurableRequestSpec,
    DurableRequestStore,
    DurableRequestTarget,
    DurableResponderMode,
)


def test_collect_runtime_readiness_reports_healthy_local_runtime(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "config.yaml").write_text(
        "model:\n  provider: openrouter\n  model: test/model\n",
        encoding="utf-8",
    )
    with sqlite3.connect(home / "state.db") as conn:
        conn.execute("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr("gateway.readiness._probe_disk", lambda _home: {"status": "ok"})

    result = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status={
            "gateway_state": "running",
            "platforms": {"telegram": {"state": "connected"}},
            "updated_at": "2026-07-09T00:00:00Z",
        },
        active_api_runs=2,
    )

    assert result["status"] == "ok"
    assert result["checks"]["state_db"]["status"] == "ok"
    assert result["checks"]["session_store"]["status"] == "ok"
    assert result["checks"]["config"]["status"] == "ok"
    assert result["checks"]["model"]["status"] == "ok"
    assert result["checks"]["gateway"]["status"] == "ok"
    assert result["checks"]["background_queues"]["active_api_runs"] == 2
    assert result["checks"]["disk"]["status"] in {"ok", "degraded"}


def test_collect_runtime_readiness_degrades_on_invalid_config_and_stopped_gateway(
    tmp_path, monkeypatch
):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "config.yaml").write_text("model: [unterminated", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))

    result = collect_runtime_readiness(
        configured_model="",
        runtime_status={"gateway_state": "stopped", "platforms": {}},
    )

    assert result["status"] == "degraded"
    assert result["checks"]["config"]["status"] == "degraded"
    assert result["checks"]["model"]["status"] == "degraded"
    assert result["checks"]["gateway"]["status"] == "degraded"
    # Readiness is diagnostic data, not an exception or a destructive repair.
    assert (home / "config.yaml").read_text(encoding="utf-8") == "model: [unterminated"


def test_readiness_uses_running_session_store_state_over_independent_probe(
    tmp_path, monkeypatch
):
    home = tmp_path / ".hermes"
    home.mkdir()
    with sqlite3.connect(home / "state.db") as conn:
        conn.execute("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
    monkeypatch.setenv("HERMES_HOME", str(home))

    unavailable = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status={
            "gateway_state": "running",
            "platforms": {},
            "session_store": {"status": "unavailable"},
        },
    )

    assert unavailable["checks"]["state_db"]["status"] == "ok"
    assert unavailable["checks"]["session_store"] == {"status": "unavailable"}
    assert unavailable["status"] == "degraded"

    recovered = collect_runtime_readiness(
        configured_model="test/model",
        runtime_status={
            "gateway_state": "running",
            "platforms": {},
            "session_store": {"status": "ok"},
        },
    )
    assert recovered["checks"]["session_store"] == {"status": "ok"}


def _seed_durable(home: Path, *, unknown=False, reconciled=False) -> str:
    store = DurableRequestStore(home / "state.db", wall_clock=lambda: 100.0)
    spec = DurableRequestSpec(
        kind="approval",
        title="title",
        body="body",
        options=(DurableRequestOption("approve", "yes"), DurableRequestOption("reject", "no")),
        ttl_seconds=1_000,
        reference="readiness",
    )
    if unknown:
        result = store.create_with_action(
            spec,
            action=PreparedDurableAction(
                action_type="cron.resume",
                action_version=1,
                payload_json='{"job_id":"secret"}',
                fingerprint="fingerprint",
                idempotency_key="readiness",
            ),
            target=DurableRequestTarget("discord", "channel"),
            responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            expected_actor_id=None,
            origin_type="session",
            origin_id="origin",
            origin_session_id="session",
        )
    else:
        result = store.create(
            spec,
            target=DurableRequestTarget("discord", "channel"),
            responder_mode=DurableResponderMode.ANY_AUTHORIZED,
            expected_actor_id=None,
            origin_type="session",
            origin_id="origin",
        )
    request_id = result.request.request_id
    with sqlite3.connect(home / "state.db") as db:
        if unknown:
            db.execute(
                "UPDATE durable_requests SET status = 'selected', resolved_at = 100 WHERE request_id = ?",
                (request_id,),
            )
            db.execute(
                "UPDATE durable_action_tickets SET status = 'unknown', completed_at = 100, "
                "notification_status = 'delivered' WHERE request_id = ?",
                (request_id,),
            )
        else:
            db.execute(
                "UPDATE durable_requests SET created_at = 0, updated_at = 0 WHERE request_id = ?",
                (request_id,),
            )
    if reconciled:
        DurableOperationsStore(home / "state.db").reconcile_action(
            request_id, outcome="abandoned", operator_identity="operator", now=200
        )
    return request_id


def test_durable_readiness_warns_on_old_pending_and_missing_worker(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "config.yaml").write_text(
        "durable_requests:\n  enabled: true\n", encoding="utf-8"
    )
    _seed_durable(home)
    monkeypatch.setenv("HERMES_HOME", str(home))

    result = collect_runtime_readiness(
        configured_model="model",
        runtime_status={"gateway_state": "running"},
        durable_request_worker_alive=False,
        durable_action_worker_alive=True,
        now=500.0,
    )

    assert result["checks"]["durable"]["status"] == "degraded"
    assert result["checks"]["durable"]["severity"] == "warning"
    assert result["checks"]["durable"]["code"] == "actionable_backlog"


def test_unreconciled_unknown_is_critical_but_reconciled_unknown_is_healthy(
    tmp_path, monkeypatch
):
    for reconciled, expected in ((False, "critical"), (True, "ok")):
        home = tmp_path / str(reconciled)
        home.mkdir()
        (home / "config.yaml").write_text("{}\n", encoding="utf-8")
        _seed_durable(home, unknown=True, reconciled=reconciled)
        monkeypatch.setenv("HERMES_HOME", str(home))

        result = collect_runtime_readiness(
            configured_model="model",
            runtime_status={"gateway_state": "running"},
            now=500.0,
        )

        assert result["checks"]["durable"]["severity"] == expected


def test_corrupt_durable_database_is_critical(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    (home / "state.db").write_bytes(b"not sqlite PHASE4D-CANARY")
    (home / "config.yaml").write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))

    result = collect_runtime_readiness(
        configured_model="model", runtime_status={"gateway_state": "running"}
    )

    assert result["checks"]["durable"]["severity"] == "critical"
    assert result["checks"]["durable"]["code"] == "database_unavailable"
