"""Content-free gauges derived from the durable SQLite ledgers."""

from __future__ import annotations

import time
from pathlib import Path

from agent.monitoring.gateway_health import GatewayHealthSnapshot, GatewayMetric
from gateway.durable_operations import DurableOperationsStore
from hermes_constants import get_hermes_home


def build_durable_health_snapshot(
    *,
    db_path: str | Path | None = None,
    now: float | None = None,
) -> GatewayHealthSnapshot:
    store = DurableOperationsStore(
        db_path or Path(get_hermes_home()) / "state.db",
        reconcile_schema=False,
    )
    metrics = [
        GatewayMetric(datum.name, datum.value, dict(datum.attributes))
        for datum in store.collect_health_data(now=time.time() if now is None else now)
    ]
    return GatewayHealthSnapshot(metrics=metrics, events=[])


__all__ = ["build_durable_health_snapshot"]
