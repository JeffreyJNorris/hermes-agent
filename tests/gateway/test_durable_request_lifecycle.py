from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import gateway.durable_request_bridge as bridge_module
from gateway.run import GatewayRunner
from hermes_constants import get_hermes_home


@pytest.mark.asyncio
async def test_gateway_worker_processes_primary_profile_under_its_home(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = []
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner._shutdown_event = asyncio.Event()
    runner._durable_request_home = tmp_path
    runner._durable_request_bridges = {}
    runner._durable_request_task = None
    runner._background_tasks = set()
    runner.adapters = {"discord": object()}
    runner.config = SimpleNamespace(multiplex_profiles=False)

    class Bridge:
        async def process_once(self, adapters):
            calls.append((Path(get_hermes_home()), dict(adapters)))
            runner._shutdown_event.set()

    monkeypatch.setattr(bridge_module, "DurableRequestBridge", Bridge)

    await runner._run_durable_request_worker()

    assert calls == [(tmp_path, runner.adapters)]
    assert runner._durable_request_task is None


@pytest.mark.asyncio
async def test_gateway_worker_task_is_owned_by_background_lifecycle(
    monkeypatch,
) -> None:
    runner = object.__new__(GatewayRunner)
    runner._durable_request_task = None
    runner._background_tasks = set()
    started = asyncio.Event()

    async def worker():
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runner, "_run_durable_request_worker", worker)

    runner._start_durable_request_worker()
    task = runner._durable_request_task
    await started.wait()

    assert task in runner._background_tasks
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
