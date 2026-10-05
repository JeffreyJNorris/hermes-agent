from dataclasses import replace
import subprocess
from types import SimpleNamespace

import pytest

from gateway.durable_actions import (
    DurableActionExecutionContext, DurableActionStatus, decode_durable_action_result,
)


@pytest.fixture(autouse=True)
def active_origin(monkeypatch):
    import gateway.durable_requests
    import hermes_state
    monkeypatch.setattr(gateway.durable_requests, 'get_durable_request',
                        lambda **k: SimpleNamespace(origin_type='session', origin_session_id='session-1'))
    monkeypatch.setattr(hermes_state.SessionDB, 'get_session', lambda *a: {'ended_at': None})
    monkeypatch.setattr(hermes_state.SessionDB, '__init__', lambda *a, **k: None)
    monkeypatch.setattr(hermes_state.SessionDB, 'close', lambda *a: None)


def context(prepared):
    return DurableActionExecutionContext('request-1', prepared.action_type,
        prepared.action_version, prepared.payload_json, prepared.fingerprint,
        prepared.idempotency_key)


def test_only_explicit_request_is_intercepted():
    from tools import host_sudo
    assert host_sudo.parse_host_sudo_command('echo sudo') is None
    assert host_sudo.parse_host_sudo_command("hermes host sudo -- /usr/bin/printf '%s' 'two words'") == ['/usr/bin/printf', '%s', 'two words']


@pytest.mark.parametrize('command', [
    'hermes host sudo -- id', 'hermes host sudo --',
    'hermes host sudo -- /usr/bin/id && /usr/bin/id',
    'hermes host sudo -- /usr/bin/id; /usr/bin/id',
])
def test_invalid_or_compound_request_is_rejected(command):
    from tools import host_sudo
    with pytest.raises(ValueError):
        host_sudo.parse_host_sudo_command(command)


def test_preparation_never_executes_and_argv_is_immutable(monkeypatch):
    from tools import host_sudo
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: pytest.fail('executed before approval'))
    argv = ['/usr/bin/id', '-u']
    prepared = host_sudo.prepare_host_sudo(argv, timeout=30)
    argv.append('changed')
    assert prepared.action_type == 'host.sudo'
    assert 'changed' not in prepared.payload_json


def test_executor_runs_exact_argv_without_shell_and_redacts_output(monkeypatch):
    from tools import host_sudo
    recorded = []
    def run(argv, **kwargs):
        recorded.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, '0\n', '')
    monkeypatch.setattr(subprocess, 'run', run)
    prepared = host_sudo.prepare_host_sudo(['/usr/bin/id', '-u'], timeout=30)
    result = host_sudo.execute_host_sudo(context(prepared))
    assert result.status is DurableActionStatus.SUCCEEDED
    assert recorded[0][0] == ['/usr/bin/sudo', '-n', '--', '/usr/bin/id', '-u']
    assert recorded[0][1]['shell'] is False
    assert recorded[0][1]['stdin'] is subprocess.DEVNULL
    assert decode_durable_action_result(result.result_json)['output'] == '0\n'


def test_tampered_payload_cannot_execute(monkeypatch):
    from tools import host_sudo
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: pytest.fail('executed changed payload'))
    prepared = host_sudo.prepare_host_sudo(['/usr/bin/id'], timeout=30)
    changed = replace(context(prepared), fingerprint='incorrect')
    assert host_sudo.execute_host_sudo(changed).status is DurableActionStatus.INVALIDATED


def test_command_timeout_is_unknown_and_never_retried(monkeypatch):
    from tools import host_sudo
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs['timeout'])
    monkeypatch.setattr(subprocess, 'run', timeout)
    prepared = host_sudo.prepare_host_sudo(['/usr/bin/id'], timeout=30)
    assert host_sudo.execute_host_sudo(context(prepared)).status is DurableActionStatus.UNKNOWN


def test_manual_cli_does_not_bypass_approval(capsys):
    import argparse
    from hermes_cli.subcommands.host import build_host_parser
    parser = argparse.ArgumentParser()
    build_host_parser(parser.add_subparsers())
    args = parser.parse_args(['host', 'sudo', '--', '/usr/bin/id'])
    assert args.func(args) == 2
    assert 'Discord' in capsys.readouterr().err


def test_unicode_output_is_bounded_for_durable_result(monkeypatch):
    from tools import host_sudo
    monkeypatch.setattr(subprocess, 'run', lambda argv, **k:
                        subprocess.CompletedProcess(argv, 0, '🦉' * 12000, ''))
    result = host_sudo.execute_host_sudo(context(host_sudo.prepare_host_sudo(['/usr/bin/id'])))
    assert result.status is DurableActionStatus.SUCCEEDED
    assert len(result.result_json.encode()) <= 16384


def test_ended_origin_cannot_execute_privileged_command(monkeypatch):
    from tools import host_sudo
    import hermes_state
    monkeypatch.setattr(hermes_state.SessionDB, 'get_session', lambda *a: {'ended_at': 123})
    monkeypatch.setattr(subprocess, 'run', lambda *a, **k: pytest.fail('executed stale approved request'))
    result = host_sudo.execute_host_sudo(context(host_sudo.prepare_host_sudo(['/usr/bin/id'])))
    assert result.status is DurableActionStatus.INVALIDATED


def test_catastrophic_command_cannot_be_requested():
    from tools import host_sudo
    with pytest.raises(ValueError):
        host_sudo.prepare_host_sudo(['/bin/bash', '-lc', 'rm -rf /'])
