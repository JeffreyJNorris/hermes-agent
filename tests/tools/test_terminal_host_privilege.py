"""Privileged terminal calls reach the operator gate before normal allowances."""

import json
from contextlib import contextmanager

import pytest

from hermes_constants import reset_hermes_home_override, set_hermes_home_override


@pytest.fixture
def local_profile(tmp_path):
    token = set_hermes_home_override(tmp_path)
    (tmp_path / 'config.yaml').write_text('terminal:\n  backend: local\n  cwd: /tmp\napprovals:\n  mode: off\n')
    try:
        yield tmp_path
    finally:
        reset_hermes_home_override(token)


@pytest.mark.parametrize('command', [
    'sudo -n /usr/bin/id -u',
    "ssh -F /root/.hermes/host-access/ssh_config hermes-host 'sudo -n /usr/bin/systemctl --user --machine=jeff@.host show hermes-google-tasks.timer && printf done'",
    "bash -lc \"ssh -F /root/conf hermes-host 'sudo -n id'\"",
])
def test_terminal_host_sudo_requires_operator_before_force_and_bypass(local_profile, monkeypatch, command):
    from tools import approval, durable_execution_approval as durable, terminal_tool
    calls = []
    original_context = durable.durable_execution_context
    @contextmanager
    def capture_context(*args, privileged_host=False, **kwargs):
        calls.append(('context', privileged_host))
        with original_context(*args, **kwargs):
            yield
    def defer(reviewed, reason, *, required=False):
        calls.append(('operator', reviewed, required))
        return {'approved': False, 'status': 'approval_pending', 'request_id': 'persistent-request'}
    monkeypatch.setattr(durable, 'durable_execution_context', capture_context)
    monkeypatch.setattr(durable, 'maybe_defer_execution', defer)
    monkeypatch.setattr(approval, '_yolo_active', lambda: True)
    monkeypatch.setattr(approval, '_command_matches_permanent_allowlist', lambda command: True)
    monkeypatch.setattr(terminal_tool, '_run_approval_guards', lambda *args, **kwargs:
                        pytest.fail('ordinary guard bypassed mandatory operator approval'))
    result = json.loads(terminal_tool.terminal_tool(command, task_id='privileged-test', force=True))
    assert result['status'] == 'approval_pending', result
    assert calls == [('context', True), ('operator', command, True)]


def test_terminal_host_sudo_unavailable_gate_fails_closed(local_profile, monkeypatch):
    from tools import durable_execution_approval as durable, terminal_tool
    original_context = durable.durable_execution_context
    @contextmanager
    def compatible_context(*args, privileged_host=False, **kwargs):
        with original_context(*args, **kwargs):
            yield
    monkeypatch.setattr(durable, 'durable_execution_context', compatible_context)
    monkeypatch.setattr(durable, 'maybe_defer_execution', lambda *args, **kwargs:
                        {'approved': False, 'status': 'blocked', 'message': 'Operator approval is unavailable.'})
    result = json.loads(terminal_tool.terminal_tool('sudo -n /usr/bin/id -u', task_id='unavailable-test', force=True))
    assert result['status'] == 'blocked'
    assert result['exit_code'] != 0


def test_inline_host_completion_returns_existing_result_without_dispatch(local_profile, monkeypatch):
    from tools import durable_execution_approval as durable, terminal_tool
    original_context = durable.durable_execution_context
    @contextmanager
    def compatible_context(*args, privileged_host=False, **kwargs):
        with original_context(*args, **kwargs):
            yield
    native = {'output': 'operator-reviewed result', 'exit_code': 0}
    monkeypatch.setattr(durable, 'durable_execution_context', compatible_context)
    monkeypatch.setattr(durable, 'maybe_defer_execution', lambda *args, **kwargs:
                        {'approved': False, 'status': 'execution_completed', 'tool_result': native})
    monkeypatch.setattr(terminal_tool, '_run_approval_guards', lambda *args, **kwargs:
                        pytest.fail('completed operation was dispatched again'))
    assert json.loads(terminal_tool.terminal_tool('sudo -n /usr/bin/id -u', task_id='completed-host')) == native


def test_inline_ordinary_completion_returns_existing_result_without_dispatch(local_profile, monkeypatch):
    from tools import terminal_tool
    native = {'output': 'completed once', 'exit_code': 0}
    monkeypatch.setattr(terminal_tool, '_check_all_guards', lambda *args, **kwargs:
                        {'approved': False, 'status': 'execution_completed', 'tool_result': native})
    with pytest.raises(terminal_tool._Rejected) as rejected:
        terminal_tool._run_approval_guards('printf ordinary', 'local', {}, force=False)
    assert json.loads(rejected.value.result_json) == native
