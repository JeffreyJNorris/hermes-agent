"""Real code-kernel RPC preserves one exact operator-approved script boundary."""

import json
import sqlite3

import pytest

from tests.tools.test_durable_execution_approval import profile, _approve_and_run
from gateway.durable_actions import DurableActionStatus
from gateway.durable_requests import get_durable_request_store


HOST_COMMAND = "ssh -F /root/.hermes/host-access/ssh_config hermes-host 'sudo -n /usr/bin/id -u'"


def _intercept_host_execution(monkeypatch, task_id):
    from tools import terminal_tool
    from tools.environments.local import LocalEnvironment
    from tools.host_sudo import requires_host_sudo_approval
    original = LocalEnvironment.execute
    executions = []
    def execute(env, command, **kwargs):
        if requires_host_sudo_approval(command, 'local'):
            executions.append(command)
            return {'output': '0\n', 'exit_code': 0}
        return original(env, command, **kwargs)
    monkeypatch.setattr(LocalEnvironment, 'execute', execute)
    # Real terminal guards run after backend initialization; stabilize that
    # same local backend before the parent code ticket captures its context.
    assert json.loads(terminal_tool.terminal_tool('true', task_id=task_id, workdir='/tmp'))['exit_code'] == 0
    return executions


def _requests():
    with sqlite3.connect(get_durable_request_store().db_path) as conn:
        return conn.execute('SELECT request_id, kind, status FROM durable_requests ORDER BY created_at').fetchall()


def test_operator_approves_outer_script_once_and_real_rpc_does_not_stage_nested_request(profile, monkeypatch):
    from tools import approval, code_execution_tool
    executions = _intercept_host_execution(monkeypatch, 'approved-host-rpc')
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    code = f'from hermes_tools import terminal\nr = terminal({HOST_COMMAND!r}, timeout=30)\nprint("host result:", r["output"].strip())'
    pending = json.loads(code_execution_tool.execute_code(code, task_id='approved-host-rpc', reset=True))
    assert pending['status'] == 'approval_pending', pending
    request = get_durable_request_store().get(request_id=pending['request_id'])
    assert request.kind == 'tool.durable_approval.host.execute_code.execute'
    assert [option.id for option in request.options] == ['approve', 'reject']
    assert executions == []
    ticket = _approve_and_run(pending['request_id'], 'execute_code.execute')
    assert ticket.status is DurableActionStatus.SUCCEEDED, ticket
    result = json.loads(ticket.outcome_json)
    assert 'host result: 0' in result['output'], result
    assert executions == [HOST_COMMAND]
    assert len(_requests()) == 1


def test_unreviewed_dynamic_nested_host_command_needs_its_own_operator_request(profile, monkeypatch):
    from tools import approval, code_execution_tool
    executions = _intercept_host_execution(monkeypatch, 'changed-host-rpc')
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    code = (f'from hermes_tools import terminal\n'
            f'first = terminal({HOST_COMMAND!r}, timeout=30)\n'
            f'changed = {HOST_COMMAND!r} + " -g"\n'
            f'second = terminal(changed, timeout=30)\n'
            f'print("second status:", second.get("status"))')
    pending = json.loads(code_execution_tool.execute_code(code, task_id='changed-host-rpc', reset=True))
    assert pending['status'] == 'approval_pending', pending
    assert get_durable_request_store().get(request_id=pending['request_id']).kind == 'tool.durable_approval.host.execute_code.execute'
    ticket = _approve_and_run(pending['request_id'], 'execute_code.execute')
    assert ticket.status is DurableActionStatus.SUCCEEDED, ticket
    result = json.loads(ticket.outcome_json)
    assert 'second status: approval_pending' in result['output'], result
    assert executions == [HOST_COMMAND]
    rows = _requests()
    assert len(rows) == 2
    assert rows[1][1] == 'tool.durable_approval.host.terminal.execute'


@pytest.mark.parametrize('mode', ['off', 'permanent'])
def test_outer_literal_host_rpc_cannot_use_ordinary_code_auto_allow(profile, monkeypatch, mode):
    from tools import approval, code_execution_tool
    executions = _intercept_host_execution(monkeypatch, 'bypassed-host-rpc')
    path = profile / 'config.yaml'
    if mode == 'off':
        path.write_text(path.read_text().replace('mode: manual', 'mode: off'))
    else:
        monkeypatch.setattr(approval, 'is_approved', lambda *args: True)
    code = f'from hermes_tools import terminal\nterminal({HOST_COMMAND!r}, timeout=30)'
    pending = json.loads(code_execution_tool.execute_code(code, task_id='bypassed-host-rpc', reset=True))
    assert pending['status'] == 'approval_pending', pending
    request = get_durable_request_store().get(request_id=pending['request_id'])
    assert request.kind == 'tool.durable_approval.host.execute_code.execute'
    assert [option.id for option in request.options] == ['approve', 'reject']
    assert executions == []
