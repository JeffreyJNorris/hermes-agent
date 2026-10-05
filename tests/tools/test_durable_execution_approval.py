from __future__ import annotations

import asyncio
import importlib.util
import json
import shlex
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from gateway.durable_actions import DurableActionExecutionContext, DurableActionStatus
from gateway.durable_requests import get_durable_request_store, reset_durable_request_store_cache
from gateway.session_context import reset_session_vars, set_session_vars
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


@pytest.fixture
def profile(tmp_path, monkeypatch):
    token = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    reset_session_vars()
    (tmp_path / 'config.yaml').write_text('''terminal:
  backend: local
  cwd: /tmp
approvals:
  mode: manual
durable_requests:
  enabled: true
  inbox:
    platform: discord
    chat_id: "999999999999999999"
durable_approvals:
  enabled: true
  allowed_actions: [terminal.execute, execute_code.execute, host.sudo]
  host_sudo_allowed_users: ["42"]
''')
    set_session_vars(platform='discord', chat_id='123456789012345678',
                     thread_id='123456789012345678', user_id='42',
                     session_key='agent:main:discord:thread:123456789012345678:42',
                     session_id='physical-session', cron_session='')
    from hermes_state import SessionDB
    db = SessionDB()
    db.create_session('physical-session', 'discord',
                      session_key='agent:main:discord:thread:123456789012345678:42',
                      chat_id='123456789012345678', user_id='42')
    db.close()
    from tools.approval_context import set_current_session_key, reset_current_session_key
    session = set_current_session_key('agent:main:discord:thread:123456789012345678:42')
    try:
        yield tmp_path
    finally:
        reset_current_session_key(session)
        reset_session_vars()
        reset_durable_request_store_cache()
        reset_hermes_home_override(token)


def _module():
    assert importlib.util.find_spec('tools.durable_execution_approval') is not None, 'durable execution bridge missing'
    from tools import durable_execution_approval
    return durable_execution_approval


def _stage(command, **args):
    module = _module()
    from tools.terminal_tool import _get_env_config
    arguments = dict(command=command, background=False, timeout=_get_env_config()['timeout'],
                     task_id='test-task', session_id=None, workdir='/tmp', pty=False,
                     notify_on_complete=False, watch_patterns=None, heartbeat=0,
                     persist_on_release=False)
    arguments.update(args)
    with module.durable_execution_context('terminal.execute', arguments):
        return module.maybe_defer_execution(command, 'script execution')


def _context(request_id):
    store = get_durable_request_store()
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute('SELECT * FROM durable_action_tickets WHERE request_id=?', (request_id,)).fetchone()
    return DurableActionExecutionContext(request_id, row['action_type'], row['action_version'],
                                         row['payload_json'], row['fingerprint'], row['idempotency_key'])


def test_stage_is_private_exact_and_in_origin_thread(profile):
    command = "printf '%s' 'sk-abcdefghijklmnopqrstuvwxyz0123456789'"
    result = _stage(command)
    assert result['status'] == 'approval_pending'
    assert result['approved'] is False
    request = get_durable_request_store().get(request_id=result['request_id'])
    assert request.target.delivery_channel_id == '123456789012345678'
    assert request.expected_actor_id == '42'
    assert 'sk-abcdefghijklmnopqrstuvwxyz0123456789' not in request.body
    payload = json.loads(_context(result['request_id']).payload_json)
    assert payload['arguments']['command'] == command


def test_missing_thread_never_uses_approvals_inbox(profile):
    set_session_vars(platform='discord', user_id='42', session_key='session', session_id='physical')
    result = _stage('echo hello')
    assert result['approved'] is False
    assert result['status'] == 'blocked'
    assert 'request_id' not in result


def test_disabled_feature_preserves_original_approval_path(profile):
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('allowed_actions: [terminal.execute, execute_code.execute, host.sudo]', 'allowed_actions: []'))
    assert _stage('echo hello') is None


def test_deferred_terminal_does_not_run_before_approval(profile, monkeypatch):
    from tools import approval, terminal_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    monkeypatch.setattr(approval, '_gateway_notify_cb', lambda session: pytest.fail('ephemeral approval used'))
    marker = profile / 'marker'
    marker.write_text('keep')
    result = json.loads(terminal_tool.terminal_tool(command=f'rm -rf -- {shlex.quote(str(marker))}', task_id='test-task'))
    assert result.get('status') == 'approval_pending', result
    assert marker.exists()


def test_exact_ticket_executes_once_through_durable_runner(profile):
    from gateway.durable_action_runner import DurableActionRunner
    from gateway.durable_actions import DurableActionStore, _DURABLE_ACTION_HANDLERS
    module = _module()
    _DURABLE_ACTION_HANDLERS.setdefault(('terminal.execute', 1), (module.prepare_durable_execution, module.execute_durable_execution))
    marker = profile / 'counter'
    result = _stage(f'printf x >> {shlex.quote(str(marker))}')
    store = get_durable_request_store()
    delivery = store.claim_next_pending(['discord'])
    assert store.bind_delivery(request_id=delivery.request.request_id, claim_id=delivery.claim_id,
                               platform='discord', message_id='message')
    decision = store.resolve(request_id=result['request_id'], platform='discord', channel_id='123456789012345678',
                             message_id='message', actor_id='42', option_token='1')
    runner = DurableActionRunner(DurableActionStore(store.db_path), enabled=True, allowed_actions=['terminal.execute'])
    assert asyncio.run(runner.process_once())
    assert not asyncio.run(runner.process_once())
    assert marker.read_text() == 'x'


def test_changed_backend_invalidates_before_execution(profile):
    marker = profile / 'never'
    result = _stage(f'touch {shlex.quote(str(marker))}')
    context = _context(result['request_id'])
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('backend: local', 'backend: ssh'))
    outcome = _module().execute_durable_execution(context)
    assert outcome.status is DurableActionStatus.INVALIDATED
    assert not marker.exists()


def test_payload_tampering_invalidates(profile):
    result = _stage('echo reviewed')
    context = _context(result['request_id'])
    payload = json.loads(context.payload_json)
    payload['arguments']['command'] = 'echo unreviewed'
    from gateway.durable_actions import encode_durable_action_payload
    modified = replace(context, payload_json=encode_durable_action_payload(payload))
    assert _module().execute_durable_execution(modified).status is DurableActionStatus.INVALIDATED


def test_pending_survives_request_store_restart(profile):
    result = _stage('echo waiting')
    reset_durable_request_store_cache()
    request = get_durable_request_store().get(request_id=result['request_id'])
    assert request.target.delivery_channel_id == '123456789012345678'
    assert request.expires_at - request.created_at >= 86400 - 1


def _approve_and_run(request_id, action_type='terminal.execute'):
    from gateway.durable_action_runner import DurableActionRunner
    from gateway.durable_actions import DurableActionStore, _DURABLE_ACTION_HANDLERS
    module = _module()
    _DURABLE_ACTION_HANDLERS.setdefault((action_type, 1), (module.prepare_durable_execution, module.execute_durable_execution))
    store = get_durable_request_store()
    delivery = store.claim_next_pending(['discord'])
    assert delivery is not None
    assert store.bind_delivery(request_id=request_id, claim_id=delivery.claim_id, platform='discord', message_id='message')
    assert store.resolve(request_id=request_id, option_token='1', platform='discord',
                         channel_id='123456789012345678', message_id='message', actor_id='42').status.value == 'resolved'
    runner = DurableActionRunner(DurableActionStore(store.db_path), enabled=True, allowed_actions=[action_type])
    assert asyncio.run(runner.process_once())
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute('SELECT * FROM durable_action_tickets WHERE request_id=?', (request_id,)).fetchone()
    from types import SimpleNamespace
    return SimpleNamespace(status=DurableActionStatus(row['status']), outcome_reason=row['outcome_reason'], outcome_json=row['outcome_json'])


def test_original_session_reset_invalidates_before_execution(profile):
    marker = profile / 'never-after-reset'
    result = _stage(f'touch {shlex.quote(str(marker))}')
    from hermes_state import SessionDB
    db = SessionDB()
    db.end_session('physical-session', 'new')
    db.close()
    ticket = _approve_and_run(result['request_id'])
    assert ticket.status is DurableActionStatus.INVALIDATED
    assert not marker.exists()


def test_real_dangerous_terminal_replay_uses_exact_approval(profile, monkeypatch):
    from tools import approval, terminal_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    monkeypatch.setattr(approval, '_gateway_notify_cb', lambda session: pytest.fail('ephemeral approval used'))
    marker = profile / 'remove-after-approval'
    marker.write_text('keep')
    result = json.loads(terminal_tool.terminal_tool(f'rm -rf -- {shlex.quote(str(marker))}', task_id='test-task'))
    assert marker.exists()
    ticket = _approve_and_run(result['request_id'])
    assert ticket.status is DurableActionStatus.SUCCEEDED
    assert not marker.exists()


def test_code_block_defers_and_executes_after_approval(profile, monkeypatch):
    from tools import approval, code_execution_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    monkeypatch.setattr(approval, '_gateway_notify_cb', lambda session: pytest.fail('ephemeral approval used'))
    marker = profile / 'code-result'
    code = f'from pathlib import Path\nPath({str(marker)!r}).write_text("done")\nprint("done")'
    result = json.loads(code_execution_tool.execute_code(code, task_id='code-task', reset=True))
    assert result.get('status') == 'approval_pending', result
    assert not marker.exists()
    ticket = _approve_and_run(result['request_id'], 'execute_code.execute')
    assert ticket.status is DurableActionStatus.SUCCEEDED, ticket
    assert marker.read_text() == 'done'


def test_lost_code_kernel_invalidates_instead_of_replaying(profile):
    from types import SimpleNamespace
    from tools.code_kernel import _REGISTRY, _resolve_owner
    module = _module()
    arguments = dict(code='print(existing_variable)', task_id='state-task', enabled_tools=None, reset=False)
    key = (_resolve_owner('state-task'), 'test')
    with _REGISTRY.lock:
        _REGISTRY.kernels[key] = SimpleNamespace(execution_count=3)
    try:
        state = module.capture_code_execution_state(arguments)
        with module.durable_execution_context('execute_code.execute', arguments, execution_state=state):
            result = module.maybe_defer_execution('print(existing_variable)', 'code script')
    finally:
        with _REGISTRY.lock:
            _REGISTRY.kernels.pop(key)
    ticket = _approve_and_run(result['request_id'], 'execute_code.execute')
    assert ticket.status is DurableActionStatus.INVALIDATED
    assert ticket.outcome_reason == 'code_kernel_state_changed'


def test_code_kernel_process_restart_invalidates_snapshot(profile, monkeypatch):
    from types import SimpleNamespace
    from tools.code_kernel import _REGISTRY, _resolve_owner
    module = _module()
    args = dict(code='print(existing_variable)', task_id='state-task', enabled_tools=None, reset=False)
    key = (_resolve_owner('state-task'), 'test')
    with _REGISTRY.lock:
        _REGISTRY.kernels[key] = SimpleNamespace(execution_count=3)
    try:
        state = module.capture_code_execution_state(args)
        assert state
        assert hasattr(module, '_kernel_process_generation'), 'kernel snapshots need a process-generation identity'
        monkeypatch.setattr(module, '_kernel_process_generation', 'restarted-process')
        assert module.capture_code_execution_state(args) != state
    finally:
        with _REGISTRY.lock:
            _REGISTRY.kernels.pop(key)


def test_privileged_prefix_is_always_staged_even_with_approvals_off(profile):
    from tools.terminal_tool import terminal_tool
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('mode: manual', 'mode: off'))
    result = json.loads(terminal_tool('hermes host sudo -- /usr/bin/id'))
    assert result['status'] == 'approval_pending'
    assert _context(result['request_id']).action_type == 'host.sudo'


def test_unicode_output_remains_bounded_success_after_execution(profile):
    command = "python3 -c " + shlex.quote("print('🦀' * 10000)")
    result = _stage(command)
    ticket = _approve_and_run(result['request_id'])
    assert ticket.status is DurableActionStatus.SUCCEEDED
    assert len(ticket.outcome_json.encode()) <= 16384
    assert json.loads(ticket.outcome_json)['output']


def test_background_approval_result_includes_process_id(profile):
    result = _stage('sleep 0.1', background=True)
    ticket = _approve_and_run(result['request_id'])
    assert ticket.status is DurableActionStatus.SUCCEEDED
    assert json.loads(ticket.outcome_json)['session_id']


def test_combined_guard_findings_share_one_exact_execution_consent(profile):
    module = _module()
    with module.durable_execution_context('terminal.execute', {'command': 'echo reviewed'}):
        operation = module._operation.get()
        token = module._consent.set({'operation_digest': module._digest(operation), 'action_type': 'terminal.execute'})
        try:
            assert module.maybe_defer_execution('echo reviewed', 'first finding')['approved']
            assert module.maybe_defer_execution('echo reviewed', 'second finding')['approved']
            with module.durable_execution_context('terminal.execute', {'command': 'echo changed'}):
                assert module.maybe_defer_execution('echo changed', 'finding')['status'] == 'invalidated'
        finally:
            module._consent.reset(token)


def test_approval_keeps_unconditional_command_floor(profile):
    module = _module()
    from tools import approval
    command = 'rm -rf /'
    with module.durable_execution_context('terminal.execute', {'command': command}):
        token = module._consent.set({'operation_digest': module._digest(module._operation.get()), 'action_type': 'terminal.execute'})
        try:
            assert approval.check_all_command_guards(command, 'local')['approved'] is False
        finally:
            module._consent.reset(token)


def test_durable_foreground_never_retries_ambiguous_backend_execution(profile):
    from types import SimpleNamespace
    from tools import terminal_tool
    module = _module()
    calls = []
    class Backend:
        def execute(self, command, **kwargs):
            calls.append(command)
            raise RuntimeError('connection lost after submitting the command')
    plan = SimpleNamespace(env_type='local', effective_task_id='task', effective_timeout=1,
                           cwd='/tmp', host_cwd=None)
    proof = module._consent.set({'operation_digest': 'proof', 'action_type': 'terminal.execute'})
    try:
        result = json.loads(terminal_tool._run_foreground('echo submitted', Backend(), plan,
                            task_id='task', session_id=None, session_key='session', workdir='/tmp',
                            approval_note=None, clear_interrupt=False))
    finally:
        module._consent.reset(proof)
    assert calls == ['echo submitted']
    assert result['status'] == 'unknown'


def test_reviewed_session_cwd_survives_a_later_cd(profile, monkeypatch):
    from tools import approval, terminal_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    first, second = profile / 'first', profile / 'second'
    first.mkdir()
    second.mkdir()
    (first / 'marker').write_text('first')
    (second / 'marker').write_text('second')
    terminal_tool.terminal_tool(f'cd {shlex.quote(str(first))}', task_id='cwd-task')
    result = json.loads(terminal_tool.terminal_tool('rm -rf -- marker', task_id='cwd-task'))
    assert result['status'] == 'approval_pending'
    assert json.loads(_context(result['request_id']).payload_json)['arguments']['workdir'] == str(first)
    terminal_tool.terminal_tool(f'cd {shlex.quote(str(second))}', task_id='cwd-task')
    ticket = _approve_and_run(result['request_id'])
    assert ticket.status is DurableActionStatus.SUCCEEDED, ticket
    assert not (first / 'marker').exists()
    assert (second / 'marker').read_text() == 'second'


@pytest.mark.parametrize('result', [
    {'exit_code': 124, 'output': 'partial'},
    {'status': 'interrupted', 'output': 'partial'},
])
def test_ambiguous_partial_outcomes_are_unknown(profile, monkeypatch, result):
    from tools import terminal_tool
    pending = _stage('echo reviewed')
    monkeypatch.setattr(terminal_tool, 'terminal_tool', lambda **args: json.dumps(result))
    ticket = _approve_and_run(pending['request_id'])
    assert ticket.status is DurableActionStatus.UNKNOWN


def test_durable_docker_dispatch_never_recreates_and_replays(profile, monkeypatch):
    from tools.environments.docker import DockerEnvironment
    from tools.environments.base import BaseEnvironment
    module = _module()
    calls = []
    def execute(self, command, cwd, **kwargs):
        calls.append(command)
        return {'returncode': 1, 'output': 'No such container'}
    monkeypatch.setattr(BaseEnvironment, 'execute', execute)
    env = DockerEnvironment.__new__(DockerEnvironment)
    env._persist_across_processes = True
    monkeypatch.setattr(env, '_recreate_container', lambda: calls.append('recreate') or True)
    proof = module._consent.set({'operation_digest': 'proof', 'action_type': 'terminal.execute'})
    try:
        result = env.execute('submitted')
    finally:
        module._consent.reset(proof)
    assert calls == ['submitted']
    assert result['returncode'] == 1


def test_dead_registered_kernel_cannot_be_silently_replaced(profile, monkeypatch):
    from types import SimpleNamespace
    from tools.code_kernel import _REGISTRY, _resolve_owner
    from tools import code_execution_tool
    module = _module()
    args = dict(code='print(previous_variable)', task_id='state-task', enabled_tools=None, reset=False)
    key = (_resolve_owner('state-task'), 'test-dead')
    with _REGISTRY.lock:
        _REGISTRY.kernels[key] = SimpleNamespace(execution_count=3, alive=lambda: False)
    try:
        with module.durable_execution_context('execute_code.execute', args,
                                              execution_state=module.capture_code_execution_state(args)):
            pending = module.maybe_defer_execution('print(previous_variable)', 'code script')
        calls = []
        monkeypatch.setattr(code_execution_tool, 'execute_code', lambda **args: calls.append(args) or '{"status":"completed"}')
        ticket = _approve_and_run(pending['request_id'], 'execute_code.execute')
        assert ticket.status is DurableActionStatus.INVALIDATED
        assert calls == []
    finally:
        with _REGISTRY.lock:
            _REGISTRY.kernels.pop(key)


def test_unanswered_ticket_does_not_authorize_direct_handler_invocation(profile):
    marker = profile / 'not-approved'
    pending = _stage(f'touch {shlex.quote(str(marker))}')
    outcome = _module().execute_durable_execution(_context(pending['request_id']))
    assert outcome.status is DurableActionStatus.INVALIDATED
    assert not marker.exists()
