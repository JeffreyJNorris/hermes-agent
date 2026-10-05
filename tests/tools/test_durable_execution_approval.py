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
  inline_wait_seconds: 0
  enabled: true
  allowed_actions: [terminal.execute, execute_code.execute, host.sudo]
  host_sudo_allowed_users: ["42"]
''')
    set_session_vars(platform='discord', chat_id='123456789012345678',
                     thread_id='123456789012345678', user_id='42',
                     session_key='agent:main:discord:thread:123456789012345678:42',
                     session_id='physical-session', cron_session='')
    from hermes_state import SessionDB
    import hermes_state
    # The global test sandbox may have patched this constant before import.
    # Keep the real session store beside this profile's durable grant rows so
    # a fresh child process observes the same isolated database.
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', tmp_path / 'state.db')
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


def _approve_and_run(request_id, action_type='terminal.execute', option_token='1'):
    from gateway.durable_action_runner import DurableActionRunner
    from gateway.durable_actions import DurableActionStore, _DURABLE_ACTION_HANDLERS
    module = _module()
    _DURABLE_ACTION_HANDLERS.setdefault((action_type, 1), (module.prepare_durable_execution, module.execute_durable_execution))
    store = get_durable_request_store()
    delivery = store.claim_next_pending(['discord'])
    assert delivery is not None
    assert store.bind_delivery(request_id=request_id, claim_id=delivery.claim_id, platform='discord', message_id='message')
    if not option_token.isdigit():
        option_token = str(next(index for index, option in enumerate(delivery.request.options, 1)
                               if option.id == option_token))
    assert store.resolve(request_id=request_id, option_token=option_token, platform='discord',
                         channel_id='123456789012345678', message_id='message', actor_id='42').status.value == 'resolved'
    runner = DurableActionRunner(DurableActionStore(store.db_path), enabled=True, allowed_actions=[action_type])
    assert asyncio.run(runner.process_once())
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute('SELECT * FROM durable_action_tickets WHERE request_id=?', (request_id,)).fetchone()
    from types import SimpleNamespace
    return SimpleNamespace(status=DurableActionStatus(row['status']), outcome_reason=row['outcome_reason'], outcome_json=row['outcome_json'])


def _stage_guarded(command, *, warnings=None, permanent_capable=True, smart_denied=False):
    from tools import approval
    module = _module()
    from tools.terminal_tool import _get_env_config
    arguments = dict(command=command, background=False, timeout=_get_env_config()['timeout'],
                     task_id='test-task', session_id=None, workdir='/tmp', pty=False,
                     notify_on_complete=False, watch_patterns=None, heartbeat=0,
                     persist_on_release=False)
    # A real guard is reached after backend initialization has resolved its cwd.
    from tools.terminal_tool import terminal_tool
    assert json.loads(terminal_tool(command='true', task_id='test-task', workdir='/tmp'))['exit_code'] == 0
    warnings = warnings or [('scope-test-pattern', 'test guard', False)]
    with module.durable_execution_context('terminal.execute', arguments):
        return approval._human_decision(
            approval._COMMAND_GATE, command=command, description='test guard',
            pattern_key=warnings[0][0], pattern_keys=[warning[0] for warning in warnings],
            warnings=warnings, session_key=module._session_values()['session_key'],
            approval_callback=None, is_cli=False, is_gateway=True, is_ask=False,
            permanent_capable=permanent_capable, smart=smart_denied,
        )


def test_native_gate_restores_scoped_durable_choices(profile):
    pending = _stage_guarded('printf reviewed')
    request = get_durable_request_store().get(request_id=pending['request_id'])
    assert [option.id for option in request.options] == ['approve', 'session', 'always', 'reject']
    payload = json.loads(_context(pending['request_id']).payload_json)
    assert payload['approval_scope']['warnings'] == [['scope-test-pattern', 'test guard', False]]


@pytest.mark.parametrize('choice', ['session', 'always'])
def test_scoped_choice_executes_then_survives_memory_restart(profile, choice):
    from tools import approval
    marker = profile / 'scoped-marker'
    pending = _stage_guarded(f'printf reviewed > {shlex.quote(str(marker))}')
    assert _approve_and_run(pending['request_id'], option_token=choice).status is DurableActionStatus.SUCCEEDED
    assert marker.read_text() == 'reviewed'
    approval._session_approved.clear()
    approval._permanent_approved_by_home.clear()
    assert approval.is_approved(_module()._session_values()['session_key'], 'scope-test-pattern')
    if choice == 'always':
        assert 'scope-test-pattern' in approval._read_permanent_allowlist()


def test_session_scope_does_not_follow_new_conversation(profile):
    from tools import approval
    pending = _stage_guarded('printf reviewed')
    assert _approve_and_run(pending['request_id'], option_token='session').status is DurableActionStatus.SUCCEEDED
    assert 'scope-test-pattern' not in approval._session_approved.get(_module()._session_values()['session_key'], set())
    from hermes_state import SessionDB
    db = SessionDB()
    db.end_session('physical-session', 'new')
    db.create_session('replacement-session', 'discord', session_key=_module()._session_values()['session_key'])
    db.close()
    set_session_vars(**{**_module()._session_values(), 'session_id': 'replacement-session'})
    assert not approval.is_approved(_module()._session_values()['session_key'], 'scope-test-pattern')


def test_session_scope_survives_an_actual_python_process_restart(profile):
    import os
    import subprocess
    import sys
    pending = _stage_guarded('printf reviewed')
    assert _approve_and_run(pending['request_id'], option_token='session').status is DurableActionStatus.SUCCEEDED
    identity = _module()._session_values()
    script = (
        'from gateway.session_context import set_session_vars; '
        f'set_session_vars(**{identity!r}); '
        'from tools.approval import is_approved; '
        f'print(is_approved({identity["session_key"]!r}, "scope-test-pattern"))'
    )
    process = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                             env={**os.environ, 'HERMES_HOME': str(profile)}, timeout=30)
    assert process.returncode == 0, process.stderr
    assert process.stdout.strip() == 'True'


def test_conversation_reset_during_execution_never_grants_scope(profile, monkeypatch):
    from tools import approval, terminal_tool
    pending = _stage_guarded('printf reviewed')
    def close_origin(**kwargs):
        from hermes_state import SessionDB
        db = SessionDB()
        db.end_session('physical-session', 'new')
        db.close()
        approval.clear_session(_module()._session_values()['session_key'])
        return json.dumps({'output': 'reviewed', 'exit_code': 0})
    monkeypatch.setattr(terminal_tool, 'terminal_tool', close_origin)
    assert _approve_and_run(pending['request_id'], option_token='always').status is DurableActionStatus.SUCCEEDED
    assert 'scope-test-pattern' not in approval._read_permanent_allowlist()
    assert not approval.is_approved(_module()._session_values()['session_key'], 'scope-test-pattern')


def test_tirith_only_scope_does_not_offer_permanent_choice(profile):
    pending = _stage_guarded('printf reviewed', warnings=[('tirith:test', 'content guard', True)], permanent_capable=False)
    request = get_durable_request_store().get(request_id=pending['request_id'])
    assert [option.id for option in request.options] == ['approve', 'session', 'reject']


def test_changed_environment_never_persists_selected_scope(profile):
    from tools import approval
    pending = _stage_guarded('printf reviewed')
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('backend: local', 'backend: ssh'))
    assert _approve_and_run(pending['request_id'], option_token='always').status is DurableActionStatus.INVALIDATED
    assert not approval.is_approved(_module()._session_values()['session_key'], 'scope-test-pattern')
    assert 'scope-test-pattern' not in approval._read_permanent_allowlist()


def test_smart_deny_override_keeps_once_only_choices(profile, monkeypatch):
    from tools import approval
    monkeypatch.setattr(approval, '_smart_gate', lambda *args, **kwargs: (None, True))
    pending = _stage_guarded('printf reviewed', smart_denied=True)
    request = get_durable_request_store().get(request_id=pending['request_id'])
    assert [option.id for option in request.options] == ['approve', 'reject']


def test_mixed_always_keeps_tirith_findings_session_only(profile):
    from tools import approval
    pending = _stage_guarded('printf reviewed', warnings=[('scope-test-pattern', 'pattern', False),
                                                        ('tirith:test', 'content guard', True)])
    assert _approve_and_run(pending['request_id'], option_token='always').status is DurableActionStatus.SUCCEEDED
    permanent = approval._read_permanent_allowlist()
    assert 'scope-test-pattern' in permanent
    assert 'tirith:test' not in permanent
    approval._session_approved.clear()
    assert approval.is_approved(_module()._session_values()['session_key'], 'tirith:test')


def test_session_grant_is_actor_bound_and_clear_revokes_it(profile):
    from tools import approval
    pending = _stage_guarded('printf reviewed')
    assert _approve_and_run(pending['request_id'], option_token='session').status is DurableActionStatus.SUCCEEDED
    key = _module()._session_values()['session_key']
    identity = _module()._session_values()
    approval._session_approved.clear()
    set_session_vars(**{**identity, 'user_id': 'other-person'})
    assert not approval.is_approved(key, 'scope-test-pattern')
    set_session_vars(**identity)
    assert approval.is_approved(key, 'scope-test-pattern')
    approval.clear_session(key)
    assert not approval.is_approved(key, 'scope-test-pattern')


def test_actual_terminal_session_grant_allows_same_native_pattern(profile, monkeypatch):
    from tools import approval, terminal_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    marker = profile / 'remove-on-scope'
    marker.write_text('keep')
    command = f'rm -rf -- {shlex.quote(str(marker))}'
    pending = json.loads(terminal_tool.terminal_tool(command, task_id='test-task'))
    assert pending['status'] == 'approval_pending'
    assert _approve_and_run(pending['request_id'], option_token='session').status is DurableActionStatus.SUCCEEDED
    approval._session_approved.clear()
    marker.write_text('keep')
    result = json.loads(terminal_tool.terminal_tool(command, task_id='test-task'))
    assert result.get('status') != 'approval_pending'
    assert result['exit_code'] == 0
    assert not marker.exists()


def test_actual_code_session_grant_allows_next_cell(profile, monkeypatch):
    from tools import approval, code_execution_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    marker = profile / 'code-scope'
    code = f'from pathlib import Path\nPath({str(marker)!r}).write_text("first")'
    pending = json.loads(code_execution_tool.execute_code(code, task_id='scoped-code', reset=True))
    assert pending['status'] == 'approval_pending'
    assert _approve_and_run(pending['request_id'], 'execute_code.execute', option_token='session').status is DurableActionStatus.SUCCEEDED
    approval._session_approved.clear()
    result = json.loads(code_execution_tool.execute_code(
        f'from pathlib import Path\nPath({str(marker)!r}).write_text("second")', task_id='scoped-code', reset=True,
    ))
    assert result.get('status') != 'approval_pending'
    assert marker.read_text() == 'second'


def test_code_failure_preserves_explicit_always_allow_policy(profile, monkeypatch):
    from tools import approval, code_execution_tool
    monkeypatch.setattr(approval, '_is_gateway_approval_context', lambda: True)
    pending = json.loads(code_execution_tool.execute_code(
        'raise RuntimeError("reviewed script failed")', task_id='failed-scoped-code', reset=True))
    assert pending['status'] == 'approval_pending'
    ticket = _approve_and_run(pending['request_id'], 'execute_code.execute', option_token='always')
    assert ticket.status is DurableActionStatus.FAILED
    # The outcome of one script does not revoke the user's policy choice.
    assert 'execute_code' in approval._read_permanent_allowlist()


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


def test_fresh_code_approval_invalidates_if_working_directory_changes(profile, monkeypatch):
    from tools import code_execution_tool
    module = _module()
    current = {'cwd': '/tmp/original-reviewed-directory'}
    monkeypatch.setattr(code_execution_tool, '_resolve_child_cwd', lambda *a, **k: current['cwd'])
    args = dict(code='print("reviewed")', task_id='fresh-code', enabled_tools=None, reset=True)
    state = module.capture_code_execution_state(args)
    with module.durable_execution_context('execute_code.execute', args, execution_state=state):
        result = module.maybe_defer_execution(args['code'], 'code script')
    current['cwd'] = '/tmp/different-directory'
    calls = []
    monkeypatch.setattr(code_execution_tool, 'execute_code', lambda **kwargs:
                        calls.append(kwargs) or json.dumps({'status': 'completed', 'output': 'reviewed'}))
    ticket = _approve_and_run(result['request_id'], 'execute_code.execute')
    assert ticket.status is DurableActionStatus.INVALIDATED
    assert calls == []


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


def _inline_operator(profile, *, reject=False):
    """Use real persistence, actor validation and action dispatch in another worker."""
    import contextvars
    import threading
    import time
    from gateway.durable_actions import DurableActionStore
    from gateway.durable_action_runner import DurableActionRunner
    errors = []
    def operate():
        try:
            store = get_durable_request_store()
            limit = time.monotonic() + 5
            while time.monotonic() < limit:
                delivery = store.claim_next_pending(['discord'])
                if delivery:
                    break
                time.sleep(.01)
            else:
                raise AssertionError('Approval was not staged')
            request = delivery.request
            store.bind_delivery(request_id=request.request_id, claim_id=delivery.claim_id,
                                platform='discord', message_id='inline-review')
            result = store.resolve(request_id=request.request_id, option_token='2' if reject else '1',
                                   platform='discord', channel_id=request.target.delivery_channel_id,
                                   message_id='inline-review', actor_id='42')
            assert result.status.value == 'resolved'
            runner = DurableActionRunner(DurableActionStore(store.db_path), enabled=True,
                                         allowed_actions=['terminal.execute', 'execute_code.execute'])
            asyncio.run(runner.process_once())
        except BaseException as exc:
            errors.append(exc)
    context = contextvars.copy_context()
    worker = threading.Thread(target=lambda: context.run(operate))
    worker.start()
    return worker, errors


def test_inline_approval_returns_native_result_once_without_a_continuation(profile):
    from gateway.durable_actions import DurableActionStore
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('inline_wait_seconds: 0', 'inline_wait_seconds: 2'))
    target = profile / 'once.txt'
    worker, errors = _inline_operator(profile)
    try:
        result = _stage(f"printf 'once' >> {shlex.quote(str(target))}; printf 'native result'", timeout=5)
    finally:
        worker.join(10)
    assert not worker.is_alive()
    assert not errors, errors
    assert result['status'] == 'execution_completed', result
    assert result['tool_result']['exit_code'] == 0
    assert result['tool_result']['output'] == 'native result'
    assert target.read_text() == 'once'
    assert DurableActionStore(get_durable_request_store().db_path).claim_next_notification() is None


def test_inline_rejection_returns_without_execution_or_extra_turn(profile):
    from gateway.durable_actions import DurableActionStore
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('inline_wait_seconds: 0', 'inline_wait_seconds: 2'))
    worker, errors = _inline_operator(profile, reject=True)
    try:
        result = _stage('printf should-not-run')
    finally:
        worker.join(10)
    assert not worker.is_alive()
    assert not errors, errors
    assert result['status'] == 'execution_completed', result
    assert result['tool_result']['status'] == 'blocked'
    assert DurableActionStore(get_durable_request_store().db_path).claim_next_notification() is None


def test_inline_wait_expires_into_same_durable_request(profile):
    from gateway.durable_actions import DurableActionStore
    path = profile / 'config.yaml'
    path.write_text(path.read_text().replace('inline_wait_seconds: 0', 'inline_wait_seconds: .03'))
    result = _stage('printf later')
    assert result['status'] == 'approval_pending'
    store = get_durable_request_store()
    request = store.get(request_id=result['request_id'])
    assert request.status.value == 'pending_delivery'
    with sqlite3.connect(store.db_path) as conn:
        owner, until = conn.execute('SELECT inline_wait_owner, inline_wait_until FROM durable_action_tickets WHERE request_id=?',
                                    (request.request_id,)).fetchone()
    assert owner is None and until is None


@pytest.mark.parametrize('code', [
    "import subprocess; subprocess.run(['ssh','hermes-host','sudo -n /usr/bin/id -u'])",
    "import os; os.system(\"ssh hermes-host 'sudo -n /usr/bin/id -u'\")",
    "from subprocess import check_output; check_output('sudo -n /usr/bin/id -u',shell=True)",
])
def test_visible_code_host_elevation_is_recognized_before_saved_grants(code):
    assert _module().code_requests_host_sudo(code, 'local')


def test_terminal_rpc_code_requests_operator_for_its_reviewed_script():
    assert _module().code_requests_host_sudo('terminal("ssh hermes-host sudo id")', 'docker')
    assert not _module().code_requests_host_sudo('print("sudo id")', 'local')


def test_ssh_sudo_review_is_operator_only_even_when_scopes_requested(profile):
    bridge = _module()
    command = "ssh hermes-host 'sudo -n /usr/bin/id -u'"
    with bridge.durable_execution_context('terminal.execute', {'command': command}, privileged_host=True):
        result = bridge.maybe_defer_execution(command, 'host sudo', warnings=[('script', 'script', False)],
                                             allow_session=True, allow_permanent=True, required=True)
    request = get_durable_request_store().get(request_id=result['request_id'])
    assert request.kind == 'tool.durable_approval.host.terminal.execute'
    assert [option.id for option in request.options] == ['approve', 'reject']
    payload = json.loads(_context(result['request_id']).payload_json)
    assert payload['privileged_host'] is True


def test_host_sudo_request_from_non_operator_is_blocked(profile):
    bridge = _module()
    set_session_vars(user_id='99')
    with bridge.durable_execution_context('terminal.execute', {'command': 'sudo id'}, privileged_host=True):
        result = bridge.maybe_defer_execution('sudo id', 'host sudo', required=True)
    assert result['status'] == 'blocked'
    assert 'request_id' not in result


def test_direct_code_sudo_reaches_operator_before_off_mode(profile, monkeypatch):
    from tools import code_execution_tool, approval
    code = "import subprocess; subprocess.run(['ssh', 'hermes-host', 'sudo -n /usr/bin/id -u'])"
    monkeypatch.setattr(approval, 'check_execute_code_guard', lambda *args, **kwargs:
                        pytest.fail('ordinary code approval bypassed host operator gate'))
    result = json.loads(code_execution_tool.execute_code(code, task_id='direct-privileged-code'))
    assert result['status'] == 'approval_pending', result
    request = get_durable_request_store().get(request_id=result['request_id'])
    assert request.kind == 'tool.durable_approval.host.execute_code.execute'
    assert [option.id for option in request.options] == ['approve', 'reject']



def test_review_redaction_keeps_multiline_secret_boundaries():
    secret = 'sk-' + 'a' * 40
    result = _module().redact_execution_value({'content': 'first line\n' + secret, 'output': ['next\n' + secret]})
    assert secret not in json.dumps(result)
    assert result['content'].startswith('first line\n')
