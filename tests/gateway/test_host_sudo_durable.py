"""Real request/action persistence; only the privileged subprocess is replaced."""
import subprocess

import pytest

from gateway.durable_action_runner import DurableActionRunner
from gateway.durable_actions import DurableActionStore, DurableActionExecutionContext, DurableActionStatus
from gateway.durable_requests import (
    DurableRequestOption, DurableRequestSpec, DurableRequestStore,
    DurableRequestTarget, DurableResponderMode, reset_durable_request_store_cache,
)
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from hermes_state import SessionDB
from tools.host_sudo import prepare_host_sudo, execute_host_sudo


@pytest.mark.asyncio
async def test_owner_decision_survives_reopen_and_executes_once(tmp_path, monkeypatch):
    override = set_hermes_home_override(tmp_path)
    reset_durable_request_store_cache()
    calls = []
    monkeypatch.setattr(subprocess, 'run', lambda argv, **kwargs:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, '0\n', ''))
    try:
        (tmp_path / 'config.yaml').write_text('''durable_approvals:
  enabled: true
  allowed_actions: [host.sudo]
  host_sudo_allowed_users: ["42"]
''')
        db = SessionDB()
        db.create_session('session-1', 'discord')
        db.close()
        store = DurableRequestStore(tmp_path / 'state.db')
        prepared = prepare_host_sudo(['/usr/bin/id', '-u'])
        created = store.create_with_action(
            DurableRequestSpec(kind='tool.durable_approval.host.sudo', title='Host sudo?',
                body='sudo -- /usr/bin/id -u', ttl_seconds=3600,
                options=(DurableRequestOption('approve', 'Approve'), DurableRequestOption('reject', 'Reject'))),
            action=prepared,
            target=DurableRequestTarget('discord', '123456789012345678'),
            responder_mode=DurableResponderMode.INITIATOR, expected_actor_id='42',
            origin_type='session', origin_id='discord:parent', origin_session_id='session-1',
        )
        context = DurableActionExecutionContext(created.request.request_id, prepared.action_type,
            prepared.action_version, prepared.payload_json, prepared.fingerprint, prepared.idempotency_key)
        assert execute_host_sudo(context).status is DurableActionStatus.INVALIDATED
        assert calls == []
        runner = DurableActionRunner(DurableActionStore(tmp_path / 'state.db'),
                                     enabled=True, allowed_actions=['host.sudo'])
        assert not await runner.process_once()
        assert calls == []
        claim = store.claim_next_pending(['discord'])
        store.bind_delivery(request_id=created.request.request_id, claim_id=claim.claim_id,
                            platform='discord', message_id='message-1')
        store = DurableRequestStore(tmp_path / 'state.db')
        store.resolve(request_id=created.request.request_id, option_token='1',
                      platform='discord', channel_id='123456789012345678', message_id='message-1', actor_id='99')
        assert not await runner.process_once()
        assert calls == []
        store.resolve(request_id=created.request.request_id, option_token='1',
                      platform='discord', channel_id='123456789012345678', message_id='message-1', actor_id='42')
        assert execute_host_sudo(context).status is DurableActionStatus.INVALIDATED
        assert calls == []
        assert await runner.process_once()
        assert calls == [['/usr/bin/sudo', '-n', '--', '/usr/bin/id', '-u']]
        assert not await runner.process_once()
        assert len(calls) == 1
    finally:
        reset_durable_request_store_cache()
        reset_hermes_home_override(override)
