"""Only ordinary tool execution can turn a scoped human choice into work."""
import sqlite3

import pytest

from gateway.durable_actions import PreparedDurableAction
from gateway.durable_requests import (
    DurableRequestError, DurableRequestOption, DurableRequestSpec,
    DurableRequestStore, DurableRequestTarget, DurableResponderMode,
)


def create(store, action_type, options, kind=None):
    return store.create_with_action(
        DurableRequestSpec(
            kind=kind or 'tool.durable_approval.' + action_type,
            title='Review operation', body='Reviewed operation', ttl_seconds=60,
            options=tuple(DurableRequestOption(value, value) for value in options)),
        action=PreparedDurableAction(action_type, 1, '{}', 'test', action_type),
        target=DurableRequestTarget('discord', 'parent-thread'),
        responder_mode=DurableResponderMode.INITIATOR, expected_actor_id='42',
        origin_type='session', origin_id='parent', origin_session_id='parent-session',
    )


@pytest.mark.parametrize('action_type', ['terminal.execute', 'execute_code.execute'])
@pytest.mark.parametrize('choice', ['session', 'always'])
def test_scoped_choice_only_readies_original_tool_after_actor_approval(tmp_path, action_type, choice):
    db = tmp_path / 'state.db'
    store = DurableRequestStore(db)
    created = create(store, action_type, ('approve', 'session', 'always', 'reject'))
    claim = store.claim_next_pending(['discord'])
    store.bind_delivery(request_id=created.request.request_id, claim_id=claim.claim_id,
                        platform='discord', message_id='message')
    store = DurableRequestStore(db)  # Choices and actor constraints survive a reopen.
    for actor, expected in [('someone-else', 'waiting_for_decision'), ('42', 'ready')]:
        store.resolve(request_id=created.request.request_id, option_token='2' if choice == 'session' else '3',
                      platform='discord', channel_id='parent-thread', message_id='message', actor_id=actor)
        with sqlite3.connect(db) as conn:
            assert conn.execute('SELECT status FROM durable_action_tickets').fetchone()[0] == expected


@pytest.mark.parametrize('action_type,kind', [
    ('host.sudo', None), ('cron.resume', None), ('terminal.execute', 'unrelated.approval'),
])
def test_non_tool_action_cannot_offer_scoped_approval(tmp_path, action_type, kind):
    store = DurableRequestStore(tmp_path / 'state.db')
    with pytest.raises(DurableRequestError, match='approve.*reject'):
        create(store, action_type, ('approve', 'session', 'always', 'reject'), kind=kind)


def test_tampered_host_choice_never_readies_sudo_ticket(tmp_path):
    db = tmp_path / 'state.db'
    store = DurableRequestStore(db)
    created = create(store, 'host.sudo', ('approve', 'reject'))
    # Exercise the ticket guard independently of UI option validation.
    from gateway.durable_requests import DurableRequestStatus
    with sqlite3.connect(db) as conn:
        store._transition_action_for_terminal_request_locked(
            conn, request_id=created.request.request_id,
            request_status=DurableRequestStatus.SELECTED, now=1000,
            selected_option_id='always')
        assert conn.execute('SELECT status FROM durable_action_tickets').fetchone()[0] == 'abandoned'
