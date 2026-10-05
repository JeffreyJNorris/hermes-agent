"""The active tool turn and asynchronous notification compete for one outcome."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from gateway import durable_actions
from gateway.durable_actions import DurableActionStore, PreparedDurableAction, encode_durable_action_result
from gateway.durable_requests import (
    DurableRequestOption, DurableRequestSpec, DurableRequestStore,
    DurableRequestTarget, DurableResponderMode,
)


def _create(path, *, owner=None, until=1300):
    store = DurableRequestStore(path, wall_clock=lambda: 1000)
    result = store.create_with_action(
        DurableRequestSpec(kind='tool.durable_approval.terminal.execute', title='Allow command?',
                           body='One reviewed operation.',
                           options=(DurableRequestOption('approve', 'Allow Once'), DurableRequestOption('reject', 'Deny')),
                           ttl_seconds=600),
        action=PreparedDurableAction('terminal.execute', 1, '{}', 'fingerprint', 'one-ticket'),
        target=DurableRequestTarget('discord', 'original-thread'),
        responder_mode=DurableResponderMode.INITIATOR, expected_actor_id='42',
        origin_type='session', origin_id='logical-session', origin_session_id='physical-session',
        inline_wait_owner=durable_actions.INLINE_WAIT_OWNER if owner is None else owner, inline_wait_until=until,
    )
    return store, result.request.request_id


def _complete(path, request_id, status='succeeded'):
    result = encode_durable_action_result({'tool': 'terminal', 'output': 'reviewed output', 'exit_code': 0}) if status == 'succeeded' else None
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE durable_action_tickets SET status=?, outcome_json=?, outcome_reason=?, "
                     "completed_at=1001, notification_status='pending' WHERE request_id=?",
                     (status, result, 'execution_completed' if result else 'rejected', request_id))
    return result


def test_live_inline_owner_reserves_completion_until_consumed(tmp_path):
    path = tmp_path / 'state.db'
    store, request_id = _create(path)
    expected = _complete(path, request_id)
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification() is None
    outcome = store.consume_inline_result(request_id, durable_actions.INLINE_WAIT_OWNER)
    assert outcome == {'status': 'succeeded', 'result_json': expected, 'reason': 'execution_completed'}
    assert store.consume_inline_result(request_id, durable_actions.INLINE_WAIT_OWNER) is None
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification() is None


def test_inline_wait_can_end_before_completion_and_release_future_notification(tmp_path):
    path = tmp_path / 'state.db'
    store, request_id = _create(path)
    assert store.consume_inline_result(request_id, durable_actions.INLINE_WAIT_OWNER) is None
    assert store.release_inline_wait(request_id, durable_actions.INLINE_WAIT_OWNER)
    _complete(path, request_id)
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification().request_id == request_id


def test_foreign_process_lease_does_not_block_restart_notification(tmp_path):
    path = tmp_path / 'state.db'
    _, request_id = _create(path, owner='previous-gateway-process')
    _complete(path, request_id)
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification().request_id == request_id


def test_expired_inline_lease_does_not_block_notification(tmp_path):
    path = tmp_path / 'state.db'
    store, request_id = _create(path, until=1001)
    _complete(path, request_id)
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification().request_id == request_id
    assert store.consume_inline_result(request_id, durable_actions.INLINE_WAIT_OWNER) is None


def test_denial_is_returned_inline_once_without_duplicate_notification(tmp_path):
    path = tmp_path / 'state.db'
    store, request_id = _create(path)
    _complete(path, request_id, 'declined')
    assert store.consume_inline_result(request_id, durable_actions.INLINE_WAIT_OWNER) == {
        'status': 'declined', 'result_json': None, 'reason': 'rejected',
    }
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification() is None


def test_wrong_owner_cannot_consume_or_release_original_turn_lease(tmp_path):
    path = tmp_path / 'state.db'
    store, request_id = _create(path)
    _complete(path, request_id)
    assert store.consume_inline_result(request_id, 'another-turn') is None
    assert not store.release_inline_wait(request_id, 'another-turn')
    assert DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification() is None


def test_waiter_and_notification_claim_have_one_winner(tmp_path):
    path = tmp_path / 'state.db'
    store, request_id = _create(path)
    _complete(path, request_id)
    barrier = Barrier(2)
    def inline():
        barrier.wait()
        return store.consume_inline_result(request_id, durable_actions.INLINE_WAIT_OWNER)
    def notification():
        barrier.wait()
        return DurableActionStore(path, wall_clock=lambda: 1002).claim_next_notification()
    with ThreadPoolExecutor(max_workers=2) as pool:
        a, b = pool.submit(inline), pool.submit(notification)
        assert sum(result is not None for result in (a.result(), b.result())) == 1


@pytest.mark.parametrize('owner, until', [('', 1300), ('owner', True), ('owner', float('inf')), ('owner', 999), ('owner', None)])
def test_invalid_inline_lease_cannot_be_created(tmp_path, owner, until):
    with pytest.raises(ValueError):
        _create(tmp_path / 'state.db', owner=owner, until=until)


def test_existing_ledger_adds_nullable_inline_columns_without_losing_outcomes(tmp_path):
    from hermes_state_common import DURABLE_ACTION_SCHEMA_SQL
    path = tmp_path / 'legacy.db'
    legacy = DURABLE_ACTION_SCHEMA_SQL.replace('    inline_wait_owner TEXT,\n', '').replace('    inline_wait_until REAL,\n', '')
    with sqlite3.connect(path) as conn:
        conn.executescript(legacy)
        conn.execute("INSERT INTO durable_action_tickets "
                     "(request_id,action_type,action_version,payload_json,fingerprint,idempotency_key,status,"
                     "notification_status,outcome_json,created_at,updated_at) "
                     "VALUES ('old','terminal.execute',1,'{}','fingerprint','old','succeeded','pending','{}',1,1)")
    DurableActionStore(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT inline_wait_owner,inline_wait_until,outcome_json FROM durable_action_tickets WHERE request_id='old'").fetchone() == (None, None, '{}')
