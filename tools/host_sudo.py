"""Explicit host elevation, executed only by the approved durable action runner.

``hermes host sudo -- /absolute/program args`` is a request marker handled by
the existing terminal tool, not a Docker command or a Unix privilege grant.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
import uuid


def _validate_argv(argv: list[str]) -> list[str]:
    if (type(argv) is not list or not argv or len(argv) > 128
            or any(type(arg) is not str or '\x00' in arg for arg in argv)
            or not Path(argv[0]).is_absolute()
            or sum(len(arg) for arg in argv) > 16384):
        raise ValueError('host sudo needs a bounded argv starting with an absolute executable')
    return list(argv)


def parse_host_sudo_command(command: str) -> list[str] | None:
    """Recognize a single request, without evaluating shell expansions."""
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=';&|<>()')
        lexer.whitespace_split = True
        lexer.commenters = ''
        words = list(lexer)
    except ValueError:
        if command.strip().startswith('hermes host sudo'):
            raise
        return None
    if words[:3] != ['hermes', 'host', 'sudo']:
        return None
    if len(words) < 5 or words[3] != '--':
        raise ValueError('use hermes host sudo -- /absolute/program args')
    if any(word and all(char in ';&|<>()' for char in word) for word in words[4:]):
        raise ValueError('request one exact argv; use an explicit shell executable for shell commands')
    return _validate_argv(words[4:])


def prepare_host_sudo(argv: list[str], timeout: float = 180):
    from gateway.durable_actions import PreparedDurableAction, encode_durable_action_payload
    from tools.approval import _floor_block

    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 180:
        raise ValueError('host sudo timeout must be between 0 and 180 seconds')
    argv = _validate_argv(argv)
    if _floor_block(shlex.join(argv), sudo_guard=True) is not None:
        raise ValueError('host command is blocked by the unconditional command policy')
    payload = encode_durable_action_payload({'argv': argv, 'timeout': timeout})
    fingerprint = hashlib.sha256(payload.encode()).hexdigest()
    return PreparedDurableAction('host.sudo', 1, payload, fingerprint,
                                 f'host.sudo:{uuid.uuid4().hex}')


def execute_host_sudo(context):
    """Run the immutable reviewed argv once on the gateway host, without a shell."""
    from agent.redact import redact_sensitive_text
    from gateway.durable_actions import (
        DurableActionOutcome, DurableActionStatus, decode_durable_action_payload,
        encode_durable_action_result,
    )
    from gateway.durable_requests import get_durable_request
    from hermes_state import SessionDB
    from tools.approval import _floor_block

    invalid = DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='invalid_payload')
    try:
        payload = decode_durable_action_payload(context.payload_json)
        if (context.action_type != 'host.sudo' or context.action_version != 1
                or type(payload) is not dict or set(payload) != {'argv', 'timeout'}
                or hashlib.sha256(context.payload_json.encode()).hexdigest() != context.fingerprint):
            return invalid
        argv = _validate_argv(payload['argv'])
        timeout = payload['timeout']
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 180:
            return invalid
    except (TypeError, ValueError):
        return invalid
    if _floor_block(shlex.join(argv), sudo_guard=True) is not None:
        return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='command_blocked')
    # Approval belongs to the original conversation. A later /new or deleted
    # source session must not leave a root action runnable in a different turn.
    try:
        request = get_durable_request(request_id=context.request_id)
        if request is None or request.origin_type != 'session' or not request.origin_session_id:
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='origin_unavailable')
        db = SessionDB()
        try:
            session = db.get_session(request.origin_session_id)
        finally:
            db.close()
        if session is None or session.get('ended_at') is not None:
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='origin_unavailable')
    except (OSError, ValueError, sqlite3.Error):
        return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='origin_unavailable')
    try:
        result = subprocess.run(['/usr/bin/sudo', '-n', '--', *argv],
                                stdin=subprocess.DEVNULL, capture_output=True,
                                text=True, errors='replace', shell=False,
                                timeout=timeout, cwd=str(Path.home()))
    except subprocess.TimeoutExpired:
        return DurableActionOutcome(DurableActionStatus.UNKNOWN, reason='command_timeout')
    except OSError:
        return DurableActionOutcome(DurableActionStatus.FAILED, reason='command_unavailable')
    output = redact_sensitive_text(result.stdout + result.stderr, force=True)
    output = output.encode('utf-8')[-12000:].decode('utf-8', errors='ignore')
    while len(json.dumps(output, ensure_ascii=False).encode('utf-8')) > 15000:
        output = output[len(output) // 2:]
    return DurableActionOutcome(
        DurableActionStatus.SUCCEEDED if result.returncode == 0 else DurableActionStatus.FAILED,
        reason='command_completed' if result.returncode == 0 else 'command_failed',
        result_json=encode_durable_action_result({'exit_code': result.returncode, 'output': output}),
    )
