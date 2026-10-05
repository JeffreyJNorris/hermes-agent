"""Explicit host elevation, executed only by the approved durable action runner.

``hermes host sudo -- /absolute/program args`` is a request marker handled by
the existing terminal tool, not a Docker command or a Unix privilege grant.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import uuid


_SHELLS = frozenset({'sh', 'bash', 'dash', 'zsh', 'ksh', 'ash', 'fish'})
_SSH_VALUE_OPTIONS = frozenset({'-F', '-i', '-o', '-l', '-p', '-J', '-L', '-R', '-D', '-S',
                                '-W', '-E', '-b', '-c', '-m', '-O', '-Q', '-w'})


def requires_host_sudo_approval(command: str, env_type: str) -> bool:
    """Classify visible sudo execution, including SSH and explicit shell wrappers.

    This is a command-path guard, not an interpreter sandbox. Code which calls
    subprocess directly must still be constrained at the Unix privilege boundary.
    """
    return _shell_uses_host_sudo(command, env_type in {'local', 'ssh'}, depth=0)


def _shell_word(word):
    decoded = shlex.split(word)
    return decoded[0] if len(decoded) == 1 else word


def _shell_uses_host_sudo(command, host_context, *, depth):
    if depth > 12:
        return bool(re.search(r'\bsudo\b', command))
    try:
        # Preserve quoted punctuation: `printf '%s' '|' sudo` is data, not a pipe.
        lexer = shlex.shlex(command, posix=False, punctuation_chars=';&|<>()\n')
        lexer.whitespace = ' \t\r'
        lexer.whitespace_split = True
        words = list(lexer)
        parts, current, operators = [], [], []
        for word in words:
            if word and all(char in ';&|<>()\n' for char in word):
                operators.append(word)
                if current:
                    parts.append(current)
                    current = []
            else:
                current.append(word)
                # Double-quoted/unquoted substitutions execute in the current
                # shell. Literal single-quoted examples remain ordinary data.
                if not word.startswith("'"):
                    for match in re.finditer(r'\$\((.*?)\)|`([^`]*)`', word, re.DOTALL):
                        if _shell_uses_host_sudo(match.group(1) or match.group(2) or '', host_context, depth=depth + 1):
                            return True
        if current:
            parts.append(current)
        if any(_argv_uses_host_sudo(part, host_context, depth=depth) for part in parts):
            return True
        # SSH can run a remote login shell from stdin instead of a command argv.
        # A visible sudo script piped/heredoc'd to it still needs an operator.
        if re.search(r'\bsudo\b', command) and any(operator == '|' or operator.startswith('<') for operator in operators):
            for part in parts:
                decoded = [_shell_word(word) for word in part]
                if decoded and Path(decoded[0]).name == 'ssh':
                    remote = _ssh_remote_command(decoded)
                    shell_stdin = (remote and Path(remote[0]).name in _SHELLS
                                   and not any(option.startswith('-') and not option.startswith('--')
                                               and 'c' in option[1:] for option in remote[1:]))
                    if remote is not None and (not remote or shell_stdin):
                        return True
        return False
    except ValueError:
        # An ambiguous visible sudo request cannot fall back to a broad grant.
        return bool(re.search(r'\bsudo\b', command)) and (host_context or bool(re.search(r'\bssh\b', command)))


def _argv_uses_host_sudo(raw_words, host_context, *, depth):
    words = [_shell_word(word) for word in raw_words]
    while words and (re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*=.*', words[0], re.DOTALL)
                     or words[0] in {'if', 'then', 'else', 'elif', 'do', 'while', 'until', '!'}):
        words.pop(0)
    if not words:
        return False
    program = Path(words[0]).name
    if program == 'sudo':
        if host_context:
            return True
        # Container elevation is ordinary, but it may wrap a host SSH operation.
        words = words[1:]
        while words and words[0].startswith('-'):
            option = words.pop(0)
            if option in {'-u', '-g', '-h', '-p', '-C', '-T', '-R', '-D'} and words:
                words.pop(0)
        return _argv_uses_host_sudo(words, False, depth=depth + 1)
    if program == 'ssh':
        # The first positional argument is the remote host. OpenSSH joins the
        # remaining argv into a remote command interpreted by its login shell.
        remote = _ssh_remote_command(words)
        return bool(remote) and _shell_uses_host_sudo(' '.join(remote), True, depth=depth + 1)
    if program in _SHELLS:
        for index, option in enumerate(words[1:], 1):
            if option.startswith('-') and not option.startswith('--') and 'c' in option[1:]:
                index += 1
                if index < len(words) and words[index] == '--':
                    index += 1
                return index < len(words) and _shell_uses_host_sudo(words[index], host_context, depth=depth + 1)
        return False
    if program in {'env', 'command', 'exec', 'nohup', 'timeout'}:
        if program == 'command' and any(option in {'-v', '-V'} for option in words[1:]):
            return False
        index = 1
        while index < len(words) and (words[index].startswith('-') or '=' in words[index]):
            option = words[index]
            index += 1
            if program == 'env' and option in {'-S', '--split-string'} and index < len(words):
                return _shell_uses_host_sudo(words[index], host_context, depth=depth + 1)
            if option in {'-u', '--unset', '-C', '--chdir', '-a', '-k', '--kill-after', '-s', '--signal'}:
                index += 1
        if program == 'timeout':
            index += 1
        return _argv_uses_host_sudo(words[index:], host_context, depth=depth + 1)
    return False


def _ssh_remote_command(words):
    index = 1
    while index < len(words) and words[index].startswith('-'):
        option = words[index]
        index += 1
        if option == '--':
            break
        if option in _SSH_VALUE_OPTIONS:
            index += 1
    return words[index + 1:] if index < len(words) else None


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


def host_sudo_actor_allowed(actor_id: str | None) -> bool:
    """Elevated host requests require an explicit install-owner allowlist."""
    from hermes_cli.config import load_config_readonly

    try:
        policy = load_config_readonly().get('durable_approvals', {})
        if not isinstance(policy, dict) or policy.get('enabled') is not True:
            return False
        actions = policy.get('allowed_actions', [])
        owners = policy.get('host_sudo_allowed_users', [])
        return (type(actions) is list and 'host.sudo' in actions
                and type(owners) is list and all(type(owner) is str and owner.strip() for owner in owners)
                and bool(actor_id) and actor_id in owners)
    except (OSError, ValueError, TypeError):
        return False


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
    from tools.durable_execution_approval import approved_action_is_executing

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
    if not approved_action_is_executing(context):
        return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='action_not_authorized')
    # Approval belongs to the original conversation. A later /new or deleted
    # source session must not leave a root action runnable in a different turn.
    try:
        request = get_durable_request(request_id=context.request_id)
        if request is None or request.origin_type != 'session' or not request.origin_session_id:
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='origin_unavailable')
        if (request.target.platform != 'discord'
                or not host_sudo_actor_allowed(request.expected_actor_id)):
            return DurableActionOutcome(DurableActionStatus.INVALIDATED, reason='host_actor_not_allowed')
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
