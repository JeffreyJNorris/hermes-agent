"""Exact target baselines for deferred native file writes and patches."""

import functools
import inspect
import json
import os
import stat


def _paths(action_type, arguments):
    from tools.file_tools import _collect_v4a_header_paths

    paths = [arguments["path"]] if arguments.get("path") else []
    if action_type == "file.patch" and arguments.get("mode") == "patch":
        collected = _collect_v4a_header_paths(arguments.get("patch") or "")
        if isinstance(collected, str):
            raise ValueError("Invalid patch paths")
        paths.extend(collected[0])
    return list(dict.fromkeys(paths))


def _host_snapshot(entry, resolved):
    from tools.file_tools_read_tracking import _file_version

    try:
        entry_stat = os.lstat(entry)
        entry_identity = [entry_stat.st_dev, entry_stat.st_ino, entry_stat.st_mode,
                          entry_stat.st_mtime_ns, entry_stat.st_ctime_ns]
    except FileNotFoundError:
        entry_identity = None
    try:
        target_stat = os.stat(resolved)
    except FileNotFoundError:
        return {"entry_identity": entry_identity, "exists": False, "version": None}
    if not stat.S_ISREG(target_stat.st_mode):
        raise ValueError("Deferred file targets must be regular files")
    version = _file_version(resolved)
    if version is None:
        raise ValueError("Cannot capture a stable file version")
    return {"entry_identity": entry_identity, "exists": True,
            "version": [*version[:-1], version[-1].hex()]}


def _remote_snapshots(file_ops, entries):
    # Reuse the shell file layer's interpreter transport. The probe checks the
    # backend namespace, never a same-named path on the gateway host.
    snippet = (
        "import os, stat, hashlib, json\n"
        f"entries = {entries!r}\n"
        "result = []\n"
        "fields = ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')\n"
        "for raw, entry in entries:\n"
        "    resolved = os.path.realpath(entry)\n"
        "    try:\n"
        "        e = os.lstat(entry)\n"
        "        identity = [e.st_dev,e.st_ino,e.st_mode,e.st_mtime_ns,e.st_ctime_ns]\n"
        "    except FileNotFoundError:\n"
        "        identity = None\n"
        "    exists = os.path.exists(resolved)\n"
        "    version = None\n"
        "    if exists:\n"
        "        before = os.stat(resolved)\n"
        "        if not stat.S_ISREG(before.st_mode): raise ValueError('not a regular file')\n"
        "        fd = os.open(resolved, os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0))\n"
        "        with os.fdopen(fd, 'rb') as stream:\n"
        "            opened = os.fstat(stream.fileno())\n"
        "            if not stat.S_ISREG(opened.st_mode): raise ValueError('not a regular file')\n"
        "            digest = hashlib.sha256()\n"
        "            for chunk in iter(lambda: stream.read(131072), b''): digest.update(chunk)\n"
        "        after = os.stat(resolved)\n"
        "        version = [getattr(opened, name) for name in fields]\n"
        "        if version != [getattr(after, name) for name in fields]: raise ValueError('file changed')\n"
        "        version.append(digest.hexdigest())\n"
        "    result.append({'path':raw,'entry':entry,'resolved':resolved,'exists':exists,'entry_identity':identity,'version':version})\n"
        "print(json.dumps(result))\n"
    )
    probe = file_ops._exec_python_snippet(snippet)
    if probe.exit_code != 0:
        raise ValueError("Cannot capture remote file versions")
    snapshots = json.loads(probe.stdout)
    if not isinstance(snapshots, list) or len(snapshots) != len(entries):
        raise ValueError("Invalid remote file version response")
    return snapshots


def capture_file_execution_state(action_type, arguments):
    from tools import file_tools as ft
    from tools.file_tools_paths import _terminal_env_type_for_task
    from tools.file_tools_read_tracking import _has_full_write_baseline
    from tools.durable_execution_approval import capture_terminal_execution_state

    task_id = arguments.get("task_id") or "default"
    paths = _paths(action_type, arguments)
    file_ops = ft._get_file_ops(task_id)
    host = ft._file_ops_uses_host_paths(file_ops)
    entries = [(path, str(ft._resolve_entry_for_task(path, task_id))) for path in paths]
    if host:
        snapshots = []
        for path, entry in entries:
            resolved = str(ft._resolve_path_for_task(path, task_id))
            snapshots.append({"path": path, "entry": entry, "resolved": resolved,
                              **_host_snapshot(entry, resolved)})
    else:
        snapshots = _remote_snapshots(file_ops, entries)
    for snapshot in snapshots:
        snapshot["full_read_baseline"] = (
            host and _has_full_write_baseline(snapshot["resolved"], task_id)
            if action_type == "file.write" and snapshot["exists"] else False
        )
    backend = _terminal_env_type_for_task(task_id)
    return {"backend": backend, "backend_state": capture_terminal_execution_state(file_ops.env, backend),
            "host_paths": host, "targets": snapshots}


def restore_file_execution_baseline(action_type, arguments, baseline):
    """Restore only a previously current native overwrite proof after restart."""
    from tools.file_tools_read_tracking import _mark_full_write_baseline

    current = capture_file_execution_state(action_type, arguments)
    # Read coverage is process-local. Its absence after a restart does not
    # alter the reviewed target, but it must never create a new permission.
    for state in (current, baseline):
        if not isinstance(state, dict) or not isinstance(state.get("targets"), list):
            return False
    expected = json.loads(json.dumps(baseline))
    for target in expected["targets"]:
        target["full_read_baseline"] = False
    for target in current["targets"]:
        target["full_read_baseline"] = False
    if current != expected:
        return False
    if action_type == "file.write" and baseline.get("host_paths"):
        for target in baseline["targets"]:
            if target["exists"] and not target["full_read_baseline"]:
                return False
            if target["full_read_baseline"]:
                _mark_full_write_baseline(target["resolved"], arguments.get("task_id") or "default",
                                          target["version"][-1])
    return True


def durable_file_execution(action_type):
    """Bind only approval-gated Discord edits; ordinary file operations stay native."""
    def decorate(function):
        signature = inspect.signature(function)

        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            from tools import file_tools as ft
            from tools.file_tools_write_guards import _protected_instruction_reason
            from tools.durable_execution_approval import _policy_for, durable_execution_context
            from gateway.session_context import get_session_env
            from agent.file_safety import get_write_denied_error, is_write_approval_required

            if get_session_env("HERMES_SESSION_PLATFORM", "").lower() != "discord" or _policy_for(action_type) is None:
                return function(*args, **kwargs)
            bound = signature.bind(*args, **kwargs)
            bound.apply_defaults()
            arguments = dict(bound.arguments)
            task_id = arguments["task_id"]
            try:
                paths = _paths(action_type, arguments)
                gated = any(_protected_instruction_reason(path, task_id) or is_write_approval_required(path)
                            for path in paths)
                if not gated:
                    return function(*args, **kwargs)
                for path in paths:
                    resolved = str(ft._resolve_path_for_task(path, task_id))
                    error = ft._check_sensitive_path(path, task_id) or get_write_denied_error(resolved)
                    if error:
                        return ft.tool_error(error)
                baseline = capture_file_execution_state(action_type, arguments)
            except (ValueError, OSError, RuntimeError) as exc:
                return ft.tool_error("Cannot capture the reviewed file target: " + str(exc))
            with durable_execution_context(action_type, arguments, execution_state=baseline):
                return function(*args, **kwargs)
        return wrapped
    return decorate
