"""Thin local CLI over :mod:`gateway.durable_operations`."""

from __future__ import annotations

import json
import getpass
import sqlite3
import sys
from dataclasses import asdict
from typing import Any

from gateway.durable_operations import DurableOperationsStore
from hermes_cli.cli_output import (
    print_error,
    print_header,
    print_info,
    print_warning,
    prompt_yes_no,
)
from hermes_constants import get_hermes_home


EXIT_OK = 0
EXIT_WARNING = 1
EXIT_INVALID = 2
EXIT_NOT_FOUND = 3
EXIT_CONFLICT = 4
EXIT_FAILURE = 5


def _emit(payload: dict[str, Any], *, json_output: bool, error: bool = False) -> None:
    if json_output:
        print(json.dumps(payload, sort_keys=True))
        return
    message = payload.get("code", "unknown").replace("_", " ")
    (print_error if error else print_info)(message)


def _emit_page(page, *, ledger: str, json_output: bool) -> None:
    items = [asdict(item) for item in page.items]
    if json_output:
        _emit(
            {"code": "ok", "items": items, "next_offset": page.next_offset},
            json_output=True,
        )
        return
    print_header(f"Durable {ledger}")
    if not items:
        print_info("No matching records.")
    for item in items:
        print_info(
            f"{item['request_id']}  {item['status']}  "
            f"{item.get('action_type', item.get('kind', ''))}"
        )
    if page.next_offset is not None:
        print_info(f"More records: --offset {page.next_offset}")


def _emit_detail(detail, *, ledger: str, json_output: bool) -> None:
    payload = {"code": "ok", **asdict(detail)}
    if json_output:
        _emit(payload, json_output=True)
        return
    print_header(f"Durable {ledger} {detail.request_id}")
    for key, value in asdict(detail).items():
        print_info(f"{key}: {value}")


def _result_exit(code: str) -> int:
    if code in {"cancelled", "reconciled"}:
        return EXIT_OK
    if code == "not_found":
        return EXIT_NOT_FOUND
    if code in {
        "already_terminal",
        "already_reconciled",
        "execution_in_progress",
        "concurrent_update",
        "invalid_state",
        "invalid_superseding_receipt",
    }:
        return EXIT_CONFLICT
    return EXIT_FAILURE


def durable_command(args) -> int:
    json_output = bool(getattr(args, "json", False))
    group = getattr(args, "durable_group", None)
    action = getattr(args, "durable_action", None)
    if group not in {"requests", "actions"} or action not in {
        "list",
        "inspect",
        "cancel",
        "reconcile",
    }:
        _emit({"code": "invalid_command"}, json_output=json_output, error=True)
        return EXIT_INVALID

    try:
        store = DurableOperationsStore(get_hermes_home() / "state.db")
        if action == "list":
            if group == "requests":
                page = store.list_requests(
                    status=args.status,
                    origin_type=args.origin_type,
                    limit=args.limit,
                    offset=args.offset,
                )
            else:
                page = store.list_actions(
                    status=args.status,
                    action_type=args.action_type,
                    minimum_age_seconds=args.minimum_age_seconds,
                    limit=args.limit,
                    offset=args.offset,
                )
            _emit_page(page, ledger=group, json_output=json_output)
            return EXIT_OK

        if action == "inspect":
            detail = (
                store.get_request(args.request_id)
                if group == "requests"
                else store.inspect_action(
                    args.request_id, diagnose=bool(getattr(args, "diagnose", False))
                )
            )
            if detail is None:
                _emit(
                    {"code": "not_found", "request_id": args.request_id},
                    json_output=json_output,
                    error=True,
                )
                return EXIT_NOT_FOUND
            _emit_detail(detail, ledger=group[:-1], json_output=json_output)
            return EXIT_OK

        if action == "reconcile":
            if group != "actions":
                _emit({"code": "invalid_command"}, json_output=json_output, error=True)
                return EXIT_INVALID
            detail = store.get_action(args.request_id)
            if detail is None:
                _emit(
                    {"code": "not_found", "request_id": args.request_id},
                    json_output=json_output,
                    error=True,
                )
                return EXIT_NOT_FOUND
            if not args.yes:
                if json_output or not sys.stdin.isatty():
                    _emit(
                        {"code": "confirmation_required", "request_id": args.request_id},
                        json_output=json_output,
                        error=True,
                    )
                    return EXIT_INVALID
                print_header(f"Reconcile durable action {args.request_id}")
                print_info(f"status: {detail.status}; outcome: {args.outcome}")
                if not prompt_yes_no("Record this immutable conclusion?", default=False):
                    print_warning("Confirmation required; no changes made.")
                    return EXIT_INVALID
            result = store.reconcile_action(
                args.request_id,
                outcome=args.outcome,
                operator_identity=getpass.getuser(),
                note=args.note,
                superseded_by_request_id=args.superseded_by,
            )
            _emit(asdict(result), json_output=json_output, error=not result.ok)
            return _result_exit(result.code)

        if group != "requests":
            _emit({"code": "invalid_command"}, json_output=json_output, error=True)
            return EXIT_INVALID
        if not args.yes:
            if json_output or not sys.stdin.isatty():
                _emit(
                    {"code": "confirmation_required", "request_id": args.request_id},
                    json_output=json_output,
                    error=True,
                )
                return EXIT_INVALID
            print_header(f"Cancel durable request {args.request_id}")
            if not prompt_yes_no("Proceed?", default=False):
                print_warning("Confirmation required; no changes made.")
                return EXIT_INVALID
        result = store.cancel_request(args.request_id)
        payload = asdict(result)
        _emit(payload, json_output=json_output, error=not result.ok)
        return _result_exit(result.code)
    except (TypeError, ValueError) as exc:
        _emit(
            {"code": "invalid_input", "error": type(exc).__name__},
            json_output=json_output,
            error=True,
        )
        return EXIT_INVALID
    except (OSError, sqlite3.Error) as exc:
        _emit(
            {"code": "database_unavailable", "error": type(exc).__name__},
            json_output=json_output,
            error=True,
        )
        return EXIT_FAILURE


__all__ = [
    "EXIT_CONFLICT",
    "EXIT_FAILURE",
    "EXIT_INVALID",
    "EXIT_NOT_FOUND",
    "EXIT_OK",
    "EXIT_WARNING",
    "durable_command",
]
