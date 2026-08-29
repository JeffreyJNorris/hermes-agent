"""Parser for trusted local durable-operation commands."""

from __future__ import annotations

from typing import Callable


def _add_page_options(parser, *, actions: bool = False) -> None:
    parser.add_argument("--status")
    if actions:
        parser.add_argument("--action-type")
        parser.add_argument("--minimum-age-seconds", type=float)
    else:
        parser.add_argument("--origin-type")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--json", action="store_true")


def build_durable_parser(subparsers, *, cmd_durable: Callable) -> None:
    durable = subparsers.add_parser(
        "durable",
        help="Inspect and safely manage local durable operations",
    )
    groups = durable.add_subparsers(dest="durable_group", metavar="<ledger>")

    requests = groups.add_parser("requests", help="Durable request ledger")
    request_commands = requests.add_subparsers(
        dest="durable_action", metavar="<command>"
    )
    request_list = request_commands.add_parser("list", help="List requests")
    _add_page_options(request_list)
    request_inspect = request_commands.add_parser("inspect", help="Inspect a request")
    request_inspect.add_argument("request_id")
    request_inspect.add_argument("--json", action="store_true")
    request_cancel = request_commands.add_parser("cancel", help="Cancel a request")
    request_cancel.add_argument("request_id")
    request_cancel.add_argument("-y", "--yes", action="store_true")
    request_cancel.add_argument("--json", action="store_true")

    actions = groups.add_parser("actions", help="Durable action ledger")
    action_commands = actions.add_subparsers(
        dest="durable_action", metavar="<command>"
    )
    action_list = action_commands.add_parser("list", help="List actions")
    _add_page_options(action_list, actions=True)
    action_inspect = action_commands.add_parser("inspect", help="Inspect an action")
    action_inspect.add_argument("request_id")
    action_inspect.add_argument("--json", action="store_true")

    for parser in (
        durable,
        requests,
        request_list,
        request_inspect,
        request_cancel,
        actions,
        action_list,
        action_inspect,
    ):
        parser.set_defaults(func=cmd_durable)


__all__ = ["build_durable_parser"]
