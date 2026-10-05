"""Discoverable request marker for approval-gated host commands."""

import argparse
import sys


def host_sudo_command(args):
    print('Host sudo requires owner approval. Ask Hermes in Discord to run '
          'this exact command through its terminal tool; an inline durable '
          'approval will be created. This standalone CLI does not execute sudo.',
          file=sys.stderr)
    return 2


def build_host_parser(subparsers):
    host = subparsers.add_parser('host', help='Request approval-gated host operations')
    operations = host.add_subparsers(dest='host_operation')
    sudo = operations.add_parser('sudo', help='Request exact host argv through the agent terminal',
                                 description='Use in a Discord agent terminal call: '
                                 'hermes host sudo -- /absolute/program args. '
                                 'The reviewed command runs once after owner approval.')
    sudo.add_argument('argv', nargs=argparse.REMAINDER)
    host.set_defaults(func=host_sudo_command)
    sudo.set_defaults(func=host_sudo_command)
