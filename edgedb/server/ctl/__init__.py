##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import sys

from . import init as init_mod


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='EdgeDB Server Control')
    backend_info = parser.add_mutually_exclusive_group(required=True)
    backend_info.add_argument('-D', '--data-dir', type=str,
                              help='database cluster directory')

    backend_info.add_argument(
        '-P', '--postgres', type=str,
        help='address of Postgres backend server')

    parser.add_argument(
        '--postgres-superuser', type=str, default='postgres',
        metavar='ROLE',
        help='name of Postgres superuser role (use with --postgres)')

    sub = parser.add_subparsers(title='control commands', dest='command')
    sub.required = True

    sub.add_parser('init', help='initialize EdgeDB cluster')

    args = parser.parse_args(argv)

    if args.command == 'init':
        init_mod.main(args)


if __name__ == '__main__':
    main()
