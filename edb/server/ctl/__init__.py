#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import argparse
import sys

from . import init as init_mod


def main(argv=sys.argv[1:], env=None):
    parser = argparse.ArgumentParser(description='EdgeDB Server Control')
    backend_info = parser.add_mutually_exclusive_group(required=True)
    backend_info.add_argument(
        '-D', '--data-dir', type=str, help='database cluster directory')

    backend_info.add_argument(
        '-P', '--postgres', type=str,
        help='address of Postgres backend server')

    parser.add_argument(
        '--postgres-superuser', type=str, default='postgres', metavar='ROLE',
        help='name of Postgres superuser role (use with --postgres)')

    sub = parser.add_subparsers(title='control commands', dest='command')
    sub.required = True

    sub.add_parser('init', help='initialize EdgeDB cluster')

    args = parser.parse_args(argv)

    if args.command == 'init':
        init_mod.main(args, env)


if __name__ == '__main__':
    main()
