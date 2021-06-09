#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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


import sys

import click

from edb import cli as rustcli
from edb.tools.edb import edbcommands


@edbcommands.command('cli',
                     add_help_option=False,
                     context_settings=dict(ignore_unknown_options=True))
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def cli(args: list[str]):
    """Run edgedb CLI with `-H localhost`."""

    args = list(args)

    if (
        '-H' not in args and
        '--host' not in args and
        not any('--host=' in a for a in args)
    ):
        args += ['-H', 'localhost']

    if (
        '--wait-until-available' not in args and
        not any('--wait-until-available=' in a for a in args)
    ):
        args += ['--wait-until-available', '5s']

    sys.exit(rustcli.rustcli(args=[sys.argv[0], *args]))
