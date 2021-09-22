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


import subprocess
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
        '--host' not in args and
        not any(a.startswith('-H') for a in args) and
        not any(a.startswith('--host=') for a in args) and
        '--port' not in args and
        not any(a.startswith('-P') for a in args) and
        not any(a.startswith('--port=') for a in args) and
        '--instance' not in args and
        not any(a.startswith('-I') for a in args) and
        not any(a.startswith('--instance=') for a in args)
    ):
        args += ['-I', '_localdev']
        subprocess.check_call([
            sys.executable,
            "-m",
            "edb.cli",
            "instance",
            "link",
            "--host=localhost",
            "--non-interactive",
            "--trust-tls-cert",
            "--overwrite",
            "--quiet",
            "_localdev",
        ])

    if (
        '--wait-until-available' not in args and
        not any('--wait-until-available=' in a for a in args)
    ):
        args += ['--wait-until-available', '5s']

    sys.exit(rustcli.rustcli(args=[sys.argv[0], *args]))
