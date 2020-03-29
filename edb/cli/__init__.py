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


from __future__ import annotations
from typing import *  # NoQA

import os
import pathlib
import sys

import click

from edb.common import devmode as dm

from edb import repl
from . import utils


@click.group(
    invoke_without_command=True,
    context_settings=dict(help_option_names=['-h', '--help']))
@utils.connect_command
@click.pass_context
def cli(ctx):
    if ctx.invoked_subcommand is None:
        status = repl.main(ctx.obj['connargs'])
        sys.exit(status)


def cli_dev():
    dm.enable_dev_mode()
    cli()


def rustcli() -> NoReturn:
    thisdir = pathlib.Path(__file__).parent
    os.execve(str(thisdir / 'edgedb'), sys.argv, os.environ)


# Import subcommands to register them

from . import dump  # noqa
from . import mng  # noqa
