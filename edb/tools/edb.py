#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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

import os

import click

from edb.common import debug
from edb.common import devmode as dm
from edb.server import main as srv_main


@click.group(
    context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--devmode/--no-devmode',
              help='enable or disable the development mode',
              default=True)
def edbcommands(devmode: bool):
    if devmode:
        dm.enable_dev_mode()


@edbcommands.command()
@srv_main.server_options
def server(**kwargs):
    os.environ['EDGEDB_DEBUG_SERVER'] = '1'
    debug.init_debug_flags()

    srv_main.server_main(insecure=True, **kwargs)


# Import at the end of the file so that "edb.tools.edb.edbcommands"
# is defined for all of the below modules when they try to import it.
from . import dflags  # noqa
from . import gen_errors  # noqa
from . import gen_types  # noqa
from . import gen_meta_grammars  # noqa
from . import inittestdb  # noqa
from . import test  # noqa
from . import wipe  # noqa
from .profiling import cli  # noqa
