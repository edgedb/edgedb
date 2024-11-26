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

from edb.common.log import early_setup
# ruff: noqa: E402
early_setup()

import os
import sys

import click

from edb import buildmeta
from edb.common import debug
from edb.common import devmode as dm
from edb.server import args as srv_args
from edb.server import main as srv_main
from edb.load_ext import main as load_ext_main


@click.group(
    context_settings=dict(help_option_names=['-h', '--help']))
@click.option('--devmode/--no-devmode',
              help='enable or disable the development mode',
              default=True)
@click.pass_context
def edbcommands(ctx, devmode: bool):
    if devmode:
        dm.enable_dev_mode()


@edbcommands.command()
@srv_args.server_options
def server(version=False, **kwargs):
    if version:
        print(f"edb, version {buildmeta.get_version()}")
        sys.exit(0)

    os.environ['EDGEDB_DEBUG_SERVER'] = '1'
    debug.init_debug_flags()
    kwargs['security'] = srv_args.ServerSecurityMode.InsecureDevMode
    srv_main.server_main(**kwargs)


@edbcommands.command(add_help_option=False,
                     context_settings=dict(ignore_unknown_options=True))
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def load_ext(args: tuple[str, ...]):
    load_ext_main.main(args)


# Import at the end of the file so that "edb.tools.edb.edbcommands"
# is defined for all of the below modules when they try to import it.
from . import cli  # noqa
from . import config  # noqa
from . import rm_data_dir  # noqa
from . import dflags  # noqa
from . import gen_errors  # noqa
from . import gen_types  # noqa
from . import gen_meta_grammars  # noqa
from . import gen_cast_table  # noqa
from . import inittestdb  # noqa
from . import test  # noqa
from . import test_extension  # noqa
from . import wipe  # noqa
from . import gen_test_dumps  # noqa
from . import gen_sql_introspection  # noqa
from . import gen_rust_ast  # noqa
from . import ast_inheritance_graph  # noqa
from . import parser_demo  # noqa
from . import ls_forbidden_functions  # noqa
from . import redo_metaschema  # noqa
from .profiling import cli as prof_cli  # noqa
from .experimental_interpreter import edb_entry # noqa
