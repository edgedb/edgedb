#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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


import click

from edb import buildmeta
from edb.common import devmode
from edb.tools.edb import edbcommands


@edbcommands.command("config")
@click.option(
    "--make-include",
    is_flag=True,
    help='Print path to the include file for extension Makefiles',
)
@click.option(
    "--pg-config",
    is_flag=True,
    help='Print path to bundled pg_config',
)
def config(make_include: bool, pg_config: bool) -> None:
    '''Query certain parameters about an edgedb environment'''
    if make_include:
        share = buildmeta.get_extension_dir_path()
        base = share.parent.parent.parent
        # XXX: It should not be here.
        if not devmode.is_in_dev_mode():
            base = base / 'share'
        mk = (
            base / 'tests' / 'extension-testing' / 'exts.mk'
        )
        print(mk)
    if pg_config:
        print(buildmeta.get_pg_config_path())
