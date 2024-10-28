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

import pathlib
import sys
import typing

import click
import edgedb

from edb import edgeql
from edb.edgeql import ast as qlast
from edb.tools.edb import edbcommands


@edbcommands.command("test-extension-package")
@click.argument(
    "script_path",
    type=pathlib.Path,
)
@click.option('--localdev/--no-localdev',
              help='whether to connect to _localdev instance by default',
              default=True)
def test_extension(
    script_path: pathlib.Path,
    localdev: bool
) -> None:
    '''Installs an extension package into a dev environment and creates it.

    Removes the extension and package first if it already exists.'''

    with open(script_path) as f:
        script = f.read()

    statements = edgeql.parse_block(script)
    if not statements or not isinstance(
        statements[0], qlast.CreateExtensionPackage
    ):
        print("Script does not begin with CREATE EXTENSION PACKAGE")
        sys.exit(1)
    extension_name = statements[0].name.name

    conn_params: dict[str, typing.Any] = {}
    if localdev:
        conn_params = dict(
            dsn='_localdev',
            tls_security='insecure',
        )
    db = edgedb.create_client(**conn_params)

    db.execute(f'''
        configure current database set __internal_testmode := true;
    ''')

    # Delete the extension and the package if it already exists
    ext = db.query('''
        select schema::Extension filter .name = <str>$0;
    ''', extension_name)
    if ext:
        print(f"Dropping existing extension {extension_name}")
        db.execute(f'''
            drop extension {extension_name};
        ''')
    ext_package = db.query('''
        select sys::ExtensionPackage {version} filter .name = <str>$0;
    ''', extension_name)
    if ext_package:
        v = ext_package[0].version
        version = f'{v.major}.{v.minor}.{v.stage_no}'
        print(
            f"Dropping existing extension package {extension_name} "
            f"version {version}"
        )
        db.execute(f'''
            drop extension package {extension_name} VERSION '{version}';
        ''')

    # Run the script; should create the package
    print(f"Creating extension package {extension_name}")
    db.execute(script)

    # Create the extension
    print(f"Creating extension {extension_name}")
    db.execute(f'''
        create extension {extension_name};
    ''')
