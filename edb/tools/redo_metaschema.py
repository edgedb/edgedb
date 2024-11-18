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

from edb.tools.edb import edbcommands


@edbcommands.command("redo-metaschema-sql")
def run():
    """
    Generates DDL to recreate metaschema for sql introspection.
    Can be used to apply changes to metaschema to an existing database.

    edb redo-metaschema-sql | ./build/postgres/install/bin/psql \
        "postgresql://postgres@/E_main?host=$(pwd)/tmp/devdatadir&port=5432" \
        -v ON_ERROR_STOP=ON
    """

    from edb.common import devmode
    devmode.enable_dev_mode()

    from edb.pgsql import dbops, metaschema
    from edb import buildmeta

    version = buildmeta.get_pg_version()
    commands = metaschema._generate_sql_information_schema(version)

    for command in commands:
        block = dbops.PLTopBlock()

        if isinstance(command, dbops.CreateFunction):
            command.or_replace = True
        if isinstance(command, dbops.CreateView):
            command.or_replace = True

        command.generate(block)

        print(block.to_string())
