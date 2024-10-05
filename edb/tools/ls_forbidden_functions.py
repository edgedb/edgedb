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

import asyncio

from edb.tools.edb import edbcommands


async def run():
    import asyncpg

    # import os
    # localdev = os.path.expanduser('~/.local/share/edgedb/_localdev')
    # c = await asyncpg.connect(
    #     host=localdev, database='postgres', user='postgres'
    # )

    # docker run -it -p 5433:5432 --rm -e POSTGRES_PASSWORD=pass postgres:13
    c = await asyncpg.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='pass',
        port='5433',
    )

    res = await c.fetch(
        r'''
        SELECT DISTINCT proname
        FROM pg_proc p
        INNER JOIN pg_namespace n ON pronamespace = n.oid
        WHERE n.nspname = 'pg_catalog' AND proname like 'pg_%'
        ORDER BY proname;
        '''
    )

    import edb.pgsql.resolver.static as pg_r_static

    print('Forbidden pg_* functions:')
    for row in res:
        [func_name] = row
        if func_name in pg_r_static.ALLOWED_ADMIN_FUNCTIONS:
            continue
        print('  ', func_name)


@edbcommands.command("ls-forbidden-functions")
def main():
    asyncio.run(run())
