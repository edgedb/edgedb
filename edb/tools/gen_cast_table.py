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


from __future__ import annotations

import sys

from edb.tools.edb import edbcommands

from edb.server import defines as edgedb_defines

import edgedb


# Hardcode the scalar types (and the order of their appearance in
# the table), because it's hard to group them otherwise.
SCALARS = [
    'std::json',
    'std::str',
    'std::float32',
    'std::float64',
    'std::int16',
    'std::int32',
    'std::int64',
    'std::bigint',
    'std::decimal',
    'std::bool',
    'std::bytes',
    'std::uuid',
    'std::datetime',
    'std::duration',
    'cal::local_date',
    'cal::local_datetime',
    'cal::local_time',
]
SCALAR_SET = set(SCALARS)


def die(msg):
    print(f'FATAL: {msg}', file=sys.stderr)
    sys.exit(1)


def get_casts_to_type(target, impl_cast):
    results = []
    for source in SCALARS:
        cast = (source, target)
        if impl_cast.get(cast):
            results.append(cast)

    return results


def is_reachable(source, target, impl_cast):
    if source == target:
        return True

    casts = get_casts_to_type(target, impl_cast)
    if not casts:
        return False

    sources = {c[0] for c in casts}

    if source in sources:
        return True
    else:
        reachable = any(is_reachable(source, s, impl_cast) for s in sources)

        if reachable:
            impl_cast[(source, target)] = True

        return reachable


def get_all_casts(con):
    # Read the casts.
    casts = con.query('''
        WITH MODULE schema
        SELECT Cast {
            source := .from_type.name,
            target := .to_type.name,
            allow_assignment,
            allow_implicit,
        }
        FILTER .from_type IS ScalarType AND .to_type IS ScalarType
    ''')

    # Calculate the explicit, assignment, and implicit cast tables.
    expl_cast = {}
    assn_cast = {}
    impl_cast = {}
    for cast in casts:
        source = cast.source
        target = cast.target
        if source in SCALAR_SET and target in SCALAR_SET:
            expl_cast[(source, target)] = True
            if cast.allow_assignment:
                assn_cast[(source, target)] = True
            if cast.allow_implicit:
                assn_cast[(source, target)] = True
                impl_cast[(source, target)] = True

    # Implicit cast table needs to be recursively expanded from the
    # starting casts.
    for source in SCALARS:
        for target in SCALARS:
            is_reachable(source, target, impl_cast)

    return (expl_cast, assn_cast, impl_cast)


def main(con):
    expl_cast, assn_cast, impl_cast = get_all_casts(con)

    # Top row with all the scalars listed
    code = []
    line = ['from \\\\ to']
    for target in SCALARS:
        line.append(f':eql:type:`{target.split("::")[1]} <{target}>`')
    code.append(','.join(line))

    for source in SCALARS:
        line = [f':eql:type:`{source.split("::")[1]} <{source}>`']
        for target in SCALARS:
            val = ''
            if impl_cast.get((source, target)):
                val = 'impl'
            elif assn_cast.get((source, target)):
                val = '``:=``'
            elif expl_cast.get((source, target)):
                val = '``<>``'
            line.append(val)

        code.append(','.join(line))

    code = '\n'.join(code) + '\n'

    print(code, end='')


@edbcommands.command('gen-cast-table')
def gen_cast_table():
    """Generate a table of scalar casts to be used in the documentation.

    NAME - at the moment there's only one option 'edgeql'
    """

    con = None
    try:
        con = edgedb.connect(user=edgedb_defines.EDGEDB_SUPERUSER,
                             database=edgedb_defines.EDGEDB_SUPERUSER_DB,
                             port=5656)
        main(con)
    except Exception as ex:
        die(str(ex))
    finally:
        if con is not None:
            con.close()
