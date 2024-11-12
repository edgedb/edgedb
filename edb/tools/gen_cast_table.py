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

import json
import subprocess
import sys

from edb.tools.edb import edbcommands


# NOTE: The types are HARDCODED here to trim the cast table to relevant
# built-in types and the order of their appearance in the table, because it's
# hard to group them otherwise.
#
# Please update this if new types need to be included.
TYPES = [
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
    'std::cal::local_date',
    'std::cal::local_datetime',
    'std::cal::local_time',
    'std::cal::relative_duration',
    'std::cal::date_duration',
    'std::anyenum',
    'std::BaseObject',
]
TYPES_SET = set(TYPES)


def die(msg):
    print(f'FATAL: {msg}', file=sys.stderr)
    sys.exit(1)


def get_casts_to_type(target, impl_cast):
    results = []
    for source in TYPES:
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


def get_all_casts(casts):
    # Calculate the explicit, assignment, and implicit cast tables.
    expl_cast = {}
    assn_cast = {}
    impl_cast = {}
    for cast in casts:
        source = cast['source']
        target = cast['target']
        if source in TYPES_SET and target in TYPES_SET:
            expl_cast[(source, target)] = True
            if cast['allow_assignment']:
                assn_cast[(source, target)] = True
            if cast['allow_implicit']:
                assn_cast[(source, target)] = True
                impl_cast[(source, target)] = True

    # Implicit cast table needs to be recursively expanded from the
    # starting casts.
    for source in TYPES:
        for target in TYPES:
            is_reachable(source, target, impl_cast)

    # HACK: We add the `uuid` -> `BaseObject` cast manually because it's
    # currently missing from the casting table.
    expl_cast[('std::uuid', 'std::BaseObject')] = True

    return (expl_cast, assn_cast, impl_cast)


def render_type(name):
    match name:
        case 'std::anyenum':
            return ':eql:type:`enum`'
        case 'std::BaseObject':
            return 'object'
        case _:
            return f':eql:type:`{name.split("::")[1]} <{name}>`'


def main(casts):
    expl_cast, assn_cast, impl_cast = get_all_casts(casts)

    # Top row with all the scalars listed
    code = []
    line = ['from \\\\ to']
    for target in TYPES:
        line.append(render_type(target))
    code.append(','.join(line))

    for source in TYPES:
        line = [render_type(source)]
        for target in TYPES:
            val = ''
            if impl_cast.get((source, target)):
                val = 'impl'
            elif assn_cast.get((source, target)):
                val = '``:=``'
            elif expl_cast.get((source, target)):
                if source in {
                    'std::float32', 'std::float64'
                } and target in {
                    'std::int16', 'std::int32', 'std::int64', 'std::bigint'
                }:
                    val = '``<>*``'
                else:
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

    try:
        res = subprocess.run([
            'edb',
            'cli',
            'query',
            '-Fjson',

            r"""
            WITH MODULE schema
            SELECT Cast {
                source := .from_type.name,
                target := .to_type.name,
                allow_assignment,
                allow_implicit,
            }
            FILTER all({.from_type, .to_type} IS ScalarType | ObjectType)
            """,
        ], capture_output=True)

        if res.returncode != 0:
            die('Could not connect to the dev Gel instance')

        main(json.loads(res.stdout))
    except Exception as ex:
        die(str(ex))
