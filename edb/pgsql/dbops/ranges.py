#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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

import textwrap

from ..common import qname as qn
from ..common import quote_literal as ql

from . import base
from . import ddl


class RangeExists(base.Condition):
    def __init__(self, name):
        self.name = name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                t.typname
            FROM
                pg_catalog.pg_type t
                INNER JOIN pg_namespace nsp
                    ON (t.typnamespace = nsp.oid)
            WHERE
                nsp.nspname = {ql(self.name[0])}
                AND t.typname = {ql(self.name[1])}
                AND t.typtype = 'r'
        ''')


class Range(base.DBObject):
    def __init__(self, name, subtype, *, subtype_diff=None):
        super().__init__()
        self.name = name
        self.subtype = subtype
        self.subtype_diff = subtype_diff


class CreateRange(ddl.SchemaObjectOperation):
    def __init__(self, range, *, conditions=None, neg_conditions=None):
        super().__init__(
            range.name, conditions=conditions, neg_conditions=neg_conditions)
        self.range = range

    def code(self) -> str:
        subs = [f'subtype = {qn(*self.range.subtype)}']
        if self.range.subtype_diff is not None:
            subs.append(f'subtype_diff = {qn(*self.range.subtype_diff)}')
        subcommands = ', '.join(subs)
        return f'''\
            CREATE TYPE {qn(*self.name)} AS RANGE ({subcommands})
        '''


class DropRange(ddl.SchemaObjectOperation):
    def code(self) -> str:
        return f'DROP TYPE {qn(*self.name)}'
