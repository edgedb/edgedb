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

import textwrap

from ..common import qname as qn
from ..common import quote_literal as ql

from . import base
from . import ddl


class View(base.DBObject):
    def __init__(self, name, query, materialized=False):
        super().__init__()
        self.name = name
        self.query = query
        self.materialized = materialized

    def get_type(self) -> str:
        return "VIEW" if not self.materialized else "MATERIALIZED VIEW"

    def get_id(self):
        return qn(*self.name)


class CreateView(ddl.SchemaObjectOperation):
    def __init__(
        self,
        view,
        *,
        conditions=None,
        neg_conditions=None,
        or_replace=False,
    ):
        super().__init__(view.name, conditions=conditions,
                         neg_conditions=neg_conditions)
        self.view = view
        self.or_replace = or_replace

    def code(self) -> str:
        query = textwrap.indent(textwrap.dedent(self.view.query), '    ')
        return (
            f'CREATE {"OR REPLACE" if self.or_replace else ""}'
            f' {self.view.get_type()} {qn(*self.view.name)} AS\n{query}'
        )


class DropView(ddl.SchemaObjectOperation):

    def __init__(
        self,
        name,
        *,
        conditional=False,
        conditions=None,
        neg_conditions=None,
        materialized=False,
    ):
        super().__init__(
            name,
            conditions=conditions,
            neg_conditions=neg_conditions,
        )
        self.conditional = conditional
        self.materialized = materialized

    def code(self) -> str:
        mat = 'MATERIALIZED ' if self.materialized else ''
        if self.conditional:
            return f'DROP {mat}VIEW IF EXISTS {qn(*self.name)}'
        else:
            return f'DROP {mat}VIEW {qn(*self.name)}'


class ViewExists(base.Condition):

    def __init__(self, name, materialized=False):
        self.name = name
        self.materialized = materialized

    def code(self) -> str:
        mat = 'mat' if self.materialized else ''
        return textwrap.dedent(f'''\
            SELECT
                {mat}viewname
            FROM
                pg_catalog.pg_{mat}views
            WHERE
                schemaname = {ql(self.name[0])}
                AND {mat}viewname = {ql(self.name[1])}
        ''')
