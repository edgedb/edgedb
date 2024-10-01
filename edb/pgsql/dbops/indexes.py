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

import re
import textwrap
from typing import Any, Tuple, Iterable, Dict, List

from edb.common import ordered

from ..common import qname as qn
from ..common import quote_ident as qi
from ..common import quote_literal as ql
from .. import ast as pgast

from . import base
from . import ddl
from . import tables


class Index(tables.InheritableTableObject):
    def __init__(
        self,
        name: str,
        table_name: Tuple[str, str],
        unique: bool = True,
        exprs: Iterable[str] | None = None,
        with_clause: Dict[str, str] | None = None,
        predicate: str | None = None,
        inherit: bool = False,
        metadata: Dict[str, Any] | None = None,
        columns: Iterable[str | pgast.Star] | None = None,
    ) -> None:
        super().__init__(inherit=inherit, metadata=metadata)

        assert table_name[1] != 'feature'

        self.name = name
        self.table_name = table_name
        self._columns: ordered.OrderedSet[str] = ordered.OrderedSet()
        if columns:
            self.add_columns(columns)
        self.with_clause = with_clause
        self.predicate = predicate
        self.unique = unique
        self.exprs = exprs

        self.add_metadata('fullname', self.name)

    @property
    def name_in_catalog(self) -> str:
        return self.name

    def add_columns(self, columns: Iterable[str | pgast.Star]) -> None:
        for col in columns:
            if isinstance(col, pgast.Star):
                raise NotImplementedError()
            self._columns.add(col)

    def creation_code(self) -> str:
        if self.exprs:
            exprs = self.exprs
        else:
            exprs = [qi(c) for c in self.columns]

        # Break down the code into the index name (if present) and the rest of
        # the expression
        m = re.match(r'(?P<using>\w+)?\s*(?P<expr>.+)',
                     self.get_metadata('code').strip())
        assert m is not None
        using = m['using']
        expr = m['expr']

        code = 'CREATE'
        if self.unique:
            code += ' UNIQUE'
        code += f' INDEX {qn(self.name_in_catalog)} ON {qn(*self.table_name)}'

        if using:
            code += f' USING {using}'

        # expr is expected to be wrapped in parentheses, but in order to
        # manipulate it better we strip the parentheses
        expr = expr[1:-1].replace('__col__', '{col}')
        expr = ', '.join(expr.format(col=e) for e in exprs)

        kwargs = self.get_metadata('kwargs')
        if kwargs is not None:
            # Escape the expression first
            expr = expr.replace('{', '{{').replace('}', '}}')
            expr = re.sub(r'(__kw_(\w+?)__)', r'{\2}', expr)
            expr = expr.format(**kwargs)

        # Put the stripped parentheses back
        code += f'({expr})'

        if self.with_clause:
            with_content = ','.join(
                f'{k}={v}' for (k, v) in self.with_clause.items()
            )
            code += f' WITH ({with_content})'

        if self.predicate:
            code += f' WHERE {self.predicate}'

        return code

    @property
    def columns(self) -> List[str]:
        return list(self._columns)

    def get_type(self) -> str:
        return 'INDEX'

    def get_id(self) -> str:
        return qn(self.table_name[0], self.name_in_catalog)

    def get_oid(self) -> base.Query:
        qry = textwrap.dedent(f'''\
            SELECT
                'pg_class'::regclass::oid AS classoid,
                i.indexrelid AS objectoid,
                0
            FROM
                pg_class AS c
                INNER JOIN pg_namespace AS ns ON ns.oid = c.relnamespace
                INNER JOIN pg_index AS i ON i.indrelid = c.oid
                INNER JOIN pg_class AS ic ON i.indexrelid = ic.oid
            WHERE
                ic.relname = {ql(self.name_in_catalog)}
                AND ns.nspname = {ql(self.table_name[0])}
                AND c.relname = {ql(self.table_name[1])}
        ''')

        return base.Query(text=qry)

    def copy(self) -> Index:
        return self.__class__(
            name=self.name,
            table_name=self.table_name,
            unique=self.unique,
            exprs=self.exprs,
            predicate=self.predicate,
            columns=self.columns,
            metadata=(
                self.metadata.copy() if self.metadata is not None else None
            )
        )

    def __repr__(self) -> str:
        return \
            '<%(mod)s.%(cls)s table=%(table)s name=%(name)s ' \
            'cols=(%(cols)s) unique=%(uniq)s predicate=%(pred)s>' % \
            {'mod': self.__class__.__module__,
             'cls': self.__class__.__name__,
             'name': self.name,
             'cols': ','.join('%r' % c for c in self.columns),
             'uniq': self.unique,
             'pred': self.predicate,
             'table': '{}.{}'.format(*self.table_name)}


class IndexExists(base.Condition):
    def __init__(self, index_name: tuple[str, str]):
        self.index_name = index_name

    def code(self) -> str:
        return textwrap.dedent(f'''\
            SELECT
                   i.indexrelid
               FROM
                   pg_catalog.pg_index i
                   INNER JOIN pg_catalog.pg_class ic
                        ON ic.oid = i.indexrelid
                   INNER JOIN pg_catalog.pg_namespace icn
                        ON icn.oid = ic.relnamespace
               WHERE
                   icn.nspname = {ql(self.index_name[0])}
                   AND ic.relname = {ql(self.index_name[1])}
        ''')


class CreateIndex(ddl.CreateObject):
    def __init__(
        self,
        index: Index,
        *,
        conditional: bool = False,
        **kwargs: Any
    ) -> None:
        super().__init__(index, **kwargs)
        self.index = index
        if conditional:
            self.neg_conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog))
            )

    def code(self) -> str:
        return self.index.creation_code()


class DropIndex(ddl.DropObject):
    def __init__(
        self,
        index: Index,
        *,
        conditional: bool = False,
        **kwargs: Any
    ):
        super().__init__(index, **kwargs)
        if conditional:
            self.conditions.add(
                IndexExists((index.table_name[0], index.name_in_catalog))
            )

    def code(self) -> str:
        assert isinstance(self.object, Index)
        name = qn(self.object.table_name[0], self.object.name_in_catalog)
        return f'DROP INDEX {name}'
