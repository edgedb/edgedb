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

from copy import deepcopy
from typing import Optional, Sequence, List
from dataclasses import dataclass, field
import enum
import uuid

from edb.common import compiler
from edb.schema import schema as s_schema


@dataclass(frozen=True)
class Options:
    current_database: str

    current_user: str

    current_query: str

    # schemas that will be searched when idents don't have an explicit one
    search_path: Sequence[str] = ("public",)


@dataclass(kw_only=True)
class Scope:
    """
    Information about that objects are visible at a specific point in an
    SQL query.

    Scope is modified during resolving of a query, when new tables are
    discovered in FROM or JOIN or new columns declared in SELECT's projection.

    After a query is done resolving, resulting relations are extracted from its
    scope and inserted into parent scope.
    """

    # RangeVars (table instances) in this query
    tables: List[Table] = field(default_factory=lambda: [])

    # Common Table Expressions
    ctes: List[CTE] = field(default_factory=lambda: [])


@dataclass(kw_only=True)
class Table:

    # Public SQL
    name: Optional[str] = None
    alias: Optional[str] = None

    columns: List[Column] = field(default_factory=lambda: [])

    # Internal SQL
    reference_as: Optional[str] = None

    # For ambiguous references, this fields determines lookup order.
    # Higher value is matched before lower.
    # Aliases from current relation have higher precedence in GROUP BY
    # than columns of input rel vars (tables).
    # Columns from parent scopes have lower precedence
    # than columns of input rel vars (tables).
    precedence: int = 0

    def __str__(self) -> str:
        columns = ', '.join(str(c) for c in self.columns)
        alias = f'{self.alias} = ' if self.alias else ''
        return f'{alias}{self.name or "<unnamed>"}({columns})'


@dataclass(kw_only=True)
class CTE:
    name: Optional[str] = None
    columns: List[Column] = field(default_factory=lambda: [])


@dataclass(kw_only=True)
class Column:
    # Public SQL
    name: str

    # When true, column is not included when selecting *
    # Used for system columns
    # https://www.postgresql.org/docs/14/ddl-system-columns.html
    hidden: bool = False

    kind: ColumnKind

    def __str__(self) -> str:
        return self.name or '<unnamed>'


class ColumnKind:
    # When a column is referenced, implementation of this class determined
    # into what it is compiled to.
    # The base case is ColumnByName, which just means that it compiles to an
    # identifier to a column.
    pass


@dataclass(kw_only=True)
class ColumnByName(ColumnKind):
    # Internal SQL column name
    reference_as: str


@dataclass(kw_only=True)
class ColumnStaticVal(ColumnKind):
    # Value that can be used instead referencing the column.
    # Used from __type__ only, so that's why it is UUID (for now).
    val: uuid.UUID


class ContextSwitchMode(enum.Enum):
    EMPTY = enum.auto()
    CHILD = enum.auto()
    LATERAL = enum.auto()


class ResolverContextLevel(compiler.ContextLevel):
    schema: s_schema.Schema
    names: compiler.AliasGenerator

    # Visible names in scope
    scope: Scope

    # True iff relation currently resolving should also include instances of
    # child objects.
    include_inherited: bool

    options: Options

    def __init__(
        self,
        prevlevel: Optional[ResolverContextLevel],
        mode: ContextSwitchMode,
        *,
        schema: Optional[s_schema.Schema] = None,
        options: Optional[Options] = None,
    ) -> None:
        if prevlevel is None:
            assert schema
            assert options

            self.schema = schema
            self.options = options
            self.scope = Scope()
            self.include_inherited = True
            self.names = compiler.AliasGenerator()

        else:
            self.schema = prevlevel.schema
            self.options = prevlevel.options
            self.names = prevlevel.names

            self.include_inherited = True

            if mode == ContextSwitchMode.EMPTY:
                self.scope = Scope(ctes=prevlevel.scope.ctes)
            elif mode == ContextSwitchMode.CHILD:
                self.scope = deepcopy(prevlevel.scope)
                for t in self.scope.tables:
                    t.precedence -= 1
            elif mode == ContextSwitchMode.LATERAL:
                self.scope = deepcopy(prevlevel.scope)

    def empty(
        self,
    ) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Create a new empty context"""
        return self.new(ContextSwitchMode.EMPTY)

    def child(self) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Clone current context, prevent changes from leaking to parent"""
        return self.new(ContextSwitchMode.CHILD)

    def lateral(self) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Clone current context, prevent changes from leaking to parent"""
        return self.new(ContextSwitchMode.LATERAL)


class ResolverContext(compiler.CompilerContext[ResolverContextLevel]):
    ContextLevelClass = ResolverContextLevel
    default_mode = ContextSwitchMode.EMPTY
