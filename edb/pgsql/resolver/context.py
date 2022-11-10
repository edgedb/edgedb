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
from typing import *
import enum

from edb.common import compiler
from edb.schema import schema as s_schema


class Scope:
    """
    Information about that objects are visible at a specific point in an
    SQL query.

    Scope is modified during resolving of a query, when new tables are
    discovered in FROM or JOIN or new columns declared in SELECT's projection.

    After a query is done resolving, resulting relations are extracted from its
    scope and inserted into parent scope.
    """

    rel: Table
    "Current relation where ResTargets are added to"

    join_relations: List[Table]
    "Current relation where Join relations are added to"

    tables: List[Table]
    """Tables visible in this scope"""

    ctes: List[CTE]
    """Common Table Expressions"""

    def __init__(self):
        self.tables = []
        self.rel = Table()
        self.join_relations = []
        self.ctes = []


class Table:

    # Public SQL
    name: Optional[str] = None
    alias: Optional[str] = None

    columns: List[Column]

    # Internal SQL
    reference_as: Optional[str] = None

    def __init__(self):
        self.columns = []

    def __str__(self) -> str:
        columns = ', '.join(str(c) for c in self.columns)
        alias = f'{self.alias} = ' if self.alias else ''
        return f'{alias}{self.name or "<unnamed>"}({columns})'


class Column:
    # Public SQL
    name: Optional[str] = None

    # Internal SQL
    reference_as: Optional[str] = None

    def __init__(
        self, name: Optional[str] = None, reference_as: Optional[str] = None
    ):
        self.name = name
        self.reference_as = reference_as

    def __str__(self) -> str:
        return self.name or '<unnamed>'


class CTE:
    name: Optional[str] = None
    columns: List[Column]

    def __init__(self):
        self.columns = []


class NameGenerator:
    next_rel_index = 0

    def generate_relation(self) -> str:
        name = f'_rel_{self.next_rel_index}'
        self.next_rel_index += 1
        return name


class ContextSwitchMode(enum.Enum):
    EMPTY = enum.auto()
    ISOLATED = enum.auto()


class ResolverContextLevel(compiler.ContextLevel):
    schema: s_schema.Schema
    names: NameGenerator

    scope: Scope
    """Visible names in scope"""

    include_inherited: bool
    """
    True iff relation currently resolving should also include instances of
    child objects.
    """

    def __init__(
        self,
        prevlevel: Optional[ResolverContextLevel],
        mode: ContextSwitchMode,
        *,
        schema: Optional[s_schema.Schema] = None,
    ) -> None:
        if prevlevel is None:
            assert schema is not None

            self.schema = schema
            self.scope = Scope()
            self.include_inherited = True
            self.names = NameGenerator()

        else:
            self.schema = prevlevel.schema
            self.names = prevlevel.names

            self.include_inherited = True

            if mode == ContextSwitchMode.EMPTY:
                self.scope = Scope()
                self.scope.ctes = prevlevel.scope.ctes
            elif mode == ContextSwitchMode.ISOLATED:
                self.scope = deepcopy(prevlevel.scope)

    def empty(
        self,
    ) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Create a new empty context"""
        return self.new(ContextSwitchMode.EMPTY)

    def isolated(
        self,
    ) -> compiler.CompilerContextManager[ResolverContextLevel]:
        """Clone current context, prevent changes from leaking to parent"""
        return self.new(ContextSwitchMode.ISOLATED)


class ResolverContext(compiler.CompilerContext[ResolverContextLevel]):
    ContextLevelClass = ResolverContextLevel
    default_mode = ContextSwitchMode.EMPTY
