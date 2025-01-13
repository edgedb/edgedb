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
from typing import Optional, Sequence, List, Dict, Mapping, Tuple
from dataclasses import dataclass, field
import enum
import uuid

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import aliases

from edb.common import compiler
from edb.server.compiler import dbstate

from edb.schema import schema as s_schema
from edb.schema import objects as s_objects
from edb.schema import pointers as s_pointers


@dataclass(frozen=True, kw_only=True, repr=False, match_args=False)
class Options:
    current_database: str

    current_user: str

    current_query: str

    # schemas that will be searched when idents don't have an explicit one
    search_path: Sequence[str]

    # allow setting id in inserts
    allow_user_specified_id: bool

    # apply access policies to select & dml statements
    apply_access_policies: bool

    # whether to generate an EdgeQL-compatible single-column output variant.
    include_edgeql_io_format_alternative: Optional[bool]

    # makes sure that output does not contain duplicated column names
    disambiguate_column_names: bool

    # Type oids of parameters that have taken place of constants during query
    # normalization.
    # When this is non-empty, the resolver is allowed to raise
    # DisableNormalization to recompile the query without normalization.
    normalized_params: List[int]

    # Apply a limit to the number of rows in the top-level query
    implicit_limit: Optional[int]


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

    # Pairs of columns of the same name that have been compared in a USING
    # clause. This makes unqualified references to their name them un-ambiguous.
    # The fourth tuple element is the join type.
    factored_columns: List[Tuple[str, Table, Table, str]] = field(
        default_factory=lambda: []
    )


@dataclass(kw_only=True)
class Table:
    # The schema id of the object that is the source of this table
    schema_id: Optional[uuid.UUID] = None

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

    # True when this relation is compiled to a direct reference to the
    # underlying table, without any views or CTEs.
    # Is the condition for usage of locking clauses.
    is_direct_relation: bool = False

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


@dataclass(kw_only=True)
class ColumnComputable(ColumnKind):
    # An EdgeQL computable property. To get the AST for this column, EdgeQL
    # compiler needs to be invoked.
    pointer: s_pointers.Pointer


@dataclass(kw_only=True)
class ColumnPgExpr(ColumnKind):
    # Value that was provided by some special resolver path.
    expr: pgast.BaseExpr


@dataclass(kw_only=True, eq=False, slots=True, repr=False)
class CompiledDML:
    # relation that provides the DML value. not yet resolved.
    value_cte_name: str

    # relation that provides the DML value. not yet resolved.
    value_relation_input: pgast.BaseRelation

    # columns that are expected to be produced by the value relation
    value_columns: List[Tuple[str, bool]]

    # name of the column in the value relation, that should provide the identity
    value_iterator_name: Optional[str]

    # CTEs that perform the operation
    output_ctes: List[pgast.CommonTableExpr]

    # name of the CTE that contains the output of the insert
    output_relation_name: str

    # mapping from output column names into output vars
    output_namespace: Mapping[str, pgast.BaseExpr]


class ContextSwitchMode(enum.Enum):
    EMPTY = enum.auto()
    CHILD = enum.auto()
    LATERAL = enum.auto()


class ResolverContextLevel(compiler.ContextLevel):
    schema: s_schema.Schema
    alias_generator: aliases.AliasGenerator

    # Visible names in scope
    scope: Scope

    # 0 for top-level statement, 1 for its CTEs/sub-relations/links
    # and so on for all subqueries.
    subquery_depth: int

    # List of CTEs to add the top-level statement.
    # This is used, for example, by DML compilation to ensure that all DML is
    # in the top-level WITH binding.
    ctes_buffer: List[pgast.CommonTableExpr]

    # A mapping of from objects to CTEs that provide an "inheritance view",
    # which is basically a union of all of their descendant's tables.
    inheritance_ctes: Dict[s_objects.InheritingObject, str]

    compiled_dml: Mapping[pgast.Query, CompiledDML]

    options: Options

    query_params: List[dbstate.SQLParam]
    """List of params needed by the compiled query. Gets populated during
    compilation and also includes params needed for globals, from calls to ql
    compiler."""

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
            self.alias_generator = aliases.AliasGenerator()
            self.subquery_depth = 0
            self.ctes_buffer = []
            self.inheritance_ctes = dict()
            self.compiled_dml = dict()
            self.query_params = []

        else:
            self.schema = prevlevel.schema
            self.options = prevlevel.options
            self.alias_generator = prevlevel.alias_generator

            self.subquery_depth = prevlevel.subquery_depth + 1
            self.ctes_buffer = prevlevel.ctes_buffer
            self.inheritance_ctes = prevlevel.inheritance_ctes
            self.compiled_dml = prevlevel.compiled_dml
            self.query_params = prevlevel.query_params

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
