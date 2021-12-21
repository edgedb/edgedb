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

import enum
import dataclasses
import typing
import uuid

from edb.common import ast
from edb.common import typeutils
from edb.edgeql import ast as qlast
from edb.ir import ast as irast


# The structure of the nodes mostly follows that of Postgres'
# parsenodes.h and primnodes.h, but only with fields that are
# relevant to parsing and code generation.
#
# Certain nodes have EdgeDB-specific fields used by the
# compiler.


class Base(ast.AST):
    def __repr__(self):
        return f'<pg.{self.__class__.__name__} at 0x{id(self):x}>'

    def dump_sql(self) -> None:
        from edb.common.debug import dump_sql
        dump_sql(self)


class ImmutableBase(ast.ImmutableASTMixin, Base):
    pass


class Alias(ImmutableBase):
    """Alias for a range variable."""

    # aliased relation name
    aliasname: str
    # optional list of column aliases
    colnames: typing.Optional[typing.List[str]] = None


class Keyword(ImmutableBase):
    """An SQL keyword that must be output without quoting."""

    name: str                   # Keyword name


class Star(Base):
    """'*' representing all columns of a table or compound field."""


class BaseExpr(Base):
    """Any non-statement expression node that returns a value."""

    __ast_meta__ = {'nullable'}

    nullable: typing.Optional[bool] = None  # Whether the result can be NULL.
    ser_safe: bool = False  # Whether the expr is serialization-safe.

    def __init__(self, *, nullable: typing.Optional[bool]=None,
                 **kwargs) -> None:
        nullable = self._is_nullable(kwargs, nullable)
        super().__init__(nullable=nullable, **kwargs)

    def _is_nullable(self, kwargs: typing.Dict[str, object],
                     nullable: typing.Optional[bool]) -> bool:
        if nullable is None:
            default = type(self).get_field('nullable').default
            if default is not None:
                nullable = default
            else:
                nullable = self._infer_nullability(kwargs)
        return nullable

    def _infer_nullability(self, kwargs: typing.Dict[str, object]) -> bool:
        nullable = False
        for v in kwargs.values():
            if typeutils.is_container(v):
                items = typing.cast(typing.Iterable, v)
                nullable = all(getattr(vv, 'nullable', False) for vv in items)

            elif getattr(v, 'nullable', None):
                nullable = True

            if nullable:
                break

        return nullable


class ImmutableBaseExpr(BaseExpr, ImmutableBase):
    pass


class OutputVar(ImmutableBaseExpr):
    """A base class representing expression output address."""

    # Whether this represents a packed array of data
    is_packed_multi: bool = False


class EdgeQLPathInfo(Base):
    """A general mixin providing EdgeQL-specific metadata on certain nodes."""

    # Ignore the below fields in AST visitor/transformer.
    __ast_meta__ = {
        'path_scope', 'path_outputs', 'path_id', 'is_distinct',
        'path_id_mask', 'path_namespace',
        'packed_path_outputs', 'packed_path_namespace',
    }

    # The path id represented by the node.
    path_id: typing.Optional[irast.PathId] = None

    # Whether the node represents a distinct set.
    is_distinct: bool = True

    # A subset of paths necessary to perform joining.
    path_scope: typing.Set[irast.PathId] = ast.field(factory=set)

    # Map of res target names corresponding to paths.
    path_outputs: typing.Dict[
        typing.Tuple[irast.PathId, str], OutputVar
    ] = ast.field(factory=dict)

    # Map of res target names corresponding to materialized paths.
    packed_path_outputs: typing.Optional[typing.Dict[
        typing.Tuple[irast.PathId, str],
        OutputVar,
    ]] = None

    def get_path_outputs(self, flavor: str) -> typing.Dict[
            typing.Tuple[irast.PathId, str], OutputVar]:
        if flavor == 'packed':
            if self.packed_path_outputs is None:
                self.packed_path_outputs = {}
            return self.packed_path_outputs
        elif flavor == 'normal':
            return self.path_outputs
        else:
            raise AssertionError(f'unexpected flavor "{flavor}"')

    path_id_mask: typing.Set[irast.PathId] = ast.field(factory=set)

    # Map of col refs corresponding to paths.
    path_namespace: typing.Dict[
        typing.Tuple[irast.PathId, str], BaseExpr
    ] = ast.field(factory=dict)

    # Same, but for packed.
    packed_path_namespace: typing.Optional[typing.Dict[
        typing.Tuple[irast.PathId, str],
        BaseExpr,
    ]] = None


class BaseRangeVar(ImmutableBaseExpr):
    """Range variable, used in FROM clauses."""

    __ast_meta__ = {'schema_object_id', 'tag'}

    # This is a hack, since there is some code that relies on not
    # having an alias on a range var (to refer to a CTE directly, for
    # example, while other code depends on reading the alias name out
    # of range vars. This is mostly disjoint code, so we hack around it
    # with an empty aliasname.
    alias: Alias = Alias(aliasname='')

    #: The id of the schema object this rvar represents
    schema_object_id: typing.Optional[uuid.UUID] = None

    #: Optional identification piece to describe what's inside the rvar
    tag: typing.Optional[str] = None

    def __repr__(self) -> str:
        return (
            f'<pg.{self.__class__.__name__} '
            f'alias={self.alias.aliasname} '
            f'at {id(self):#x}>'
        )


class BaseRelation(EdgeQLPathInfo, BaseExpr):
    name: typing.Optional[str] = None
    nullable: typing.Optional[bool] = None  # Whether the result can be NULL.


class Relation(BaseRelation):
    """Regular relation."""

    catalogname: typing.Optional[str] = None
    schemaname: typing.Optional[str] = None


class CommonTableExpr(Base):

    # Query name (unqualified)
    name: str
    # Whether the result can be NULL.
    nullable: typing.Optional[bool] = None
    # Optional list of column names
    aliascolnames: typing.Optional[list] = None
    # The CTE query
    query: Query
    # True if this CTE is recursive
    recursive: bool = False
    # If specified, determines if CTE is [NOT] MATERIALIZED
    materialized: typing.Optional[bool] = None

    def __repr__(self):
        return (
            f'<pg.{self.__class__.__name__} '
            f'name={self.name!r} at 0x{id(self):x}>'
        )


class PathRangeVar(BaseRangeVar):

    #: The IR TypeRef this rvar represents (if any).
    typeref: typing.Optional[irast.TypeRef] = None

    @property
    def query(self) -> BaseRelation:
        raise NotImplementedError


class RelRangeVar(PathRangeVar):
    """Relation range variable, used in FROM clauses."""

    relation: typing.Union[BaseRelation, CommonTableExpr]
    include_inherited: bool = True

    @property
    def query(self) -> BaseRelation:
        if isinstance(self.relation, CommonTableExpr):
            return self.relation.query
        else:
            return self.relation

    def __repr__(self) -> str:
        return (
            f'<pg.{self.__class__.__name__} '
            f'name={self.relation.name!r} alias={self.alias.aliasname} '
            f'at {id(self):#x}>'
        )


class IntersectionRangeVar(PathRangeVar):

    component_rvars: typing.List[PathRangeVar]


class TypeName(ImmutableBase):
    """Type in definitions and casts."""

    name: typing.Tuple[str, ...]                # Type name
    setof: bool = False                         # SET OF?
    typmods: typing.Optional[list] = None       # Type modifiers
    array_bounds: typing.Optional[list] = None  # Array bounds


class ColumnRef(OutputVar):
    """Specifies a reference to a column."""

    # Column name list.
    name: typing.Sequence[typing.Union[str, Star]]
    # Whether the col is an optional path bond (i.e accepted when NULL)
    optional: typing.Optional[bool] = None

    def __repr__(self):
        if hasattr(self, 'name'):
            return (
                f'<pg.{self.__class__.__name__} '
                f'name={".".join(self.name)!r} at 0x{id(self):x}>'
            )
        else:
            return super().__repr__()


class TupleElementBase(ImmutableBase):

    path_id: irast.PathId
    name: typing.Optional[typing.Union[OutputVar, str]]

    def __init__(self, path_id: irast.PathId,
                 name: typing.Optional[typing.Union[OutputVar, str]]=None):
        self.path_id = path_id
        self.name = name

    def __repr__(self):
        return f'<{self.__class__.__name__} ' \
               f'name={self.name} path_id={self.path_id}>'


class TupleElement(TupleElementBase):

    val: BaseExpr

    def __init__(self, path_id: irast.PathId, val: BaseExpr, *,
                 name: typing.Optional[typing.Union[OutputVar, str]]=None):
        super().__init__(path_id, name)
        self.val = val

    def __repr__(self):
        return f'<{self.__class__.__name__} ' \
               f'name={self.name} val={self.val} path_id={self.path_id}>'


class TupleVarBase(OutputVar):

    elements: typing.Sequence[TupleElementBase]
    named: bool
    nullable: bool
    typeref: typing.Optional[irast.TypeRef]

    def __init__(self, elements: typing.List[TupleElementBase], *,
                 named: bool=False, nullable: bool=False,
                 is_packed_multi: bool=False,
                 typeref: typing.Optional[irast.TypeRef]=None):
        self.elements = elements
        self.named = named
        self.nullable = nullable
        self.is_packed_multi = is_packed_multi
        self.typeref = typeref

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.elements!r}]'


class TupleVar(TupleVarBase):

    elements: typing.Sequence[TupleElement]

    def __init__(self, elements: typing.List[TupleElement], *,
                 named: bool=False, nullable: bool=False,
                 is_packed_multi: bool=False,
                 typeref: typing.Optional[irast.TypeRef]=None):
        self.elements = elements
        self.named = named
        self.nullable = nullable
        self.is_packed_multi = is_packed_multi
        self.typeref = typeref


class BaseParamRef(ImmutableBaseExpr):
    pass


class ParamRef(BaseParamRef):
    """Query parameter ($0..$n)."""

    # Number of the parameter.
    number: int


class NamedParamRef(BaseParamRef):
    """Named query parameter."""

    name: str


class ResTarget(ImmutableBaseExpr):
    """Query result target."""

    # Column name (optional)
    name: typing.Optional[str] = None
    # subscripts, field names and '*'
    indirection: typing.Optional[list] = None
    # value expression to compute
    val: BaseExpr


class UpdateTarget(ImmutableBaseExpr):
    """Query update target."""

    # column name (optional)
    name: str
    # value expression to assign
    val: BaseExpr


class InferClause(ImmutableBaseExpr):

    # IndexElems to infer unique index
    index_elems: typing.Optional[list] = None
    # Partial-index predicate
    where_clause: typing.Optional[BaseExpr] = None
    # Constraint name
    conname: typing.Optional[str] = None


class OnConflictClause(ImmutableBaseExpr):

    action: str
    infer: typing.Optional[InferClause]
    target_list: typing.Optional[list] = None
    where: typing.Optional[BaseExpr] = None


class ReturningQuery(BaseRelation):

    target_list: typing.List[ResTarget] = ast.field(factory=list)


class NullRelation(ReturningQuery):
    """Special relation that produces nulls for all its attributes."""

    where_clause: typing.Optional[BaseExpr] = None


@dataclasses.dataclass(frozen=True)
class Param:
    #: postgres' variable index
    index: int

    #: whether parameter is required
    required: bool


class Query(ReturningQuery):
    """Generic superclass representing a query."""

    # Ignore the below fields in AST visitor/transformer.
    __ast_meta__ = {'path_rvar_map', 'path_packed_rvar_map',
                    'view_path_id_map', 'argnames', 'nullable'}

    view_path_id_map: typing.Dict[
        irast.PathId, irast.PathId
    ] = ast.field(factory=dict)
    # Map of RangeVars corresponding to paths.
    path_rvar_map: typing.Dict[
        typing.Tuple[irast.PathId, str], PathRangeVar
    ] = ast.field(factory=dict)
    # Map of materialized RangeVars corresponding to paths.
    path_packed_rvar_map: typing.Optional[typing.Dict[
        typing.Tuple[irast.PathId, str],
        PathRangeVar,
    ]] = None

    argnames: typing.Optional[typing.Dict[str, Param]] = None

    ctes: typing.Optional[typing.List[CommonTableExpr]] = None

    def get_rvar_map(self, flavor: str) -> typing.Dict[
            typing.Tuple[irast.PathId, str], PathRangeVar]:
        if flavor == 'packed':
            if self.path_packed_rvar_map is None:
                self.path_packed_rvar_map = {}
            return self.path_packed_rvar_map
        elif flavor == 'normal':
            return self.path_rvar_map
        else:
            raise AssertionError(f'unexpected flavor "{flavor}"')

    def maybe_get_rvar_map(self, flavor: str) -> typing.Optional[typing.Dict[
            typing.Tuple[irast.PathId, str], PathRangeVar]]:
        if flavor == 'packed':
            return self.path_packed_rvar_map
        elif flavor == 'normal':
            return self.path_rvar_map
        else:
            raise AssertionError(f'unexpected flavor "{flavor}"')

    @property
    def ser_safe(self):
        return all(t.ser_safe for t in self.target_list)

    def append_cte(self, cte: CommonTableExpr) -> None:
        if self.ctes is None:
            self.ctes = []
        self.ctes.append(cte)


class DMLQuery(Query):
    """Generic superclass for INSERT/UPDATE/DELETE statements."""

    # Target relation to perform the operation on.
    relation: typing.Optional[PathRangeVar] = None
    # List of expressions returned
    returning_list: typing.List[ResTarget] = ast.field(factory=list)

    @property
    def target_list(self):
        return self.returning_list


class InsertStmt(DMLQuery):

    # (optional) list of target column names
    cols: typing.Optional[typing.List[ColumnRef]] = None
    # source SELECT/VALUES or None
    select_stmt: typing.Optional[Query] = None
    # ON CONFLICT clause
    on_conflict: typing.Optional[OnConflictClause] = None


class UpdateStmt(DMLQuery):

    # The UPDATE target list
    targets: typing.List[UpdateTarget] = ast.field(factory=list)
    # WHERE clause
    where_clause: typing.Optional[BaseExpr] = None
    # optional FROM clause
    from_clause: typing.List[BaseRangeVar] = ast.field(factory=list)


class DeleteStmt(DMLQuery):
    # WHERE clause
    where_clause: typing.Optional[BaseExpr] = None
    # optional USING clause
    using_clause: typing.List[BaseRangeVar] = ast.field(factory=list)


class SelectStmt(Query):

    # List of DISTINCT ON expressions, empty list for DISTINCT ALL
    distinct_clause: typing.Optional[list] = None
    # The target list
    target_list: typing.List[ResTarget] = ast.field(factory=list)
    # The FROM clause
    from_clause: typing.List[BaseRangeVar] = ast.field(factory=list)
    # The WHERE clause
    where_clause: typing.Optional[BaseExpr] = None
    # GROUP BY clauses
    group_clause: typing.Optional[typing.List[Base]] = None
    # HAVING expression
    having: typing.Optional[BaseExpr] = None
    # WINDOW window_name AS(...),
    window_clause: typing.Optional[typing.List[Base]] = None
    # List of ImplicitRow's in a VALUES query
    values: typing.Optional[typing.List[Base]] = None
    # ORDER BY clause
    sort_clause: typing.Optional[typing.List[SortBy]] = None
    # OFFSET expression
    limit_offset: typing.Optional[BaseExpr] = None
    # LIMIT expression
    limit_count: typing.Optional[BaseExpr] = None
    # FOR UPDATE clause
    locking_clause: typing.Optional[list] = None

    # Set operation type
    op: typing.Optional[str] = None
    # ALL modifier
    all: bool = False
    # Left operand of set op
    larg: typing.Optional[Query] = None
    # Right operand of set op,
    rarg: typing.Optional[Query] = None


class ExprKind(enum.IntEnum):
    OP = enum.auto()


class Expr(ImmutableBaseExpr):
    """Infix, prefix, and postfix expressions."""

    # Operator kind
    kind: ExprKind
    # Possibly-qualified name of operator
    name: str
    # Left argument, if any
    lexpr: typing.Optional[BaseExpr] = None
    # Right argument, if any
    rexpr: typing.Optional[BaseExpr] = None


class BaseConstant(ImmutableBaseExpr):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not isinstance(self, NullConstant) and self.val is None:
            raise ValueError('cannot create a pgast.Constant without a value')


class StringConstant(BaseConstant):
    """A literal string constant."""

    # Constant value
    val: str


class NullConstant(BaseConstant):
    """A NULL constant."""

    nullable: bool = True


class ByteaConstant(BaseConstant):
    """An bytea string."""

    val: str


class NumericConstant(BaseConstant):

    val: str


class BooleanConstant(BaseConstant):

    val: str


class LiteralExpr(ImmutableBaseExpr):
    """A literal expression."""

    # Expression text
    expr: str


class TypeCast(ImmutableBaseExpr):
    """A CAST expression."""

    # Expression being casted.
    arg: BaseExpr
    # Target type.
    type_name: TypeName


class CollateClause(ImmutableBaseExpr):
    """A COLLATE expression."""

    # Input expression
    arg: BaseExpr
    # Possibly-qualified collation name
    collname: str


class VariadicArgument(ImmutableBaseExpr):

    expr: BaseExpr
    nullable: bool = False


class ColumnDef(ImmutableBase):

    # name of column
    name: str
    # type of column
    typename: TypeName
    # default value, if any
    default_expr: typing.Optional[BaseExpr] = None
    # COLLATE clause, if any
    coll_clause: typing.Optional[BaseExpr] = None


class FuncCall(ImmutableBaseExpr):

    # Function name
    name: typing.Tuple[str, ...]
    # List of arguments
    args: typing.List[BaseExpr]
    # ORDER BY
    agg_order: typing.List[SortBy]
    # FILTER clause
    agg_filter: BaseExpr
    # Argument list is '*'
    agg_star: bool
    # Arguments were labeled DISTINCT
    agg_distinct: bool
    # OVER clause, if any
    over: typing.Optional[WindowDef]
    # WITH ORDINALITY
    with_ordinality: bool = False
    # list of ColumnDef nodes to describe result of
    # the function returning RECORD.
    coldeflist: typing.List[ColumnDef]

    def __init__(self, *, nullable: typing.Optional[bool]=None,
                 null_safe: bool=False, **kwargs) -> None:
        """Function call node.

        @param null_safe:
            Specifies whether this function is guaranteed
            to never return NULL on non-NULL input.
        """
        if nullable is None and not null_safe:
            nullable = True
        super().__init__(nullable=nullable, **kwargs)


class NamedFuncArg(ImmutableBaseExpr):

    name: str
    val: BaseExpr


class Indices(ImmutableBase):
    """Array subscript or slice bounds."""

    # True, if slice
    is_slice: bool
    # Lower bound, if any
    lidx: BaseExpr
    # Upper bound if any
    ridx: BaseExpr


class Indirection(ImmutableBaseExpr):
    """Field and/or array element indirection."""

    # Indirection subject
    arg: BaseExpr
    # Subscripts and/or field names and/or '*'
    indirection: list


class ArrayExpr(ImmutableBaseExpr):
    """ARRAY[] construct."""

    # array element expressions
    elements: typing.List[BaseExpr]


class MultiAssignRef(ImmutableBase):
    """UPDATE (a, b, c) = row-valued-expr."""

    # row-valued expression
    source: BaseExpr
    # list of columns to assign to
    columns: typing.List[ColumnRef]


class SortBy(ImmutableBase):
    """ORDER BY clause element."""

    # expression to sort on
    node: BaseExpr
    # ASC/DESC/USING/default
    dir: typing.Optional[qlast.SortOrder] = None
    # NULLS FIRST/LAST
    nulls: typing.Optional[qlast.NonesOrder] = None


class WindowDef(ImmutableBase):
    """WINDOW and OVER clauses."""

    # window name
    name: typing.Optional[str] = None
    # referenced window name, if any
    refname: typing.Optional[str] = None
    # PARTITION BY expr list
    partition_clause: typing.Optional[typing.List[BaseExpr]] = None
    # ORDER BY
    order_clause: typing.Optional[typing.List[SortBy]] = None
    # Window frame options
    frame_options: typing.Optional[list] = None
    # expression for starting bound, if any
    start_offset: typing.Optional[BaseExpr] = None
    # expression for ending ound, if any
    end_offset: typing.Optional[BaseExpr] = None


class RangeSubselect(PathRangeVar):
    """Subquery appearing in FROM clauses."""

    lateral: bool = False
    subquery: Query

    @property
    def query(self):
        return self.subquery


class RangeFunction(BaseRangeVar):

    lateral: bool = False
    # WITH ORDINALITY
    with_ordinality: bool = False
    # ROWS FROM form
    is_rowsfrom: bool = False
    functions: typing.List[FuncCall]


class JoinExpr(BaseRangeVar):

    # Type of join
    type: str

    # Left subtree
    larg: BaseExpr
    # Right subtree
    rarg: BaseExpr
    # USING clause, if any
    using_clause: typing.Optional[typing.List[BaseExpr]] = None
    # Qualifiers on join, if any
    quals: typing.Optional[BaseExpr] = None

    def copy(self):
        result = self.__class__()
        result.copyfrom(self)
        return result

    def copyfrom(self, other):
        self.larg = other.larg
        self.rarg = other.rarg
        self.quals = other.quals
        self.type = other.type


class SubLinkType(enum.IntEnum):
    EXISTS = enum.auto()
    NOT_EXISTS = enum.auto()
    ALL = enum.auto()
    ANY = enum.auto()


class SubLink(ImmutableBaseExpr):
    """Subselect appearing in an expression."""

    # Type of sublink
    type: SubLinkType
    # Sublink expression
    expr: BaseExpr
    # Sublink is never NULL
    nullable: bool = False


class RowExpr(ImmutableBaseExpr):
    """A ROW() expression."""

    # The fields.
    args: typing.List[BaseExpr]
    # Row expressions, while may contain NULLs, are not NULL themselves.
    nullable: bool = False


class ImplicitRowExpr(ImmutableBaseExpr):
    """A (a, b, c) expression."""

    # The fields.
    args: typing.Sequence[BaseExpr]
    # Row expressions, while may contain NULLs, are not NULL themselves.
    nullable: bool = False


class CoalesceExpr(ImmutableBaseExpr):
    """A COALESCE() expression."""

    # The arguments.
    args: typing.List[Base]


class NullTest(ImmutableBaseExpr):
    """IS [NOT] NULL."""

    # Input expression,
    arg: BaseExpr
    # NOT NULL?
    negated: bool = False
    # NullTest is never NULL
    nullable: bool = False


class CaseWhen(ImmutableBase):

    # Condition expression
    expr: BaseExpr
    # subsitution result
    result: BaseExpr


class CaseExpr(ImmutableBaseExpr):

    # Equality comparison argument
    arg: typing.Optional[BaseExpr] = None
    # List of WHEN clauses
    args: typing.List[CaseWhen]
    # ELSE clause
    defresult: typing.Optional[BaseExpr] = None


class GroupingOperation(Base):
    operation: typing.Optional[str] = None
    args: typing.List[Base]


SortAsc = qlast.SortAsc
SortDesc = qlast.SortDesc
SortDefault = qlast.SortDefault

NullsFirst = qlast.NonesFirst
NullsLast = qlast.NonesLast


class AlterSystem(ImmutableBaseExpr):

    name: str
    value: typing.Optional[BaseExpr]


class Set(ImmutableBaseExpr):

    name: str
    value: BaseExpr


class ConfigureDatabase(ImmutableBase):

    database_name: str
    parameter_name: str
    value: BaseExpr


class IteratorCTE(ImmutableBase):
    path_id: irast.PathId
    cte: CommonTableExpr
    parent: typing.Optional[IteratorCTE]
    is_dml_pseudo_iterator: bool = False
