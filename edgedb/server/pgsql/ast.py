##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import enum
import typing

from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.ir import ast as irast


# The structure of the nodes mostly follows that of Postgres'
# parsenodes.h and primnodes.h, but only with fields that are
# relevant to parsing and code generation.
#
# Certain nodes have EdgeDB-specific fields used by the
# compiler.


class Base(ast.AST):
    def __repr__(self):
        return f'<pg.{self.__class__.__name__} at 0x{id(self):x}>'


class _Ref(Base):

    node: Base

    def __repr__(self):
        return f'<pg.{self.__class__.__name__} -> {self.node!r}>'


class Alias(Base):
    """Alias for a range variable."""

    aliasname: str              # aliased relation name
    colnames: typing.List[str]  # optional list of column aliases


class Keyword(Base):
    """An SQL keyword that must be output without quoting."""

    name: str                   # Keyword name


class EdgeQLPathInfo(Base):
    """A general mixin providing EdgeQL-specific metadata on certain nodes."""

    # Ignore the below fields in AST visitor/transformer.
    __ast_meta__ = {'path_bonds'}

    # A subset of paths necessary to perform joining.
    path_bonds: typing.Set[irast.PathId]


class BaseRangeVar(Base):
    """Range variable, used in FROM clauses."""

    alias: Alias
    nullable: bool

    @property
    def path_id(self):
        return self.query.path_id

    @property
    def path_outputs(self):
        return self.query.path_outputs

    @property
    def path_namespace(self):
        return self.query.path_namespace

    @property
    def path_bonds(self):
        return self.query.path_bonds

    @property
    def inner_path_bonds(self):
        return self.query.inner_path_bonds


RangeTypes = typing.Union[BaseRangeVar, _Ref]


class BaseRelation(Base):
    pass


class Relation(BaseRelation, EdgeQLPathInfo):
    """Regular relation."""

    catalogname: str
    schemaname: str
    relname: str


class RangeVar(BaseRangeVar):
    """Relation range variable, used in FROM clauses."""

    relation: BaseRelation
    inhopt: bool

    @property
    def query(self):
        if isinstance(self.relation, CommonTableExpr):
            return self.relation.query
        else:
            return self.relation


class TypeName(Base):
    """Type in definitions and casts."""

    name: typing.Tuple[str, ...]    # Type name
    setof: bool                     # SET OF?
    typmods: list                   # Type modifiers
    array_bounds: list              # Array bounds


class Star(Base):
    """'*' representing all columns of a table or compound field."""


class ColumnRef(Base):
    """Specifies a reference to a column."""

    # Column name list.
    name: typing.List[typing.Union[str, Star]]
    # Whether NULL is possible.
    nullable: bool
    # Whether the col is grouped.
    grouped: bool
    # Whether the col is a weak path bond.
    weak: bool

    def __repr__(self):
        return (
            f'<pg.{self.__class__.__name__} '
            f'name={".".join(self.name)!r} at 0x{id(self):x}>'
        )


ColumnRefTypes = typing.Union[ColumnRef, _Ref]


class ParamRef(Base):
    """Query parameter ($1..$n)."""

    # Number of the parameter.
    number: int


class ResTarget(Base):
    """Query result target."""

    # Column name (optional)
    name: str
    # subscripts, field names and '*'
    indirection: list
    # value expression to compute
    val: Base


class UpdateTarget(Base):
    """Query update target."""

    # column name (optional)
    name: str
    # value expression to assign
    val: Base


class InferClause(Base):

    # IndexElems to infer unique index
    index_elems: list
    # Partial-index predicate
    where_clause: Base
    # Constraint name
    conname: str


class OnConflictClause(Base):

    action: str
    infer: InferClause
    target_list: list
    where: Base


class CommonTableExpr(BaseRelation):

    # Query name (unqualified)
    name: str
    # Optional list of column names
    aliascolnames: list
    # The CTE query
    query: Base
    # True if this CTE is recursive
    recursive: bool

    def __repr__(self):
        return (
            f'<pg.{self.__class__.__name__} '
            f'name={self.name!r} at 0x{id(self):x}>'
        )


class Query(BaseRelation, EdgeQLPathInfo):
    """Generic superclass representing a query."""

    # Ignore the below fields in AST visitor/transformer.
    __ast_meta__ = {'ptr_join_map', 'path_rvar_map', 'path_namespace',
                    'path_outputs', 'view_path_id_map', 'argnames'}

    # Map of RangeVars corresponding to pointer relations.
    ptr_join_map: dict
    # Map of RangeVars corresponding to paths.
    path_rvar_map: typing.Dict[irast.PathId, BaseRangeVar]
    # Map of col refs corresponding to paths.
    path_namespace: dict
    # Map of res target names corresponding to paths.
    path_outputs: dict

    argnames: typing.Dict[str, int]

    view_path_id_map: typing.Dict[irast.PathId, irast.PathId]

    ctes: typing.List[CommonTableExpr]


class DML(Base):
    """Generic superclass for INSERT/UPDATE/DELETE statements."""

    # Target relation to perform the operation on.
    relation: RangeTypes
    # List of expressions returned
    returning_list: typing.List[ResTarget]


class InsertStmt(Query, DML):

    # (optional) list of target column names
    cols: typing.List[ColumnRefTypes]
    # source SELECT/VALUES or None
    select_stmt: Query
    # ON CONFLICT clause
    on_conflict: OnConflictClause


class UpdateStmt(Query, DML):

    # The UPDATE target list
    targets: typing.List[UpdateTarget]
    # WHERE clause
    where_clause: Base
    # optional FROM clause
    from_clause: typing.List[RangeTypes]


class DeleteStmt(Query, DML):
    # WHERE clause
    where_clause: Base
    # optional USING clause
    using_clause: typing.List[RangeTypes]


class SelectStmt(Query):

    # List of DISTINCT ON expressions, empty list for DISTINCT ALL
    distinct_clause: list
    # The target list
    target_list: typing.List[ResTarget]
    # The FROM clause
    from_clause: typing.List[RangeTypes]
    # The WHERE clause
    where_clause: Base
    # GROUP BY clauses
    group_clause: typing.List[Base]
    # HAVING expression
    having: Base
    # WINDOW window_name AS(...),
    window_clause: typing.List[Base]
    # List of ImplicitRow's in a VALUES query
    values: typing.List[Base]
    # ORDER BY clause
    sort_clause: typing.List[Base]
    # OFFSET expression
    limit_offset: Base
    # LIMIT expression
    limit_count: Base
    # FOR UPDATE clause
    locking_clause: list

    # Set operation type
    op: str
    # ALL modifier
    all: bool
    # Left operand of set op
    larg: Query
    # Right operand of set op,
    rarg: Query


class ExprKind(enum.IntEnum):
    OP = enum.auto()


class Expr(Base):
    """Infix, prefix, and postfix expressions."""

    # Operator kind
    kind: ExprKind
    # Possibly-qualified name of operator
    name: str
    # Left argument, if any
    lexpr: Base
    # Right argument, if any
    rexpr: Base


class Constant(Base):
    """A literal constant."""

    # Constant value
    val: object


class LiteralExpr(Base):
    """A literal expression."""

    # Expression text
    expr: str


class TypeCast(Base):
    """A CAST expression."""

    # Expression being casted.
    arg: Base
    # Target type.
    type_name: TypeName


class CollateClause(Base):
    """A COLLATE expression."""

    # Input expression
    arg: Base
    # Possibly-qualified collation name
    collname: str


class FuncCall(Base):

    # Function name
    name: typing.Tuple[str, ...]
    # List of arguments
    args: typing.List[Base]
    # ORDER BY
    agg_order: typing.List[Base]
    # FILTER clause
    agg_filter: Base
    # Argument list is '*'
    agg_star: bool
    # Arguments were labeled DISTINCT
    agg_distinct: bool
    # OVER clause, if any
    over: Base
    # WITH ORDINALITY
    with_ordinality: bool = False  # noqa (pyflakes bug)


class Indices(Base):
    """Array subscript or slice bounds."""

    # True, if slice
    is_slice: bool
    # Lower bound, if any
    lidx: Base
    # Upper bound if any
    ridx: Base


class Indirection(Base):
    """Field and/or array element indirection."""

    # Indirection subject
    arg: Base
    # Subscripts and/or field names and/or '*'
    indirection: list


class ArrayExpr(Base):
    """ARRAY[] construct."""

    # array element expressions
    elements: typing.List[Base]


class MultiAssignRef(Base):
    """UPDATE (a, b, c) = row-valued-expr."""

    # row-valued expression
    source: Base
    # list of columns to assign to
    columns: typing.List[ColumnRefTypes]


class SortBy(Base):
    """ORDER BY clause element."""

    # expression to sort on
    node: Base
    # ASC/DESC/USING/default
    dir: qlast.SortOrder
    # NULLS FIRST/LAST
    nulls: qlast.NonesOrder


class WindowDef(Base):
    """WINDOW and OVER clauses."""

    # window name
    name: str
    # referenced window name, if any
    refname: str
    # PARTITION BY expr list
    partition_clause: typing.List[Base]
    # ORDER BY
    order_clause: typing.List[SortBy]
    # Window frame options
    frame_options: list
    # expression for starting bound, if any
    start_offset: Base
    # expression for ending ound, if any
    end_offset: Base


class RangeSubselect(BaseRangeVar):
    """Subquery appearing in FROM clauses."""

    lateral: bool
    subquery: BaseRelation

    @property
    def query(self):
        if isinstance(self.subquery, CommonTableExpr):
            return self.subquery.query
        else:
            return self.subquery


class ColumnDef(Base):

    # name of column
    name: str
    # type of column
    typename: TypeName
    # default value, if any
    default_expr: Base  # noqa (pyflakes bug)
    # COLLATE clause, if any
    coll_clause: Base


class RangeFunction(BaseRangeVar):

    lateral: bool
    ordinality: Base
    is_rowsfrom: bool
    functions: typing.List[FuncCall]
    # list of ColumnDef nodes to describe result of
    # the function returning RECORD.
    coldeflist: typing.List[ColumnDef]


class JoinExpr(BaseRangeVar):

    # Type of join
    type: str

    # Left subtree
    larg: Base
    # Right subtree
    rarg: Base
    # USING clause, if any
    using_clause: typing.List[Base]
    # Qualifiers on join, if any
    quals: Base

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
    ALL = enum.auto()
    ANY = enum.auto()


class SubLink(Base):
    """Subselect appearing in an expression."""

    # Type of sublink
    type: SubLinkType
    # Sublink expression
    expr: Base


class RowExpr(Base):
    """A ROW() expression."""

    # The fields.
    args: typing.List[Base]


class ImplicitRowExpr(Base):
    """A (a, b, c) expression."""

    # The fields.
    args: typing.List[Base]


class CoalesceExpr(Base):
    """A COALESCE() expression."""

    # The arguments.
    args: typing.List[Base]


class NullTest(Base):
    """IS [NOT] NULL."""

    # Input expression,
    arg: Base
    # NOT NULL?
    negated: bool


class CaseWhen(Base):

    # Condition expression
    expr: Base
    # subsitution result
    result: Base


class CaseExpr(Base):

    # Equality comparison argument
    arg: Base
    # List of WHEN clauses
    args: typing.List[CaseWhen]
    # ELSE clause
    defresult: Base  # noqa (pyflakes bug)


class PgSQLOperator(ast.ops.Operator):
    pass


class PgSQLComparisonOperator(PgSQLOperator, ast.ops.ComparisonOperator):
    pass


LIKE = PgSQLComparisonOperator('~~')
NOT_LIKE = PgSQLComparisonOperator('!~~')
ILIKE = PgSQLComparisonOperator('~~*')
NOT_ILIKE = PgSQLComparisonOperator('!~~*')

SIMILAR_TO = PgSQLComparisonOperator('~')
NOT_SIMILAR_TO = PgSQLComparisonOperator('!~')

IS_DISTINCT = PgSQLComparisonOperator('IS DISTINCT')
IS_NOT_DISTINCT = PgSQLComparisonOperator('IS NOT DISTINCT')
IS_OF = PgSQLComparisonOperator('IS OF')
IS_NOT_OF = PgSQLComparisonOperator('IS NOT OF')


class PgSQLSetOperator(PgSQLOperator):
    pass


UNION = PgSQLSetOperator('UNION')
INTERSECT = PgSQLSetOperator('INTERSECT')
EXCEPT = PgSQLSetOperator('EXCEPT')

SortAsc = qlast.SortAsc
SortDesc = qlast.SortDesc
SortDefault = qlast.SortDefault

NullsFirst = qlast.NonesFirst
NullsLast = qlast.NonesLast
