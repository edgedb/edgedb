##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import enum

from edgedb.lang.common import ast
from edgedb.lang.edgeql import ast as qlast


# The structure of the nodes mostly follows that of Postgres'
# parsenodes.h and primnodes.h, but only with fields that are
# relevant to parsing and code generation.
#
# Certain nodes have EdgeDB-specific fields used by the
# compiler.


class Base(ast.AST):
    pass


class Alias(Base):
    """Alias for a range variable."""

    __fields = [
        'aliasname',            # aliased relation name
        ('colnames', list)      # optional list of column aliases
    ]


class EdgeQLPathInfo:
    """A general mixin providing EdgeQL-specific metadata on certain nodes."""

    __fields = [
        'path_id',              # The ID of EdgeQL path expression
                                # that this RangeVar represents
        ('path_vars', dict),    # A mapping between value path-ids and
                                # columns reachable via this RangeVar.
        ('path_bonds', dict),   # A subset of path_cols with paths
                                # necessary to perform joining.
    ]


class BaseRangeVar(Base):
    """Range variable, used in FROM clauses."""

    __fields = [
        ('alias', Alias, None),
    ]


class Relation(Base, EdgeQLPathInfo):
    """Regular relation."""

    __fields = [
        'catalogname',
        'schemaname',
        'relname',
    ]


class RangeVar(BaseRangeVar):
    """Relation range variable, used in FROM clauses."""

    __fields = [
        ('relation', Base),
        'inhopt'
    ]

    @property
    def query(self):
        if isinstance(self.relation, CommonTableExpr):
            return self.relation.query
        else:
            return self.relation

    @property
    def path_id(self):
        return self.query.path_id

    @property
    def path_vars(self):
        return self.query.path_vars

    @property
    def path_bonds(self):
        return self.query.path_bonds


class TypeName(Base):
    """Type in definitions and casts."""

    __fields = [
        'name',                     # Type name
        'setof',                    # SET OF?
        ('typmods', list),          # Type modifiers
        ('array_bounds', list),     # Array bounds
    ]


class InferClause(Base):
    __fields = [
        ('index_elems', list),    # IndexElems to infer unique index
        ('where_clause', Base),   # Partial-index predicate
        'conname'                 # Constraint name
    ]


class OnConflictClause(Base):
    __fields = [
        'action',
        'infer',
        ('target_list', list),
        'where'
    ]


class CommonTableExpr(Base):
    __fields = [
        'name',                     # Query name (unqualified)
        'aliascolnames',            # Optional list of column names
        'query',                    # The CTE query
        'recursive',                # True if this CTE is recursive
    ]


class Query(Base, EdgeQLPathInfo):
    """Generic superclass representing a query."""

    __fields = [
        ('ctes', list),             # list of CommonTableExpr's

        ('path_namespace', dict),   # A map of paths onto Vars visible in this
                                    # Query.

        ('ptr_rvar_map', dict),     # Map of RangeVars corresponding to pointer
                                    # relations.

        'scls_rvar',
        'rptr_rvar'
    ]


class DML(Base):
    """Generic superclass for INSERT/UPDATE/DELETE statements."""

    __fields = [
        ('relation', RangeVar),   # Target relation to perform the operation on
        ('returning_list', list)  # List of expressions returned
    ]


class InsertStmt(Query, DML):
    __fields = [
        ('cols', list),         # (optional) list of target column names
        'select_stmt',          # source SELECT/VALUES or None
        ('on_conflict', OnConflictClause, None),  # ON CONFLICT clause
    ]


class UpdateStmt(Query, DML):
    __fields = [
        ('targets', list),      # The UPDATE target list
        'where_clause',         # WHERE clause
        'from_clause',          # optional FROM clause
    ]


class DeleteStmt(Query, DML):
    __fields = [
        'where_clause',         # WHERE clause
        'using_clause',         # optional USING clause
    ]


class SelectStmt(Query):
    __fields = [
        ('distinct_clause', list, None),  # List of DISTINCT ON expressions,
                                          # empty list for DISTINCT ALL
        ('target_list', list),            # The target list
        ('from_clause', list),            # The FROM clause
        ('where_clause', Base, None),     # The WHERE clause
        ('group_clause', list),           # GROUP BY clauses
        ('having', Base, None),           # HAVING expression
        ('window_clause', list),          # WINDOW window_name AS(...),
        ('values', list),                 # List of ImplicitRow's in
                                          # a VALUES query

        ('sort_clause', list),            # ORDER BY clause
        ('limit_offset', Base, None),     # OFFSET expression
        ('limit_count', Base, None),      # LIMIT expression
        ('locking_clause', list),         # FOR UPDATE clause

        'op',                             # Set operation type
        'all',                            # ALL modifier
        'larg',                           # Left operand of set op
        'rarg',                           # Right operand of set op,
    ]


class ColumnRef(Base):
    """Specifies a reference to a column."""

    __fields = [
        ('name', list)  # Column name list
    ]


class ParamRef(Base):
    """Query parameter ($1..$n)."""

    __fields = [
        'number'        # Number of the parameter
    ]


class ExprKind(enum.IntEnum):
    OP = enum.auto()


class Expr(Base):
    """Infix, prefix, and postfix expressions."""

    __fields = [
        'kind',         # Operator kind
        'name',         # Possibly-qualified name of operator
        'lexpr',        # Left argument, if any
        'rexpr',        # Right argument, if any
    ]


class Constant(Base):
    """A literal constant."""

    __fields = [
        'val'           # Constant value
    ]


class LiteralExpr(Base):
    """A literal expression."""

    __fields = [
        'expr'          # Expression text
    ]


class TypeCast(Base):
    """A CAST expression."""

    __fields = [
        'arg',                      # Expression being casted
        ('type_name', TypeName),    # Target type
    ]


class CollateClause(Base):
    """A COLLATE expression."""

    __fields = [
        'arg',                      # Input expression
        'collname',                 # Possibly-qualified collation name
    ]


class FuncCall(Base):
    __fields = [
        'name',                     # Function name
        ('args', list),             # List of arguments
        ('agg_order', list),        # ORDER BY
        ('agg_filter', Base, None),  # FILTER clause
        'agg_star',                 # Argument list is '*'
        'agg_distinct',             # Arguments were labeled DISTINCT
        'over'                      # OVER clause, if any
    ]


class Star(Base):
    """'*' representing all columns of a table or compound field."""


class Indices(Base):
    """Array subscript or slice bounds."""

    __fields = [
        'is_slice',     # True, if slice
        'lidx',         # Lower bound, if any
        'ridx'          # Upper bound if any
    ]


class Indirection(Base):
    """Field and/or array element indirection."""

    __fields = [
        'arg',                  # Indirection subject
        ('indirection', list)   # Subscripts and/or field names and/or '*'
    ]


class ArrayExpr(Base):
    """ARRAY[] construct."""

    __fields = [
        ('elements', list)      # array element expressions
    ]


class ResTarget(Base):
    """Query result target."""

    __fields = [
        'name',                 # column name (optional)
        ('indirection', list),  # subscripts, field names and '*'
        'val',                  # value expression to compute
    ]


class UpdateTarget(Base):
    """Query update target."""

    __fields = [
        'name',                 # column name (optional)
        'val',                  # value expression to assign
    ]


class MultiAssignRef(Base):
    """UPDATE (a, b, c) = row-valued-expr."""

    __fields = [
        'source',               # row-valued expression
        ('columns', list),      # list of columns to assign to
    ]


class SortBy(Base):
    """ORDER BY clause element."""

    __fields = [
        'node',                 # expression to sort on
        'dir',                  # ASC/DESC/USING/default
        'nulls'                 # NULLS FIRST/LAST
    ]


class WindowDef(Base):
    """WINDOW and OVER clauses."""

    __fields = [
        'name',                 # window name
        'refname',              # referenced window name, if any
        ('partition_clause', list),  # PARTITION BY expr list
        ('order_clause', list),      # ORDER BY
        'frame_options',        # Window frame options
        'start_offset',         # expression for starting bound, if any
        'end_offset',           # expression for ending ound, if any
    ]


class RangeSubselect(BaseRangeVar):
    """Subquery appearing in FROM clauses."""

    __fields = [
        'lateral',
        'subquery'
    ]


class RangeFunction(BaseRangeVar):
    __fields = [
        'lateral',
        'ordinality',
        'is_rowsfrom',
        'functions',
        ('coldeflist', list)    # list of ColumnDef nodes to describe result
                                # of the function returning RECORD.
    ]


class ColumnDef(Base):
    __fields = [
        'name',                     # name of column
        ('typename', TypeName, None),     # type of column
        'default_expr',             # default value, if any
        'coll_clause',              # COLLATE clause, if any
    ]


class JoinExpr(BaseRangeVar):
    __fields = [
        'type',                     # Type of join
        'larg',                     # Left subtree
        'rarg',                     # Right subtree
        ('using_clause', list),     # USING clause, if any
        'quals',                    # Qualifiers on join, if any
    ]

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

    __fields = [
        'type',                     # Type of sublink
        'testexpr',                 # outer-query test ofr ALL/ANY/ROWCOMPARE
        'subselect',                # subselect
    ]


class RowExpr(Base):
    """A ROW() expression."""

    __fields = [
        ('args', list)              # The fields.
    ]


class ImplicitRowExpr(Base):
    """A (a, b, c) expression."""

    __fields = [
        ('args', list)              # The fields.
    ]


class CoalesceExpr(Base):
    """A COALESCE() expression."""

    __fields = [
        ('args', list)              # The arguments.
    ]


class NullTest(Base):
    """IS [NOT] NULL."""

    __fields = [
        'arg',                      # Input expression,
        'negated'                   # NOT NULL?
    ]


class CaseExpr(Base):
    __fields = [
        'arg',                      # Equality comparison argument
        ('args', list),             # List of WHEN clauses
        'defresult',                # ELSE clause
    ]


class CaseWhen(Base):
    __fields = [
        'expr',                     # Condition expression
        'result'                    # subsitution result
    ]


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
