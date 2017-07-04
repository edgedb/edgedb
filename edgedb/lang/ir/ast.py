##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import typing

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import ast, parsing

from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as so
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import sources as s_sources

from edgedb.lang.edgeql import ast as qlast


class ASTError(EdgeDBError):
    pass


class PathId(tuple):
    """Unique identifier of a path in an expression."""

    def __new__(cls, t=()):
        self = super().__new__(cls, t)
        if not self:
            return self

        if not isinstance(self[0], so.NodeClass):
            raise ValueError(f'invalid PathId: bad source: {self[0]!r}')

        for i in range(1, len(self) - 1, 2):
            if not isinstance(self[i], tuple):
                raise ValueError('invalid PathId: bad ptr spec')
            ptr = self[i][0]
            if not isinstance(ptr, s_pointers.Pointer):
                raise ValueError('invalid PathId: bad ptr')
            ptrdir = self[i][1]
            if not isinstance(ptrdir, s_pointers.PointerDirection):
                raise ValueError('invalid PathId: bad dir')

            try:
                tgt = self[i + 1]
            except IndexError:
                # There may be no target for link PathIds
                pass
            else:
                if not isinstance(tgt, so.NodeClass):
                    raise ValueError(f'invalid PathId: bad target: {tgt!r}')

        return self

    def rptr(self, schema, near_endpoint=None):
        if len(self) > 1:
            if isinstance(self[-1], so.NodeClass):
                ptroffset = -2
            else:
                ptroffset = -1

            genptr = self[ptroffset][0]
            direction = self[ptroffset][1]
            if isinstance(genptr, s_lprops.LinkProperty):
                src = PathId(self[:ptroffset]).rptr(schema)
                tgt = None
            else:
                if near_endpoint is None:
                    near_endpoint = self[ptroffset - 1]

                if direction == s_pointers.PointerDirection.Outbound:
                    src = near_endpoint
                    if isinstance(self[-1], so.NodeClass):
                        tgt = self[-1]
                    else:
                        tgt = None
                else:
                    if isinstance(self[-1], so.NodeClass):
                        src = self[-1]
                    else:
                        src = None
                    tgt = near_endpoint

            if isinstance(src, s_sources.Source):
                if src.bases and src.bases[0].is_virtual:
                    # View sets may involve dynamic concepts inherited
                    # from a virtual parent (e.g. a view on UNION).
                    src = src.bases[0]
                return src.resolve_pointer(
                    schema, genptr.name,
                    far_endpoint=tgt, look_in_children=True,
                    update_schema=False)
            else:
                return None
        else:
            return None

    def rptr_dir(self):
        if len(self) > 1:
            if isinstance(self[-1], so.NodeClass):
                ptroffset = -2
            else:
                ptroffset = -1
            return self[ptroffset][1]
        else:
            return None

    def src_path(self):
        if len(self) > 1:
            if isinstance(self[-1], so.NodeClass):
                ptroffset = -2
            else:
                ptroffset = -1
            return self[:ptroffset]
        else:
            return None

    def iter_prefixes(self):
        yield self.__class__(self[:1])

        for i in range(1, len(self) - 1, 2):
            if self[i + 1]:
                yield self.__class__(self[:i + 2])
            else:
                break

    def starts_any_of(self, scope):
        for path_id in scope:
            if path_id.startswith(self):
                return True
        else:
            return False

    def is_in_scope(self, scope):
        for path_id in scope:
            if self.startswith(path_id):
                return True
        else:
            return False

    def is_concept_path(self):
        return isinstance(self[-1], s_concepts.Concept)

    def startswith(self, path_id):
        return self[:len(path_id)] == path_id

    def common_suffix_len(self, other):
        suffix_len = 0

        for i in range(min(len(self), len(other)), 0, -1):
            if self[i - 1] != other[i - 1]:
                break
            else:
                suffix_len += 1

        return suffix_len

    def replace_prefix(self, prefix, replacement):
        if self.startswith(prefix):
            if len(prefix) < len(self):
                return replacement + tuple(self)[len(prefix):]
            else:
                return replacement
        else:
            return self

    def extend(self, link, direction, target):
        if not self:
            raise ValueError('cannot extend empty PathId')

        if not link.generic():
            link = link.bases[0]

        if not isinstance(self[-1], so.NodeClass):
            raise ValueError('cannot extend link PathId')

        return self + (self[-1], (link, direction), target)

    def __add__(self, other):
        if (self and isinstance(self[-1], so.NodeClass) and
                other and isinstance(other[0], so.NodeClass)):
            return self[:-1] + other
        else:
            return self.__class__(super().__add__(other))

    def __str__(self):
        if not self:
            return ''

        result = f'({self[0].name})'

        for i in range(1, len(self) - 1, 2):
            ptr = self[i][0]
            ptrdir = self[i][1]
            tgt = self[i + 1]

            if tgt:
                lexpr = f'({ptr.name} [IS {tgt.name}])'
            else:
                lexpr = f'({ptr.name})'

            if isinstance(ptr, s_lprops.LinkProperty):
                step = '@'
            else:
                step = f'.{ptrdir}'

            result += f'{step}{lexpr}'

        if len(self) == 2:
            ptr = self[1][0]
            ptrdir = self[1][1]
            result += f'.{ptrdir}({ptr.name})'

        return result

    def __getitem__(self, n):
        res = super().__getitem__(n)
        if isinstance(n, slice):
            res = self.__class__(res)
        return res

    __repr__ = __str__


class Base(ast.AST):

    __ast_hidden__ = {'context'}

    context: parsing.ParserContext

    def __repr__(self):
        return (
            f'<ir.{self.__class__.__name__} at 0x{id(self):x}>'
        )


class Pointer(Base):

    source: Base
    target: Base
    ptrcls: so.Class
    direction: str
    anchor: str
    show_as_anchor: str


class Set(Base):

    path_id: PathId
    real_path_id: PathId
    scls: so.NodeClass
    source: Base
    view_source: Base
    expr: Base
    rptr: Pointer
    anchor: str
    show_as_anchor: str
    shape: typing.List[Base]
    path_scope: typing.Dict[PathId, int]
    specific_path_scope: typing.Set[Base]

    def __repr__(self):
        return \
            f'<ir.Set \'{self.path_id or self.scls.name}\' at 0x{id(self):x}>'


class Expr(Base):
    pass


class EmptySet(Base):
    pass


class Constant(Expr):

    value: object
    type: so.NodeClass

    def __init__(self, *args, type, **kwargs):
        if type is None:
            raise ValueError('type argument must not be None')
        super().__init__(*args, type=type, **kwargs)


class Parameter(Base):

    name: str
    type: so.NodeClass


class TupleElement(Base):

    name: str
    val: Base


class Tuple(Expr):
    named: bool = False
    elements: typing.List[TupleElement]


class Array(Expr):

    elements: typing.List[Base]


class Mapping(Expr):

    keys: typing.List[Base]
    values: typing.List[Base]


class SetOp(Expr):
    left: Base
    right: Base
    op: ast.ops.Operator
    exclusive: bool = False


class BinOp(Expr):

    left: Base
    right: Base
    op: ast.ops.Operator


class UnaryOp(Expr):

    expr: Base
    op: ast.ops.Operator


class ExistPred(Expr):

    expr: Base
    negated: bool = False


class IfElseExpr(Expr):

    condition: Base
    if_expr: Base  # noqa (pyflakes bug)
    else_expr: Base  # noqa (pyflakes bug)
    singleton: bool = False


class Coalesce(Base):
    args: typing.List[Base]


class SortExpr(Base):

    expr: Base
    direction: str
    nones_order: qlast.NonesOrder


class FunctionCall(Expr):

    func: so.Class
    args: typing.List[Base]
    kwargs: dict
    agg_sort: typing.List[SortExpr]
    agg_filter: Base
    agg_set_modifier: qlast.SetModifier
    partition: typing.List[Base]
    window: bool
    initial_value: Base


class TupleIndirection(Expr):

    expr: Base
    name: str
    path_id: PathId


class IndexIndirection(Expr):

    expr: Base
    index: Base


class SliceIndirection(Expr):

    expr: Base
    start: Base
    stop: Base
    step: Base


class TypeRef(Expr):

    maintype: str
    subtypes: typing.List[sn.Name]


class TypeCast(Expr):
    """<Type>Expr"""

    expr: Base
    type: TypeRef


class TypeFilter(Expr):
    """Expr[IS Type]"""

    path_id: PathId
    expr: Base
    type: TypeRef


class CompositeType(Base):

    node: so.Class
    shape: list


class Stmt(Base):

    result: Base
    singleton: bool
    main_stmt: Base
    parent_stmt: Base
    substmts: list
    path_scope: typing.Dict[PathId, int]
    specific_path_scope: typing.Set[Set]


class SelectStmt(Stmt):

    where: Base
    orderby: typing.List[SortExpr]
    offset: Base
    limit: Base


class GroupStmt(Stmt):
    subject: Base
    groupby: typing.List[Base]
    result: SelectStmt
    group_path_id: PathId


class MutatingStmt(Stmt):
    subject: Set


class InsertStmt(MutatingStmt):
    pass


class UpdateStmt(MutatingStmt):

    where: Base


class DeleteStmt(MutatingStmt):

    where: Base


TextSearchOperator = qlast.TextSearchOperator
EdgeDBMatchOperator = qlast.EdgeQLMatchOperator
EquivalenceOperator = qlast.EquivalenceOperator
SetOperator = qlast.SetOperator
SetModifier = qlast.SetModifier
