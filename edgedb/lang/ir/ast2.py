##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import ast, parsing

from edgedb.lang.schema import objects as so

from edgedb.lang.edgeql import ast as qlast


class ASTError(EdgeDBError):
    pass


class Base(ast.AST):
    __fields = [
        # Pointer to an original node replaced by this node during rewrites
        ('rewrite_original', object, None, False, False),
        # Whether or not the node is a product of a rewrite
        ('is_rewrite_product', bool, False),
        ('rewrite_flags', set),
        ('as_type', so.Class, None),
        ('context', parsing.ParserContext, None, True, None, True)
    ]


class Stmt(Base):
    __fields = [
        'result',
        ('substmts', list),
        'name'
    ]


class SubstmtRef(Base):
    __fields = [
        ('stmt', Stmt),
        'rptr'
    ]


class SelectStmt(Stmt):
    __fields = [
        'where',
        ('groupby', list),
        ('orderby', list),
        'offset',
        'limit',
        'set_op',
        'set_op_larg',
        'set_op_rarg',
    ]


class MutatingStmt(Stmt):
    __fields = [
        'shape'
    ]


class InsertStmt(MutatingStmt):
    pass


class UpdateStmt(MutatingStmt):
    __fields = [
        'where'
    ]


class DeleteStmt(MutatingStmt):
    __fields = [
        'where'
    ]


class Pointer(Base):
    __fields = [
        'source',
        'target',
        'ptrcls',
        'direction',
        'anchor',
        'show_as_anchor'
    ]


class Set(Base):
    __fields = [
        'path_id',
        ('scls', so.NodeClass),
        ('sources', set),
        ('source_conjunction', bool, False),
        'expr',
        'rptr',
        'reference',
        'pathvar',
        'anchor',
        'show_as_anchor'
    ]


class Constant(Base):
    __fields = ['value', 'index', 'expr', 'type', 'substitute_for']

    def __init__(self, **kwargs):
        self._check_type(kwargs.get('expr'), kwargs.get('type'))
        super().__init__(**kwargs)

    def __setattr__(self, name, value):
        if name in ('expr', 'type'):
            expr = value if name == 'expr' else self.expr
            type = value if name == 'value' else self.type
            self._check_type(expr, type)

    def _check_type(self, expr, type):
        if type:
            if isinstance(type, tuple):
                item_type = type[1]
            else:
                item_type = type

            if not isinstance(item_type, (so.Class, so.MetaClass)):
                raise ASTError('unexpected constant type representation, '
                               'expected Class, got {!r}'.format(type))


class Expr(Base):
    pass


class Shape(Expr):
    __fields = [
        ('elements', list),
        'scls',
        ('rptr', Pointer, None),
    ]


class Sequence(Expr):
    __fields = [
        ('elements', list),
        ('is_array', bool)
    ]


class BinOp(Expr):
    __fields = [
        'left',
        'right',
        'op'
    ]


class UnaryOp(Expr):
    __fields = [
        'expr',
        'op'
    ]


class NoneTest(Expr):
    __fields = [
        'expr'
    ]


class ExistPred(Expr):
    __fields = [
        'expr'
    ]


class SortExpr(Base):
    __fields = [
        'expr',
        'direction',
        ('nones_order', qlast.NonesOrder, None)
    ]


class FunctionCall(Expr):
    __fields = [
        'name',
        'result_type',
        ('args', list),
        ('kwargs', dict),
        ('aggregate', bool),
        ('window', bool),
        ('agg_sort', list),
        'agg_filter',
        ('partition', list)
    ]


class IndexIndirection(Expr):
    __fields = ['expr', 'index']


class SliceIndirection(Expr):
    __fields = ['expr', 'start', 'stop', 'step']


class TypeCast(Expr):
    __fields = ['expr', 'type']


class CompositeType(Base):
    __fields = ['node', 'pathspec']


class TypeRef(Expr):
    __fields = ['maintype', 'subtypes']


TextSearchOperator = qlast.TextSearchOperator
EdgeDBMatchOperator = qlast.EdgeQLMatchOperator
