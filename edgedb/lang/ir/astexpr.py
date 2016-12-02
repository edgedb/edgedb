##
# Copyright (c) 2013-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.common.ast import match as astmatch

from . import astmatch as irastmatch


class DistinctConjunctionExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            # Basic std::is_distinct(blah) expression
            distinct_expr = irastmatch.FunctionCall(
                name=('std', 'is_distinct'),
                args=[astmatch.group('expr', irastmatch.Base())]
            )

            # A logical conjunction of unique constraint expressions
            binop = irastmatch.BinOp(op=ast.ops.AND)

            # Set expression with the above binop
            set_expr = irastmatch.Set(
                expr=astmatch.Or(
                    distinct_expr, binop
                )
            )

            # A unique constraint expression can be either one of the
            # three above
            constr_expr = astmatch.Or(
                distinct_expr, binop, set_expr
            )

            # Populate expression alternatives to complete recursive
            # pattern definition.
            binop.left = binop.right = constr_expr

            self.pattern = constr_expr

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return [mg.node for mg in m.expr]
        else:
            return None
