##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos import proto

from metamagic.utils import ast
from metamagic.utils.ast import match as astmatch

from . import ast as caos_ast
from  . import astmatch as caos_astmatch


class ExistsConjunctionExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            # Basic NOT EXISTS (blah) expression
            nex_expr = caos_astmatch.UnaryOp(
                expr = caos_astmatch.ExistPred(
                    expr = astmatch.group('expr', caos_astmatch.Base())
                ),

                op = ast.ops.NOT
            )

            # A logical conjunction of unique constraint expressions
            binop = caos_astmatch.BinOp(op=ast.ops.AND)

            # A RefExpr node containing an unique constraint expression
            refexpr = caos_astmatch.BaseRefExpr()

            # A unique constraint expression can be either one of the
            # three above
            constr_expr = astmatch.Or(
                nex_expr, binop, refexpr
            )

            # Populate expression alternatives to complete recursive
            # pattern definition.
            binop.left = binop.right = refexpr.expr = constr_expr

            self.pattern = constr_expr

        return self.pattern

    def match(self, tree):
        m = astmatch.match(self.get_pattern(), tree)
        if m:
            return [mg.node for mg in m.expr]
        else:
            return None

