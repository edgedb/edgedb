##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import ast
from edgedb.lang.common.ast import match as astmatch

from . import ast as irast
from . import astmatch as irastmatch


class ExistsConjunctionExpr:
    def __init__(self):
        self.pattern = None

    def get_pattern(self):
        if self.pattern is None:
            # Basic NOT EXISTS (blah) expression
            nex_expr = irastmatch.UnaryOp(
                expr=irastmatch.ExistPred(
                    expr=astmatch.Or(
                        irastmatch.SubgraphRef(
                            ref=irastmatch.GraphExpr(
                                selector=[
                                    irastmatch.SelectorExpr(
                                        expr=astmatch.group(
                                            'expr', irastmatch.Base())
                                    )
                                ]
                            )
                        ),
                        astmatch.group('expr', irastmatch.Base()),
                    )
                ),

                op=ast.ops.NOT
            )

            # A logical conjunction of unique constraint expressions
            binop = irastmatch.BinOp(op=ast.ops.AND)

            # A RefExpr node containing an unique constraint expression
            refexpr = irastmatch.BaseRefExpr()

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
