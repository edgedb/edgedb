##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import ast
from edgedb.lang.common.ast import match

from . import ast as tast
from . import astmatch as tastmatch


class TestUtilsASTMatch:
    tree1 = tast.BinOp(
        left=tast.BinOp(
                left=tast.FunctionCall(
                        name='test',
                        args=[
                           tast.BinOp(
                             left=tast.Constant(value=1),
                             op='+',
                             right=tast.Constant(value=2)
                           ),

                           tast.Constant(value='test'),
                        ]
                     ),
                op='*',
                right=tast.BinOp(
                        left=tast.Constant(value=1),
                        op='-',
                        right=tast.FunctionCall(name='test1')
                      )
             ),
        op='-',
        right=tast.Constant(value='value')
    )


    pat1 = tastmatch.BinOp(
        left=tastmatch.BinOp(
                left=tastmatch.FunctionCall(
                        name='test',
                        args=[
                           tastmatch.BinOp(
                             left=tastmatch.Constant(value=1),
                             op='+',
                             right=tastmatch.Constant(value=2)
                           ),

                           tastmatch.Constant(value='test')
                        ]
                     ),
                op='*',
                right=tastmatch.BinOp(
                        left=tastmatch.Constant(value=1),
                        op='-',
                        right=tastmatch.FunctionCall(name='test1')
                      )
             ),
        op='-',
        right=tastmatch.Constant(value='value')
    )


    pat2 = tastmatch.Constant(value='test')


    pat3 = tastmatch.BinOp(
        left=tastmatch.BinOp(
                left=tastmatch.FunctionCall(
                        name='test',
                        args=[
                           tastmatch.BinOp(
                             left=match.group('important_constant', tastmatch.Constant()),
                             op='+',
                             right=tastmatch.Constant()
                           ),

                           tastmatch.Constant()
                        ]
                     ),
                op='*',
                right=tastmatch.BinOp(
                        left=tastmatch.Constant(),
                        op='-',
                        right=tastmatch.FunctionCall()
                      )
             ),
        op='-',
        right=match.group('important_constant2', tastmatch.Constant())
    )


    binop = tastmatch.BinOp(
        op='+',
        right=match.group('recursive', tastmatch.Constant())
    )

    binop.left=match.Or(binop, tastmatch.Constant())

    pat4 = match.Or(match.group('recursive', tastmatch.Constant()), binop)

    tree2 = tast.BinOp(
        left=tast.BinOp(
            left=tast.BinOp(
                left=tast.BinOp(
                    left=tast.Constant(value=1),
                    op='+',
                    right=tast.Constant(value=2),
                ),
                op='+',
                right=tast.Constant(value=3),
            ),
            op='+',
            right=tast.Constant(value=4),
        ),
        op='+',
        right=tast.Constant(value=5),
    )


    tree3 = tast.BinOp(
        left=tast.BinOp(
            left=tast.Constant(value=1),
            op='+',
            right=tast.Constant(value=2),
        ),
        op='-',
        right=tast.Constant(value=3),
    )

    tree4 = tast.Constant(value='one and only')


    def test_utils_ast_match(self):
        assert match.match(self.pat1, self.tree1)
        assert not match.match(self.pat2, self.tree1)

        result = match.match(self.pat3, self.tree1)
        assert result.important_constant[0].node.value == 1
        assert result.important_constant2[0].node.value == 'value'

        result = match.match(self.pat4, self.tree2)
        assert sorted([c.node.value for c in result.recursive]) == [2, 3, 4, 5]

        # tree3 won't match because pat4 wants '+' op everywhere
        assert not match.match(self.pat4, self.tree3)

        # but the single constant matches just fine
        result = match.match(self.pat4, self.tree4)
        assert result and result.recursive[0].node.value == 'one and only'
