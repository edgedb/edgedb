##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from . import ast as tast


class TestUtilsASTBase:
    def test_utils_ast_copy(self):
        import copy

        lconst = tast.Constant(value='foo')
        tree1 = tast.BinOp(left=lconst)
        ctree11 = copy.copy(tree1)

        assert ctree11 is not tree1
        assert ctree11.left is lconst

        ctree12 = copy.deepcopy(tree1)
        assert ctree12 is not tree1
        assert ctree12.left is not lconst
        assert ctree12.left.value == lconst.value

        class Dict(tast.Base):
            __fields = [('node', dict)]

        tree2 = tast.BinOp(
            left=tast.FunctionCall(args=[Dict(node={'lconst': lconst})]))

        ctree21 = copy.copy(tree2)
        assert ctree21 is not tree2
        assert ctree21.left.args[0].node['lconst'] is lconst

        ctree22 = copy.deepcopy(tree2)
        assert ctree22 is not tree2
        assert ctree22.left.args[0].node['lconst'] is not lconst
        assert ctree22.left.args[0].node['lconst'].value == lconst.value
