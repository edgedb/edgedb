##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import copy
import unittest

from edgedb.lang.common import ast
from edgedb.lang.common.ast import match


class tast:

    class Base(ast.AST):
        pass

    class BinOp(Base):
        __fields = ['op', 'left', 'right']

    class UnaryOp(Base):
        __fields = ['op', 'operand']

    class FunctionCall(Base):
        __fields = ['name', ('args', list)]

    class Constant(Base):
        __fields = ['value']


class tastmatch:

    for name, cls in tast.__dict__.items():
        if isinstance(cls, type) and issubclass(cls, ast.AST):
            adapter = match.MatchASTMeta(
                name, (match.MatchASTNode, ), {'__module__': __name__},
                adapts=cls)
            locals()[name] = adapter


class ASTBaseTests(unittest.TestCase):

    def test_common_ast_copy(self):
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


class ASTMatchTests(unittest.TestCase):
    tree1 = tast.BinOp(
        left=tast.BinOp(
            left=tast.FunctionCall(
                name='test', args=[
                    tast.BinOp(
                        left=tast.Constant(value=1), op='+',
                        right=tast.Constant(value=2)),
                    tast.Constant(value='test'),
                ]), op='*', right=tast.BinOp(
                    left=tast.Constant(value=1), op='-',
                    right=tast.FunctionCall(name='test1'))), op='-',
        right=tast.Constant(value='value'))

    pat1 = tastmatch.BinOp(
        left=tastmatch.BinOp(
            left=tastmatch.FunctionCall(
                name='test', args=[
                    tastmatch.BinOp(
                        left=tastmatch.Constant(value=1), op='+',
                        right=tastmatch.Constant(value=2)),
                    tastmatch.Constant(value='test')
                ]), op='*', right=tastmatch.BinOp(
                    left=tastmatch.Constant(value=1), op='-',
                    right=tastmatch.FunctionCall(name='test1'))), op='-',
        right=tastmatch.Constant(value='value'))

    pat2 = tastmatch.Constant(value='test')

    pat3 = tastmatch.BinOp(
        left=tastmatch.BinOp(
            left=tastmatch.FunctionCall(
                name='test', args=[
                    tastmatch.BinOp(
                        left=match.group(
                            'important_constant', tastmatch.Constant()),
                        op='+', right=tastmatch.Constant()),
                    tastmatch.Constant()
                ]), op='*', right=tastmatch.BinOp(
                    left=tastmatch.Constant(), op='-',
                    right=tastmatch.FunctionCall())), op='-',
        right=match.group('important_constant2', tastmatch.Constant()))

    binop = tastmatch.BinOp(
        op='+', right=match.group('recursive', tastmatch.Constant()))

    binop.left = match.Or(binop, tastmatch.Constant())

    pat4 = match.Or(match.group('recursive', tastmatch.Constant()), binop)

    tree2 = tast.BinOp(
        left=tast.BinOp(
            left=tast.BinOp(
                left=tast.BinOp(
                    left=tast.Constant(value=1),
                    op='+',
                    right=tast.Constant(value=2), ),
                op='+',
                right=tast.Constant(value=3), ),
            op='+',
            right=tast.Constant(value=4), ),
        op='+',
        right=tast.Constant(value=5), )

    tree3 = tast.BinOp(
        left=tast.BinOp(
            left=tast.Constant(value=1),
            op='+',
            right=tast.Constant(value=2), ),
        op='-',
        right=tast.Constant(value=3), )

    tree4 = tast.Constant(value='one and only')

    def test_common_ast_match(self):
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
