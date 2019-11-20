#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import copy
import typing
import unittest
import unittest.mock

from edb.common import ast
from edb.common.ast import match


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

    @unittest.mock.patch(
        'edb.common.ast.base._check_type',
        ast.base._check_type_real,
    )
    def test_common_ast_typing(self):
        class Base(ast.AST):
            pass

        class Node(Base):
            field_list: list
            field_typing_list: typing.List[Base]
            field_typing_tuple: typing.Tuple[Base, ...]
            field_typing_union: typing.Union[str, bytes]
            field_typing_union_list: typing.List[typing.Union[str, bytes]]
            field_typing_str: str
            field_typing_optional_str: typing.Optional[str]
            field_typing_mapping: typing.Dict[int, str]
            field_typing_mapping_opt_key: \
                typing.Dict[typing.Optional[int], str]

        self.assertEqual(Node().field_list, [])
        self.assertEqual(Node().field_typing_list, [])
        self.assertEqual(Node().field_typing_tuple, ())

        Node().field_list = []
        Node().field_list = [12, 2]
        with self.assertRaises(TypeError):
            Node().field_list = 'abc'

        Node().field_typing_list = []
        Node().field_typing_list = [Base()]
        with self.assertRaises(TypeError):
            Node().field_typing_list = 'abc'
        with self.assertRaises(TypeError):
            Node().field_typing_list = ['abc']

        Node().field_typing_tuple = ()
        Node().field_typing_tuple = (Base(),)
        with self.assertRaises(TypeError):
            Node().field_typing_tuple = 'abc'
        with self.assertRaises(TypeError):
            Node().field_typing_tuple = ('abc',)

        Node().field_typing_union = 'abc'
        Node().field_typing_union = b'abc'
        with self.assertRaises(TypeError):
            Node().field_typing_union = 1

        self.assertEqual(Node().field_typing_union_list, [])
        Node().field_typing_union_list = ['abc', b'abc']
        Node().field_typing_union_list = [b'abc', 'abc']
        with self.assertRaises(TypeError):
            Node().field_typing_union_list = [1]
        with self.assertRaises(TypeError):
            Node().field_typing_union_list = 'abc'

        Node().field_typing_str = 'aaa'
        # All fields in AST are optional
        Node().field_typing_str = None

        Node().field_typing_optional_str = None
        Node().field_typing_optional_str = 'aaa'

        Node().field_typing_mapping = {1: 'a'}
        with self.assertRaises(TypeError):
            Node().field_typing_mapping = {'a': 1}
        with self.assertRaisesRegex(RuntimeError, 'empty key'):
            Node().field_typing_mapping = {None: 1}
        with self.assertRaisesRegex(TypeError, 'expected str but got int'):
            Node().field_typing_mapping_opt_key = {None: 1}
        Node().field_typing_mapping_opt_key = {None: '1'}

        class Node(ast.AST):
            field1: str
            field2: object
            field3: object = 123

        Node().field1 = '123'
        Node().field2 = 'aaa'
        Node().field3 = 'aaa'
        self.assertEqual(Node().field1, None)
        self.assertEqual(Node().field3, 123)

    def test_common_ast_type_anno(self):
        with self.assertRaisesRegex(RuntimeError, r"1 is not a type"):
            class Node1(ast.AST):
                field: 1

        with self.assertRaisesRegex(RuntimeError,
                                    r"Mapping.*is not supported"):
            class Node3(ast.AST):
                field: typing.Mapping[int, str]

        with self.assertRaisesRegex(RuntimeError,
                                    r"default is defined for.*List"):
            class Node4(ast.AST):
                field: typing.List[int] = [1]

        with self.assertRaisesRegex(RuntimeError,
                                    r"default is defined for.*list"):
            class Node5(ast.AST):
                field: list = list


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
