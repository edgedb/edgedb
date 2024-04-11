# mypy: ignore-errors

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
from edb.common.ast import visitor


class tast:

    class Base(ast.AST):
        pass

    class BinOp(Base):
        op: typing.Any = None
        left: typing.Any = None
        right: typing.Any = None

    class UnaryOp(Base):
        op: typing.Any = None
        operand: typing.Any = None

    class FunctionCall(Base):
        name: typing.Any = None
        args: typing.List[int]

    class Constant(Base):
        value: typing.Any = None


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
            node: dict

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
    @unittest.mock.patch(
        'edb.common.ast.base.AST.__setattr__',
        ast.base.AST._checked_setattr,
    )
    def test_common_ast_typing(self):
        class Base(ast.AST):
            pass

        class Node(Base):
            field_list: list = ast.field(factory=list)
            field_typing_list: typing.List[Base] = ast.field(factory=list)
            field_typing_tuple: typing.Tuple[Base, ...] = ()
            field_typing_union: typing.Union[str, bytes]
            field_typing_union_list: typing.List[
                typing.Union[str, bytes]] = ast.field(factory=list)
            field_typing_str: str
            field_typing_optional_str: typing.Optional[str]
            field_typing_mapping: typing.Dict[
                int, str] = ast.field(factory=dict)
            field_typing_mapping_opt_key: \
                typing.Dict[
                    typing.Optional[int], str] = ast.field(factory=dict)

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


class ASTFindChildrenTests(unittest.TestCase):

    def test_common_ast_find_children(self):
        node = tast.UnaryOp(
            op='NamedTuple',
            operand=[
                ('foo', tast.Constant(value=2)),
                ('bar', [
                    tast.UnaryOp(op='-', operand=tast.Constant(value=3))]),
            ],
        )
        children = visitor.find_children(node, tast.Constant)
        assert {x.value for x in children} == {2, 3}
