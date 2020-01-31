#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


import pickle
import unittest

from edb.common.struct import Struct, MixedStruct, Field


class PickleTest(Struct):
    a = Field(str, default='42')
    b = Field(int)


class PickleTestMixed(MixedStruct):
    a = Field(str, default='42')
    b = Field(int)


class StructTests(unittest.TestCase):

    def test_common_struct_basics(self):
        class Test(Struct):
            field1 = Field(str, default='42')
            field2 = Field(bool)

        with self.assertRaisesRegex(TypeError, 'field2 is required'):
            Test()

        t = Test(field2=False)
        assert t.field1 == '42'
        assert t.field2 is False

        assert set(t) == {'field1', 'field2'}

    def test_common_struct_coercion(self):
        class Test(Struct):
            field = Field(int, coerce=True)

        assert Test(field=1).field == 1
        assert Test(field='42').field == 42
        with self.assertRaisesRegex(TypeError, 'cannot coerce'):
            Test(field='42.2')

        class Test(Struct):
            field = Field(int)

        assert Test(field=1).field == 1
        with self.assertRaisesRegex(TypeError, 'expected int'):
            Test(field='42')

    def test_common_struct_strictness(self):
        class Test(Struct):
            field = Field(str, None)

        assert Test.__slots__ == ('field', )

        t = Test()
        t.field = 'foo'
        assert t.field == 'foo'
        with self.assertRaisesRegex(AttributeError, 'has no attribute'):
            t.foo = 'bar'

        class DTest(Test):
            field2 = Field(int, None)

        t = DTest()
        t.field = '1'
        t.field2 = 2
        assert t.field == '1'
        assert t.field2 == 2
        with self.assertRaisesRegex(AttributeError, 'has no attribute'):
            t.foo = 'bar'

        with self.assertRaisesRegex(
                TypeError, 'field3 is an invalid argument'):
            DTest(field='1', field2=2, field3='aaa')

        t = DTest()

        with self.assertRaisesRegex(TypeError,
                                    'field3 is an invalid argument'):
            t.update(field='1', field2=2, field3='aaa')

    def test_common_struct_mixed(self):
        class Test(MixedStruct):
            field1 = Field(str, default='42')
            field2 = Field(bool)

        t1 = Test(field1='field1', field2=True, spam='ham')
        t1.update(ham='spam')

        t1.monty = 'python'
        assert t1.monty == 'python'

    def test_common_struct_pickle(self):
        s1 = PickleTest(b=41)
        s2 = pickle.loads(pickle.dumps(s1))
        assert s2.b == 41 and s2.a == '42'
        assert s2.__class__.__name__ == 'PickleTest'

        s1 = PickleTestMixed(b=41)
        s2 = pickle.loads(pickle.dumps(s1))
        assert s2.b == 41 and s2.a == '42'
        assert s2.__class__.__name__ == 'PickleTestMixed'

    def test_common_struct_frozen(self):
        class Test(MixedStruct):
            field1 = Field(str, default='42', frozen=True)
            field2 = Field(bool)

        t1 = Test(field1='field1', field2=True, spam='ham')

        with self.assertRaisesRegex(ValueError, 'cannot assign'):
            t1.update(field1='spam')

        with self.assertRaisesRegex(ValueError, 'cannot assign'):
            t1.field1 = 'aaa'

        self.assertEqual(t1.field1, 'field1')
        self.assertEqual(t1.field2, True)

        t1.field2 = False
        self.assertEqual(t1.field2, False)
