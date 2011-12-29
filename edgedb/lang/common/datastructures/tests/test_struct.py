##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import pickle

from semantix.utils.datastructures.struct import Struct, Field
from semantix.utils.debug import assert_raises


class PickleTest(Struct):
    a = Field(str, default='42')
    b = Field(int)

class PickleTestSlots(Struct, use_slots=True):
    a = Field(str, default='42')
    b = Field(int)


class TestUtilsDSStruct:
    def test_utils_ds_struct_basics(self):
        class Test(Struct):
            field1 = Field(type=str, default='42')
            field2 = Field(type=bool)

        with assert_raises(TypeError, error_re='field2 is required'):
            Test()

        t = Test(field2=False)
        assert t.field1 == '42'
        assert t.field2 is False

        assert set(t) == {'field1', 'field2'}

        Test(foo='bar', field2=True)

    def test_utils_ds_struct_coercion(self):
        class Test(Struct):
            field = Field(type=int, coerce=True)
        assert Test(field=1).field == 1
        assert Test(field='42').field == 42
        with assert_raises(TypeError, error_re='auto-coercion'):
            Test(field='42.2')


        class Test(Struct):
            field = Field(type=int)
        assert Test(field=1).field == 1
        with assert_raises(TypeError, error_re='expected int'):
            Test(field='42')

    def test_utils_ds_struct_slots(self):
        class Test(Struct, use_slots=True):
            field = Field(str, None)

        assert Test.__slots__ == ('field',)

        t = Test()
        t.field = 'foo'
        assert t.field == 'foo'
        with assert_raises(AttributeError, error_re='has no attribute'):
            t.foo = 'bar'

        class DTest(Test):
            field2 = Field(int, None)

        t = DTest()
        t.field = '1'
        t.field2 = 2
        assert t.field == '1'
        assert t.field2 == 2
        with assert_raises(AttributeError, error_re='has no attribute'):
            t.foo = 'bar'

    def test_utils_ds_struct_pickle(self):
        s1 = PickleTest(b=41)
        s2 = pickle.loads(pickle.dumps(s1))
        assert s2.b == 41 and s2.a == '42'
        assert s2.__class__.__name__ == 'PickleTest'

        s1 = PickleTestSlots(b=41)
        s2 = pickle.loads(pickle.dumps(s1))
        assert s2.b == 41 and s2.a == '42'
        assert s2.__class__.__name__ == 'PickleTestSlots'
