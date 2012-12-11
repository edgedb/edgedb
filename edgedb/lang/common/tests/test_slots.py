##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import slots
from metamagic.utils.debug import assert_raises


class TestUtilsSlotsMeta:
    def test_utils_slots_meta_1(self):
        for mcls in (slots.SlotsAbstractMeta, slots.SlotsMeta):
            with assert_raises(TypeError, error_re='must have __slots__'):
                class Foo(metaclass=mcls):
                    pass

            with assert_raises(TypeError, error_re='must be a tuple'):
                class Foo(metaclass=mcls):
                    __slots__ = ('foo')

            class Foo(metaclass=mcls):
                __slots__ = ()

            with assert_raises(TypeError, error_re='must have __slots__'):
                class Bar(Foo):
                    pass

            with assert_raises(TypeError, error_re='must be a tuple'):
                class Bar(Foo):
                    __slots__ = 'foo'

    def test_utils_slots_meta_2(self):
        class A(metaclass=slots.SlotsMeta): __slots__ = ('a',)
        class B(A): __slots__ = ('b',)

        with assert_raises(TypeError, error_re='intersection'):
            class C(B): __slots__ = ('a',)
