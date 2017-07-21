##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import pickle
import unittest

from edgedb.lang.common.typed import TypedDict, TypedList, StrList, TypedSet


# StrDict and StrList are declared here (not in tests that use them)
# to test pickling support (classes have to be accessible from the module
# where declared)
#
class StrDict(TypedDict, keytype=str, valuetype=int):
    pass


class TypedTests(unittest.TestCase):

    def test_common_typeddict_basics(self):
        assert StrDict({'1': 2})['1'] == 2
        assert StrDict(foo=1, initdict=2)['initdict'] == 2

        sd = StrDict(**{'1': 2})
        assert sd['1'] == 2

        assert dict(sd) == {'1': 2}

        sd['foo'] = 42

        with self.assertRaises(ValueError):
            sd['foo'] = 'bar'
        assert sd['foo'] == 42

        with self.assertRaises(ValueError):
            sd.update({'spam': 'ham'})

        sd.update({'spam': 12})
        assert sd['spam'] == 12

        with self.assertRaises(ValueError):
            StrDict(**{'foo': 'bar'})

        with self.assertRaisesRegex(TypeError, "'valuetype'"):

            class InvalidTypedDict(TypedDict, keytype=int):
                """no 'valuetype' arg -- this class cannot be instantiated."""

    def test_common_typeddict_pickling(self):
        sd = StrDict()
        sd['foo'] = 123

        sd = pickle.loads(pickle.dumps(sd))

        assert sd.keytype is str and sd.valuetype is int
        assert type(sd) is StrDict
        assert sd['foo'] == 123

    def test_common_typedlist_basics(self):
        tl = StrList()
        tl.append('1')
        tl.extend(('2', '3'))
        tl += ['4']
        tl += ('5', )
        tl = tl + ('6', )
        tl = ('0', ) + tl
        tl.insert(0, '-1')
        assert list(tl) == ['-1', '0', '1', '2', '3', '4', '5', '6']

        with self.assertRaises(ValueError):
            tl.append(42)

        with self.assertRaises(ValueError):
            tl.extend((42, ))

        with self.assertRaises(ValueError):
            tl.insert(0, 42)

        with self.assertRaises(ValueError):
            tl += (42, )

        with self.assertRaises(ValueError):
            tl = tl + (42, )

        with self.assertRaises(ValueError):
            tl = (42, ) + tl

        class IntList(TypedList, type=int):
            pass

        with self.assertRaises(ValueError):
            IntList(('1', '2'))

        assert StrList(('1', '2')) == ['1', '2']

        class Foo:
            def __repr__(self):
                return self.__class__.__name__

        class Bar(Foo):
            pass

        class FooList(TypedList, type=Foo):
            pass

        tl = FooList()
        tl.append(Bar())
        tl.append(Foo())
        assert str(tl) == '[Bar, Foo]'

    def test_common_typedlist_none(self):
        tl = StrList()
        with self.assertRaises(ValueError):
            tl.append(None)

    def test_common_typedlist_pickling(self):
        sd = StrList()
        sd.append('123')

        sd = pickle.loads(pickle.dumps(sd))

        assert sd.type is str
        assert type(sd) is StrList
        assert sd[0] == '123'

    def test_common_typedset_basics(self):
        class StrSet(TypedSet, type=str):
            pass

        tl = StrSet()
        tl.add('1')
        tl.update(('2', '3'))
        tl |= ['4']
        tl |= ('5', )
        tl = tl | ('6', )
        tl = {'0'} | tl
        assert set(tl) == {'0', '1', '2', '3', '4', '5', '6'}

        tl = {'6', '7', '8', '9'} - tl
        assert set(tl) == {'7', '8', '9'}
        assert set(tl - {'8', '9'}) == {'7'}

        assert set(tl ^ {'8', '9', '10'}) == {'7', '10'}
        assert set({'8', '9', '10'} ^ tl) == {'7', '10'}

        with self.assertRaises(ValueError):
            tl.add(42)

        with self.assertRaises(ValueError):
            tl.update((42, ))

        with self.assertRaises(ValueError):
            tl |= {42}

        with self.assertRaises(ValueError):
            tl = tl | {42}

        with self.assertRaises(ValueError):
            tl = {42} | tl

        with self.assertRaises(ValueError):
            tl = {42} ^ tl

        with self.assertRaises(ValueError):
            tl &= {42}

        with self.assertRaises(ValueError):
            tl ^= {42}
