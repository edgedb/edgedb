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

from edb.common.typed import TypedList, StrList


class TypedTests(unittest.TestCase):

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
