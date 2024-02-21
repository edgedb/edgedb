# mypy: ignore-errors

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

from __future__ import annotations
from typing import TypeVar

import pickle
import sys
import unittest

from edb.common.checked import CheckedDict
from edb.common.checked import CheckedList
from edb.common.checked import CheckedSet
from edb.common.checked import FrozenCheckedList
from edb.common.checked import FrozenCheckedSet
from edb.common.checked import enable_typechecks, disable_typechecks
from edb.common import debug


class EnsureTypeChecking:
    def setUp(self):
        if not debug.flags.typecheck:
            enable_typechecks()

    def tearDown(self):
        if not debug.flags.typecheck:
            disable_typechecks()


class CheckedDictTests(EnsureTypeChecking, unittest.TestCase):
    def test_common_checked_checkeddict_basics(self) -> None:
        StrDict = CheckedDict[str, int]
        assert StrDict({"1": 2})["1"] == 2
        assert StrDict(foo=1, initdict=2)["initdict"] == 2

        sd = StrDict(**{"1": 2})
        assert sd["1"] == 2

        assert dict(sd) == {"1": 2}

        sd["foo"] = 42

        with self.assertRaises(KeyError):
            sd[0] = 0
        with self.assertRaises(ValueError):
            sd["foo"] = "bar"
        assert sd["foo"] == 42

        with self.assertRaises(ValueError):
            sd.update({"spam": "ham"})

        sd.update({"spam": 12})
        assert sd["spam"] == 12

        with self.assertRaises(ValueError):
            StrDict(**{"foo": "bar"})

        with self.assertRaisesRegex(TypeError, "expects 2 type parameters"):
            # no value type given
            CheckedDict[int]

        class Foo:
            def __repr__(self):
                return self.__class__.__name__

        class Bar(Foo):
            pass

        FooDict = CheckedDict[str, Foo]

        td = FooDict(bar=Bar(), foo=Foo())
        module_path = self.__module__
        expected = (
            f"edb.common.checked.CheckedDict[str, {module_path}."
            "CheckedDictTests.test_common_checked_checkeddict_basics."
            "<locals>.Foo]({'bar': Bar, 'foo': Foo})"
        )
        assert repr(td) == expected
        expected = "{'bar': Bar, 'foo': Foo}"
        assert str(td) == expected

        with self.assertRaisesRegex(ValueError, "expected at most 1"):
            FooDict(Foo(), Bar())

        td = FooDict.fromkeys("abc", value=Bar())
        assert len(td) == 3
        del td["b"]
        assert "b" not in td
        assert len(td) == 2
        assert str(td) == "{'a': Bar, 'c': Bar}"

    def test_common_checked_checkeddict_pickling(self) -> None:
        StrDict = CheckedDict[str, int]
        sd = StrDict()
        sd["foo"] = 123
        sd["bar"] = 456

        assert sd.keytype is str and sd.valuetype is int
        assert type(sd) is StrDict
        assert sd["foo"] == 123
        assert sd["bar"] == 456

        sd2 = pickle.loads(pickle.dumps(sd))

        assert sd2.keytype is str and sd2.valuetype is int
        assert type(sd2) is StrDict
        assert sd2["foo"] == 123
        assert sd2["bar"] == 456
        assert sd is not sd2
        assert sd == sd2


class CheckedListTestBase(EnsureTypeChecking):
    BaseList = FrozenCheckedList

    def test_common_checked_shared_list_basics(self) -> None:
        IntList = self.BaseList[int]
        StrList = self.BaseList[str]

        with self.assertRaises(ValueError):
            IntList(("1", "2"))

        with self.assertRaises(ValueError):
            StrList([1])

        with self.assertRaises(ValueError):
            StrList([None])

        sl = StrList(["Some", "strings", "here"])
        assert sl == ["Some", "strings", "here"]
        assert list(sl) == ["Some", "strings", "here"]
        assert sl > ["Some", "strings"]
        assert sl < ["Some", "strings", "here", "too"]
        assert sl >= ["Some", "strings"]
        assert sl <= ["Some", "strings", "here", "too"]
        assert sl >= ["Some", "strings", "here"]
        assert sl <= StrList(["Some", "strings", "here"])
        assert sl + ["too"] == ["Some", "strings", "here", "too"]
        assert ["Hey"] + sl == ["Hey", "Some", "strings", "here"]
        assert type(sl + ["too"]) is StrList
        assert type(["Hey"] + sl) is StrList
        assert sl[0] == "Some"
        assert type(sl[:2]) is StrList
        assert sl[:2] == StrList(["Some", "strings"])
        assert len(sl) == 3
        assert sl[1:2] * 3 == ["strings", "strings", "strings"]
        assert 3 * sl[1:2] == ["strings", "strings", "strings"]
        assert type(3 * sl[1:2]) is StrList

        class Foo:
            def __repr__(self):
                return self.__class__.__name__

        class Bar(Foo):
            pass

        FooList = self.BaseList[Foo]

        tl = FooList([Bar(), Foo()])
        cls_name = self.BaseList.__name__
        module_path = self.__module__
        expected = (
            f"edb.common.checked.{cls_name}[{module_path}."
            "CheckedListTestBase.test_common_checked_shared_list_basics."
            "<locals>.Foo]([Bar, Foo])"
        )
        assert repr(tl) == expected, repr(tl)
        expected = "[Bar, Foo]"
        assert str(tl) == expected

    def test_common_checked_shared_list_pickling(self):
        StrList = self.BaseList[str]
        sd = StrList(["123", "456"])

        assert sd.type is str
        assert type(sd) is StrList
        assert sd[0] == "123"
        assert sd[1] == "456"

        sd = pickle.loads(pickle.dumps(sd))

        assert sd.type is str
        assert type(sd) is StrList
        assert sd[0] == "123"
        assert sd[1] == "456"

    def test_common_checked_shared_list_invalid_parameters(self):
        with self.assertRaisesRegex(TypeError, "must be parametrized"):
            self.BaseList()

        with self.assertRaisesRegex(TypeError, "expects 1 type parameter"):
            self.BaseList[int, int]()

        with self.assertRaisesRegex(TypeError, "already parametrized"):
            self.BaseList[int][int]

    @unittest.skipUnless(sys.version_info >= (3, 7, 3), "BPO-35992")
    def test_common_checked_shared_list_non_type_parameter(self):
        with self.assertRaisesRegex(TypeError, "expects types"):
            self.BaseList[1]()


class FrozenCheckedListTests(CheckedListTestBase, unittest.TestCase):
    BaseList = FrozenCheckedList

    def test_common_checked_frozenlist_basics(self) -> None:
        StrList = self.BaseList[str]
        sl = StrList(["1", "2"])
        with self.assertRaises(AttributeError):
            sl.append("3")

    def test_common_checked_frozenlist_hashable(self) -> None:
        StrList = self.BaseList[str]
        s1 = StrList(["1", "2"])
        s2 = StrList(["1", "2"])
        self.assertEqual(hash(s1), hash(tuple(s1)))
        self.assertEqual(hash(s1), hash(s2))


class CheckedListTests(CheckedListTestBase, unittest.TestCase):
    BaseList = CheckedList

    def test_common_checked_checkedlist_basics(self) -> None:
        StrList = self.BaseList[str]
        tl = StrList()
        tl.append("1")
        tl.extend(("2", "3"))
        tl += ["4"]
        tl += ("5",)
        tl = tl + ("6",)
        tl = ("0",) + tl
        tl.insert(0, "-1")
        assert tl == ["-1", "0", "1", "2", "3", "4", "5", "6"]
        del tl[1]
        assert tl == ["-1", "1", "2", "3", "4", "5", "6"]
        del tl[1:3]
        assert tl == ["-1", "3", "4", "5", "6"]
        tl[2] = "X"
        assert tl == ["-1", "3", "X", "5", "6"]
        tl[1:4] = ("A", "B", "C")
        assert tl == ["-1", "A", "B", "C", "6"]
        tl *= 2
        assert tl == ["-1", "A", "B", "C", "6", "-1", "A", "B", "C", "6"]
        tl.sort()
        assert tl == ["-1", "-1", "6", "6", "A", "A", "B", "B", "C", "C"]

        with self.assertRaises(ValueError):
            tl.append(42)

        with self.assertRaises(ValueError):
            tl.extend((42,))

        with self.assertRaises(ValueError):
            tl.insert(0, 42)

        with self.assertRaises(ValueError):
            tl += (42,)

        with self.assertRaises(ValueError):
            tl = tl + (42,)

        with self.assertRaises(ValueError):
            tl = (42,) + tl


class CheckedSetTestBase(EnsureTypeChecking):
    BaseSet = FrozenCheckedSet

    def test_common_checked_shared_set_basics(self) -> None:
        StrSet = self.BaseSet[str]
        s1 = StrSet("sphinx of black quartz judge my vow")
        assert s1 == set("abcdefghijklmnopqrstuvwxyz ")
        s2 = StrSet("hunter2")
        assert (s1 & s2) == StrSet("hunter")
        assert type(s1 & s2) is StrSet
        assert (s1 | s2) == set("abcdefghijklmnopqrstuvwxyz 2")
        assert type(s1 | s2) is StrSet
        assert (s1 - s2) == set("abcdfgijklmopqsvwxyz ")
        assert type(s1 - s2) is StrSet
        assert (set("hunter2") - s1) == StrSet("2")
        assert type(set("hunter2") - s1) is StrSet

        class Foo:
            def __repr__(self):
                return self.__class__.__name__

        class Bar(Foo):
            def __eq__(self, other):
                return isinstance(other, Bar)

            def __hash__(self):
                return 1

        FooSet = self.BaseSet[Foo]

        tl = FooSet([Bar(), Foo(), Bar()])
        tl2 = FooSet(tl | {Foo()})
        assert len(tl) == 2
        assert len(tl ^ tl2) == 1
        assert tl.issuperset({Bar()})
        assert tl.issubset(tl2)
        # We have to do some gymnastics due to sets being unordered.
        expected = {"{Bar, Foo}", "{Foo, Bar}"}
        assert str(tl) in expected
        cls_name = self.BaseSet.__name__
        module_path = self.__module__
        expected_template = (
            f"edb.common.checked.{cls_name}[{module_path}."
            "CheckedSetTestBase.test_common_checked_shared_set_basics."
            "<locals>.Foo]({})"
        )
        assert repr(tl) in {expected_template.format(e) for e in expected}

    def test_common_checkedset_pickling(self):
        StrSet = self.BaseSet[str]
        sd = StrSet({"123", "456"})

        self.assertIs(sd.type, str)
        self.assertIs(type(sd), StrSet)
        self.assertIn("123", sd)
        self.assertIn("456", sd)

        sd = pickle.loads(pickle.dumps(sd))

        self.assertIs(sd.type, str)
        self.assertIs(type(sd), StrSet)
        self.assertIn("123", sd)
        self.assertIn("456", sd)


class FrozenCheckedSetTests(CheckedSetTestBase, unittest.TestCase):
    BaseSet = FrozenCheckedSet

    def test_common_checked_frozenset_hashable(self) -> None:
        StrSet = self.BaseSet[str]
        s1 = StrSet(["1", "2"])
        s2 = StrSet(["2", "1"])
        self.assertEqual(hash(s1), hash(frozenset(("1", "2"))))
        self.assertEqual(hash(s1), hash(s2))


class CheckedSetTests(CheckedSetTestBase, unittest.TestCase):
    BaseSet = CheckedSet

    def test_common_checked_checkedset_basics(self) -> None:
        StrSet = self.BaseSet[str]
        tl = StrSet()
        tl.add("1")
        tl.update(("2", "3"))
        tl |= ["4"]
        tl |= ("5",)
        tl = tl | StrSet(["6"])
        tl = {"0"} | tl
        assert set(tl) == {"0", "1", "2", "3", "4", "5", "6"}

        tl = "67896789" - tl  # sic, TypedSet used to coerce arguments, too.
        assert tl == {"7", "8", "9"}
        assert set(tl - {"8", "9"}) == {"7"}

        assert set(tl ^ {"8", "9", "10"}) == {"7", "10"}
        assert set({"8", "9", "10"} ^ tl) == {"7", "10"}
        tl -= {"8"}
        assert tl == StrSet("79")

        with self.assertRaises(ValueError):
            tl.add(42)

        with self.assertRaises(ValueError):
            tl.update((42,))

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


T = TypeVar("T")


class ConcreteFrozenCheckedSetSubclass1(FrozenCheckedSet[int]):
    def sum(self) -> int:
        return sum(elem for elem in self)


class GenericFrozenCheckedSetSubclass(FrozenCheckedSet[T]):
    def sum(self) -> T:
        return sum(elem for elem in self)


ConcreteFrozenCheckedSetSubclass2 = GenericFrozenCheckedSetSubclass[int]


class CheckedSubclassingTestBase(EnsureTypeChecking):
    BaseSet = GenericFrozenCheckedSetSubclass

    def test_common_checked_checkedset_subclass_pickling(self):
        cfcss = self.BaseSet([0, 2, 4, 6, 8])
        self.assertIs(cfcss.type, int)
        self.assertIs(type(cfcss), self.BaseSet)
        self.assertEqual(cfcss, {0, 2, 4, 6, 8})
        self.assertEqual(cfcss.sum(), 20)

        pickled = pickle.dumps(cfcss)
        cfcss2 = pickle.loads(pickled)

        self.assertTrue(cfcss2.type, int)
        self.assertIs(type(cfcss2), self.BaseSet)
        self.assertIsNot(cfcss, cfcss2)
        self.assertEqual(cfcss, cfcss2)
        self.assertEqual(cfcss.sum(), 20)


class CheckedSubclass1Tests(CheckedSubclassingTestBase, unittest.TestCase):
    BaseSet = ConcreteFrozenCheckedSetSubclass1


class CheckedSubclass2Tests(CheckedSubclassingTestBase, unittest.TestCase):
    BaseSet = ConcreteFrozenCheckedSetSubclass2
