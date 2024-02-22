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
from typing import ClassVar, Generic, Type, TypeVar

# I'm not really sure why this is needed here
# These are things needed for ParametricType?
from typing import Optional, Tuple, Dict, Any  # NoQA

import unittest

from edb.common.parametric import ParametricType

T = TypeVar("T")
K = TypeVar("K")
Z = TypeVar("Z")
A = TypeVar("A")
B = TypeVar("B")


class ParametricTypeTests(unittest.TestCase):

    def test_common_parametric_basics(self) -> None:
        with self.assertRaisesRegex(
            TypeError,
            'must be declared as Generic'
        ):
            class Foo(ParametricType):
                pass

        with self.assertRaisesRegex(
            TypeError,
            'P1: missing ClassVar for generic parameter ~T'
        ):
            class P1(ParametricType, Generic[T]):
                pass

        class P2(ParametricType, Generic[T]):
            random_class_var: ClassVar[int]
            another_class_var: ClassVar[Type[str]]
            not_class_var: int
            t: ClassVar[Type[T]]  # type: ignore

        self.assertTrue(issubclass(P2[int].t, int))  # type: ignore

        with self.assertRaisesRegex(
            TypeError,
            "type 'P2' expects 1 type parameter, got 2"
        ):
            P2[int, str]  # type: ignore

        with self.assertRaisesRegex(
            TypeError,
            "P3: missing one or more type arguments for base 'P2'"
        ):
            class P3(P2, Generic[K]):
                pass

        class P4(P2[Z], Generic[K, Z]):
            pass

        self.assertTrue(issubclass(P4[str, int].t, int))  # type: ignore

        class Bar:
            pass

        class P5(P4[A, B], Bar):
            pass

        self.assertTrue(issubclass(P5[float, str].t, str))  # type: ignore

        # Note the origin class error below is imperfect because we
        # rely on __orig_bases__ in the subclass check and that elides
        # P5.
        with self.assertRaisesRegex(
            TypeError,
            "P6: missing one or more type arguments for base 'P4'"
        ):
            class P6(P5):
                pass

        class P7(P5[int, float]):
            pass

        self.assertTrue(issubclass(P7.t, float))  # type: ignore
