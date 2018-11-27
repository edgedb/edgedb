#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import dataclasses
import unittest

from edb.lang.common import lru


@dataclasses.dataclass(frozen=True)
class Key:

    name: str


class TestLRU(unittest.TestCase):

    def test_lru_1(self):
        l = lru.LRUMapping(maxsize=3)  # noqa

        k1 = Key('1')
        k2 = Key('2')
        k3 = Key('3')
        k4 = Key('4')
        k5 = Key('5')

        l[k1] = '1'
        l[k2] = '2'
        l[k3] = '3'
        l[k4] = '4'
        l[k5] = '5'

        self.assertEqual(len(l), 3)
        self.assertNotIn(k1, l)
        self.assertNotIn(k2, l)

        self.assertEqual(list(l), [k3, k4, k5])
        self.assertEqual(list(l), [k3, k4, k5])

        self.assertEqual(l[k4], '4')
        self.assertEqual(list(l), [k3, k5, k4])
        self.assertEqual(list(l), [k3, k5, k4])

        l[k1] = '10'
        self.assertEqual(list(l), [k5, k4, k1])

        self.assertEqual(l[k4], '4')
        self.assertEqual(l[k5], '5')
        self.assertEqual(l[k1], '10')

        self.assertEqual(list(l), [k4, k5, k1])

        l[k5] = '50'
        self.assertEqual(list(l), [k4, k1, k5])

        l[k4] = l[k4]
        self.assertEqual(list(l), [k1, k5, k4])
