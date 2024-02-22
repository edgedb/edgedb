#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

import unittest
import unittest.mock

from edb.common.windowedsum import WindowedSum


class ManualClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class WindowedSumTests(unittest.TestCase):
    def test_common_windowedsum(self) -> None:
        monotonic = ManualClock(0)
        with unittest.mock.patch("time.monotonic", monotonic):
            s = WindowedSum()
            s += 1
            self.assertEqual(1, int(s))
            monotonic.value += 60
            self.assertEqual(0, int(s))
            s += 1
            s += 2
            s += 3
            self.assertEqual(6, int(s))
            monotonic.value += 59
            self.assertEqual(6, int(s))
            s += 4
            self.assertEqual(10, int(s))
            monotonic.value += 1.5
            self.assertEqual(4, int(s))
            monotonic.value += 1.5
            s += 5
            self.assertEqual(9, int(s))
            monotonic.value += 1.5
            s += 6
            self.assertEqual(15, int(s))
