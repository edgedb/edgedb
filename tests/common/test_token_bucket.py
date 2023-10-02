#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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

import unittest
import unittest.mock

from edb.common.token_bucket import TokenBucket


class ManualClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class WindowedSumTests(unittest.TestCase):
    def test_common_token_bucket(self) -> None:
        monotonic = ManualClock(0)
        with unittest.mock.patch("time.monotonic", monotonic):
            tb = TokenBucket(10, 0.1)
            self.assertEqual(tb.consume(5), 0)

            monotonic.value += 12
            self.assertEqual(tb.consume(6), 0)
            self.assertGreater(tb.consume(1), 0)
            self.assertGreater(tb.consume(2), tb.consume(1))

            monotonic.value += 30
            self.assertEqual(tb.consume(2), 0)
            self.assertEqual(tb.consume(1), 0)
            self.assertGreater(tb.consume(1), 0)
