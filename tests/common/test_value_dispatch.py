#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

from edb.common import value_dispatch


class TestValueDispatch(unittest.TestCase):

    def test_common_value_dispatch_01(self):

        @value_dispatch.value_dispatch
        def eat(fruit):
            return f"I don't want a {fruit}..."

        @eat.register('apple')
        def _eat_apple(fruit):
            return "I love apples!"

        @eat.register('eggplant')
        @eat.register('squash')
        def _eat_what(fruit):
            return f"I didn't know {fruit} is a fruit!"

        self.assertEqual(eat('apple'), "I love apples!")
        self.assertEqual(eat('squash'), "I didn't know squash is a fruit!")
        self.assertEqual(eat('eggplant'), "I didn't know eggplant is a fruit!")
        self.assertEqual(eat('banana'), "I don't want a banana...")

    def test_common_value_dispatch_02(self):

        @value_dispatch.value_dispatch
        def eat(fruit):
            return f"I don't want a {fruit}..."

        @eat.register_for_all({'eggplant', 'squash'})
        def _eat_what(fruit):
            return f"I didn't know {fruit} is a fruit!"

        self.assertEqual(eat('squash'), "I didn't know squash is a fruit!")
        self.assertEqual(eat('eggplant'), "I didn't know eggplant is a fruit!")
        self.assertEqual(eat('banana'), "I don't want a banana...")

    def test_common_value_dispatch_03(self):
        @value_dispatch.value_dispatch
        def eat(fruit):
            return f"I don't want a {fruit}..."

        @eat.register('apple')
        def _eat_apple(fruit):
            return "I love apples!"

        with self.assertRaisesRegex(
                ValueError,
                "there is already a handler registered for 'apple'"):
            @eat.register('apple')
            def _eat_apple_bogus(fruit):
                return "I love apples, do I?"

        with self.assertRaisesRegex(
                ValueError,
                "there is already a handler registered for 'apple'"):
            @eat.register_for_all({'apple'})
            def _eat_apple_bogus2(fruit):
                return "I love apples, do I?"
