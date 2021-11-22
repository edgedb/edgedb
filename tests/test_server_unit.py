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

from edb.server import server


class TestServerUnittests(unittest.TestCase):

    def test_server_unittest_fix_wildcard_addrs(self):
        CASES = [
            (
                ['*'],
                (['0.0.0.0', '::'], [])
            ),
            (
                ['0.0.0.0', '::0'],
                (['0.0.0.0', '::'], [])
            ),
            (
                ['0.0.0.0', '127.0.0.1', '2001:db8::8a2e:370:7334', '::0'],
                (['0.0.0.0', '::'], ['127.0.0.1', '2001:db8::8a2e:370:7334'])
            ),
            (
                ['0.0.0.0', 'example.com', '2001:db8::8a2e:370:7334', '::0'],
                (['0.0.0.0', '::'], ['example.com', '2001:db8::8a2e:370:7334'])
            ),
            (
                ['127.0.0.1', 'example.com', '2001:db8::8a2e:370:7334', '::0'],
                (
                    ['127.0.0.1', '::'],
                    ['example.com', '2001:db8::8a2e:370:7334']
                )
            ),
            (
                ['example.com', '2001:db8::8a2e:370:7334', '::0'],
                (['::'], ['example.com', '2001:db8::8a2e:370:7334'])
            ),
            (
                ['example.com', 'sub.example.com'],
                (['example.com', 'sub.example.com'], [])
            ),
            (
                ['example.com', '127.0.0.1'],
                (['example.com', '127.0.0.1'], [])
            ),
            (
                ['example.com', '::1'],
                (['::1', 'example.com'], [])
            ),
            (
                ['example.com', '::'],
                (['::'], ['example.com'])
            ),
            (
                ['example.com', '::1', '127.0.0.1'],
                (['example.com', '::1', '127.0.0.1'], [])
            ),
            (
                ['0.0.0.0', '2001:db8::8a2e:370:7334'],
                (['0.0.0.0', '2001:db8::8a2e:370:7334'], [])
            ),
            (
                ['127.0.0.1', '2001:db8::8a2e:370:7334', '::'],
                (['127.0.0.1', '::'], ['2001:db8::8a2e:370:7334'])
            ),
        ]

        for hosts, expected in CASES:
            new_hosts, rej_hosts = server._cleanup_wildcard_addrs(hosts)
            self.assertEqual(
                (set(new_hosts), set(rej_hosts)),
                (set(expected[0]), set(expected[1]))
            )
