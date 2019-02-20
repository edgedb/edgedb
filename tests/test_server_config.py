#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


import json
import unittest

import immutables

from edb import errors

from edb.server.config import ops
from edb.server.config import spec
from edb.server.config import types


testspec1 = spec.Spec(
    spec.Setting(
        'int',
        type=int,
        default=0),
    spec.Setting(
        'str',
        type=str,
        default='hello'),
    spec.Setting(
        'bool',
        type=bool,
        default=True),

    spec.Setting(
        'port',
        type=types.Port,
        default=types.Port('http+graphql', 'foo', 8080, 4)),

    spec.Setting(
        'ints',
        type=int, set_of=True,
        default=frozenset()),
    spec.Setting(
        'strings',
        type=str, set_of=True,
        default=frozenset()),
    spec.Setting(
        'bools',
        type=bool, set_of=True,
        default=frozenset()),

    spec.Setting(
        'ports',
        type=types.Port, set_of=True,
        default=frozenset()),
)


class TestServerConfigUtils(unittest.TestCase):

    def test_server_config_01(self):
        j = ops.to_json(
            testspec1,
            immutables.Map({s.name: s.default for s in testspec1.values()}))

        self.assertEqual(
            json.loads(j),
            {
                'bool': [True, 'true'],
                'bools': [[], '{}'],
                'int': [0, '0'],
                'ints': [[], '{}'],
                'port': [
                    'protocol=http+graphql;database=foo;port=8080;' +
                    'concurrency=4',
                    '\'protocol=http+graphql;database=foo;port=8080;' +
                    'concurrency=4\''
                ],
                'ports': [[], '{}'],
                'str': ['hello', "'hello'"],
                'strings': [[], '{}']
            }
        )

        self.assertEqual(
            ops.from_json(testspec1, j),
            immutables.Map({s.name: s.default for s in testspec1.values()})
        )

    def test_server_config_02(self):
        storage = immutables.Map()

        storage1 = ops.apply(
            testspec1,
            storage,
            ops.Operation(
                ops.OpCode.CONFIG_ADD,
                ops.OpLevel.SYSTEM,
                'ports',
                'protocol=http+edgeql;database=f1;port=8080;concurrency=4'
            )
        )

        storage2 = ops.apply(
            testspec1,
            storage1,
            ops.Operation(
                ops.OpCode.CONFIG_ADD,
                ops.OpLevel.SYSTEM,
                'ports',
                'protocol=http+edgeql;database=f2;port=8080;concurrency=4'
            )
        )

        self.assertEqual(
            storage2['ports'],
            {
                types.Port('http+edgeql', 'f1', 8080, 4),
                types.Port('http+edgeql', 'f2', 8080, 4),
            })

        j = ops.to_json(testspec1, storage2)
        storage3 = ops.from_json(testspec1, j)
        self.assertEqual(storage3, storage2)

        storage3 = ops.apply(
            testspec1,
            storage2,
            ops.Operation(
                ops.OpCode.CONFIG_REM,
                ops.OpLevel.SYSTEM,
                'ports',
                'protocol=http+edgeql;database=f1;port=8080;concurrency=4'
            )
        )

        self.assertEqual(
            storage3['ports'],
            {
                types.Port('http+edgeql', 'f2', 8080, 4),
            })

        storage4 = ops.apply(
            testspec1,
            storage3,
            ops.Operation(
                ops.OpCode.CONFIG_REM,
                ops.OpLevel.SYSTEM,
                'ports',
                'protocol=http+edgeql;database=f1;port=8080;concurrency=4'
            )
        )
        self.assertEqual(storage3, storage4)

    def test_server_config_03(self):
        storage = immutables.Map()

        with self.assertRaisesRegex(errors.ConfigurationError, "'protocl"):
            ops.apply(
                testspec1,
                storage,
                ops.Operation(
                    ops.OpCode.CONFIG_ADD,
                    ops.OpLevel.SYSTEM,
                    'ports',
                    'protocl=http+edgeql;database'
                )
            )

        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown setting"):
            ops.apply(
                testspec1,
                storage,
                ops.Operation(
                    ops.OpCode.CONFIG_ADD,
                    ops.OpLevel.SYSTEM,
                    'por',
                    'protocl=http+edgeql;database'
                )
            )

    def test_server_config_04(self):
        storage = immutables.Map()

        storage1 = ops.apply(
            testspec1,
            storage,
            ops.Operation(
                ops.OpCode.CONFIG_SET,
                ops.OpLevel.SESSION,
                'int',
                11
            )
        )
        self.assertEqual(storage1['int'], 11)

        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "invalid value type for the 'int'"):
            ops.apply(
                testspec1,
                storage1,
                ops.Operation(
                    ops.OpCode.CONFIG_SET,
                    ops.OpLevel.SESSION,
                    'int',
                    '42'
                )
            )

        storage2 = ops.apply(
            testspec1,
            storage1,
            ops.Operation(
                ops.OpCode.CONFIG_SET,
                ops.OpLevel.SESSION,
                'int',
                42
            )
        )
        storage2 = ops.apply(
            testspec1,
            storage2,
            ops.Operation(
                ops.OpCode.CONFIG_ADD,
                ops.OpLevel.SESSION,
                'ints',
                42
            )
        )
        storage2 = ops.apply(
            testspec1,
            storage2,
            ops.Operation(
                ops.OpCode.CONFIG_ADD,
                ops.OpLevel.SESSION,
                'ints',
                43
            )
        )
        self.assertEqual(storage1['int'], 11)
        self.assertEqual(storage2['int'], 42)
        self.assertEqual(storage2['ints'], {42, 43})

    def test_server_config_05(self):
        j = ops.spec_to_json(testspec1)

        self.assertEqual(
            json.loads(j)['bool'],
            {
                'default': [True, 'true'],
                'internal': False,
                'system': False,
                'set_of': False,
            }
        )
