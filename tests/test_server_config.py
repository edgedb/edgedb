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
from edb.schema import objects as s_obj

from edb.server.config import ops
from edb.server.config import spec
from edb.server.config import types


def make_port_json(*, protocol='http+graphql',
                   database='testdb',
                   user='test',
                   concurrency=4,
                   port=1000,
                   **kwargs):
    return json.dumps(dict(
        protocol=protocol, user=user, database=database,
        concurrency=concurrency, port=port, **kwargs))


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
        default=types.Port.from_pyvalue(make_port_json())),

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
                    testspec1['port'].default.to_json(),
                    testspec1['port'].default.to_edgeql(),
                ],
                'ports': [[], '{}'],
                'str': ['hello', "'hello'"],
                'strings': [[], '{}'],
            }
        )

        self.assertEqual(
            ops.from_json(testspec1, j),
            immutables.Map({s.name: s.default for s in testspec1.values()})
        )

    def test_server_config_02(self):
        storage = immutables.Map()

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(database='f1')
        )
        storage1 = op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(database='f2')
        )
        storage2 = op.apply(testspec1, storage1)

        self.assertEqual(
            storage2['ports'],
            {
                types.Port.from_pyvalue(make_port_json(database='f1')),
                types.Port.from_pyvalue(make_port_json(database='f2')),
            })

        j = ops.to_json(testspec1, storage2)
        storage3 = ops.from_json(testspec1, j)
        self.assertEqual(storage3, storage2)

        op = ops.Operation(
            ops.OpCode.CONFIG_REM,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(database='f1')
        )
        storage3 = op.apply(testspec1, storage2)

        self.assertEqual(
            storage3['ports'],
            {
                types.Port.from_pyvalue(make_port_json(database='f2')),
            })

        op = ops.Operation(
            ops.OpCode.CONFIG_REM,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(database='f1')
        )
        storage4 = op.apply(testspec1, storage3)
        self.assertEqual(storage3, storage4)

    def test_server_config_03(self):
        storage = immutables.Map()

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(zzzzzzz='zzzzz')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown fields: 'zzz"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(concurrency='a')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    '"concurrency" field must be a int'):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'por',
            make_port_json(concurrency='a')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown setting"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(address=["aaa", 123])
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "string or an array of strings"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_json(address="aaa")
        )
        op.apply(testspec1, storage)

        self.assertEqual(
            types.Port.from_pyvalue(make_port_json(address='aaa')),
            types.Port.from_pyvalue(make_port_json(address=['aaa'])))

        self.assertEqual(
            types.Port.from_pyvalue(make_port_json(address=['aaa', 'bbb'])),
            types.Port.from_pyvalue(make_port_json(address=['bbb', 'aaa'])))

        self.assertNotEqual(
            types.Port.from_pyvalue(make_port_json(address=['aaa', 'bbb'])),
            types.Port.from_pyvalue(make_port_json(address=['bbb', 'aa1'])))

    def test_server_config_04(self):
        storage = immutables.Map()

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            ops.OpLevel.SESSION,
            'int',
            11
        )
        storage1 = op.apply(testspec1, storage)
        self.assertEqual(storage1['int'], 11)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            ops.OpLevel.SESSION,
            'int',
            '42'
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "invalid value type for the 'int'"):
            op.apply(testspec1, storage1)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            ops.OpLevel.SESSION,
            'int',
            42
        )
        storage2 = op.apply(testspec1, storage1)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SESSION,
            'ints',
            42
        )
        storage2 = op.apply(testspec1, storage2)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SESSION,
            'ints',
            43
        )
        storage2 = op.apply(testspec1, storage2)

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
                'typemod': 'SINGLETON',
                'typeid': str(s_obj.get_known_type_id('std::bool')),
            }
        )
