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


import dataclasses
import json
import typing
import unittest

import immutables

import edgedb

from edb import errors

from edb.testbase import server as tb
from edb.schema import objects as s_obj

from edb.server import buildmeta
from edb.server import config
from edb.server.config import ops
from edb.server.config import spec
from edb.server.config import types


def make_port_value(*, protocol='graphql+http',
                    database='testdb',
                    user='test',
                    concurrency=4,
                    port=1000,
                    **kwargs):
    return dict(
        protocol=protocol, user=user, database=database,
        concurrency=concurrency, port=port, **kwargs)


@dataclasses.dataclass(frozen=True, eq=True)
class Port(types.CompositeConfigType):

    protocol: str
    database: str
    port: int
    concurrency: int
    user: str
    address: typing.FrozenSet[str] = frozenset({'localhost'})


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
        type=Port,
        default=Port.from_pyvalue(make_port_value())),

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
        type=Port, set_of=True,
        default=frozenset()),
)


class TestServerConfigUtils(unittest.TestCase):

    def setUp(self):
        self._cfgspec = config.get_settings()
        config.set_settings(testspec1)

    def tearDown(self):
        config.set_settings(self._cfgspec)

    def test_server_config_01(self):
        j = ops.to_json(
            testspec1,
            immutables.Map({s.name: s.default for s in testspec1.values()}))

        self.assertEqual(
            json.loads(j),
            {
                'bool': True,
                'bools': [],
                'int': 0,
                'ints': [],
                'port': testspec1['port'].default.to_json_value(),
                'ports': [],
                'str': 'hello',
                'strings': [],
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
            make_port_value(database='f1')
        )
        storage1 = op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(database='f2')
        )
        storage2 = op.apply(testspec1, storage1)

        self.assertEqual(
            storage2['ports'],
            {
                Port.from_pyvalue(make_port_value(database='f1')),
                Port.from_pyvalue(make_port_value(database='f2')),
            })

        j = ops.to_json(testspec1, storage2)
        storage3 = ops.from_json(testspec1, j)
        self.assertEqual(storage3, storage2)

        op = ops.Operation(
            ops.OpCode.CONFIG_REM,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(database='f1')
        )
        storage3 = op.apply(testspec1, storage2)

        self.assertEqual(
            storage3['ports'],
            {
                Port.from_pyvalue(make_port_value(database='f2')),
            })

        op = ops.Operation(
            ops.OpCode.CONFIG_REM,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(database='f1')
        )
        storage4 = op.apply(testspec1, storage3)
        self.assertEqual(storage3, storage4)

    def test_server_config_03(self):
        storage = immutables.Map()

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(zzzzzzz='zzzzz')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown fields: 'zzz"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(concurrency='a')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "invalid 'concurrency'.*expecting int"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'por',
            make_port_value(concurrency='a')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown setting"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(address=["aaa", 123])
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "str or a list"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            ops.OpLevel.SYSTEM,
            'ports',
            make_port_value(address="aaa")
        )
        op.apply(testspec1, storage)

        self.assertEqual(
            Port.from_pyvalue(make_port_value(address='aaa')),
            Port.from_pyvalue(make_port_value(address=['aaa'])))

        self.assertEqual(
            Port.from_pyvalue(make_port_value(address=['aaa', 'bbb'])),
            Port.from_pyvalue(make_port_value(address=['bbb', 'aaa'])))

        self.assertNotEqual(
            Port.from_pyvalue(make_port_value(address=['aaa', 'bbb'])),
            Port.from_pyvalue(make_port_value(address=['bbb', 'aa1'])))

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
            ops.OpCode.CONFIG_SET,
            ops.OpLevel.SESSION,
            'ints',
            {42}
        )
        storage2 = op.apply(testspec1, storage2)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            ops.OpLevel.SESSION,
            'ints',
            {42, 43}
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
                'default': True,
                'internal': False,
                'system': False,
                'typemod': 'SINGLETON',
                'typeid': str(s_obj.get_known_type_id('std::bool')),
            }
        )


class TestServerConfig(tb.QueryTestCase, tb.CLITestCaseMixin):

    ISOLATED_METHODS = False
    SERIALIZED = True

    async def test_server_proto_configure_01(self):
        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                'invalid value type'):
            await self.con.execute('''
                CONFIGURE SESSION SET __internal_no_const_folding := 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            await self.con.execute('''
                CONFIGURE SESSION SET __internal_no_const_folding := false;
                CONFIGURE SYSTEM SET __internal_testvalue := 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            await self.con.execute('''
                CONFIGURE SYSTEM SET __internal_testvalue := 1;
                CONFIGURE SESSION SET __internal_no_const_folding := false;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            async with self.con.transaction():
                await self.con.fetchall('''
                    CONFIGURE SYSTEM SET __internal_testvalue := 1;
                ''')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'CONFIGURE SESSION INSERT is not supported'):
            await self.con.fetchall('''
                CONFIGURE SESSION INSERT SessionConfig { name := 'foo' };
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'module must be either \'cfg\' or empty'):
            await self.con.fetchall('''
                CONFIGURE SYSTEM INSERT cf::SystemConfig { name := 'foo' };
            ''')

    async def test_server_proto_configure_02(self):
        conf = await self.con.fetchone('''
            SELECT cfg::Config.__internal_testvalue LIMIT 1
        ''')
        self.assertEqual(conf, 0)

        jsonconf = await self.con.fetchone('''
            SELECT cfg::get_config_json()
        ''')

        all_conf = json.loads(jsonconf)
        conf = all_conf['__internal_testvalue']

        self.assertEqual(conf['value'], 0)
        self.assertEqual(conf['source'], 'default')

        await self.con.fetchall('''
            CONFIGURE SYSTEM SET __internal_testvalue := 1;
        ''')

        conf = await self.con.fetchone('''
            SELECT cfg::Config.__internal_testvalue LIMIT 1
        ''')
        self.assertEqual(conf, 1)

        jsonconf = await self.con.fetchone('''
            SELECT cfg::get_config_json()
        ''')

        all_conf = json.loads(jsonconf)
        conf = all_conf['__internal_testvalue']

        self.assertEqual(conf['value'], 1)
        self.assertEqual(conf['source'], 'system override')

    async def test_server_proto_configure_03(self):
        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name } FILTER .name LIKE 'test_03%';
            ''',
            [],
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT SystemConfig { name := 'test_03' };
        ''')

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT cfg::SystemConfig {
                name := 'test_03_01'
            };
        ''')

        with self.assertRaisesRegex(edgedb.InterfaceError, r'\bfetchone\('):
            await self.con.fetchone('''
                CONFIGURE SYSTEM INSERT cfg::SystemConfig {
                    name := 'test_03_0122222222'
                };
            ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%'
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'test_03',
                },
                {
                    'name': 'test_03_01',
                }
            ]
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM RESET SystemConfig FILTER .name = 'test_03';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            [
                {
                    'name': 'test_03_01',
                },
            ],
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM RESET SystemConfig FILTER .name = 'test_03_01';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            []
        )

        # Repeat reset that doesn't match anything this time.
        await self.con.fetchall('''
            CONFIGURE SYSTEM RESET SystemConfig FILTER .name = 'test_03_01';
        ''')

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT SystemConfig {
                name := 'test_03',
                obj := (INSERT Subclass1 { name := 'foo', sub1 := 'sub1' })
            }
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj {
                name,
                obj[IS cfg::Subclass1]: {
                    name,
                    sub1,
                },
            }
            FILTER .name LIKE 'test_03%';
            ''',
            [
                {
                    'name': 'test_03',
                    'obj': {
                        'name': 'foo',
                        'sub1': 'sub1',
                    },
                },
            ],
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT SystemConfig {
                name := 'test_03_01',
                obj := (INSERT Subclass2 { name := 'bar', sub2 := 'sub2' })
            }
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj {
                name,
                obj: {
                    __type__: {name},
                    name,
                },
            }
            FILTER .name LIKE 'test_03%'
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'test_03',
                    'obj': {
                        '__type__': {'name': 'cfg::Subclass1'},
                        'name': 'foo',
                    },
                },
                {
                    'name': 'test_03_01',
                    'obj': {
                        '__type__': {'name': 'cfg::Subclass2'},
                        'name': 'bar',
                    },
                },
            ],
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM RESET SystemConfig
            FILTER .obj.name IN {'foo', 'bar'} AND .name ILIKE 'test_03%';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            []
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT SystemConfig {
                name := 'test_03_' ++ <str>count(DETACHED SystemConfig),
            }
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj {
                name,
            }
            FILTER .name LIKE 'test_03%'
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'test_03_0',
                },
            ],
        )

        await self.con.fetchall('''
            CONFIGURE SYSTEM RESET SystemConfig
            FILTER .name ILIKE 'test_03%';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            []
        )

    async def test_server_proto_configure_04(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'CONFIGURE SESSION INSERT is not supported'):
            await self.con.fetchall('''
                CONFIGURE SESSION INSERT SessionConfig {name := 'test_04'}
            ''')

        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                "unrecognized configuration object 'Unrecognized'"):
            await self.con.fetchall('''
                CONFIGURE SYSTEM INSERT Unrecognized {name := 'test_04'}
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "must not have a FILTER clause"):
            await self.con.fetchall('''
                CONFIGURE SYSTEM RESET __internal_testvalue FILTER 1 = 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "non-constant expression"):
            await self.con.fetchall('''
                CONFIGURE SESSION SET __internal_testmode := (random() = 0);
            ''')

        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                "'Subclass1' cannot be configured directly"):
            await self.con.fetchall('''
                CONFIGURE SYSTEM INSERT Subclass1 {
                    name := 'foo'
                };
            ''')

        await self.con.fetchall('''
            CONFIGURE SYSTEM INSERT SystemConfig {
                name := 'test_04',
            }
        ''')

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "SystemConfig.name violates exclusivity constriant"):
            await self.con.fetchall('''
                CONFIGURE SYSTEM INSERT SystemConfig {
                    name := 'test_04',
                }
            ''')

    async def test_server_proto_configure_05(self):
        try:
            await self.con.execute('''
                CONFIGURE SESSION SET effective_cache_size := '1GB';
            ''')

            await self.assert_query_result(
                '''
                SELECT cfg::Config.effective_cache_size
                ''',
                [
                    '1048576kB'
                ],
            )

            await self.con.execute('''
                CONFIGURE SYSTEM SET effective_cache_size := '2GB';
            ''')

            await self.assert_query_result(
                '''
                SELECT cfg::Config.effective_cache_size
                ''',
                [
                    '1048576kB'
                ],
            )

            await self.con.execute('''
                CONFIGURE SESSION RESET effective_cache_size;
            ''')

            await self.assert_query_result(
                '''
                SELECT cfg::Config.effective_cache_size
                ''',
                [
                    '2097152kB'
                ],
            )
        finally:
            await self.con.execute('''
                CONFIGURE SESSION RESET effective_cache_size;
            ''')

            await self.con.execute('''
                CONFIGURE SYSTEM RESET effective_cache_size;
            ''')

    async def test_server_proto_configure_06(self):
        try:
            await self.con.execute('''
                CONFIGURE SESSION SET multiprop := {'1', '2', '3'};
            ''')

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [
                    '1', '2', '3'
                ],
            )

            await self.con.execute('''
                CONFIGURE SYSTEM SET multiprop := {'4', '5'};
            ''')

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [
                    '1', '2', '3'
                ],
            )

            await self.con.execute('''
                CONFIGURE SESSION RESET multiprop;
            ''')

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [
                    '4', '5'
                ],
            )
        finally:
            await self.con.execute('''
                CONFIGURE SESSION RESET multiprop;
            ''')

            await self.con.execute('''
                CONFIGURE SYSTEM RESET multiprop;
            ''')

    async def test_server_version(self):
        srv_ver = await self.con.fetchone(r"""
            SELECT sys::get_version()
        """)

        ver = buildmeta.get_version()

        self.assertEqual(
            (ver.major, ver.minor, ver.stage.name.lower(),
             ver.stage_no, ver.local),
            (srv_ver.major, srv_ver.minor, str(srv_ver.stage),
             srv_ver.stage_no, tuple(srv_ver.local))
        )

        srv_ver_string = await self.con.fetchone(r"""
            SELECT sys::get_version_as_str()
        """)

        self.assertEqual(srv_ver_string, str(ver))

    async def test_config_cli(self):
        try:
            self.run_cli(
                'configure', 'set', '__internal_testvalue', '10',
            )

            await self.assert_query_result(
                '''
                SELECT cfg::Config.__internal_testvalue
                ''',
                [
                    10,
                ],
            )

            self.run_cli(
                'configure', 'set', 'multiprop', 'a', 'b', 'c'
            )

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [
                    'a', 'b', 'c'
                ],
            )

            self.run_cli(
                'configure', 'reset', 'multiprop'
            )

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [],
            )

            self.run_cli(
                'configure', 'insert', 'systemconfig', '--name=cliconf'
            )

            await self.assert_query_result(
                '''
                SELECT cfg::Config.sysobj {
                    name,
                }
                FILTER .name = 'cliconf'
                ORDER BY .name;
                ''',
                [
                    {
                        'name': 'cliconf',
                    },
                ],
            )

            self.run_cli(
                'configure', 'reset', 'systemconfig', '--name=cliconf'
            )

        finally:
            await self.con.execute('''
                CONFIGURE SYSTEM RESET __internal_testvalue;
            ''')

            await self.con.execute('''
                CONFIGURE SYSTEM RESET multiprop;
            ''')

            await self.con.execute('''
                CONFIGURE SYSTEM RESET SystemConfig FILTER .name = 'cliconf';
            ''')
