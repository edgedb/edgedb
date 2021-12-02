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


import asyncio
import dataclasses
import datetime
import json
import platform
import random
import tempfile
import textwrap
import typing
import unittest

import immutables

import edgedb

from edb import buildmeta
from edb import errors
from edb.edgeql import qltypes

from edb.testbase import server as tb
from edb.schema import objects as s_obj

from edb.server import args
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
        self._cfgspec = None  # some settings cannot be pickled by runner.py

    def test_server_config_01(self):
        conf = immutables.Map({
            s.name: ops.SettingValue(
                name=s.name,
                value=s.default,
                source='system override',
                scope=qltypes.ConfigScope.INSTANCE,
            ) for s in testspec1.values()
        })

        j = ops.to_json(testspec1, conf)
        self.assertEqual(
            json.loads(j),
            {
                'bool': {
                    'name': 'bool',
                    'value': True,
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'bools': {
                    'name': 'bools',
                    'value': [],
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'int': {
                    'name': 'int',
                    'value': 0,
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'ints': {
                    'name': 'ints',
                    'value': [],
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'port': {
                    'name': 'port',
                    'value': testspec1['port'].default.to_json_value(),
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'ports': {
                    'name': 'ports',
                    'value': [],
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'str': {
                    'name': 'str',
                    'value': 'hello',
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
                'strings': {
                    'name': 'strings',
                    'value': [],
                    'source': 'system override',
                    'scope': qltypes.ConfigScope.INSTANCE,
                },
            }
        )

        self.assertEqual(ops.from_json(testspec1, j), conf)

    def test_server_config_02(self):
        storage = immutables.Map()

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(database='f1')
        )
        storage1 = op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(database='f2')
        )
        storage2 = op.apply(testspec1, storage1)

        self.assertEqual(
            config.lookup('ports', storage2, spec=testspec1),
            {
                Port.from_pyvalue(make_port_value(database='f1')),
                Port.from_pyvalue(make_port_value(database='f2')),
            })

        j = ops.to_json(testspec1, storage2)
        storage3 = ops.from_json(testspec1, j)
        self.assertEqual(storage3, storage2)

        op = ops.Operation(
            ops.OpCode.CONFIG_REM,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(database='f1')
        )
        storage3 = op.apply(testspec1, storage2)

        self.assertEqual(
            config.lookup('ports', storage3, spec=testspec1),
            {
                Port.from_pyvalue(make_port_value(database='f2')),
            })

        op = ops.Operation(
            ops.OpCode.CONFIG_REM,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(database='f1')
        )
        storage4 = op.apply(testspec1, storage3)
        self.assertEqual(storage3, storage4)

    def test_server_config_03(self):
        storage = immutables.Map()

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(zzzzzzz='zzzzz')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown fields: 'zzz"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(concurrency='a')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "invalid 'concurrency'.*expecting int"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
            'por',
            make_port_value(concurrency='a')
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "unknown setting"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
            'ports',
            make_port_value(address=["aaa", 123])
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "str or a list"):
            op.apply(testspec1, storage)

        op = ops.Operation(
            ops.OpCode.CONFIG_ADD,
            config.ConfigScope.INSTANCE,
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
            config.ConfigScope.SESSION,
            'int',
            11
        )
        storage1 = op.apply(testspec1, storage)
        self.assertEqual(config.lookup('int', storage1, spec=testspec1), 11)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            config.ConfigScope.SESSION,
            'int',
            '42'
        )
        with self.assertRaisesRegex(errors.ConfigurationError,
                                    "invalid value type for the 'int'"):
            op.apply(testspec1, storage1)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            config.ConfigScope.SESSION,
            'int',
            42
        )
        storage2 = op.apply(testspec1, storage1)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            config.ConfigScope.SESSION,
            'ints',
            {42}
        )
        storage2 = op.apply(testspec1, storage2)

        op = ops.Operation(
            ops.OpCode.CONFIG_SET,
            config.ConfigScope.SESSION,
            'ints',
            {42, 43}
        )
        storage2 = op.apply(testspec1, storage2)

        self.assertEqual(config.lookup('int', storage1, spec=testspec1), 11)
        self.assertEqual(config.lookup('int', storage2, spec=testspec1), 42)
        self.assertEqual(
            config.lookup('ints', storage2, spec=testspec1), {42, 43})

    def test_server_config_05(self):
        j = ops.spec_to_json(testspec1)

        self.assertEqual(
            json.loads(j)['bool'],
            {
                'backend_setting': None,
                'default': True,
                'internal': False,
                'system': False,
                'report': False,
                'typemod': 'SingletonType',
                'typeid': str(s_obj.get_known_type_id('std::bool')),
            }
        )


class TestServerConfig(tb.QueryTestCase):

    PARALLELISM_GRANULARITY = 'system'
    TRANSACTION_ISOLATION = False

    async def test_server_proto_config_objects(self):
        await self.assert_query_result(
            """SELECT cfg::InstanceConfig IS cfg::AbstractConfig""",
            [True],
        )

        await self.assert_query_result(
            """SELECT cfg::InstanceConfig IS cfg::DatabaseConfig""",
            [False],
        )

        await self.assert_query_result(
            """SELECT cfg::InstanceConfig IS cfg::InstanceConfig""",
            [True],
        )

        await self.assert_query_result(
            """
            SELECT cfg::AbstractConfig {
                tname := .__type__.name
            }
            ORDER BY .__type__.name
            """,
            [{
                "tname": "cfg::Config",
            }, {
                "tname": "cfg::DatabaseConfig",
            }, {
                "tname": "cfg::InstanceConfig",
            }]
        )

    async def test_server_proto_configure_01(self):
        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                'invalid setting value type'):
            await self.con.execute('''
                CONFIGURE SESSION SET __internal_no_const_folding := 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            await self.con.execute('''
                CONFIGURE SESSION SET __internal_no_const_folding := false;
                CONFIGURE INSTANCE SET __internal_testvalue := 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            await self.con.execute('''
                CONFIGURE INSTANCE SET __internal_testvalue := 1;
                CONFIGURE SESSION SET __internal_no_const_folding := false;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            async with self.con.transaction():
                await self.con.query('''
                    CONFIGURE INSTANCE SET __internal_testvalue := 1;
                ''')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'CONFIGURE SESSION INSERT is not supported'):
            await self.con.query('''
                CONFIGURE SESSION INSERT TestSessionConfig { name := 'foo' };
            ''')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                'CONFIGURE DATABASE INSERT is not supported'):
            await self.con.query('''
                CONFIGURE CURRENT DATABASE
                INSERT TestSessionConfig { name := 'foo' };
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'module must be either \'cfg\' or empty'):
            await self.con.query('''
                CONFIGURE INSTANCE INSERT cf::TestInstanceConfig {
                    name := 'foo'
                };
            ''')

    async def test_server_proto_configure_02(self):
        conf = await self.con.query_single('''
            SELECT cfg::Config.__internal_testvalue LIMIT 1
        ''')
        self.assertEqual(conf, 0)

        jsonconf = await self.con.query_single('''
            SELECT cfg::get_config_json()
        ''')

        all_conf = json.loads(jsonconf)
        conf = all_conf['__internal_testvalue']

        self.assertEqual(conf['value'], 0)
        self.assertEqual(conf['source'], 'default')

        try:
            await self.con.query('''
                CONFIGURE SESSION SET multiprop := {"one", "two"};
            ''')

            # The "Configure" is spelled the way it's spelled on purpose
            # to test that we handle keywords in a case-insensitive manner
            # in constant extraction code.
            await self.con.query('''
                Configure INSTANCE SET __internal_testvalue := 1;
            ''')

            conf = await self.con.query_single('''
                SELECT cfg::Config.__internal_testvalue LIMIT 1
            ''')
            self.assertEqual(conf, 1)

            jsonconf = await self.con.query_single('''
                SELECT cfg::get_config_json()
            ''')

            all_conf = json.loads(jsonconf)

            conf = all_conf['__internal_testvalue']
            self.assertEqual(conf['value'], 1)
            self.assertEqual(conf['source'], 'system override')

            conf = all_conf['multiprop']
            self.assertEqual(set(conf['value']), {'one', 'two'})
            self.assertEqual(conf['source'], 'session')
        finally:
            await self.con.execute('''
                CONFIGURE INSTANCE RESET __internal_testvalue
            ''')
            await self.con.execute('''
                CONFIGURE SESSION RESET multiprop
            ''')

    async def test_server_proto_configure_03(self):
        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name } FILTER .name LIKE 'test_03%';
            ''',
            [],
        )

        await self.con.query('''
            CONFIGURE INSTANCE INSERT TestInstanceConfig { name := 'test_03' };
        ''')

        await self.con.query('''
            CONFIGURE INSTANCE INSERT cfg::TestInstanceConfig {
                name := 'test_03_01'
            };
        ''')

        with self.assertRaisesRegex(
            edgedb.InterfaceError,
            r'it does not return any data',
        ):
            await self.con.query_required_single('''
                CONFIGURE INSTANCE INSERT cfg::TestInstanceConfig {
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

        await self.con.query('''
            CONFIGURE INSTANCE
            RESET TestInstanceConfig FILTER .name = 'test_03';
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

        await self.con.query('''
            CONFIGURE INSTANCE
            RESET TestInstanceConfig FILTER .name = 'test_03_01';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            []
        )

        # Repeat reset that doesn't match anything this time.
        await self.con.query('''
            CONFIGURE INSTANCE
            RESET TestInstanceConfig FILTER .name = 'test_03_01';
        ''')

        await self.con.query('''
            CONFIGURE INSTANCE INSERT TestInstanceConfig {
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

        await self.con.query('''
            CONFIGURE INSTANCE INSERT TestInstanceConfig {
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

        await self.con.query('''
            CONFIGURE INSTANCE RESET TestInstanceConfig
            FILTER .obj.name IN {'foo', 'bar'} AND .name ILIKE 'test_03%';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            []
        )

        await self.con.query('''
            CONFIGURE INSTANCE INSERT TestInstanceConfig {
                name := 'test_03_' ++ <str>count(DETACHED TestInstanceConfig),
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

        await self.con.query('''
            CONFIGURE INSTANCE RESET TestInstanceConfig
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
            await self.con.query('''
                CONFIGURE SESSION INSERT TestSessionConfig {name := 'test_04'}
            ''')

        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                "unrecognized configuration object 'Unrecognized'"):
            await self.con.query('''
                CONFIGURE INSTANCE INSERT Unrecognized {name := 'test_04'}
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "must not have a FILTER clause"):
            await self.con.query('''
                CONFIGURE INSTANCE RESET __internal_testvalue FILTER 1 = 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "non-constant expression"):
            await self.con.query('''
                CONFIGURE SESSION SET __internal_testmode := (random() = 0);
            ''')

        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                "'Subclass1' cannot be configured directly"):
            await self.con.query('''
                CONFIGURE INSTANCE INSERT Subclass1 {
                    name := 'foo'
                };
            ''')

        await self.con.query('''
            CONFIGURE INSTANCE INSERT TestInstanceConfig {
                name := 'test_04',
            }
        ''')

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "TestInstanceConfig.name violates exclusivity constraint"):
            await self.con.query('''
                CONFIGURE INSTANCE INSERT TestInstanceConfig {
                    name := 'test_04',
                }
            ''')

    async def test_server_proto_configure_05(self):
        await self.con.execute('''
            CONFIGURE SESSION SET __internal_sess_testvalue := 1;
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.__internal_sess_testvalue
            ''',
            [
                1
            ],
        )

        await self.con.execute('''
            CONFIGURE CURRENT DATABASE SET __internal_sess_testvalue := 3;
        ''')

        await self.con.execute('''
            CONFIGURE INSTANCE SET __internal_sess_testvalue := 2;
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.__internal_sess_testvalue
            ''',
            [
                1  # fail
            ],
        )

        await self.assert_query_result(
            '''
            SELECT cfg::InstanceConfig.__internal_sess_testvalue
            ''',
            [
                2
            ],
        )

        await self.assert_query_result(
            '''
            SELECT cfg::DatabaseConfig.__internal_sess_testvalue
            ''',
            [
                3
            ],
        )

        await self.con.execute('''
            CONFIGURE SESSION RESET __internal_sess_testvalue;
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.__internal_sess_testvalue
            ''',
            [
                3
            ],
        )

        await self.con.execute('''
            CONFIGURE CURRENT DATABASE RESET __internal_sess_testvalue;
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.__internal_sess_testvalue
            ''',
            [
                2
            ],
        )

    async def test_server_proto_configure_06(self):
        try:
            await self.con.execute('''
                CONFIGURE SESSION SET singleprop := '42';
            ''')

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
                CONFIGURE INSTANCE SET multiprop := {'4', '5'};
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
                CONFIGURE INSTANCE RESET multiprop;
            ''')

    async def test_server_proto_configure_07(self):
        try:
            await self.con.execute('''
                CONFIGURE SESSION SET multiprop := <str>{};
            ''')

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [],
            )

            await self.con.execute('''
                CONFIGURE INSTANCE SET multiprop := {'4'};
            ''')

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [],
            )

            await self.con.execute('''
                CONFIGURE SESSION SET multiprop := {'5'};
            ''')

            await self.assert_query_result(
                '''
                SELECT _ := cfg::Config.multiprop ORDER BY _
                ''',
                [
                    '5',
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
                    '4',
                ],
            )

        finally:
            await self.con.execute('''
                CONFIGURE SESSION RESET multiprop;
            ''')

            await self.con.execute('''
                CONFIGURE INSTANCE RESET multiprop;
            ''')

    async def test_server_proto_configure_describe_system_config(self):
        try:
            conf1 = "CONFIGURE INSTANCE SET singleprop := '1337';"
            await self.con.execute(conf1)

            conf2 = textwrap.dedent('''\
                CONFIGURE INSTANCE INSERT cfg::TestInstanceConfig {
                    name := 'test_describe',
                    obj := (
                        (INSERT cfg::Subclass1 {
                            name := 'foo',
                            sub1 := 'sub1',
                        })
                    ),
                };
            ''')
            await self.con.execute(conf2)

            conf3 = "CONFIGURE SESSION SET singleprop := '42';"
            await self.con.execute(conf3)

            conf4 = "CONFIGURE INSTANCE SET memprop := <cfg::memory>'100MiB';"
            await self.con.execute(conf4)

            res = await self.con.query_single('DESCRIBE INSTANCE CONFIG;')
            self.assertIn(conf1, res)
            self.assertIn(conf2, res)
            self.assertNotIn(conf3, res)
            self.assertIn(conf4, res)

        finally:
            await self.con.execute('''
                CONFIGURE INSTANCE
                RESET TestInstanceConfig FILTER .name = 'test_describe'
            ''')
            await self.con.execute('''
                CONFIGURE INSTANCE RESET singleprop;
            ''')
            await self.con.execute('''
                CONFIGURE INSTANCE RESET memprop;
            ''')

    async def test_server_proto_configure_describe_database_config(self):
        try:
            conf1 = (
                "CONFIGURE CURRENT DATABASE "
                "SET singleprop := '1337';"
            )
            await self.con.execute(conf1)

            conf2 = "CONFIGURE SESSION SET singleprop := '42';"
            await self.con.execute(conf2)

            res = await self.con.query_single(
                'DESCRIBE CURRENT DATABASE CONFIG;')
            self.assertIn(conf1, res)
            self.assertNotIn(conf2, res)

        finally:
            await self.con.execute('''
                CONFIGURE CURRENT DATABASE RESET singleprop;
            ''')

    async def test_server_version(self):
        srv_ver = await self.con.query_single(r"""
            SELECT sys::get_version()
        """)

        ver = buildmeta.get_version()

        self.assertEqual(
            (ver.major, ver.minor, ver.stage.name.lower(),
             ver.stage_no,),
            (srv_ver.major, srv_ver.minor, str(srv_ver.stage),
             srv_ver.stage_no,)
        )

    async def test_server_proto_configure_invalid_duration(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                "invalid input syntax for type std::duration: "
                "unable to parse '12mse'"):
            await self.con.execute('''
                configure session set
                    durprop := <duration>'12mse'
            ''')

        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                r"invalid setting value type for durprop: "
                r"'std::str' \(expecting 'std::duration"):
            await self.con.execute('''
                configure instance set
                    durprop := '12 seconds'
            ''')

    async def test_server_proto_configure_compilation(self):
        try:
            await self.con.execute('''
                CREATE TYPE Foo;
            ''')

            async with self.assertRaisesRegexTx(
                edgedb.InvalidFunctionDefinitionError,
                'data-modifying statements are not allowed in function bodies'
            ):
                await self.con.execute('''
                    CREATE FUNCTION foo() -> Foo USING (INSERT Foo);
                ''')

            async with self._run_and_rollback():
                await self.con.execute('''
                    CONFIGURE SESSION SET allow_dml_in_functions := true;
                ''')

                await self.con.execute('''
                    CREATE FUNCTION foo() -> Foo USING (INSERT Foo);
                ''')

            async with self.assertRaisesRegexTx(
                edgedb.InvalidFunctionDefinitionError,
                'data-modifying statements are not allowed in function bodies'
            ):
                await self.con.execute('''
                    CREATE FUNCTION foo() -> Foo USING (INSERT Foo);
                ''')

            async with self._run_and_rollback():
                # Session prohibits DML in functions.
                await self.con.execute('''
                    CONFIGURE SESSION SET allow_dml_in_functions := false;
                ''')

                # Database allows it.
                await self.con.execute('''
                    CONFIGURE CURRENT DATABASE
                        SET allow_dml_in_functions := true;
                ''')

                # Session wins.
                async with self.assertRaisesRegexTx(
                    edgedb.InvalidFunctionDefinitionError,
                    'data-modifying statements are not'
                    ' allowed in function bodies'
                ):
                    await self.con.execute('''
                        CREATE FUNCTION foo() -> Foo USING (INSERT Foo);
                    ''')

                await self.con.execute('''
                    CONFIGURE SESSION RESET allow_dml_in_functions;
                ''')

                # Now OK.
                await self.con.execute('''
                    CREATE FUNCTION foo() -> Foo USING (INSERT Foo);
                ''')

        finally:
            await self.con.execute('''
                DROP TYPE Foo;
            ''')


class TestSeparateCluster(tb.TestCase):

    @unittest.skipIf(
        platform.system() == "Darwin",
        "loopback aliases aren't set up on macOS by default"
    )
    async def test_server_proto_configure_listen_addresses(self):
        con1 = con2 = con3 = con4 = con5 = None

        async with tb.start_edgedb_server() as sd:
            try:
                with self.assertRaises(
                    edgedb.ClientConnectionFailedTemporarilyError
                ):
                    await sd.connect(
                        host="127.0.0.2", timeout=1, wait_until_available=1
                    )
                con1 = await sd.connect()
                await con1.execute("""
                    CONFIGURE INSTANCE SET listen_addresses := {
                        '127.0.0.2',
                    };
                """)

                con2 = await sd.connect(host="127.0.0.2")

                self.assertEqual(await con1.query_single("SELECT 1"), 1)
                self.assertEqual(await con2.query_single("SELECT 2"), 2)

                with self.assertRaises(
                    edgedb.ClientConnectionFailedTemporarilyError
                ):
                    await sd.connect(timeout=1, wait_until_available=1)

                await con1.execute("""
                    CONFIGURE INSTANCE SET listen_addresses := {
                        '127.0.0.1', '127.0.0.2',
                    };
                """)
                con3 = await sd.connect()

                for i, con in enumerate((con1, con2, con3)):
                    self.assertEqual(await con.query_single(f"SELECT {i}"), i)

                await con1.execute("""
                    CONFIGURE INSTANCE SET listen_addresses := <str>{};
                """)
                await con1.execute("""
                    CONFIGURE INSTANCE SET listen_addresses := {
                        '0.0.0.0',
                    };
                """)
                con4 = await sd.connect()
                con5 = await sd.connect(host="127.0.0.2")

                for i, con in enumerate((con1, con2, con3, con4, con5)):
                    self.assertEqual(await con.query_single(f"SELECT {i}"), i)

            finally:
                closings = []
                for con in (con1, con2, con3, con4, con5):
                    if con is not None:
                        closings.append(con.aclose())
                await asyncio.gather(*closings)

    async def test_server_config_idle_connection_01(self):
        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            active_cons = []
            idle_cons = []
            for i in range(20):
                if i % 2:
                    active_cons.append(await sd.connect())
                else:
                    idle_cons.append(await sd.connect())

            # Set the timeout to 5 seconds.
            await idle_cons[0].execute('''
                configure system set session_idle_timeout := <duration>'5s'
            ''')

            for _ in range(5):
                random.shuffle(active_cons)
                await asyncio.gather(
                    *(con.query('SELECT 1') for con in active_cons)
                )
                await asyncio.sleep(3)

            metrics = sd.fetch_metrics()

            await asyncio.gather(
                *(con.aclose() for con in active_cons)
            )

        self.assertIn(
            f'\nedgedb_server_client_connections_idle_total ' +
            f'{float(len(idle_cons))}\n',
            metrics
        )

    async def test_server_config_idle_connection_02(self):
        from edb import protocol

        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            conn = await sd.connect_test_protocol()

            await conn.simple_query('''
                configure system set session_idle_timeout := <duration>'10ms'
            ''')

            await asyncio.sleep(1)

            await conn.recv_match(
                protocol.ErrorResponse,
                message='closing the connection due to idling'
            )

    async def test_server_config_db_config(self):
        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            con1 = await sd.connect()
            con2 = await sd.connect()

            await con1.execute('''
                configure current database set __internal_sess_testvalue := 0;
            ''')

            await con2.execute('''
                configure current database set __internal_sess_testvalue := 5;
            ''')

            # Check that the DB (Backend) was updated.
            conf = await con2.query_single('''
                SELECT assert_single(cfg::Config.__internal_sess_testvalue)
            ''')
            self.assertEqual(conf, 5)
            # The changes should be immediately visible at EdgeQL level
            # in concurrent transactions.
            conf = await con1.query_single('''
                SELECT assert_single(cfg::Config.__internal_sess_testvalue)
            ''')
            self.assertEqual(conf, 5)

            # Use `try_until_succeeds` because it might take the server a few
            # seconds on slow CI to reload the DB config in the server process.
            async for tr in self.try_until_succeeds(
                    ignore=AssertionError):
                async with tr:
                    info = sd.fetch_server_info()
                    dbconf = info['databases']['edgedb']['config']
                    self.assertEqual(
                        dbconf.get('__internal_sess_testvalue'), 5)

            # Now check that the server state is updated when a configure
            # command is in a transaction.

            async for tx in con1.retrying_transaction():
                async with tx:
                    await tx.execute('''
                        configure current database set
                            __internal_sess_testvalue := 10;
                    ''')

            async for tr in self.try_until_succeeds(
                    ignore=AssertionError):
                async with tr:
                    info = sd.fetch_server_info()
                    dbconf = info['databases']['edgedb']['config']
                    self.assertEqual(
                        dbconf.get('__internal_sess_testvalue'), 10)

    async def test_server_config_backend_levels(self):

        async def assert_conf(con, name, expected_val):
            val = await con.query_single(f'''
                select assert_single(cfg::Config.{name})
            ''')

            if isinstance(val, datetime.timedelta):
                val //= datetime.timedelta(milliseconds=1)

            self.assertEqual(
                val,
                expected_val
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            async with tb.start_edgedb_server(
                data_dir=tmpdir,
                security=args.ServerSecurityMode.InsecureDevMode,
            ) as sd:
                c1 = await sd.connect()
                c2 = await sd.connect()

                await c2.query('create database test')
                t1 = await sd.connect(database='test')
                t2 = await sd.connect(database='test')

                # check that the default was set correctly
                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 10000)

                ####

                await c1.query('''
                    configure instance set
                        session_idle_transaction_timeout :=
                            <duration>'20s'
                ''')

                for c in {c1, c2, t1}:
                    await assert_conf(
                        c, 'session_idle_transaction_timeout', 20000)

                ####

                await t1.query('''
                    configure current database set
                        session_idle_transaction_timeout :=
                            <duration>'30000ms'
                ''')

                for c in {c1, c2}:
                    await assert_conf(
                        c, 'session_idle_transaction_timeout', 20000)

                await assert_conf(
                    t1, 'session_idle_transaction_timeout', 30000)

                ####

                await c2.query('''
                    configure session set
                        session_idle_transaction_timeout :=
                            <duration>'40000000us'
                ''')
                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 20000)
                await assert_conf(
                    t1, 'session_idle_transaction_timeout', 30000)
                await assert_conf(
                    c2, 'session_idle_transaction_timeout', 40000)

                ####

                await t1.query('''
                    configure session set
                        session_idle_transaction_timeout :=
                            <duration>'50 seconds'
                ''')
                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 20000)
                await assert_conf(
                    t1, 'session_idle_transaction_timeout', 50000)
                await assert_conf(
                    c2, 'session_idle_transaction_timeout', 40000)

                ####

                await c1.query('''
                    configure instance set
                        session_idle_transaction_timeout :=
                            <duration>'15000 milliseconds'
                ''')

                await c2.query('''
                    configure session reset
                        session_idle_transaction_timeout;
                ''')

                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 15000)
                await assert_conf(
                    t1, 'session_idle_transaction_timeout', 50000)
                await assert_conf(
                    t2, 'session_idle_transaction_timeout', 30000)
                await assert_conf(
                    c2, 'session_idle_transaction_timeout', 15000)

                ####

                await t1.query('''
                    configure session reset
                        session_idle_transaction_timeout;
                ''')

                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 15000)
                await assert_conf(
                    t1, 'session_idle_transaction_timeout', 30000)
                await assert_conf(
                    c2, 'session_idle_transaction_timeout', 15000)

                ####

                await t2.query('''
                    configure current database reset
                        session_idle_transaction_timeout;
                ''')

                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 15000)
                await assert_conf(
                    t1, 'session_idle_transaction_timeout', 15000)
                await assert_conf(
                    t2, 'session_idle_transaction_timeout', 15000)
                await assert_conf(
                    c2, 'session_idle_transaction_timeout', 15000)

                ####

                cur_shared = await c1.query('''
                    select <str>cfg::Config.shared_buffers
                ''')

                # not a test, just need to make sure our random value
                # does not happen to be the Postgres' default.
                assert cur_shared != ['20002KiB']

                await c1.query('''
                    configure instance set
                        shared_buffers := <cfg::memory>'20002KiB'
                ''')

                # shared_buffers requires a restart, so the value shouldn't
                # change just yet
                self.assertEqual(
                    await c1.query_single(f'''
                        select assert_single(<str>cfg::Config.shared_buffers)
                    '''),
                    cur_shared[0]
                )

                await c1.aclose()
                await c2.aclose()
                await t1.aclose()

            async with tb.start_edgedb_server(
                data_dir=tmpdir,
                security=args.ServerSecurityMode.InsecureDevMode,
            ) as sd:

                c1 = await sd.connect()

                # check that the default was set correctly
                await assert_conf(
                    c1, 'session_idle_transaction_timeout', 15000)

                self.assertEqual(
                    await c1.query_single(f'''
                        select assert_single(<str>cfg::Config.shared_buffers)
                    '''),
                    '20002KiB'
                )

                await c1.aclose()

    async def test_server_config_idle_transaction(self):
        from edb import protocol

        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            conn = await sd.connect_test_protocol()

            await conn.simple_query('''
                configure session set
                    session_idle_transaction_timeout :=
                        <duration>'1 second'
            ''')

            await conn.simple_query('''
                start transaction
            ''')

            await conn.simple_query('''
                select sys::_sleep(4)
            ''')

            await conn.recv_match(
                protocol.ErrorResponse,
                message='terminating connection due to '
                        'idle-in-transaction timeout'
            )

            with self.assertRaises(edgedb.ClientConnectionClosedError):
                await conn.simple_query('''
                    select 1
                ''')

            data = sd.fetch_metrics()

            # Postgres: ERROR_IDLE_IN_TRANSACTION_TIMEOUT=25P03
            self.assertIn(
                '\nedgedb_server_backend_connections_aborted_total' +
                '{pgcode="25P03"} 1.0\n',
                data
            )

    async def test_server_config_query_timeout(self):
        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            conn = await sd.connect()

            await conn.execute('''
                configure session set
                    query_execution_timeout :=
                        <duration>'1 second'
            ''')

            for _ in range(2):
                with self.assertRaisesRegex(
                        edgedb.QueryError,
                        'canceling statement due to statement timeout'):
                    await conn.execute('''
                        select sys::_sleep(4)
                    ''')

                self.assertEqual(
                    await conn.query_single('select 42'),
                    42
                )

            await conn.aclose()

            data = sd.fetch_metrics()
            self.assertNotIn(
                '\nedgedb_server_backend_connections_aborted_total',
                data
            )
