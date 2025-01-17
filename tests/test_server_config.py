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
import datetime
import enum
import json
import os
import platform
import random
import signal
import tempfile
import textwrap
import unittest

import immutables

import edgedb

from edb import buildmeta
from edb import errors
from edb.edgeql import qltypes
from edb.edgeql import quote as qlquote
from edb.protocol import messages

from edb.testbase import server as tb
from edb.schema import objects as s_obj

from edb.ir import statypes


from edb.server import args
from edb.server import cluster
from edb.server import config
from edb.server.config import ops
from edb.server.config import spec
from edb.server.config import types
from edb.tools import test


def make_port_value(*, protocol='graphql+http',
                    database='testdb',
                    user='test',
                    concurrency=4,
                    port=1000,
                    address=None,
                    **kwargs):
    if address is None:
        address = frozenset([f'localhost/{database}'])
    return dict(
        protocol=protocol, user=user, database=database,
        concurrency=concurrency, port=port, address=address, **kwargs)


Field = statypes.CompositeTypeSpecField


def _mk_fields(*fields):
    return immutables.Map({f.name: f for f in fields})


Port = types.ConfigTypeSpec(
    name='Port',
    fields=_mk_fields(
        Field('protocol', str),
        Field('database', str, unique=True),
        Field('port', int),
        Field('concurrency', int),
        Field('user', str),
        Field('address',
              frozenset[str], default=frozenset({'localhost'}), unique=True),
    ),
)


testspec1 = spec.FlatSpec(
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
        default=Port(**make_port_value())),

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
                    'value': [testspec1['port'].default.to_json_value()],
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
                Port.from_pyvalue(
                    make_port_value(database='f1'), spec=testspec1),
                Port.from_pyvalue(
                    make_port_value(database='f2'), spec=testspec1),
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
                Port.from_pyvalue(
                    make_port_value(database='f2'),
                    spec=testspec1),
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
            Port.from_pyvalue(
                make_port_value(address='aaa'), spec=testspec1),
            Port.from_pyvalue(
                make_port_value(address=['aaa']), spec=testspec1))

        self.assertEqual(
            Port.from_pyvalue(
                make_port_value(address=['aaa', 'bbb']), spec=testspec1),
            Port.from_pyvalue(
                make_port_value(address=['bbb', 'aaa']), spec=testspec1))

        self.assertNotEqual(
            Port.from_pyvalue(
                make_port_value(address=['aaa', 'bbb']), spec=testspec1),
            Port.from_pyvalue(
                make_port_value(address=['bbb', 'aa1']), spec=testspec1))

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
                CONFIGURE SESSION SET __internal_sess_testvalue := 'test';
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            await self.con.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 10;
                CONFIGURE INSTANCE SET __internal_testvalue := 1;
            ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                'cannot be executed in a transaction block'):
            await self.con.execute('''
                CONFIGURE INSTANCE SET __internal_testvalue := 1;
                CONFIGURE SESSION SET __internal_sess_testvalue := 10;
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
                edgedb.ConfigurationError,
                'unrecognized configuration object'):
            await self.con.query('''
                CONFIGURE INSTANCE INSERT cf::TestInstanceConfig {
                    name := 'foo'
                };
            ''')

        props = {str(x) for x in range(500)}
        with self.assertRaisesRegex(
                edgedb.ConfigurationError,
                'too large'):
            await self.con.query(f'''
                CONFIGURE SESSION SET multiprop := {props};
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

    async def _server_proto_configure_03(self, scope, base_result=None):
        # scope is either INSTANCE or CURRENT DATABASE

        # when scope is CURRENT DATABASE, base_result can be an INSTANCE
        # config that should be shadowed whenever there is a database config
        if base_result is None:
            base_result = []

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name } FILTER .name LIKE 'test_03%';
            ''',
            base_result,
        )

        await self.con.query(f'''
            CONFIGURE {scope} INSERT TestInstanceConfig {{
                name := 'test_03'
            }};
        ''')

        await self.con.query(f'''
            CONFIGURE {scope} INSERT cfg::TestInstanceConfig {{
                name := 'test_03_01'
            }};
        ''')

        with self.assertRaisesRegex(
            edgedb.InterfaceError,
            r'it does not return any data',
        ):
            await self.con.query_required_single(f'''
                CONFIGURE {scope} INSERT cfg::TestInstanceConfig {{
                    name := 'test_03_0122222222'
                }};
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

        await self.con.query(f'''
            CONFIGURE {scope}
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

        await self.con.query(f'''
            CONFIGURE {scope}
            RESET TestInstanceConfig FILTER .name = 'test_03_01';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            base_result,
        )

        # Repeat reset that doesn't match anything this time.
        await self.con.query(f'''
            CONFIGURE {scope}
            RESET TestInstanceConfig FILTER .name = 'test_03_01';
        ''')

        await self.con.query(f'''
            CONFIGURE {scope} INSERT TestInstanceConfig {{
                name := 'test_03',
                obj := (INSERT Subclass1 {{ name := 'foo', sub1 := 'sub1' }})
            }}
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

        await self.con.query(f'''
            CONFIGURE {scope} INSERT TestInstanceConfig {{
                name := 'test_03_01',
                obj := (INSERT Subclass2 {{ name := 'bar', sub2 := 'sub2' }})
            }}
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

        await self.con.query(f'''
            CONFIGURE {scope} RESET TestInstanceConfig
            FILTER .obj.name IN {{'foo', 'bar'}} AND .name ILIKE 'test_03%';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            base_result,
        )

        await self.con.query(f'''
            CONFIGURE {scope} INSERT TestInstanceConfig {{
                name := 'test_03_' ++ <str>count(DETACHED TestInstanceConfig),
            }}
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

        await self.con.query(f'''
            CONFIGURE {scope} INSERT TestInstanceConfigStatTypes {{
                name := 'test_03_02',
                memprop := <cfg::memory>'108MiB',
                durprop := <duration>'108 seconds',
            }}
        ''')
        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj {
                name,
                [IS cfg::TestInstanceConfigStatTypes].memprop,
                [IS cfg::TestInstanceConfigStatTypes].durprop,
            }
            FILTER .name = 'test_03_02';
            ''',
            [
                {
                    'name': 'test_03_02',
                    'memprop': '108MiB',
                    'durprop': 'PT1M48S',
                },
            ],
            [
                {
                    'name': 'test_03_02',
                    'memprop': '108MiB',
                    'durprop': datetime.timedelta(seconds=108),
                },
            ],
        )

        await self.con.query(f'''
            CONFIGURE {scope} RESET TestInstanceConfig
            FILTER .name ILIKE 'test_03%';
        ''')

        await self.assert_query_result(
            '''
            SELECT cfg::Config.sysobj { name }
            FILTER .name LIKE 'test_03%';
            ''',
            base_result,
        )

    async def test_server_proto_configure_03a(self):
        await self._server_proto_configure_03('CURRENT DATABASE')

    async def test_server_proto_configure_03b(self):
        await self._server_proto_configure_03('INSTANCE')

    async def test_server_proto_configure_03c(self):
        await self.con.query('''
            CONFIGURE INSTANCE INSERT TestInstanceConfig {
                name := 'test_03_base'
            };
        ''')

        await self._server_proto_configure_03(
            'CURRENT DATABASE', [{'name': 'test_03_base'}]
        )

        await self.con.query('''
            CONFIGURE INSTANCE RESET TestInstanceConfig;
        ''')

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
            SELECT cfg::BranchConfig.__internal_sess_testvalue
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
        con2 = None
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

            con2 = await self.connect(database=self.con.dbname)
            await con2.execute('''
                start transaction
            ''')

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
            if con2:
                await con2.aclose()

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

    async def test_server_proto_configure_08(self):
        with self.assertRaisesRegex(
            edgedb.ConfigurationError, 'invalid setting value'
        ):
            await self.con.execute('''
                CONFIGURE INSTANCE SET _pg_prepared_statement_cache_size := -5;
            ''')
        with self.assertRaisesRegex(
            edgedb.ConfigurationError, 'invalid setting value'
        ):
            await self.con.execute('''
                CONFIGURE INSTANCE SET _pg_prepared_statement_cache_size := 0;
            ''')

        try:
            await self.con.execute('''
                CONFIGURE INSTANCE SET _pg_prepared_statement_cache_size := 42;
            ''')
            conf = await self.con.query_single('''
                SELECT cfg::Config._pg_prepared_statement_cache_size LIMIT 1
            ''')
            self.assertEqual(conf, 42)
        finally:
            await self.con.execute('''
                CONFIGURE INSTANCE RESET _pg_prepared_statement_cache_size;
            ''')

    async def test_server_proto_configure_09(self):
        con2 = await self.connect(database=self.con.dbname)
        default_value = await con2.query_single(
            'SELECT assert_single(cfg::Config).boolprop'
        )
        try:
            for value in [True, False, True, False]:
                await self.con.execute(f'''
                    CONFIGURE SESSION SET boolprop := <bool>'{value}';
                ''')
                # The first immediate query is likely NOT syncing in-memory
                # state to the backend connection, so this will test that
                # the state in the SQL temp table is correctly set.
                await self.assert_query_result(
                    '''
                    SELECT cfg::Config.boolprop
                    ''',
                    [value],
                )
                # Now change the state on the backend connection, hopefully,
                # by running a query with con2 with different state.
                self.assertEqual(
                    await con2.query_single(
                        'SELECT assert_single(cfg::Config).boolprop'
                    ),
                    default_value,
                )
                # The second query shall sync in-memory state to the backend
                # connection, so this tests if the statically evaluated bool
                # value is correct.
                await self.assert_query_result(
                    '''
                    SELECT cfg::Config.boolprop
                    ''',
                    [value],
                )
        finally:
            await con2.aclose()

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

            conf5 = "CONFIGURE INSTANCE SET enumprop := <cfg::TestEnum>'Two';"
            await self.con.execute(conf5)

            res = await self.con.query_single('DESCRIBE INSTANCE CONFIG;')
            self.assertIn(conf1, res)
            self.assertIn(conf2, res)
            self.assertNotIn(conf3, res)
            self.assertIn(conf4, res)
            self.assertIn(conf5, res)

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
                "CONFIGURE CURRENT BRANCH "
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

    async def test_server_proto_configure_invalid_enum(self):
        with self.assertRaisesRegex(
            edgedb.InvalidValueError,
            "invalid input value for enum 'cfg::TestEnum': \"foo\"",
        ):
            await self.con.execute('''
                configure session set
                    enumprop := <cfg::TestEnum>'foo'
            ''')

    async def test_server_proto_configure_compilation(self):
        try:
            await self.con.execute('''
                CREATE TYPE Foo;
            ''')

            async with self._run_and_rollback():
                await self.con.execute('''
                    CONFIGURE SESSION SET allow_bare_ddl :=
                        cfg::AllowBareDDL.NeverAllow;
                ''')

                async with self.assertRaisesRegexTx(
                    edgedb.QueryError,
                    'bare DDL statements are not allowed on this database'
                ):
                    await self.con.execute('''
                        CREATE FUNCTION intfunc() -> int64 USING (1);
                    ''')

            async with self._run_and_rollback():
                await self.con.execute('''
                    CONFIGURE SESSION SET store_migration_sdl :=
                        cfg::StoreMigrationSDL.NeverStore;
                ''')

                await self.con.execute('''
                    CREATE TYPE Bar;
                ''')

                await self.assert_query_result(
                    'select schema::Migration { sdl }',
                    [
                        {'sdl': None},
                        {'sdl': None},
                    ]
                )

            async with self._run_and_rollback():
                await self.con.execute('''
                    CONFIGURE SESSION SET store_migration_sdl :=
                        cfg::StoreMigrationSDL.AlwaysStore;
                ''')

                await self.con.execute('''
                    CREATE TYPE Bar;
                ''')

                await self.assert_query_result(
                    'select schema::Migration { sdl }',
                    [
                        {'sdl': None},
                        {'sdl': (
                            'module default {\n'
                            '    type Bar;\n'
                            '    type Foo;\n'
                            '};'
                        )},
                    ]
                )

        finally:
            await self.con.execute('''
                DROP TYPE Foo;
            ''')

    async def test_server_proto_rollback_state(self):
        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 2;
            ''')
            await con1.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 1;
            ''')
            self.assertEqual(
                await con2.query_single('''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                '''),
                2,
            )
            self.assertEqual(
                await con1.query_single('''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                '''),
                1,
            )
            with self.assertRaises(edgedb.DivisionByZeroError):
                async for tx in con2.retrying_transaction():
                    async with tx:
                        await tx.query_single("SELECT 1/0")
            self.assertEqual(
                await con1.query_single('''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                '''),
                1,
            )
        finally:
            await con2.aclose()

    async def test_server_proto_orphan_rollback_state(self):
        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 2;
            ''')
            await con1.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 1;
            ''')
            self.assertEqual(
                await con2.query_single('''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                '''),
                2,
            )
            self.assertEqual(
                await con1.query_single('''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                '''),
                1,
            )

            # an orphan ROLLBACK must not change the last_state,
            # because the implicit transaction is rolled back
            await con2.execute("ROLLBACK")

            # ... so that we can actually do the state sync again here
            self.assertEqual(
                await con2.query_single('''
                    SELECT assert_single(cfg::Config.__internal_sess_testvalue)
                '''),
                2,
            )
        finally:
            await con2.aclose()

    async def test_server_proto_configure_error(self):
        con1 = self.con
        con2 = await self.connect(database=con1.dbname)

        version_str = await con1.query_single('''
            select sys::get_version_as_str();
        ''')

        try:
            await con2.execute('''
                select 1;
            ''')

            err = {
                'type': 'SchemaError',
                'message': 'danger 1',
                'context': {'start': 42},
            }

            await con1.execute(f'''
                configure current database set force_database_error :=
                  {qlquote.quote_literal(json.dumps(err))};
            ''')

            with self.assertRaisesRegex(edgedb.SchemaError, 'danger 1'):
                async for tx in con1.retrying_transaction():
                    async with tx:
                        await tx.query('select schema::Object')

            with self.assertRaisesRegex(edgedb.SchemaError, 'danger 1',
                                        _position=42):
                async for tx in con2.retrying_transaction():
                    async with tx:
                        await tx.query('select schema::Object')

            # If we change the '_version' to something else we
            # should be good
            err = {
                'type': 'SchemaError',
                'message': 'danger 2',
                'context': {'start': 42},
                '_versions': [version_str + '1'],
            }
            await con1.execute(f'''
                configure current database set force_database_error :=
                  {qlquote.quote_literal(json.dumps(err))};
            ''')
            await con1.query('select schema::Object')

            # It should also be fine if we set a '_scopes' other than 'query'
            err = {
                'type': 'SchemaError',
                'message': 'danger 3',
                'context': {'start': 42},
                '_scopes': ['restore'],
            }
            await con1.execute(f'''
                configure current database set force_database_error :=
                  {qlquote.quote_literal(json.dumps(err))};
            ''')
            await con1.query('select schema::Object')

            # But if we make it the current version it should still fail
            err = {
                'type': 'SchemaError',
                'message': 'danger 4',
                'context': {'start': 42},
                '_versions': [version_str],
            }
            await con1.execute(f'''
                configure current database set force_database_error :=
                  {qlquote.quote_literal(json.dumps(err))};
            ''')
            with self.assertRaisesRegex(edgedb.SchemaError, 'danger 4'):
                async for tx in con2.retrying_transaction():
                    async with tx:
                        await tx.query('select schema::Object')

            with self.assertRaisesRegex(edgedb.SchemaError, 'danger 4'):
                async for tx in con1.retrying_transaction():
                    async with tx:
                        await tx.query('select schema::Object')

            await con2.execute(f'''
                configure session set force_database_error := "false";
            ''')
            await con2.query('select schema::Object')

        finally:
            try:
                await con1.execute(f'''
                    configure current database reset force_database_error;
                ''')
                await con2.execute(f'''
                    configure session reset force_database_error;
                ''')

                # Make sure both connections are working.
                await con2.execute('select 1')
                await con1.execute('select 1')
            finally:
                await con2.aclose()

    async def test_server_proto_non_transactional_pg_14_7(self):
        con1 = self.con
        con2 = await self.connect(database=con1.dbname)
        try:
            await con2.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 2;
            ''')
            await con1.execute('''
                CONFIGURE SESSION SET __internal_sess_testvalue := 1;
            ''')
            await con2.execute('''
                CREATE DATABASE pg_14_7;
            ''')
        finally:
            await con2.aclose()
            await con1.execute('''
                CONFIGURE SESSION RESET __internal_sess_testvalue;
            ''')
            await con1.execute('''
                DROP DATABASE pg_14_7;
            ''')

    async def test_server_proto_recompile_on_db_config(self):
        await self.con.execute("create type RecompileOnDBConfig;")
        try:
            # We need the retries here because the 2 `configure database` may
            # race with each other and cause temporary inconsistency
            await self.con.execute('''
                configure current database set allow_user_specified_id := true;
            ''')
            async for tr in self.try_until_succeeds(
                ignore=edgedb.QueryError,
                ignore_regexp="cannot assign to property 'id'",
            ):
                async with tr:
                    await self.con.execute('''
                        insert RecompileOnDBConfig {
                            id := <uuid>'8c425e34-d1c3-11ee-8c78-8f34556d1111'
                        };
                    ''')

            await self.con.execute('''
                configure current database set allow_user_specified_id := false;
            ''')
            async for tr in self.try_until_fails(
                wait_for=edgedb.QueryError,
                wait_for_regexp="cannot assign to property 'id'",
            ):
                async with tr:
                    await self.con.execute('''
                        insert RecompileOnDBConfig {
                            id := <uuid>
                            '8c425e34-d1c3-11ee-8c78-8f34556d2222'
                        };
                    ''')
                    await self.con.execute('''
                        delete RecompileOnDBConfig;
                    ''')
        finally:
            await self.con.execute('''
                configure current database reset allow_user_specified_id;
            ''')
            await self.con.execute("drop type RecompileOnDBConfig;")

    async def test_server_proto_remember_pgcon_state(self):
        query = 'SELECT assert_single(cfg::Config.__internal_sess_testvalue)'
        con1 = await self.connect(database=self.con.dbname)
        con2 = await self.connect(database=self.con.dbname)
        try:
            # make sure the default state is remembered in a pgcon
            async with con1.transaction():
                # `transaction()` is used as a fail-safe to store the state in
                # pgcon, in case the query itself failed to do so by mistake
                default = await con1.query_single(query)

            # update the state in most-likely the same pgcon
            await con2.execute(f'''
                CONFIGURE SESSION
                SET __internal_sess_testvalue := {default + 1};
            ''')

            # verify the state is successfully updated
            self.assertEqual(await con2.query_single(query), default + 1)

            # now switch back to the default state in con1 with the same pgcon
            self.assertEqual(await con1.query_single(query), default)
        finally:
            await con1.aclose()
            await con2.aclose()


class TestSeparateCluster(tb.TestCaseWithHttpClient):

    @unittest.skipIf(
        platform.system() == "Darwin",
        "loopback aliases aren't set up on macOS by default"
    )
    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
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

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE SYSTEM in multi-tenant mode",
    )
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

        self.assertRegex(
            metrics,
            r'\nedgedb_server_client_connections_idle_total\{.*\} ' +
            f'{float(len(idle_cons))}\\n',
        )

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE SYSTEM in multi-tenant mode",
    )
    async def test_server_config_idle_connection_02(self):
        from edb import protocol

        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            conn = await sd.connect_test_protocol()

            await conn.execute('''
                configure system set session_idle_timeout := <duration>'5010ms'
            ''')

            # Check new connections are fed with the new value
            async for tr in self.try_until_succeeds(ignore=AssertionError):
                async with tr:
                    con = await sd.connect()
                    try:
                        sysconfig = con.get_settings()["system_config"]
                        self.assertEqual(
                            sysconfig.session_idle_timeout,
                            datetime.timedelta(milliseconds=5010),
                        )
                    finally:
                        await con.aclose()

            await conn.execute('''
                configure system set session_idle_timeout := <duration>'10ms'
            ''')

            await asyncio.sleep(1)

            msg = await conn.recv_match(
                protocol.ErrorResponse,
                message='closing the connection due to idling'
            )

            # Resolve error code before comparing for better error messages
            errcls = errors.EdgeDBError.get_error_class_from_code(
                msg.error_code)
            self.assertEqual(errcls, errors.IdleSessionTimeoutError)

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

            DB = 'main'

            # Use `try_until_succeeds` because it might take the server a few
            # seconds on slow CI to reload the DB config in the server process.
            async for tr in self.try_until_succeeds(
                    ignore=AssertionError):
                async with tr:
                    info = sd.fetch_server_info()
                    if 'databases' in info:
                        databases = info['databases']
                    else:
                        databases = info['tenants']['localhost']['databases']
                    dbconf = databases[DB]['config']
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
                    if 'databases' in info:
                        databases = info['databases']
                    else:
                        databases = info['tenants']['localhost']['databases']
                    dbconf = databases[DB]['config']
                    self.assertEqual(
                        dbconf.get('__internal_sess_testvalue'), 10)

    async def test_server_config_default_branch_01(self):
        # Test default branch configuration and default branch
        # connection fallback behavior.
        # TODO: The python bindings don't support the branch argument
        # and the __default__ behavior, so we don't test that yet.
        # We do test all the different combos for HTTP, though.

        DBNAME = 'asdf'
        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
            security=args.ServerSecurityMode.InsecureDevMode,
            default_branch=DBNAME,
        ) as sd:
            def check(mode, name, current, ok=True):
                with self.http_con(sd) as hcon:
                    res, _, code = self.http_con_request(
                        hcon,
                        method='POST',
                        body=json.dumps(dict(query=qry)).encode(),
                        headers={'Content-Type': 'application/json'},
                        path=f'/{mode}/{name}/edgeql',
                    )
                    if ok:
                        self.assertEqual(code, 200, f'Request failed: {res}')
                        self.assertEqual(
                            json.loads(res).get('data'),
                            [current],
                        )
                    else:
                        self.assertEqual(
                            code, 404, f'Expected 404, got: {code}/{res}')

            # Since 'edgedb' doesn't exist, trying to connect to it
            # should route to our default database instead.
            con = await sd.connect(database='edgedb')
            await con.query('CREATE EXTENSION edgeql_http')

            qry = '''
                select sys::get_current_database()
            '''

            dbname = await con.query_single(qry)
            self.assertEqual(dbname, DBNAME)

            check('db', 'edgedb', DBNAME)
            check('branch', '__default__', DBNAME)
            check('db', '__default__', DBNAME, ok=False)
            check('branch', 'edgedb', DBNAME, ok=False)

            await con.query_single('''
                create database edgedb
            ''')
            await con.aclose()

            # Now that 'edgedb' exists, we should connect to it
            con = await sd.connect(database='edgedb')
            await con.query('CREATE EXTENSION edgeql_http')
            dbname = await con.query_single(qry)
            self.assertEqual(dbname, 'edgedb')
            await con.aclose()

            check('db', 'edgedb', 'edgedb')
            check('branch', '__default__', DBNAME)
            check('db', '__default__', DBNAME, ok=False)
            check('branch', 'edgedb', 'edgedb')

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
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

                cur_eff = await c1.query_single('''
                    select assert_single(cfg::Config.effective_io_concurrency)
                ''')
                await c1.query(f'''
                    configure instance set
                        effective_io_concurrency := {cur_eff}
                ''')

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

            query = '''
                configure session set
                    session_idle_transaction_timeout :=
                        <duration>'1 second'
            '''
            await conn.send(
                messages.Execute(
                    annotations=[],
                    command_text=query,
                    input_language=messages.InputLanguage.EDGEQL,
                    output_format=messages.OutputFormat.NONE,
                    expected_cardinality=messages.Cardinality.MANY,
                    allowed_capabilities=messages.Capability.ALL,
                    compilation_flags=messages.CompilationFlag(9),
                    implicit_limit=0,
                    input_typedesc_id=b'\0' * 16,
                    output_typedesc_id=b'\0' * 16,
                    state_typedesc_id=b'\0' * 16,
                    arguments=b'',
                    state_data=b'',
                ),
                messages.Sync(),
            )
            state_msg = await conn.recv_match(messages.CommandComplete)
            await conn.recv_match(messages.ReadyForCommand)

            await conn.execute(
                '''
                start transaction
                ''',
                state_id=state_msg.state_typedesc_id,
                state=state_msg.state_data,
            )

            msg = await asyncio.wait_for(conn.recv_match(
                protocol.ErrorResponse,
                message='terminating connection due to '
                        'idle-in-transaction timeout'
            ), 8)

            # Resolve error code before comparing for better error messages
            errcls = errors.EdgeDBError.get_error_class_from_code(
                msg.error_code)
            self.assertEqual(errcls, errors.IdleTransactionTimeoutError)

            with self.assertRaises((
                edgedb.ClientConnectionFailedTemporarilyError,
                edgedb.ClientConnectionClosedError,
            )):
                await conn.execute('''
                    select 1
                ''')

            data = sd.fetch_metrics()

            # Postgres: ERROR_IDLE_IN_TRANSACTION_TIMEOUT=25P03
            self.assertRegex(
                data,
                r'\nedgedb_server_backend_connections_aborted_total' +
                r'\{.*pgcode="25P03"\} 1.0\n',
            )

    async def test_server_config_query_timeout(self):
        async with tb.start_edgedb_server(
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            conn = await sd.connect()

            KEY = 'edgedb_server_backend_connections_aborted_total'

            data = sd.fetch_metrics()
            orig_aborted = 0.0
            for line in data.split('\n'):
                if line.startswith(KEY):
                    orig_aborted += float(line.split(' ')[1])

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
            new_aborted = 0.0
            for line in data.split('\n'):
                if line.startswith(KEY):
                    new_aborted += float(line.split(' ')[1])
            self.assertEqual(orig_aborted, new_aborted)


class TestStaticServerConfig(tb.TestCase):
    @test.xerror("static config args not supported")
    async def test_server_config_args_01(self):
        async with tb.start_edgedb_server(
            extra_args=[
                "--config-cfg::session_idle_timeout", "2m18s",
                "--config-cfg::query_execution_timeout", "609",
                "--config-no-cfg::apply_access_policies",
            ]
        ) as sd:
            conn = await sd.connect()
            try:
                sysconfig = conn.get_settings()["system_config"]
                self.assertEqual(
                    sysconfig.session_idle_timeout,
                    datetime.timedelta(minutes=2, seconds=18),
                )

                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.session_idle_timeout)
                    """),
                    datetime.timedelta(minutes=2, seconds=18),
                )
                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(
                            cfg::Config.query_execution_timeout)
                    """),
                    datetime.timedelta(seconds=609),
                )
                self.assertFalse(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.apply_access_policies)
                    """)
                )
            finally:
                await conn.aclose()

    @test.xfail("static config args not supported")
    async def test_server_config_args_02(self):
        with self.assertRaisesRegex(
            cluster.ClusterError,
            "invalid input syntax for type std::duration",
        ):
            async with tb.start_edgedb_server(
                extra_args=[
                    "--config-cfg::session_idle_timeout",
                    "illegal input",
                ]
            ):
                pass

    async def test_server_config_args_03(self):
        with self.assertRaisesRegex(cluster.ClusterError, "No such option"):
            async with tb.start_edgedb_server(
                extra_args=["--config-cfg::non_exist"]
            ):
                pass

    async def test_server_config_env_01(self):
        # Backend settings cannot be set statically with remote backend
        remote_pg = bool(os.getenv("EDGEDB_TEST_BACKEND_DSN"))

        env = {
            "EDGEDB_SERVER_CONFIG_cfg::session_idle_timeout": "1m22s",
            "EDGEDB_SERVER_CONFIG_cfg::apply_access_policies": "false",
            "EDGEDB_SERVER_CONFIG_cfg::multiprop": "single",
        }
        if not remote_pg:
            env["EDGEDB_SERVER_CONFIG_cfg::query_execution_timeout"] = "403"

        async with tb.start_edgedb_server(env=env) as sd:
            conn = await sd.connect()
            try:
                sysconfig = conn.get_settings()["system_config"]
                self.assertEqual(
                    sysconfig.session_idle_timeout,
                    datetime.timedelta(minutes=1, seconds=22),
                )

                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.session_idle_timeout)
                    """),
                    datetime.timedelta(minutes=1, seconds=22),
                )
                if not remote_pg:
                    self.assertEqual(
                        await conn.query_single("""\
                            select assert_single(
                                cfg::Config.query_execution_timeout)
                        """),
                        datetime.timedelta(seconds=403),
                    )
                self.assertFalse(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.apply_access_policies)
                    """)
                )
                self.assertEqual(
                    await conn.query("""\
                        select assert_single(cfg::Config).multiprop
                    """),
                    ["single"],
                )
            finally:
                await conn.aclose()

    async def test_server_config_env_02(self):
        env = {
            "EDGEDB_SERVER_CONFIG_cfg::allow_bare_ddl": "illegal_input"
        }
        with self.assertRaisesRegex(
            cluster.ClusterError,
            "'cfg::AllowBareDDL' enum has no member called 'illegal_input'"
        ):
            async with tb.start_edgedb_server(env=env):
                pass

    async def test_server_config_env_03(self):
        env = {"EDGEDB_SERVER_CONFIG_cfg::apply_access_policies": "on"}
        with self.assertRaisesRegex(
            cluster.ClusterError,
            "can only be one of: true, false",
        ):
            async with tb.start_edgedb_server(env=env):
                pass

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use CONFIGURE INSTANCE in multi-tenant mode",
    )
    async def test_server_config_default(self):
        p1 = tb.find_available_port(max_value=50000)
        async with tb.start_edgedb_server(
            extra_args=["--port", str(p1)]
        ) as sd:
            conn = await sd.connect()
            try:
                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.listen_port)
                    """),
                    p1,
                )
                p2 = tb.find_available_port(p1 - 1)
                await conn.execute(f"""\
                    configure instance set listen_port := {p2}
                """)
            finally:
                await conn.aclose()

            conn = await sd.connect(port=p2)
            try:
                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.listen_port)
                    """),
                    p2,
                )
                await conn.execute("""\
                    configure instance reset listen_port
                """)
            finally:
                await conn.aclose()

            conn = await sd.connect()
            try:
                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.listen_port)
                    """),
                    p1,
                )
            finally:
                await conn.aclose()

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use --config-file in multi-tenant mode",
    )
    async def test_server_config_file_01(self):
        conf = textwrap.dedent('''
            ["cfg::Config"]
            session_idle_timeout = "8m42s"
            durprop = "996"
            apply_access_policies = false
            multiprop = "single"
            current_email_provider_name = "localmock"

            [[magic_smtp_config]]
            _tname = "cfg::SMTPProviderConfig"
            name = "localmock"
            sender = "sender@example.com"
            timeout_per_email = "1 minute 48 seconds"
        ''')
        async with tb.temp_file_with(
            conf.encode()
        ) as config_file, tb.start_edgedb_server(
            config_file=config_file.name,
            http_endpoint_security=args.ServerEndpointSecurityMode.Optional,
        ) as sd:
            conn = await sd.connect()
            try:
                sysconfig = conn.get_settings()["system_config"]
                self.assertEqual(
                    sysconfig.session_idle_timeout,
                    datetime.timedelta(minutes=8, seconds=42),
                )

                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.session_idle_timeout)
                    """),
                    datetime.timedelta(minutes=8, seconds=42),
                )
                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(
                            cfg::Config.durprop)
                    """),
                    datetime.timedelta(seconds=996),
                )
                self.assertFalse(
                    await conn.query_single("""\
                        select assert_single(cfg::Config.apply_access_policies)
                    """)
                )
                self.assertEqual(
                    await conn.query("""\
                        select assert_single(cfg::Config).multiprop
                    """),
                    ["single"],
                )

                dbname = await conn.query_single("""\
                    select sys::get_current_branch()
                """)
                provider = sd.fetch_server_info()["databases"][dbname][
                    "current_email_provider"
                ]
                self.assertEqual(provider['name'], 'localmock')
                self.assertEqual(provider['sender'], 'sender@example.com')
                self.assertEqual(provider['timeout_per_email'], 'PT1M48S')

                await conn.query("""\
                    configure current database
                    set current_email_provider_name := 'non_exist';
                """)
                async for tr in self.try_until_succeeds(ignore=AssertionError):
                    async with tr:
                        provider = sd.fetch_server_info()["databases"][dbname][
                            "current_email_provider"
                        ]
                        self.assertIsNone(provider)
            finally:
                await conn.aclose()

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use --config-file in multi-tenant mode",
    )
    async def test_server_config_file_02(self):
        conf = textwrap.dedent('''
            ["cfg::Config"]
            allow_bare_ddl = "illegal_input"
        ''')
        with self.assertRaisesRegex(
            cluster.ClusterError,
            "'cfg::AllowBareDDL' enum has no member called 'illegal_input'"
        ):
            async with tb.temp_file_with(
                conf.encode()
            ) as config_file, tb.start_edgedb_server(
                config_file=config_file.name,
            ):
                pass

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use --config-file in multi-tenant mode",
    )
    async def test_server_config_file_03(self):
        conf = textwrap.dedent('''
            ["cfg::Config"]
            apply_access_policies = "on"
        ''')
        with self.assertRaisesRegex(
            cluster.ClusterError,
            "can only be one of: true, false",
        ):
            async with tb.temp_file_with(
                conf.encode()
            ) as config_file, tb.start_edgedb_server(
                config_file=config_file.name,
            ):
                pass

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use --config-file in multi-tenant mode",
    )
    async def test_server_config_file_04(self):
        conf = textwrap.dedent('''
            ["cfg::Config"]
            query_execution_timeout = "1 hour"
        ''')
        with self.assertRaisesRegex(
            cluster.ClusterError,
            "backend config 'query_execution_timeout' cannot be set "
            "via config file"
        ):
            async with tb.temp_file_with(
                conf.encode()
            ) as config_file, tb.start_edgedb_server(
                config_file=config_file.name,
            ):
                pass

    @unittest.skipIf(
        "EDGEDB_SERVER_MULTITENANT_CONFIG_FILE" in os.environ,
        "cannot use --config-file in multi-tenant mode",
    )
    async def test_server_config_file_05(self):
        class Prop(enum.Enum):
            One = "One"
            Two = "Two"
            Three = "Three"

        conf = textwrap.dedent('''
            ["cfg::Config"]
            enumprop = "One"
        ''')
        async with tb.temp_file_with(
            conf.encode()
        ) as config_file, tb.start_edgedb_server(
            config_file=config_file.name,
        ) as sd:
            conn = await sd.connect()
            try:
                self.assertEqual(
                    await conn.query_single("""\
                        select assert_single(
                            cfg::Config.enumprop)
                    """),
                    Prop.One,
                )

                config_file.seek(0)
                config_file.truncate()
                config_file.write(textwrap.dedent('''
                    ["cfg::Config"]
                    enumprop = "Three"
                ''').encode())
                config_file.flush()
                os.kill(sd.pid, signal.SIGHUP)

                async for tr in self.try_until_succeeds(ignore=AssertionError):
                    async with tr:
                        self.assertEqual(
                            await conn.query_single("""\
                                select assert_single(
                                    cfg::Config.enumprop)
                            """),
                            Prop.Three,
                        )
            finally:
                await conn.aclose()


class TestDynamicSystemConfig(tb.TestCase):
    async def test_server_dynamic_system_config(self):
        async with tb.start_edgedb_server(
            extra_args=["--disable-dynamic-system-config"]
        ) as sd:
            conn = await sd.connect()
            try:
                conf, sess = await conn.query_single('''
                    SELECT (
                        cfg::Config.__internal_testvalue,
                        cfg::Config.__internal_sess_testvalue
                    ) LIMIT 1
                ''')

                with self.assertRaisesRegex(
                    edgedb.ConfigurationError, "cannot change"
                ):
                    await conn.query(f'''
                        CONFIGURE INSTANCE
                        SET __internal_testvalue := {conf + 1};
                    ''')

                await conn.query(f'''
                    CONFIGURE INSTANCE
                    SET __internal_sess_testvalue := {sess + 1};
                ''')

                conf2, sess2 = await conn.query_single('''
                    SELECT (
                        cfg::Config.__internal_testvalue,
                        cfg::Config.__internal_sess_testvalue
                    ) LIMIT 1
                ''')
                self.assertEqual(conf, conf2)
                self.assertEqual(sess + 1, sess2)
            finally:
                await conn.aclose()
