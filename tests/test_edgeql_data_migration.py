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
import os.path
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLDataMigration(tb.DDLTestCase):
    """Test that migrations preserve data under certain circumstances.

    Renaming, changing constraints, increasing cardinality should not
    destroy data.

    Some of the test cases here use the same migrations as
    `test_schema_migrations_equivalence`, therefore the test numbers
    should match for easy reference, even if it means skipping some.
    """

    async def assert_describe_migration(self, exp_result_json, *, msg=None):
        try:
            tx = self.con.transaction()
            await tx.start()
            try:
                res = await self.con.query_one(
                    'DESCRIBE CURRENT MIGRATION AS JSON;')
            finally:
                await tx.rollback()

            res = json.loads(res)
            self._assert_data_shape(res, exp_result_json, message=msg)
        except Exception:
            self.add_fail_notes(serialization='json')
            raise

    async def fast_forward_describe_migration(self):
        '''Repeatedly get the next step from DESCRIBE and execute it.

        The point of this as opposed to just using "POPULATE
        MIGRATION; COMMIT MIGRATION;" is that we want to make sure
        that the generated DDL is valid and in case it's not, narrow
        down which step is causing issues.
        '''

        # Keep track of proposed DDL
        prevddl = ''

        try:
            while True:
                mig = await self.con.query_one(
                    'DESCRIBE CURRENT MIGRATION AS JSON;')
                mig = json.loads(mig)
                if mig['proposed'] is None:
                    self._assert_data_shape(
                        mig, {'complete': True},
                        message='No more "proposed", but not "completed" '
                                'either.'
                    )
                    await self.con.execute('COMMIT MIGRATION;')
                    break

                for stmt in mig['proposed']['statements']:
                    curddl = stmt['text']
                    if prevddl == curddl:
                        raise Exception(
                            f"Repeated previous proposed DDL {curddl!r}"
                        )
                    try:
                        await self.con.execute(curddl)
                    except Exception as exc:
                        raise Exception(
                            f"Error while processing {curddl!r}"
                        ) from exc
                    prevddl = curddl

        except Exception:
            self.add_fail_notes(serialization='json')
            raise

    async def migrate(self, migration, *, module: str = 'test'):
        async with self.con.transaction():
            mig = f"""
                START MIGRATION TO {{
                    module {module} {{
                        {migration}
                    }}
                }};
            """
            await self.con.execute(mig)
            await self.fast_forward_describe_migration()

    async def test_edgeql_migration_simple_01(self):
        # Base case, ensuring a single SDL migration from a clean
        # state works.
        await self.migrate("""
            type NamedObject {
                required property name -> str;
                multi link related -> NamedObject {
                    property lang -> str;
                };
            };
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT NamedObject {
                name := 'Test'
            };

            INSERT NamedObject {
                name := 'Test 2',
                related := (SELECT DETACHED NamedObject
                            FILTER .name = 'Test')
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    NamedObject {
                        related: {
                            name,
                            @lang
                        }
                    }
                FILTER
                    .name = 'Test 2';
            """,
            [
                {
                    'related': [{'name': 'Test', '@lang': None}],
                }
            ]
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the first migration.
    ''')
    async def test_edgeql_migration_link_inheritance(self):
        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1.esdl')

        with open(schema_f) as f:
            schema = f.read()

        await self.migrate(schema)

        await self.con.execute('''
            SET MODULE test;

            INSERT Target1 {
                name := 'Target1_linkinh_2'
            };

            INSERT ObjectType01 {
                target := (SELECT Target1
                           FILTER .name = 'Target1_linkinh_2'
                           LIMIT 1)
            };

            INSERT Target0 {
                name := 'Target0_linkinh_2'
            };

            INSERT ObjectType23 {
                target := (SELECT Target0
                           FILTER .name = 'Target0_linkinh_2'
                           LIMIT 1)
            };
        ''')

        await self.con.execute('DECLARE SAVEPOINT t0;')

        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'target' of object type "
                r"'test::ObjectType01': "
                r"'test::Target0' \(expecting 'test::Target1'\)"):
            # Target0 is not allowed to be targeted by ObjectType01, since
            # ObjectType01 inherits from ObjectType1 which requires more
            # specific Target1.
            await self.con.execute('''
                INSERT ObjectType01 {
                    target := (
                        SELECT
                            Target0
                        FILTER
                            .name = 'Target0_linkinh_2'
                        LIMIT 1
                    )
                };
            ''')

        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1_migrated.esdl')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute('''
            ROLLBACK TO SAVEPOINT t0;
        ''')
        await self.migrate(schema)

    async def test_edgeql_migration_describe_reject_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"not currently in a migration block"):
            await self.con.execute('''
                ALTER CURRENT MIGRATION REJECT PROPOSED;
            ''')

    async def test_edgeql_migration_describe_reject_02(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': True,
            'proposed': None,
        })

        # Reject an empty proposal, which should be an idempotent
        # operation. So reject it several times.
        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

    async def test_edgeql_migration_describe_reject_03(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type0;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE test::Type0;'
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type0',
                'prompt': "did you create object type 'test::Type0'?",
            },
        })

        # Reject a proposal until we run out of options.
        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_reject_04(self):
        # Migration involving 2 modules
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Test;
                };

                module other {
                    type Test;
                };
            };
        ''')
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Test2;
                };

                module other {
                    type Test3;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE other::Test RENAME TO other::Test3;',
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE other::Test',
                'prompt': (
                    "did you rename object type 'other::Test' to "
                    "'other::Test3'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE other::Test RENAME TO test::Test2;',
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE other::Test',
                'prompt': (
                    "did you rename object type 'other::Test' to "
                    "'test::Test2'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER TYPE other::Test RENAME TO test::Test2;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE other::Test RENAME TO test::Test2;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Test RENAME TO other::Test3;',
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Test',
                'prompt': (
                    "did you rename object type 'test::Test' to "
                    "'other::Test3'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE other::Test RENAME TO test::Test2;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE other::Test3;',
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE other::Test3',
                'prompt': (
                    "did you create object type 'other::Test3'?"
                ),
            },
        })

        # Change our mind and use a rejected operation to rename the
        # type after all. So, we should be done now.
        await self.con.execute('''
            ALTER TYPE test::Test RENAME TO other::Test3;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE other::Test RENAME TO test::Test2;',
                'ALTER TYPE test::Test RENAME TO other::Test3;',
            ],
            'complete': True,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_rollback_01(self):
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type1 {
                        property field1 -> str;
                    };
                };
            };

            DECLARE SAVEPOINT migration_01;
        ''')

        await self.assert_describe_migration(orig_expected := {
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type1 {\n'
                        '    CREATE OPTIONAL SINGLE PROPERTY field1'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type1',
                'prompt': "did you create object type 'test::Type1'?",
            },
        })

        await self.con.execute('''
            CREATE TYPE test::Type1 {
                CREATE OPTIONAL SINGLE PROPERTY field1 -> std::str;
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [
                'CREATE TYPE test::Type1 {\n'
                '    CREATE OPTIONAL SINGLE PROPERTY field1 -> std::str;\n'
                '};'
            ],
            'complete': True,
            'proposed': None,
        })

        await self.con.execute('ROLLBACK TO SAVEPOINT migration_01;')

        await self.assert_describe_migration(orig_expected)

    async def test_edgeql_migration_describe_module_01(self):
        # Migration that creates a new module.
        await self.con.execute('''
            START MIGRATION TO {
                module new_module {
                    type Type0;
                };
            };
        ''')

        # Validate that we create a 'new_module'
        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE MODULE new_module IF NOT EXISTS;'
                }],
                'confidence': 1.0,
            },
        })

        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Drop the 'new_module'
        await self.con.execute('''
            START MIGRATION TO {
                module default {};
            };
        ''')

        # Validate that we drop a 'new_module'
        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'DROP TYPE new_module::Type0;'
                }],
                'confidence': 1.0,
            },
        })
        await self.con.execute('''
            DROP TYPE new_module::Type0;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'DROP TYPE new_module::Type0;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'DROP MODULE new_module;'
                }],
                'confidence': 1.0,
            },
        })

        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Make sure that 'new_module' can be created again with no
        # problems (i.e. it was dropped cleanly).
        await self.con.execute('''
            START MIGRATION TO {
                module new_module {
                    type Type0;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE MODULE new_module IF NOT EXISTS;'
                }],
                'confidence': 1.0,
            },
        })
        await self.con.execute('''
            CREATE MODULE new_module;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'CREATE MODULE new_module;',
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE new_module::Type0;'
                }],
            },
        })
        await self.con.execute('''
            CREATE TYPE new_module::Type0;
            COMMIT MIGRATION;
        ''')

        await self.assert_query_result(
            r"""
                INSERT new_module::Type0;
            """,
            [{
                'id': uuid.UUID,
            }],
        )

    async def test_edgeql_migration_describe_type_01(self):
        # Migration that renames a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type1;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type1;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type1',
                'prompt': "did you create object type 'test::Type1'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query(r'''INSERT test::Type1;''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type01;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1jywblj6c7z25ouifcicpxniu37jdpyunf62q4th7isdafcqu67gq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type1 RENAME TO test::Type01;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type1',
                'prompt': (
                    "did you rename object type 'test::Type1' to "
                    "'test::Type01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type01;
        ''', [{'id': res[0].id}])

    async def test_edgeql_migration_describe_type_02(self):
        # Migration that creates a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type02',
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.query(r'''INSERT test::Type02;''')

        # Migration that drops a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1fcvk56n44i62qwjnw5nqgafnbpulfhhaeb6kxqhh4c6lc4elwysa',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP TYPE test::Type02;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'DROP TYPE test::Type02',
                'prompt': (
                    "did you drop object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ObjectType
            FILTER .name = 'test::Type02';
        ''', [])

        # Make sure that type dropped cleanly by re-creating and
        # using the type again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1yee6qj63nps27cjnrcudwiupusdqkzrwistpvfbqf2fstcmwauwa',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type02',
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            INSERT test::Type02;
        ''', [{'id': uuid.UUID}])

    async def test_edgeql_migration_describe_type_03(self):
        await self.migrate('''
            type Type0;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type1;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': "ALTER TYPE test::Type0 RENAME TO test::Type1;"
                }],
                'confidence': 1.0,
                'operation_id': "ALTER TYPE test::Type0",
                'prompt': (
                    "did you rename object type 'test::Type0' to "
                    "'test::Type1'?"
                ),
            },
        })

        # Instead of the suggestion do a couple of different, but
        # equivalent commands.
        await self.con.execute('''
            ALTER TYPE test::Type0 RENAME TO test::TypeXX;
            ALTER TYPE test::TypeXX RENAME TO test::Type1;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'ALTER TYPE test::Type0 RENAME TO test::TypeXX;',
                'ALTER TYPE test::TypeXX RENAME TO test::Type1;',
            ],
            'complete': True,
            'proposed': None,
        })

    async def test_edgeql_migration_describe_type_04(self):
        self.maxDiff = None
        await self.migrate('''
            type Test;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Test2;
                    type Test3;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1xh653zionj2aehqbh7x6km5lo3b2mjaftxdkvqoh3wluc3iv6k2a',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Test RENAME TO test::Test2;',
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Test',
                'prompt': (
                    "did you rename object type 'test::Test' to 'test::Test2'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER CURRENT MIGRATION REJECT PROPOSED;
        ''')

        await self.assert_describe_migration({
            'parent': 'm1xh653zionj2aehqbh7x6km5lo3b2mjaftxdkvqoh3wluc3iv6k2a',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'ALTER TYPE test::Test RENAME TO test::Test3;',
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Test',
                'prompt': (
                    "did you rename object type 'test::Test' to 'test::Test3'?"
                ),
            },
        })

        await self.con.execute('''
            ALTER TYPE test::Test RENAME TO test::Test3;
        ''')

        await self.assert_describe_migration({
            'parent': 'm1xh653zionj2aehqbh7x6km5lo3b2mjaftxdkvqoh3wluc3iv6k2a',
            'confirmed': ['ALTER TYPE test::Test RENAME TO test::Test3;'],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': 'CREATE TYPE test::Test2;',
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Test2',
                'prompt': (
                    "did you create object type 'test::Test2'?"
                ),
            },
        })

    async def test_edgeql_migration_describe_property_01(self):
        # Migration that renames a property.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type01 {
                        property field1 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type01 {\n'
                        '    CREATE OPTIONAL SINGLE PROPERTY field1'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type01',
                'prompt': "did you create object type 'test::Type01'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            INSERT test::Type01 {
                field1 := 'prop_test'
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type01 {
                        property field01 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1qbv2f3km5xs5teyya5yog6areb33lnsqvs5prmyumtehnmpdfy3q',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type01 {\n'
                        '    ALTER PROPERTY field1 {\n'
                        '        RENAME TO field01;\n'
                        '    };\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type01',
                'prompt': (
                    "did you alter object type 'test::Type01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type01 {
                field01
            };
        ''', [{'field01': 'prop_test'}])

    async def test_edgeql_migration_describe_property_02(self):
        # Migration that creates a type with property.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02 {
                        property field02 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02 {\n'
                        '    CREATE OPTIONAL SINGLE PROPERTY field02'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type02',
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query('''
            INSERT test::Type02 {
                field02 := 'prop_test'
            };
        ''')

        # Migration that drops a property.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1plg55ylmquxeeurgqtp7uuaupb463z4htxw3rregmzx42zs5lxea',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    DROP PROPERTY field02;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type02',
                'prompt': (
                    "did you alter object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id
            };
        ''', [{
            'id': res[0].id
        }])

        # Make sure that property dropped cleanly by re-creating and
        # using the property again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Type02 {
                        property field02 -> str;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1dsogsjmchh4kivd633z6jjivjlve4hmqofr2obt3rq5koakemc5a',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    CREATE OPTIONAL SINGLE PROPERTY field02'
                        ' -> std::str;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type02',
                'prompt': (
                    "did you alter object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id,
                field02,
            };
        ''', [{
            'id': res[0].id,
            'field02': None,
        }])

    async def test_edgeql_migration_describe_link_01(self):
        # Migration that renames a link.
        await self.con.execute(r'''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type01 {
                        link foo1 -> Foo;
                    };
                };
            };

            # just initialize Foo, since we're interested in the other type
            CREATE TYPE test::Foo;
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': ['CREATE TYPE test::Foo;'],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type01 {\n'
                        '    CREATE OPTIONAL SINGLE LINK foo1'
                        ' -> test::Foo;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type01',
                'prompt': "did you create object type 'test::Type01'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query('''
            WITH MODULE test
            SELECT (
                INSERT Type01 {
                    foo1 := (INSERT Foo)
                }
            ) {
                foo1
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type01 {
                        link foo01 -> Foo;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1wmfeopwqccjy35fuf73j6g6sgrqnmes53gjpizw5tyehwiij6yhq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type01 {\n'
                        '    ALTER LINK foo1 {\n'
                        '        RENAME TO foo01;\n'
                        '    };\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type01',
                'prompt': (
                    "did you alter object type 'test::Type01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type01 {
                foo01: {
                    id
                }
            };
        ''', [{'foo01': {'id': res[0].foo1.id}}])

    async def test_edgeql_migration_describe_link_02(self):
        # Migration that creates a type with link.
        await self.con.execute(r'''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type02 {
                        link foo02 -> Foo;
                    };
                };
            };

            # just initialize Foo, since we're interested in the other type
            CREATE TYPE test::Foo;
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': ['CREATE TYPE test::Foo;'],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE TYPE test::Type02 {\n'
                        '    CREATE OPTIONAL SINGLE LINK foo02'
                        ' -> test::Foo;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE TYPE test::Type02',
                'prompt': "did you create object type 'test::Type02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        res = await self.con.query('''
            WITH MODULE test
            SELECT (
                INSERT Type02 {
                    foo02 := (INSERT Foo)
                }
            ) {
                foo02
            }
        ''')

        # Migration that drops a link.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type02;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1tul34c3bsnuzypwqo4cgpryguamjffvqme3c66id7nxasbsjyhda',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    DROP LINK foo02;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type02',
                'prompt': (
                    "did you alter object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id
            };
        ''', [{
            'id': res[0].id
        }])
        await self.assert_query_result('''
            SELECT test::Foo {
                id
            };
        ''', [{
            'id': res[0].foo02.id
        }])

        # Make sure that link dropped cleanly by re-creating and
        # using the link again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type Foo;
                    type Type02 {
                        link foo02 -> Foo;
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1zo2zt4im2gkkavxn6hzs432k5fvkdz42tswbphejihqe2yul47la',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::Type02 {\n'
                        '    CREATE OPTIONAL SINGLE LINK foo02'
                        ' -> test::Foo;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER TYPE test::Type02',
                'prompt': (
                    "did you alter object type 'test::Type02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT test::Type02 {
                id,
                foo02: {
                    id
                },
            };
        ''', [{
            'id': res[0].id,
            'foo02': None,
        }])

    @test.xfail('''
        In the final migration DESCRIBE generates "RENAME TO foo01;" twice.

        Also, if the double RENAME validation is skipped, the
        following error appears in the final (DROP) migration:

        dgedb.errors.InternalServerError: cannot drop table
        "edgedb_193fe3f0-fcad-11ea-9d53-39dd03c0a79a".
        "1b16e5a3-fcad-11ea-a559-4f0a062303a6"
        because other objects depend on it
    ''')
    async def test_edgeql_migration_describe_link_03(self):
        # Migration that renames a link.
        await self.con.execute(r'''
            START MIGRATION TO {
                module test {
                    abstract link foo3;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT LINK test::foo3;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE LINK test::foo3',
                'prompt': "did you create abstract link 'test::foo3'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract link foo03;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1okv4ltfh3dphmqfmmx5bjusyzsnvc7sgjtb6vdo26mjf6rtmdqxq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT LINK test::foo3 {\n'
                        '    RENAME TO test::foo03;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER LINK test::foo3',
                'prompt': (
                    "did you rename abstract link 'test::foo3' to "
                    "'test::foo03'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1wk3m67nkkglmcbx32wvjh75n4pqbqtz5bs2rwdgcoefiwveqpfcq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP ABSTRACT LINK test::foo03;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'DROP LINK test::foo03',
                'prompt': (
                    "did you drop abstract link 'test::foo03'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

    async def test_edgeql_migration_describe_scalar_01(self):
        # Migration that renames a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType1 extending int64;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE SCALAR TYPE test::ScalarType1'
                        ' EXTENDING std::int64;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE SCALAR TYPE test::ScalarType1',
                'prompt': "did you create scalar type 'test::ScalarType1'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType1>'1' + 2;
        ''', [3])

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType01 extending int64;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1svrt2rvolv2f3fgtpgj2qikec4o4v6a5le5u6jfpyrzfhybr2ipa',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER SCALAR TYPE test::ScalarType1'
                        ' RENAME TO test::ScalarType01;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER SCALAR TYPE test::ScalarType1',
                'prompt': (
                    "did you rename scalar type 'test::ScalarType1' to "
                    "'test::ScalarType01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType01>'2' + 1;
        ''', [3])

    async def test_edgeql_migration_describe_scalar_02(self):
        # Migration that creates a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType02 extending str;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE SCALAR TYPE test::ScalarType02'
                        ' EXTENDING std::str;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE SCALAR TYPE test::ScalarType02',
                'prompt': "did you create scalar type 'test::ScalarType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType02>1 ++ '2';
        ''', ['12'])

        # Migration that drops a type.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1azidpr7ai7z2u4rcfx2awxdxy5ouenhvmb2otxew4penhxnruvuq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP SCALAR TYPE test::ScalarType02;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'DROP SCALAR TYPE test::ScalarType02',
                'prompt': (
                    "did you drop scalar type 'test::ScalarType02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ScalarType
            FILTER .name = 'test::ScalarType02';
        ''', [])

        # Make sure that type dropped cleanly by re-creating and
        # using the type again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type ScalarType02 extending str;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm13agjryb2lawnugaty4gkqzjvxvrbod3olf3abupck7x2777yntta',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE SCALAR TYPE test::ScalarType02'
                        ' EXTENDING std::str;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE SCALAR TYPE test::ScalarType02',
                'prompt': "did you create scalar type 'test::ScalarType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::ScalarType02>2 ++ '1';
        ''', ['21'])

    async def test_edgeql_migration_describe_enum_01(self):
        # Migration that renames an enum.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType1 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "CREATE FINAL SCALAR TYPE test::EnumType1"
                        " EXTENDING enum<foo, bar>;"
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE SCALAR TYPE test::EnumType1',
                'prompt': "did you create enumerated type 'test::EnumType1'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType1>'bar';
        ''', ['bar'])

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType01 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1e7h52ims4j4ijfbdfrvm453vgldwsok6f7oiosyhvcmjvrjgefqq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER SCALAR TYPE test::EnumType1'
                        ' RENAME TO test::EnumType01;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'ALTER SCALAR TYPE test::EnumType1',
                'prompt': (
                    "did you rename enumerated type 'test::EnumType1' to "
                    "'test::EnumType01'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType01>'foo';
        ''', ['foo'])

    async def test_edgeql_migration_describe_enum_02(self):
        # Migration that creates an enum.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType02 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1a2l6lbzimqokzygdzbkyjrhbmjh3iljg7i2m6r2ias2z2de4x4cq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "CREATE FINAL SCALAR TYPE test::EnumType02"
                        " EXTENDING enum<foo, bar>;"
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE SCALAR TYPE test::EnumType02',
                'prompt': "did you create enumerated type 'test::EnumType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType02>'bar';
        ''', ['bar'])

        # Migration that drops an enum.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm16yh2sfnw2of6eikwc3u4odjeie2cvz54qe3e4jk7o3tvc3q5xzjq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP SCALAR TYPE test::EnumType02;'
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'DROP SCALAR TYPE test::EnumType02',
                'prompt': (
                    "did you drop enumerated type 'test::EnumType02'?"
                ),
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            WITH MODULE schema
            SELECT ScalarType
            FILTER .name = 'test::EnumType02';
        ''', [])

        # Make sure that enum dropped cleanly by re-creating and
        # using the enum again.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type EnumType02 extending enum<foo, bar>;
                };
            };
        ''')

        await self.assert_describe_migration({
            'parent': 'm1pbb5jssdc652jn74enr3cnvynydww476glgodzyufbru6hcqsmsq',
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "CREATE FINAL SCALAR TYPE test::EnumType02"
                        " EXTENDING enum<foo, bar>;"
                    )
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE SCALAR TYPE test::EnumType02',
                'prompt': "did you create enumerated type 'test::EnumType02'?",
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result('''
            SELECT <test::EnumType02>'foo';
        ''', ['foo'])

    @test.xfail('''
        Abstract annotation doesn't offer renaming.
    ''')
    async def test_edgeql_migration_describe_annotation_01(self):
        # Migration that renames an annotation.
        await self.migrate('''
            abstract annotation my_anno1;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation renamed_anno1;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT ANNOTATION test::my_anno1 '
                        'RENAME TO test::renamed_anno1;'
                    )
                }],
                'confidence': 1.0,
            },
        })

    @test.xfail('''
        edgedb.errors.SchemaError: cannot get 'name' value: item
        'bc6c9f68-049d-11eb-a183-c1f86eaf323b' is not present in the
        schema <Schema gen:3708 at 0x7efef93f1430>

        The error occurs on committing the second migration.
    ''')
    async def test_edgeql_migration_describe_annotation_02(self):
        # Migration that creates an annotation.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation my_anno2;

                    type AnnoType2 {
                        annotation my_anno2 := 'test_my_anno2';
                    }
                };
            };
        ''')

        await self.con.execute('''
            CREATE TYPE test::AnnoType2;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'CREATE TYPE test::AnnoType2;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT ANNOTATION test::my_anno2;'
                    )
                }],
                'confidence': 1.0,
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Migration that drops an annotation.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    type AnnoType2;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER TYPE test::AnnoType2 {\n'
                        '    DROP ANNOTATION test::my_anno2;\n'
                        '};'
                    )
                }],
                'confidence': 1.0,
            },
        })
        # Auto-complete migration
        await self.con.execute('''
            ALTER TYPE test::AnnoType2 {
                DROP ANNOTATION test::my_anno2;
            };
        ''')

        await self.assert_describe_migration({
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'DROP ABSTRACT ANNOTATION test::my_anno2;'
                    )
                }],
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        # Make sure that annotation dropped cleanly by re-creating and
        # using the annotation.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract annotation my_anno2;

                    type AnnoType2 {
                        annotation my_anno2 := 'retest_my_anno2';
                    }
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT ANNOTATION test::my_anno2;'
                    )
                }],
                'confidence': 1.0,
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value,
                    },
                } FILTER .name = 'test::AnnoType2';
            """,
            [{
                'name': 'test::AnnoType2',
                'annotations': [{
                    'name': 'my_anno2',
                    '@value': 'retest_my_anno2',
                }]
            }],
        )

    @test.xfail('''
        We drop and create a new constraint but would prefer to
        rename it directly.
    ''')
    async def test_edgeql_migration_describe_constraint_01(self):
        # Migration that renames a constraint.
        await self.migrate('''
            abstract constraint my_oneof(one_of: array<anytype>) {
                using (contains(one_of, __subject__));
            };
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT CONSTRAINT test::my_oneof '
                        'RENAME TO test::my_one_of;'
                    )
                }],
                'confidence': 1.0,
            },
        })

    async def test_edgeql_migration_describe_constraint_02(self):
        # Migration that creates a constraint.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };

                    scalar type my_str extending str {
                        constraint my_one_of(['my', 'str']);
                    };
                };
            };
        ''')

        await self.con.execute('''
            CREATE SCALAR TYPE test::my_str EXTENDING std::str;
        ''')

        await self.assert_describe_migration({
            'confirmed': [
                'CREATE SCALAR TYPE test::my_str EXTENDING std::str;'
            ],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT CONSTRAINT test::my_one_of('
                        'one_of: array<anytype>) {\n'
                        '    USING (std::contains(one_of, __subject__));\n'
                        '};'
                    ),
                }],
                'confidence': 1.0,
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()
        await self.con.execute('''
            DECLARE SAVEPOINT migration_01;
        ''')

        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'my';
            """,
            ['my'],
        )
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"invalid my_str"):
            await self.con.execute(r"""
                SELECT <test::my_str>'nope';
            """)
        await self.con.execute(r"""
            ROLLBACK TO SAVEPOINT migration_01;
        """)

        # Migration that drops a constraint.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    scalar type my_str extending str;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        "ALTER SCALAR TYPE test::my_str {\n"
                        "    DROP CONSTRAINT test::my_one_of(['my', 'str']);\n"
                        "};"
                    ),
                }],
                'confidence': 1.0,
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'my';
            """,
            ['my'],
        )
        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'nope';
            """,
            ['nope'],
        )

        # Test that dropping constraint was clean with a migration
        # that re-creates a constraint.
        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract constraint my_one_of(one_of: array<anytype>) {
                        using (contains(one_of, __subject__));
                    };

                    scalar type my_str extending str {
                        constraint my_one_of(['my2', 'str2']);
                    };
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'CREATE ABSTRACT CONSTRAINT '
                        'test::my_one_of(one_of: array<anytype>) {\n'
                        '    USING (std::contains(one_of, __subject__));\n'
                        '};'
                    ),
                }],
                'confidence': 1.0,
                'operation_id': 'CREATE CONSTRAINT test::my_one_of',
            },
        })
        # Auto-complete migration
        await self.fast_forward_describe_migration()

        await self.assert_query_result(
            r"""
                SELECT <test::my_str>'my2';
            """,
            ['my2'],
        )
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"invalid my_str"):
            await self.con.execute(r"""
                SELECT <test::my_str>'my';
            """)

    async def test_edgeql_migration_describe_abs_ptr_01(self):
        await self.migrate('''
            abstract link abs_link;
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    abstract link new_abs_link;
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                'statements': [{
                    'text': (
                        'ALTER ABSTRACT LINK test::abs_link '
                        'RENAME TO test::new_abs_link;'
                    )
                }],
                'confidence': 1.0,
            },
        })

    @test.xfail('''
        Function rename DESCRIBE fails with:

        InvalidReferenceError: function 'default::foo' does not exist
    ''')
    async def test_edgeql_migration_describe_function_01(self):
        # Migration that renames a function (currently we expect a
        # drop/create instead of renaming).
        await self.migrate('''
            function foo() -> str using (SELECT <str>random());
        ''')

        await self.con.execute('''
            START MIGRATION TO {
                module test {
                    function bar() -> str using (SELECT <str>random());
                };
            };
        ''')

        await self.assert_describe_migration({
            'confirmed': [],
            'complete': False,
            'proposed': {
                # TODO: add actual validation for statements
                'confidence': 1.0,
            },
        })

    @test.xfail('''
       ISE: relation "<blah>" does not exist
    ''')
    async def test_edgeql_migration_function_01(self):
        await self.migrate('''
            type Note {
                required property name -> str;
            }

            function hello_note(x: Note) -> str {
                USING (SELECT x.name)
            }
        ''')

    async def test_edgeql_migration_eq_01(self):
        await self.migrate("""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base;
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r"missing value for required property test::Base.name"):
            await self.migrate("""
                type Base {
                    required property name -> str;
                }
            """)
        # Migration without making the property required.
        await self.migrate("""
            type Base {
                property name -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name
                };
            """,
            [{
                'name': None,
            }],
        )

        await self.con.execute("""
            UPDATE
                Base
            SET {
                name := 'base_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name
                };
            """,
            [{
                'name': 'base_01',
            }],
        )

        # Inherit from the Base, making name required.
        await self.migrate("""
            type Base {
                property name -> str;
            }

            type Derived extending Base {
                overloaded required property name -> str;
            }
        """)
        await self.con.execute("""
            INSERT Derived {
                name := 'derived_01'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base.name;
            """,
            {'base_01', 'derived_01'},
        )

    async def test_edgeql_migration_eq_02(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 'base_02',
            };
            INSERT Derived {
                foo := 'derived_02',
            };
        """)

        await self.migrate(r"""
            type Base {
                # rename 'foo'
                property foo2 -> str;
            }

            type Derived extending Base {
                overloaded required property foo2 -> str;
            }
        """)

        # the data still persists
        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': 'base_02',
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': 'derived_02',
            }],
        )

    async def test_edgeql_migration_eq_03(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 'base_03',
            };
            INSERT Derived {
                foo := 'derived_03',
            };
        """)

        await self.migrate(r"""
            type Base;
                # drop 'foo'

            type Derived extending Base {
                # completely different property
                property foo2 -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    [IS Derived].foo2,
                } ORDER BY .foo2;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': None,
            }],
        )

    async def test_edgeql_migration_eq_04(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }

            type Derived extending Base;

            type Further extending Derived {
                overloaded required property foo -> str;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 'base_04',
            };
            INSERT Derived {
                foo := 'derived_04',
            };
            INSERT Further {
                foo := 'further_04',
            };
        """)

        await self.migrate(r"""
            type Base;
                # drop 'foo'

            type Derived extending Base;

            type Further extending Derived {
                # completely different property
                property foo2 -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    [IS Further].foo2,
                } ORDER BY .__type__.name;
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Derived'},
                'foo2': None,
            }, {
                '__type__': {'name': 'test::Further'},
                'foo2': None,
            }],
        )

    async def test_edgeql_migration_eq_06(self):
        await self.migrate(r"""
            type Base {
                property foo -> int64;
            }

            type Derived extending Base {
                overloaded required property foo -> int64;
            }
        """)
        await self.con.execute("""
            SET MODULE test;

            INSERT Base {
                foo := 6,
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo,
                };
            """,
            [{
                '__type__': {'name': 'test::Base'},
                # the value was correctly inserted
                'foo': 6,
            }],
        )

        await self.migrate(r"""
            type Base {
                # change property type (can't preserve value)
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    __type__: {name},
                    foo,
                };
            """,
            [{
                '__type__': {'name': 'test::Base'},
                'foo': '6',
            }],
        )

    async def test_edgeql_migration_eq_07(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        res = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child),
                }
            ) {
                bar: {id}
            }
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                required link bar -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': res[0].bar.id},
            }],
        )

    async def test_edgeql_migration_eq_08(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'very_long_test_str_base_08',
            };
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        new_state = r"""
            type Base {
                required property foo -> str {
                    # add a constraint
                    constraint max_len_value(10);
                }
            }
        """
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r"foo must be no longer than 10 characters"):
            await self.migrate(new_state)

        # Fix the data.
        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_08',
            };
        """)

        # Migrate to same state as before now that the data is fixed.
        await self.migrate(new_state)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': 'base_08',
            }],
        )

    async def test_edgeql_migration_eq_09(self):
        await self.migrate(r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
            }
            type Base {
                property foo -> constraint_length;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'b09',
            };
        """)

        # Try altering the schema to a state inconsistent with current
        # data.
        new_state = r"""
            scalar type constraint_length extending str {
                constraint max_len_value(10);
                # add a constraint
                constraint min_len_value(5);
            }
            type Base {
                property foo -> constraint_length;
            }
        """
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'Existing test::Base\.foo values violate the new constraint'):
            await self.migrate(new_state)

        # Fix the data.
        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_09',
            };
        """)

        # Migrate to same state as before now that the data is fixed.
        await self.migrate(new_state)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': 'base_09',
            }],
        )

    async def test_edgeql_migration_eq_11(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'base_11',
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # change property to link with same name
                link foo -> Child {
                    # add a constraint
                    constraint exclusive;
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': None,
            }],
        )

    async def test_edgeql_migration_eq_12(self):
        await self.migrate(r"""
            type Child;

            type Base {
                property foo -> str {
                    constraint exclusive;
                }

                link bar -> Child {
                    constraint exclusive;
                }
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    foo := 'base_12',
                    bar := (INSERT Child)
                })
            {
                foo,
                bar: {id}
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # drop constraints
                property foo -> str;
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar: {id}
                };
            """,
            [{
                'foo': 'base_12',
                'bar': {'id': data[0].bar.id}
            }],
        )

    async def test_edgeql_migration_eq_13(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                overloaded required link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Derived {
                    bar := (INSERT Child)
                })
            {
                bar: {id}
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base;
                # drop 'bar'

            type Derived extending Base {
                # no longer inherit link 'bar'
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar: {id}
                };
            """,
            [{
                'bar': {'id': data[0].bar.id}
            }],
        )

    async def test_edgeql_migration_eq_14(self):
        await self.migrate(r"""
            type Base;

            type Derived extending Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {
                foo := 'derived_14',
            };
        """)

        await self.migrate(r"""
            type Base {
                # move the property earlier in the inheritance
                property foo -> str;
            }

            type Derived extending Base {
                overloaded required property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    foo,
                };
            """,
            [{
                'foo': 'derived_14',
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        The second migration fails.
    ''')
    async def test_edgeql_migration_eq_16(self):
        await self.migrate(r"""
            type Child;

            type Base;

            type Derived extending Base {
                link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Derived {
                    bar := (INSERT Child),
                }
            ) {
                bar: {id}
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # move the link earlier in the inheritance
                link bar -> Child;
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar,
                };
            """,
            [{
                'bar': {'id': data[0].bar.id},
            }],
        )

        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }

            type Derived extending Base {
                # also make the link 'required'
                overloaded required link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    bar,
                };
            """,
            [{
                'bar': {'id': data[0].bar.id},
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_18(self):
        await self.migrate(r"""
            type Base {
                property name := 'computable'
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                # change a property from a computable to regular with a default
                property name -> str {
                    default := 'something'
                }
            }
        """)

        # Insert a new object, this one should have a new default name.
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                } ORDER BY .name EMPTY LAST;
            """,
            [{
                'name': 'something',
            }, {
                'name': None,
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_19(self):
        await self.migrate(r"""
            type Base {
                property name -> str
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'base_19'
            };
        """)

        await self.migrate(r"""
            type Base {
                # change a regular property to a computable
                property name := 'computable'
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    name,
                };
            """,
            [{
                'name': 'computable',
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the third migration.
    ''')
    async def test_edgeql_migration_eq_21(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'base_21'
            };
        """)

        await self.migrate(r"""
            type Base {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                bar := 21
            };
        """)
        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar
                };
            """,
            [{
                'foo': 'base_21',
                'bar': 21,
            }],
        )

        await self.migrate(r"""
            type Base {
                # make the old property into a computable
                property foo := <str>__source__.bar;
                property bar -> int64;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                    bar
                };
            """,
            [{
                'foo': '21',
                'bar': 21,
            }],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: property 'foo' does not exist

        This error happens in the last migration.
    ''')
    async def test_edgeql_migration_eq_22(self):
        await self.migrate(r"""
            type Base {
                property foo -> str;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 'base_22'
            };
        """)

        await self.migrate(r"""
            # rename the type, although this test doesn't ensure that
            # renaming actually took place
            type NewBase {
                property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT NewBase {
                    foo,
                };
            """,
            [{
                'foo': 'base_22',
            }],
        )

        await self.migrate(r"""
            type NewBase {
                property foo -> str;
                # add a property
                property bar -> int64;
            }
        """)

        await self.con.execute(r"""
            UPDATE NewBase
            SET {
                bar := 22
            };
        """)
        await self.assert_query_result(
            r"""
                SELECT NewBase {
                    foo,
                    bar
                };
            """,
            [{
                'foo': 'base_22',
                'bar': 22,
            }],
        )

        await self.migrate(r"""
            type NewBase {
                # drop 'foo'
                property bar -> int64;
            }

            # add a alias to emulate the original
            alias Base := (
                SELECT NewBase {
                    foo := <str>.bar
                }
            );
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo,
                };
            """,
            [{
                'foo': '22',
            }],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: ObjectType 'test::Base' is already
        present in the schema <Schema gen:3757 at 0x7fc3319fa820>

        Exception: Error while processing
        'CREATE ALIAS test::Base := (
            SELECT
                test::Child {
                    bar := test::Child
                }
        );'
    ''')
    async def test_edgeql_migration_eq_23(self):
        await self.migrate(r"""
            type Child {
                property foo -> str;
            }

            type Base {
                link bar -> Child;
            }

            alias Alias01 := (
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                bar := (
                    INSERT Child {
                        foo := 'child_23'
                    }
                )
            };
        """)

        await self.migrate(r"""
            type Child {
                property foo -> str;
            }

            # exchange a type for a alias
            alias Base := (
                SELECT Child {
                    # bar is the same as the root object
                    bar := Child
                }
            );

            alias Alias01 := (
                # now this alias refers to another alias
                SELECT Base {
                    child_foo := .bar.foo
                }
            );
        """)

        await self.assert_query_result(
            r"""
                SELECT Alias01 {
                    child_foo,
                };
            """,
            [{
                'child_foo': 'child_23',
            }],
        )

    async def test_edgeql_migration_eq_24(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child)
                }
            ) {
                bar: {id}
            }
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # increase link cardinality
                multi link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': [{'id': data[0].bar.id}],
            }],
        )

    async def test_edgeql_migration_eq_25(self):
        await self.migrate(r"""
            type Child;

            type Base {
                multi link bar -> Child;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)
        data = await self.con.query(r"""
            SELECT (
                INSERT Base {
                    bar := (INSERT Child)
                }
            ) {
                bar: {id}
            }
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # reduce link cardinality
                link bar -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': data[0].bar[0].id},
            }],
        )

        await self.migrate(r"""
            type Child;

            type Base {
                link bar -> Child {
                    # further restrict the link
                    constraint exclusive
                }
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    bar: {id},
                };
            """,
            [{
                'bar': {'id': data[0].bar[0].id},
            }],
        )

    async def test_edgeql_migration_eq_26(self):
        await self.migrate(r"""
            type Child;

            type Parent {
                link bar -> Child;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Parent {
                bar := (INSERT Child)
            };
        """)

        await self.migrate(r"""
            type Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type
            type DerivedParent extending Parent;
        """)

        await self.assert_query_result(
            r"""
                SELECT Parent {
                    type := .__type__.name,
                    bar_type := .bar.__type__.name
                };
            """,
            [{
                'type': 'test::Parent',
                'bar_type': 'test::Child',
            }],
        )

        await self.migrate(r"""
            type Child;

            type DerivedChild extending Child;

            type Parent {
                link bar -> Child;
            }

            # derive a type with a more restrictive link
            type DerivedParent extending Parent {
                overloaded link bar -> DerivedChild;
            }
        """)

        await self.con.execute(r"""
            INSERT DerivedParent {
                bar := (INSERT DerivedChild)
            }
        """)
        await self.assert_query_result(
            r"""
                SELECT Parent {
                    type := .__type__.name,
                    bar_type := .bar.__type__.name
                } ORDER BY .bar_type;
            """,
            [{
                'type': 'test::Parent',
                'bar_type': 'test::Child',
            }, {
                'type': 'test::DerivedParent',
                'bar_type': 'test::DerivedChild',
            }],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: property 'name' does not exist

        Exception: Error while processing
        'ALTER TYPE test::Bar {
            DROP EXTENDING test::Named;
            ALTER PROPERTY name {
                DROP OWNED;
                SET TYPE std::str;
            };
        };'
    ''')
    async def test_edgeql_migration_eq_27(self):
        await self.migrate(r"""
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Foo {
                name := 'foo_27',
            };
            INSERT Bar {
                name := 'bar_27',
            };
        """)

        await self.migrate(r"""
            abstract type Named {
                property name -> str;
            }

            # the types stop extending named, but retain the property
            # 'name'
            type Foo {
                property name -> str;
            };

            type Bar {
                property name -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_27',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_27',
            ],
        )

        await self.migrate(r"""
            abstract type Named {
                property name -> str;
            }

            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'name' to 'title'
                property title -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_27',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.title;
            """,
            [
                'bar_27',
            ],
        )

    async def test_edgeql_migration_eq_29(self):
        await self.migrate(r"""
            type Child {
                property foo -> str;
            }

            alias Base := (
                SELECT Child {
                    bar := .foo
                }
            );
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child {
                foo := 'child_29',
            };
        """)

        await self.migrate(r"""
            # drop everything
        """)

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the third migration.
    ''')
    async def test_edgeql_migration_eq_30(self):
        await self.migrate(r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                property title -> str;
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Foo {
                name := 'foo_30',
            };
            INSERT Bar {
                title := 'bar_30',
            };
        """)

        await self.migrate(r"""
            type Foo {
                property name -> str;
            };

            type Bar {
                # rename 'title' to 'name'
                property name -> str;
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_30',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_30',
            ],
        )

        await self.migrate(r"""
            # both types have a name, so the name prop is factored out
            # into a more basic type.
            abstract type Named {
                property name -> str;
            }

            type Foo extending Named;
            type Bar extending Named;
        """)

        await self.assert_query_result(
            r"""
                SELECT Foo.name;
            """,
            [
                'foo_30',
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT Bar.name;
            """,
            [
                'bar_30',
            ],
        )

    @test.xfail('''
        edgedb.errors.EdgeQLSyntaxError: Unexpected '{'

        Exception: Error while processing
        'ALTER TYPE test::Text {
            DROP PROPERTY body {
                DROP CONSTRAINT std::max_len_value(10000);
            };
        };'
    ''')
    async def test_edgeql_migration_eq_31(self):
        # Issue 727.
        #
        # Starting with the sample schema (from frontpage) migrate to
        # a schema with only type User.
        await self.migrate(r"""
            # This is an abstract object containing
            # text.
            abstract type Text {
              required property body -> str {
                # Maximum length of text is 10000
                # characters.
                constraint max_len_value(10000);
              }
            }

            type User {
              required property name -> str;
            }

            abstract type Owned {
              # By default links are optional.
              required link owner -> User;
            }

            # UniquelyNamed is a an abstract type that
            # enforces name uniqueness across all
            # instances of its subtype.
            abstract type UniquelyNamed {
              required property name -> str {
                delegated constraint exclusive;
              }
            }

            type Status extending UniquelyNamed;

            type Priority extending UniquelyNamed;

            # LogEntry is an Owned and a Text,
            # so it will have all of their links
            # and properties, in particular, the
            # "owner" link and the "body" property.
            type LogEntry extending Owned, Text {
              required property spent_time -> int64;
            }

            type Comment extending Text, Owned {
              required link issue -> Issue;
              link parent -> Comment;
            }
            # issue_num_t is defined as a concrete
            # sequence type, used to generate
            # sequential issue numbers.
            scalar type issue_num_t extending sequence;

            type Issue extending Owned, Text {
              required property title -> str;

              required property number -> issue_num_t {
                # The number values are automatically
                # generated, and are not supposed to be
                # directly writable.
                readonly := true;
              }

              property time_estimate -> int64;

              property start_date -> datetime {
                # The default value of start_date will be a
                # result of the EdgeQL expression above.
                default := (SELECT datetime_current());
              }

              property due_date -> datetime;

              required link status -> Status;

              link priority -> Priority;

              # The watchers link is mapped to User
              # type in many-to-many relation.
              multi link watchers -> User;

              multi link time_spent_log -> LogEntry {
                # Exclusive multi-link represents
                # a one-to-many relation.
                constraint exclusive;
              }

              multi link related_to -> Issue;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Status {
                name := 'Open'
            };
            INSERT Status {
                name := 'Closed'
            };

            INSERT User {
                name := 'cosmophile'
            };
        """)

        await self.migrate(r"""
            type User {
              required property name -> str;
            }
        """)

        # there's only the User left
        await self.assert_query_result(
            r"""
                SELECT User.name;
            """,
            [
                'cosmophile',
            ],
        )

    async def test_edgeql_migration_eq_32(self):
        # Issue 727.
        #
        # Starting with a small schema migrate to remove its elements.
        # There are non-zero default Objects existing in a fresh blank
        # database because of placeholder objects used for GraphQL.
        start_objects = await self.con.query_one(r"""
            SELECT count(Object);
        """)

        await self.migrate(r"""
            type LogEntry {
              required property spent_time -> int64;
            }
            type Issue {
              multi link time_spent_log -> LogEntry {
                constraint exclusive;
              }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT LogEntry {
                spent_time := 100
            };

            INSERT Issue {
                time_spent_log := LogEntry
            };
        """)

        await self.migrate(r"""
            type LogEntry {
              required property spent_time -> int64;
            }
        """)

        # there's only the LogEntry left
        await self.assert_query_result(
            r"""
                SELECT LogEntry.spent_time;
            """,
            [
                100,
            ],
        )
        await self.assert_query_result(
            r"""
                SELECT count(Object);
            """,
            [
                start_objects + 1,
            ],
        )

        await self.migrate(r"""
            # empty schema
        """)

        # no more additional objects
        await self.assert_query_result(
            r"""
                SELECT count(Object);
            """,
            [
                start_objects,
            ],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_33(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child;
            INSERT Base {
                foo := (SELECT Child LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child'},
                }
            }],
        )

        await self.migrate(r"""
            type Child;
            type Child2;

            type Base {
                # change link type
                link foo -> Child2;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                # the link is empty because the target was changed
                'foo': None
            }],
        )

        await self.con.execute(r"""
            INSERT Child2;

            UPDATE Base
            SET {
                foo := (SELECT Child2 LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child2'},
                }
            }],
        )

    async def test_edgeql_migration_eq_34(self):
        # this is the reverse of test_edgeql_migration_eq_11
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    constraint exclusive;
                }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child;
            INSERT Base {
                foo := (SELECT Child LIMIT 1)
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        __type__: {name},
                    }
                };
            """,
            [{
                'foo': {
                    '__type__': {'name': 'test::Child'},
                }
            }],
        )

        await self.migrate(r"""
            type Base {
                # change link to property with same name
                property foo -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo
                };
            """,
            [{
                # the property is empty now
                'foo': None
            }],
        )

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := 'base_foo_34'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo
                };
            """,
            [{
                'foo': 'base_foo_34'
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_35(self):
        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo := (
                    SELECT Child FILTER .name = 'computable_35'
                )
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child {
                name := 'computable_35'
            };
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'computable_35',
                }]
            }]
        )

        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a link from a computable to regular
                multi link foo -> Child;
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': []
            }]
        )

        # Make sure that the new 'foo' can be updated.
        await self.con.execute(r"""
            INSERT Child {
                name := 'child_35'
            };
            UPDATE Base
            SET {
                foo := (
                    SELECT Child FILTER .name = 'child_35'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': [{
                    'name': 'child_35'
                }]
            }]
        )

    async def test_edgeql_migration_eq_36(self):
        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                multi link foo -> Child;
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Child {
                name := 'computable_36'
            };
            INSERT Child {
                name := 'child_36'
            };
            INSERT Base {
                foo := (
                    SELECT Child FILTER .name = 'child_36'
                )
            };
        """)

        await self.migrate(r"""
            type Child {
                required property name -> str;
            }

            type Base {
                # change a regular link to a computable
                link foo := (
                    SELECT Child FILTER .name = 'computable_36'
                    LIMIT 1
                )
            }
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: {
                        name
                    },
                };
            """,
            [{
                'foo': {
                    'name': 'computable_36'
                }
            }]
        )

    async def test_edgeql_migration_eq_37(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    foo := 'base_alias_37'
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo
                };
            """,
            [{
                'foo': 'base_alias_37'
            }]
        )

        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new property
                    foo2 := 'base_alias_37'
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo2
                };
            """,
            [{
                'foo2': 'base_alias_37'
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"object type 'test::Base' has no link or property 'foo'"):
            await self.con.execute(r"""
                SELECT BaseAlias {
                    foo
                };
            """)

    async def test_edgeql_migration_eq_38(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    foo := 'base_alias_38'
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo
                };
            """,
            [{
                'foo': 'base_alias_38'
            }]
        )

        await self.migrate(r"""
            type Base;

            alias BaseAlias := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := 38
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo
                };
            """,
            [{
                'foo': 38
            }]
        )

    async def test_edgeql_migration_eq_39(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'base_alias_39')
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'base_alias_39'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_39'
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    # "rename" a computable, since the value is given and
                    # not stored, this is no different from dropping
                    # original and creating a new multi-link
                    foo2 := (SELECT Foo FILTER .name = 'base_alias_39')
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo2: {
                        name
                    }
                };
            """,
            [{
                'foo2': [{
                    'name': 'base_alias_39'
                }]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"object type 'test::Base' has no link or property 'foo'"):
            await self.con.execute(r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """)

    async def test_edgeql_migration_eq_40(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (SELECT Foo FILTER .name = 'foo_40')
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'foo_40'};
            INSERT Bar {name := 'bar_40'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'foo_40'
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            type Bar {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    # keep the name, but change the type
                    foo := (SELECT Bar FILTER .name = 'bar_40')
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'bar_40'
                }]
            }]
        )

    async def test_edgeql_migration_eq_41(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_alias_41'
                        }
                        FILTER .name = 'base_alias_41'
                    )
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'base_alias_41'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_41',
                    '@bar': 'foo_bar_alias_41',
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # "rename" a computable link property, since
                            # the value is given and not stored, this is
                            # no different from dropping original and
                            # creating a new multi-link
                            @baz := 'foo_bar_alias_41'
                        }
                        FILTER .name = 'base_alias_41'
                    )
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @baz
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_41',
                    '@baz': 'foo_bar_alias_41'
                }]
            }]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidReferenceError,
                r"link 'foo' .* has no property 'bar'"):
            await self.con.execute(r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """)

    @test.xfail('''
        edgedb.errors.InternalServerError: relation
        "edgedb_fe4eeff4-..." does not exist

        The error occurs at the second "migrate".
    ''')
    async def test_edgeql_migration_eq_42(self):
        # testing schema alias
        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            @bar := 'foo_bar_alias_42'
                        }
                        FILTER .name = 'base_alias_42'
                    )
                }
            )
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
            INSERT Foo {name := 'base_alias_42'};
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_42',
                    '@bar': 'foo_bar_alias_42',
                }]
            }]
        )

        await self.migrate(r"""
            type Base;

            type Foo {
                property name -> str
            }

            alias BaseAlias := (
                SELECT Base {
                    foo := (
                        SELECT Foo {
                            # keep the name, but change the type
                            @bar := 42
                        }
                        FILTER .name = 'base_alias_42'
                    )
                }
            )
        """)

        await self.assert_query_result(
            r"""
                SELECT BaseAlias {
                    foo: {
                        name,
                        @bar
                    }
                };
            """,
            [{
                'foo': [{
                    'name': 'base_alias_42',
                    '@bar': 42,
                }]
            }]
        )

    async def test_edgeql_migration_eq_43(self):
        await self.migrate(r"""
            abstract link Ordered {
                property index -> int32;
            }
            type User;
            abstract type Permissions {
                multi link owners extending Ordered -> User;
            };
        """)
        await self.migrate(r"")

    async def test_edgeql_migration_eq_function_01(self):
        await self.migrate(r"""
            function hello01(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello01(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self.migrate(r"""
            function hello01(a: int64, b: int64=42) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello01(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello01(1, 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_eq_function_02(self):
        await self.migrate(r"""
            function hello02(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello02(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self.migrate(r"""
            function hello02(a: int64, b: OPTIONAL int64=42) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello02(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello02(1, 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_eq_function_03(self):
        await self.migrate(r"""
            function hello03(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello03(1);""",
            ['hello1'],
        )

        # add an extra parameter with a default (so it can be omitted
        # in principle)
        await self.migrate(r"""
            function hello03(a: int64, NAMED ONLY b: int64=42) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>(a + b)
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello03(1);""",
            ['hello43'],
        )

        await self.assert_query_result(
            r"""SELECT hello03(1, b := 2);""",
            ['hello3'],
        )

    async def test_edgeql_migration_eq_function_04(self):
        await self.migrate(r"""
            function hello04(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello04(1);""",
            ['hello1'],
        )

        # same parameters, different return type
        await self.migrate(r"""
            function hello04(a: int64) -> int64
                using edgeql $$
                    SELECT -a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello04(1);""",
            [-1],
        )

    async def test_edgeql_migration_eq_function_05(self):
        await self.migrate(r"""
            function hello05(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello05(1);""",
            ['1'],
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello05(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello05(1);""",
            [[1]],
        )

    @test.xfail('''
        It should be possible to change the underlying function (to a
        compatible one) of a default value without explicitly dropping
        the default first.

        edgedb.errors.InternalServerError: cannot drop function
        "edgedb_06261450-db74-11e9-9e9a-9520733a1c54".hello06(bigint)
        because other objects depend on it

        This is similar to the problem with renaming property used in
        an expression.

        See also `test_edgeql_migration_eq_function_10` and
        `test_edgeql_migration_eq_index_01`.
    ''')
    async def test_edgeql_migration_eq_function_06(self):
        await self.migrate(r"""
            function hello06(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello06(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base {
                property foo -> int64 {
                    # use the function in default value computation
                    default := len(hello06(2) ++ hello06(123))
                }
            }
        """)
        await self.con.execute(r"""
            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4, 2},
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop function
        'test::hello07(a: std::int64)' because other objects in the
        schema depend on it
    ''')
    async def test_edgeql_migration_eq_function_07(self):
        await self.migrate(r"""
            function hello07(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                # use the function in computable value
                property foo := len(hello07(2) ++ hello07(123))
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello07(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base {
                # use the function in computable value
                property foo := len(hello07(2) ++ hello07(123))
            }
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            {2},
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop function
        'test::hello08(a: std::int64)' because other objects in the
        schema depend on it
    ''')
    async def test_edgeql_migration_eq_function_08(self):
        await self.migrate(r"""
            function hello08(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            # use the function in a alias directly
            alias foo := len(hello08(2) ++ hello08(123));
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello08(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            # use the function in a alias directly
            alias foo := len(hello08(2) ++ hello08(123));
        """)

        await self.assert_query_result(
            r"""SELECT foo;""",
            {2},
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop function
        'test::hello09(a: std::int64)' because other objects in the
        schema depend on it
    ''')
    async def test_edgeql_migration_eq_function_09(self):
        await self.migrate(r"""
            function hello09(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base;

            # use the function in a alias directly
            alias BaseAlias := (
                SELECT Base {
                    foo := len(hello09(2) ++ hello09(123))
                }
            );
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias.foo;""",
            {4},
        )

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello09(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base;

            # use the function in a alias directly
            alias BaseAlias := (
                SELECT Base {
                    foo := len(hello09(2) ++ hello09(123))
                }
            );
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias.foo;""",
            {2},
        )

    @test.xfail('''
        edgedb.errors.InternalServerError: cannot drop function
        "edgedb_bea19e4a-ec4b-11e9-9900-557227410171".hello10(bigint)
        because other objects depend on it

        This is similar to the problem with renaming property used in
        an expression.

        See also `test_schema_migrations_equivalence_function_10`,
        `test_edgeql_migration_eq_function_06`,
        `test_edgeql_migration_eq_index_01`.
    ''')
    async def test_edgeql_migration_eq_function_10(self):
        await self.migrate(r"""
            function hello10(a: int64) -> str
                using edgeql $$
                    SELECT <str>a
                $$;

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """)
        await self.con.execute(r"""
            SET MODULE test;
        """)

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                r'invalid foo'):
            async with self.con.transaction():
                await self.con.execute(r"""
                    INSERT Base {foo := 42};
                """)

        # same parameters, different return type (array)
        await self.migrate(r"""
            function hello10(a: int64) -> array<int64>
                using edgeql $$
                    SELECT [a]
                $$;

            type Base {
                required property foo -> int64 {
                    # use the function in a constraint expression
                    constraint expression on (len(hello10(__subject__)) < 2)
                }
            }
        """)

        # no problem with the constraint now
        await self.con.execute(r"""
            INSERT Base {foo := 42};
        """)

    async def test_edgeql_migration_eq_function_11(self):
        await self.migrate(r"""
            function hello11(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello11(1);""",
            ['hello1'],
        )

        await self.migrate(r"""
            # replace the function with a new one by the same name
            function hello11(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello11(' world');""",
            ['hello world'],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant hello11'):
            await self.con.execute(
                r"""SELECT hello11(1);"""
            )

    async def test_edgeql_migration_eq_function_12(self):
        await self.migrate(r"""
            function hello12(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello12(1);""",
            ['hello1'],
        )

        await self.migrate(r"""
            function hello12(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            # make the function polymorphic
            function hello12(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello12(' world');""",
            ['hello world'],
        )

        # make sure that the old one still works
        await self.assert_query_result(
            r"""SELECT hello12(1);""",
            ['hello1'],
        )

    async def test_edgeql_migration_eq_function_13(self):
        # this is the inverse of test_edgeql_migration_eq_function_12
        await self.migrate(r"""
            # start with a polymorphic function
            function hello13(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;

            function hello13(a: str) -> str
                using edgeql $$
                    SELECT 'hello' ++ a
                $$;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello13(' world');""",
            ['hello world'],
        )
        await self.assert_query_result(
            r"""SELECT hello13(1);""",
            ['hello1'],
        )

        await self.migrate(r"""
            # remove one of the 2 versions
            function hello13(a: int64) -> str
                using edgeql $$
                    SELECT 'hello' ++ <str>a
                $$;
        """)

        await self.assert_query_result(
            r"""SELECT hello13(1);""",
            ['hello1'],
        )

        # make sure that the other one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant hello13'):
            await self.con.execute(
                r"""SELECT hello13(' world');"""
            )

    async def test_edgeql_migration_eq_function_14(self):
        await self.migrate(r"""
            function hello14(a: str, b: str) -> str
                using edgeql $$
                    SELECT a ++ b
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello14('hello', '14');""",
            ['hello14'],
        )

        await self.migrate(r"""
            # Replace the function with a new one by the same name,
            # but working with arrays.
            function hello14(a: array<str>, b: array<str>) -> array<str>
                using edgeql $$
                    SELECT a ++ b
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello14(['hello'], ['14']);""",
            [['hello', '14']],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant hello14'):
            await self.assert_query_result(
                r"""SELECT hello14('hello', '14');""",
                ['hello14'],
            )

    async def test_edgeql_migration_eq_function_15(self):
        await self.migrate(r"""
            function hello15(a: str, b: str) -> str
                using edgeql $$
                    SELECT a ++ b
                $$
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT hello15('hello', '15');""",
            ['hello15'],
        )

        await self.migrate(r"""
            # Replace the function with a new one by the same name,
            # but working with arrays.
            function hello15(a: tuple<str, str>) -> str
                using edgeql $$
                    SELECT a.0 ++ a.1
                $$
        """)

        await self.assert_query_result(
            r"""SELECT hello15(('hello', '15'));""",
            ['hello15'],
        )

        # make sure that the old one is gone
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not find a function variant hello15'):
            await self.assert_query_result(
                r"""SELECT hello15('hello', '15');""",
                ['hello15'],
            )

    async def test_edgeql_migration_eq_function_16(self):
        # Test prop default and function order of definition. The
        # function happens to be shadowing a "std" function. We expect
        # that the function `test::to_upper` will actually be used.
        #
        # See also `test_schema_get_migration_21`
        await self.migrate(r"""
            type Foo16 {
                property name -> str {
                    default := str_upper('some_name');
                };
            }

            function str_upper(val: str) -> str {
                using (SELECT '^^' ++ std::str_upper(val) ++ '^^');
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""SELECT str_upper('hello');""",
            ['^^HELLO^^'],
        )

        await self.con.execute("""
            INSERT Foo16;
        """)
        await self.assert_query_result(
            r"""SELECT Foo16.name;""",
            ['^^SOME_NAME^^'],
        )

    async def test_edgeql_migration_eq_linkprops_01(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child;
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := (INSERT Child)
            };
        """)

        # Migration adding a link property.
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)

        # actually record a link property
        await self.con.execute(r"""
            UPDATE
                Base
            SET {
                foo: {
                    @bar := 'lp01'
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 'lp01'}}],
        )

    async def test_edgeql_migration_eq_linkprops_02(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 'lp02'},
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property name
                    property bar2 -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar2 }
                };
            """,
            [{'foo': {'@bar2': 'lp02'}}],
        )

    async def test_edgeql_migration_eq_linkprops_03(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> int64
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 3},
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    # change the link property type
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': '3'}}],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_linkprops_04(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 'lp04'},
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # change the link cardinality
                multi link foo -> Child {
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': [{'@bar': 'lp04'}]}],
        )

    async def test_edgeql_migration_eq_linkprops_05(self):
        await self.migrate(r"""
            type Child;

            type Base {
                multi link foo -> Child {
                    property bar -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {foo := (INSERT Child)};
            UPDATE Base
            SET {
                foo: {@bar := 'lp05'},
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # change the link cardinality
                link foo -> Child {
                    property bar -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    foo: { @bar }
                };
            """,
            [{'foo': {'@bar': 'lp05'}}],
        )

    async def test_edgeql_migration_eq_linkprops_06(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str;
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {child := (INSERT Child)};
            UPDATE Base
            SET {
                child: {
                    @foo := 'lp06',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str;
                    # add another link prop
                    property bar -> int64;
                }
            };
        """)
        # update the existing data with a new link prop 'bar'
        await self.con.execute(r"""
            UPDATE Base
            SET {
                child: {
                    @bar := 111,
                },
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                        @bar
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp06',
                    '@bar': 111
                }
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        The second migration fails.
    ''')
    async def test_edgeql_migration_eq_linkprops_07(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    property foo -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp07',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                # move the link property earlier in the inheritance tree
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp07',
                }
            }],
        )

    async def test_edgeql_migration_eq_linkprops_08(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child {
                    property foo -> str
                }
            };

            type Derived extending Base;
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
        """)
        await self.con.execute(r"""
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp08',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    # move the link property later in the inheritance tree
                    property foo -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    children := count(.child)
                };
            """,
            [{
                'children': 1,
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Derived {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp08',
                }
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        The second migration fails.
    ''')
    async def test_edgeql_migration_eq_linkprops_09(self):
        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    property foo -> str
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp09',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            # factor out link property all the way to an abstract link
            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp09',
                }
            }],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop abstract link
        'test::base_child' because other objects in the schema depend
        on it

        DETAILS: link 'child' of object type 'test::Derived' depends
        on test::base_child; link 'child' of object type 'test::Base'
        depends on test::base_child

        Exception: Error while processing
        'DROP ABSTRACT LINK test::base_child {
            DROP PROPERTY foo;
        };'
    ''')
    async def test_edgeql_migration_eq_linkprops_10(self):
        # reverse of the test_edgeql_migration_eq_linkprops_09 refactoring
        await self.migrate(r"""
            type Child;

            abstract link base_child {
                property foo -> str;
            }

            type Base {
                link child extending base_child -> Child;
            };

            type Derived extending Base;
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Derived {child := (INSERT Child)};
            UPDATE Derived
            SET {
                child: {
                    @foo := 'lp10',
                },
            };
        """)

        await self.migrate(r"""
            type Child;

            type Base {
                link child -> Child
            };

            type Derived extending Base {
                overloaded link child -> Child {
                    # move the link property later in the inheritance tree
                    property foo -> str
                }
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT Base {
                    child: {
                        @foo,
                    }
                };
            """,
            [{
                'child': {
                    '@foo': 'lp10',
                }
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        The second migration fails.
    ''')
    async def test_edgeql_migration_eq_linkprops_11(self):
        # merging a link with the same properties
        await self.migrate(r"""
            type Thing;

            type Owner {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Renter {
                link item -> Thing {
                    property foo -> str;
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Owner {item := (INSERT Thing)};
            UPDATE Owner
            SET {
                item: {
                    @foo := 'owner_lp11',
                },
            };

            INSERT Renter {item := (INSERT Thing)};
            UPDATE Renter
            SET {
                item: {
                    @foo := 'renter_lp11',
                },
            };
        """)

        await self.migrate(r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Owner {
                    item: {
                        @foo,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'owner_lp11',
                }
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Renter {
                    item: {
                        @foo,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'renter_lp11',
                }
            }],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop inherited property
        'bar' of link 'item' of object type 'test::Owner'

        DETAILS: property 'bar' of link 'item' of object type
        'test::Owner' is inherited from:
        - link 'item' of object type 'test::Base'

        Exception: Error while processing
        'ALTER TYPE test::Owner {
            EXTENDING test::Base LAST;
            ALTER LINK item {
                DROP OWNED;
                ALTER PROPERTY foo {
                    DROP OWNED;
                };
            };
        };'
    ''')
    async def test_edgeql_migration_eq_linkprops_12(self):
        # merging a link with different properties
        await self.migrate(r"""
            type Thing;

            type Owner {
                link item -> Thing {
                    property foo -> str;
                }
            };

            type Renter {
                link item -> Thing {
                    property bar -> str;
                }
            };
        """)
        await self.con.execute(r"""
            SET MODULE test;

            INSERT Owner {item := (INSERT Thing)};
            UPDATE Owner
            SET {
                item: {
                    @foo := 'owner_lp11',
                },
            };

            INSERT Renter {item := (INSERT Thing)};
            UPDATE Renter
            SET {
                item: {
                    @bar := 'renter_lp11',
                },
            };
        """)

        await self.migrate(r"""
            type Thing;

            type Base {
                link item -> Thing {
                    property foo -> str;
                    property bar -> str;
                }
            };

            type Owner extending Base;

            type Renter extending Base;
        """)

        await self.assert_query_result(
            r"""
                SELECT Owner {
                    item: {
                        @foo,
                        @bar,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': 'owner_lp11',
                    '@bar': None,
                }
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT Renter {
                    item: {
                        @foo,
                        @bar,
                    }
                };
            """,
            [{
                'item': {
                    '@foo': None,
                    '@bar': 'renter_lp11',
                }
            }],
        )

    async def test_edgeql_migration_eq_annotation_01(self):
        await self.migrate(r"""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                # add a title annotation
                annotation title := 'Base description 01'
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }],
            }],
        )

        await self.migrate(r"""
            # add inheritable and non-inheritable annotations
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 01'
                }],
            }],
        )

        await self.migrate(r"""
            abstract annotation foo_anno;
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 01';
                annotation foo_anno := 'Base foo_anno 01';
                annotation bar_anno := 'Base bar_anno 01';
            }

            # extend Base
            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 01'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 01'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [{
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 01'
                }],
            }],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot get 'name' value: item
        '4e2fe443-04a6-11eb-a38f-a315784c86dc' is not present in the
        schema <Schema gen:3747 at 0x7fd78b613790>

        This happens on the final migration.
    ''')
    async def test_edgeql_migration_eq_annotation_02(self):
        await self.migrate(r"""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            abstract annotation foo_anno;

            type Base {
                annotation title := 'Base description 02';
                annotation foo_anno := 'Base foo_anno 02';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 02'
                }, {
                    'name': 'test::foo_anno',
                    '@value': 'Base foo_anno 02'
                }],
            }, {
                'name': 'test::Derived',
                # annotation not inherited
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            # remove foo_anno
            type Base {
                annotation title := 'Base description 02';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 02'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [],
            }],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot get 'name' value: item
        '54615193-04a6-11eb-a05a-af5ecac7408d' is not present in the
        schema <Schema gen:3751 at 0x7fd78bbe74c0>

        This happens on the final migration.
    ''')
    async def test_edgeql_migration_eq_annotation_03(self):
        await self.migrate(r"""
            type Base;
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [],
            }],
        )

        await self.migrate(r"""
            abstract inheritable annotation bar_anno;

            type Base {
                annotation title := 'Base description 03';
                annotation bar_anno := 'Base bar_anno 03';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 03'
                }, {
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 03'
                }],
            }, {
                'name': 'test::Derived',
                # annotation inherited
                'annotations': [{
                    'name': 'test::bar_anno',
                    '@value': 'Base bar_anno 03'
                }],
            }],
        )

        await self.migrate(r"""
            # remove bar_anno
            type Base {
                annotation title := 'Base description 03';
            }

            type Derived extending Base;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'std::title',
                    '@value': 'Base description 03'
                }],
            }, {
                'name': 'test::Derived',
                'annotations': [],
            }],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_annotation_04(self):
        # Test migration of annotation value ano nothing else.
        await self.migrate(r"""
            abstract annotation description;

            type Base {
                annotation description := "1";
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'test::description',
                    '@value': '1',
                }],
            }],
        )

        await self.migrate(r"""
            abstract annotation description;

            type Base {
                annotation description := "2";
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    annotations: {
                        name,
                        @value
                    } ORDER BY .name
                }
                FILTER .name LIKE 'test::%'
                ORDER BY .name;
            """,
            [{
                'name': 'test::Base',
                'annotations': [{
                    'name': 'test::description',
                    '@value': '2',
                }],
            }],
        )

    async def test_edgeql_migration_eq_index_01(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self.migrate(r"""
            type Base {
                property name -> str;
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                # an index
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self.migrate(r"""
            type Base {
                # rename the indexed property
                property title -> str;
                index on (.title);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.title'
                }]
            }],
        )

    async def test_edgeql_migration_eq_index_02(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                index on (.name);
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                # remove the index
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

    async def test_edgeql_migration_eq_index_03(self):
        await self.migrate(r"""
            type Base {
                property name -> int64;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                property name -> int64;
                # an index
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

        await self.migrate(r"""
            type Base {
                # change the indexed property type
                property name -> str;
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

    async def test_edgeql_migration_eq_index_04(self):
        await self.migrate(r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [],
            }],
        )

        await self.migrate(r"""
            type Base {
                property first_name -> str;
                property last_name -> str;
                property name := .first_name ++ ' ' ++ .last_name;
                # an index on a computable
                index on (.name);
            }
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

    @test.xfail('''
        AssertionError: data shape differs: 'SELECT .name' != '.name'
        PATH: [0]["indexes"][0]["expr"]

        This may be using obsolete `orig_expr`, so perhaps the test is
        no longer correct.
    ''')
    async def test_edgeql_migration_eq_index_05(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                # an index with a verbose definition (similar to
                # DESCRIBE AS SDL)
                index on (
                    WITH MODULE test
                    SELECT .name
                ) {
                    orig_expr := '.name';
                }
            }
        """)
        await self.con.execute("""
            SET MODULE test;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT ObjectType {
                    name,
                    indexes: {
                        expr
                    }
                }
                FILTER .name = 'test::Base';
            """,
            [{
                'name': 'test::Base',
                'indexes': [{
                    'expr': '.name'
                }]
            }],
        )

    async def test_edgeql_migration_eq_collections_01(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                property foo -> array<float32>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := [1.2, 4.5]
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[1.2, 4.5]],
        )

    async def test_edgeql_migration_eq_collections_02(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := ('hello', 42)
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['hello', 42]],
        )

    async def test_edgeql_migration_eq_collections_03(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                # nested collection
                property foo -> tuple<str, int32, array<float32>>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := ('test', 42, [1.2, 4.5])
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['test', 42, [1.2, 4.5]]],
        )

    async def test_edgeql_migration_eq_collections_04(self):
        await self.migrate(r"""
            type Base;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base;
        """)

        await self.migrate(r"""
            type Base {
                property foo -> tuple<a: str, b: int32>;
            }
        """)

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := (a := 'hello', b := 42)
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [{'a': 'hello', 'b': 42}],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_06(self):
        await self.migrate(r"""
            type Base {
                property foo -> array<int32>;
            }
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := [1, 2]
            }
        """)
        # sanity check
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[1, 2]],
        )

        await self.migrate(r"""
            type Base {
                property foo -> array<float32>;
            }
        """)

        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [[1, 2]],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_07(self):
        await self.con.execute("""
            SET MODULE test;
        """)
        await self.migrate(r"""
            type Base {
                # convert property type to tuple
                property foo -> tuple<str, int32>;
            }
        """)

        await self.con.execute(r"""
            INSERT Base {
                foo := ('test', <int32>7)
            }
        """)

        await self.migrate(r"""
            type Base {
                # convert property type to a bigger tuple
                property foo -> tuple<str, int32, int32>;
            }
        """)

        # expect that the new value is simply empty
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [],
        )

        await self.con.execute(r"""
            UPDATE Base
            SET {
                foo := ('new', 7, 1)
            };
        """)
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['new', 7, 1]],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_08(self):
        await self.migrate(r"""
            type Base {
                property foo -> tuple<int32, int32>;
            }
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := (0, 8)
            }
        """)

        await self.migrate(r"""
            type Base {
                # convert property type to a tuple with different (but
                # cast-compatible) element types
                property foo -> tuple<str, int32>;
            }
        """)

        # In theory, since under normal circumstances we can cast one
        # tuple into the other, it's reasonable to expect this
        # migration to preserve data here.
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [['0', 8]],
        )

    @test.xfail('''
        The "complete" flag is not set even though the DDL from
        "proposed" list is used.

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_09(self):
        await self.migrate(r"""
            type Base {
                property foo -> tuple<str, int32>;
            }
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := ('test', 9)
            }
        """)

        await self.migrate(r"""
            type Base {
                # convert property type from unnamed to named tuple
                property foo -> tuple<a: str, b: int32>;
            }
        """)

        # In theory, since under normal circumstances we can cast an
        # unnamed tuple into named, it's reasonable to expect this
        # migration to preserve data here.
        await self.assert_query_result(
            r"""SELECT Base.foo;""",
            [{'a': 'test', 'b': 9}],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop scalar type
        'test::CollAlias' because other objects in the schema depend
        on it

        Exception: Error while processing 'DROP SCALAR TYPE test::CollAlias;'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_13(self):
        await self.migrate(r"""
            type Base {
                property foo -> float32;
            };

            # alias that don't have arrays
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 13.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 13.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [13.5],
        )

        await self.migrate(r"""
            type Base {
                property foo -> float32;
            };

            # "same" alias that now have arrays
            alias BaseAlias := Base { bar := [Base.foo] };
            alias CollAlias := [Base.foo];
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': [13.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [[13.5]],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop scalar type
        'test::CollAlias' because other objects in the schema depend
        on it

        DETAILS: alias 'test::CollAlias' depends on test::CollAlias

        Exception: Error while processing
        'DROP SCALAR TYPE test::CollAlias;'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_14(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias that don't have tuples
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            INSERT Base {
                name := 'coll_14',
                foo := 14.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 14.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [14.5],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # "same" alias that now have tuples
            alias BaseAlias := Base { bar := (Base.name, Base.foo) };
            alias CollAlias := (Base.name, Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': ['coll_14', 14.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_14', 14.5]],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop scalar type
        'test::CollAlias' because other objects in the schema depend
        on it

        Exception: Error while processing
        'DROP SCALAR TYPE test::CollAlias;'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_15(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias that don't have nested collections
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_15',
                number := 15,
                foo := 15.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 15.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [15.5],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # "same" alias that now have nested collections
            alias BaseAlias := Base {
                bar := (Base.name, Base.number, [Base.foo])
            };
            alias CollAlias := (Base.name, Base.number, [Base.foo]);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': ['coll_15', 15, [15.5]]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_15', 15, [15.5]]],
        )

    @test.xfail('''
        edgedb.errors.SchemaError: cannot drop scalar type
        'test::CollAlias' because other objects in the schema depend
        on it

        Exception: Error while processing
        'DROP SCALAR TYPE test::CollAlias;'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_16(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias that don't have named tuples
            alias BaseAlias := Base { bar := Base.foo };
            alias CollAlias := Base.foo;
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_16',
                foo := 16.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': 16.5}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [16.5],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # "same" alias that now have named tuples
            alias BaseAlias := Base {
                bar := (a := Base.name, b := Base.foo)
            };
            alias CollAlias := (a := Base.name, b := Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{bar};""",
            [{'bar': {'a': 'coll_16', 'b': 16.5}}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [{'a': 'coll_16', 'b': 16.5}],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: schema item
        'test::CollAlias' does not exist

        Exception: Error while processing
        'ALTER ALIAS test::CollAlias USING ([test::Base.foo]);'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_17(self):
        await self.migrate(r"""
            type Base {
                property foo -> float32;
                property bar -> int32;
            };

            # alias with array<int32>
            alias BaseAlias := Base { data := [Base.bar] };
            alias CollAlias := [Base.bar];
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                foo := 17.5,
                bar := 17,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': [17]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [[17]],
        )

        await self.migrate(r"""
            type Base {
                property foo -> float32;
                property bar -> int32;
            };

            # alias with array<float32>
            alias BaseAlias := Base { data := [Base.foo] };
            alias CollAlias := [Base.foo];
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': [17.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [[17.5]],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: schema item
        'test::CollAlias' does not exist

        Exception: Error while processing
        'ALTER ALIAS test::CollAlias USING ((
            test::Base.name,
            test::Base.number,
            test::Base.foo
        ));'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_18(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, int32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number)
            };
            alias CollAlias := (Base.name, Base.number);
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_18',
                number := 18,
                foo := 18.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['coll_18', 18]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_18', 18]],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, int32, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number, Base.foo)
            };
            alias CollAlias := (Base.name, Base.number, Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['coll_18', 18, 18.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_18', 18, 18.5]],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: schema item
        'test::CollAlias' does not exist

        Exception: Error while processing
        'ALTER ALIAS test::CollAlias USING ((
            test::Base.name,
            test::Base.foo
        ));'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_20(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, int32>
            alias BaseAlias := Base {
                data := (Base.name, Base.number)
            };
            alias CollAlias := (Base.name, Base.number);
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'test20',
                number := 20,
                foo := 123.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['test20', 20]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['test20', 20]],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property number -> int32;
                property foo -> float32;
            };

            # alias with tuple<str, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.foo)
            };
            alias CollAlias := (Base.name, Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias {data};""",
            [{'data': ['test20', 123.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['test20', 123.5]],
        )

    @test.xfail('''
        edgedb.errors.InvalidReferenceError: schema item
        'test::CollAlias' does not exist

        Exception: Error while processing
        'ALTER ALIAS test::CollAlias USING ((
            a := test::Base.name,
            b := test::Base.foo
        ));'

        This happens on the second migration.
    ''')
    async def test_edgeql_migration_eq_collections_21(self):
        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias with tuple<str, float32>
            alias BaseAlias := Base {
                data := (Base.name, Base.foo)
            };
            alias CollAlias := (Base.name, Base.foo);
        """)

        await self.con.execute(r"""
            SET MODULE test;

            INSERT Base {
                name := 'coll_21',
                foo := 21.5,
            }
        """)

        # make sure that the alias initialized correctly
        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': ['coll_21', 21.5]}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [['coll_21', 21.5]],
        )

        await self.migrate(r"""
            type Base {
                property name -> str;
                property foo -> float32;
            };

            # alias with named tuple<a: str, b: float32>
            alias BaseAlias := Base {
                data := (a := Base.name, b := Base.foo)
            };
            alias CollAlias := (a := Base.name, b := Base.foo);
        """)

        await self.assert_query_result(
            r"""SELECT BaseAlias{data};""",
            [{'data': {'a': 'coll_21', 'b': 21.5}}],
        )
        await self.assert_query_result(
            r"""SELECT CollAlias;""",
            [{'a': 'coll_21', 'b': 21.5}],
        )

    async def test_edgeql_migration_eq_drop_module(self):
        await self.migrate(r"""
            type Base;
        """, module='test')

        await self.migrate(r"""
            scalar type foo extending std::str;
        """, module='newtest')

        await self.assert_query_result(
            'SELECT (SELECT schema::Module FILTER .name LIKE "%test").name;',
            ['newtest']
        )

    async def test_edgeql_migration_rename_type_02(self):
        await self.migrate(r"""
            type Note {
                property note -> str;
            }
            type Subtype extending Note;
            type Link {
                link a -> Note;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Note.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Note FILTER Note.note = ComputeLink.foo);
            };
            alias Alias := Note;
        """)

        await self.migrate(r"""
            type Remark {
                property note -> str;
            }
            type Subtype extending Remark;
            type Link {
                link a -> Remark;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Remark.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Remark FILTER Remark.note = ComputeLink.foo);
            };
            alias Alias := Remark;
        """)

        await self.migrate("")

    async def test_edgeql_migration_rename_type_03(self):
        await self.migrate(r"""
            type Note {
                property note -> str;
            }
        """)

        await self.migrate(r"""
            type Remark {
                property note -> str;
            }
            type Subtype extending Remark;
            type Link {
                link a -> Remark;
            }
            type Uses {
                required property x -> str {
                    default := (SELECT Remark.note LIMIT 1)
                }
            };
            type ComputeLink {
                property foo -> str;
                multi link x := (
                    SELECT Remark FILTER Remark.note = ComputeLink.foo);
            };
            alias Alias := Remark;
        """)

        await self.migrate("")
