#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import os.path

import edgedb

from edb.testbase import server as tb


class TestDeltas(tb.DDLTestCase):

    async def test_delta_simple_01(self):
        await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO {
                type NamedObject {
                    required property name -> str;
                    multi link related -> NamedObject {
                        property lang -> str;
                    };
                };
            };

            COMMIT MIGRATION test::d1;

            INSERT test::NamedObject {
                name := 'Test'
            };

            INSERT test::NamedObject {
                name := 'Test 2',
                related := (SELECT DETACHED test::NamedObject
                            FILTER .name = 'Test')
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    test::NamedObject {
                        related: {
                            name,
                            @lang
                        }
                    }
                FILTER
                    test::NamedObject.name = 'Test 2';
            """,
            [
                {
                    'related': [{'name': 'Test', '@lang': None}],
                }
            ]
        )

    async def test_delta_drop_01(self):
        # Check that constraints defined on scalars being dropped are
        # dropped.
        await self.con.execute("""
            CREATE SCALAR TYPE test::a1 EXTENDING std::str;

            ALTER SCALAR TYPE test::a1 {
                CREATE CONSTRAINT std::one_of('a', 'b') {
                    SET ANNOTATION description :=
                        'test_delta_drop_01_constraint';
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Constraint {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_01_constraint';
            """,
            [
                {
                    'name': 'std::one_of',
                }
            ],
        )

        await self.con.execute("""
            DROP SCALAR TYPE test::a1;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Constraint {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_01_constraint';
            """,
            []
        )

    async def test_delta_drop_02(self):
        # Check that links defined on types being dropped are
        # dropped.
        await self.con.execute("""
            CREATE TYPE test::C1 {
                CREATE PROPERTY l1 -> std::str {
                    SET ANNOTATION description := 'test_delta_drop_02_link';
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Property {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_02_link';
            """,
            [
                {
                    'name': 'l1',
                }
            ],
        )

        await self.con.execute("""
            DROP TYPE test::C1;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Property {name}
                FILTER
                    .annotations.name = 'std::description'
                    AND .annotations@value = 'test_delta_drop_02_link';
            """,
            []
        )

    async def test_delta_drop_refuse_01(self):
        # Check that the schema refuses to drop objects with live references
        await self.con.execute("""
            CREATE TYPE test::DropA;
            CREATE ABSTRACT ANNOTATION test::dropattr;
            CREATE ABSTRACT LINK test::l1_parent;
            CREATE TYPE test::DropB {
                CREATE LINK l1 EXTENDING test::l1_parent -> test::DropA {
                    SET ANNOTATION test::dropattr := 'foo';
                };
            };
            CREATE SCALAR TYPE test::dropint EXTENDING int64;
            CREATE FUNCTION test::dropfunc(a: test::dropint) -> int64
                FROM EdgeQL $$ SELECT a; $$;
        """)

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop object type.*test::DropA.*other objects'):
            await self.con.execute('DROP TYPE test::DropA')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop abstract anno.*test::dropattr.*other objects'):
            await self.con.execute('DROP ABSTRACT ANNOTATION test::dropattr')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop abstract link.*test::l1_parent.*other objects'):
            await self.con.execute('DROP ABSTRACT LINK test::l1_parent')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaError,
                'cannot drop.*dropint.*other objects'):
            await self.con.execute('DROP SCALAR TYPE test::dropint')

    async def test_delta_unicode_01(self):
        await self.con.execute(r"""
            # setup delta
            CREATE MIGRATION test::u1 TO {
                type Пример {
                    required property номер -> int16;
                };
            };
            COMMIT MIGRATION test::u1;
            SET MODULE test;

            INSERT Пример {
                номер := 987
            };
            INSERT Пример {
                номер := 456
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    Пример {
                        номер
                    }
                ORDER BY
                    Пример.номер;
            """,
            [{'номер': 456}, {'номер': 987}]
        )


class TestDeltaLinkInheritance(tb.DDLTestCase):
    async def test_delta_link_inheritance(self):
        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1.esdl')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute(f'''
            CREATE MIGRATION test::d_links01_0 TO {{ {schema} }};
            COMMIT MIGRATION test::d_links01_0;
            ''')

        await self.con.execute('''
            INSERT test::Target1 {
                name := 'Target1_linkinh_2'
            };

            INSERT test::ObjectType01 {
                target := (SELECT test::Target1
                           FILTER test::Target1.name = 'Target1_linkinh_2'
                           LIMIT 1)
            };

            INSERT test::Target0 {
                name := 'Target0_linkinh_2'
            };

            INSERT test::ObjectType23 {
                target := (SELECT test::Target0
                           FILTER test::Target0.name = 'Target0_linkinh_2'
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
                INSERT test::ObjectType01 {
                    target := (
                        SELECT
                            test::Target0
                        FILTER
                            test::Target0.name = 'Target0_linkinh_2'
                        LIMIT 1
                    )
                };
            ''')

        schema_f = os.path.join(os.path.dirname(__file__), 'schemas',
                                'links_1_migrated.esdl')

        with open(schema_f) as f:
            schema = f.read()

        await self.con.execute(f'''
            ROLLBACK TO SAVEPOINT t0;
            CREATE MIGRATION test::d_links01_1 TO {{ {schema} }};
            COMMIT MIGRATION test::d_links01_1;
            ''')
