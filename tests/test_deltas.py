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


import difflib
import os.path
import textwrap

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
                    SET ATTRIBUTE description :=
                        'test_delta_drop_01_constraint';
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Constraint {name}
                FILTER
                    .attributes.name = 'std::description'
                    AND .attributes@value = 'test_delta_drop_01_constraint';
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
                    .attributes.name = 'std::description'
                    AND .attributes@value = 'test_delta_drop_01_constraint';
            """,
            []
        )

    async def test_delta_drop_02(self):
        # Check that links defined on types being dropped are
        # dropped.
        await self.con.execute("""
            CREATE TYPE test::C1 {
                CREATE PROPERTY l1 -> std::str {
                    SET ATTRIBUTE description := 'test_delta_drop_02_link';
                };
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE schema
                SELECT Property {name}
                FILTER
                    .attributes.name = 'std::description'
                    AND .attributes@value = 'test_delta_drop_02_link';
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
                    .attributes.name = 'std::description'
                    AND .attributes@value = 'test_delta_drop_02_link';
            """,
            []
        )

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
                r"invalid target for link '\(test::ObjectType01\)\.target': "
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


class TestDeltaDDLGeneration(tb.DDLTestCase):
    def _assert_result(self, result, expected):
        result = result.strip()
        expected = textwrap.dedent(expected).strip()

        if result != expected:
            diff = '\n'.join(difflib.context_diff(
                expected.split('\n'), result.split('\n')))

            self.fail(
                f'DDL does not match the expected result.'
                f'\nEXPECTED:\n{expected}\nACTUAL:\n{result}'
                f'\nDIFF:\n{diff}')

    async def test_delta_ddlgen_01(self):
        await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d1 TO {
                abstract link related {
                    property lang -> str;
                };

                type NamedObject {
                    required property name -> str;
                    required link related extending related -> NamedObject {
                        inherited property lang -> str {
                            attribute title := 'Language';
                        };
                    };
                };
            };
        """)
        result = await self.con.fetchone("""
            GET MIGRATION test::d1;
        """)

        self._assert_result(
            result,
            '''\
CREATE ABSTRACT LINK test::related EXTENDING std::link;
ALTER ABSTRACT LINK test::related \
CREATE SINGLE PROPERTY lang EXTENDING std::property -> std::str;
CREATE TYPE test::NamedObject EXTENDING std::Object;
ALTER TYPE test::NamedObject {
    CREATE REQUIRED SINGLE PROPERTY name EXTENDING std::property -> std::str;
    CREATE REQUIRED SINGLE LINK related EXTENDING test::related \
-> test::NamedObject {
        SET on_target_delete := 'RESTRICT';
    };
    ALTER LINK related {
        CREATE SINGLE PROPERTY source EXTENDING std::source \
-> test::NamedObject;
        CREATE SINGLE PROPERTY target EXTENDING std::target \
-> test::NamedObject;
        CREATE SINGLE PROPERTY lang EXTENDING std::property -> std::str;
        ALTER PROPERTY lang {
            SET ATTRIBUTE std::title := 'Language';
        };
    };
};
            '''
        )

    async def test_delta_ddlgen_02(self):
        await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d2 TO {
                type NamedObject {
                    required property a -> array<int64>;
                };
            };
        """)
        result = await self.con.fetchone("""
            GET MIGRATION test::d2;
        """)

        self._assert_result(
            result,
            '''\
CREATE TYPE test::NamedObject EXTENDING std::Object;
ALTER TYPE test::NamedObject CREATE REQUIRED SINGLE PROPERTY \
a EXTENDING std::property -> array<std::int64>;
            '''
        )

    async def test_delta_ddlgen_03(self):
        await self.con.execute("""
            # setup delta
            CREATE MIGRATION test::d3 TO {
                abstract type Foo {
                    property bar -> str;
                    property __typename := 'foo';
                };
            };
        """)
        result = await self.con.fetchone("""
            GET MIGRATION test::d3;
        """)

        self._assert_result(
            result,
            '''\
            CREATE ABSTRACT TYPE test::Foo EXTENDING std::Object;
            ALTER TYPE test::Foo {
                CREATE SINGLE PROPERTY bar EXTENDING std::property -> std::str;
                CREATE SINGLE PROPERTY __typename EXTENDING std::property \
-> std::str {
                    SET computable := true;
                    SET default := SELECT
                        'foo'
                    ;
                };
            };
            '''
        )

    async def test_delta_ddlgen_04(self):
        await self.con.execute("""
            # setup delta
            CREATE MIGRATION test::d4 TO {
                abstract type Foo {
                    property bar -> str;
                    property __typename := __source__.__type__.name;
                };
            };
        """)
        result = await self.con.fetchone("""
            GET MIGRATION test::d4;
        """)

        self._assert_result(
            result,
            '''\
            CREATE ABSTRACT TYPE test::Foo EXTENDING std::Object;
            ALTER TYPE test::Foo {
                CREATE SINGLE PROPERTY bar EXTENDING std::property -> std::str;
                CREATE SINGLE PROPERTY __typename EXTENDING std::property \
-> std::str {
                    SET computable := true;
                    SET default := SELECT
                        __source__.__type__.name
                    ;
                };
            };
            '''
        )

    async def test_delta_ddlgen_05(self):
        await self.con.execute("""
            # setup delta
            #
            CREATE MIGRATION test::d5 TO {
                type NamedObject2 {
                    property a2 -> array<int64> {
                        readonly := true;
                    };
                };
            };

            GET MIGRATION test::d5;
        """)
        result = await self.con.fetchone("""
            GET MIGRATION test::d5;
        """)

        self._assert_result(
            result,
            '''\
CREATE TYPE test::NamedObject2 EXTENDING std::Object;
ALTER TYPE test::NamedObject2 CREATE SINGLE PROPERTY \
a2 EXTENDING std::property -> array<std::int64> {
    SET readonly := true;
};
            '''
        )

    async def test_delta_ddlgen_06(self):
        with self.assertRaisesRegex(
                edgedb.SchemaError, r"unexpected field aaa"):

            await self.con.execute("""
                CREATE MIGRATION test::d5 TO {
                    type NamedObject2 {
                        property a2 -> array<int64> {
                            aaa := true;
                        };
                    };
                };
            """)
