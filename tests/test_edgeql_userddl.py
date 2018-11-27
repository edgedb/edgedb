#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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


import edgedb

from edb.server import _testbase as tb


class TestEdgeQLUserDDL(tb.DDLTestCase):
    INTERNAL_TESTMODE = False

    async def test_edgeql_userddl_01(self):
        # testing anytype polymorphism
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::func_01.*'
                r'generic types are not supported in '
                r'user-defined functions'):
            await self.query('''
                CREATE FUNCTION test::func_01(
                    a: anytype
                ) -> bool
                    FROM EdgeQL $$
                        SELECT a IS float32
                    $$;
            ''')

    async def test_edgeql_userddl_02(self):
        # testing anyreal polymorphism, which is an actual abstract
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::func_02.*'
                r'generic types are not supported in '
                r'user-defined functions'):
            await self.query('''
                CREATE FUNCTION test::func_02(
                    a: anyreal
                ) -> bool
                    FROM EdgeQL $$
                        SELECT a IS float32
                    $$;
            ''')

    async def test_edgeql_userddl_03(self):
        # testing anytype as return type
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::func_03.*'
                r'generic types are not supported in '
                r'user-defined functions'):
            await self.query('''
                CREATE FUNCTION test::func_03(
                    a: str
                ) -> anytype
                    FROM EdgeQL $$
                        SELECT a
                    $$;
            ''')

    async def test_edgeql_userddl_04(self):
        # testing anyreal as return type
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::func_04.*'
                r'generic types are not supported in '
                r'user-defined functions'):
            await self.query('''
                CREATE FUNCTION test::func_04(
                    a: str
                ) -> anyscalar
                    FROM EdgeQL $$
                        SELECT a
                    $$;
            ''')

    async def test_edgeql_userddl_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::func_05.*'
                r'FROM SQL FUNCTION.*not supported in '
                r'user-defined functions'):
            await self.query('''
                CREATE FUNCTION test::func_05(
                    a: str
                ) -> str
                    FROM SQL FUNCTION 'lower';
            ''')

    async def test_edgeql_userddl_06(self):
        with self.assertRaisesRegex(
                edgedb.InvalidFunctionDefinitionError,
                r'cannot create.*test::func_06.*'
                r'FROM SQL.*not supported in '
                r'user-defined functions'):
            await self.query('''
                CREATE FUNCTION test::func_06(
                    a: str
                ) -> str
                    FROM SQL $$ SELECT "a" $$;
            ''')

    async def test_edgeql_userddl_07(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'user-defined operators are not supported'):
            await self.query('''
                CREATE INFIX OPERATOR
                std::`+` (l: std::str, r: std::str) -> std::str
                    FROM SQL OPERATOR r'||';
            ''')

    async def test_edgeql_userddl_08(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'user-defined casts are not supported'):
            await self.query('''
                CREATE CAST FROM std::int64 TO std::timedelta {
                    FROM SQL CAST;
                    ALLOW ASSIGNMENT;
                };
            ''')

    async def test_edgeql_userddl_09(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot create.*module std is read-only'):
            await self.query('''
                CREATE FUNCTION std::func_09(
                    a: str
                ) -> str
                    FROM EdgeQL $$
                        SELECT a
                    $$;
            ''')

    async def test_edgeql_userddl_10(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot create.*module math is read-only'):
            await self.query('''
                CREATE FUNCTION math::func_10(
                    a: str
                ) -> str
                    FROM EdgeQL $$
                        SELECT a
                    $$;
            ''')

    async def test_edgeql_userddl_11(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot create.*module std is read-only'):
            await self.query('''
                CREATE TYPE std::Foo_11;
            ''')

    async def test_edgeql_userddl_12(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot create.*module math is read-only'):
            await self.query('''
                CREATE TYPE math::Foo_11;
            ''')

    async def test_edgeql_userddl_13(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot delete.*module std is read-only'):
            await self.query('''
                DROP TYPE std::Object;
            ''')

    async def test_edgeql_userddl_14(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot delete.*module stdgraphql is read-only'):
            await self.query('''
                DROP TYPE stdgraphql::Query;
            ''')

    async def test_edgeql_userddl_15(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot alter.*module std is read-only'):
            await self.query('''
                ALTER TYPE std::Object {
                    CREATE PROPERTY std::foo_15 -> std::str;
                };
            ''')

    async def test_edgeql_userddl_16(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot alter.*module stdgraphql is read-only'):
            await self.query('''
                ALTER TYPE stdgraphql::Query {
                    CREATE PROPERTY std::foo_15 -> std::str;
                };
            ''')

    async def test_edgeql_userddl_17(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot delete.*module std is read-only'):
            await self.query('''
                DROP MODULE std;
            ''')

    async def test_edgeql_userddl_18(self):
        with self.assertRaisesRegex(
                edgedb.SchemaDefinitionError,
                r'cannot delete.*module math is read-only'):
            await self.query('''
                DROP MODULE math;
            ''')

    async def test_edgeql_userddl_19(self):
        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                r'cannot create.*test::func_19.*'
                r'SET OF parameters in user-defined EdgeQL '
                r'functions are not supported'):
            await self.query('''
                CREATE FUNCTION test::func_19(
                    a: SET OF str
                ) -> bool
                    FROM EdgeQL $$
                        SELECT EXISTS a
                    $$;
            ''')

    async def test_edgeql_userddl_20(self):
        await self.query('''
            CREATE FUNCTION test::func_20(
                a: str
            ) -> SET OF str
                FROM EdgeQL $$
                    SELECT {a, 'a'}
                $$;
        ''')

        await self.assert_query_result(r'''
            SELECT test::func_20('q');
            SELECT count(test::func_20({'q', 'w'}));
        ''', [
            {'q', 'a'},
            {4},
        ])
