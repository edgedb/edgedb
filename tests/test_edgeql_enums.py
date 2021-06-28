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


import os.path

import edgedb

from edb.testbase import server as tb


class TestEdgeQLEnums(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'enums.esdl')

    async def test_edgeql_enums_cast_01(self):
        await self.assert_query_result(
            r'''
                SELECT <color_enum_t>{'RED', 'GREEN', 'BLUE'};
            ''',
            {'RED', 'GREEN', 'BLUE'},
        )

    async def test_edgeql_enums_cast_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'invalid input value for enum .+color_enum_t.+YELLOW'):
            await self.con.execute(r'''
                SELECT <color_enum_t>'YELLOW';
            ''')

    async def test_edgeql_enums_cast_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'invalid input value for enum .+color_enum_t.+red'):
            await self.con.execute(r'''
                SELECT <color_enum_t>'red';
            ''')

    async def test_edgeql_enums_cast_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\+\+' cannot be applied to operands of type "
                r"'std::str' and 'default::color_enum_t'"):
            await self.con.execute(r'''
                INSERT Foo {
                    color := 'BLUE'
                };

                SELECT 'The test color is: ' ++ Foo.color;
            ''')

    async def test_edgeql_enums_cast_05(self):
        await self.con.execute(
            r'''
                INSERT Foo {
                    color := 'BLUE'
                };
            ''')

        await self.assert_query_result(
            r'''
                SELECT 'The test color is: ' ++ <str>Foo.color;
            ''',
            ['The test color is: BLUE'],
        )

    async def test_edgeql_enums_pathsyntax_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "enum path expression lacks an enum member name"):
            async with self._run_and_rollback():
                await self.con.execute('SELECT color_enum_t')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "enum path expression lacks an enum member name"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'WITH e := color_enum_t SELECT e.RED'
                )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "unexpected reference to link property 'RED'"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'SELECT color_enum_t@RED'
                )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "enum types do not support backlink"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'SELECT color_enum_t.<RED'
                )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "an enum member name must follow enum type name in the path"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'SELECT color_enum_t[IS color_enum_t].RED'
                )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "invalid property reference on a primitive type expression"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'SELECT color_enum_t.RED.GREEN'
                )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "invalid property reference on a primitive type expression"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'WITH x := color_enum_t.RED SELECT x.GREEN'
                )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                "enum has no member called 'RAD'",
                _hint="did you mean 'RED'?"):
            async with self._run_and_rollback():
                await self.con.execute(
                    'SELECT color_enum_t.RAD'
                )

    async def test_edgeql_enums_pathsyntax_02(self):
        await self.assert_query_result(
            r'''
                SELECT color_enum_t.GREEN;
            ''',
            {'GREEN'},
        )

        await self.assert_query_result(
            r'''
                SELECT default::color_enum_t.BLUE;
            ''',
            {'BLUE'},
        )

        await self.assert_query_result(
            r'''
                WITH x := default::color_enum_t.RED SELECT x;
            ''',
            {'RED'},
        )

    async def test_edgeql_enums_assignment_01(self):
        # testing the INSERT assignment cast
        await self.con.execute(
            r'''
                INSERT Foo {
                    color := 'RED'
                };
            ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    color
                };
            ''',
            [{
                'color': 'RED',
            }],
        )

    async def test_edgeql_enums_assignment_02(self):
        await self.con.execute(
            r'''
                INSERT Foo {
                    color := 'RED'
                };
            ''')

        # testing the UPDATE assignment cast
        await self.con.execute(
            r'''
                UPDATE Foo
                SET {
                    color := 'GREEN'
                };
            ''')

        await self.assert_query_result(
            r'''
                SELECT Foo {
                    color
                };
            ''',
            [{
                'color': 'GREEN',
            }],
        )

    async def test_edgeql_enums_assignment_03(self):
        # testing the INSERT assignment cast
        await self.con.execute(
            r'''
                INSERT Bar;
            ''')

        await self.assert_query_result(
            r'''
                SELECT Bar {
                    color
                };
            ''',
            [{
                'color': 'RED',
            }],
        )

    async def test_edgeql_enums_assignment_04(self):
        await self.con.execute(
            r'''
                INSERT Bar;
            ''')

        # testing the UPDATE assignment cast
        await self.con.execute(
            r'''
                UPDATE Bar
                SET {
                    color := 'GREEN'
                };
            ''')

        await self.assert_query_result(
            r'''
                SELECT Bar {
                    color
                };
            ''',
            [{
                'color': 'GREEN',
            }],
        )

    async def test_edgeql_enums_json_cast_01(self):
        self.assertEqual(
            await self.con.query(
                "SELECT <json><color_enum_t>'RED'"
            ),
            ['"RED"'])

        await self.assert_query_result(
            "SELECT <color_enum_t><json>'RED'",
            ['RED'])

        await self.assert_query_result(
            "SELECT <color_enum_t>'RED'",
            ['RED'])

    async def test_edgeql_enums_json_cast_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'invalid input value for enum .+color_enum_t.+: "BANANA"'):
            await self.con.execute("SELECT <color_enum_t><json>'BANANA'")

    async def test_edgeql_enums_json_cast_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'expected json string or null; got json number'):
            await self.con.execute("SELECT <color_enum_t><json>12")
