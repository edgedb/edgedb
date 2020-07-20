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


class TestEdgeQLEnuma(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'enums.esdl')

    async def test_edgeql_enums_cast_01(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT <color_enum_t>{'RED', 'GREEN', 'BLUE'};
            ''',
            {'RED', 'GREEN', 'BLUE'},
        )

    async def test_edgeql_enums_cast_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'invalid input value for enum .+color_enum_t.+YELLOW'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT <color_enum_t>'YELLOW';
            ''')

    async def test_edgeql_enums_cast_03(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'invalid input value for enum .+color_enum_t.+red'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT <color_enum_t>'red';
            ''')

    async def test_edgeql_enums_cast_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\+\+' cannot be applied to operands of type "
                r"'std::str' and 'test::color_enum_t'"):
            await self.con.execute(r'''
                WITH MODULE test
                INSERT Foo {
                    color := 'BLUE'
                };

                WITH MODULE test
                SELECT 'The test color is: ' ++ Foo.color;
            ''')

    async def test_edgeql_enums_cast_05(self):
        await self.con.execute(
            r'''
                WITH MODULE test
                INSERT Foo {
                    color := 'BLUE'
                };
            ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT 'The test color is: ' ++ <str>Foo.color;
            ''',
            ['The test color is: BLUE'],
        )

    async def test_edgeql_enums_assignment_01(self):
        # testing the INSERT assignment cast
        await self.con.execute(
            r'''
                WITH MODULE test
                INSERT Foo {
                    color := 'RED'
                };
            ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
                INSERT Foo {
                    color := 'RED'
                };
            ''')

        # testing the UPDATE assignment cast
        await self.con.execute(
            r'''
                WITH MODULE test
                UPDATE Foo
                SET {
                    color := 'GREEN'
                };
            ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
                INSERT Bar;
            ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                WITH MODULE test
                INSERT Bar;
            ''')

        # testing the UPDATE assignment cast
        await self.con.execute(
            r'''
                WITH MODULE test
                UPDATE Bar
                SET {
                    color := 'GREEN'
                };
            ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
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
                "SELECT <json><test::color_enum_t>'RED'"
            ),
            ['"RED"'])

        await self.assert_query_result(
            "SELECT <test::color_enum_t><json>'RED'",
            ['RED'])

        await self.assert_query_result(
            "SELECT <test::color_enum_t>'RED'",
            ['RED'])

    async def test_edgeql_enums_json_cast_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'invalid input value for enum .+color_enum_t.+: "BANANA"'):
            await self.con.execute("SELECT <test::color_enum_t><json>'BANANA'")

    async def test_edgeql_enums_json_cast_03(self):
        with self.assertRaisesRegex(
                # FIXME: This should be a different error
                edgedb.InternalServerError,
                r'expected json string, null; got json number'):
            await self.con.execute("SELECT <test::color_enum_t><json>12")
