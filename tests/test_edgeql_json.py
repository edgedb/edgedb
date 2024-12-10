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


import json
import os.path

import edgedb

from edb.schema import defines as s_defs

from edb.testbase import server as tb
from edb.tools import test


class TestEdgeQLJSON(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'json.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'json_setup.edgeql')

    async def test_edgeql_json_cast_01(self):
        await self.assert_query_result(
            r'''SELECT to_json('"qwerty"');''',
            # JSON:
            ['qwerty'],
            # Binary:
            ['"qwerty"'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('1');''',
            # JSON:
            [1],
            # Binary:
            ['1'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('2.3e-2');''',
            # JSON:
            [0.023],
            # Binary:
            ['0.023'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('true');''',
            # JSON:
            [True],
            # Binary:
            ['true'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('false');''',
            # JSON:
            [False],
            # Binary:
            ['false'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('null');''',
            # JSON:
            [None],
            # Binary:
            ['null'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[2, "a", 3.456]');''',
            # JSON:
            [[2, 'a', 3.456]],
            # Binary:
            ['[2, "a", 3.456]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[2, "a", 3.456, [["b", 1]]]');''',
            # JSON:
            [[2, 'a', 3.456, [['b', 1]]]],
            # Binary:
            ['[2, "a", 3.456, [["b", 1]]]']
        )

    async def test_edgeql_json_cast_02(self):
        await self.assert_query_result(
            r'''SELECT <str>to_json('"qwerty"');''',
            ['qwerty'],
        )

        await self.assert_query_result(
            r'''SELECT <int64>to_json('1');''',
            [1],
        )

        await self.assert_query_result(
            r'''SELECT <float64>to_json('2.3e-2');''',
            [0.023],
        )

        await self.assert_query_result(
            r'''SELECT <bool>to_json('true');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <bool>to_json('false');''',
            [False],
        )

    async def test_edgeql_json_cast_03(self):
        await self.assert_query_result(
            r'''SELECT <str>to_json('null');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT <int64>to_json('null');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT <float64>to_json('null');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT <bool>to_json('null');''',
            [],
        )

    async def test_edgeql_json_cast_04(self):
        await self.assert_query_result(
            r'''SELECT <str>to_json('null') ?= <str>{};''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <int64>to_json('null') ?= <int64>{};''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <float64>to_json('null') ?= <float64>{};''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT <bool>to_json('null') ?= <bool>{};''',
            [True],
        )

    async def test_edgeql_json_cast_05(self):
        await self.assert_query_result(
            """
                SELECT <json>{} ?= (
                    SELECT x := to_json('1') FILTER x = to_json('2')
                );
            """,
            [True],
        )

        await self.assert_query_result(
            """SELECT <json>{} ?= to_json('null');""",
            [False],
        )

    @test.not_implemented('''
        # casting into tuples or other deeply nested structures
        # is not currently implemented
    ''')
    async def test_edgeql_json_cast_06(self):
        await self.assert_query_result(
            r'''SELECT <tuple<int64, str>><json>[1, 2];''',
            [[1, '2']],
        )

        await self.assert_query_result(
            r'''SELECT <array<int64>>to_json('[2, 3, 5]');''',
            [[2, 3, 5]],
        )

    async def test_edgeql_json_cast_07(self):
        """Check that JSON of array preserves order."""

        await self.assert_query_result(
            r'''
                SELECT <json>array_agg(
                    (SELECT JSONTest{number}
                     FILTER .number IN {0, 1}
                     ORDER BY .j_string)
                ) = to_json(
                    '[{"number": 1}, {"number": 0}]'
                )
            ''',
            [True]
        )

    async def test_edgeql_json_accessor_01(self):
        await self.assert_query_result(
            r'''SELECT (to_json('[1, "a", 3]'))[0] = to_json('1');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (to_json('[1, "a", 3]'))[1] = to_json('"a"');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (to_json('[1, "a", 3]'))[2] = to_json('3');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (to_json('[1, "a", 3]'))[<int16>0] = to_json('1');''',
            [True],
        )

        await self.assert_query_result(
            r'''SELECT (to_json('[1, "a", 3]'))[<int32>0] = to_json('1');''',
            [True],
        )

    async def test_edgeql_json_accessor_02(self):
        await self.assert_query_result(
            r'''
                SELECT (to_json('{"a": 1, "b": null}'))["a"] =
                    to_json('1');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
            SELECT (to_json('{"a": 1, "b": null}'))["b"] =
                to_json('null');
            ''',
            [True],
        )

    async def test_edgeql_json_accessor_03(self):
        await self.assert_query_result(
            """
                SELECT (<str>(to_json('["qwerty"]'))[0])[1];
            """,
            ['w'],
        )

    async def test_edgeql_json_accessor_04(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index 10 is out of bounds'):
            await self.con.query(r"""
                SELECT (to_json('[1, "a", 3]'))[10];
            """)

    async def test_edgeql_json_accessor_05(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index -10 is out of bounds'):
            await self.con.query(r"""
                SELECT (to_json('[1, "a", 3]'))[-10];
            """)

    async def test_edgeql_json_accessor_06(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON array by text'):
            await self.con.query(r"""
                SELECT (to_json('[1, "a", 3]'))['1'];
            """)

    async def test_edgeql_json_accessor_07(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"JSON index 'c' is out of bounds"):
            await self.con.query(r"""
                SELECT (to_json('{"a": 1, "b": null}'))["c"];
            """)

    async def test_edgeql_json_accessor_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON object by bigint'):
            await self.con.execute(r"""
                SELECT (to_json('{"a": 1, "b": null}'))[0];
            """)

    async def test_edgeql_json_accessor_09(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON null'):
            await self.con.query(r"""
                SELECT (to_json('null'))[0];
            """)

    async def test_edgeql_json_accessor_10(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON boolean'):
            await self.con.execute(r"""
                SELECT (to_json('true'))[0];
            """)

    async def test_edgeql_json_accessor_11(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON number'):
            await self.con.execute(r"""
                SELECT (to_json('123'))[0];
            """)

    async def test_edgeql_json_accessor_13(self):

        await self.assert_query_result(
            r"""
                WITH JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][2]['bingo'];
            """,
            ['42!'],
            ['"42!"'],
        )

        await self.assert_query_result(
            r"""
                WITH JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][-1]['bingo'];
            """,
            ['42!'],
            ['"42!"'],
        )

    async def test_edgeql_json_accessor_14(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index 10 is out of bounds'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][10]['bingo'];
            """)

    async def test_edgeql_json_accessor_15(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index -10 is out of bounds'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[-10]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_16(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON array by text'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data['4']['b']['bar'][10]['bingo'];
            """)

    async def test_edgeql_json_accessor_17(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"JSON index 'c' is out of bounds"):
            await self.con.execute(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['c'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_18(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON object by bigint'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4][1]['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_19(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON null'):
            await self.con.execute(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][0]['bingo'];
            """)

    async def test_edgeql_json_accessor_20(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON boolean'):
            await self.con.execute(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[-1]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_21(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON number'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[0]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_22(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'cannot index JSON string'):
            await self.con.execute(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[2]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_23(self):
        await self.assert_query_result(
            r'''select to_json('"hello"')[0] = <json>'h';''',
            [True],
        )

        await self.assert_query_result(
            r'''select to_json('"hello"')[-2] = <json>'l';''',
            [True],
        )

    async def test_edgeql_json_accessor_24(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index 10 is out of bounds'):
            await self.con.query(r"""
                select to_json('"hello"')[10];
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index -10 is out of bounds'):
            await self.con.query(r"""
                select to_json('"hello"')[-10];
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index 10 is out of bounds'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['c'][10];
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'JSON index -10 is out of bounds'):
            await self.con.query(r"""
                WITH
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['c'][-10];
            """)

    async def test_edgeql_json_null_01(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    SELECT JSONTest FILTER .number = 0
                ).data ?= <json>{};
            ''',
            {False},
        )

        await self.assert_query_result(
            r'''
                SELECT (
                    SELECT JSONTest FILTER .number = 0
                ).data ?= to_json('null');
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                SELECT (
                    SELECT JSONTest FILTER .number = 2
                ).data ?= <json>{};
            ''',
            {True},
        )

        await self.assert_query_result(
            r'''
                SELECT (
                    SELECT JSONTest FILTER .number = 2
                ).data ?= to_json('null');
            ''',
            {False},
        )

    async def test_edgeql_json_typeof_01(self):
        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('2'));''',
            ['number'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('"foo"'));''',
            ['string'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('true'));''',
            ['boolean'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('false'));''',
            ['boolean'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('null'));''',
            ['null'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('[]'));''',
            ['array'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('[2]'));''',
            ['array'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('{}'));''',
            ['object'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(to_json('{"a": 2}'));''',
            ['object'],
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(<json>{});''',
            [],
        )

    async def test_edgeql_json_typeof_02(self):
        await self.assert_query_result(
            r'''SELECT json_typeof(JSONTest.j_string);''',
            ['string', 'string', 'string'],
            sort=True,
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(JSONTest.j_number);''',
            ['number', 'number', 'number'],
            sort=True,
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(JSONTest.j_boolean);''',
            ['boolean', 'boolean', 'boolean'],
            sort=True,
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(JSONTest.j_array);''',
            ['array', 'array', 'array'],
            sort=True,
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(JSONTest.j_object);''',
            ['object', 'object'],
            sort=True,
        )

        await self.assert_query_result(
            r'''SELECT json_typeof(JSONTest.data);''',
            ['array', 'null', 'object'],
            sort=True,
        )

    async def test_edgeql_json_array_unpack_01(self):
        await self.assert_query_result(
            """
                SELECT json_array_unpack(to_json('[1, "a", null]'));
            """,
            # JSON:
            [1, 'a', None],
            # Binary:
            ['1', '"a"', 'null'],
        )

    async def test_edgeql_json_array_unpack_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"operator 'IN' cannot.*'std::json' and 'std::int64'"):
            await self.con.query(r'''
                SELECT json_array_unpack(to_json('[2,3,4]')) IN
                    {2, 3, 4};
            ''')

    async def test_edgeql_json_array_unpack_03(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"operator 'IN' cannot.*'std::json' and 'std::str'"):
            await self.con.query_json(r'''
                SELECT json_array_unpack(to_json('[2,3,4]')) IN
                    {'2', '3', '4'};
            ''')

    async def test_edgeql_json_array_unpack_04(self):
        await self.assert_query_result(
            r'''
                SELECT json_array_unpack(to_json('[2,3,4]')) IN
                    <json>{2, 3, 4};
            ''',
            [True, True, True],
        )

        await self.assert_query_result(
            r'''
                SELECT json_array_unpack(to_json('[2,3,4]')) NOT IN
                    <json>{2, 3, 4};
            ''',
            [False, False, False],
        )

    async def test_edgeql_json_array_unpack_05(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"operator '=' cannot.*'std::json' and 'std::int64'"):
            await self.con.query_json(r'''
                WITH
                    JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_array_unpack(JT0.j_array) = 1;
            ''')

    async def test_edgeql_json_array_unpack_06(self):
        await self.assert_query_result(
            r'''
                WITH
                    JT0 := (SELECT JSONTest FILTER .number = 0)
                # unpacking [1, 1, 1]
                SELECT json_array_unpack(JT0.j_array) = to_json('1');
            ''',
            [True, True, True],
        )

    async def test_edgeql_json_array_unpack_07(self):
        await self.assert_query_result(
            r'''
                WITH
                    JT0 := (SELECT JSONTest FILTER .number = 2)
                # unpacking [2, "q", [3], {}, null], should preserve the
                # order
                SELECT array_agg(json_array_unpack(JT0.j_array)) =
                    <array<json>>JT0.j_array;
            ''',
            [True],
        )

    async def test_edgeql_json_array_unpack_08(self):
        await self.assert_query_result(
            r'''
                WITH
                    JT0 := (SELECT JSONTest FILTER .number = 2)
                # unpacking [2, "q", [3], {}, null], should preserve the
                # order
                SELECT json_typeof(json_array_unpack(JT0.j_array));
            ''',
            ['number', 'string', 'array', 'object', 'null'],
        )

    async def test_edgeql_json_object_01(self):
        await self.assert_query_result(
            r'''
                SELECT to_json('{"a":1,"b":2}') =
                    to_json('{"b":2,"a":1}');
            ''',
            [True],
        )

        await self.assert_query_result(
            r'''
                SELECT to_json('{"a":1,"b":2}') =
                    to_json('{"b":3,"a":1,"b":2}');
            ''',
            [True],
        )

    async def test_edgeql_json_object_unpack_01(self):
        await self.assert_query_result(
            r'''
                SELECT json_object_unpack(to_json('{
                    "q": 1,
                    "w": [2, null, 3],
                    "e": null
                }'));
            ''',
            # JSON:
            [
                # Nones in the output are legitimate JSON nulls
                ['e', None],
                ['q', 1],
                ['w', [2, None, 3]],
            ],
            # Binary:
            [
                ['e', 'null'],
                ['q', '1'],
                ['w', '[2, null, 3]'],
            ],
            sort=lambda x: x[0],
        )

    async def test_edgeql_json_object_unpack_02(self):
        await self.assert_query_result(
            r'''
                SELECT
                    _ := json_object_unpack(JSONTest.j_object)
                ORDER BY _.0 THEN _.1;
            ''',
            # JSON:
            [['a', 1], ['b', 1], ['b', 2], ['c', 2]],
            # Binary:
            [['a', '1'], ['b', '1'], ['b', '2'], ['c', '2']],
        )

        await self.assert_query_result(
            r'''
                SELECT json_object_unpack(JSONTest.j_object) =
                    ('c', to_json('1'));
            ''',
            [False, False, False, False],
        )

        await self.assert_query_result(
            r'''
                SELECT json_object_unpack(JSONTest.j_object).0 IN
                    {'a', 'b', 'c'};
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                SELECT json_object_unpack(JSONTest.j_object).1 IN
                    <json>{1, 2};
            ''',
            [True, True, True, True],
        )

        await self.assert_query_result(
            r'''
                SELECT json_object_unpack(JSONTest.j_object).1 IN
                    <json>{'1', '2'};
            ''',
            [False, False, False, False],
        )

    async def test_edgeql_json_object_unpack_03(self):
        async with self.assertRaisesRegexTx(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InvalidValueError,
                r'cannot call jsonb_each on a non-object'):
            await self.con.query_json(r'''
                WITH
                    JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT
                    count(json_object_unpack(JT0.data));
            ''')

    async def test_edgeql_json_object_unpack_04(self):
        await self.assert_query_result(
            r'''
                WITH
                    JT1 := (SELECT JSONTest FILTER .number = 1),
                    JT2 := (SELECT JSONTest FILTER .number = 2)
                SELECT (
                    count(json_object_unpack(JT1.data)),
                    count(json_object_unpack(JT2.data)),
                );
            ''',
            [[0, 0]],
        )

    async def test_edgeql_json_object_unpack_05(self):
        await self.assert_query_result(
            r'''
                WITH
                    q := enumerate(
                        json_object_unpack(to_json('{
                            "q": 1,
                            "w": 2
                        }'))
                    )
                SELECT
                    <int64>q.1.1
                ORDER BY
                    q.1.0;
            ''',
            [1, 2]
        )

    async def test_edgeql_json_object_pack(self):
        await self.assert_query_result(
            r'''
                select std::json_object_pack({
                    ("foo", to_json("1")),
                    ("bar", to_json("null")),
                    ("baz", to_json("[]"))
                })
            ''',
            [{'bar': None, 'baz': [], 'foo': 1}],
            ['{"bar": null, "baz": [], "foo": 1}'],
        )

        await self.assert_query_result(
            r"""
                select std::json_object_pack(
                    <tuple<str, json>>{}
                )
            """,
            [{}],
            ["{}"],
        )

        await self.assert_query_result(
            r"""
                select std::json_object_pack(
                    array_unpack([
                        ('foo', <json>1),
                        ('bar', <json>2),
                        ('baz', <json>3)
                    ])
                )
            """,
            [{"bar": 2, "baz": 3, "foo": 1}],
            ['{"bar": 2, "baz": 3, "foo": 1}'],
        )

    async def test_edgeql_json_get_01(self):
        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '2');
            ''',
            # JSON
            {'Fraka'},
            # Binary
            {'"Fraka"'}
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '-1');
            ''',
            # JSON
            {True},
            # Binary
            {"true"}
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '100');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, 'foo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '0', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '1', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '2', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '3', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '4', 'b', 'bar', '2', 'bingo');
            ''',
            # JSON
            {'42!'},
            # Binary
            {'"42!"'}
        )

        await self.assert_query_result(
            r'''
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '4', 'b', 'foo', '2', 'bingo');
            ''',
            []
        )

    async def test_edgeql_json_get_02(self):
        # since only one JSONTest has a non-trivial `data`, we
        # don't need to filter to get the same results as above

        await self.assert_query_result(
            r'''SELECT json_get(JSONTest.data, '2');''',
            {'Fraka'},
            {'"Fraka"'},
        )

        await self.assert_query_result(
            r'''SELECT json_get(JSONTest.data, '-1');''',
            {True},
            {'true'},
        )

        await self.assert_query_result(
            r'''SELECT json_get(JSONTest.data, '100');''',
            [],
        )

        await self.assert_query_result(
            r'''SELECT json_get(JSONTest.data, 'foo');''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '0', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '1', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '2', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '3', 'b', 'bar', '2', 'bingo');
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '4', 'b', 'bar', '2', 'bingo');
            ''',
            {'42!'},
            {'"42!"'},
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '4', 'b', 'foo', '2', 'bingo');
            ''',
            []
        )

    async def test_edgeql_json_get_03(self):
        # chaining json_get should get the same effect as a single call

        await self.assert_query_result(
            r'''
                SELECT json_get(JSONTest.data, '4', 'b', 'bar', '2', 'bingo');
            ''',
            {'42!'},
            {'"42!"'},
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(json_get(json_get(json_get(json_get(
                    JSONTest.data, '4'), 'b'), 'bar'), '2'), 'bingo');
            ''',
            {'42!'},
            {'"42!"'},
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(json_get(
                    JSONTest.data, '4', 'b'), 'bar', '2', 'bingo');
            ''',
            {'42!'},
            {'"42!"'},
        )

    async def test_edgeql_json_get_04(self):
        await self.assert_query_result(
            r'''SELECT json_get(JSONTest.data, 'bogus') ?? <json>'oups';''',
            # JSON
            ['oups'],
            # Binary
            ['"oups"'],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(
                    JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                    default := <json>'hello'
                ) ?? <json>'oups';
            ''',
            ['hello', 'hello', 'hello'],
            ['"hello"', '"hello"', '"hello"'],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(
                    JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                    default := <json>''
                ) ?? <json>'oups';
            ''',
            ['', '', ''],
            ['""', '""', '""'],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(
                    JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                    default := to_json('null')
                ) ?? <json>'oups';
            ''',
            [None, None, None],
            ['null', 'null', 'null'],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(
                    JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                    default := <json>{}
                ) ?? <json>'oups';
            ''',
            ['oups'],
            ['"oups"'],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(
                    JSONTest.data, '4', 'b', 'bar', '2', 'bingo',
                    default := <json>''
                ) ?? <json>'oups';
            ''',
            ['', '', '42!'],
            ['""', '""', '"42!"'],
        )

        await self.assert_query_result(
            r'''
                SELECT json_get(
                    JSONTest.data, '4', 'b', 'bar', '2', 'bingo'
                ) ?? <json>'oups';
            ''',
            ['42!'],
            ['"42!"']
        )

    async def test_edgeql_json_set_01(self):
        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(JT0.j_object, 'a', value := <json>42);
            ''',
            [{"a": 42, "b": 2}],
            ['{"a": 42, "b": 2}'],
        )

        # by default create_if_missing should create new value
        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(JT0.j_object, 'c', value := <json>42);
            ''',
            [{"a": 1, "b": 2, "c": 42}],
            ['{"a": 1, "b": 2, "c": 42}'],
        )

        # create_if_missing should also work inside of nested objects
        # by variadic path
        await self.assert_query_result(
            r'''
                WITH j_object := to_json('{"a": {"b": {}}}')
                SELECT json_set(j_object, 'a', 'b', 'c', value := <json>42);
            ''',
            [{"a": {"b": {"c": 42}}}],
            ['{"a": {"b": {"c": 42}}}'],
        )

        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(
                    JT0.j_object,
                    'c',
                    value := <json>42,
                    create_if_missing := false,
                );
            ''',
            [{"a": 1, "b": 2}],
            ['{"a": 1, "b": 2}'],
        )

        # by default empty_treatment should return empty set
        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(JT0.j_object, 'a', value := <json>{});
            ''',
            [],
            [],
        )

        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(
                    JT0.j_object,
                    'a',
                    value := <json>{},
                    empty_treatment := JsonEmpty.ReturnEmpty,
                );
            ''',
            [],
            [],
        )

        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(
                    JT0.j_object,
                    'a',
                    value := <json>{},
                    empty_treatment := JsonEmpty.ReturnTarget,
                );
            ''',
            [{"a": 1, "b": 2}],
            ['{"a": 1, "b": 2}'],
        )

        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(
                    JT0.j_object,
                    'a',
                    value := <json>{},
                    empty_treatment := JsonEmpty.UseNull,
                );
            ''',
            [{"a": None, "b": 2}],
            ['{"a": null, "b": 2}'],
        )

        await self.assert_query_result(
            r'''
                WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_set(
                    JT0.j_object,
                    'a',
                    value := <json>{},
                    empty_treatment := JsonEmpty.DeleteKey,
                );
            ''',
            [{"b": 2}],
            ['{"b": 2}'],
        )

        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r"invalid empty JSON value"):
            await self.con.execute(
                r'''
                    WITH JT0 := (SELECT JSONTest FILTER .number = 0)
                    SELECT json_set(
                        JT0.j_object,
                        'a',
                        value := <json>{},
                        empty_treatment := JsonEmpty.Error,
                    );
                ''',
            )

    async def test_edgeql_json_cast_object_to_json_01(self):
        res = await self.con.query("""
            WITH MODULE schema
            SELECT
                to_str(<json>(
                    SELECT Object {
                        name,
                        foo := 'bar',
                    }
                    FILTER Object.name = 'std::json'
                ));
        """)

        val = res[0]
        self.assertIsInstance(val, str)

        self.assertEqual(
            json.loads(val),
            {"foo": "bar", "name": "std::json"}
        )

    async def test_edgeql_json_cast_object_to_json_02(self):
        # Test that object-to-json cast works in non-SELECT clause.
        await self.assert_query_result(
            """
                WITH MODULE schema
                SELECT
                    ScalarType {
                        name
                    }
                FILTER
                    to_str(<json>(ScalarType {name})) LIKE '%std::json%';
            """,
            [{
                'name': 'std::json',
            }]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_json_cast_object_to_json_03(self):
        # Test that object-to-json cast works in tuples as well.
        await self.assert_query_result(
            """
                WITH MODULE schema
                SELECT
                    True
                FILTER
                    to_str(
                        (
                            <tuple<json>>(
                                (
                                    SELECT Object {
                                        name,
                                        foo := 'bar',
                                    }
                                    FILTER Object.name = 'std::json'
                                ),
                            )
                        ).0
                    )
                    LIKE '%std%';
            """,
            [True],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_json_cast_object_to_json_04(self):
        # Test that object-to-json cast works in arrays as well.
        await self.assert_query_result(
            """
                WITH MODULE schema
                SELECT
                    True
                FILTER
                    to_str(<json>[(
                        SELECT Object {
                            name,
                            foo := 'bar',
                        }
                        FILTER Object.name = 'std::json'
                    )])
                    LIKE '%std%';
            """,
            [True],
        )

    async def test_edgeql_json_cast_object_to_json_05(self):
        await self.assert_query_result(
            r"""
                # base case
                SELECT
                    JSONTest {number, edb_string};
            """,
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
                {'number': 2, 'edb_string': None},
                {'number': 3, 'edb_string': None},
            ],
            sort=lambda x: x['number'],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    JSONTest {number, edb_string}
                FILTER
                    # casting all the way to the original type with a
                    # strict `=` will discard objects with empty `edb_string`
                    .edb_string =
                        <str>(<json>(JSONTest{edb_string}))['edb_string'];
            """,
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
            ],
            sort=lambda x: x['number'],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    JSONTest {number, edb_string}
                FILTER
                    # strict `=` will discard objects with empty `edb_string`
                    <json>.edb_string =
                        (<json>(JSONTest{edb_string}))['edb_string'];
            """,
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
            ],
            sort=lambda x: x['number'],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    JSONTest {number, edb_string}
                FILTER
                    # casting all the way to the original type with a
                    # weak `?=` will not discard anything
                    .edb_string ?=
                        <str>(<json>(JSONTest{edb_string}))['edb_string'];
            """,
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
                {'number': 2, 'edb_string': None},
                {'number': 3, 'edb_string': None},
            ],
            sort=lambda x: x['number'],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    JSONTest {number, edb_string}
                FILTER
                    # casting both sides into json combined with a
                    # weak `?=` will discard objects with empty `edb_string`
                    <json>.edb_string ?=
                        (<json>(JSONTest{edb_string}))['edb_string'];
            """,
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
            ],
            sort=lambda x: x['number'],
        )

    async def test_edgeql_json_cast_object_to_json_06(self):
        res = await self.con.query("""
            SELECT to_str(<json>{x := random()});
        """)

        val = res[0]
        self.assertIsInstance(val, str)
        data = json.loads(val)

        self.assertTrue('id' not in data)

    async def test_edgeql_json_cast_object_to_json_07(self):
        await self.con.execute('''
            create function _get(idx: int64) -> set of json {
                using (
                    select <json> (
                        select JSONTest {
                            number, edb_string
                        } filter .number = idx
                    )
                )
            };
        ''')
        await self.assert_query_result(
            r"""
                SELECT _get(0)
            """,
            [
                {'number': 0, 'edb_string': 'jumps'},
            ],
            json_only=True,
        )

    async def test_edgeql_json_cast_tuple_to_json_01(self):
        res = await self.con.query("""
            WITH MODULE schema
            SELECT
                to_str(<json>(
                    1,
                    (SELECT Object {
                            name,
                            foo := 'bar',
                        }
                        FILTER Object.name = 'std::json'),
                ));
        """)

        val = res[0]
        self.assertIsInstance(val, str)

        self.assertEqual(
            json.loads(val),
            [1, {"foo": "bar", "name": "std::json"}]
        )

    async def test_edgeql_json_cast_tuple_to_json_02(self):
        res = await self.con.query("""
            SELECT
                to_str(<json>(
                    foo := 1,
                    bar := [1, 2, 3]
                ));
        """)

        val = res[0]
        self.assertIsInstance(val, str)

        self.assertEqual(
            json.loads(val),
            {"foo": 1, "bar": [1, 2, 3]}
        )

    async def test_edgeql_json_slice_01(self):
        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[:1];''',
            [[1]],
            ['[1]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[:-1];''',
            [[1, 'a', 3]],
            ['[1, "a", 3]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[1:-1];''',
            [['a', 3]],
            ['["a", 3]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[-1:1];''',
            [[]],
            ['[]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[-100:100];''',
            [[1, 'a', 3, None]],
            ['[1, "a", 3, null]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[100:-100];''',
            [[]],
            ['[]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[100:];''',
            [[]],
            ['[]'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('[1, "a", 3, null]')[:-100];''',
            [[]],
            ['[]'],
        )

    async def test_edgeql_json_slice_02(self):

        await self.assert_query_result(
            r'''
            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[:1];
            ''',
            [[2]],
            ['[2]'],
        )

        await self.assert_query_result(
            r'''
            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[:-1];
            ''',
            [[2, "q", [3], {}]],
            ['[2, "q", [3], {}]'],
        )

        await self.assert_query_result(
            r'''
            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[1:-1];
            ''',
            [["q", [3], {}]],
            ['["q", [3], {}]'],
        )

        await self.assert_query_result(
            r'''
            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[-1:1];
            ''',
            [[]],
            ['[]'],
        )

        await self.assert_query_result(
            r'''
            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[-100:100];
            ''',
            [[2, "q", [3], {}, None]],
            ['[2, "q", [3], {}, null]'],
        )

        await self.assert_query_result(
            r'''
            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[100:-100];
            ''',
            [[]],
            ['[]'],
        )

    async def test_edgeql_json_slice_03(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError, r'cannot slice JSON array by.*str'):

            await self.con.execute(r"""
                SELECT to_json('[1, "a", 3, null]')[:'1'];
            """)

    async def test_edgeql_json_slice_04(self):
        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[:1];''',
            ['h'],
            ['"h"'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[:-1];''',
            ['hell'],
            ['"hell"'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[1:-1];''',
            ['ell'],
            ['"ell"'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[-1:1];''',
            [''],
            ['""'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[-100:100];''',
            ['hello'],
            ['"hello"'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[100:-100];''',
            [''],
            ['""'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[:-100];''',
            [''],
            ['""'],
        )

        await self.assert_query_result(
            r'''SELECT to_json('"hello"')[100:];''',
            [''],
            ['""'],
        )

    async def test_edgeql_json_slice_05(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError, r'cannot slice JSON number'):

            await self.con.execute(r"""
                select to_json('123')[0:1];
            """)

    async def test_edgeql_json_slice_06(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError, r'cannot slice JSON object'):

            await self.con.execute(r"""
                select to_json('{"a":123}')[0:1];
            """)

    async def test_edgeql_json_slice_07(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError, r'cannot slice JSON boolean'):

            await self.con.execute(r"""
                select to_json('true')[0:1];
            """)

    async def test_edgeql_json_slice_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError, r'cannot slice JSON null'):

            await self.con.execute(r"""
                select to_json('null')[0:1];
            """)

    async def test_edgeql_json_bytes_cast_01(self):
        await self.assert_query_result(
            r"""SELECT <json>b'foo';""",
            ['Zm9v'],
            ['"Zm9v"'],
        )

        await self.assert_query_result(
            r"""SELECT <json>(foo := b'hello', bar := [b'world']);""",
            [{'bar': ['d29ybGQ='], 'foo': 'aGVsbG8='}],
            ['{"bar": ["d29ybGQ="], "foo": "aGVsbG8="}'],
        )

        await self.assert_query_result(
            r"""SELECT <json>{ x := b'hello' };""",
            [{'x': 'aGVsbG8='}],
            ['{"x": "aGVsbG8="}'],
        )

        await self.assert_query_result(
            r"""SELECT <json>[b'foo'];""",
            [['Zm9v']],
            ['["Zm9v"]'],
        )

        await self.assert_query_result(
            r"""SELECT <json>(b'foo',)""",
            [['Zm9v']],
            ['["Zm9v"]'],
        )

        await self.assert_query_result(
            r"""SELECT <json>[(b'foo',)][0]""",
            [['Zm9v']],
            ['["Zm9v"]'],
        )

        await self.assert_query_result(
            r"""SELECT <json>(a := b'foo')""",
            [{"a": "Zm9v"}],
            ['{"a": "Zm9v"}'],
        )

        await self.assert_query_result(
            r"""SELECT <json>[(a := b'foo')][0]""",
            [{"a": "Zm9v"}],
            ['{"a": "Zm9v"}'],
        )

    async def test_edgeql_json_bytes_output_01(self):
        await self.assert_query_result(
            r"""SELECT b'foo';""",
            ['Zm9v'],
            [b'foo'],
        )

        await self.assert_query_result(
            r"""SELECT { x := b'hello' };""",
            [{'x': 'aGVsbG8='}],
            [{'x': b'hello'}],
        )

        await self.assert_query_result(
            r"""SELECT (b'foo',)""",
            [['Zm9v']],
            [[b'foo']],
        )

        await self.assert_query_result(
            r"""SELECT [(b'foo',)][0]""",
            [['Zm9v']],
            [[b'foo']],
        )

        await self.assert_query_result(
            r"""SELECT (a := b'foo')""",
            [{"a": "Zm9v"}],
            [{"a": b'foo'}],
        )

        await self.assert_query_result(
            r"""SELECT [(a := b'foo')][0]""",
            [{"a": "Zm9v"}],
            [{"a": b'foo'}],
        )

        await self.assert_query_result(
            r"""SELECT [b'foo'];""",
            [['Zm9v']],
            [[b'foo']],
        )

        await self.assert_query_result(
            r"""SELECT (foo := b'hello', bar := [b'world']);""",
            [{'bar': ['d29ybGQ='], 'foo': 'aGVsbG8='}],
            [{'bar': [b'world'], 'foo': b'hello'}],
        )

    async def test_edgeql_json_bytes_output_02(self):
        await self.con.execute(r'''
            CREATE SCALAR TYPE bytes2 EXTENDING bytes;
        ''')

        await self.assert_query_result(
            r"""SELECT [<bytes2>b'foo'];""",
            [['Zm9v']],
            [[b'foo']],
        )

    async def test_edgeql_json_alias_01(self):
        await self.assert_query_result(
            r'''
            SELECT _ := json_get(JSONTest.j_array, '0')
            ORDER BY _;
            ''',
            # JSON
            [1, 2],
            # Binary
            ['1', '2'],
        )

        await self.assert_query_result(
            r'''
            SELECT _ := json_get(JSONTest.j_array, '10')
            ORDER BY _;
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            SELECT _ := json_get(JSONTest.j_array, {'-1', '4'})
            ORDER BY _;
            ''',
            # JSON
            [None, None, 1],
            # Binary
            ['null', 'null', '1'],
        )

        await self.assert_query_result(
            r'''
            SELECT _ := json_get(
                JSONTest.data,
                # Each of the variadic "steps" is a set, so we should
                # get a cross-product of all the possibilities. It so
                # happens that the only 2 valid paths are:
                # - '4', 'b', 'bar', '1' -> null
                # - '4', 'b', 'bar', '2' -> {"bingo": "42!"}
                {'2', '4', '4'}, {'0', 'b'}, {'bar', 'foo'}, {'1', '2'}
            )
            ORDER BY _;
            ''',
            # JSON
            [None, None, {'bingo': '42!'}, {'bingo': '42!'}],
            # Binary
            ['null', 'null', '{"bingo": "42!"}', '{"bingo": "42!"}']
        )

    async def test_edgeql_json_alias_02(self):
        await self.assert_query_result(
            r'''
            SELECT _ := json_get(
                JSONTest.data,
                # Each of the variadic "steps" is a set, so we should
                # get a cross-product of all the possibilities. It so
                # happens that the only 2 valid paths are:
                # - '4', 'b', 'bar', '1' -> null
                # - '4', 'b', 'bar', '2' -> {"bingo": "42!"}
                {'2', '4', '4'}, {'0', 'b'}, {'bar', 'foo'}, {'1', '2'},
                default := <json>'N/A'
            )
            ORDER BY _;
            ''',

            # JSON:
            # We expect 3 * 2 * 2 * 2 = 24 results per JSONTest.data.
            # There are 3 non-empty data properties. Out of them all
            # only 4 will have a valid path. So we expect 24 * 3 - 4 =
            # 68 default 'N/A' results, 2 JSON `nulls` represented as
            # `None` and 2 JSON objects.
            [None, None] + ['N/A'] * 68 + [{'bingo': '42!'}, {'bingo': '42!'}],

            # Binary:
            ['null'] * 2 + ['"N/A"'] * 68 + ['{"bingo": "42!"}'] * 2,
        )

    async def test_edgeql_json_alias_03(self):
        await self.assert_query_result(
            r'''
            WITH
                JT := JSONTest
            SELECT JSONTest {
                a0 := (
                    SELECT _ := json_get(JT.j_array, '0')
                    ORDER BY _
                ),
                a1 := (
                    SELECT _ := json_get(JT.j_array, '10')
                    ORDER BY _
                ),
                a2 := (
                    SELECT _ := json_get(JT.j_array, {'-1', '4'})
                    ORDER BY _
                ),
                a3 := (
                    SELECT _ := json_get(
                        JT.data,
                        # Each of the variadic "steps" is a set, so we should
                        # get a cross-product of all the possibilities. It so
                        # happens that the only 2 valid paths are:
                        # - '4', 'b', 'bar', '1' -> null
                        # - '4', 'b', 'bar', '2' -> {"bingo": "42!"}
                        {'2', '4', '4'}, {'0', 'b'}, {'bar', 'foo'}, {'1', '2'}
                    )
                    ORDER BY _
                )
            }
            FILTER .number = 0;
            ''',
            # JSON
            [{
                'a0': [1, 2],
                'a1': [],
                # Nones in the output are legitimate JSON nulls
                'a2': [None, None, 1],
                'a3': [None, None, {'bingo': '42!'}, {'bingo': '42!'}],
            }],
            # Binary
            [{
                'a0': ['1', '2'],
                'a1': [],
                # Nones in the output are legitimate JSON nulls
                'a2': ['null', 'null', '1'],
                'a3': ['null', 'null', '{"bingo": "42!"}', '{"bingo": "42!"}'],
            }],
        )

    async def test_edgeql_json_alias_04(self):
        await self.assert_query_result(
            r'''
                SELECT _ := json_get(
                    JSONTest.data,
                    '0', '1', '2',
                    default := <json>{'N/A', 'nope', '-'}
                )
                ORDER BY _;
            ''',
            # JSON:
            # None of the 3 data objects have the path 0, 1, 2, so we
            # expect default values in the result in triplicate.
            ['-', '-', '-', 'N/A', 'N/A', 'N/A', 'nope', 'nope', 'nope'],
            # Binary:
            ['"-"', '"-"', '"-"', '"N/A"', '"N/A"', '"N/A"',
             '"nope"', '"nope"', '"nope"'],
        )

    async def test_edgeql_json_alias_05(self):
        await self.assert_query_result(
            r'''
                WITH
                    JT := JSONTest
                SELECT JSONTest {
                    a4 := (
                        SELECT _ := json_get(
                            JT.data,
                            '0', '1', '2',
                            default := <json>{'N/A', 'nope', '-'}
                        )
                        ORDER BY _
                    ),
                }
                FILTER .number = 0;
            ''',
            # JSON
            [{
                'a4': [
                    '-', '-', '-',
                    'N/A', 'N/A', 'N/A',
                    'nope', 'nope', 'nope'
                ],
            }],
            # Binary
            [{
                'a4': [
                    '"-"', '"-"', '"-"',
                    '"N/A"', '"N/A"', '"N/A"',
                    '"nope"', '"nope"', '"nope"'
                ],
            }],
        )

    async def test_edgeql_json_str_function_01(self):
        await self.assert_query_result(
            r'''SELECT to_str(<json>[1, 2, 3, 4]);''',
            {'[1, 2, 3, 4]'},
        )

        await self.assert_query_result(
            r'''SELECT to_str(<json>[1, 2, 3, 4], 'pretty');''',
            {'[\n    1,\n    2,\n    3,\n    4\n]'},
        )

    async def test_edgeql_json_str_function_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"format 'foo' is invalid"):
            async with self.con.transaction():
                await self.con.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'foo');
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r'"fmt" argument must be a non-empty string'):
            async with self.con.transaction():
                await self.con.query_json(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], '');
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"format 'PRETTY' is invalid"):
            async with self.con.transaction():
                await self.con.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'PRETTY');
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"format 'Pretty' is invalid"):
            async with self.con.transaction():
                await self.con.query_json(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'Pretty');
                ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"format 'p' is invalid"):
            async with self.con.transaction():
                await self.con.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'p');
                ''')

    async def test_edgeql_json_long_array(self):
        count = s_defs.MAX_FUNC_ARG_COUNT + 11
        await self.assert_query_result(
            f'''SELECT to_str(<json>[
                {", ".join(str(v) for v in range(count))}
            ]);''',
            {f'[{", ".join(str(v) for v in range(count))}]'},
        )

    async def test_edgeql_json_long_tuple(self):
        count = s_defs.MAX_FUNC_ARG_COUNT + 11
        await self.assert_query_result(
            f'''SELECT to_str(<json>(
                {", ".join(str(v) for v in range(count))}
            ));''',
            {f'[{", ".join(str(v) for v in range(count))}]'},
        )

    async def test_edgeql_json_long_named_tuple(self):
        count = s_defs.MAX_FUNC_ARG_COUNT + 11
        args = (f'_{i} := {v}' for i, v in enumerate(range(count)))
        result = (f'"_{i}": {v}' for i, v in enumerate(range(count)))
        await self.assert_query_result(
            f'SELECT to_str(<json>({", ".join(args)}));',
            {f'{{{", ".join(result)}}}'},
        )

    async def test_edgeql_json_long_shape(self):
        count = s_defs.MAX_FUNC_ARG_COUNT + 11
        args = (f'_{i} := {v}' for i, v in enumerate(range(count)))
        result = (f'"_{i}": {v}' for i, v in enumerate(range(count)))
        await self.assert_query_result(
            f'''
                SELECT to_str(<json>JSONTest{{ {", ".join(args)} }})
                LIMIT 1
            ''',
            {f'{{{", ".join(result)}}}'},
        )

    async def test_edgeql_json_concatenate_01(self):
        await self.assert_query_result(
            r'''SELECT to_str(to_json('[1, 2]') ++ to_json('[3]'));''',
            {'[1, 2, 3]'}
        )

        await self.assert_query_result(
            r'''SELECT to_str(to_json('{"a": 1}') ++ to_json('{"b": 2}'));''',
            {'{"a": 1, "b": 2}'}
        )

        await self.assert_query_result(
            r'''SELECT to_str(to_json('{"a": 1}') ++ to_json('{"a": 2}'));''',
            {'{"a": 2}'}
        )

        await self.assert_query_result(
            r'''SELECT to_str(to_json('"123"') ++ to_json('"456"'));''',
            {'"123456"'}
        )

    async def test_edgeql_json_concatenate_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"invalid JSON values for \+\+ operator"):
            await self.con.query_json(
                r'''SELECT to_str(to_json('"123"') ++ to_json('42'));'''
            )
