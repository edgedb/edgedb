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
import unittest  # NOQA

import edgedb

from edb.server import _testbase as tb


class TestEdgeQLJSON(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'json.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'json_setup.eql')

    ISOLATED_METHODS = False

    async def test_edgeql_json_cast_01(self):
        await self.assert_query_result("""
            SELECT to_json('"qwerty"');
            SELECT to_json('1');
            SELECT to_json('2.3e-2');

            SELECT to_json('true');
            SELECT to_json('false');
            SELECT to_json('null');

            SELECT to_json('[2, "a", 3.456]');
            SELECT to_json('[2, "a", 3.456, [["b", 1]]]');

            SELECT to_json('{
                "a": 1,
                "b": 2.87,
                "c": [2, "a", 3.456],
                "d": {
                    "d1": 1,
                    "d2": {
                        "d3": true
                    }
                },
                "e": null,
                "f": false
            }');
        """, [
            ['qwerty'],
            [1],
            [0.023],

            [True],
            [False],
            [None],  # JSON null

            [[2, 'a', 3.456]],
            [[2, 'a', 3.456, [['b', 1]]]],

            [{
                'a': 1,
                'b': 2.87,
                'c': [2, 'a', 3.456],
                'd': {
                    'd1': 1,
                    'd2': {
                        'd3': True
                    }
                },
                'e': None,  # JSON null
                'f': False
            }],
        ])

    async def test_edgeql_json_cast_02(self):
        await self.assert_query_result("""
            SELECT <str>to_json('"qwerty"');
            SELECT <int64>to_json('1');
            SELECT <float64>to_json('2.3e-2');

            SELECT <bool>to_json('true');
            SELECT <bool>to_json('false');
        """, [
            ['qwerty'],
            [1],
            [0.023],

            [True],
            [False],

        ])

    async def test_edgeql_json_cast_03(self):
        await self.assert_query_result("""
            SELECT <str>to_json('null');
            SELECT <int64>to_json('null');
            SELECT <float64>to_json('null');
            SELECT <bool>to_json('null');
        """, [
            [],
            [],
            [],
            [],
        ])

    async def test_edgeql_json_cast_04(self):
        await self.assert_query_result("""
            SELECT <str>to_json('null') ?= <str>{};
            SELECT <int64>to_json('null') ?= <int64>{};
            SELECT <float64>to_json('null') ?= <float64>{};
            SELECT <bool>to_json('null') ?= <bool>{};
        """, [
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_json_cast_05(self):
        await self.assert_query_result("""
            SELECT <json>{} ?= (
                SELECT x := to_json('1') FILTER x = to_json('2')
            );
            SELECT <json>{} ?= to_json('null');
        """, [
            [True],
            [False],
        ])

    @unittest.expectedFailure
    async def test_edgeql_json_cast_06(self):
        # XXX: casting into tuples or other deeply nested structures
        # is not currently implemented
        await self.assert_query_result(r"""
            SELECT <tuple<int64, str>><json>[1, 2];
            SELECT <array<int64>>to_json('[2, 3, 5]');
        """, [
            [[1, '2']],
            [[2, 3, 5]],
        ])

    async def test_edgeql_json_accessor_01(self):
        await self.assert_query_result("""
            SELECT (to_json('[1, "a", 3]'))[0] = to_json('1');
            SELECT (to_json('[1, "a", 3]'))[1] = to_json('"a"');
            SELECT (to_json('[1, "a", 3]'))[2] = to_json('3');

            SELECT (to_json('[1, "a", 3]'))[<int16>0] = to_json('1');
            SELECT (to_json('[1, "a", 3]'))[<int32>0] = to_json('1');
        """, [
            [True],
            [True],
            [True],

            [True],
            [True],
        ])

    async def test_edgeql_json_accessor_02(self):
        await self.assert_query_result("""
            SELECT (to_json('{"a": 1, "b": null}'))["a"] =
                to_json('1');

            SELECT (to_json('{"a": 1, "b": null}'))["b"] =
                to_json('null');
        """, [
            [True],
            [True],
        ])

    async def test_edgeql_json_accessor_03(self):
        await self.assert_query_result("""
            SELECT (<str>(to_json('["qwerty"]'))[0])[1];
        """, [
            ['w'],
        ])

    async def test_edgeql_json_accessor_04(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                edgedb.InternalServerError,
                r'json index 10 is out of bounds'):
            await self.query(r"""
                SELECT (to_json('[1, "a", 3]'))[10];
            """)

    async def test_edgeql_json_accessor_05(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                edgedb.InternalServerError,
                r'json index -10 is out of bounds'):
            await self.query(r"""
                SELECT (to_json('[1, "a", 3]'))[-10];
            """)

    async def test_edgeql_json_accessor_06(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json array by text'):
            await self.query(r"""
                SELECT (to_json('[1, "a", 3]'))['1'];
            """)

    async def test_edgeql_json_accessor_07(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                edgedb.InternalServerError,
                r"json index 'c' is out of bounds"):
            await self.query(r"""
                SELECT (to_json('{"a": 1, "b": null}'))["c"];
            """)

    async def test_edgeql_json_accessor_08(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json object by integer'):
            await self.query(r"""
                SELECT (to_json('{"a": 1, "b": null}'))[0];
            """)

    async def test_edgeql_json_accessor_09(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json null'):
            await self.query(r"""
                SELECT (to_json('null'))[0];
            """)

    async def test_edgeql_json_accessor_10(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json boolean'):
            await self.query(r"""
                SELECT (to_json('true'))[0];
            """)

    async def test_edgeql_json_accessor_11(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json number'):
            await self.query(r"""
                SELECT (to_json('123'))[0];
            """)

    async def test_edgeql_json_accessor_12(self):
        with self.assertRaisesRegex(
                # FIXME: currently JSON strings cannot be indexed or sliced
                edgedb.InternalServerError,
                r'cannot index json string'):
            await self.query(r"""
                SELECT (to_json('"qwerty"'))[0];
            """)

    async def test_edgeql_json_accessor_13(self):
        await self.assert_query_result(r"""
            SET MODULE test;

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT JT3.data[4]['b']['bar'][2]['bingo'];
            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT JT3.data[4]['b']['bar'][-1]['bingo'];
        """, [
            None,
            ['42!'],
            ['42!'],
        ])

    async def test_edgeql_json_accessor_14(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                edgedb.InternalServerError,
                r'json index 10 is out of bounds'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][10]['bingo'];
            """)

    async def test_edgeql_json_accessor_15(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                edgedb.InternalServerError,
                r'json index -10 is out of bounds'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[-10]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_16(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json array by text'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data['4']['b']['bar'][10]['bingo'];
            """)

    async def test_edgeql_json_accessor_17(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                edgedb.InternalServerError,
                r"json index 'c' is out of bounds"):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['c'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_18(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json object by integer'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4][1]['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_19(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json null'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][0]['bingo'];
            """)

    async def test_edgeql_json_accessor_20(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json boolean'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[-1]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_21(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot index json number'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[0]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_22(self):
        with self.assertRaisesRegex(
                # FIXME: currently JSON strings cannot be indexed or sliced
                edgedb.InternalServerError,
                r'cannot index json string'):
            await self.query(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[2]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_null_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (
                SELECT JSONTest FILTER .number = 0
            ).data ?= <json>{};

            WITH MODULE test
            SELECT (
                SELECT JSONTest FILTER .number = 0
            ).data ?= to_json('null');

            WITH MODULE test
            SELECT (
                SELECT JSONTest FILTER .number = 2
            ).data ?= <json>{};

            WITH MODULE test
            SELECT (
                SELECT JSONTest FILTER .number = 2
            ).data ?= to_json('null');
        ''', [
            {False},
            {True},
            {True},
            {False},
        ])

    async def test_edgeql_json_typeof_01(self):
        await self.assert_query_result(r"""
            SELECT json_typeof(to_json('2'));
            SELECT json_typeof(to_json('"foo"'));
            SELECT json_typeof(to_json('true'));
            SELECT json_typeof(to_json('false'));
            SELECT json_typeof(to_json('null'));
            SELECT json_typeof(to_json('[]'));
            SELECT json_typeof(to_json('[2]'));
            SELECT json_typeof(to_json('{}'));
            SELECT json_typeof(to_json('{"a": 2}'));
            SELECT json_typeof(<json>{});
        """, [
            ['number'],
            ['string'],
            ['boolean'],
            ['boolean'],
            ['null'],
            ['array'],
            ['array'],
            ['object'],
            ['object'],
            [],
        ])

    async def test_edgeql_json_typeof_02(self):
        await self.assert_sorted_query_result(r'''
            SET MODULE test;
            SELECT json_typeof(JSONTest.j_string);
            SELECT json_typeof(JSONTest.j_number);
            SELECT json_typeof(JSONTest.j_boolean);
            SELECT json_typeof(JSONTest.j_array);
            SELECT json_typeof(JSONTest.j_object);
            SELECT json_typeof(JSONTest.data);
        ''', lambda x: x, [
            None,
            ['string', 'string', 'string'],
            ['number', 'number', 'number'],
            ['boolean', 'boolean', 'boolean'],
            ['array', 'array', 'array'],
            ['object', 'object'],
            ['array', 'null', 'object'],
        ])

    async def test_edgeql_json_array_unpack_01(self):
        await self.assert_query_result("""
            SELECT json_array_unpack(to_json('[1, "a", null]'));
        """, [
            [1, 'a', None],  # None is legitimate JSON null
        ])

    async def test_edgeql_json_array_unpack_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator 'IN' cannot.*'std::json' and 'std::int64'"):
            await self.query(r'''
                SELECT json_array_unpack(to_json('[2,3,4]')) IN
                    {2, 3, 4};
            ''')

    async def test_edgeql_json_array_unpack_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator 'IN' cannot.*'std::json' and 'std::str'"):
            await self.query(r'''
                SELECT json_array_unpack(to_json('[2,3,4]')) IN
                    {'2', '3', '4'};
            ''')

    async def test_edgeql_json_array_unpack_04(self):
        await self.assert_query_result(r'''
            SELECT json_array_unpack(to_json('[2,3,4]')) IN
                <json>{2, 3, 4};
            SELECT json_array_unpack(to_json('[2,3,4]')) NOT IN
                <json>{2, 3, 4};
        ''', [
            [True, True, True],
            [False, False, False],
        ])

    async def test_edgeql_json_array_unpack_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '=' cannot.*'std::json' and 'std::int64'"):
            await self.query(r'''
                WITH
                    MODULE test,
                    JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT json_array_unpack(JT0.j_array) = 1;
            ''')

    async def test_edgeql_json_array_unpack_06(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                JT0 := (SELECT JSONTest FILTER .number = 0)
            # unpacking [1, 1, 1]
            SELECT json_array_unpack(JT0.j_array) = to_json('1');
        ''', [
            [True, True, True],
        ])

    async def test_edgeql_json_array_unpack_07(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                JT0 := (SELECT JSONTest FILTER .number = 2)
            # unpacking [2, "q", [3], {}, null], should preserve the
            # order
            SELECT array_agg(json_array_unpack(JT0.j_array)) =
                <array<json>>JT0.j_array;
        ''', [
            [True],
        ])

    async def test_edgeql_json_array_unpack_08(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                JT0 := (SELECT JSONTest FILTER .number = 2)
            # unpacking [2, "q", [3], {}, null], should preserve the
            # order
            SELECT json_typeof(json_array_unpack(JT0.j_array));
        ''', [
            ['number', 'string', 'array', 'object', 'null'],
        ])

    async def test_edgeql_json_object_unpack_01(self):
        await self.assert_sorted_query_result(r'''
            SELECT json_object_unpack(to_json('{
                "q": 1,
                "w": [2, null, 3],
                "e": null
            }'));
        ''', lambda x: x[0], [[
            # Nones in the output are legitimate JSON nulls
            ['e', None],
            ['q', 1],
            ['w', [2, None, 3]],
        ]])

    async def test_edgeql_json_object_unpack_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT
                _ := json_object_unpack(JSONTest.j_object)
            ORDER BY _.0 THEN _.1;

            WITH MODULE test
            SELECT json_object_unpack(JSONTest.j_object) =
                ('c', to_json('1'));

            WITH MODULE test
            SELECT json_object_unpack(JSONTest.j_object).0 IN {'a', 'b', 'c'};

            WITH MODULE test
            SELECT json_object_unpack(JSONTest.j_object).1 IN <json>{1, 2};

            WITH MODULE test
            SELECT json_object_unpack(JSONTest.j_object).1 IN <json>{'1', '2'};
        ''', [
            [['a', 1], ['b', 1], ['b', 2], ['c', 2]],
            [False, False, False, False],
            [True, True, True, True],
            [True, True, True, True],
            [False, False, False, False],
        ])

    async def test_edgeql_json_object_unpack_03(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                edgedb.InternalServerError,
                r'cannot call jsonb_each on a non-object'):
            await self.query(r'''
                WITH
                    MODULE test,
                    JT0 := (SELECT JSONTest FILTER .number = 0)
                SELECT
                    count(json_object_unpack(JT0.data));
            ''')

    async def test_edgeql_json_object_unpack_04(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                JT1 := (SELECT JSONTest FILTER .number = 1),
                JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT (
                count(json_object_unpack(JT1.data)),
                count(json_object_unpack(JT2.data)),
            );
        ''', [
            [[0, 0]],
        ])

    async def test_edgeql_json_get_01(self):
        await self.assert_query_result(r'''
            SET MODULE test;

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '2');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '-1');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '100');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, 'foo');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '0', 'b', 'bar', '2', 'bingo');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '1', 'b', 'bar', '2', 'bingo');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '2', 'b', 'bar', '2', 'bingo');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '3', 'b', 'bar', '2', 'bingo');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '4', 'b', 'bar', '2', 'bingo');

            WITH JT3 := (SELECT JSONTest FILTER .number = 3)
            SELECT json_get(JT3.data, '4', 'b', 'foo', '2', 'bingo');
        ''', [
            None,
            {'Fraka'},
            {True},
            {},
            {},
            {},
            {},
            {},
            {},
            {'42!'},
            {},
        ])

    async def test_edgeql_json_get_02(self):
        # since only one JSONTest has a non-trivial `data`, we
        # don't need to filter to get the same results as above
        await self.assert_query_result(r'''
            SET MODULE test;

            SELECT json_get(JSONTest.data, '2');
            SELECT json_get(JSONTest.data, '-1');
            SELECT json_get(JSONTest.data, '100');
            SELECT json_get(JSONTest.data, 'foo');

            SELECT json_get(JSONTest.data, '0', 'b', 'bar', '2', 'bingo');
            SELECT json_get(JSONTest.data, '1', 'b', 'bar', '2', 'bingo');
            SELECT json_get(JSONTest.data, '2', 'b', 'bar', '2', 'bingo');
            SELECT json_get(JSONTest.data, '3', 'b', 'bar', '2', 'bingo');
            SELECT json_get(JSONTest.data, '4', 'b', 'bar', '2', 'bingo');
            SELECT json_get(JSONTest.data, '4', 'b', 'foo', '2', 'bingo');
        ''', [
            None,
            {'Fraka'},
            {True},
            {},
            {},
            {},
            {},
            {},
            {},
            {'42!'},
            {},
        ])

    async def test_edgeql_json_get_03(self):
        # chaining json_get should get the same effect as a single call
        await self.assert_query_result(r'''
            SET MODULE test;

            SELECT json_get(JSONTest.data, '4', 'b', 'bar', '2', 'bingo');
            SELECT json_get(json_get(json_get(json_get(json_get(
                JSONTest.data, '4'), 'b'), 'bar'), '2'), 'bingo');
            SELECT json_get(json_get(
                JSONTest.data, '4', 'b'), 'bar', '2', 'bingo');
        ''', [
            None,
            {'42!'},
            {'42!'},
            {'42!'},
        ])

    async def test_edgeql_json_get_04(self):
        await self.assert_query_result(r'''
            SET MODULE test;

            SELECT json_get(JSONTest.data, 'bogus') ?? <json>'oups';

            SELECT json_get(
                JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                default := <json>'hello'
            ) ?? <json>'oups';

            SELECT json_get(
                JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                default := <json>''
            ) ?? <json>'oups';

            SELECT json_get(
                JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                default := to_json('null')
            ) ?? <json>'oups';

            SELECT json_get(
                JSONTest.data, '4', 'b', 'bar', '2', 'bogus',
                default := <json>{}
            ) ?? <json>'oups';

            SELECT json_get(
                JSONTest.data, '4', 'b', 'bar', '2', 'bingo',
                default := <json>''
            ) ?? <json>'oups';

            SELECT json_get(
                JSONTest.data, '4', 'b', 'bar', '2', 'bingo'
            ) ?? <json>'oups';
        ''', [
            None,  # SET MODULE
            ['oups'],
            ['hello', 'hello', 'hello'],
            ['', '', ''],
            # Nones are legitimate JSON nulls used as defaults
            [None, None, None],
            ['oups'],
            ['', '', '42!'],
            ['42!']
        ])

    async def test_edgeql_json_cast_object_to_json_01(self):
        res = await self.query("""
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

        val = res[0][0]
        self.assertIsInstance(val, str)

        self.assertEqual(
            json.loads(val),
            {"foo": "bar", "name": "std::json"}
        )

    async def test_edgeql_json_cast_object_to_json_02(self):
        # Test that object-to-json cast works in non-SELECT clause.
        await self.assert_query_result("""
            WITH MODULE schema
            SELECT
                ScalarType {
                    name
                }
            FILTER
                to_str(<json>(ScalarType {name})) LIKE '%std::json%';
        """, [
            [{
                'name': 'std::json',
            }]
        ])

    async def test_edgeql_json_cast_object_to_json_03(self):
        # Test that object-to-json cast works in tuples as well.
        await self.assert_query_result("""
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
        """, [
            [True],
        ])

    async def test_edgeql_json_cast_object_to_json_04(self):
        # Test that object-to-json cast works in arrays as well.
        await self.assert_query_result("""
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
        """, [
            [True],
        ])

    async def test_edgeql_json_cast_object_to_json_05(self):
        await self.assert_sorted_query_result(r"""
            # base case
            WITH MODULE test
            SELECT
                JSONTest {number, edb_string};

            WITH MODULE test
            SELECT
                JSONTest {number, edb_string}
            FILTER
                # casting all the way to the original type with a
                # strict `=` will discard objects with empty `edb_string`
                .edb_string =
                    <str>(<json>(JSONTest{edb_string}))['edb_string'];

            WITH MODULE test
            SELECT
                JSONTest {number, edb_string}
            FILTER
                # strict `=` will discard objects with empty `edb_string`
                <json>.edb_string =
                    (<json>(JSONTest{edb_string}))['edb_string'];

            WITH MODULE test
            SELECT
                JSONTest {number, edb_string}
            FILTER
                # casting all the way to the original type with a
                # weak `?=` will not discard anything
                .edb_string ?=
                    <str>(<json>(JSONTest{edb_string}))['edb_string'];

            WITH MODULE test
            SELECT
                JSONTest {number, edb_string}
            FILTER
                # casting both sides into json combined with a
                # weak `?=` will discard objects with empty `edb_string`
                <json>.edb_string ?=
                    (<json>(JSONTest{edb_string}))['edb_string'];
        """, lambda x: x['number'], [
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
                {'number': 2, 'edb_string': None},
                {'number': 3, 'edb_string': None},
            ],


            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
            ],
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
            ],
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
                {'number': 2, 'edb_string': None},
                {'number': 3, 'edb_string': None},
            ],
            [
                {'number': 0, 'edb_string': 'jumps'},
                {'number': 1, 'edb_string': 'over'},
            ],
        ])

    async def test_edgeql_json_cast_tuple_to_json_01(self):
        res = await self.query("""
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

        val = res[0][0]
        self.assertIsInstance(val, str)

        self.assertEqual(
            json.loads(val),
            [1, {"foo": "bar", "name": "std::json"}]
        )

    # Casting of arbitrary arrays to std::json
    # is not currently implemented.
    @unittest.expectedFailure
    async def test_edgeql_json_cast_tuple_to_json_02(self):
        res = await self.query("""
            SELECT
                to_str(<json>(
                    foo := 1,
                    bar := [1, 2, 3]
                ));
        """)

        val = res[0][0]
        self.assertIsInstance(val, str)

        self.assertEqual(
            json.loads(val),
            {"foo": 1, "bar": [1, 2, 3]}
        )

    async def test_edgeql_json_slice_01(self):
        await self.assert_query_result(r'''
            SELECT to_json('[1, "a", 3, null]')[:1];
            SELECT to_json('[1, "a", 3, null]')[:-1];
            SELECT to_json('[1, "a", 3, null]')[1:-1];
            SELECT to_json('[1, "a", 3, null]')[-1:1];
            SELECT to_json('[1, "a", 3, null]')[-100:100];
            SELECT to_json('[1, "a", 3, null]')[100:-100];
        ''', [
            [[1]],
            [[1, 'a', 3]],
            [['a', 3]],
            [[]],
            [[1, 'a', 3, None]],
            [[]],
        ])

    async def test_edgeql_json_slice_02(self):
        await self.assert_query_result(r'''
            SET MODULE test;

            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[:1];

            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[:-1];

            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[1:-1];

            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[-1:1];

            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[-100:100];

            WITH JT2 := (SELECT JSONTest FILTER .number = 2)
            SELECT JT2.j_array[100:-100];
        ''', [
            None,
            [[2]],
            [[2, "q", [3], {}]],
            [["q", [3], {}]],
            [[]],
            [[2, "q", [3], {}, None]],  # None is a legitimate JSON null
            [[]],
        ])

    async def test_edgeql_json_slice_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot slice json array by.*str'):

            await self.query(r"""
                SELECT to_json('[1, "a", 3, null]')[:'1'];
            """)

    async def test_edgeql_json_bytes_cast_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'cannot cast.*bytes.*to.*json.*'):

            await self.query(r"""
                SELECT <json>b'foo';
            """)

    async def test_edgeql_json_view_01(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := json_get(JSONTest.j_array, '0')
            ORDER BY _;

            WITH MODULE test
            SELECT _ := json_get(JSONTest.j_array, '10')
            ORDER BY _;

            WITH MODULE test
            SELECT _ := json_get(JSONTest.j_array, {'-1', '4'})
            ORDER BY _;

            WITH MODULE test
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
        ''', [
            [1, 2],
            [],
            # Nones in the output are legitimate JSON nulls
            [None, None, 1],
            [None, None, {'bingo': '42!'}, {'bingo': '42!'}],
        ])

    async def test_edgeql_json_view_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
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
        ''', [
            # We expect 3 * 2 * 2 * 2 = 24 results per JSONTest.data.
            # There are 3 non-empty data properties. Out of them all
            # only 4 will have a valid path. So we expect 24 * 3 - 4 =
            # 68 default 'N/A' results, 2 JSON `nulls` represented as
            # `None` and 2 JSON objects.
            [None, None] + ['N/A'] * 68 + [{'bingo': '42!'}, {'bingo': '42!'}],
        ])

    async def test_edgeql_json_view_03(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
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
        ''', [
            [{
                'a0': [1, 2],
                'a1': [],
                # Nones in the output are legitimate JSON nulls
                'a2': [None, None, 1],
                'a3': [None, None, {'bingo': '42!'}, {'bingo': '42!'}],
            }],
        ])

    async def test_edgeql_json_view_04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT _ := json_get(
                JSONTest.data,
                '0', '1', '2',
                default := <json>{'N/A', 'nope', '-'}
            )
            ORDER BY _;
        ''', [
            # None of the 3 data objects have the path 0, 1, 2, so we
            # expect default values in the result in triplicate.
            ['-', '-', '-', 'N/A', 'N/A', 'N/A', 'nope', 'nope', 'nope'],
        ])

    async def test_edgeql_json_view_05(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
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
        ''', [
            [{
                'a4': [
                    '-', '-', '-',
                    'N/A', 'N/A', 'N/A',
                    'nope', 'nope', 'nope'
                ],
            }],
        ])

    async def test_edgeql_json_str_function_01(self):
        await self.assert_query_result(r'''
            SELECT to_str(<json>[1, 2, 3, 4]);
            SELECT to_str(<json>[1, 2, 3, 4], 'pretty');
        ''', [
            {'[1, 2, 3, 4]'},
            {'[\n    1,\n    2,\n    3,\n    4\n]'},
        ])

    async def test_edgeql_json_str_function_02(self):
        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"format 'foo' is invalid for JSON"):
            async with self.con.transaction():
                await self.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'foo');
                ''')

        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"format '' is invalid for JSON"):
            async with self.con.transaction():
                await self.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], '');
                ''')

        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"format 'PRETTY' is invalid for JSON"):
            async with self.con.transaction():
                await self.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'PRETTY');
                ''')

        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"format 'Pretty' is invalid for JSON"):
            async with self.con.transaction():
                await self.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'Pretty');
                ''')

        with self.assertRaisesRegex(
                edgedb.InternalServerError,
                r"format 'p' is invalid for JSON"):
            async with self.con.transaction():
                await self.query(r'''
                    SELECT to_str(<json>[1, 2, 3, 4], 'p');
                ''')
