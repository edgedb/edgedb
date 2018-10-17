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

from edb.client import exceptions as exc
from edb.server import _testbase as tb


class TestEdgeQLJSON(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'json.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'json_setup.eql')

    async def test_edgeql_json_cast_01(self):
        await self.assert_query_result("""
            SELECT str_to_json('"qwerty"');
            SELECT str_to_json('1');
            SELECT str_to_json('2.3e-2');

            SELECT str_to_json('true');
            SELECT str_to_json('false');
            SELECT str_to_json('null');

            SELECT str_to_json('[2, "a", 3.456]');
            SELECT str_to_json('[2, "a", 3.456, [["b", 1]]]');

            SELECT str_to_json('{
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
            [None],

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
                'e': None,
                'f': False
            }],
        ])

    async def test_edgeql_json_cast_02(self):
        await self.assert_query_result("""
            SELECT <str>str_to_json('"qwerty"');
            SELECT <int64>str_to_json('1');
            SELECT <float64>str_to_json('2.3e-2');

            SELECT <bool>str_to_json('true');
            SELECT <bool>str_to_json('false');

            SELECT <array<int64>>str_to_json('[2, 3, 5]');
        """, [
            ['qwerty'],
            [1],
            [0.023],

            [True],
            [False],

            [[2, 3, 5]],
        ])

    async def test_edgeql_json_cast_03(self):
        await self.assert_query_result("""
            SELECT <str>str_to_json('null');
            SELECT <int64>str_to_json('null');
            SELECT <float64>str_to_json('null');
            SELECT <bool>str_to_json('null');
        """, [
            [],
            [],
            [],
            [],
        ])

    async def test_edgeql_json_cast_04(self):
        await self.assert_query_result("""
            SELECT <str>str_to_json('null') ?= <str>{};
            SELECT <int64>str_to_json('null') ?= <int64>{};
            SELECT <float64>str_to_json('null') ?= <float64>{};
            SELECT <bool>str_to_json('null') ?= <bool>{};
        """, [
            [True],
            [True],
            [True],
            [True],
        ])

    async def test_edgeql_json_cast_05(self):
        await self.assert_query_result("""
            SELECT <json>{} ?= (
                SELECT x := str_to_json('1') FILTER x = str_to_json('2')
            );
            SELECT <json>{} ?= str_to_json('null');
        """, [
            [True],
            [False],
        ])

    async def test_edgeql_json_accessor_01(self):
        await self.assert_query_result("""
            SELECT (str_to_json('[1, "a", 3]'))[0] = str_to_json('1');
            SELECT (str_to_json('[1, "a", 3]'))[1] = str_to_json('"a"');
            SELECT (str_to_json('[1, "a", 3]'))[2] = str_to_json('3');

            SELECT (str_to_json('[1, "a", 3]'))[<int16>0] = str_to_json('1');
            SELECT (str_to_json('[1, "a", 3]'))[<int32>0] = str_to_json('1');
        """, [
            [True],
            [True],
            [True],

            [True],
            [True],
        ])

    async def test_edgeql_json_accessor_02(self):
        await self.assert_query_result("""
            SELECT (str_to_json('{"a": 1, "b": null}'))["a"] =
                str_to_json('1');

            SELECT (str_to_json('{"a": 1, "b": null}'))["b"] =
                str_to_json('null');
        """, [
            [True],
            [True],
        ])

    async def test_edgeql_json_accessor_03(self):
        await self.assert_query_result("""
            SELECT (<str>(str_to_json('["qwerty"]'))[0])[1];
        """, [
            ['w'],
        ])

    async def test_edgeql_json_accessor_04(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                exc.UnknownEdgeDBError,
                r'json index 10 is out of bounds'):
            await self.con.execute(r"""
                SELECT (str_to_json('[1, "a", 3]'))[10];
            """)

    async def test_edgeql_json_accessor_05(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                exc.UnknownEdgeDBError,
                r'json index -10 is out of bounds'):
            await self.con.execute(r"""
                SELECT (str_to_json('[1, "a", 3]'))[-10];
            """)

    async def test_edgeql_json_accessor_06(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json array by text'):
            await self.con.execute(r"""
                SELECT (str_to_json('[1, "a", 3]'))['1'];
            """)

    async def test_edgeql_json_accessor_07(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                exc.UnknownEdgeDBError,
                r"json index 'c' is out of bounds"):
            await self.con.execute(r"""
                SELECT (str_to_json('{"a": 1, "b": null}'))["c"];
            """)

    async def test_edgeql_json_accessor_08(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json object by integer'):
            await self.con.execute(r"""
                SELECT (str_to_json('{"a": 1, "b": null}'))[0];
            """)

    async def test_edgeql_json_accessor_09(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json null'):
            await self.con.execute(r"""
                SELECT (str_to_json('null'))[0];
            """)

    async def test_edgeql_json_accessor_10(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json boolean'):
            await self.con.execute(r"""
                SELECT (str_to_json('true'))[0];
            """)

    async def test_edgeql_json_accessor_11(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json number'):
            await self.con.execute(r"""
                SELECT (str_to_json('123'))[0];
            """)

    async def test_edgeql_json_accessor_12(self):
        with self.assertRaisesRegex(
                # FIXME: currently JSON strings cannot be indexed or sliced
                exc.UnknownEdgeDBError,
                r'cannot index json string'):
            await self.con.execute(r"""
                SELECT (str_to_json('"qwerty"'))[0];
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
                exc.UnknownEdgeDBError,
                r'json index 10 is out of bounds'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][10]['bingo'];
            """)

    async def test_edgeql_json_accessor_15(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                exc.UnknownEdgeDBError,
                r'json index -10 is out of bounds'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[-10]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_16(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json array by text'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data['4']['b']['bar'][10]['bingo'];
            """)

    async def test_edgeql_json_accessor_17(self):
        with self.assertRaisesRegex(
                # FIXME: maybe a different error type should be used here
                exc.UnknownEdgeDBError,
                r"json index 'c' is out of bounds"):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['c'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_18(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json object by integer'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4][1]['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_19(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json null'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[4]['b']['bar'][0]['bingo'];
            """)

    async def test_edgeql_json_accessor_20(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json boolean'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[-1]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_21(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'cannot index json number'):
            await self.con.execute(r"""
                WITH
                    MODULE test,
                    JT3 := (SELECT JSONTest FILTER .number = 3)
                SELECT JT3.data[0]['b']['bar'][2]['bingo'];
            """)

    async def test_edgeql_json_accessor_22(self):
        with self.assertRaisesRegex(
                # FIXME: currently JSON strings cannot be indexed or sliced
                exc.UnknownEdgeDBError,
                r'cannot index json string'):
            await self.con.execute(r"""
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
            ).data ?= str_to_json('null');

            WITH MODULE test
            SELECT (
                SELECT JSONTest FILTER .number = 2
            ).data ?= <json>{};

            WITH MODULE test
            SELECT (
                SELECT JSONTest FILTER .number = 2
            ).data ?= str_to_json('null');
        ''', [
            {False},
            {True},
            {True},
            {False},
        ])

    async def test_edgeql_json_typeof_01(self):
        await self.assert_query_result(r"""
            SELECT json_typeof(str_to_json('2'));
            SELECT json_typeof(str_to_json('"foo"'));
            SELECT json_typeof(str_to_json('true'));
            SELECT json_typeof(str_to_json('false'));
            SELECT json_typeof(str_to_json('null'));
            SELECT json_typeof(str_to_json('[]'));
            SELECT json_typeof(str_to_json('[2]'));
            SELECT json_typeof(str_to_json('{}'));
            SELECT json_typeof(str_to_json('{"a": 2}'));
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
            SELECT json_array_unpack(str_to_json('[1, "a", null]'));
        """, [
            [1, 'a', None],
        ])

    async def test_edgeql_json_array_unpack_02(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'operator does not exist: jsonb = bigint'):
            await self.con.execute(r'''
                SELECT json_array_unpack(str_to_json('[2,3,4]')) IN
                    {2, 3, 4};
            ''')

    async def test_edgeql_json_array_unpack_03(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'operator does not exist: jsonb = text'):
            await self.con.execute(r'''
                SELECT json_array_unpack(str_to_json('[2,3,4]')) IN
                    {'2', '3', '4'};
            ''')

    async def test_edgeql_json_array_unpack_04(self):
        await self.assert_query_result(r'''
            SELECT json_array_unpack(str_to_json('[2,3,4]')) IN
                <json>{2, 3, 4};
            SELECT json_array_unpack(str_to_json('[2,3,4]')) NOT IN
                <json>{2, 3, 4};
        ''', [
            [True, True, True],
            [False, False, False],
        ])

    async def test_edgeql_json_array_unpack_05(self):
        with self.assertRaisesRegex(
                # FIXME: a different error should be used here, this
                # one leaks postgres types
                exc.UnknownEdgeDBError,
                r'operator does not exist: jsonb = bigint'):
            await self.con.execute(r'''
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
            SELECT json_array_unpack(JT0.j_array) = str_to_json('1');
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
            SELECT json_object_unpack(str_to_json('{
                "q": 1,
                "w": [2, null, 3],
                "e": null
            }'));
        ''', lambda x: x[0], [[
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
                ('c', str_to_json('1'));

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
                exc.UnknownEdgeDBError,
                r'cannot call jsonb_each on a non-object'):
            await self.con.execute(r'''
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

    async def test_edgeql_json_cast_object_to_json_01(self):
        res = await self.query("""
            WITH MODULE schema
            SELECT
                json_to_str(<json>(
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
                Object {
                    name
                }
            FILTER
                json_to_str(<json>(Object {name})) LIKE '%std::json%';
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
                json_to_str(
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
                json_to_str(<json>[(
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

    async def test_edgeql_json_cast_tuple_to_json_01(self):
        res = await self.query("""
            WITH MODULE schema
            SELECT
                json_to_str(<json>(
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

    async def test_edgeql_json_cast_tuple_to_json_02(self):
        res = await self.query("""
            SELECT
                json_to_str(<json>(
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
