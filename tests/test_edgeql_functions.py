##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest  # NOQA

from edgedb.server import _testbase as tb


class TestEdgeQLFunctions(tb.QueryTestCase):
    async def test_edgeql_functions_array_unpack_01(self):
        await self.assert_query_result(r'''
            SELECT [1, 2];
            SELECT array_unpack([1, 2]);
            SELECT array_unpack([10, 20]) - 1;
        ''', [
            [[1, 2]],
            [1, 2],
            [9, 19],
        ])

    async def test_edgeql_functions_array_enumerate_01(self):
        await self.assert_query_result(r'''
            SELECT [10, 20];
            SELECT array_enumerate([10,20]);
            SELECT array_enumerate([10,20]).1 + 100;
        ''', [
            [[10, 20]],
            [[10, 0], [20, 1]],
            [100, 101],
        ])

    async def test_edgeql_functions_array_contains_01(self):
        await self.assert_query_result(r'''
            SELECT std::array_contains(<array<int>>[], {1, 3});
            SELECT array_contains([1], {1, 3});
            SELECT array_contains([1, 2], 1);
            SELECT array_contains([1, 2], 3);
            SELECT array_contains(['a'], <std::str>{});
        ''', [
            [False, False],
            [True, False],
            [True],
            [False],
            [],
        ])

    @unittest.expectedFailure
    async def test_edgeql_functions_array_enumerate_02(self):
        # Fix type inference for functions.
        await self.assert_query_result(r'''
            SELECT array_enumerate([10,20]).0 + 100;
        ''', [
            [110, 120],
        ])

    async def test_edgeql_functions_re_match_01(self):
        await self.assert_query_result(r'''
            SELECT re_match('AbabaB', 'ab');
            SELECT re_match('AbabaB', 'AB');
            SELECT re_match('AbabaB', '(?i)AB');
            SELECT re_match('AbabaB', 'ac');

            SELECT EXISTS re_match('AbabaB', 'ac');
            SELECT NOT EXISTS re_match('AbabaB', 'ac');

            SELECT EXISTS re_match('AbabaB', 'ab');
            SELECT NOT EXISTS re_match('AbabaB', 'ab');

            SELECT x := re_match('AbabaB', {'(?i)ab', 'a'}) ORDER BY x;
            SELECT x := re_match({'AbabaB', 'qwerty'}, {'(?i)ab', 'a'})
                ORDER BY x;
        ''', [
            [['ab']],
            [],
            [['Ab']],
            [],

            [False],
            [True],

            [True],
            [False],

            [['Ab'], ['a']],
            [['Ab'], ['a']],
        ])

    async def test_edgeql_functions_re_match_02(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT x := re_match(Concept.name, '(\w+)::(Link\w*)')
            ORDER BY x;
        ''', [
            [['schema', 'Link'], ['schema', 'LinkProperty']],
        ])

    async def test_edgeql_functions_re_match_all_01(self):
        await self.assert_query_result(r'''
            SELECT re_match_all('AbabaB', 'ab');
            SELECT re_match_all('AbabaB', 'AB');
            SELECT re_match_all('AbabaB', '(?i)AB');
            SELECT re_match_all('AbabaB', 'ac');

            SELECT EXISTS re_match_all('AbabaB', 'ac');
            SELECT NOT EXISTS re_match_all('AbabaB', 'ac');

            SELECT EXISTS re_match_all('AbabaB', '(?i)ab');
            SELECT NOT EXISTS re_match_all('AbabaB', '(?i)ab');

            SELECT x := re_match_all('AbabaB', {'(?i)ab', 'a'}) ORDER BY x;
            SELECT x := re_match_all({'AbabaB', 'qwerty'}, {'(?i)ab', 'a'})
                ORDER BY x;
        ''', [
            [['ab']],
            [],
            [['Ab'], ['ab'], ['aB']],
            [],

            [False],
            [True],

            [True],
            [False],

            [['Ab'], ['a'], ['a'], ['aB'], ['ab']],
            [['Ab'], ['a'], ['a'], ['aB'], ['ab']],
        ])

    async def test_edgeql_functions_re_match_all_02(self):
        await self.assert_query_result(r'''
            WITH
                MODULE schema,
                C2 := Concept
            SELECT
                count(ALL re_match_all(Concept.name, '(\w+)')) =
                2 * count(ALL C2);
        ''', [
            [True],
        ])

    async def test_edgeql_functions_re_test_01(self):
        await self.assert_query_result(r'''
            SELECT re_test('AbabaB', 'ac');
            SELECT NOT re_test('AbabaB', 'ac');

            SELECT re_test('AbabaB', '(?i)ab');
            SELECT NOT re_test('AbabaB', '(?i)ab');

            # the result always exists
            SELECT EXISTS re_test('AbabaB', '(?i)ac');
            SELECT NOT EXISTS re_test('AbabaB', '(?i)ac');

            SELECT x := re_test('AbabaB', {'ab', 'a'}) ORDER BY x;
            SELECT x := re_test({'AbabaB', 'qwerty'}, {'ab', 'a'}) ORDER BY x;
        ''', [
            [False],
            [True],

            [True],
            [False],

            [True],
            [False],

            [True, True],
            [False, False, True, True],
        ])

    async def test_edgeql_functions_re_test_02(self):
        await self.assert_query_result(r'''
            WITH MODULE schema
            SELECT count(ALL
                Concept FILTER re_test(Concept.name, '(\W\w)bject')
            ) = 1;
        ''', [
            [True],
        ])
