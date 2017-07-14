##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.server import _testbase as tb


class TestEdgeQLFunctions(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.eql')

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
        # Fix type inference for functions.
        await self.assert_query_result(r'''
            SELECT re_match('ababab', 'ab');
            SELECT re_match('ababab', 'ab', 'g');
            SELECT re_match('ababab', 'ac');

            SELECT EXISTS re_match('ababab', 'ac', 'g');
            SELECT NOT EXISTS re_match('ababab', 'ac', 'g');

            SELECT EXISTS re_match('ababab', 'ac');
            SELECT NOT EXISTS re_match('ababab', 'ac');

            SELECT EXISTS re_match('ababab', 'ab', 'g');
            SELECT NOT EXISTS re_match('ababab', 'ab', 'g');

            SELECT EXISTS re_match('ababab', 'ab');
            SELECT NOT EXISTS re_match('ababab', 'ab');

            SELECT x := re_match('ababab', {'ab', 'a'}, 'g') ORDER BY x;
        ''', [
            [['ab']],
            [['ab'], ['ab'], ['ab']],
            [],

            [False],
            [True],

            [False],
            [True],

            [True],
            [False],

            [True],
            [False],

            [['a'], ['a'], ['a'], ['ab'], ['ab'], ['ab']],
        ])
