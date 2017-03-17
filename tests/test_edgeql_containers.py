##
# Copyright (c) 2017-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.server import _testbase as tb
from edgedb.client import exceptions as exc


class TestEdgeQLContainers(tb.QueryTestCase):
    '''The test DB is the same as for TestEdgeQLSelect.

    The scope of the tests here is creation and access of:
        - sets
        - tuples
        - structs
        - arrays
        - maps
    '''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'queries.eschema')

    SETUP = r"""
        WITH MODULE test
        INSERT Priority {
            name := 'High'
        };

        WITH MODULE test
        INSERT Priority {
            name := 'Low'
        };

        WITH MODULE test
        INSERT Status {
            name := 'Open'
        };

        WITH MODULE test
        INSERT Status {
            name := 'Closed'
        };


        WITH MODULE test
        INSERT User {
            name := 'Elvis'
        };

        WITH MODULE test
        INSERT User {
            name := 'Yury'
        };

        WITH MODULE test
        INSERT URL {
            name := 'edgedb.com',
            address := 'https://edgedb.com'
        };

        WITH MODULE test
        INSERT File {
            name := 'screenshot.png'
        };

        WITH MODULE test
        INSERT LogEntry {
            owner := (SELECT User FILTER User.name = 'Elvis'),
            spent_time := 50000,
            body := 'Rewriting everything.'
        };

        WITH MODULE test
        INSERT Issue {
            number := '1',
            name := 'Release EdgeDB',
            body := 'Initial public release of EdgeDB.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            watchers := (SELECT User FILTER User.name = 'Yury'),
            status := (SELECT Status FILTER Status.name = 'Open'),
            time_spent_log := (SELECT LogEntry),
            time_estimate := 3000
        };

        WITH MODULE test
        INSERT Comment {
            body := 'EdgeDB needs to happen soon.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            issue := (SELECT Issue FILTER Issue.number = '1')
        };


        WITH MODULE test
        INSERT Issue {
            number := '2',
            name := 'Improve EdgeDB repl output rendering.',
            body := 'We need to be able to render data in tabular format.',
            owner := (SELECT User FILTER User.name = 'Yury'),
            watchers := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Open'),
            priority := (SELECT Priority FILTER Priority.name = 'High'),
            references :=
                (SELECT URL FILTER URL.address = 'https://edgedb.com')
                UNION
                (SELECT File FILTER File.name = 'screenshot.png')
        };

        WITH
            MODULE test,
            I := (SELECT Issue)
        INSERT Issue {
            number := '3',
            name := 'Repl tweak.',
            body := 'Minor lexer tweaks.',
            owner := (SELECT User FILTER User.name = 'Yury'),
            watchers := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Closed'),
            related_to := (
                SELECT I FILTER I.number = '2'
            ),
            priority := (SELECT Priority FILTER Priority.name = 'Low')
        };

        WITH
            MODULE test,
            I := (SELECT Issue)
        INSERT Issue {
            number := '4',
            name := 'Regression.',
            body := 'Fix regression introduced by lexer tweak.',
            owner := (SELECT User FILTER User.name = 'Elvis'),
            status := (SELECT Status FILTER Status.name = 'Closed'),
            related_to := (
                SELECT I FILTER I.number = '3'
            ),
            tags := ['regression', 'lexer']
        };

        # NOTE: UPDATE Users for testing the link properties
        #
        WITH MODULE test
        UPDATE User
        FILTER User.name = 'Elvis'
        SET {
            todo := (SELECT Issue FILTER Issue.number in ('1', '2'))
        };

        WITH MODULE test
        UPDATE User
        FILTER User.name = 'Yury'
        SET {
            todo := (SELECT Issue FILTER Issue.number in ('3', '4'))
        };
    """

    @unittest.expectedFailure
    async def test_edgeql_containers_creation01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT array_agg(ALL Issue.number ORDER BY Issue.number);

            WITH MODULE test
            SELECT array_agg(ALL Issue.number ORDER BY Issue.number) =
                ['1', '2', '3', '4'];
        ''', [[
            ['1', '2', '3', '4'],
            [True],
        ]])

    async def test_edgeql_containers_in01(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number IN ('1', '2', '3')
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    async def test_edgeql_containers_in02(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number IN ['1', '2', '3']
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    async def test_edgeql_containers_in03(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number IN ('1' UNION '2' UNION '3')
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    async def test_edgeql_containers_in04(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number IN ('1', '2', '3', '3', '3', '2')
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    async def test_edgeql_containers_in05(self):
        await self.assert_query_result('''
            WITH MODULE test
            SELECT Issue {
                number,
            } FILTER .number IN ['1', '2', '3', '3', '3', '2']
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    @unittest.expectedFailure
    async def test_edgeql_containers_in06(self):
        await self.assert_query_result('''
            WITH
                MODULE test,
                x := ('1', '2', '3')
            SELECT Issue {
                number,
            } FILTER .number IN x
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    @unittest.expectedFailure
    async def test_edgeql_containers_in07(self):
        await self.assert_query_result('''
            WITH
                MODULE test,
                x := ['1', '2', '3']
            SELECT Issue {
                number,
            } FILTER .number IN x
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    async def test_edgeql_containers_in08(self):
        await self.assert_query_result('''
            WITH
                MODULE test,
                x := '1' UNION '2' UNION '3'
            SELECT Issue {
                number,
            } FILTER .number IN x
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])

    @unittest.expectedFailure
    async def test_edgeql_containers_in09(self):
        await self.assert_query_result('''
            WITH
                MODULE test,
                x := array_agg(ALL '1' UNION '2' UNION '3')
            SELECT Issue {
                number,
            } FILTER .number IN x
              ORDER BY .number;
        ''', [[
            {'number': '1'},
            {'number': '2'},
            {'number': '3'},
        ]])
