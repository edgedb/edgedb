##
# Copyright (c) 2012-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.server import _testbase as tb
from edgedb.client import exceptions


class TestConstraints(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'queries.eschema')

    SETUP = """
        USING NAMESPACE test
        INSERT Priority {
            name := 'High'
        };

        USING NAMESPACE test
        INSERT Priority {
            name := 'Low'
        };

        USING NAMESPACE test
        INSERT Status {
            name := 'Open'
        };

        USING NAMESPACE test
        INSERT Status {
            name := 'Closed'
        };


        USING NAMESPACE test
        INSERT User {
            name := 'Elvis'
        };


        USING NAMESPACE test
        INSERT User {
            name := 'Yury'
        };


        USING NAMESPACE test
        INSERT LogEntry {
            owner := (SELECT User WHERE User.name = 'Elvis'),
            spent_time := 50000,
            body := 'Rewriting everything.'
        };


        USING NAMESPACE test
        INSERT Issue {
            number := '1',
            name := 'Release EdgeDB',
            body := 'Initial public release of EdgeDB.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            watchers := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            time_spent_log := (SELECT LogEntry)
        };


        USING NAMESPACE test
        INSERT Comment {
            body := 'EdgeDB needs to happen soon.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            issue := (SELECT Issue WHERE Issue.number = '1')
        };


        USING NAMESPACE test
        INSERT Issue {
            number := '2',
            name := 'Improve EdgeDB repl output rendering.',
            body := 'We need to be able to render data in tabular format.',
            owner := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            priority := (SELECT Priority WHERE Priority.name = 'High')
        };
    """

    async def test_queries_computable(self):
        res = await self.con.execute('''
            USING NAMESPACE test
            SELECT
                Issue {
                    number,
                    aliased_number := Issue.number,
                    total_time_spent := (
                        SELECT sum(Issue.time_spent_log.spent_time)
                    )
                }
            WHERE
                Issue.number = '1';
        ''')

        self.assert_data_shape(res[0], [{
            'number': '1',
            'aliased_number': '1',
            'total_time_spent': 50000
        }])

        res = await self.con.execute('''
            USING NAMESPACE test
            SELECT
                Issue {
                    number,
                    total_time_spent := sum(Issue.time_spent_log.spent_time)
                }
            WHERE
                Issue.number = '1';
        ''')

        self.assert_data_shape(res[0], [{
            'number': '1',
            'total_time_spent': 50000
        }])

    async def test_queries_parentheses(self):
        res = await self.con.execute('''
            SELECT
                test::Issue {
                    number
                }
            WHERE
                (((test::Issue)).number) = '1';

            SELECT
                (test::Issue) {
                    number
                }
            WHERE
                (((test::Issue)).(number)) = '1';

            SELECT
                test::Issue {
                    test::number
                }
            WHERE
                (((test::Issue)).(test::number)) = '1';
        ''')

        self.assert_data_shape(res[0], [{
            'number': '1',
        }, {
            'number': '1',
        }, {
            'number': '1',
        }])

    async def test_queries_type01(self):
        res = await self.con.execute('''
            USING NAMESPACE test
            SELECT
                Issue {
                    number,
                    __type__: {
                        name
                    }
                }
            WHERE
                Issue.number = '1';
        ''')

        self.assert_data_shape(res[0], [{
            'number': '1',
            '__type__': {'name': 'test::Issue'},
        }])

    async def test_queries_exists01(self):
        res = await self.con.execute('''
            USING NAMESPACE test
            SELECT
                Issue {
                    number
                }
            WHERE
                NOT EXISTS (Issue.<(issue TO Comment));
        ''')

        self.assert_data_shape(res[0], [{
            'number': '2',
        }])

        res = await self.con.execute('''
            USING NAMESPACE test
            SELECT
                Issue {
                    number
                }
            WHERE
                NOT EXISTS (SELECT Issue.<(issue TO Comment));
        ''')

        self.assert_data_shape(res[0], [{
            'number': '2',
        }])

        res = await self.con.execute('''
            USING NAMESPACE test
            SELECT
                Issue {
                    number
                }
            WHERE
                EXISTS (Issue.<(issue TO Comment));
        ''')

        self.assert_data_shape(res[0], [{
            'number': '1',
        }])
