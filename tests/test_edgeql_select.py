##
# Copyright (c) 2012-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.server import _testbase as tb
from edgedb.client import exceptions


class TestEdgeQLSelect(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'queries.eschema')

    SETUP = """
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
        INSERT LogEntry {
            owner := (SELECT User WHERE User.name = 'Elvis'),
            spent_time := 50000,
            body := 'Rewriting everything.'
        };


        WITH MODULE test
        INSERT Issue {
            number := '1',
            name := 'Release EdgeDB',
            body := 'Initial public release of EdgeDB.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            watchers := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            time_spent_log := (SELECT LogEntry),
            time_estimate := 3000
        };


        WITH MODULE test
        INSERT Comment {
            body := 'EdgeDB needs to happen soon.',
            owner := (SELECT User WHERE User.name = 'Elvis'),
            issue := (SELECT Issue WHERE Issue.number = '1')
        };


        WITH MODULE test
        INSERT Issue {
            number := '2',
            name := 'Improve EdgeDB repl output rendering.',
            body := 'We need to be able to render data in tabular format.',
            owner := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            priority := (SELECT Priority WHERE Priority.name = 'High')
        };

        WITH MODULE test
        INSERT Issue {
            number := '3',
            name := 'Repl tweak.',
            body := 'Minor lexer tweaks.',
            owner := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            related_to := (SELECT Issue WHERE Issue.number = '2'),
            priority := (SELECT Priority WHERE Priority.name = 'Low')
        };

        WITH MODULE test
        INSERT Issue {
            number := '4',
            name := 'Regression.',
            body := 'Fix regression introduced by lexer tweak.',
            owner := (SELECT User WHERE User.name = 'Yury'),
            status := (SELECT Status WHERE Status.name = 'Open'),
            related_to := (SELECT Issue WHERE Issue.number = '3')
        };
    """

    async def test_edgeql_select_computable(self):
        res = await self.con.execute('''
            WITH MODULE test
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
            WITH MODULE test
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

    async def test_edgeql_select_match01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name LIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name LIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name LIKE '%Edge%'
            ORDER BY Issue.number;
        ''')

        self.assert_data_shape(res, [
            [],
            [{'number': '1'}],
            [{'number': '1'}, {'number': '2'}],
        ])

    async def test_edgeql_select_match02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT LIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT LIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT LIKE '%Edge%'
            ORDER BY Issue.number;
        ''')

        self.assert_data_shape(res, [
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [{'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_match03(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name ILIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name ILIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name ILIKE '%re%'
            ORDER BY Issue.number;
        ''')

        self.assert_data_shape(res, [
            [{'number': '1'}],
            [{'number': '1'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        ])

    async def test_edgeql_select_match04(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT ILIKE '%edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT ILIKE '%EdgeDB'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name NOT ILIKE '%re%'
            ORDER BY Issue.number;
        ''')

        self.assert_data_shape(res, [
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
            [],
        ])

    async def test_edgeql_select_match05(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name @@ 'edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.body @@ 'need'
            ORDER BY Issue.number;
        ''')

        self.assert_data_shape(res, [
            [{'number': '1'}, {'number': '2'}],
            [{'number': '2'}],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_match06(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.name @@! 'edgedb'
            ORDER BY Issue.number;

            WITH MODULE test
            SELECT
                Issue {number}
            WHERE
                Issue.body @@! 'need'
            ORDER BY Issue.number;
        ''')

        self.assert_data_shape(res, [
            [],
            [{'number': '1'}],
        ])

    async def test_edgeql_select_match07(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~ 'ed'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~ 'eD'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~ 'ed([S\s]|$)'
            ORDER BY Text.body;
        ''')

        self.assert_data_shape(res, [
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Initial public release of EdgeDB.'}],
            [{'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}]
        ])

    async def test_edgeql_select_match08(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~* 'ed'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~* 'eD'
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {body}
            WHERE
                Text.body ~* 'ed([S\s]|$)'
            ORDER BY Text.body;
        ''')

        self.assert_data_shape(res, [
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        ])

    async def test_edgeql_select_type01(self):
        res = await self.con.execute('''
            WITH MODULE test
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

        self.assert_data_shape(res, [
            [{
                'number': '1',
                '__type__': {'name': 'test::Issue'},
            }],
        ])

    async def test_edgeql_select_type02(self):
        res = await self.con.execute('''
            WITH MODULE test
            SELECT User.__type__.name LIMIT 1;
        ''')

        self.assert_data_shape(res, [
            ['test::User']
        ])

    async def test_edgeql_select_exists01(self):
        res = await self.con.execute('''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
                NOT EXISTS (Issue.<issue[TO Comment]);
        ''')

        self.assert_data_shape(res[0], [{
            'number': '2',
        }, {
            'number': '3',
        }, {
            'number': '4',
        }])

        res = await self.con.execute('''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
                NOT EXISTS (SELECT Issue.<issue[TO Comment]);
        ''')

        self.assert_data_shape(res[0], [{
            'number': '2',
        }, {
            'number': '3',
        }, {
            'number': '4',
        }])

        res = await self.con.execute('''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
                EXISTS (Issue.<issue[TO Comment]);
        ''')

        self.assert_data_shape(res[0], [{
            'number': '1',
        }])

    async def test_edgeql_select_exists02(self):
        res = await self.con.execute('''
            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
                NOT EXISTS Issue.time_estimate;

            WITH MODULE test
            SELECT
                Issue {
                    number
                }
            WHERE
                EXISTS Issue.time_estimate;

        ''')

        self.assert_data_shape(res, [
            [{
                'number': '2',
            }, {
                'number': '3',
            }, {
                'number': '4',
            }],
            [{
                'number': '1',
            }]
        ])

    async def test_edgeql_select_recursive01(self):
        res = await self.con.execute('''
            WITH MODULE test
            SELECT
                Issue {
                    number,
                    <related_to: {
                        number,
                    },
                }
            WHERE
                Issue.number = '2';

            WITH MODULE test
            SELECT
                Issue {
                    number,
                    <related_to *1
                }
            WHERE
                Issue.number = '2';
        ''')

        self.assert_data_shape(res, [
            [{
                'number': '2',
                'related_to': [{
                    'number': '3',
                }]
            }],
            [{
                'number': '2',
                'related_to': [{
                    'number': '3',
                }]
            }],
        ])

    async def test_edgeql_select_limit01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2;

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 3;

            WITH MODULE test
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2 LIMIT 3;
        ''')

        self.assert_data_shape(res, [
            [{'number': '3'}, {'number': '4'}],
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
            [{'number': '3'}, {'number': '4'}],
        ])

    async def test_edgeql_select_specialized01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Text {body}
            ORDER BY Text.body;

            WITH MODULE test
            SELECT
                Text {
                    Issue.name,
                    body,
                }
            ORDER BY Text.body;
        ''')

        self.assert_data_shape(res, [
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render data in tabular format.'}
            ],
            [
                {'body': 'EdgeDB needs to happen soon.',
                 'name': None},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'Rewriting everything.',
                 'name': None},
                {'body': 'We need to be able to render data in tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ]
        ])

    async def test_edgeql_select_specialized02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT User{
                name,
                <owner: LogEntry {
                    body
                },
            } WHERE User.name = 'Elvis';
        ''')

        self.assert_data_shape(res, [
            [{
                'name': 'Elvis',
                'owner': [
                    {'body': 'Rewriting everything.'}
                ],
            }],
        ])

    async def test_edgeql_select_instance01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Text {body}
            WHERE Text IS Comment
            ORDER BY Text.body;
        ''')

        self.assert_data_shape(res, [
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
        ])

        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Text {body}
            WHERE Text IS NOT (Comment, Issue)
            ORDER BY Text.body;
        ''')

        self.assert_data_shape(res, [
            [
                {'body': 'Rewriting everything.'},
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_instance02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Text {body}
            WHERE Text IS Issue AND (Text AS Issue).number = 1
            ORDER BY Text.body;
        ''')

        self.assert_data_shape(res, [
            [
                {'body': 'Initial public release of EdgeDB.'},
            ],
        ])

    async def test_edgeql_select_combined01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {name, body}
            UNION
            SELECT
                Comment {body};

            WITH MODULE test
            SELECT
                Text {body}
            INTERSECT
            SELECT
                Comment {body};

            WITH MODULE test
            SELECT
                Text {body}
            EXCEPT
            SELECT
                Comment {body};
        ''')
        # sorting manually to test basic functionality first
        for r in res:
            r.sort(key=lambda x: x['body'])

        self.assert_data_shape(res, [
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'We need to be able to render data in tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ],
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
            [
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render data in tabular format.'}
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_combined02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT
                Issue {name, body}
            UNION
            SELECT
                Comment {body}
            ORDER BY (Object AS Text).body;

            WITH MODULE test
            SELECT
                Text {body}
            INTERSECT
            SELECT
                Comment {body}
            ORDER BY (Object AS Text).body;

            WITH MODULE test
            SELECT
                Text {body}
            EXCEPT
            SELECT
                Comment {body}
            ORDER BY (Object AS Text).body;
        ''')

        self.assert_data_shape(res, [
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'We need to be able to render data in tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ],
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
            [
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render data in tabular format.'}
            ],
        ])

    async def test_edgeql_select_order01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC NULLS LAST THEN Issue.name;

            WITH MODULE test
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC NULLS FIRST THEN Issue.name;
        ''')

        self.assert_data_shape(res, [
            [
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
            ],
            [
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
            ]
        ])

    async def test_edgeql_select_func01(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT std::strlen(User.name) ORDER BY User.name;

            WITH MODULE test
            SELECT std::sum(<std::int>Issue.number);
        ''')

        self.assert_data_shape(res, [
            [5, 4],
            [10]
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_func02(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT std::lower(string:=User.name) ORDER BY User.name;
        ''')

        self.assert_data_shape(res, [
            ['elvis', 'yury'],
        ])

    @unittest.expectedFailure
    async def test_edgeql_select_func03(self):
        res = await self.con.execute(r'''
            WITH MODULE test
            SELECT std::count(User.<owner.id)
            GROUP BY User ORDER BY User.name;
        ''')
        self.assert_data_shape(res, [
            [3, 3],
        ])
