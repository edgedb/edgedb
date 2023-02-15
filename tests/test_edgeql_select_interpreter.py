#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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
import unittest

import edb.tools.experimental_interpreter.new_interpreter as model

from edb.common import assert_data_shape

bag = assert_data_shape.bag


class TestEdgeQLSelectInterpreter(unittest.TestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.edgeql')


    def assert_query_result(
        self, query, expected, *, db, sort=None, singleton_cheating=False
    ):
        if db is None:
            db = model.db_with_initilial_queries(open(TestEdgeQLSelectInterpreter.SETUP).read())
        qltree = model.parse(query)
        result = model.go(qltree, db, singleton_cheating)
        if sort:
            assert_data_shape.sort_results(result, sort)

        assert_data_shape.assert_data_shape(result, expected, self.fail)

    def test_edgeql_select_unique_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue.watchers.<owner[IS Issue] {
                    name
                } ORDER BY .name;
            ''',
            [{
                'name': 'Improve EdgeDB repl output rendering.',
            }, {
                'name': 'Regression.',
            }, {
                'name': 'Release EdgeDB',
            }, {
                'name': 'Repl tweak.',
            }]
        )

    def test_edgeql_select_unique_02(self):
        self.assert_query_result(
            r'''
            SELECT Issue.owner{name}
            ORDER BY Issue.owner.name;
            ''',
            [
                {'name': 'Elvis'}, {'name': 'Yury'},
            ]
        )

    def test_edgeql_select_computable_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number,
                    aliased_number := Issue.number,
                    total_time_spent := (
                        SELECT sum(Issue.time_spent_log.spent_time)
                    )
                }
            FILTER
                Issue.number = '1';
            ''',
            [{
                'number': '1',
                'aliased_number': '1',
                'total_time_spent': 50000
            }]
        )

    def test_edgeql_select_computable_02(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number,
                    total_time_spent := (
                        SELECT sum(Issue.time_spent_log.spent_time)
                    )
                }
            FILTER
                Issue.number = '1';
            ''',
            [{
                'number': '1',
                'total_time_spent': 50000
            }]
        )

    def test_edgeql_select_computable_03(self):
        self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT
                            Text {
                                body
                            }
                        FILTER
                            Text[IS Owned].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT 1
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Rewriting everything.',
                },
            }]
        )

    def test_edgeql_select_computable_04(self):
        self.assert_query_result(
            r'''
            WITH
                # we aren't referencing User in any way, so this works
                # best as a subquery, rather than inline computable
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT 1
                )
            SELECT
                User {
                    name,
                    shortest_text := sub {
                        body
                    }
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    def test_edgeql_select_computable_05(self):
        self.assert_query_result(
            r'''
            WITH
                # we aren't referencing User in any way, so this works
                # best as a subquery, than inline computable
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                User {
                    name,
                    shortest_own_text := (
                        SELECT
                            Text {body}
                        FILTER
                            Text[IS Owned].owner = User
                        ORDER BY
                            len(Text.body) ASC
                        LIMIT
                            1
                    ),
                    shortest_text := sub {
                        body
                    },
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_own_text': {
                    'body': 'Rewriting everything.',
                },
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    def test_edgeql_select_computable_06(self):
        self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    shortest_text := (
                        SELECT
                            Text {body}
                        # a clause that references User and is always true
                        FILTER
                            User IS User
                        ORDER
                            BY len(Text.body) ASC
                        LIMIT 1
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text': {
                    'body': 'Minor lexer tweaks.',
                },
            }]
        )

    def test_edgeql_select_computable_07(self):
        self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    # ad-hoc computable with many results
                    special_texts := (
                        SELECT Text {body}
                        FILTER Text[IS Owned].owner != User
                        ORDER BY len(Text.body) DESC
                    ),
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'special_texts': [
                    {'body': 'We need to be able to render data in '
                             'tabular format.'},
                    {'body': 'Minor lexer tweaks.'}
                ],
            }]
        )

    def test_edgeql_select_computable_08(self):
        self.assert_query_result(
            r"""
            # get a user + the latest issue (regardless of owner), which has
            # the same number of characters in the status as the user's name
            SELECT User{
                name,
                special_issue := (
                    SELECT Issue {
                        name,
                        number,
                        owner: {
                            name
                        },
                        status: {
                            name
                        }
                    }
                    FILTER len(Issue.status.name) = len(User.name)
                    ORDER BY Issue.number DESC
                    LIMIT 1
                )
            }
            ORDER BY User.name;
            """,
            [
                {
                    'name': 'Elvis',
                    'special_issue': None
                }, {
                    'name': 'Yury',
                    'special_issue': {
                        'name': 'Improve EdgeDB repl output rendering.',
                        'owner': {'name': 'Yury'},
                        'status': {'name': 'Open'},
                        'number': '2'
                    },
                }
            ],
        )

    def test_edgeql_select_computable_09(self):
        self.assert_query_result(
            r"""
            SELECT Text{
                body,
                name := Text[IS Issue].name IF Text IS Issue      ELSE
                        'log'                IF Text IS LogEntry   ELSE
                        'comment'            IF Text IS Comment    ELSE
                        'unknown'
            }
            ORDER BY Text.body;
            """,
            [
                {'body': 'EdgeDB needs to happen soon.',
                 'name': 'comment'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'Rewriting everything.',
                 'name': 'log'},
                {'body': 'We need to be able to render data in '
                         'tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ],
        )

    def test_edgeql_select_computable_10(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                name,
                number,
                # use shorthand with some simple operations
                foo := <int64>Issue.number + 10,
            }
            FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': 11,
            }],
        )

    def test_edgeql_select_computable_11(self):
        self.assert_query_result(
            r'''
            WITH
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub.body;
            ''',
            ['Minor lexer tweaks.']
        )

    def test_edgeql_select_computable_12(self):
        self.assert_query_result(
            r'''
            WITH
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub.__type__.name;
            ''',
            ['default::Issue']
        )

    def test_edgeql_select_computable_13(self):
        self.assert_query_result(
            r'''
            WITH
                sub := (
                    SELECT
                        Text
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT
                        1
                )
            SELECT
                sub[IS Issue].number;
            ''',
            ['3']
        )

    def test_edgeql_select_computable_14(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                name,
                number,
                # Explicit cardinality override
                multi foo := <int64>Issue.number + 10,
                }
                FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': [11],
            }],
        )

    def test_edgeql_select_computable_15(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly more than one element returned by an expression "
                r"for a computed property 'foo' declared as 'single'",
                _position=166):
            self.con.query("""\
                SELECT Issue{
                    name,
                    number,
                    # Explicit erroneous cardinality override
                    single foo := {1, 2}
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_16(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                name,
                number,
                single foo := <int64>{},
                single bar := 11,
            }
            FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'number': '1',
                'foo': None,
                'bar': 11,
            }]
        )

    def test_edgeql_select_computable_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly more than one element returned by an expression "
                r"for a computed property 'foo' declared as 'single'",
                _position=215):
            self.con.query("""\
                WITH
                    V := (SELECT Issue {
                        foo := {1, 2}
                    } FILTER .number = '1')
                SELECT
                    V {
                        single foo := .foo
                    };
            """)

    def test_edgeql_select_computable_18(self):
        with self._run_and_rollback():
            self.con.execute(
                '''
                    INSERT Publication {
                        title := 'aaa'
                    }
                '''
            )

            self.assert_query_result(
                r"""
                    SELECT Publication {
                        title,
                        title1,
                        title2,
                        title3,
                        title4,
                        title5,
                        title6,
                    }
                    FILTER .title = 'aaa'
                """,
                [{
                    'title': 'aaa',
                    'title1': 'aaa',
                    'title2': 'aaa',
                    'title3': 'aaa',
                    'title4': 'aaa',
                    'title5': ['aaa'],
                    'title6': ['aaa'],
                }]
            )

    def test_edgeql_select_computable_19(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                number,
                required foo := 42,
            }
            FILTER Issue.number = '1';
            """,
            [{
                'number': '1',
                'foo': 42,
            }]
        )

    def test_edgeql_select_computable_20(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                number,
                required foo := <int64>.number,
                required single bar := <int64>.number,
                required multi baz := <int64>.number,
                optional te := <str>.time_estimate,
            }
            FILTER Issue.number = '1';
            """,
            [{
                'number': '1',
                'foo': 1,
                'bar': 1,
                'baz': {1},
                'te': '3000',
            }]
        )

    def test_edgeql_select_computable_21(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed property 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required foo := <int64>{},
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_22(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed property 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required single foo := <int64>{},
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_23(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed property 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required multi foo := <int64>{},
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_24(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly more than one element returned by an expression "
                r"for a computed property 'foo' declared as 'single'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required single foo := {1, 2},
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_25(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed property 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required foo := <str>.time_estimate,
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_26(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed property 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required single foo := <str>.time_estimate,
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_27(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed property 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required multi foo := <str>.time_estimate,
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_28(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                number,
                required foo := .owner{
                    name
                },
                required single bar := .owner{
                    name
                },
                required multi baz := .owner{
                    name
                },
            }
            FILTER Issue.number = '1';
            """,
            [{
                'number': '1',
                'foo': {
                    'name': 'Elvis'
                },
                'bar': {
                    'name': 'Elvis'
                },
                'baz': [{
                    'name': 'Elvis'
                }],
            }]
        )

    def test_edgeql_select_computable_29(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"possibly an empty set returned by an expression for "
                r"a computed link 'foo' declared as 'required'",
                _position=78):
            self.con.query("""\
                SELECT Issue{
                    number,
                    required multi foo := .owner.todo,
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_30(self):
        self.assert_query_result(
            r"""
                WITH O := (SELECT {m := 10}),
                SELECT (O {m}, O.m);
            """,
            [
                [{'m': 10}, 10],
            ]
        )

    def test_edgeql_select_computable_31(self):
        self.assert_query_result(
            r"""
                WITH O := (SELECT {multi m := 10}),
                SELECT (O {m});
            """,
            [
                {'m': [10]},
            ]
        )

    def test_edgeql_select_computable_32(self):
        self.assert_query_result(
            r"""
            SELECT _ := (User {x := .name}.x, (SELECT User.name)) ORDER BY _;
            """,
            [
                ['Elvis', 'Elvis'],
                ['Yury', 'Yury'],
            ]
        )

        self.assert_query_result(
            r"""
            SELECT _ := ((SELECT User.name), User {x := .name}.x) ORDER BY _;
            """,
            [
                ['Elvis', 'Elvis'],
                ['Yury', 'Yury'],
            ]
        )

        self.assert_query_result(
            r"""
            SELECT _ := ((SELECT User.name), (User {x := .name},).0.x)
            ORDER BY _;
            """,
            [
                ['Elvis', 'Elvis'],
                ['Yury', 'Yury'],
            ]
        )

    def test_edgeql_select_computable_33(self):
        self.assert_query_result(
            r"""
            SELECT User {name, todo_ids := .todo.id} FILTER .name = 'Elvis';
            """,
            [
                {'name': 'Elvis', 'todo_ids': [str, str]},
            ]
        )

        self.assert_query_result(
            r"""
            WITH Z := (SELECT User {
                asdf := (SELECT .todo ORDER BY .number LIMIT 1)})
            SELECT Z {name, asdf_id := .asdf.id} FILTER .name = 'Elvis';
            """,
            [
                {'name': 'Elvis', 'asdf_id': str},
            ]
        )

    def test_edgeql_select_computable_34(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"possibly not a distinct set returned by an expression for "
            r"a computed link 'foo'",
            _position=78,
        ):
            self.con.query("""\
                SELECT Issue{
                    number,
                    foo := .owner.todo UNION .owner.todo,
                }
                FILTER Issue.number = '1';
            """)

    def test_edgeql_select_computable_35(self):
        # allow computed __type__ field
        self.assert_query_result(
            """
            SELECT Issue {
                number,
                __type__ := (select Issue.__type__ { name }),
            }
            FILTER .number = '3'
            """,
            [
                {'number': '3', '__type__': {'name': 'default::Issue'}},
            ],
        )

    def test_edgeql_select_match_01(self):
        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name LIKE '%Edge%'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '2'}],
        )

    def test_edgeql_select_match_02(self):
        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT LIKE '%Edge%'
            ORDER BY Issue.number;
            """,
            [{'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_match_03(self):
        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name ILIKE '%re%'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

    def test_edgeql_select_match_04(self):
        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%edgedb'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%EdgeDB'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Issue {number}
            FILTER
                Issue.name NOT ILIKE '%re%'
            ORDER BY Issue.number;
            """,
            [],
        )

    def test_edgeql_select_match_07(self):
        self.assert_query_result(
            r"""
            SELECT
                Text {body}
            FILTER
                re_test('ed', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Text {body}
            FILTER
                re_test('eD', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Initial public release of EdgeDB.'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Text {body}
            FILTER
                re_test(r'ed([S\s]|$)', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}]
        )

    def test_edgeql_select_match_08(self):
        self.assert_query_result(
            r"""
            SELECT
                Text {body}
            FILTER
                re_test('(?i)ed', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Text {body}
            FILTER
                re_test('(?i)eD', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'Initial public release of EdgeDB.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

        self.assert_query_result(
            r"""
            SELECT
                Text {body}
            FILTER
                re_test(r'(?i)ed([S\s]|$)', Text.body)
            ORDER BY Text.body;
            """,
            [{'body': 'EdgeDB needs to happen soon.'},
             {'body': 'Fix regression introduced by lexer tweak.'},
             {'body': 'We need to be able to render data in tabular format.'}],
        )

    def test_edgeql_select_type_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number,
                    __type__: {
                        name
                    }
                }
            FILTER
                Issue.number = '1';
            ''',
            [{
                'number': '1',
                '__type__': {'name': 'default::Issue'},
            }],
        )

    def test_edgeql_select_type_02(self):
        self.assert_query_result(
            r'''
            SELECT User.__type__.name LIMIT 1;
            ''',
            ['default::User']
        )

    def test_edgeql_select_type_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'invalid property reference'):
            self.con.query(r'''
                SELECT User.name.__type__.name LIMIT 1;
            ''')

    def test_edgeql_select_type_04(self):
        # Make sure that the __type__ attribute gets the same object
        # as a direct schema::ObjectType query. As long as this is true,
        # we can test the schema separately without any other data.
        res = self.con.query_single(r'''
            SELECT User {
                __type__: {
                    name,
                    id,
                }
            } LIMIT 1;
        ''')

        self.assert_query_result(
            r'''
            WITH MODULE schema
            SELECT `ObjectType` {
                name,
                id,
            } FILTER `ObjectType`.name = 'default::User';
            ''',
            [{
                'name': res.__type__.name,
                'id': str(res.__type__.id),
            }]
        )

    def test_edgeql_select_type_05(self):
        self.assert_query_result(
            r'''
            SELECT User.__type__ { name };
            ''',
            [{
                'name': 'default::User'
            }]
        )

    # @test.not_implemented('recursive queries are not implemented')
    def test_edgeql_select_recursive_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number,
                    related_to: {
                        number,
                    },
                }
            FILTER
                Issue.number = '2';
            ''',
            [{
                'number': '3',
                'related_to': [{
                    'number': '2',
                }]
            }],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number,
                    related_to *1
                }
            FILTER
                Issue.number = '2';
            ''',
            [{
                'number': '3',
                'related_to': [{
                    'number': '2',
                }]
            }],
        )

    def test_edgeql_select_limit_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 3;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 2 LIMIT 3;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_limit_02(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 1 + 1;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT 6 // 2;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET 4 - 2 LIMIT 5 * 2 - 7;
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_limit_03(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(Status));
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            LIMIT (SELECT count(Status) + 1);
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {number}
            ORDER BY Issue.number
            OFFSET (SELECT count(Status))
            LIMIT (SELECT count(Priority) + 1);
            ''',
            [{'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_limit_04(self):
        self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    owner_of := (
                        SELECT User.<owner[IS Issue] {
                            number
                        } ORDER BY .number
                        LIMIT 1
                    )
                }
            ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': {'number': '1'},
                },
                {
                    'name': 'Yury',
                    'owner_of': {'number': '2'},
                }
            ]
        )

    def test_edgeql_select_limit_05(self):
        self.assert_query_result(
            r'''
            SELECT
                User {
                    name,
                    owner_of := (
                        SELECT User.<owner[IS Issue] {
                            number
                        } ORDER BY .number
                        LIMIT len(User.name) - 3
                    )
                }
            ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': [{'number': '1'}, {'number': '4'}],
                },
                {
                    'name': 'Yury',
                    'owner_of': [{'number': '2'}],
                }
            ]
        )

    def test_edgeql_select_limit_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            self.con.query("""
                SELECT
                    User { name }
                LIMIT <int64>User.<owner[IS Issue].number;
            """)

    def test_edgeql_select_limit_07(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            self.con.query("""
                SELECT
                    User { name }
                OFFSET <int64>User.<owner[IS Issue].number;
            """)

    def test_edgeql_select_limit_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not resolve partial path'):

            self.con.query("""
                SELECT
                    User { name }
                LIMIT <int64>.<owner[IS Issue].number;
            """)

    def test_edgeql_select_limit_09(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'could not resolve partial path'):

            self.con.query("""
                SELECT
                    User { name }
                OFFSET <int64>.<owner[IS Issue].number;
            """)

    def test_edgeql_select_limit_10(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'LIMIT must not be negative'):

            self.con.query("""
                SELECT 1 LIMIT -1
            """)

    def test_edgeql_select_offset_01(self):
        with self.assertRaisesRegex(
                edgedb.InvalidValueError,
                r'OFFSET must not be negative'):

            self.con.query("""
                SELECT 1 OFFSET -1
            """)

    def test_edgeql_select_polymorphic_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Text {body}
            ORDER BY Text.body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'Rewriting everything.'},
                {'body': 'We need to be able to render data '
                         'in tabular format.'}
            ],
        )

        self.assert_query_result(
            r'''
            SELECT
                Text {
                    [IS Issue].name,
                    body,
                }
            ORDER BY Text.body;
            ''',
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
                {'body': 'We need to be able to render data in '
                         'tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ]
        )

    def test_edgeql_select_polymorphic_02(self):
        self.assert_query_result(
            r'''
            SELECT User{
                name,
                owner_of := User.<owner[IS LogEntry] {
                    body
                },
            } FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'owner_of': [
                    {'body': 'Rewriting everything.'}
                ],
            }],
        )

    def test_edgeql_select_polymorphic_03(self):
        self.assert_query_result(
            r'''
            SELECT User{
                name,
                owner_of := (
                    SELECT User.<owner[IS Issue] {
                        number
                    } FILTER <int64>(.number) < 3
                ),
            } FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'owner_of': [
                    {'number': '1'},
                ],
            }],
        )

    def test_edgeql_select_polymorphic_04(self):
        # Since using a polymorphic shape element means that sometimes
        # that element may be empty, it is prohibited to access
        # protected property such as `id` on it as that would be
        # equivalent to re-writing it.
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot access id on a polymorphic shape element'):
            self.con.query(r'''
                SELECT User {
                    [IS Named].id,
                };
            ''')

    def test_edgeql_select_polymorphic_06(self):
        self.assert_query_result(
            r'''
            SELECT Object[IS Status].name;
            ''',
            {
                'Closed',
                'Open',
            },
        )

        self.assert_query_result(
            r'''
            SELECT Object[IS Priority].name;
            ''',
            {
                'High',
                'Low',
            },
        )

        self.assert_query_result(
            r'''
            SELECT Object[IS Status].name ?? Object[IS Priority].name;
            ''',
            {
                'Closed',
                'High',
                'Low',
                'Open',
            },
        )

    # @test.not_implemented('type expressions are not implemented')
    def test_edgeql_select_polymorphic_07(self):
        self.assert_query_result(
            r'''
            SELECT Object[IS Status | Priority].name;
            # the above should be equivalent to this:
            # SELECT Object[IS Status].name ?? Object[IS Priority].name;
            ''',
            {
                'Closed',
                'High',
                'Low',
                'Open',
            },
        )

    # @test.not_implemented('type expressions are not implemented')
    def test_edgeql_select_polymorphic_08(self):
        self.assert_query_result(
            r'''
            SELECT Object {
                [IS Status | Priority].name,
            } ORDER BY .name;
            ''',
            [
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': 'Closed'},
                {'name': 'High'},
                {'name': 'Low'},
                {'name': 'Open'}
            ],
        )

        self.assert_query_result(
            r'''
            # the above should be equivalent to this:
            SELECT Object {
                name := Object[IS Status].name ?? Object[IS Priority].name,
            } ORDER BY .name;
            ''',
            [
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': None},
                {'name': 'Closed'},
                {'name': 'High'},
                {'name': 'Low'},
                {'name': 'Open'}
            ],
        )

    # @test.xerror(
    #     "Known collation issue on Heroku Postgres",
    #     unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    # )
    def test_edgeql_select_polymorphic_09(self):
        # Test simultaneous type intersection on source and target
        # of a shape element.
        self.assert_query_result(
            r'''
            SELECT Named {
                name,
                [IS Issue].references[IS File]: {
                    name
                }
            }
            FILTER .name ILIKE '%edgedb%'
            ORDER BY .name;
            ''',
            [
                {
                    'name': 'Improve EdgeDB repl output rendering.',
                    'references': [{'name': 'screenshot.png'}],
                },
                {
                    'name': 'Release EdgeDB',
                    'references': [],
                },
                {
                    'name': 'edgedb.com',
                    'references': [],
                },
            ],
        )

    def test_edgeql_select_polymorphic_10(self):
        self.assert_query_result(
            r'''
            SELECT
                count(Object[IS Named][IS Text])
                != count(Object[IS Text]);
            ''',
            [True]
        )

        self.assert_query_result(
            r'''
            SELECT
                count(User.<owner[IS Named][IS Text])
                != count(User.<owner[IS Text]);
            ''',
            [True]
        )

    def test_edgeql_select_polymorphic_11(self):
        self.assert_query_result(
            r'''
            WITH
                Texts := Text {
                    [IS LogEntry].spent_time
                }
            SELECT
                _ := Texts.spent_time
            ORDER BY
                _
            ''',
            [50000]
        )

    def test_edgeql_select_polymorphic_12(self):
        self.con.execute('''
            INSERT Issue {
                name := 'Polymorphic Test 12',
                body := 'foo',
                number := '333',
                owner := (SELECT User FILTER .name = 'Elvis'),
                status := (SELECT Status FILTER .name = 'Open'),
                references := (
                    INSERT Publication {
                        title := 'Introduction to EdgeDB',
                        authors := (
                            FOR v IN enumerate({'Yury', 'Elvis'})
                            UNION (
                                SELECT User { @list_order := v.0 }
                                FILTER .name = v.1
                            )
                        ),
                    }
                )
            }
        ''')

        self.assert_query_result(
            r'''
            SELECT Issue {
                references: {
                    [IS Publication].authors: {
                        name
                    } ORDER BY @list_order
                }
            }
            FILTER .number = '333'
            ''',
            [
                {
                    'references': [{
                        'authors': [{
                            'name': 'Yury'
                        }, {
                            'name': 'Elvis'
                        }]
                    }],
                },
            ],
        )

    def test_edgeql_select_polymorphic_13(self):
        self.con.execute('''
            INSERT Issue {
                name := 'Polymorphic Test 13',
                body := 'foo',
                number := '333',
                owner := (SELECT User FILTER .name = 'Elvis'),
                status := (SELECT Status FILTER .name = 'Open'),
                references := (
                    FOR v IN {
                        ('Introduction to EdgeDB Part Deux', 2),
                        ('Introduction to EdgeDB', 1),
                    }
                    UNION (
                        INSERT Publication {
                            title := v.0,
                            @list_order := v.1,
                        }
                    )
                )
            }
        ''')

        self.assert_query_result(
            r'''
            SELECT Issue {
                references[IS Publication]: {
                    title
                } ORDER BY @list_order
            }
            FILTER .name = 'Polymorphic Test 13'
            ''',
            [
                {
                    'references': [{
                        'title': 'Introduction to EdgeDB',
                    }, {
                        'title': 'Introduction to EdgeDB Part Deux',
                    }],
                },
            ],
        )

    def test_edgeql_select_id_01(self):
        # allow assigning id to a computed (#4781)
        self.con.query('SELECT schema::Type { XYZ := .id};')

    def test_edgeql_select_reverse_link_01(self):
        self.assert_query_result(
            r'''
            SELECT
                (INTROSPECT TYPEOF User.<owner).name;
            ''',
            ['std::BaseObject']
        )

    def test_edgeql_select_reverse_link_02(self):
        self.assert_query_result(
            r'''
            SELECT
                User.<owner[IS Issue]@since
            ''',
            ['2018-01-01T00:00:00+00:00'],
        )

        self.assert_query_result(
            r'''
            SELECT
                User.<owner[IS Named]@since
            ''',
            ['2018-01-01T00:00:00+00:00'],
        )

    def test_edgeql_select_reverse_link_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            "no property 'since'",
        ):
            # Property "since" is only defined on the
            # Issue.owner link, whereas the Text intersection
            # resolves to a union of links Issue.owner, LogEntry.owner,
            # and Comment.owner.
            self.con.execute(
                r'''
                SELECT
                    User.<owner[IS Text]@since
                ''',
            )

    def test_edgeql_select_reverse_link_04(self):
        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            "no link or property 'number'",
        ):
            self.con.execute(
                r'''
                SELECT
                    Issue.<related_to.number
                ''',
            )

    def test_edgeql_select_reverse_link_05(self):
        self.assert_query_result(
            r'''
            SELECT (User.<owner[IS Comment], User.<owner[IS Issue]);
            ''',
            [],
        )

    def test_edgeql_select_empty_intersection_property(self):
        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            "property 'since' does not exist.*no 'owner' links*",
        ):
            # Test the situation when the target type intersection
            # results in no candidate links to resolve the
            # property on.
            self.con.execute(
                r'''
                SELECT
                    User.<owner[IS Status]@since
                ''',
            )

    def test_edgeql_select_nested_redefined_link(self):
        self.assert_query_result(
            '''
                SELECT (SELECT (SELECT Issue { watchers: {name} }).watchers);
            ''',
            tb.bag([
                {'name': 'Elvis'},
                {'name': 'Yury'},
            ]),
        )

    def test_edgeql_select_tvariant_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue{
                number,
                related_to: {
                    number
                } FILTER Issue.related_to.owner = Issue.owner,
            } ORDER BY Issue.number;
            ''',
            [
                {
                    'number': '1',
                    'related_to': []
                },
                {
                    'number': '2',
                    'related_to': []
                },
                {
                    'number': '3',
                    'related_to': [
                        {'number': '2'}
                    ]
                },
                {
                    'number': '4',
                    'related_to': []
                }
            ],
        )

    def test_edgeql_select_tvariant_02(self):
        self.assert_query_result(
            r'''
            SELECT User{
                name,
                owner_of := (
                    SELECT User.<owner[IS Issue] {
                        number
                    } FILTER EXISTS .related_to
                ),
            } ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': [{
                        'number': '4'
                    }]
                }, {
                    'name': 'Yury',
                    'owner_of': [{
                        'number': '3'
                    }]
                }
            ],
        )

    def test_edgeql_select_tvariant_03(self):
        self.assert_query_result(
            r'''
            SELECT User{
                name,
                owner_of := (
                    SELECT User.<owner[IS Issue] {
                        number
                    } ORDER BY .number DESC
                ),
            } ORDER BY User.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'owner_of': [{
                        'number': '4'
                    }, {
                        'number': '1'
                    }]
                }, {
                    'name': 'Yury',
                    'owner_of': [{
                        'number': '3'
                    }, {
                        'number': '2'
                    }]
                }
            ],
        )

    def test_edgeql_select_tvariant_04(self):
        self.assert_query_result(
            r"""
            WITH
                L := LogEntry   # there happens to only be 1 entry
            SELECT
                # define a type variant that assigns a log to every Issue
                Issue {
                    tsl := (Issue.time_spent_log ?? L)
                }.tsl {
                    body
                };
            """,
            [
                # no duplicates are possible, because the expression
                # is a path pointing to an object
                {'body': 'Rewriting everything.'},
            ],
        )

    def test_edgeql_select_tvariant_05(self):
        self.assert_query_result(
            r"""
            SELECT Issue.owner {
                name,
                # this path extends `Issue.owner` from top scope
                foo := Issue.owner.<owner[IS Issue]{
                    number,
                    # this path *also* extends `Issue.owner` from top scope
                    bar := Issue.owner.name
                }
            };
            """,
            tb.bag([
                {
                    'name': 'Elvis',
                    'foo': [
                        {'bar': 'Elvis', 'number': '1'},
                        {'bar': 'Elvis', 'number': '4'}
                    ],
                },
                {
                    'name': 'Yury',
                    'foo': [
                        {'bar': 'Yury', 'number': '2'},
                        {'bar': 'Yury', 'number': '3'}
                    ],
                },
            ]),
        )

    def test_edgeql_select_tvariant_06(self):
        self.assert_query_result(
            r"""
            SELECT User {
                name,
                foo := (
                    SELECT (
                        SELECT Status
                        FILTER Status.name = 'Open'
                    ).name
                )
            } FILTER User.name = 'Elvis';
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': 'Open',
                },
            ],
        )

    def test_edgeql_select_tvariant_07(self):
        self.assert_query_result(
            r"""
            # semantically identical to the previous test
            SELECT User {
                name,
                foo := {
                    (
                        SELECT Status
                        FILTER Status.name = 'Open'
                    ).name
                }
            } FILTER User.name = 'Elvis';
            # FIXME: please also fix the error message to be less
            # arcane with some sort of reference to where things go
            # wrong in the query
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': 'Open',
                },
            ],
        )

    def test_edgeql_select_tvariant_08(self):
        self.assert_query_result(
            r"""
            # semantically similar to previous test, but involving
            # schema (since schema often has special handling)
            SELECT User {
                name,
                foo := {
                    (
                        SELECT schema::ObjectType
                        FILTER schema::ObjectType.name = 'default::User'
                    ).name
                }
            } FILTER User.name = 'Elvis';
            """,
            [
                {
                    'name': 'Elvis',
                    'foo': ['default::User'],
                },
            ],
        )

    def test_edgeql_select_tvariant_09(self):
        self.assert_query_result(
            r"""
                SELECT
                    (((SELECT Issue {
                        x := .number ++ "!"
                    }), Issue).0.x ++ (SELECT Issue.number));
            """,
            {"1!1", "2!2", "3!3", "4!4"},
        )

    def test_edgeql_select_tvariant_bad_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine property 'name' of object type 'default::User' "
            "as scalar type 'std::int64'",
            _position=59,
        ):
            self.con.execute("""
                SELECT User {
                    name := 1
                }
            """)

    def test_edgeql_select_tvariant_bad_02(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine property 'name' of object type 'default::User' "
            "as object type 'default::Issue'",
            _position=59,
        ):
            self.con.execute("""
                SELECT User {
                    name := Issue
                }
            """)

    def test_edgeql_select_tvariant_bad_03(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine link 'related_to' of object type "
            "'default::Issue' as scalar type 'std::int64'",
            _position=66,
        ):
            self.con.execute("""
                SELECT Issue {
                    related_to := 1
                }
            """)

    def test_edgeql_select_tvariant_bad_04(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine link 'related_to' of object type "
            "'default::Issue' as object type 'default::Text'",
            _position=66,
        ):
            self.con.execute("""
                SELECT Issue {
                    related_to := Text
                }
            """)

    def test_edgeql_select_tvariant_bad_05(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "possibly more than one element returned by an expression for a "
            "computed link 'priority' declared as 'single'",
            _position=52,
        ):
            self.con.execute("""
                SELECT Issue {
                    priority := Priority
                }
            """)

    def test_edgeql_select_tvariant_bad_06(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine the cardinality of link 'owner': it is defined "
            "as 'single' in the base object type 'default::Issue'",
            _position=67,
        ):
            self.con.execute("""
                SELECT Issue {
                    multi owner := User
                }
            """)

    def test_edgeql_select_tvariant_bad_07(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine the cardinality of link 'related_to': it is "
            "defined as 'multi' in the base object type 'default::Issue'",
            _position=73,
        ):
            self.con.execute("""
                SELECT Issue {
                    single related_to := (SELECT Issue LIMIT 1)
                }
            """)

    def test_edgeql_select_tvariant_bad_08(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "possibly an empty set returned by an expression for a "
            "computed link 'owner' declared as 'required'",
            _position=52,
        ):
            self.con.execute("""
                SELECT Issue {
                    owner := (SELECT User LIMIT 1)
                }
            """)

    def test_edgeql_select_tvariant_bad_09(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot redefine link 'status' as optional: it is "
            "defined as required in the base object type 'default::Issue'",
            _position=71,
        ):
            self.con.execute("""
                SELECT Issue {
                    optional status := (SELECT Status FILTER .name = "Open")
                }
            """)

    def test_edgeql_select_instance_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Text {body}
            FILTER Text IS Comment
            ORDER BY Text.body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
            ],
        )

    def test_edgeql_select_instance_02(self):
        self.assert_query_result(
            r'''
            SELECT
                Text {body}
            FILTER Text IS NOT Comment | Issue
            ORDER BY Text.body;
            ''',
            [
                {'body': 'Rewriting everything.'},
            ],
        )

    def test_edgeql_select_instance_03(self):
        self.assert_query_result(
            r'''
            SELECT
                Text {body}
            FILTER Text IS Issue AND Text[IS Issue].number = '1'
            ORDER BY Text.body;
            ''',
            [
                {'body': 'Initial public release of EdgeDB.'},
            ],
        )

    def test_edgeql_select_setops_01(self):
        self.assert_query_result(
            r"""
            SELECT
                (Issue UNION Comment) {
                    [IS Issue].name,  # name is not in the duck type
                    body  # body should appear in the duck type
                };
            """,
            tb.bag([
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.',
                 'name': 'Regression.'},
                {'body': 'Initial public release of EdgeDB.',
                 'name': 'Release EdgeDB'},
                {'body': 'Minor lexer tweaks.',
                 'name': 'Repl tweak.'},
                {'body': 'We need to be able to render data '
                         'in tabular format.',
                 'name': 'Improve EdgeDB repl output rendering.'}
            ]),
        )

    def test_edgeql_select_setops_02(self):
        self.assert_query_result(
            r'''
            WITH
                Obj := (SELECT Issue UNION Comment)
            SELECT Obj {
                [IS Issue].name,
                [IS Text].body
            };
            ''',
            tb.bag([
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.'}
            ]),
        )

        self.assert_query_result(
            r'''
            # XXX: I think we should be able to drop [IS Text] from
            # the query below.
            WITH
                Obj := (SELECT Issue UNION Comment)
            SELECT Obj[IS Text] { id, body }
            ORDER BY Obj[IS Text].body;
            ''',
            [
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'Minor lexer tweaks.'},
                {'body': 'We need to be able to render '
                         'data in tabular format.'}
            ],
        )

    def test_edgeql_select_setops_03(self):
        self.assert_query_result(
            r"""
            SELECT Issue {
                number,
                # open := 'yes' IF Issue.status.name = 'Open' ELSE 'no'
                # equivalent to
                open := (SELECT (
                    (SELECT 'yes' FILTER Issue.status.name = 'Open')
                    UNION
                    (SELECT 'no' FILTER NOT Issue.status.name = 'Open')
                ) LIMIT 1)
            }
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'open': 'yes',
            }, {
                'number': '2',
                'open': 'yes',
            }, {
                'number': '3',
                'open': 'no',
            }, {
                'number': '4',
                'open': 'no',
            }],
        )

    def test_edgeql_select_setops_04(self):
        self.assert_query_result(
            r"""
            # equivalent to ?=
            SELECT Issue {number}
            FILTER
                # Issue.priority.name ?= 'High'
                # equivalent to this via an if/else translation
                (SELECT Issue.priority.name = 'High'
                 FILTER EXISTS Issue.priority.name)
                UNION
                (SELECT EXISTS Issue.priority.name = TRUE
                 FILTER NOT EXISTS Issue.priority.name)
            ORDER BY Issue.number;
            """,
            [{'number': '2'}],
        )

    def test_edgeql_select_setops_05(self):
        self.assert_query_result(
            r"""
            # using DISTINCT on a UNION with overlapping sets of Objects
            SELECT _ := (
                DISTINCT ((
                    # Issue 1, 4
                    (SELECT User
                     FILTER User.name = 'Elvis').<owner[IS Issue]
                ) UNION (
                    # Issue 1
                    (SELECT User
                     FILTER User.name = 'Yury').<watchers[IS Issue]
                ) UNION (
                    # Issue 1, 4
                    SELECT Issue
                    FILTER NOT EXISTS Issue.priority
                ))
            ) { number }
            ORDER BY _.number;
            """,
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_setops_06(self):
        self.assert_query_result(
            r"""
            # using DISTINCT on a UNION with overlapping sets of Objects
            SELECT _ := count(DISTINCT ((
                # Issue 1, 4
                (SELECT User
                 FILTER User.name = 'Elvis').<owner[IS Issue]
            ) UNION (
                # Issue 1
                (SELECT User
                 FILTER User.name = 'Yury').<watchers[IS Issue]
            ) UNION (
                # Issue 1, 4
                SELECT Issue
                FILTER NOT EXISTS Issue.priority
            )));
            """,
            [2],
        )

    def test_edgeql_select_setops_07(self):
        self.assert_query_result(
            r"""
            # using UNION with overlapping sets of Objects
            SELECT _ := {  # equivalent to UNION for Objects
                # Issue 1, 4
                (
                    SELECT Issue
                    FILTER Issue.owner.name = 'Elvis'
                ), (
                    SELECT Issue
                    FILTER Issue.number = '1'
                )
            } { number }
            ORDER BY _.number;
            """,
            [{'number': '1'}, {'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_setops_08(self):
        self.assert_query_result(
            r"""
            # using implicit nested UNION with overlapping sets of Objects
            SELECT _ := {  # equivalent to UNION for Objects
                # Issue 1, 4
                (
                    SELECT Issue
                    FILTER Issue.owner.name = 'Elvis'
                ),
                {
                    (
                        # Issue 1, 4
                        (
                            SELECT User
                            FILTER User.name = 'Elvis'
                        ).<owner[IS Issue]
                    ) UNION (
                        # Issue 1
                        (
                            SELECT User
                            FILTER User.name = 'Yury'
                        ).<watchers[IS Issue]
                    ),
                    (
                        # Issue 1, 4
                        SELECT Issue
                        FILTER NOT EXISTS Issue.priority
                    )
                },
                (
                    SELECT Issue FILTER Issue.number = '1'
                )
            } { number }
            ORDER BY _.number;
            """,
            [
                {'number': '1'}, {'number': '1'}, {'number': '1'},
                {'number': '1'}, {'number': '1'},
                {'number': '4'}, {'number': '4'}, {'number': '4'},
            ],
        )

    def test_edgeql_select_setops_09(self):
        self.assert_query_result(
            r"""
            # same as above but with a DISTINCT
            SELECT _ := (DISTINCT {  # equivalent to UNION for Objects
                # Issue 1, 4
                (
                    SELECT Issue
                    FILTER Issue.owner.name = 'Elvis'
                ),
                {
                    (
                        # Issue 1, 4
                        (
                            SELECT User
                            FILTER User.name = 'Elvis'
                        ).<owner[IS Issue]
                    ) UNION (
                        # Issue 1
                        (
                            SELECT User
                            FILTER User.name = 'Yury'
                        ).<watchers[IS Issue]
                    ),
                    (
                        # Issue 1, 4
                        SELECT Issue
                        FILTER NOT EXISTS Issue.priority
                    )
                },
                (
                    SELECT Issue
                    FILTER Issue.number = '1'
                )
            }) { number }
            ORDER BY _.number;
            """,
            [
                {'number': '1'}, {'number': '4'},
            ],
        )

    def test_edgeql_select_setops_10(self):
        self.assert_query_result(
            r"""
            # using UNION in a FILTER
            SELECT _ := User{name}
            FILTER (
                (
                    SELECT User.<owner[IS Issue]
                ) UNION (
                    # this part should guarantee the filter is always true
                    SELECT Issue
                    FILTER Issue.number = '1'
                )
            ).number = '1'
            ORDER BY _.name;
            """,
            [{'name': 'Elvis'}, {'name': 'Yury'}],
        )

    def test_edgeql_select_setops_11(self):
        self.assert_query_result(
            r"""
            WITH
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (Issue.time_spent_log UNION L) {
                    body
                };
            """,
            [
                # duplicates are allowed in a plain UNION
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
            ],
        )

    def test_edgeql_select_setops_12(self):
        self.assert_query_result(
            r"""
            WITH
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (DISTINCT (Issue.time_spent_log UNION L)) {
                    body
                };
            """,
            [
                # no duplicates are allowed due to DISTINCT
                {'body': 'Rewriting everything.'},
            ],
        )

    def test_edgeql_select_setops_13a(self):
        self.assert_query_result(
            r"""
            WITH
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (Issue.time_spent_log UNION L, Issue).0 {
                    body
                };
            """,
            [
                # not only do we expect duplicates, but we actually
                # expect 5 entries here:
                # - 1 for the actual `time_spent_log' links from Issue
                # - 4 from the UNION for each Issue.time_spent_log
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
            ],
        )

    def test_edgeql_select_setops_13b(self):
        # This should be equivalent to the above test, but actually we
        # end up deduplicating.
        self.assert_query_result(
            r"""
            WITH
                L := LogEntry  # there happens to only be 1 entry
            SELECT
                (SELECT (Issue.time_spent_log UNION L, Issue)).0 {
                    body
                };
            """,
            [
                # not only do we expect duplicates, but we actually
                # expect 5 entries here:
                # - 1 for the actual `time_spent_log' links from Issue
                # - 4 from the UNION for each Issue.time_spent_log
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Rewriting everything.'},
            ],
        )

    def test_edgeql_select_setops_14(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "it is illegal to create a type union that causes "
            "a computed property 'number' to mix with other "
            "versions of the same property 'number'"
        ):
            self.con.execute(
                r"""
                SELECT {
                    Issue{number := 'foo'}, Issue
                }.number;
                """
            )

    def test_edgeql_select_setops_15(self):
        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "it is illegal to create a type union that causes "
            "a computed property 'number' to mix with other "
            "versions of the same property 'number'"
        ):
            self.con.execute(
                r"""
                WITH
                    I := Issue{number := 'foo'}
                SELECT {I, Issue}.number;
                """
            )

    def test_edgeql_select_setops_16(self):
        self.assert_query_result(
            r"""
            # Named doesn't have a property number.
            SELECT Issue[IS Named].number;
            """,
            {'1', '2', '3', '4'},
        )

    def test_edgeql_select_setops_17(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r"has no link or property 'number'"):
            self.con.query(r"""
                # UNION between Issue and empty set Named should be
                # duck-typed to be effectively equivalent to Issue[IS Named].
                SELECT (Issue UNION <Named>{}).number;
            """)

    def test_edgeql_select_setops_18(self):
        self.assert_query_result(
            r"""
            # UNION between Issue and empty set Named should be
            # duck-typed to be effectively equivalent to Issue[IS Named].
            SELECT (Issue UNION <Named>{}).name;
            """,
            {
                'Release EdgeDB',
                'Improve EdgeDB repl output rendering.',
                'Repl tweak.',
                'Regression.',
            },
        )

    def test_edgeql_select_setops_19(self):
        self.assert_query_result(
            r"""
            # UNION between Issue and empty set Issue should be
            # duck-typed to be effectively equivalent to Issue[IS
            # Issue], which is just an Issue.
            SELECT (Issue UNION <Issue>{}).name;
            """,
            {
                'Release EdgeDB',
                'Improve EdgeDB repl output rendering.',
                'Repl tweak.',
                'Regression.',
            },
        )

        self.assert_query_result(
            r"""
            SELECT (Issue UNION <Issue>{}).number;
            """,
            {'1', '2', '3', '4'},
        )

    def test_edgeql_select_setops_20(self):
        res = self.con.query(r'''
            SELECT (
                {(SELECT Issue.time_spent_log.body FILTER false), 'asdf'},
                Issue,
            )
        ''')
        self.assertEqual(len(res), 4)
        for row in res:
            self.assertNotEqual(row[1].id, None)

    def test_edgeql_select_setops_21(self):
        res = self.con.query(r'''
            SELECT (
                'oh no' ?? (SELECT Issue.time_spent_log.body FILTER false),
                Issue,
            )
        ''')
        self.assertEqual(len(res), 4)
        for row in res:
            self.assertNotEqual(row[1].id, None)

    def test_edgeql_select_setops_22(self):
        res = self.con.query(r'''
            SELECT (
                (SELECT Issue.time_spent_log.body FILTER false)
                 if false else 'asdf',
                Issue,
            )
        ''')
        self.assertEqual(len(res), 4)
        for row in res:
            self.assertNotEqual(row[1].id, None)

    def test_edgeql_select_setops_23(self):
        self.assert_query_result(
            r"""
            WITH X := (insert Publication { title := "x" }),
                 Y := (insert Publication { title := "y" }),
                 foo := X union Y,
            select foo { title1 };
            """,
            tb.bag([
                {'title1': 'x'},
                {'title1': 'y'},
            ])
        )

        self.assert_query_result(
            r"""
            WITH X := (select Publication filter .title = 'x'),
                 Y := (select Publication filter .title = 'y'),
                 foo := X union Y,
            select foo { title1 };
            """,
            tb.bag([
                {'title1': 'x'},
                {'title1': 'y'},
            ])
        )

        self.assert_query_result(
            r"""
            SELECT (Issue UNION <Issue>{}).number;
            """,
            {'1', '2', '3', '4'},
        )

    def test_edgeql_select_setops_24(self):
        # Establish that EXCEPT and INTERSECT filter out the objects we'd
        # expect.
        self.assert_query_result(
            r"""
            with A := Owned except {LogEntry, Comment}
            select all(A in Issue) and all(Issue in A)
            """,
            {
                True
            },
        )

        self.assert_query_result(
            r"""
            with A := Owned intersect Issue
            select all(A in Owned[is Issue]) and all(Owned[is Issue] in A)
            """,
            {
                True
            },
        )

    def test_edgeql_select_setops_25(self):
        # Establish that EXCEPT and INTERSECT filter out the objects we'd
        # expect.
        self.assert_query_result(
            r"""
            with
              A := (select Issue filter .name ilike '%edgedb%'),
              B := (select Issue filter .owner.name = 'Elvis')
            select (B except A) {name};
            """,
            [
                {'name': 'Regression.'},
            ],
        )

        self.assert_query_result(
            r"""
            with
              A := (select Issue filter .name ilike '%edgedb%'),
              B := (select Issue filter .owner.name = 'Elvis')
            select (B intersect A) {name};
            """,
            [
                {'name': 'Release EdgeDB'},
            ],
        )

    def test_edgeql_select_setops_26(self):
        # Establish that EXCEPT and INTERSECT filter out the objects we'd
        # expect.
        self.assert_query_result(
            r"""
            select (Issue except Named);
            """,
            [],
        )

        self.assert_query_result(
            r"""
            select (Issue intersect <Named>{});
            """,
            [],
        )

    def test_edgeql_select_setops_27(self):
        self.assert_query_result(
            r"""
            with
                A := (select Issue filter .name not ilike '%edgedb%').body
            select _ :=
                str_lower(array_unpack(str_split(A, ' ')))
                except
                {'minor', 'fix', 'lexer'}
            order by _
            """,
            [
                "by",
                "introduced",
                "lexer",
                "regression",
                "tweak.",
                "tweaks.",
            ],
        )

        self.assert_query_result(
            r"""
            with A := (select Issue filter .name not ilike '%edgedb%')
            select _ :=
              str_lower(array_unpack(str_split(A.body, ' ')))
              except
              str_lower(array_unpack(str_split(A.name, ' ')))
            order by _
            """,
            [
                "by",
                "fix",
                "introduced",
                "lexer",
                "lexer",
                "minor",
                "regression",
                "tweaks.",
            ],
        )

    def test_edgeql_select_setops_28(self):
        self.assert_query_result(
            r"""
            select _ :=
              len(array_unpack(str_split(Issue.body, ' ')))
              intersect {1, 2, 2, 3, 3, 3, 7, 7, 7, 7, 7, 7, 7}
            order by _
            """,
            [
                2, 2, 3, 7, 7, 7, 7, 7, 7
            ],
        )

        self.assert_query_result(
            r"""
            select _ :=
              str_lower(array_unpack(str_split(Issue.name, ' ')))
              except
              str_lower(array_unpack(str_split(Issue.body, ' ')))
            order by _
            """,
            [
                "edgedb",
                "edgedb",
                "improve",
                "output",
                "regression.",
                "rendering.",
                "repl",
                "repl",
            ],
        )

    def test_edgeql_select_order_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC EMPTY LAST THEN Issue.name;
            ''',
            [
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
            ],
        )

        self.assert_query_result(
            r'''
            SELECT Issue {name}
            ORDER BY Issue.priority.name ASC EMPTY FIRST THEN Issue.name;
            ''',
            [
                {'name': 'Regression.'},
                {'name': 'Release EdgeDB'},
                {'name': 'Improve EdgeDB repl output rendering.'},
                {'name': 'Repl tweak.'},
            ]
        )

    def test_edgeql_select_order_02(self):
        self.assert_query_result(
            r'''
            SELECT Text {body}
            ORDER BY len(Text.body) DESC;
            ''',
            [
                {'body': 'We need to be able to render '
                         'data in tabular format.'},
                {'body': 'Fix regression introduced by lexer tweak.'},
                {'body': 'Initial public release of EdgeDB.'},
                {'body': 'EdgeDB needs to happen soon.'},
                {'body': 'Rewriting everything.'},
                {'body': 'Minor lexer tweaks.'}
            ]
        )

    def test_edgeql_select_order_03(self):
        self.assert_query_result(
            r'''
            SELECT User {name}
            ORDER BY (
                SELECT sum(<int64>User.<watchers[IS Issue].number)
            );
            ''',
            [
                {'name': 'Yury'},
                {'name': 'Elvis'},
            ]
        )

    def test_edgeql_select_order_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'possibly more than one element returned by an expression '
                r'where only singletons are allowed'):

            self.con.query("""
                SELECT
                    User { name }
                ORDER BY User.<owner[IS Issue].number;
            """)

    def test_edgeql_select_where_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment with non-empty body
            FILTER Issue.owner.<owner[IS Comment].body != ''
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_where_02(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            FILTER Issue.owner.<owner[IS Comment].issue = Issue;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_where_03(self):
        self.assert_query_result(
            r'''
            SELECT Issue{
                name,
                number,
                owner: {
                    name
                },
                status: {
                    name
                }
            } FILTER len(Issue.status.name) = 4
            ORDER BY Issue.number;
            ''',
            [{
                'owner': {'name': 'Elvis'},
                'status': {'name': 'Open'},
                'name': 'Release EdgeDB',
                'number': '1'
            }, {
                'owner': {'name': 'Yury'},
                'status': {'name': 'Open'},
                'name': 'Improve EdgeDB repl output rendering.',
                'number': '2'
            }],
        )

    def test_edgeql_select_func_01(self):
        self.assert_query_result(
            r'''
            SELECT std::len(User.name) ORDER BY User.name;
            ''',
            [5, 4],
        )

        self.assert_query_result(
            r'''
            SELECT std::sum(<std::int64>Issue.number);
            ''',
            [10]
        )

    def test_edgeql_select_func_05(self):
        self.con.execute(r'''
            CREATE FUNCTION concat1(VARIADIC s: anytype) -> std::str
                USING SQL FUNCTION 'concat';
        ''')

        self.assert_query_result(
            r'''
            SELECT schema::Function {
                params: {
                    num,
                    kind,
                    type: {
                        name
                    }
                }
            } FILTER schema::Function.name = 'default::concat1';
            ''',
            [{'params': [
                {
                    'num': 0,
                    'kind': 'VariadicParam',
                    'type': {
                        'name': 'array<anytype>'
                    }
                }
            ]}]
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):
            with self.con.transaction():
                self.con.query(
                    "SELECT concat1('aaa', 'bbb', 2);")

        self.con.execute(r'''
            DROP FUNCTION concat1(VARIADIC s: anytype);
        ''')

    def test_edgeql_select_func_06(self):
        self.con.execute(r'''
            CREATE FUNCTION concat2(VARIADIC s: std::str) -> std::str {
                SET impl_is_strict := false;
                USING SQL FUNCTION 'concat';
            }
        ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):
            self.con.execute(r'SELECT concat2(123);')

    def test_edgeql_select_func_07(self):
        self.con.execute(r'''
            CREATE FUNCTION concat3(sep: OPTIONAL std::str,
                                          VARIADIC s: std::str)
                    -> std::str
                USING EdgeQL $$
                    # poor man concat
                    SELECT (array_get(s, 0) ?? '') ++
                           (sep ?? '::') ++
                           (array_get(s, 1) ?? '')
                $$;
        ''')

        self.assert_query_result(
            r'''
            SELECT schema::Function {
                params: {
                    num,
                    name,
                    kind,
                    type: {
                        name,
                        [IS schema::Array].element_type: {
                            name
                        }
                    },
                    typemod
                } ORDER BY .num ASC,
                return_type: {
                    name
                },
                return_typemod
            } FILTER schema::Function.name = 'default::concat3';
            ''',
            [{
                'params': [
                    {
                        'num': 0,
                        'name': 'sep',
                        'kind': 'PositionalParam',
                        'type': {
                            'name': 'std::str',
                            'element_type': None
                        },
                        'typemod': 'OptionalType'
                    },
                    {
                        'num': 1,
                        'name': 's',
                        'kind': 'VariadicParam',
                        'type': {
                            'name': 'array<std::str>',
                            'element_type': {'name': 'std::str'}
                        },
                        'typemod': 'SingletonType'
                    }
                ],
                'return_type': {
                    'name': 'std::str'
                },
                'return_typemod': 'SingletonType'
            }]
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):
            with self.con.transaction():
                self.con.query(r'SELECT concat3(123);')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function .+ does not exist'):
            with self.con.transaction():
                self.con.query(r'SELECT concat3("a", 123);')

        self.assert_query_result(
            r'''
            SELECT concat3('|', '1', '2');
            ''',
            ['1|2'],
        )

        self.con.execute(r'''
            DROP FUNCTION concat3(sep: std::str, VARIADIC s: std::str);
        ''')

    def test_edgeql_select_func_08(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'function "sum\(arg0: std::str\)" does not exist'):
            with self.con.transaction():
                self.con.query(r"with x := 'a', select sum(x);")

    def test_edgeql_select_exists_01(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number
                }
            FILTER
                EXISTS Issue.time_estimate
            ORDER BY
                Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_exists_02(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS (Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_03(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number
                }
            FILTER
                NOT EXISTS (SELECT Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_04(self):
        self.assert_query_result(
            r'''
            SELECT
                Issue {
                    number
                }
            FILTER
                EXISTS (Issue.<issue[IS Comment])
            ORDER BY
                Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_exists_05(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_exists_06(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_exists_07(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_exists_08(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_exists_09(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_10(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_11(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_12(self):
        # using IDs in EXISTS clauses should be semantically identical
        # to using object types
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_13(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment
            FILTER EXISTS Issue.owner.<owner[IS Comment]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_exists_14(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment to it
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        Comment.issue = Issue
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_exists_15(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment, but not to the
            # issue itself
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        Comment.issue != Issue
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '4'}],
        )

    def test_edgeql_select_exists_16(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment, but not to the
            # issue itself
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        Comment.issue.id != Issue.id
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '4'}],
        )

    def test_edgeql_select_exists_17(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            # issue where the owner also has a comment, but not to the
            # issue itself
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = Issue.owner
                        AND
                        NOT Comment.issue = Issue
                )
            ORDER BY
                Issue.number;
            ''',
            [{'number': '4'}],
        )

    def test_edgeql_select_exists_18(self):
        self.assert_query_result(
            r'''
            SELECT EXISTS (
                SELECT Issue
                FILTER Issue.status.name = 'Open'
            );
            ''',
            [True],
        )

    def test_edgeql_select_coalesce_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue{
                kind := Issue.priority.name ?? Issue.status.name
            }
            ORDER BY Issue.number;
            ''',
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        )

    def test_edgeql_select_coalesce_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"operator '\?\?' cannot.*'std::str' and 'std::int64'"):

            self.con.execute(r'''
                SELECT Issue{
                    kind := Issue.priority.name ?? 1
                };
            ''')

    def test_edgeql_select_coalesce_03(self):
        issues_h = self.con.query(r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
            ORDER BY Issue.number;
        ''')

        issues_n = self.con.query(r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority
            ORDER BY Issue.number;
        ''')

        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name ?? 'High' = 'High'
            ORDER BY
                Issue.priority.name EMPTY LAST THEN Issue.number;
            ''',
            [{'number': o.number} for o in [*issues_h, *issues_n]]
        )

    def test_edgeql_select_equivalence_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue {
                number,
                h1 := Issue.priority.name = 'High',
                h2 := Issue.priority.name ?= 'High',
                l1 := Issue.priority.name != 'High',
                l2 := Issue.priority.name ?!= 'High'
            }
            ORDER BY Issue.number;
            ''',
            [{
                'number': '1',
                'h1': None,
                'h2': False,
                'l1': None,
                'l2': True,
            }, {
                'number': '2',
                'h1': True,
                'h2': True,
                'l1': False,
                'l2': False,
            }, {
                'number': '3',
                'h1': False,
                'h2': False,
                'l1': True,
                'l2': True,
            }, {
                'number': '4',
                'h1': None,
                'h2': False,
                'l1': None,
                'l2': True,
            }],
        )

    def test_edgeql_select_equivalence_02(self):
        self.assert_query_result(
            r'''
            # get Issues such that there's another Issue with
            # equivalent priority
            WITH
                I2 := Issue
            SELECT Issue {number}
            FILTER
                I2 != Issue
                AND
                I2.priority.name ?= Issue.priority.name
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_equivalence_03(self):
        self.assert_query_result(
            r'''
            # get Issues with priority equivalent to empty
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?= <str>{}
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_equivalence_04(self):
        self.assert_query_result(
            r'''
            # get Issues with priority equivalent to empty
            SELECT Issue {number}
            FILTER
                NOT Issue.priority.name ?!= <str>{}
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_as_01(self):
        # NOTE: for the expected ordering of Text see instance04 test
        self.assert_query_result(
            r'''
            SELECT (SELECT T := Text[IS Issue] ORDER BY T.body).number;
            ''',
            ['4', '1', '3', '2'],
        )

    def test_edgeql_select_as_02(self):
        self.assert_query_result(
            r'''
            SELECT (
                SELECT T := Text[IS Issue]
                FILTER T.body LIKE '%EdgeDB%'
                ORDER BY T.name
            ).name;
            ''',
            ['Release EdgeDB']
        )

    def test_edgeql_select_and_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [],
        )

    def test_edgeql_select_and_02(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [],
        )

    def test_edgeql_select_and_03(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '4'}],
        )

    def test_edgeql_select_and_04(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '4'}],
        )

    def test_edgeql_select_and_05(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_and_06(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                AND
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}],
        )

    def test_edgeql_select_and_07(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_and_08(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                AND
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_and_09(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := .val < 5 and .name like '%on'
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': False},
                {'name': 'hexagon', 'val': 4, 'x': True},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': False},
            ],
        )

    def test_edgeql_select_and_10(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := not (.val < 5 and .name like '%on')
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': True},
                {'name': 'hexagon', 'val': 4, 'x': False},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': True},
            ],
        )

    def test_edgeql_select_and_11(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                tags,
                x := (
                    select _ := .tags = 'red' and .name like '%a%' order by _
                )
            } order by .name;
            ''',
            [{
                'name': 'circle',
                'tags': {'red', 'black'},
                'x': [False, False],
            }, {
                'name': 'hexagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'pentagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'square',
                'tags': {'red'},
                'x': [True],
            }, {
                'name': 'triangle',
                'tags': {'red', 'green'},
                'x': [False, True],
            }],
        )

    def test_edgeql_select_and_12(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                tags,
                x := (
                    select _ := not (.tags = 'red' and .name like '%a%')
                    order by _
                )
            } order by .name;
            ''',
            [{
                'name': 'circle',
                'tags': {'red', 'black'},
                'x': [True, True],
            }, {
                'name': 'hexagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'pentagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'square',
                'tags': {'red'},
                'x': [False],
            }, {
                'name': 'triangle',
                'tags': {'red', 'green'},
                'x': [False, True],
            }],
        )

    def test_edgeql_select_or_01(self):
        issues_h = self.con.query(r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
            ORDER BY Issue.number;
        ''')

        issues_l = self.con.query(r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'Low'
            ORDER BY Issue.number;
        ''')

        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
            ORDER BY Issue.priority.name THEN Issue.number;
            ''',
            [{'number': o.number} for o in [*issues_h, *issues_l]]
        )

    def test_edgeql_select_or_04(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name = 'High'
                OR
                Issue.priority.name = 'Low'
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            # it so happens that all low priority issues are also closed
            [{'number': '2'}, {'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                Issue.priority.name IN {'High', 'Low'}
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_or_05(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r'''
            # should be identical
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority
                OR
                Issue.status.name = 'Closed'
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_or_06(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_or_07(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority.id        # has Priority [2, 3]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_or_08(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

    def test_edgeql_select_or_09(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority.id    # has no Priority [1, 4]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '2'}, {'number': '3'},
             {'number': '4'}],
        )

    def test_edgeql_select_or_10(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                EXISTS Issue.<issue             # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_or_11(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT EXISTS Issue.priority       # has no Priority [1, 4]
                OR
                EXISTS Issue.<issue.id          # has Comment [1]
            ORDER BY Issue.number;
            ''',
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_or_12(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                NOT EXISTS Issue.<issue         # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_or_13(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                EXISTS Issue.priority           # has Priority [2, 3]
                OR
                NOT EXISTS Issue.<issue.id      # has no Comment [2, 3, 4]
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_or_14(self):
        self.assert_query_result(
            r'''
            # Find Issues that have status 'Closed' or number 2 or 3
            #
            SELECT Issue{number}
            FILTER
                Issue.status.name = 'Closed'
                OR
                Issue.number = '2'
                OR
                Issue.number = '3'
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_or_15(self):
        self.assert_query_result(
            r'''
            # Find Issues that have status 'Closed' or number 2 or 3
            #
            SELECT Issue{number}
            FILTER
                (
                    # Issues 2, 3, 4 satisfy this subclause
                    Issue.status.name = 'Closed'
                    OR
                    Issue.number = '2'
                    OR
                    Issue.number = '3'
                ) AND (
                    # Issues 1, 2, 3 satisfy this subclause
                    Issue.name ILIKE '%edgedb%'
                    OR
                    Issue.priority.name = 'Low'
                )
            ORDER BY Issue.number;
            ''',
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_or_16(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := .val < 5 or .name like '%on'
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': True},
                {'name': 'hexagon', 'val': 4, 'x': True},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': False},
            ],
        )

    def test_edgeql_select_or_17(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := not (.val < 5 or .name like '%on')
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': False},
                {'name': 'hexagon', 'val': 4, 'x': False},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': True},
            ],
        )

    def test_edgeql_select_or_18(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                tags,
                x := (
                    select _ := .tags = 'red' or .name like '%t%a%' order by _
                )
            } order by .name;
            ''',
            [{
                'name': 'circle',
                'tags': {'red', 'black'},
                'x': [False, True],
            }, {
                'name': 'hexagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'pentagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'square',
                'tags': {'red'},
                'x': [True],
            }, {
                'name': 'triangle',
                'tags': {'red', 'green'},
                'x': [True, True],
            }],
        )

    def test_edgeql_select_or_19(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                tags,
                x := (
                    select _ := not (.tags = 'red' or .name like '%t%a%')
                    order by _
                )
            } order by .name;
            ''',
            [{
                'name': 'circle',
                'tags': {'red', 'black'},
                'x': [False, True],
            }, {
                'name': 'hexagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'pentagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'square',
                'tags': {'red'},
                'x': [False],
            }, {
                'name': 'triangle',
                'tags': {'red', 'green'},
                'x': [False, False],
            }],
        )

    def test_edgeql_select_not_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER Issue.priority.name != 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

    def test_edgeql_select_not_02(self):
        # testing double negation
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER NOT NOT NOT Issue.priority.name = 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER NOT NOT Issue.priority.name != 'High'
            ORDER BY Issue.number;
            ''',
            [{'number': '3'}],
        )

    def test_edgeql_select_not_03(self):
        # test that: a OR b = NOT( NOT a AND NOT b)
        self.assert_query_result(
            r'''
            SELECT Issue{number}
            FILTER
                NOT (
                    NOT Issue.priority.name = 'High'
                    AND
                    NOT Issue.status.name = 'Closed'
                )
            ORDER BY Issue.number;
            ''',
            # this is the result from or04
            #
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_not_04(self):
        # testing double negation
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := not (.val < 5)
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': False},
                {'name': 'hexagon', 'val': 4, 'x': False},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': True},
            ],
        )

        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := not not (.val < 5)
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': True},
                {'name': 'hexagon', 'val': 4, 'x': True},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': False},
            ],
        )

    def test_edgeql_select_not_05(self):
        # testing double negation
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                tags,
                x := (select _ := not (.tags = 'red') order by _)
            } order by .name;
            ''',
            [{
                'name': 'circle',
                'tags': {'red', 'black'},
                'x': [False, True],
            }, {
                'name': 'hexagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'pentagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'square',
                'tags': {'red'},
                'x': [False],
            }, {
                'name': 'triangle',
                'tags': {'red', 'green'},
                'x': [False, True],
            }],
        )

        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                tags,
                x := (select _ := not not (.tags = 'red') order by _)
            } order by .name;
            ''',
            [{
                'name': 'circle',
                'tags': {'red', 'black'},
                'x': [False, True],
            }, {
                'name': 'hexagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'pentagon',
                'tags': [],
                'x': [],
            }, {
                'name': 'square',
                'tags': {'red'},
                'x': [True],
            }, {
                'name': 'triangle',
                'tags': {'red', 'green'},
                'x': [False, True],
            }],
        )

    def test_edgeql_select_empty_01(self):
        self.assert_query_result(
            r"""
            # This is not the same as checking that number does not EXIST.
            # Any binary operator with one operand as empty results in an
            # empty result, because the cross product of anything with an
            # empty set is empty.
            SELECT Issue.number = <str>{};
            """,
            [],
        )

    def test_edgeql_select_empty_02(self):
        self.assert_query_result(
            r"""
            # Test short-circuiting operations with empty
            SELECT Issue.number = '1' OR <bool>{};
            """,
            [],
        )

        self.assert_query_result(
            r"""
            SELECT Issue.number = 'X' OR <bool>{};
            """,
            [],
        )

        self.assert_query_result(
            r"""
            SELECT Issue.number = '1' AND <bool>{};
            """,
            [],
        )

        self.assert_query_result(
            r"""
            SELECT Issue.number = 'X' AND <bool>{};
            """,
            [],
        )

    def test_edgeql_select_empty_03(self):
        self.assert_query_result(
            r"""
            # Test short-circuiting operations with empty
            SELECT count(Issue.number = '1' OR <bool>{});
            """,
            [0],
        )

        self.assert_query_result(
            r"""
            SELECT count(Issue.number = 'X' OR <bool>{});
            """,
            [0],
        )

        self.assert_query_result(
            r"""
            SELECT count(Issue.number = '1' AND <bool>{});
            """,
            [0],
        )

        self.assert_query_result(
            r"""
            SELECT count(Issue.number = 'X' AND <bool>{});
            """,
            [0],
        )

    def test_edgeql_select_empty_04(self):
        self.assert_query_result(
            r"""
            # Perfectly legal way to mask 'time_estimate' with empty set.
            SELECT Issue {
                number,
                time_estimate := <int64>{}
            } ORDER BY .number;
            """,
            [
                {'number': '1', 'time_estimate': None},
                {'number': '2', 'time_estimate': None},
                {'number': '3', 'time_estimate': None},
                {'number': '4', 'time_estimate': None},
            ],
        )

    def test_edgeql_select_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'expression returns value of indeterminate type'):
            self.con.query(r"""
                SELECT Issue {
                    number,
                    # the empty set is of an unspecified type
                    time_estimate := {}
                } ORDER BY .number;
                """)

    def test_edgeql_select_empty_object_01(self):
        self.assert_query_result(
            r'''
            SELECT <Issue>{}
            ''',
            [],
        )

    def test_edgeql_select_empty_object_02(self):
        self.assert_query_result(
            r'''
            SELECT NOT EXISTS (<Issue>{})
            ''',
            [True],
        )

    def test_edgeql_select_empty_object_03(self):
        self.assert_query_result(
            r'''
            SELECT ((SELECT Issue FILTER false) ?= <Issue>{})
            ''',
            [True],
        )

    def test_edgeql_select_empty_object_04(self):
        self.assert_query_result(
            r'''
            SELECT count(<Issue>{}) = 0
            ''',
            [True],
        )

    def test_edgeql_select_cross_01(self):
        self.assert_query_result(
            r"""
            # the cross product of status and priority names
            SELECT Status.name ++ Priority.name
            ORDER BY Status.name THEN Priority.name;
            """,
            ['ClosedHigh', 'ClosedLow', 'OpenHigh', 'OpenLow'],
        )

    def test_edgeql_select_cross_02(self):
        self.assert_query_result(
            r"""
            # status and priority name for each issue
            SELECT Issue.status.name ++ Issue.priority.name
            ORDER BY Issue.number;
            """,
            ['OpenHigh', 'ClosedLow'],
        )

    def test_edgeql_select_cross_03(self):
        self.assert_query_result(
            r"""
            # cross-product of all user names and issue numbers
            SELECT User.name ++ Issue.number
            ORDER BY User.name THEN Issue.number;
            """,
            ['Elvis1', 'Elvis2', 'Elvis3', 'Elvis4',
             'Yury1', 'Yury2', 'Yury3', 'Yury4'],
        )

    def test_edgeql_select_cross_04(self):
        self.assert_query_result(
            r"""
            # concatenate the user name with every issue number that user has
            SELECT User.name ++ User.<owner[IS Issue].number
            ORDER BY User.name THEN User.<owner[IS Issue].number;
            """,
            ['Elvis1', 'Elvis4', 'Yury2', 'Yury3'],
        )

    def test_edgeql_select_cross05(self):
        self.assert_query_result(
            r"""
            # tuples will not exist for the Issue without watchers
            SELECT _ := (Issue.owner.name, Issue.watchers.name)
            ORDER BY _;
            """,
            [['Elvis', 'Yury'], ['Yury', 'Elvis'], ['Yury', 'Elvis']],
        )

    def test_edgeql_select_cross06(self):
        self.assert_query_result(
            r"""
            # tuples will not exist for the Issue without watchers
            SELECT _ := Issue.owner.name ++ Issue.watchers.name
            ORDER BY _;
            """,
            ['ElvisYury', 'YuryElvis', 'YuryElvis'],
        )

    def test_edgeql_select_cross_07(self):
        self.assert_query_result(
            r"""
            SELECT _ := count(Issue.owner.name ++ Issue.watchers.name);
            """,
            [3],
        )

        self.assert_query_result(
            r"""
            SELECT _ := count(DISTINCT (
                Issue.owner.name ++ Issue.watchers.name));
            """,
            [2],
        )

    def test_edgeql_select_cross08(self):
        self.assert_query_result(
            r"""
            SELECT _ := Issue.owner.name ++ <str>count(Issue.watchers.name)
            ORDER BY _;
            """,
            ['Elvis0', 'Elvis1', 'Yury1', 'Yury1'],
        )

    def test_edgeql_select_cross_09(self):
        self.assert_query_result(
            r"""
            SELECT _ := count(
                Issue.owner.name ++ <str>count(Issue.watchers.name));
            """,
            [4],
        )

    def test_edgeql_select_cross_10(self):
        self.assert_query_result(
            r"""
            WITH
                # this select shows all the relevant data for next tests
                x := (SELECT Issue {
                    name := Issue.owner.name,
                    w := count(Issue.watchers.name),
                })
            SELECT count(x.name ++ <str>x.w);
            """,
            [4],
        )

    def test_edgeql_select_cross_11(self):
        self.assert_query_result(
            r"""
            SELECT count(
                Issue.owner.name ++
                <str>count(Issue.watchers) ++
                <str>Issue.time_estimate ?? '0'
            );
            """,
            [4],
        )

    def test_edgeql_select_cross_12(self):
        # Same as cross11, but without coalescing the time_estimate,
        # which should collapse the counted set to a single element.
        self.assert_query_result(
            r"""
            SELECT count(
                Issue.owner.name ++
                <str>count(Issue.watchers) ++
                <str>Issue.time_estimate
            );
            """,
            [1],
        )

    def test_edgeql_select_cross_13(self):
        self.assert_query_result(
            r"""
            SELECT count(count(Issue.watchers));
            """,
            [1],
        )

        self.assert_query_result(
            r"""
            SELECT count(
                (Issue, count(Issue.watchers))
            );
            """,
            [4],
        )

    def test_edgeql_select_subqueries_01(self):
        self.assert_query_result(
            r"""
            WITH
                Issue2 := Issue
            # this is string concatenation, not integer arithmetic
            SELECT Issue.number ++ Issue2.number
            ORDER BY Issue.number ++ Issue2.number;
            """,
            ['{}{}'.format(a, b) for a in range(1, 5) for b in range(1, 5)],
        )

    def test_edgeql_select_subqueries_02(self):
        self.assert_query_result(
            r"""
            SELECT Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS (
                    # due to common prefix, the Issue referred to here is
                    # the same Issue as in the LHS of AND, therefore
                    # this condition can never be true
                    SELECT Issue FILTER Issue.number IN {'1', '6'}
                );
            """,
            [],
        )

    def test_edgeql_select_subqueries_03(self):
        self.assert_query_result(
            r"""
            WITH
                sub := (
                    SELECT Issue FILTER Issue.number IN {'1', '6'}
                )
            SELECT Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS (
                    (SELECT sub FILTER sub = Issue)
                );
            """,
            [],
        )

    def test_edgeql_select_subqueries_04(self):
        self.assert_query_result(
            r"""
            WITH
                sub := (
                    SELECT
                        Issue
                    FILTER
                        Issue.number IN {'1', '6'}
                )
            SELECT
                Issue{number}
            FILTER
                Issue.number IN {'2', '3', '4'}
                AND
                EXISTS sub
            ORDER BY
                Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_subqueries_05(self):
        self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue with the same priority
            WITH
                Issue2 := (SELECT Issue)
            SELECT
                Issue {
                    number
                }
            FILTER
                Issue != Issue2
                AND
                # NOTE: this condition is false when one of the sides is empty
                Issue.priority = Issue2.priority
            ORDER BY
                Issue.number;
            """,
            [],
        )

    def test_edgeql_select_subqueries_06(self):
        self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue with the same priority (even if the "same" means empty)
            WITH
                Issue2 := Issue
            SELECT
                Issue {
                    number
                }
            FILTER
                Issue != Issue2 AND Issue.priority ?= Issue2.priority
            ORDER BY
                Issue.number;
            """,
            [{'number': '1'}, {'number': '4'}],
        )

    def test_edgeql_select_subqueries_07(self):
        self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue watched by the same user as this one
            SELECT Issue{number}
            FILTER
                EXISTS Issue.watchers
                AND
                EXISTS (
                    (SELECT
                        User
                     FILTER
                        User = Issue.watchers AND
                        User.<watchers != Issue
                    ).<watchers
                )
            ORDER BY
                Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_subqueries_08(self):
        self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # issue watched by the same user as this one
            SELECT Issue{number}
            FILTER
                EXISTS Issue.watchers
                AND
                EXISTS (
                    SELECT Text
                    FILTER
                        Text IS Issue
                        AND
                        Text[IS Issue].watchers = Issue.watchers
                        AND
                        Text != Issue
                )
            ORDER BY
                Issue.number;
            """,
            [{'number': '2'}, {'number': '3'}],
        )

    def test_edgeql_select_subqueries_09(self):
        self.assert_query_result(
            r"""
            SELECT Issue.number ++ (SELECT Issue.number);
            """,
            {'11', '22', '33', '44'},
        )

    def test_edgeql_select_subqueries_10(self):
        self.assert_query_result(
            r"""
            WITH
                sub := (SELECT Issue.number)
            SELECT
                Issue.number ++ sub;
            """,
            {'11', '12', '13', '14', '21', '22', '23', '24',
             '31', '32', '33', '34', '41', '42', '43', '44'},
        )

    def test_edgeql_select_subqueries_11(self):
        self.assert_query_result(
            r"""
            SELECT Text{
                [IS Issue].number,
                body_length := len(Text.body)
            } ORDER BY len(Text.body);
            """,
            [
                {'number': '3', 'body_length': 19},
                {'number': None, 'body_length': 21},
                {'number': None, 'body_length': 28},
                {'number': '1', 'body_length': 33},
                {'number': '4', 'body_length': 41},
                {'number': '2', 'body_length': 52},
            ],
        )

        self.assert_query_result(
            r"""
            # find all issues such that there's at least one more
            # Text item of similar body length (+/-5 characters)
            SELECT Issue{
                number,
            }
            FILTER
                EXISTS (
                    SELECT Text
                    FILTER
                        Text != Issue
                        AND
                        (len(Text.body) - len(Issue.body)) ^ 2 <= 25
                )
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '3'}],
        )

    def test_edgeql_select_subqueries_12(self):
        self.assert_query_result(
            r"""
            # same as above, but also include the body_length computable
            SELECT Issue{
                number,
                body_length := len(Issue.body)
            }
            FILTER
                EXISTS (
                    SELECT Text
                    FILTER
                        Text != Issue
                        AND
                        (len(Text.body) - len(Issue.body)) ^ 2 <= 25
                )
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'body_length': 33,
            }, {
                'number': '3',
                'body_length': 19,
            }],
        )

    def test_edgeql_select_subqueries_13(self):
        self.assert_query_result(
            r"""
            SELECT User{name}
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = User
                );
            """,
            [{'name': 'Elvis'}],
        )

    def test_edgeql_select_subqueries_14(self):
        self.assert_query_result(
            r"""
            SELECT User{name}
            FILTER
                EXISTS (
                    SELECT Comment
                    FILTER
                        Comment.owner = User
                # adding a required link to an EXISTS should not alter
                # the result
                ).owner;
            """,
            [{'name': 'Elvis'}],
        )

    def test_edgeql_select_subqueries_15(self):
        self.assert_query_result(
            r"""
            # Find all issues such that there's at least one more
            # issue watched by the same user as this one, this user
            # must have at least one Comment.
            SELECT Issue {
                number
            }
            FILTER
                EXISTS Issue.watchers AND
                EXISTS (
                    SELECT
                        User
                    FILTER
                        # The User is among the watchers of this Issue
                        User = Issue.watchers AND
                        # and they also watch some other Issue other than this
                        User.<watchers[IS Issue] != Issue AND
                        # and they also have at least one comment
                        EXISTS (
                            SELECT Comment FILTER Comment.owner = User
                        )
                )
            ORDER BY
                Issue.number;
            """,
            [
                {'number': '2'},
                {'number': '3'}
            ],
        )

    def test_edgeql_select_subqueries_16(self):
        self.assert_query_result(
            r"""
            # testing IN and a subquery
            SELECT Comment{body}
            FILTER
                Comment.owner IN (
                    SELECT User
                    FILTER
                        User.name = 'Elvis'
                );
            """,
            [{'body': 'EdgeDB needs to happen soon.'}],
        )

    def test_edgeql_select_subqueries_17(self):
        self.assert_query_result(
            r"""
            # get a comment whose owner is part of the users who own Issue "1"
            SELECT Comment{body}
            FILTER
                Comment.owner IN (
                    SELECT User
                    FILTER
                        User.<owner IN (
                            SELECT Issue
                            FILTER
                                Issue.number = '1'
                        )
                );
            """,
            [{'body': 'EdgeDB needs to happen soon.'}],
        )

    def test_edgeql_select_subqueries_18(self):
        self.assert_query_result(
            r"""
            # here, DETACHED doesn't do anything special, because the
            # symbol U2 is reused on both sides of '+'
            WITH
                U2 := DETACHED User
            SELECT U2.name ++ U2.name;
            """,
            {'ElvisElvis', 'YuryYury'},
        )

        self.assert_query_result(
            r"""
            # DETACHED is reused on both sides of '+' directly
            SELECT (DETACHED User).name ++ (DETACHED User).name;
            """,
            {'ElvisElvis', 'ElvisYury', 'YuryElvis', 'YuryYury'},
        )

    def test_edgeql_select_alias_indirection_01(self):
        self.assert_query_result(
            r"""
            # Direct reference to a computable element in a subquery
            SELECT
                (
                    SELECT User {
                        num_issues := count(User.<owner[IS Issue])
                    } FILTER .name = 'Elvis'
                ).num_issues;
            """,
            [2],
        )

    def test_edgeql_select_alias_indirection_02(self):
        self.assert_query_result(
            r"""
            # Reference to a computable element in a subquery
            # defined as an alias.
            WITH U := (
                    SELECT User {
                        num_issues := count(User.<owner[IS Issue])
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.num_issues;
            """,
            [2],
        )

    def test_edgeql_select_alias_indirection_03(self):
        self.assert_query_result(
            r"""
            # Reference a computed object set in an alias.
            WITH U := (
                    WITH U2 := User
                    SELECT User {
                        friend := (
                            SELECT U2 FILTER U2.name = 'Yury'
                        )
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.friend.name;
            """,
            ['Yury'],
        )

    def test_edgeql_select_alias_indirection_04(self):
        result = self.con.query(r"""
            # Reference a constant expression in an alias.
            WITH U := (
                    SELECT User {
                        issues := (
                            SELECT Issue {
                                foo := 1 + random()
                            } FILTER Issue.owner = User
                        )
                    } FILTER .name = 'Elvis'
                )
            SELECT
                U.issues.foo;
            """)

        self.assertEqual(len(result), 2)

    def test_edgeql_select_alias_indirection_05(self):
        self.assert_query_result(
            r"""
            # Reference multiple aliases.
            WITH U := (
                    SELECT User FILTER User.name = 'Elvis'
                ),
                I := (
                    SELECT Issue FILTER Issue.number = '1'
                )
            SELECT
                I.owner = U;
            """,
            [True],
        )

    def test_edgeql_select_alias_indirection_06(self):
        self.assert_query_result(
            r"""
            # Reference another alias from an alias.
            WITH U := (
                    SELECT User FILTER User.name = 'Elvis'
                ),
                I := (
                    SELECT Issue FILTER Issue.owner = U
                )
            SELECT
                I.number
            ORDER BY
                I.number;
            """,
            ['1', '4'],
        )

    def test_edgeql_select_alias_indirection_07(self):
        self.assert_query_result(
            r"""
            # A combination of the above two.
            WITH U := (
                    SELECT User FILTER User.name = 'Elvis'
                ),
                I := (
                    SELECT Issue FILTER Issue.owner = U
                )
            SELECT
                I
            FILTER
                I.owner != U
            ORDER BY
                I.number;
            """,
            [],
        )

    def test_edgeql_select_alias_indirection_08(self):
        self.assert_query_result(
            r"""
            # A slightly more complex type variant.
             WITH U := (
                     WITH U2 := User
                     SELECT User {
                         friends := (
                             SELECT U2 { foo := U2.name ++ '!' }
                             FILTER U2.name = 'Yury'
                         )
                     } FILTER .name = 'Elvis'
                 )
             SELECT
                 U {
                     my_issues := (
                        SELECT U.<owner[IS Issue].number
                        ORDER BY U.<owner[IS Issue].number),
                     friends_issues := (
                        SELECT U.friends.<owner[IS Issue].number
                        ORDER BY U.friends.<owner[IS Issue].number),
                     friends_foos := (
                        SELECT U.friends.foo
                        ORDER BY U.friends.foo)
                 };
            """,
            [{
                'my_issues': ['1', '4'],
                'friends_foos': 'Yury!',
                'friends_issues': ['2', '3']
            }]
        )

    def test_edgeql_select_alias_indirection_09(self):
        self.assert_query_result(
            r'''
            WITH
                sub := (
                    SELECT
                        Text {
                            foo := Text.body ++ '!'
                        }
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT 1
                )
            SELECT
                User {
                    name,
                    shortest_text_shape := sub {
                        body,
                        foo
                    }
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text_shape': {
                    'body': 'Minor lexer tweaks.',
                    'foo': 'Minor lexer tweaks.!',
                },
            }]
        )

    def test_edgeql_select_alias_indirection_10(self):
        self.assert_query_result(
            r'''
            WITH
                sub := (
                    SELECT
                        Text {
                            foo := Text.body ++ '!'
                        }
                    ORDER BY
                        len(Text.body) ASC
                    LIMIT 1
                )
            SELECT
                User {
                    name,
                    shortest_text_foo := sub.foo
                }
            FILTER User.name = 'Elvis';
            ''',
            [{
                'name': 'Elvis',
                'shortest_text_foo': 'Minor lexer tweaks.!'
            }]
        )

    def test_edgeql_select_alias_indirection_11(self):
        self.assert_query_result(
            r'''
            WITH
                Developers := (
                    SELECT
                        User {
                            open_issues := (
                                SELECT
                                    Issue {
                                        spent_time := (
                                            SELECT
                                                sum(Issue.time_spent_log
                                                         .spent_time)
                                        )
                                    }
                                FILTER
                                    Issue.owner = User
                            )
                        }
                    FILTER
                        User.name IN {'Elvis', 'Yury'}
                )
            SELECT
                Developers {
                    name,
                    open_issues: {
                        number,
                        spent_time
                    } ORDER BY .number
                }
            ORDER BY
                Developers.name;
            ''',
            [
                {
                    'name': 'Elvis',
                    'open_issues': [
                        {'number': '1', 'spent_time': 50000},
                        {'number': '4', 'spent_time': 0},
                    ]
                },
                {
                    'name': 'Yury',
                    'open_issues': [
                        {'number': '2', 'spent_time': 0},
                        {'number': '3', 'spent_time': 0}
                    ]
                }
            ]
        )

    def test_edgeql_select_slice_01(self):
        self.assert_query_result(
            r"""
            # full name of the Issue is 'Release EdgeDB'
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2];
            """,
            ['l'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[-2];
            """,
            ['D'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2:4];
            """,
            ['le'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2:];
            """,
            ['lease EdgeDB'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[:2];
            """,
            ['Re'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[2:-1];
            """,
            ['lease EdgeD'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[-2:];
            """,
            ['DB'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).name[:-2];
            """,
            ['Release Edge'],
        )

    def test_edgeql_select_slice_02(self):
        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name;
            """,
            ['default::Issue'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2];
            """,
            ['f'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[-2];
            """,
            ['u'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2:4];
            """,
            ['fa'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2:];
            """,
            ['fault::Issue'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[:2];
            """,
            ['de'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[2:-1];
            """,
            ['fault::Issu'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[-2:];
            """,
            ['ue'],
        )

        self.assert_query_result(
            r"""
            SELECT (
                SELECT Issue
                FILTER Issue.number = '1'
            ).__type__.name[:-2];
            """,
            ['default::Iss'],
        )

    def test_edgeql_select_slice_03(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                name,
                type_name := Issue.__type__.name,
                a := Issue.name[2],
                b := Issue.name[2:-1],
                c := Issue.__type__.name[2:-1],
            }
            FILTER Issue.number = '1';
            """,
            [{
                'name': 'Release EdgeDB',
                'type_name': 'default::Issue',
                'a': 'l',
                'b': 'lease EdgeD',
                'c': 'fault::Issu',
            }],
        )

    def test_edgeql_select_slice_04(self):

        self.assert_query_result(
            r"""
            select [1,2,3,4,5][1:];
            """,
            [[2, 3, 4, 5]],
        )

        self.assert_query_result(
            r"""
            select [1,2,3,4,5][:3];
            """,
            [[1, 2, 3]],
        )

        self.assert_query_result(
            r"""
            select [1,2,3][1:<int64>{}];
            """,
            [],
        )

        # try to trick the compiler and to pass NULL into edgedb._slice
        self.assert_query_result(
            r"""
            select [1,2,3][1:<optional int64>$0];
            """,
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r"""
            select [1,2,3][<optional int64>$0:2];
            """,
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r"""
            select [1,2,3][<optional int64>$0:<optional int64>$1];
            """,
            [],
            variables=(
                None,
                None,
            ),
        )

        self.assertEqual(
            self.con.query(
                r"""
                select to_json('[true, 3, 4, null]')[1:];
                """
            ),
            edgedb.Set(('[3, 4, null]',)),
        )

        self.assertEqual(
            self.con.query(
                r"""
                select to_json('[true, 3, 4, null]')[:2];
                """
            ),
            edgedb.Set(('[true, 3]',)),
        )

        self.assert_query_result(
            r"""
            select (<optional json>$0)[2:];
            """,
            [],
            variables=(None,),
        )

        self.assertEqual(
            self.con.query(
                r"""
                select to_json('"hello world"')[2:];
                """
            ),
            edgedb.Set(('"llo world"',)),
        )

        self.assertEqual(
            self.con.query(
                r"""
                select to_json('"hello world"')[:4];
                """
            ),
            edgedb.Set(('"hell"',)),
        )

        self.assert_query_result(
            r"""
            select (<array<str>>[])[0:];
            """,
            [[]],
        )

        self.assert_query_result(
            r'''select to_json('[]')[0:];''',
            # JSON:
            [[]],
            # Binary:
            ['[]'],
        )

        self.assert_query_result(
            r'''select [(1,'foo'), (2,'bar'), (3,'baz')][1:];''',
            [[(2, 'bar'), (3, 'baz')]],
        )

        self.assert_query_result(
            r'''select [(1,'foo'), (2,'bar'), (3,'baz')][:2];''',
            [[(1, 'foo'), (2, 'bar')]],
        )

        self.assert_query_result(
            r'''select [(1,'foo'), (2,'bar'), (3,'baz')][1:2];''',
            [[(2, 'bar')]],
        )

        self.assert_query_result(
            r'''
                select [(1,'foo'), (2,'bar'), (3,'baz')][<optional int32>$0:];
            ''',
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r'''
                select (<optional array<int32>>$0)[2];
            ''',
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r'''
                select (<optional str>$0)[2];
            ''',
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r'''
                select to_json(<optional str>$0)[2];
            ''',
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r'''
                select (<optional array<int32>>$0)[1:2];
            ''',
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r'''
                select (<optional str>$0)[1:2];
            ''',
            [],
            variables=(None,),
        )

        self.assert_query_result(
            r'''
                select to_json(<optional str>$0)[1:2];
            ''',
            [],
            variables=(None,),
        )

    def test_edgeql_select_bigint_index_01(self):

        big_pos = str(2**40)
        big_neg = str(-(2**40))

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            rf"array index {big_pos} is out of bounds",
        ):
            self.con.query(f"select [1, 2, 3][{big_pos}];")

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            rf"array index {big_neg} is out of bounds",
        ):
            self.con.query(f"select [1, 2, 3][{big_neg}];")

        self.assert_query_result(
            f"""select [1, 2, 3][0:{big_pos}];""",
            [[1, 2, 3]],
        )

        self.assert_query_result(
            f"""select [1, 2, 3][0:{big_neg}];""",
            [[]],
        )

        self.assert_query_result(
            f"""select [1, 2, 3][{big_neg}:{big_pos}];""",
            [[1, 2, 3]],
        )

        self.assert_query_result(
            f"""select [1, 2, 3][{big_pos}:{big_neg}];""",
            [[]],
        )

        self.assert_query_result(
            f"""select [1, 2, 3][{big_neg}:{big_neg}];""",
            [[]],
        )

        self.assert_query_result(
            f"""select [1, 2, 3][{big_pos}:{big_pos}];""",
            [[]],
        )

    def test_edgeql_select_bigint_index_02(self):

        big_pos = str(2**40)
        big_neg = str(-(2**40))

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            rf"string index {big_pos} is out of bounds",
        ):
            self.con.query(f'select "Hello world!"[{big_pos}];')

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            rf"string index {big_neg} is out of bounds",
        ):
            self.con.query(f'select "Hello world!"[{big_neg}];')

        self.assert_query_result(
            f"""select "Hello world!"[6:{big_pos}];""",
            ["world!"],
        )

        self.assert_query_result(
            f"""select "Hello world!"[6:{big_neg}];""",
            [""],
        )

        self.assert_query_result(
            f"""select "Hello world!"[{big_neg}:{big_pos}];""",
            ["Hello world!"],
        )

        self.assert_query_result(
            f"""select "Hello world!"[{big_pos}:{big_neg}];""",
            [""],
        )

        self.assert_query_result(
            f"""select "Hello world!"[{big_neg}:{big_neg}];""",
            [""],
        )

        self.assert_query_result(
            f"""select "Hello world!"[{big_pos}:{big_pos}];""",
            [""],
        )

    def test_edgeql_select_bigint_index_03(self):

        big_pos = str(2**40)
        big_neg = str(-(2**40))

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError, rf"JSON index {big_pos} is out of bounds"
        ):
            self.con.query(f'select to_json("[1, 2, 3]")[{big_pos}];')

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError, rf"JSON index {big_neg} is out of bounds"
        ):
            self.con.query(f'select to_json("[1, 2, 3]")[{big_neg}];')

        self.assert_query_result(
            f"""select to_json("[1, 2, 3]")[1:{big_pos}];""",
            # JSON:
            [[2, 3]],
            # Binary:
            ['[2, 3]'],
        )

        self.assert_query_result(
            f"""select to_json("[1, 2, 3]")[1:{big_neg}];""",
            [[]],
            ['[]'],
        )

        self.assert_query_result(
            f"""select to_json("[1, 2, 3]")[{big_neg}:{big_pos}];""",
            # JSON:
            [[1, 2, 3]],
            # Binary:
            ['[1, 2, 3]'],
        )

        self.assert_query_result(
            f"""select to_json("[1, 2, 3]")[{big_pos}:{big_neg}];""",
            [[]],
            ['[]'],
        )

        self.assert_query_result(
            f"""select to_json("[1, 2, 3]")[{big_neg}:{big_neg}];""",
            [[]],
            ['[]'],
        )

        self.assert_query_result(
            f"""select to_json("[1, 2, 3]")[{big_pos}:{big_pos}];""",
            [[]],
            ['[]'],
        )

    def test_edgeql_select_tuple_01(self):
        self.assert_query_result(
            r"""
            # get tuples (status, number of issues)
            SELECT (Status.name, count(Status.<status))
            ORDER BY Status.name;
            """,
            [['Closed', 2], ['Open', 2]]
        )

    def test_edgeql_select_tuple_02(self):
        self.assert_query_result(
            r"""
            # nested tuples
            SELECT
                _ := (
                    User.name, (
                        User.<owner[IS Issue].status.name,
                        count(User.<owner[IS Issue])
                    )
                )
                # A tuple is essentially an identity function within our
                # set operation semantics, so here we're selecting a cross
                # product of all user names with user owned issue statuses.
                #
            ORDER BY _.0 THEN _.1;
            """,
            [
                ['Elvis', ['Closed', 1]],
                ['Elvis', ['Open', 1]],
                ['Yury', ['Closed', 1]],
                ['Yury', ['Open', 1]],
            ]
        )

    def test_edgeql_select_tuple_03(self):
        self.assert_query_result(
            r"""
            WITH
                _ := {('Elvis',), ('Yury',)}
            SELECT
                User {
                    name
                }
            FILTER
                User.name = _.0
            ORDER BY
                User.name;
            """,
            [
                {'name': 'Elvis'},
                {'name': 'Yury'},
            ]
        )

    def test_edgeql_select_tuple_04(self):
        self.assert_query_result(
            r"""
            SELECT
                User {
                    t := {(1, 2), (3, 4)}
                }
            FILTER
                User.name = 'Elvis'
            ORDER BY
                User.name;
            """,
            [
                {'t': [[1, 2], [3, 4]]},
            ]
        )

    def test_edgeql_select_tuple_05(self):
        self.assert_query_result(
            r"""
                SELECT (
                    statuses := count(Status),
                    issues := count(Issue),
                );
            """,
            [{'statuses': 2, 'issues': 4}],
        )

    def test_edgeql_select_tuple_06(self):
        # Tuple in a common set expr.
        self.assert_query_result(
            r"""
            WITH
                counts := (SELECT (
                    statuses := count(Status),
                    issues := count(Issue),
                ))
            SELECT
                counts.statuses + counts.issues;
            """,
            [6],
        )

    def test_edgeql_select_tuple_07(self):
        # Object in a tuple.
        self.assert_query_result(
            r"""
            WITH
                criteria := (SELECT (
                    user := (SELECT User FILTER User.name = 'Yury'),
                    status := (SELECT Status FILTER Status.name = 'Open'),
                ))
            SELECT (
                SELECT
                    Issue
                FILTER
                    Issue.owner = criteria.user
                    AND Issue.status = criteria.status
            ).number;
            """,
            ['2'],
        )

    def test_edgeql_select_tuple_08(self):
        # Object in a tuple returned directly.
        self.assert_query_result(
            r"""
            SELECT
                (
                    user := (SELECT User{name} FILTER User.name = 'Yury')
                );
            """,
            [{
                'user': {
                    'name': 'Yury'
                }
            }],
        )

    def test_edgeql_select_tuple_09(self):
        # Object in a tuple referred to directly.
        self.assert_query_result(
            r"""
            SELECT
                (
                    user := (SELECT User{name} FILTER User.name = 'Yury')
                ).user.name;
            """,
            ['Yury'],
        )

    def test_edgeql_select_tuple_10(self):
        # Tuple comparison
        self.assert_query_result(
            r"""
            WITH
                U1 := User,
                U2 := User
            SELECT
                (user := (SELECT U1{name} FILTER U1.name = 'Yury'))
                    =
                (user := (SELECT U2{name} FILTER U2.name = 'Yury'));
            """,
            [True],
        )

        self.assert_query_result(
            r"""
            WITH
                U1 := User,
                U2 := User
            SELECT
                (user := (SELECT U1{name} FILTER U1.name = 'Yury'))
                    =
                (user := (SELECT U2{name} FILTER U2.name = 'Elvis'));

            """,
            [False],
        )

    def test_edgeql_select_linkproperty_01(self):
        self.assert_query_result(
            r"""
            SELECT User.todo@rank + <int64>User.todo.number
            ORDER BY User.todo.number;
            """,
            [43, 44, 45, 46]
        )

    def test_edgeql_select_linkproperty_02(self):
        self.assert_query_result(
            r"""
            SELECT Issue.<todo[IS User]@rank + <int64>Issue.number
            ORDER BY Issue.number;
            """,
            [43, 44, 45, 46]
        )

    def test_edgeql_select_linkproperty_03(self):
        self.assert_query_result(
            r"""
            SELECT User {
                name,
                todo: {
                    number,
                    @rank
                } ORDER BY User.todo.number
            }
            ORDER BY User.name;
            """,
            [{
                'name': 'Elvis',
                'todo': [{
                    'number': '1',
                    '@rank': 42,
                }, {
                    'number': '2',
                    '@rank': 42,
                }]
            }, {
                'name': 'Yury',
                'todo': [{
                    'number': '3',
                    '@rank': 42,
                }, {
                    'number': '4',
                    '@rank': 42,
                }]
            }],
        )

    def test_edgeql_select_linkproperty_04(self):
        with self.assertRaisesRegex(
            edgedb.EdgeQLSyntaxError,
            "unexpected reference to link property 'since' "
            "outside of a path expression",
        ):
            self.con.execute(
                r'''
                SELECT
                    Issue { since := (SELECT .owner)@since }
                ''',
            )

    def test_edgeql_select_linkproperty_05(self):
        with self.assertRaisesRegex(
            edgedb.EdgeQLSyntaxError,
            "unexpected reference to link property 'since' "
            "outside of a path expression",
        ):
            self.con.execute(
                r'''
                SELECT
                    Issue { since := [.owner]@since }
                ''',
            )

    def test_edgeql_select_linkproperty_06(self):
        # Test that nested computed link props survive DISTINCT.
        self.assert_query_result(
            r'''
            SELECT
                User {
                    todo := DISTINCT (
                        FOR entry IN {("1", 10), ("1", 10)}
                        UNION (
                            SELECT Issue {
                                @rank := entry.1
                            } FILTER
                                .number = entry.0
                        )
                    )
                }
            FILTER
                .name = "Elvis"
            ''',
            [{
                "todo": [{
                    "@rank": 10,
                }],
            }],
        )

    def test_edgeql_select_if_else_01(self):
        self.assert_query_result(
            r"""
            SELECT Issue {
                number,
                open := 'yes' IF Issue.status.name = 'Open' ELSE 'no'
            }
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'open': 'yes',
            }, {
                'number': '2',
                'open': 'yes',
            }, {
                'number': '3',
                'open': 'no',
            }, {
                'number': '4',
                'open': 'no',
            }],
        )

    def test_edgeql_select_if_else_02(self):
        self.assert_query_result(
            r"""
            SELECT Issue {
                number,
                # foo is 'bar' for Issue number 1 and status name for the rest
                foo := 'bar' IF Issue.number = '1' ELSE Issue.status.name
            }
            ORDER BY Issue.number;
            """,
            [{
                'number': '1',
                'foo': 'bar',
            }, {
                'number': '2',
                'foo': 'Open',
            }, {
                'number': '3',
                'foo': 'Closed',
            }, {
                'number': '4',
                'foo': 'Closed',
            }],
        )

    def test_edgeql_select_if_else_03(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    r'operator.*IF.*cannot be applied'):

            self.con.execute(r"""
                SELECT Issue {
                    foo := 'bar' IF Issue.number = '1' ELSE 123
                };
                """)

    def test_edgeql_select_if_else_04(self):
        self.assert_query_result(
            r"""
            SELECT Issue{
                kind := (Issue.priority.name
                         IF EXISTS Issue.priority.name
                         ELSE Issue.status.name)
            }
            ORDER BY Issue.number;
            """,
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        )

        self.assert_query_result(
            r"""
            # Above IF is equivalent to ??,
            SELECT Issue{
                kind := Issue.priority.name ?? Issue.status.name
            }
            ORDER BY Issue.number;
            """,
            [{'kind': 'Open'}, {'kind': 'High'},
             {'kind': 'Low'}, {'kind': 'Closed'}],
        )

    def test_edgeql_select_if_else_05(self):
        self.assert_query_result(
            r"""
            SELECT Issue {number}
            FILTER
                Issue.priority.name = 'High'
                    IF EXISTS Issue.priority.name AND EXISTS 'High'
                    ELSE EXISTS Issue.priority.name = EXISTS 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}],
        )

        self.assert_query_result(
            r"""
            # Above IF is equivalent to ?=,
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?= 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '2'}],
        )

    def test_edgeql_select_if_else_06(self):
        self.assert_query_result(
            r"""
            SELECT Issue {number}
            FILTER
                Issue.priority.name != 'High'
                    IF EXISTS Issue.priority.name AND EXISTS 'High'
                    ELSE EXISTS Issue.priority.name != EXISTS 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

        self.assert_query_result(
            r"""
            # Above IF is equivalent to !?=,
            SELECT Issue {number}
            FILTER
                Issue.priority.name ?!= 'High'
            ORDER BY Issue.number;
            """,
            [{'number': '1'}, {'number': '3'}, {'number': '4'}],
        )

    def test_edgeql_select_if_else_07(self):
        self.assert_query_result(
            r'''
            WITH a := (SELECT Issue FILTER .number = '2'),
                 b := (SELECT Issue FILTER .number = '1'),
            SELECT a.number IF a.time_estimate < b.time_estimate ELSE b.number;
            ''',
            [],
        )

    def test_edgeql_partial_01(self):
        self.assert_query_result(
            '''
            SELECT
                Issue {
                    number
                }
            FILTER
                .number = '1';
            ''',
            [{
                'number': '1'
            }]
        )

    def test_edgeql_partial_02(self):
        self.assert_query_result(
            '''
            SELECT
                Issue.watchers {
                    name
                }
            FILTER
                .name = 'Yury';
            ''',
            [{
                'name': 'Yury'
            }]
        )

    def test_edgeql_partial_03(self):
        self.assert_query_result(
            '''
            SELECT Issue {
                number,
                watchers: {
                    name,
                    name_upper := str_upper(.name)
                } FILTER .name = 'Yury'
            } FILTER .status.name = 'Open' AND .owner.name = 'Elvis';
            ''',
            [{
                'number': '1',
                'watchers': [{
                    'name': 'Yury',
                    'name_upper': 'YURY',
                }]
            }]
        )

    def test_edgeql_partial_04(self):
        self.assert_query_result(
            '''
            SELECT Issue {
                number,
            } FILTER .number > '1'
              ORDER BY .number DESC;
            ''',
            [
                {'number': '4'},
                {'number': '3'},
                {'number': '2'},
            ]
        )

    def test_edgeql_partial_05(self):
        self.assert_query_result('''
            SELECT
                Issue{
                    sub := (SELECT .number)
                }
            FILTER .number = '1';
        ''', [
            {'sub': '1'},
        ])

    def test_edgeql_partial_06(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'invalid property reference on a '
                                    'primitive type expression'):
            self.con.execute('''
                SELECT Issue.number FILTER .number > '1';
            ''')

    def test_edgeql_union_target_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue {
                number,
            } FILTER EXISTS (.references)
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2'
            }],
        )

        self.assert_query_result(
            r'''
            SELECT Issue {
                number,
            } FILTER .references[IS URL].address = 'https://edgedb.com'
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2'
            }],
        )

        self.assert_query_result(
            r'''
            SELECT Issue {
                number,
            } FILTER .references[IS Named].name = 'screenshot.png'
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2'
            }],
        )

        self.assert_query_result(
            r'''
            SELECT Issue {
                number,
                references[IS Named]: {
                    __type__: {
                        name
                    },

                    name
                } ORDER BY .name
            } FILTER EXISTS (.references)
              ORDER BY .number DESC;
            ''',
            [{
                'number': '2',
                'references': [
                    {
                        'name': 'edgedb.com',
                        '__type__': {
                            'name': 'default::URL'
                        }
                    },
                    {
                        'name': 'screenshot.png',
                        '__type__': {
                            'name': 'default::File'
                        }
                    }
                ]
            }]
        )

    def test_edgeql_select_for_01(self):
        self.assert_query_result(
            r'''
            SELECT Issue := (
                FOR x IN {1, 4}
                UNION (
                    SELECT Issue {
                        name
                    }
                    FILTER
                        .number = <str>x
                )
            )
            ORDER BY
                .number;
            ''',
            [
                {'name': 'Release EdgeDB'},
                {'name': 'Regression.'},
            ]
        )

    def test_edgeql_select_for_02(self):
        self.assert_query_result(
            r'''
            SELECT I := (
                FOR x IN {1, 3, 4}
                UNION (
                    SELECT Issue {
                        name,
                        number,
                    }
                    FILTER
                        .number > <str>x
                )
            )
            ORDER BY .number;
            ''',
            [
                {
                    'name': 'Improve EdgeDB repl output rendering.',
                    'number': '2'
                },
                {
                    'name': 'Repl tweak.',
                    'number': '3'
                },
                {
                    'name': 'Regression.',
                    'number': '4'
                },
                {
                    'name': 'Regression.',
                    'number': '4'
                },
            ]
        )

    def test_edgeql_select_for_03(self):
        self.assert_query_result(
            r'''
            FOR x IN {1, 3, 4}
            UNION (
                SELECT Issue {
                    name,
                    number,
                }
                FILTER
                    Issue.number > <str>x
                ORDER BY
                    Issue.number
                LIMIT 2
            );
            ''',
            tb.bag([
                {
                    'name': 'Improve EdgeDB repl output rendering.',
                    'number': '2'
                },
                {
                    'name': 'Repl tweak.',
                    'number': '3'
                },
                {
                    'name': 'Regression.',
                    'number': '4'
                },
            ]),
        )

    def test_edgeql_select_for_04(self):
        self.assert_query_result(
            r'''
                SELECT Issue {
                    asdf := (
                        FOR z IN .due_date UNION (1)
                    )
                }
                FILTER .name = 'Release EdgeDB';
            ''',
            [
                {
                    'asdf': None
                }
            ],
        )

    def test_edgeql_select_json_01(self):
        self.assert_query_result(
            r'''
            # cast a type variant into a set of json
            SELECT (
                SELECT <json>Issue {
                    number,
                    time_estimate
                } FILTER Issue.number = '1'
            ) = to_json('{"number": "1", "time_estimate": 3000}');
            ''',
            [True],
        )

        self.assert_query_result(
            r'''
            SELECT (
                SELECT <json>Issue {
                    number,
                    time_estimate
                } FILTER Issue.number = '2'
            ) = to_json('{"number": "2", "time_estimate": null}');
            ''',
            [True],
        )

    def test_edgeql_select_bad_reference_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"object type or alias 'default::Usr' does not exist",
                _hint="did you mean one of these: User, URL?"):

            self.con.query("""
                SELECT Usr;
            """)

    def test_edgeql_select_bad_reference_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"'default::User' has no link or property 'nam'",
                _hint="did you mean 'name'?"):

            self.con.query("""
                SELECT User.nam;
            """)

    def test_edgeql_select_bad_reference_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"object type or alias 'default::number' does not exist",
                _hint="did you mean '.number'?"):

            self.con.query("""
                select Issue filter number = '4418';
            """)

    def test_edgeql_select_bad_reference_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"object type or alias 'default::referrnce' does not exist",
                _hint="did you mean '.references'?"):

            self.con.query("""
                select Issue filter referrnce = '#4418';
            """)

    def test_edgeql_select_bad_reference_05(self):

        with self.assertRaisesRegex(
            edgedb.QueryError,
            "object type 'default::Issue' has no link or property 'referrnce'",
            _hint="did you mean 'references'?",
        ):
            self.con.query(
                """
            select Issue filter .referrnce = '#4418';
            """
            )

    def test_edgeql_select_precedence_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'index indirection cannot.*int64.*'):

            self.con.query("""
                # index access is higher precedence than cast
                SELECT <str>1[0];
            """)

    def test_edgeql_select_precedence_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError, r'index indirection cannot.*int64.*'):

            self.con.query("""
                # index access is higher precedence than cast
                SELECT <str>Issue.time_estimate[0];
            """)

    def test_edgeql_select_precedence_03(self):
        self.assert_query_result(
            r'''
            SELECT (<str>1)[0];
            ''',
            ['1'],
        )

        self.assert_query_result(
            r'''
            SELECT (<str>Issue.time_estimate)[0];
            ''',
            ['3'],
        )

    def test_edgeql_select_precedence_04(self):
        self.assert_query_result(
            r'''
            SELECT EXISTS Issue{number};
            ''',
            [True],
        )

        self.assert_query_result(
            r'''
            SELECT EXISTS Issue;
            ''',
            [True],
        )

    def test_edgeql_select_precedence_05(self):
        self.assert_query_result(
            r'''
            SELECT EXISTS Issue{number};
            ''',
            [True],
        )

    def test_edgeql_select_is_01(self):
        self.assert_query_result(
            r'''SELECT 5 IS int64;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5 IS anyint;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5 IS anyreal;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5 IS anyscalar;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5 IS int16;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5 IS float64;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5 IS anyfloat;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5 IS str;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5 IS Object;''',
            [False],
        )

    def test_edgeql_select_is_02(self):
        self.assert_query_result(
            r'''SELECT 5.5 IS int64;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS anyint;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS anyreal;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS anyscalar;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS int16;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS float64;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS anyfloat;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS str;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT 5.5 IS Object;''',
            [False],
        )

    def test_edgeql_select_is_03(self):

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS int64 LIMIT 1;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyint LIMIT 1;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyreal LIMIT 1;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyscalar LIMIT 1;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS int16 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS float64 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS anyfloat LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS str LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.time_estimate IS Object LIMIT 1;''',
            [False],
        )

    def test_edgeql_select_is_04(self):

        self.assert_query_result(
            r'''SELECT Issue.number IS int64 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS anyint LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS anyreal LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS anyscalar LIMIT 1;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS int16 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS float64 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS anyfloat LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS str LIMIT 1;''',
            [True],
        )

        self.assert_query_result(
            r'''SELECT Issue.number IS Object LIMIT 1;''',
            [False],
        )

    def test_edgeql_select_is_05(self):

        self.assert_query_result(
            r'''SELECT Issue.status IS int64 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS anyint LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS anyreal LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS anyscalar LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS int16 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS float64 LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS anyfloat LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS str LIMIT 1;''',
            [False],
        )

        self.assert_query_result(
            r'''SELECT Issue.status IS Object LIMIT 1;''',
            [True],
        )

    def test_edgeql_select_is_06(self):
        self.assert_query_result(
            r'''
            SELECT 5 IS anytype;
            ''',
            [True]
        )

    def test_edgeql_select_is_07(self):
        self.assert_query_result(
            r'''
            SELECT 5 IS anyint;
            ''',
            [True]
        )

    def test_edgeql_select_is_08(self):
        self.assert_query_result(
            r'''
            SELECT 5.5 IS anyfloat;
            ''',
            [True]
        )

    def test_edgeql_select_is_09(self):
        self.assert_query_result(
            r'''
            SELECT Issue.time_estimate IS anytype LIMIT 1;
            ''',
            [True]
        )

    def test_edgeql_select_is_10(self):
        self.assert_query_result(
            r'''
            SELECT [5] IS (array<anytype>);
            ''',
            [True]
        )

    def test_edgeql_select_is_11(self):
        self.assert_query_result(
            r'''
            SELECT (5, 'hello') IS (tuple<anytype, str>);
            ''',
            [True]
        )

    def test_edgeql_select_is_12(self):
        self.assert_query_result(
            r'''
            SELECT [5] IS (array<int64>);
            ''',
            [True],
        )

        self.assert_query_result(
            r'''
            SELECT (5, 'hello') IS (tuple<int64, str>);
            ''',
            [True],
        )

    # @test.xerror('IS is broken for runtime type checks of object collections')
    def test_edgeql_select_is_13(self):
        self.assert_query_result(
            r'''
            SELECT
                NOT all([Text] IS (array<Issue>))
                AND any([Text] IS (array<Issue>));
            ''',
            [True],
        )

    def test_edgeql_select_duplicate_definition_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "duplicate definition of property 'name' of object type "
            "'default::User'",
            _position=77,
        ):
            self.con.execute("""
                SELECT User {
                    name,
                    name
                }
            """)

    def test_edgeql_select_duplicate_definition_02(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "duplicate definition of property 'name' of object type "
            "'default::User'",
            _position=77,
        ):
            self.con.execute("""
                SELECT User {
                    name,
                    name := "new_name"
                }
            """)

    def test_edgeql_select_duplicate_definition_03(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "duplicate definition of link 'todo' of object type "
            "'default::User'",
            _position=77,
        ):
            self.con.execute("""
                SELECT User {
                    todo,
                    todo
                }
            """)

    def test_edgeql_select_missing_shape_field(self):
        with self.assertRaisesRegex(
            edgedb.InvalidReferenceError,
            "has no link or property",
            _position=51,
        ):
            self.con.execute("""
                SELECT User {
                    missing,
                }
            """)

    def test_edgeql_select_big_set_literal(self):
        res = self.con.query("""
            SELECT {
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
                 (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,), (1,),
            };
        """)

        assert len(res) == 100

    def test_edgeql_select_big_unions(self):
        res = self.con.query("""
            SELECT (
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,) union
                 (1,) union (1,) union (1,) union (1,) union (1,)
            );
        """)

        assert len(res) == 100

    def test_edgeql_select_set_literal_in_order(self):
        # *Technically speaking*, we don't necessarily promise
        # semantically that a set literal's elements be returned in
        # order. In practice, we want to, though.

        # Test for a range of lengths
        for n in (2, 4, 10, 25):
            s = list(range(n))
            self.assert_query_result(
                f"SELECT {set(s)}",
                s
            )

            us = ' union '.join(str(i) for i in s)
            self.assert_query_result(
                f"SELECT {us}",
                s
            )

    def test_edgeql_select_shape_on_scalar(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "shapes cannot be applied to scalar type 'std::str'",
        ):
            self.con.execute("""
                SELECT User {
                    todo: { name: {bogus} }
                }
            """)

    def test_edgeql_select_revlink_on_union(self):
        self.assert_query_result(
            """
                SELECT
                    File {
                        referrers := (
                            SELECT .<references[IS Issue] {
                                name,
                                number,
                            } ORDER BY .number
                        )
                    }
                FILTER
                    .name = 'screenshot.png'
            """,
            [
                {
                    'referrers': [
                        {
                            'name': 'Improve EdgeDB repl output rendering.',
                            'number': '2'
                        }
                    ]
                }
            ],
        )

    def test_edgeql_select_expr_objects_01(self):
        self.assert_query_result(
            r'''
                SELECT array_agg(Issue ORDER BY .body)[0].owner.name;
            ''',
            ["Elvis"],
        )

    def test_edgeql_select_expr_objects_02(self):
        self.assert_query_result(
            r'''
                SELECT _ := array_unpack(array_agg(Issue)).owner.name
                ORDER BY _;
            ''',
            ["Elvis", "Yury"],
        )

    def test_edgeql_select_expr_objects_03(self):
        self.con.execute(
            '''
                CREATE FUNCTION issues() -> SET OF Issue
                USING (Issue);
            '''
        )

        self.assert_query_result(
            r'''
                SELECT _ := issues().owner.name ORDER BY _;
            ''',
            ["Elvis", "Yury"],
        )

    # @test.xerror(
    #     "Known collation issue on Heroku Postgres",
    #     unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    # )
    def test_edgeql_select_expr_objects_04(self):
        self.assert_query_result(
            r'''
                WITH items := array_agg((SELECT Named ORDER BY .name))
                SELECT items[0] IS Status;
            ''',
            [True],
        )

        self.assert_query_result(
            r'''
                WITH items := array_agg((
                    SELECT Named ORDER BY .name LIMIT 1))
                SELECT (items, items[0], items[0].name,
                        items[0] IS Status);
            ''',
            [
                [[{}], {}, "Closed", True]
            ],
        )

        self.assert_query_result(
            r'''
                WITH items := (User.name, array_agg(User.todo ORDER BY .name))
                SELECT _ := (items.0, items.1, items.1[0].name) ORDER BY _.0;
            ''',
            [
                [
                    "Elvis",
                    [{}, {}],
                    "Improve EdgeDB repl output rendering."
                ],
                [
                    "Yury",
                    [{}, {}],
                    "Regression."
                ],
            ]
        )

    def test_edgeql_select_expr_objects_05(self):
        self.assert_query_result(
            r"""
            WITH
                L := ('x', User)
            SELECT (L, L);
            """,
            [
                [['x', {}], ['x', {}]],
                [['x', {}], ['x', {}]],
            ]
        )

    def test_edgeql_select_expr_objects_06(self):
        self.assert_query_result(
            r"""
            SELECT (User, User {name}) ORDER BY .1.name;
            """,
            [
                [{}, {'name': 'Elvis'}],
                [{}, {'name': 'Yury'}],
            ]
        )

    def test_edgeql_select_expr_objects_07(self):
        # get the User names and ids
        res = self.con.query(r'''
            SELECT User {
                name,
                id
            }
            ORDER BY User.name;
        ''')

        # we want to make sure that the reference to L is actually
        # populated with 'id', since there was a bug in which in JSON
        # mode it was populated with 'name' instead!
        self.assert_query_result(
            r'''
            WITH
                L := ('x', User),
            SELECT _ := (L, L.1 {name})
            ORDER BY _.1.name;
            ''',
            [
                [['x', {'id': str(user.id)}], {'name': user.name}]
                for user in res
            ]
        )

        self.assert_query_result(
            r'''
            WITH
                L := ('x', User),
            SELECT _ := (L.1 {name}, L)
            ORDER BY _.0.name;
            ''',
            [
                [{'name': user.name}, ['x', {'id': str(user.id)}]]
                for user in res
            ]
        )

    def test_edgeql_select_expr_objects_08(self):
        self.assert_query_result(
            r'''
            SELECT DISTINCT
                [(SELECT Issue {number, name} FILTER .number = "1")];
            ''',
            [
                [{'number': '1', 'name': 'Release EdgeDB'}],
            ]
        )

        self.assert_query_result(
            r'''
            SELECT DISTINCT
                ((SELECT Issue {number, name} FILTER .number = "1"),
                 Issue.status.name);
            ''',
            [
                [{'number': '1', 'name': 'Release EdgeDB'}, "Open"],
            ]
        )

    def test_edgeql_select_banned_free_shape_01(self):
        with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "it is illegal to create a type union that causes a "
            "computed property 'z' to mix with other versions of the "
            "same property 'z'"
        ):
            self.con.execute("""
                SELECT DISTINCT {{ z := 1 }, { z := 2 }};
            """)

        with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "cannot use DISTINCT on free shape",
        ):
            self.con.execute("""
                SELECT DISTINCT { z := 1 } = { z := 2 };
            """)

    def test_edgeql_select_array_common_type_01(self):
        res = self.con._fetchall("""
            SELECT [User, Issue];
        """, __typenames__=True)
        for row in res:
            self.assertEqual(row[0].__tname__, "default::User")
            self.assertEqual(row[1].__tname__, "default::Issue")

    def test_edgeql_select_array_common_type_02(self):
        res = self.con._fetchall("""
            SELECT [Object];
        """, __typenames__=True)
        for row in res:
            self.assertTrue(row[0].__tname__.startswith("default::"))

    def test_edgeql_select_free_shape_01(self):
        res = self.con.query_single('SELECT {test := 1}')
        self.assertEqual(res.test, 1)

    def test_edgeql_select_result_alias_binding_01(self):
        self.assert_query_result(
            r'''
                SELECT _ := (User { tag := User.name }) ORDER BY _.name;
            ''',
            [{"tag": "Elvis"}, {"tag": "Yury"}]
        )

    def test_edgeql_select_result_alias_binding_02(self):
        with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            "object type or alias 'default::_' does not exist",
        ):
            self.con.query(r'''
                SELECT _ := (User { tag := _.name });
            ''')

    def test_edgeql_select_reverse_overload_01(self):
        self.con.execute('''
            CREATE TYPE Dummy {
                CREATE LINK owner -> User;
            }
        ''')

        self.assert_query_result(
            r'''
                SELECT User {
                    z := (SELECT .<owner[IS Named] { name }
                          ORDER BY .name)
                } FILTER .name = 'Elvis';
            ''',
            [{"z": [{"name": "Regression."}, {"name": "Release EdgeDB"}]}],
        )

    def test_edgeql_select_reverse_overload_02(self):
        self.con.execute('''
            CREATE TYPE Dummy1 {
                CREATE MULTI LINK owner -> User;
            };
            CREATE TYPE Dummy2 {
                CREATE SINGLE LINK owner -> User;
            };
        ''')

        self.assert_query_result(
            r'''
                SELECT User {
                    z := (SELECT .<owner[IS Named] { name }
                          ORDER BY .name)
                } FILTER .name = 'Elvis';
            ''',
            [{"z": [{"name": "Regression."}, {"name": "Release EdgeDB"}]}],
        )

    def test_edgeql_select_bare_backlink_01(self):
        self.con.execute('''
            CREATE ABSTRACT TYPE Action;
            CREATE TYPE Post EXTENDING Action;
            CREATE TYPE Thing;
            ALTER TYPE Action {
                CREATE REQUIRED LINK thing -> Thing;
            };
            ALTER TYPE Thing {
                CREATE LINK posts := (.<thing);
            };
        ''')

        self.assert_query_result(
            r'''
                 SELECT Thing { posts: {id} };
            ''',
            [],
        )

    def test_edgeql_select_reverse_overload_03(self):
        self.con.execute('''
            CREATE TYPE Dummy1 {
                CREATE LINK whatever -> User;
            };
            CREATE TYPE Dummy2 {
                CREATE LINK whatever := (SELECT User FILTER .name = 'Elvis');
            };
            INSERT Dummy1 { whatever := (SELECT User FILTER .name = 'Yury') };
        ''')

        # We should be able to query the whatever backlink as long as we
        # filter it properly
        self.assert_query_result(
            r'''
                SELECT User.<whatever[IS Dummy1];
            ''',
            [{}],
        )

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot follow backlink 'whatever' because link 'whatever' "
                r"of object type 'default::Dummy2' is computed"):
            self.con.query(
                r'''
                    SELECT User.<whatever
                ''',
            )

    def test_edgeql_function_source_01a(self):
        # TODO: I think we might want to eliminate this sort of shape
        # propagation out of array_unpack instead?
        self.assert_query_result(
            r'''
                SELECT DISTINCT array_unpack([(
                    SELECT User {name} FILTER .name[0] = 'E'
                )]);
           ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_01b(self):
        self.assert_query_result(
            r'''
                SELECT (DISTINCT array_unpack([(
                    SELECT User FILTER .name[0] = 'E'
                )])) { name };
           ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_02(self):
        self.assert_query_result(
            r'''
                SELECT DISTINCT enumerate((
                    SELECT User {name} FILTER .name[0] = 'E'
                )).1;
            ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_03(self):
        self.assert_query_result(
            r'''
                SELECT assert_single(array_unpack([(
                    SELECT User FILTER .name[0] = 'E'
                )])) {name};
           ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_04(self):
        self.assert_query_result(
            r'''
                SELECT assert_distinct(array_unpack([(
                    SELECT User FILTER .name[0] = 'E'
                )])) {name} ;
           ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_05(self):
        self.assert_query_result(
            r'''
                SELECT assert_exists(array_unpack([(
                    SELECT User FILTER .name[0] = 'E'
                )])) {name};
            ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_06(self):
        self.assert_query_result(
            r'''
                SELECT enumerate(array_unpack([(
                    SELECT User FILTER .name[0] = 'E'
                )]) {name});
            ''',
            [[0, {"name": "Elvis"}]],
        )

    def test_edgeql_function_source_07(self):
        self.assert_query_result(
            r'''
                SELECT (enumerate((
                    SELECT User FILTER .name[0] = 'E'
                )).1 UNION (SELECT User FILTER false)) {name};
            ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_08(self):
        self.assert_query_result(
            r'''
                SELECT (enumerate((
                    SELECT User FILTER .name[0] = 'E'
                )).1 ?? (SELECT User FILTER false)) {name};
            ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_function_source_09(self):
        self.assert_query_result(
            r'''
                SELECT (enumerate((
                    SELECT User FILTER .name[0] = 'E'
                )).1 if 1 = 1 ELSE (SELECT User FILTER false)) {name};
            ''',
            [{"name": "Elvis"}],
        )

    def test_edgeql_collection_shape_01(self):
        self.assert_query_result(
            r'''
                SELECT <array<User>>{} UNION [User]
            ''',
            [[{"id": str}], [{"id": str}]],
        )

        self.assert_query_result(
            r'''
                SELECT <array<User>>{} ?? [User]
            ''',
            [[{"id": str}], [{"id": str}]],
        )

        self.assert_query_result(
            r'''
                SELECT <array<User>>{} IF false ELSE [User]
            ''',
            [[{"id": str}], [{"id": str}]],
        )

        self.assert_query_result(
            r'''
                SELECT assert_exists([User])
            ''',
            [[{"id": str}], [{"id": str}]],
        )

    def test_edgeql_collection_shape_02(self):
        self.assert_query_result(
            r'''
                SELECT <array<User>>{} UNION array_agg(User)
            ''',
            [[{"id": str}, {"id": str}]],
        )

        self.assert_query_result(
            r'''
                SELECT <array<User>>{} ?? array_agg(User)
            ''',
            [[{"id": str}, {"id": str}]],
        )

        self.assert_query_result(
            r'''
                SELECT <array<User>>{} IF false ELSE array_agg(User)
            ''',
            [[{"id": str}, {"id": str}]],
        )

        self.assert_query_result(
            r'''
                SELECT assert_exists(array_agg(User))
            ''',
            [[{"id": str}, {"id": str}]],
        )

    def test_edgeql_collection_shape_03(self):
        self.assert_query_result(
            r'''
                SELECT <tuple<User, int64>>{} UNION (User, 2)
            ''',
            [[{"id": str}, 2], [{"id": str}, 2]],
        )

        self.assert_query_result(
            r'''
                SELECT <tuple<User, int64>>{} ?? (User, 2)
            ''',
            [[{"id": str}, 2], [{"id": str}, 2]],
        )

        self.assert_query_result(
            r'''
                SELECT <tuple<User, int64>>{} IF false ELSE (User, 2)
            ''',
            [[{"id": str}, 2], [{"id": str}, 2]],
        )

        self.assert_query_result(
            r'''
                SELECT assert_exists((User, 2))
            ''',
            [[{"id": str}, 2], [{"id": str}, 2]],
        )

    def test_edgeql_collection_shape_04(self):
        self.assert_query_result(
            r'''
                SELECT [(User,)][0]
            ''',
            [[{"id": str}], [{"id": str}]]
        )

        self.assert_query_result(
            r'''
                SELECT [((SELECT User {name} ORDER BY .name),)][0]
            ''',
            [[{"name": "Elvis"}], [{"name": "Yury"}]]
        )

    def test_edgeql_collection_shape_05(self):
        self.assert_query_result(
            r'''
                SELECT ([User],).0
            ''',
            [[{"id": str}], [{"id": str}]]
        )

        self.assert_query_result(
            r'''
                SELECT ([(SELECT User {name} ORDER BY .name)],).0
            ''',
            [[{"name": "Elvis"}], [{"name": "Yury"}]]
        )

    def test_edgeql_collection_shape_06(self):
        self.assert_query_result(
            r'''
                SELECT { z := ([User],).0 }
            ''',
            [
                {"z": [[{"id": str}], [{"id": str}]]}
            ]
        )

    def test_edgeql_collection_shape_07(self):
        self.assert_query_result(
            r'''
                WITH Z := (<array<User>>{} IF false ELSE [User]),
                SELECT (Z, array_agg(array_unpack(Z))).1;
            ''',
            [[{"id": str}], [{"id": str}]]
        )

        self.assert_query_result(
            r'''
                WITH Z := (SELECT assert_exists([User]))
                SELECT (Z, array_agg(array_unpack(Z))).1;
            ''',
            [[{"id": str}], [{"id": str}]]
        )

    def test_edgeql_collection_shape_08(self):
        self.assert_query_result(
            r'''
                SELECT X := array_agg(User) FILTER X[0].name != 'Sully';
            ''',
            [[{"id": str}, {"id": str}]]
        )

        self.assert_query_result(
            r'''
            SELECT X := [User] FILTER X[0].name = 'Elvis';
            ''',
            [[{"id": str}]]
        )

    def test_edgeql_assert_fail_object_computed_01(self):
        # check that accessing a trivial computable on an object
        # that will fail to evaluate still fails

        with self.assertRaisesRegexTx(
            edgedb.CardinalityViolationError,
            "assert_exists violation",
        ):
            self.con.query("""
                SELECT assert_exists((SELECT User {m := 10} FILTER false)).m;
            """)

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "array index 1000 is out of bounds",
        ):
            self.con.query("""
                SELECT array_agg((SELECT User {m := Issue}))[{1000}].m;
            """)

        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "array index 1000 is out of bounds",
        ):
            self.con.query("""
                SELECT array_agg((SELECT User {m := 10}))[{1000}].m;
            """)

    # @test.xfail('''
    #     Publication is empty, and so even if we join in User to the result
    #     of the array dereference, that all gets optimized out on the pg
    #     side. I'm not really sure what we can reasonably do about this.
    # ''')
    def test_edgeql_assert_fail_object_computed_02(self):
        # Publication is empty, and so
        with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "array index 1000 is out of bounds",
        ):
            self.con.query("""
                SELECT array_agg((SELECT User {m := Publication}))[{1000}].m;
            """)

    def test_edgeql_select_call_null_01(self):
        # testing calls with null args
        self.con.execute('''
            create function foo(x: str, y: int64) -> str USING (x);
        ''')

        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := foo(.name, .val)
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': 'circle'},
                {'name': 'hexagon', 'val': 4, 'x': 'hexagon'},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': 'triangle'},
            ],
        )

    def test_edgeql_select_call_null_02(self):
        # testing calls with null args to a function that we can't mark
        # as strict
        self.con.execute('''
            create function foo(x: OPTIONAL str, y: int64) -> str USING (
                x ?? "test"
            );
        ''')

        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := foo(.name, .val)
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': 'circle'},
                {'name': 'hexagon', 'val': 4, 'x': 'hexagon'},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': 'triangle'},
            ],
        )

    def test_edgeql_select_concat_null_01(self):
        self.assert_query_result(
            r'''
            select BooleanTest {
                name,
                val,
                x := [.val] ++ [0]
            } order by .name;
            ''',
            [
                {'name': 'circle', 'val': 2, 'x': [2, 0]},
                {'name': 'hexagon', 'val': 4, 'x': [4, 0]},
                {'name': 'pentagon', 'val': None, 'x': None},
                {'name': 'square', 'val': None, 'x': None},
                {'name': 'triangle', 'val': 10, 'x': [10, 0]},
            ],
        )

    def test_edgeql_select_subshape_filter_01(self):
        # TODO: produce a better error message with a hint here?
        with self.assertRaisesRegexTx(
            edgedb.QueryError,
            "possibly an empty set returned",
        ):
            self.con.query(
                r'''
                SELECT Comment { owner: { name } FILTER false }
                ''',
            )

    def test_edgeql_select_null_tuple_01(self):
        self.con.execute('''
            CREATE TYPE Foo {
                CREATE PROPERTY y -> tuple<str, int64>;
                CREATE PROPERTY z -> tuple<x: int64, y: str>;
            };
            insert Foo;
        ''')

        self.assert_query_result(
            r'''
            select Foo { y, z }
            ''',
            [
                {'y': None, 'z': None}
            ],
        )

    def test_edgeql_select_null_tuple_02(self):
        self.assert_query_result(
            r'''
            SELECT { lol := array_get([(1, '2')], 1) }
            ''',
            [
                {'lol': None}
            ],
        )

        self.assert_query_result(
            r'''
            SELECT { lol := array_get([(a := 1, b := '2')], 1) }
            ''',
            [
                {'lol': None}
            ],
        )

    def test_edgeql_select_nested_order_01(self):
        self.assert_query_result(
            r'''
                SELECT
                  Issue {
                    key := (WITH c := Issue.name,
                            SELECT { name := c })
                  }
                ORDER BY .key.name;
            ''',
            [
                {"key": {"name": "Improve EdgeDB repl output rendering."}},
                {"key": {"name": "Regression."}},
                {"key": {"name": "Release EdgeDB"}},
                {"key": {"name": "Repl tweak."}}
            ]
        )

    def test_edgeql_select_nested_order_02(self):
        self.assert_query_result(
            r'''
                SELECT
                  Issue {
                    key := (WITH n := Issue.number, c := Issue.name,
                            SELECT { name := c, number := n })
                  }
                ORDER BY .key.number THEN .key.name;
            ''',
            [
                {"key": {"name": "Release EdgeDB", "number": "1"}},
                {
                    "key": {
                        "name": "Improve EdgeDB repl output rendering.",
                        "number": "2"
                    }
                },
                {"key": {"name": "Repl tweak.", "number": "3"}},
                {"key": {"name": "Regression.", "number": "4"}},
            ]
        )

    def test_edgeql_select_scalar_views_01(self):
        # Test the fix for #3525. I did not have a lot of luck
        # minimizing this one.
        self.con.execute('''
            CREATE TYPE default::Pair {
                CREATE REQUIRED PROPERTY similarity -> std::float64;
                CREATE REQUIRED PROPERTY word1 -> std::str;
                CREATE REQUIRED PROPERTY word2 -> std::str;
            };

            for tup in {
                ('hatch', 'foo', 0.5),
                ('hatch', 'bar', 0.5),
                ('hatch', 'baz', 0.5),

                ('balkanize', 'foo', 0.1),
                ('balkanize', 'bar', 0.2),
                ('balkanize', 'baz', 0.3),

                ('defenestrate', 'foo', 0.1),
                ('defenestrate', 'bar', 0.2),
                ('defenestrate', 'baz', 0.2),

            } union {
                (insert Pair { word1 := tup.0, word2 := tup.1,
                               similarity := tup.2 }),
                (insert Pair { word1 := tup.1, word2 := tup.0,
                               similarity := tup.2 }),
            };
        ''')

        self.assert_query_result(
            '''
            with
              options := {'balkanize', 'defenestrate'},
              word2 := (select Pair
                        filter .word1 = 'hatch' and .similarity = 0.5).word2,
            select options filter (
                with opt_pair := (
                    select Pair filter .word1 = options and .word2 in (word2)),
                select count(opt_pair) = count(distinct opt_pair.similarity)
            );
            ''',
            ['balkanize'],
        )

    def test_edgeql_select_scalar_views_02(self):
        self.assert_query_result(
            '''
            select (select {1,2} filter random() > 0) filter random() > 0
            ''',
            {1, 2},
        )

    def test_edgeql_select_scalar_views_03(self):
        # The thing this is testing for
        self.assert_query_result(
            '''
            select {1,2,3,4+0} filter random() > 0
            ''',
            {1, 2, 3, 4},
        )

    def test_edgeql_select_scalar_views_04(self):
        self.assert_query_result(
            '''
            for x in 2 union (select {1,x} filter random() > 0)
            ''',
            {1, 2}
        )

    def test_edgeql_with_rebind_01(self):
        self.assert_query_result(
            r'''
            WITH Z := (SELECT User { name })
            SELECT Z
            ''',
            [
                {'id': str}, {'id': str}
            ],
        )

    def test_edgeql_select_free_object_distinct_01(self):
        foo, bar = self.con.query_single('''
            select ({foo := "test"}, {bar := 1000})
        ''')
        self.assertNotEqual(foo.id, bar.id)

    def test_edgeql_select_free_object_distinct_02(self):
        vals = self.con.query('''
            for x in {1,2,3} union { asdf := 10*x };
        ''')
        self.assertEqual(len(vals), len({v.id for v in vals}))

    def test_edgeql_select_shadow_computable_01(self):
        # The thing this is testing for
        self.assert_query_result(
            '''
            SELECT User := User { name, is_elvis := User.name = 'Elvis' }
            ORDER BY User.is_elvis
            ''',
            [
                {"is_elvis": False, "name": "Yury"},
                {"is_elvis": True, "name": "Elvis"}
            ]
        )

    def test_edgeql_select_free_object_distinct_03(self):
        vals = self.con.query('''
            with w := {x := 10}
            for x in {1,2,3} union w
        ''')
        self.assertEqual(1, len({v.id for v in vals}))

    def test_edgeql_select_card_blowup_01(self):
        # This used to really blow up cardinality inference
        self.con.query('''
        SELECT Comment {
          issue := assert_exists(( .issue {
            status1 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status2 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status3 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status4 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status5 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status6 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status7 := ( .status { a := .__type__.name, b := .__type__.id } ),
            status8 := ( .status { a := .__type__.name, b := .__type__.id } ),
          })),
        };
        ''')

    def test_edgeql_shape_computed_alias_01(self):
        # Issue #4023 had this producing an incorrect key name in JSON mode
        self.assert_query_result(
            r'''
            select schema::Type {is_abstract} filter .name = 'std::Object';
            ''',
            [{"is_abstract": True}]
        )

    def test_edgeql_select_tname_overriden_type_01(self):
        # Test that overriding type doesn't break __tname__
        res = self.con._fetchall("""
            SELECT User { __type__ := introspect Issue }
        """, __typenames__=True)
        for row in res:
            self.assertEqual(row.__tname__, "default::User")

    def test_edgeql_select_paths_01(self):
        # This is OK because Issue.id is a property, not a link
        self.assert_query_result(
            r'''
                SELECT Issue.name
                FILTER Issue.number > '2';
            ''',
            tb.bag(["Repl tweak.", "Regression."]),
        )
