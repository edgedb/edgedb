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


import unittest  # NOQA

from edb.testbase import server as tb


class TestEdgeQLTutorial(tb.QueryTestCase):

    async def test_edgeql_tutorial(self):
        await self.assert_query_result('''
            CREATE MIGRATION m1 TO eschema $$
                type User:
                    required property login -> str:
                        constraint exclusive
                    required property firstname -> str
                    required property lastname -> str


                type PullRequest:
                    required property number -> int64:
                        constraint exclusive
                    required property title -> str
                    required property body -> str
                    required property status -> str
                    required property created_on -> datetime
                    required link author -> User
                    multi link assignees -> User:
                        on target delete set empty
                    multi link comments -> Comment:
                        on target delete set empty


                type Comment:
                    required property body -> str
                    required link author -> User
                    required property created_on -> datetime
            $$;
            COMMIT MIGRATION m1;

            CREATE MIGRATION m2 TO eschema $$
                type User:
                    required property login -> str:
                        constraint exclusive
                    required property firstname -> str
                    required property lastname -> str

                abstract type AuthoredText:
                    required property body -> str
                    required link author -> User
                    required property created_on -> datetime

                type PullRequest extending AuthoredText:
                    required property number -> int64:
                        constraint exclusive
                    required property title -> str
                    required property status -> str
                    multi link assignees -> User:
                        on target delete set empty
                    multi link comments -> Comment:
                        on target delete set empty

                type Comment extending AuthoredText
            $$;
            COMMIT MIGRATION m2;

            INSERT User {
                login := 'alice',
                firstname := 'Alice',
                lastname := 'Liddell',
            };

            INSERT User {
                login := 'bob',
                firstname := 'Bob',
                lastname := 'Sponge',
            };

            INSERT User {
                login := 'carol',
                firstname := 'Carol',
                lastname := 'Danvers',
            };

            INSERT User {
                login := 'dave',
                firstname := 'Dave',
                lastname := 'Bowman',
            };

            WITH
                Alice := (SELECT User FILTER .login = "alice"),
                Bob := (SELECT User FILTER .login = "bob")
            INSERT PullRequest {
                number := 1,
                title := "Avoid attaching multiple scopes at once",
                status := "Merged",
                author := Alice,
                assignees := Bob,
                body := "Sublime Text and Atom handles multiple " ++
                        "scopes differently.",
                created_on := <datetime>"Feb 1, 2016, 5:29PM UTC",
            };

            WITH
                Bob := (SELECT User FILTER .login = 'bob'),
                NewComment := (INSERT Comment {
                    author := Bob,
                    body := "Thanks for catching that.",
                    created_on :=
                    <datetime>'Feb 2, 2016, 12:47 PM UTC',
                })
            UPDATE PullRequest
            FILTER PullRequest.number = 1
            SET {
                comments := NewComment
            };

            WITH
                Bob := (SELECT User FILTER .login = 'bob'),
                Carol := (SELECT User FILTER .login = 'carol'),
                Dave := (SELECT User FILTER .login = 'dave')
            INSERT PullRequest {
                number := 2,
                title := 'Pyhton -> Python',
                status := 'Open',
                author := Carol,
                assignees := {Bob, Dave},
                body := "Several typos fixed.",
                created_on :=
                    <datetime>'Apr 25, 2016, 6:57 PM UTC',
                comments := {
                    (INSERT Comment {
                        author := Carol,
                        body := "Couple of typos are fixed. " ++
                                "Updated VS count.",
                        created_on := <datetime>'Apr 25, 2016, 6:58 PM UTC',
                    }),
                    (INSERT Comment {
                        author := Bob,
                        body := "Thanks for catching the typo.",
                        created_on := <datetime>'Apr 25, 2016, 7:11 PM UTC',
                    }),
                    (INSERT Comment {
                        author := Dave,
                        body := "Thanks!",
                        created_on := <datetime>'Apr 25, 2016, 7:22 PM UTC',
                    }),
                }
            };

            SELECT
                PullRequest {
                    title,
                    created_on,
                    author: {
                    login
                    },
                    assignees: {
                    login
                    }
                }
            FILTER
                .status = "Open"
            ORDER BY
                .created_on DESC;

            WITH
                name := 'bob'
            SELECT
                PullRequest {
                    title,
                    created_on,
                    num_comments := count(PullRequest.comments)
                }
            FILTER
                .author.login = name OR
                .comments.author.login = name
            ORDER BY
                .created_on DESC;

            SELECT AuthoredText {
                body,
                __type__: {
                    name
                }
            }
            FILTER .author.login = 'carol'
            ORDER BY .body;

            DELETE (
                SELECT AuthoredText
                FILTER .author.login = 'carol'
            );
        ''', [
            None,
            None,
            None,
            None,
            [{}],
            [{}],
            [{}],
            [{}],
            [{}],
            [{}],
            [{}],
            [{
                'assignees': [{'login': 'bob'}, {'login': 'dave'}],
                'author': {'login': 'carol'},
                'created_on': '2016-04-25T18:57:00+00:00',
                'title': 'Pyhton -> Python'
            }],
            [{
                'created_on': '2016-04-25T18:57:00+00:00',
                'num_comments': 3,
                'title': 'Pyhton -> Python'
            }, {
                'created_on': '2016-02-01T17:29:00+00:00',
                'num_comments': 1,
                'title': 'Avoid attaching multiple scopes at once'
            }],
            [{
                '__type__': {'name': 'default::Comment'},
                'body': 'Couple of typos are fixed. Updated VS count.'
            }, {
                '__type__': {'name': 'default::PullRequest'},
                'body': 'Several typos fixed.'
            }],
            [{}, {}],
        ])
