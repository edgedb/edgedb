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


import os.path
import unittest  # NOQA

from edb.server import _testbase as tb
from edb.client import exceptions as exc  # NOQA


class TestEdgeQLTutorial(tb.QueryTestCase):
    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'tutorial.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'tutorial.eql')

    async def test_edgeql_tutorial_query_01(self):
        await self.assert_query_result(r'''
            SELECT User.login;
        ''', [
            {'alice', 'bob', 'carol', 'dave'}
        ])

    async def test_edgeql_tutorial_query_02(self):
        await self.assert_query_result(r'''
            SELECT
                PullRequest {
                    title
                }
            FILTER
                'alice' IN .comments.author.login
                AND
                'bob' IN .comments.author.login
            ORDER BY
                .title;
        ''', [[
            {
                'title': 'Avoid attaching multiple scopes at once',
            },
            {
                'title': 'Make file types consistent between '
                         'grammars/ and package.json'
            }
        ]])

    async def test_edgeql_tutorial_query_03(self):
        await self.assert_query_result(r'''
            SELECT
                User.followees {
                    fullname
                }
            ORDER BY
                User.followees.login;
        ''', [[
            {'fullname': 'Alice Liddell'},
            {'fullname': 'Bob Sponge'},
            {'fullname': 'Carol Danvers'},
            {'fullname': 'Dave Bowman'},
        ]])

    async def test_edgeql_tutorial_query_04(self):
        await self.assert_query_result(r'''
            # Get all "Open" PullRequests, their authors, and who they are
            # assigned to, in reverse chronological order.
            SELECT
                PullRequest {
                    title,
                    created_on,
                    author: {
                        fullname
                    },
                    assignees: {
                        fullname
                    }
                }
            FILTER
                .status = "Open"
            ORDER BY
                .created_on DESC;
        ''', [
            [
                {
                    'title': 'Add support for Snakemake files',
                    'author': {'fullname': 'Carol Danvers'},
                    'assignees': [{'fullname': 'Dave Bowman'}],
                    'created_on': '2018-01-24T18:19:00+00:00'
                },
                {
                    'title': 'Tokenize `@` in decorators',
                    'author': {'fullname': 'Bob Sponge'},
                    'assignees': [{'fullname': 'Dave Bowman'}],
                    'created_on': '2017-11-13T12:39:00+00:00'
                }
            ]
        ])

    async def test_edgeql_tutorial_query_05(self):
        expected_results = [
            {'login': 'dave', 'lastname': 'Bowman', 'firstname': 'Dave'}
        ]
        await self.assert_query_result(r'''
            # Get all Users who have assigned open PullRequests.
            SELECT
                (
                    SELECT
                        PullRequest
                    FILTER
                        .status = "Open"
                ).assignees {
                    login,
                    firstname,
                    lastname
                };

            # equivalently
            SELECT
                User {
                    login,
                    firstname,
                    lastname
                }
            FILTER
                .<assignees[IS PullRequest].status = "Open";
        ''', [expected_results, expected_results])

    async def test_edgeql_tutorial_query_06(self):
        await self.assert_query_result(r'''
            # User activity
            #
            # Get all Users, the total number of PullRequests and the
            # total number of Comments they created.
            SELECT
                User {
                    login,
                    fullname,
                    total_prs := count(User.<author[IS PullRequest]),
                    total_comments := count(User.<author[IS Comment])
                }
            ORDER BY
                .fullname;
        ''', [
            [
                {
                    'login': 'alice',
                    'fullname': 'Alice Liddell',
                    'total_prs': 2,
                    'total_comments': 6
                },
                {
                    'login': 'bob',
                    'fullname': 'Bob Sponge',
                    'total_prs': 2,
                    'total_comments': 7
                },
                {
                    'login': 'carol',
                    'fullname': 'Carol Danvers',
                    'total_prs': 2,
                    'total_comments': 6
                },
                {
                    'login': 'dave',
                    'fullname': 'Dave Bowman',
                    'total_prs': 0,
                    'total_comments': 1
                }
            ]
        ])

    async def test_edgeql_tutorial_query_07(self):
        await self.assert_query_result(r'''
            # User notification
            #
            # For a specific User, get all PullRequests that the User has
            # commented on or authored.
            WITH
                name := 'alice'
            SELECT
                PullRequest {
                    title,
                    created_on,
                    num_comments := count(PullRequest.comments)
                }
            FILTER
                .author.login = name
                OR .comments.author.login = name
                OR .assignees.login = name
            ORDER BY
                .created_on DESC;
        ''', [
            [
                {
                    'title':
                        'Make file types consistent between grammars' +
                        '/ and package.json',
                    'created_on': '2016-11-22T14:26:00+00:00',
                    'num_comments': 7
                },
                {
                    'title': 'Avoid attaching multiple scopes at once',
                    'created_on': '2016-02-01T17:29:00+00:00',
                    'num_comments': 3
                }
            ]
        ])

    async def test_edgeql_tutorial_query_08(self):
        await self.assert_query_result(r'''
            # Bechdel test
            #
            # Specifically, get all PullRequests such that they are
            # authored by a specific subset of users and have comments
            # from one of them (who is not the author).
            WITH
                names := {'alice', 'carol'}
            SELECT
                PullRequest {
                    title,
                    created_on
                }
            FILTER
                # It's important to use `IN` as opposed to `=` here, since
                # otherwise the set `names` in the second half of the AND would
                # represent the name already matched to the author.
                .author.login IN names
                AND .comments.author.login = (
                    SELECT names FILTER names != PullRequest.author.login
                );
        ''', [
            [
                {
                    'title':
                        'Make file types consistent between grammars' +
                        '/ and package.json',
                    'created_on': '2016-11-22T14:26:00+00:00',
                }
            ]
        ])

    async def test_edgeql_tutorial_query_09(self):
        await self.assert_query_result(r'''
            # Followers
            #
            # Get all the users and their followers.
            SELECT
                User {
                    fullname,
                    follower := User.followees.fullname
                }
            ORDER BY
                .login;
        ''', [
            [
                {
                    'follower': {'Bob Sponge', 'Carol Danvers', 'Dave Bowman'},
                    'fullname': 'Alice Liddell'
                },
                {
                    'follower': {},
                    'fullname': 'Bob Sponge'
                },
                {
                    'follower': {'Alice Liddell', 'Bob Sponge'},
                    'fullname': 'Carol Danvers'
                },
                {
                    'follower': {'Bob Sponge', 'Carol Danvers'},
                    'fullname': 'Dave Bowman'
                },
            ]
        ])

    async def test_edgeql_tutorial_query_10(self):
        await self.assert_query_result(r'''
            # Following
            #
            # For a specific user, get all PullRequests authored by people whom
            # this user follows.
            SELECT
                User {
                    fullname,
                    following := (
                        SELECT
                            User.followees.<author[IS PullRequest] {
                                title,
                                created_on
                            }
                        ORDER BY .created_on DESC
                    ),
                }
            ORDER BY
                .login;
        ''', [
            [
                {
                    'fullname': 'Alice Liddell',
                    'following': [
                        {
                            'title': 'Add support for Snakemake files',
                            'created_on': '2018-01-24T18:19:00+00:00'
                        },
                        {
                            'title': 'Tokenize `@` in decorators',
                            'created_on': '2017-11-13T12:39:00+00:00'
                        },
                        {
                            'title': 'Highlight type hint stub files',
                            'created_on': '2016-06-28T19:31:00+00:00'
                        },
                        {
                            'title': 'Pyhton -> Python',
                            'created_on': '2016-04-25T18:57:00+00:00'
                        }
                    ]
                },
                {
                    'fullname': 'Bob Sponge',
                    'following': []
                },
                {
                    'fullname': 'Carol Danvers',
                    'following': [
                        {
                            'title': 'Tokenize `@` in decorators',
                            'created_on': '2017-11-13T12:39:00+00:00'
                        },
                        {
                            'title':
                                'Make file types consistent between ' +
                                'grammars/ and package.json',
                            'created_on': '2016-11-22T14:26:00+00:00'
                        },
                        {
                            'title': 'Highlight type hint stub files',
                            'created_on': '2016-06-28T19:31:00+00:00'
                        },
                        {
                            'title': 'Avoid attaching multiple scopes at once',
                            'created_on': '2016-02-01T17:29:00+00:00'
                        }
                    ]
                },
                {
                    'fullname': 'Dave Bowman',
                    'following': [
                        {
                            'title': 'Add support for Snakemake files',
                            'created_on': '2018-01-24T18:19:00+00:00'
                        },
                        {
                            'title': 'Tokenize `@` in decorators',
                            'created_on': '2017-11-13T12:39:00+00:00'
                        },
                        {
                            'title': 'Highlight type hint stub files',
                            'created_on': '2016-06-28T19:31:00+00:00'
                        },
                        {
                            'title': 'Pyhton -> Python',
                            'created_on': '2016-04-25T18:57:00+00:00'
                        }
                    ]
                }
            ]
        ])
