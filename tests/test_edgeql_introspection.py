##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.lang.common import datetime
from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestIntrospection(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'queries.eschema')

    SETUP = """
    """

    TEARDOWN = """
    """

    async def test_edgeql_introspection_concept01(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `Concept` {
                name
            }
            WHERE `Concept`.name LIKE 'test::%'
            ORDER BY `Concept`.name;
        """, [
            [
                {'name': 'test::Comment'},
                {'name': 'test::Dictionary'},
                {'name': 'test::Issue'},
                {'name': 'test::LogEntry'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Priority'},
                {'name': 'test::Status'},
                {'name': 'test::Text'},
                {'name': 'test::User'}
            ]
        ])

    async def test_edgeql_introspection_concept02(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `Concept` {
                name,
                is_abstract,
                links: {
                    name,
                } ORDER BY `Concept`.links.name
            }
            WHERE `Concept`.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'links': [{
                    'name': 'std::__class__',
                }, {
                    'name': 'std::id',
                }, {
                    'name': 'test::name',
                }, {
                    'name': 'test::todo',
                }]
            }]
        ])

    async def test_edgeql_introspection_concept03(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `Concept` {
                name,
                is_abstract,
                links: {
                    name,
                } ORDER BY `Concept`.links.name
            }
            WHERE `Concept`.name = 'test::Owned';
        """, [
            [{
                'name': 'test::Owned',
                'is_abstract': True,
                'links': [{
                    'name': 'std::__class__',
                }, {
                    'name': 'std::id',
                }, {
                    'name': 'test::owner',
                }]
            }]
        ])

    async def test_edgeql_introspection_concept04(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `Concept` {
                name,
                is_abstract,
                links: {
                    name,
                    attributes: {
                        name,
                        @value
                    } WHERE `Concept`.links.attributes.name = 'stdattrs::name'
                      ORDER BY `Attribute`.name
                } ORDER BY `Concept`.links.name
            }
            WHERE `Concept`.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'links': [{
                    'name': 'std::__class__',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'std::__class__'
                    }]
                }, {
                    'name': 'std::id',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'std::id'
                    }]
                }, {
                    'name': 'test::name',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'test::name'
                    }]
                }, {
                    'name': 'test::todo',
                    'attributes': [{
                        'name': 'stdattrs::name',
                        '@value': 'test::todo'
                    }]
                }]
            }]
        ])

    async def test_edgeql_introspection_concept05(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `Concept` {
                name,
                is_abstract,
                links: {
                    attributes: {
                        name,
                        @value
                    } WHERE EXISTS `Concept`.links.attributes@value
                      ORDER BY `Attribute`.name
                } WHERE `Concept`.links.name LIKE 'test::%'
                  ORDER BY `Link`.name
            }
            WHERE `Concept`.name = 'test::User';
        """, [
            [{
                'name': 'test::User',
                'is_abstract': False,
                'links': [{
                    'attributes': [
                        {'name': 'stdattrs::is_abstract', '@value': 'false'},
                        {'name': 'stdattrs::is_derived', '@value': 'false'},
                        {'name': 'stdattrs::is_final', '@value': 'false'},
                        {'name': 'stdattrs::mapping', '@value': '*1'},
                        {'name': 'stdattrs::name', '@value': 'test::name'},
                        {'name': 'stdattrs::readonly', '@value': 'false'},
                        {'name': 'stdattrs::required', '@value': 'true'},
                    ]
                }, {
                    'attributes': [
                        {'name': 'stdattrs::is_abstract', '@value': 'false'},
                        {'name': 'stdattrs::is_derived', '@value': 'false'},
                        {'name': 'stdattrs::is_final', '@value': 'false'},
                        {'name': 'stdattrs::mapping', '@value': '**'},
                        {'name': 'stdattrs::name', '@value': 'test::todo'},
                        {'name': 'stdattrs::readonly', '@value': 'false'},
                        {'name': 'stdattrs::required', '@value': 'false'},
                    ]
                }]
            }]
        ])

    async def test_edgeql_introspection_link01(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT `Link` {
                name,
                link_properties: {
                    name,
                } ORDER BY `Link`.link_properties.name
            }
            WHERE
                `Link`.name = 'test::todo'
                AND EXISTS `Link`.source;
        """, [
            [{
                'name': 'test::todo',
                'link_properties': [{
                    'name': 'std::linkid',
                }, {
                    'name': 'std::source',
                }, {
                    'name': 'std::target',
                }, {
                    'name': 'test::rank',
                }]
            }]
        ])

    async def test_edgeql_introspection_meta01(self):
        # make sure that ALL schema Classes are std::Objects
        res = await self.con.execute(r"""
            WITH MODULE schema
            SELECT `Class` IS std::Object;
        """)

        self.assert_data_shape(res[0], [True] * len(res[0]))

    async def test_edgeql_introspection_meta02(self):
        await self.assert_query_result(r"""
            WITH MODULE schema
            SELECT Class {
                name
            }
            WHERE Class.name ~ '^test::\w+$'
            ORDER BY Class.name;
        """, [
            [
                {'name': 'test::body'},
                {'name': 'test::Comment'},
                {'name': 'test::Dictionary'},
                {'name': 'test::due_date'},
                {'name': 'test::Issue'},
                {'name': 'test::issue'},
                {'name': 'test::issue_num_t'},
                {'name': 'test::LogEntry'},
                {'name': 'test::name'},
                {'name': 'test::Named'},
                {'name': 'test::number'},
                {'name': 'test::Owned'},
                {'name': 'test::owner'},
                {'name': 'test::parent'},
                {'name': 'test::Priority'},
                {'name': 'test::priority'},
                {'name': 'test::rank'},
                {'name': 'test::related_to'},
                {'name': 'test::spent_time'},
                {'name': 'test::start_date'},
                {'name': 'test::Status'},
                {'name': 'test::status'},
                {'name': 'test::Text'},
                {'name': 'test::time_estimate'},
                {'name': 'test::time_spent_log'},
                {'name': 'test::todo'},
                {'name': 'test::User'},
                {'name': 'test::watchers'},
            ]
        ])
