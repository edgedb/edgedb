##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

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
                {'name': 'test::File'},
                {'name': 'test::Issue'},
                {'name': 'test::LogEntry'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Priority'},
                {'name': 'test::Publication'},
                {'name': 'test::Status'},
                {'name': 'test::Text'},
                {'name': 'test::URL'},
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
                      ORDER BY `Concept`.links.attributes.name
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
                      ORDER BY `Concept`.links.attributes.name
                } WHERE `Concept`.links.name LIKE 'test::%'
                  ORDER BY `Concept`.links.name
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

    async def test_edgeql_introspection_concept06(self):
        await self.assert_query_result(r"""
            # get all links, mappings and target names for Comment
            WITH MODULE schema
            SELECT `Concept` {
                name,
                links: {
                    name,
                    `target`: {
                        name
                    },
                    attributes: {
                        name,
                        @value
                    } WHERE
                        `Concept`.links.attributes.name LIKE '%mapping'
                } ORDER BY `Concept`.links.name
            }
            WHERE `Concept`.name LIKE '%Comment';
        """, [
            [{
                'name': 'test::Comment',
                'links': [{
                    'name': 'std::__class__',
                    'target': {'name': 'schema::Class'},
                    'attributes': [{
                        'name': 'stdattrs::mapping',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'std::id',
                    'target': {'name': 'std::uuid'},
                    'attributes': [{
                        'name': 'stdattrs::mapping',
                        '@value': '11',
                    }],
                }, {
                    'name': 'test::body',
                    'target': {'name': 'std::str'},
                    'attributes': [{
                        'name': 'stdattrs::mapping',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'test::issue',
                    'target': {'name': 'test::Issue'},
                    'attributes': [{
                        'name': 'stdattrs::mapping',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'test::owner',
                    'target': {'name': 'test::User'},
                    'attributes': [{
                        'name': 'stdattrs::mapping',
                        '@value': '*1',
                    }],
                }, {
                    'name': 'test::parent',
                    'target': {'name': 'test::Comment'},
                    'attributes': [{
                        'name': 'stdattrs::mapping',
                        '@value': '*1',
                    }],
                }]
            }]
        ])

    async def test_edgeql_introspection_concept07(self):
        await self.assert_query_result(r"""
            # get all user-defined concepts with at least one ** link
            WITH MODULE schema
            SELECT `Concept` {
                name,
            }
            WHERE
                `Concept`.name LIKE 'test::%'
                AND
                `Concept`.links.attributes.name = 'stdattrs::mapping'
                AND
                `Concept`.links.attributes@value = '**'
            ORDER BY `Concept`.name;
        """, [
            [{
                'name': 'test::Issue',
            }, {
                'name': 'test::User',
            }]
        ])

    async def test_edgeql_introspection_concept08(self):
        await self.assert_query_result(r"""
            # get all user-defined concepts with at least one 1* link
            WITH MODULE schema
            SELECT `Concept` {
                name,
            }
            WHERE
                `Concept`.name LIKE 'test::%'
                AND
                `Concept`.links.attributes.name = 'stdattrs::mapping'
                AND
                `Concept`.links.attributes@value = '1*'
            ORDER BY `Concept`.name;
        """, [
            [{
                'name': 'test::Issue',
            }]
        ])

    async def test_edgeql_introspection_concept09(self):
        await self.assert_query_result(r"""
            # get all user-defined concepts with at least one 1* link
            WITH MODULE schema
            SELECT `Concept` {
                name,
            }
            WHERE
                `Concept`.name LIKE 'test::%'
                AND
                `Concept`.<target[IS `Link`].attributes.name =
                    'stdattrs::mapping'
                AND
                `Concept`.<target[IS `Link`].attributes@value = '1*'
            ORDER BY `Concept`.name;
        """, [
            [{
                'name': 'test::LogEntry',
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
                {'name': 'test::Comment'},
                {'name': 'test::Dictionary'},
                {'name': 'test::File'},
                {'name': 'test::Issue'},
                {'name': 'test::LogEntry'},
                {'name': 'test::Named'},
                {'name': 'test::Owned'},
                {'name': 'test::Priority'},
                {'name': 'test::Publication'},
                {'name': 'test::Status'},
                {'name': 'test::Text'},
                {'name': 'test::URL'},
                {'name': 'test::User'},
                {'name': 'test::address'},
                {'name': 'test::body'},
                {'name': 'test::due_date'},
                {'name': 'test::issue'},
                {'name': 'test::issue_num_t'},
                {'name': 'test::name'},
                {'name': 'test::number'},
                {'name': 'test::owner'},
                {'name': 'test::parent'},
                {'name': 'test::priority'},
                {'name': 'test::rank'},
                {'name': 'test::references'},
                {'name': 'test::related_to'},
                {'name': 'test::spent_time'},
                {'name': 'test::start_date'},
                {'name': 'test::status'},
                {'name': 'test::time_estimate'},
                {'name': 'test::time_spent_log'},
                {'name': 'test::title'},
                {'name': 'test::todo'},
                {'name': 'test::watchers'},
            ]
        ])

    async def test_edgeql_introspection_count01(self):
        await self.con.execute(r"""
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
            INSERT Status {
                name := 'Flagged'
            };

            WITH MODULE test
            INSERT User {
                name := 'Elvis'
            };

            WITH MODULE test
            INSERT User {
                name := 'Yury'
            };
        """)

        await self.assert_query_result(r"""
            # Count the number of objects for each concept in module
            # test. This is impossible to do without introspection for
            # concepts that have 0 objects.
            #
            WITH MODULE schema
            SELECT `Concept` {
                name,
                count := (
                    SELECT SINGLETON std::count(`Concept`.<__class__)
                )
            }
            WHERE `Concept`.name LIKE 'test::%'
            ORDER BY `Concept`.name;
        """, [
            [
                {'name': 'test::Comment', 'count': 0},
                {'name': 'test::Dictionary', 'count': 0},
                {'name': 'test::File', 'count': 0},
                {'name': 'test::Issue', 'count': 0},
                {'name': 'test::LogEntry', 'count': 0},
                {'name': 'test::Named', 'count': 0},
                {'name': 'test::Owned', 'count': 0},
                {'name': 'test::Priority', 'count': 2},
                {'name': 'test::Publication', 'count': 0},
                {'name': 'test::Status', 'count': 3},
                {'name': 'test::Text', 'count': 0},
                {'name': 'test::URL', 'count': 0},
                {'name': 'test::User', 'count': 2},
            ]
        ])

        await self.con.execute(r"""
            DELETE test::Priority;
            DELETE test::Status;
            DELETE test::User;
        """)
