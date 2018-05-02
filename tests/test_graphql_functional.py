##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.server import _testbase as tb


class TestGraphQLFunctional(tb.QueryTestCase):
    SETUP = """
        CREATE MIGRATION test::d1 TO eschema $$
            abstract type NamedObject:
                required property name -> str

            type UserGroup extending NamedObject:
                link settings -> Setting:
                    cardinality := '**'

            type Setting extending NamedObject:
                required property value -> str

            type Profile extending NamedObject:
                required property value -> str

            type User extending NamedObject:
                required property active -> bool
                link groups -> UserGroup:
                    cardinality := '**'
                required property age -> int64
                required property score -> float64
                link profile -> Profile:
                    cardinality := '*1'
        $$;

        COMMIT MIGRATION test::d1;

        WITH MODULE test
        INSERT Setting {
            name := 'template',
            value := 'blue'
        };

        WITH MODULE test
        INSERT Setting {
            name := 'perks',
            value := 'full'
        };

        WITH MODULE test
        INSERT UserGroup {
            name := 'basic'
        };

        WITH MODULE test
        INSERT UserGroup {
            name := 'upgraded'
        };

        WITH MODULE test
        INSERT User {
            name := 'John',
            age := 25,
            active := True,
            score := 3.14,
            groups := (SELECT UserGroup FILTER UserGroup.name = 'basic')
        };

        WITH MODULE test
        INSERT User {
            name := 'Jane',
            age := 26,
            active := True,
            score := 1.23,
            groups := (SELECT UserGroup FILTER UserGroup.name = 'upgraded')
        };

        WITH MODULE test
        INSERT User {
            name := 'Alice',
            age := 27,
            active := True,
            score := 5.0
        };
    """

    async def test_graphql_functional_query_01(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                Setting {
                    name
                    value
                }
            }
        """, graphql=True)

        result[0][0]['Setting'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'Setting': [{
                'name': 'perks',
                'value': 'full',
            }, {
                'name': 'template',
                'value': 'blue',
            }],
        }]])

    async def test_graphql_functional_query_02(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                User {
                    name
                    age
                    groups {
                        id
                        name
                    }
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Alice',
                'age': 27,
                'groups': None
            }, {
                'name': 'Jane',
                'age': 26,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }, {
                'name': 'John',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                }]
            }],
        }]])

    async def test_graphql_functional_query_03(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                User(name: "John") {
                    name
                    age
                    groups {
                        id
                        name
                    }
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'John',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                }]
            }],
        }]])

    async def test_graphql_functional_fragment_02(self):
        result = await self.con.execute(r"""
            fragment userFrag on User @edgedb(module: "test") {
                age
                score
            }

            query @edgedb(module: "test") {
                NamedObject(name: "Alice") {
                    name
                    ... userFrag
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            'NamedObject': [{
                'name': 'Alice',
                'age': 27,
                'score': 5,
            }],
        }]])

    async def test_graphql_functional_typename_01(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                User {
                    name
                    __typename
                    groups {
                        id
                        name
                        __typename
                    }
                }
            }
        """, graphql=True)

        result[0][0]['User'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            'User': [{
                'name': 'Alice',
                '__typename': 'test::User',
                'groups': None
            }, {
                'name': 'Jane',
                '__typename': 'test::User',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                    '__typename': 'test::UserGroup',
                }]
            }, {
                'name': 'John',
                '__typename': 'test::User',
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                    '__typename': 'test::UserGroup',
                }]
            }],
        }]])

    async def test_graphql_functional_typename_02(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                __typename
                __schema {
                    __typename
                }
            }
        """, graphql=True)

        self.assert_data_shape(result, [[{
            '__typename': 'Query',
            '__schema': {
                '__typename': '__Schema',
            },
        }]])

    async def test_graphql_functional_schema_01(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                __schema {
                    directives {
                        name
                        description
                        locations
                        args {
                            name
                            description
                            defaultValue
                        }
                    }
                }
            }
        """, graphql=True)

        result[0][0]['__schema']['directives'].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [[{
            '__schema': {
                'directives': [
                    {
                        'name': 'deprecated',
                        'locations': {'FIELD'},
                        'description':
                            'Marks an element of a GraphQL schema as no ' +
                            'longer supported.',
                        'args': [
                            {
                                'name': 'reason',
                                'description':
                                    "Explains why this element was " +
                                    "deprecated, usually also including " +
                                    "a suggestion for how to access " +
                                    "supported similar data. Formatted " +
                                    "in [Markdown](https://daringfireba" +
                                    "ll.net/projects/markdown/).",
                                'defaultValue': '"No longer supported"'
                            }
                        ],
                    },
                    {
                        'name': 'edgedb',
                        'locations': {'QUERY', 'MUTATION',
                                      'FRAGMENT_DEFINITION', 'FRAGMENT_SPREAD',
                                      'INLINE_FRAGMENT'},
                        'description':
                            'Special EdgeDB compatibility directive that ' +
                            'specifies which module is being used.',
                        'args': [
                            {
                                'name': 'module',
                                'description':
                                    'EdgeDB module that needs to be ' +
                                    'accessed by the query.',
                                'defaultValue': None
                            }
                        ],
                    },
                    {
                        'name': 'include',
                        'locations': {'FIELD', 'FRAGMENT_SPREAD',
                                      'INLINE_FRAGMENT'},
                        'description':
                            'Directs the executor to include this field or ' +
                            'fragment only when the `if` argument is true.',
                        'args': [
                            {
                                'name': 'if',
                                'description': 'Included when true.',
                                'defaultValue': None
                            }
                        ],
                    },
                    {
                        'name': 'skip',
                        'locations': {'FIELD', 'FRAGMENT_SPREAD',
                                      'INLINE_FRAGMENT'},
                        'description':
                            'Directs the executor to skip this field or ' +
                            'fragment when the `if` argument is true.',
                        'args': [
                            {
                                'name': 'if',
                                'description': 'Excluded when true.',
                                'defaultValue': None
                            }
                        ],
                    },
                ],
            }
        }]])
