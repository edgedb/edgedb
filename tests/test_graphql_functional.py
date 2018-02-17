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
            abstract concept NamedObject:
                required link name to str

            concept UserGroup extending NamedObject:
                link settings to Setting:
                    mapping := '**'

            concept Setting extending NamedObject:
                required link value to str

            concept Profile extending NamedObject:
                required link value to str

            concept User extending NamedObject:
                required link active to bool
                link groups to UserGroup:
                    mapping := '**'
                required link age to int
                required link score to float
                link profile to Profile:
                    mapping := '*1'
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

    async def test_graphql_functional_query01(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                Setting {
                    name
                    value
                }
            }
        """, graphql=True)

        result[0].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [
            [{
                'name': 'perks',
                'value': 'full',
            }, {
                'name': 'template',
                'value': 'blue',
            }],
        ])

    async def test_graphql_functional_query02(self):
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

        result[0].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [
            [{
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
        ])

    async def test_graphql_functional_query03(self):
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

        self.assert_data_shape(result, [
            [{
                'name': 'John',
                'age': 25,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'basic',
                }]
            }],
        ])

    async def test_graphql_functional_fragment02(self):
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

        self.assert_data_shape(result, [
            [{
                'name': 'Alice',
                'age': 27,
                'score': 5,
            }],
        ])
