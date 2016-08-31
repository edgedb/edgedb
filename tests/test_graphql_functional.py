##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestGraphQLFunctional(tb.QueryTestCase):
    SETUP = """
        CREATE DELTA test::d1 TO $$
            abstract concept NamedObject:
                required link name to str

            concept Group extends NamedObject:
                link settings to Setting:
                    mapping: **

            concept Setting extends NamedObject:
                required link value to str

            concept User extends NamedObject:
                required link active to bool
                link groups to Group:
                    mapping: **
                required link age to int
                required link score to float
        $$;

        COMMIT DELTA test::d1;

        USING NAMESPACE test
        INSERT {Setting} {
            name := 'template',
            value := 'blue'
        };

        USING NAMESPACE test
        INSERT {Setting} {
            name := 'perks',
            value := 'full'
        };

        USING NAMESPACE test
        INSERT {Group} {
            name := 'basic'
        };

        USING NAMESPACE test
        INSERT {Group} {
            name := 'upgraded'
        };

        USING NAMESPACE test
        INSERT User {
            name := 'John',
            age := 25,
            active := True,
            score := 3.14,
            groups := (SELECT {Group} WHERE {Group}.name = 'basic')
        };

        USING NAMESPACE test
        INSERT User {
            name := 'Jane',
            age := 26,
            active := True,
            score := 1.23,
            groups := (SELECT {Group} WHERE {Group}.name = 'upgraded')
        };

        USING NAMESPACE test
        INSERT User {
            name := 'Alice',
            age := 27,
            active := True,
            score := 5
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
                # XXX: pretty sure this is not right and needs fixing
                'groups': [{
                    'id': None,
                    'name': None,
                }]
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

    async def test_graphql_functional_query04(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                User(groups__name: "upgraded") {
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
                'name': 'Jane',
                'age': 26,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'upgraded',
                }]
            }],
        ])

    async def test_graphql_functional_query05(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                User(groups__name__in: ["upgraded", "basic"]) {
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

    async def test_graphql_functional_query06(self):
        result = await self.con.execute(r"""
            query @edgedb(module: "test") {
                User(age__ne: 26, name__in: ["Alice", "Jane"]) {
                    name
                    age
                    score
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

