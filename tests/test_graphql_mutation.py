##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.server import _testbase as tb


class TestGraphQLMutation(tb.QueryTestCase):
    SETUP = """
        CREATE MIGRATION test::d1 TO eschema $$
            abstract concept NamedObject:
                required link name to str

            concept Group extending NamedObject:
                link settings to Setting:
                    mapping: **

            concept Setting extending NamedObject:
                required link value to str

            concept Profile extending NamedObject:
                required link value to str

            concept User extending NamedObject:
                required link active to bool
                link groups to Group:
                    mapping: **
                required link age to int
                required link score to float
                link profile to Profile:
                    mapping: *1
        $$;

        COMMIT MIGRATION test::d1;
    """

    SETUP_METHOD = r"""
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
            INSERT `Group` {
                name := 'basic'
            };

            WITH MODULE test
            INSERT `Group` {
                name := 'upgraded'
            };

            WITH MODULE test
            INSERT User {
                name := 'John',
                age := 25,
                active := True,
                score := 3.14,
                groups := (SELECT `Group` FILTER `Group`.name = 'basic')
            };

            WITH MODULE test
            INSERT User {
                name := 'Jane',
                age := 26,
                active := True,
                score := 1.23,
                groups := (SELECT `Group` FILTER `Group`.name = 'upgraded')
            };

            WITH MODULE test
            INSERT User {
                name := 'Alice',
                age := 27,
                active := True,
                score := 5
            };
    """

    TEARDOWN_METHOD = """
            DELETE test::Setting;
            DELETE test::Group;
            DELETE test::User;
    """

    async def test_graphql_mutation_delete01(self):
        result = await self.con.execute(r"""
            mutation @edgedb(module: "test") {
                delete__Setting {
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

    async def test_graphql_mutation_delete02(self):
        result = await self.con.execute(r"""
            mutation del @edgedb(module: "test") {
                delete__User(name: "John") {
                    name
                }
            }

            query sel @edgedb(module: "test") {
                User {
                    name
                    groups {
                        name
                    }
                }
            }
        """, graphql=True)

        result[0].sort(key=lambda x: x['name'])
        result[1].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [
            [{
                'name': 'John',
            }],
            [{
                'name': 'Alice',
            }, {
                'name': 'Jane',
                'groups': [{
                    'name': 'upgraded',
                }],
            }],
        ])

    async def test_graphql_mutation_delete03(self):
        result = await self.con.execute(r"""
            mutation @edgedb(module: "test") {
                delete__User(name: "John", active: true) {
                    name
                    groups {
                        name
                    }
                }
            }
        """, graphql=True)

        result[0].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [
            [{
                'name': 'John',
                'groups': [{
                    'name': 'basic'
                }],
            }],
        ])

    async def test_graphql_mutation_delete04(self):
        result = await self.con.execute(r"""
            mutation @edgedb(module: "test") {
                delete__User(name: "John", active: false) {
                    name
                    groups {
                        name
                    }
                }
            }
        """, graphql=True)

        result[0].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [
            [],
        ])

    async def test_graphql_mutation_insert01(self):
        result = await self.con.execute(r"""
            mutation @edgedb(module: "test") {
                insert__Group(__data: {
                    name: "new"
                }) {
                    id
                    name
                }
            }
        """, graphql=True)

        result[0]
        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
                'name': 'new',
            }],
        ])

    async def test_graphql_mutation_insert02(self):
        groups = await self.con.execute(r"""
            query @edgedb(module: "test") {
                Group(name: "basic") {
                    id,
                }
            }
        """, graphql=True)

        result = await self.con.execute(r'''
            mutation in1 @edgedb(module: "test") {
                insert__User(__data: {
                    name: "Bob",
                    active: true,
                    age: 25,
                    score: 2.34,
                    groups__id: "''' + groups[0][0]['id'] + r'''"
                }) {
                    id
                    name
                    active
                    age
                    score
                }
            }

            query q1 @edgedb(module: "test") {
                User(name: "Bob") {
                    id
                    name
                    active
                    age
                    score
                    groups {
                        id
                        name
                    }
                }
            }
        ''', graphql=True)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
                'name': 'Bob',
                'active': True,
                'age': 25,
                'score': 2.34,
            }],
            [{
                'id': uuid.UUID,
                'name': 'Bob',
                'active': True,
                'age': 25,
                'score': 2.34,
                'groups': [{
                    'id': groups[0][0]['id'],
                    'name': 'basic',
                }],
            }],
        ])

    async def test_graphql_mutation_insert03(self):
        result = await self.con.execute(r'''
            # nested insert of user and group
            mutation in1 @edgedb(module: "test") {
                insert__User(__data: {
                    name: "Bob",
                    active: true,
                    age: 25,
                    score: 2.34,
                    groups: {
                        name: "new"
                    }
                }) {
                    id
                    name
                    active
                    age
                    score
                }
            }

            query q1 @edgedb(module: "test") {
                User(name: "Bob") {
                    id
                    name
                    active
                    age
                    score
                    groups {
                        id
                        name
                    }
                }
            }
        ''', graphql=True)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
                'name': 'Bob',
                'active': True,
                'age': 25,
                'score': 2.34,
            }],
            [{
                'id': uuid.UUID,
                'name': 'Bob',
                'active': True,
                'age': 25,
                'score': 2.34,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'new',
                }],
            }],
        ])

    async def test_graphql_mutation_insert04(self):
        result = await self.con.execute(r'''
            # nested insert of user and group
            mutation in1 @edgedb(module: "test") {
                insert__User(__data: {
                    name: "Bob",
                    active: true,
                    age: 25,
                    score: 2.34,
                    groups: {
                        name: "new"
                    }
                }) {
                    id
                    name
                    active
                    age
                    score
                    groups {
                        id
                        name
                    }
                }
            }
        ''', graphql=True)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
                'name': 'Bob',
                'active': True,
                'age': 25,
                'score': 2.34,
                'groups': [{
                    'id': uuid.UUID,
                    'name': 'new',
                }],
            }],
        ])

    async def test_graphql_mutation_update01(self):
        result = await self.con.execute(r'''
            # update all users to have 0 score
            mutation up1 @edgedb(module: "test") {
                update__User(__data: {
                    score: 0
                }) {
                    id
                }
            }

            query q1 @edgedb(module: "test") {
                User {
                    name
                    score
                }
            }
        ''', graphql=True)

        result[1].sort(key=lambda x: x['name'])
        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
            }, {
                'id': uuid.UUID,
            }, {
                'id': uuid.UUID,
            }],
            [{
                'name': 'Alice',
                'score': 0,
            }, {
                'name': 'Jane',
                'score': 0,
            }, {
                'name': 'John',
                'score': 0,
            }],
        ])

    async def test_graphql_mutation_update02(self):
        groups = await self.con.execute(r"""
            query @edgedb(module: "test") {
                Group(name: "basic") {
                    id,
                }
            }
        """, graphql=True)

        result = await self.con.execute(r'''
            # update all users to have group "basic"
            mutation up1 @edgedb(module: "test") {
                update__User(__data: {
                    groups__id: "''' + groups[0][0]['id'] + r'''"
                }) {
                    id
                }
            }

            query q1 @edgedb(module: "test") {
                User {
                    id
                    name
                    groups {
                        id
                        name
                    }
                }
            }
        ''', graphql=True)

        result[1].sort(key=lambda x: x['name'])
        self.assert_data_shape(result[1], [
            {
                'id': uuid.UUID,
                'name': 'Alice',
                'groups': [{
                    'id': groups[0][0]['id'],
                    'name': 'basic',
                }],
            }, {
                'id': uuid.UUID,
                'name': 'Jane',
                'groups': [{
                    'id': groups[0][0]['id'],
                    'name': 'basic',
                }],
            }, {
                'id': uuid.UUID,
                'name': 'John',
                'groups': [{
                    'id': groups[0][0]['id'],
                    'name': 'basic',
                }],
            },
        ])
