##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import unittest
import uuid

from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestGraphQLMutation(tb.QueryTestCase):
    SETUP = """
        CREATE DELTA test::d1 TO $$
            abstract concept NamedObject:
                required link name to str

            concept Group extends NamedObject:
                link settings to Setting:
                    mapping: **

            concept Setting extends NamedObject:
                required link value to str

            concept Profile extends NamedObject:
                required link value to str

            concept User extends NamedObject:
                required link active to bool
                link groups to Group:
                    mapping: **
                required link age to int
                required link score to float
                link profile to Profile:
                    mapping: *1
        $$;

        COMMIT DELTA test::d1;
    """

    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self.con.execute(r"""
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
                groups := (SELECT `Group` WHERE `Group`.name = 'basic')
            };

            WITH MODULE test
            INSERT User {
                name := 'Jane',
                age := 26,
                active := True,
                score := 1.23,
                groups := (SELECT `Group` WHERE `Group`.name = 'upgraded')
            };

            WITH MODULE test
            INSERT User {
                name := 'Alice',
                age := 27,
                active := True,
                score := 5
            };
        """))

    def tearDown(self):
        super().tearDown()
        self.loop.run_until_complete(self.con.execute(r"""
            DELETE test::Setting;
            DELETE test::Group;
            DELETE test::User;
        """))

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

    @unittest.expectedFailure
    async def test_graphql_mutation_delete02(self):
        result = await self.con.execute(r"""
            mutation del @edgedb(module: "test") {
                delete__User(name: "John") {
                    name,
                    groups {
                        name
                    }
                }
            }

            query sel @edgedb(module: "test") {
                User {
                    name,
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
                'groups': {
                    'name': 'basic'
                },
            }],
        ])

    @unittest.expectedFailure
    async def test_graphql_mutation_delete03(self):
        result = await self.con.execute(r"""
            mutation @edgedb(module: "test") {
                delete__User(name: "John", active: true) {
                    name,
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
                'groups': {
                    'name': 'basic'
                },
            }],
        ])

    @unittest.expectedFailure
    async def test_graphql_mutation_delete04(self):
        result = await self.con.execute(r"""
            mutation @edgedb(module: "test") {
                delete__User(name: "John", active: false) {
                    name,
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
