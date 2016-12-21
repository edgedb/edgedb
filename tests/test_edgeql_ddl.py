##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.client import exceptions as client_errors
from edgedb.server import _testbase as tb


class TestDeltas(tb.QueryTestCase):
    async def test_edgeql_ddl01(self):
        await self.con.execute("""
            CREATE LINK test::test_link;
        """)

    async def test_edgeql_ddl02(self):
        await self.con.execute("""
            CREATE LINK test::test_concept_link {
                CREATE LINK PROPERTY test::test_link_prop TO std::int;
            };

            CREATE CONCEPT test::TestConcept {
                CREATE LINK test::test_concept_link TO std::str {
                    CREATE LINK PROPERTY test::test_link_prop TO std::int {
                        SET title := 'Test Property';
                    };
                };
            };
        """)

    async def test_edgeql_ddl03(self):
        await self.con.execute("""
            CREATE LINK test::test_concept_link_prop {
                CREATE LINK PROPERTY test::link_prop1 TO std::str;
            };
        """)

    async def test_edgeql_ddl04(self):
        await self.con.execute("""
            CREATE CONCEPT test::A;
            CREATE CONCEPT test::B INHERITING test::A;

            CREATE CONCEPT test::Object1 {
                CREATE REQUIRED LINK test::a TO test::A;
            };

            CREATE CONCEPT test::Object2 {
                CREATE LINK test::a TO test::B;
            };

            CREATE CONCEPT test::Object_12
                INHERITING (test::Object1, test::Object2);
        """)

    async def test_edgeql_ddl05(self):
        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'Cannot create an aggregate function'):

            await self.con.execute("""
                CREATE FUNCTION std::my_lower(std::str) RETURNING std::str
                    FROM SQL FUNCTION 'lower';

                CREATE AGGREGATE std::my_lower(std::any) RETURNING std::str
                    FROM SQL AGGREGATE 'count';
            """)

        with self.assertRaisesRegex(client_errors.EdgeQLError,
                                    'Cannot create a function'):

            await self.con.execute("""
                CREATE AGGREGATE std::my_lower2(std::any) RETURNING std::str
                    FROM SQL AGGREGATE 'count';

                CREATE FUNCTION std::my_lower2(std::str) RETURNING std::str
                    FROM SQL FUNCTION 'lower';
            """)
