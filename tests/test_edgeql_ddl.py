##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


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
