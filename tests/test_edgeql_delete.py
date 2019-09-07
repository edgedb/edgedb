#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


import edgedb

from edb.testbase import server as tb


class TestDelete(tb.QueryTestCase):
    SETUP = """
        CREATE MIGRATION d_delete01 TO {
            module test {
                type DeleteTest {
                    property name -> str;
                };

                type DeleteTest2 {
                    property name -> str;
                };
            };
        };

        COMMIT MIGRATION d_delete01;
    """

    async def test_edgeql_delete_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot delete non-ObjectType object'):

            await self.con.execute('''\
                DELETE 42;
            ''')
        pass

    async def test_edgeql_delete_simple_01(self):
        # ensure a clean slate, not part of functionality testing
        await self.con.execute(r"""
            DELETE test::DeleteTest;
        """)

        await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'delete-test'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT test::DeleteTest;
            """,
            [{}],
        )

        await self.con.execute(r"""
            DELETE test::DeleteTest;
        """)

        await self.assert_query_result(
            r"""
                SELECT test::DeleteTest;
            """,
            [],
        )

    async def test_edgeql_delete_simple_02(self):
        id1 = str((await self.con.fetchone(r"""
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test1'
            }) LIMIT 1;
        """)).id)

        id2 = str((await self.con.fetchone(r"""
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test2'
            }) LIMIT 1;
        """)).id)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                DELETE (SELECT DeleteTest
                        FILTER DeleteTest.name = 'bad name');
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT DeleteTest ORDER BY DeleteTest.name;
            """,
            [{'id': id1}, {'id': id2}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (DELETE (SELECT DeleteTest
                        FILTER DeleteTest.name = 'delete-test1'));
            """,
            [{'id': id1}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT DeleteTest ORDER BY DeleteTest.name;
            """,
            [{'id': id2}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (DELETE (SELECT DeleteTest
                        FILTER DeleteTest.name = 'delete-test2'));
            """,
            [{'id': id2}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT DeleteTest ORDER BY DeleteTest.name;
            """,
            [],
        )

    async def test_edgeql_delete_returning_01(self):
        id1 = str((await self.con.fetchone(r"""
            SELECT (INSERT test::DeleteTest {
                name := 'delete-test1'
            }) LIMIT 1;
        """)).id)

        await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'delete-test2'
            };
            INSERT test::DeleteTest {
                name := 'delete-test3'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (DELETE DeleteTest
                        FILTER DeleteTest.name = 'delete-test1');
            """,
            [{'id': id1}],
        )

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    D := (DELETE DeleteTest
                          FILTER DeleteTest.name = 'delete-test2')
                SELECT D {name};
            """,
            [{'name': 'delete-test2'}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    (DELETE DeleteTest
                     FILTER DeleteTest.name = 'delete-test3'
                    ).name ++ '--DELETED';
            """,
            ['delete-test3--DELETED'],
        )

    async def test_edgeql_delete_returning_02(self):
        await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'delete-test1'
            };
            INSERT test::DeleteTest {
                name := 'delete-test2'
            };
            INSERT test::DeleteTest {
                name := 'delete-test3'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH D := (DELETE test::DeleteTest)
                SELECT count(D);
            """,
            [3],
        )

    async def test_edgeql_delete_returning_03(self):
        await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'dt1.1'
            };
            INSERT test::DeleteTest {
                name := 'dt1.2'
            };
            INSERT test::DeleteTest {
                name := 'dt1.3'
            };
            # create a different object
            INSERT test::DeleteTest2 {
                name := 'dt2.1'
            };

            INSERT test::DeleteTest2 {
                name := 'delete test2.2'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    D := (DELETE DeleteTest)
                SELECT DeleteTest2 {
                    name,
                    foo := 'bar'
                } FILTER DeleteTest2.name LIKE D.name[:2] ++ '%';
            """,
            [{
                'name': 'dt2.1',
                'foo': 'bar',
            }],
        )

        deleted = await self.con.fetchall(
            r"""
                WITH MODULE test
                DELETE DeleteTest2;
            """,
        )

        self.assertTrue(hasattr(deleted[0], '__tid__'))

    async def test_edgeql_delete_returning_04(self):
        await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'dt1.1'
            };
            INSERT test::DeleteTest {
                name := 'dt1.2'
            };
            INSERT test::DeleteTest {
                name := 'dt1.3'
            };
            # create a different object
            INSERT test::DeleteTest2 {
                name := 'dt2.1'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    # make sure that aliased deletion works as an expression
                    #
                    Q := (DELETE DeleteTest)
                SELECT DeleteTest2 {
                    name,
                    count := count(Q),
                } FILTER DeleteTest2.name = 'dt2.1';
            """,
            [{
                'name': 'dt2.1',
                'count': 3,
            }],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (DELETE DeleteTest2) {name};
            """,
            [{
                'name': 'dt2.1',
            }],
        )

    async def test_edgeql_delete_returning_05(self):
        await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'dt1.1'
            };
            INSERT test::DeleteTest {
                name := 'dt1.2'
            };
            INSERT test::DeleteTest {
                name := 'dt1.3'
            };
            # create a different object
            INSERT test::DeleteTest2 {
                name := 'dt2.1'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
                    D := (DELETE DeleteTest)
                # the returning clause is actually trying to simulate
                # returning "stats" of deleted objects
                #
                SELECT DeleteTest2 {
                    name,
                    count := count(D),
                } FILTER DeleteTest2.name = 'dt2.1';
            """,
            [{
                'name': 'dt2.1',
                'count': 3
            }],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT (DELETE DeleteTest2) {name};
            """,
            [{
                'name': 'dt2.1',
            }],
        )

    async def test_edgeql_delete_sugar_01(self):
        await self.con.execute(r"""
            WITH MODULE test
            FOR x IN {'1', '2', '3', '4', '5', '6'}
            UNION (INSERT DeleteTest {
                name := 'sugar delete ' ++ x
            });
        """)

        await self.con.execute(r"""
            WITH MODULE test
            DELETE
                DeleteTest
            FILTER
                .name[-1] != '2'
            ORDER BY .name
            OFFSET 2 LIMIT 2;
            # should delete 4 and 5
        """)

        await self.assert_query_result(
            r"""
                SELECT test::DeleteTest.name;
            """,
            {
                'sugar delete 1',
                'sugar delete 2',
                'sugar delete 3',
                'sugar delete 6',
            },
        )

    async def test_edgeql_delete_union(self):
        await self.con.execute(r"""
            WITH MODULE test
            FOR x IN {'1', '2', '3', '4', '5', '6'}
            UNION (INSERT DeleteTest {
                name := 'delete union ' ++ x
            });

            WITH MODULE test
            FOR x IN {'7', '8', '9'}
            UNION (INSERT DeleteTest2 {
                name := 'delete union ' ++ x
            });

            WITH MODULE test
            INSERT DeleteTest { name := 'not delete union 1' };

            WITH MODULE test
            INSERT DeleteTest2 { name := 'not delete union 2' };
        """)

        await self.con.execute(r"""
            WITH
                MODULE test,
                ToDelete := (
                    (SELECT DeleteTest FILTER .name ILIKE 'delete union%')
                    UNION
                    (SELECT DeleteTest2 FILTER .name ILIKE 'delete union%')
                )
            DELETE ToDelete;
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    test::DeleteTest
                FILTER
                    .name ILIKE 'delete union%';

            """,
            [],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    test::DeleteTest {name}
                FILTER
                    .name ILIKE 'not delete union%';

            """,
            [{
                'name': 'not delete union 1'
            }],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    test::DeleteTest2
                FILTER
                    .name ILIKE 'delete union%';

            """,
            [],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    test::DeleteTest2 {name}
                FILTER
                    .name ILIKE 'not delete union%';

            """,
            [{
                'name': 'not delete union 2'
            }],
        )

    async def test_edgeql_delete_in_conditional_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'DELETE statements cannot be used'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT
                    (SELECT DeleteTest2)
                    ??
                    (DELETE DeleteTest2);
            ''')

    async def test_edgeql_update_in_conditional_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'DELETE statements cannot be used'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT
                    (SELECT DeleteTest FILTER .name = 'foo')
                    IF EXISTS DeleteTest
                    ELSE (
                        (SELECT DeleteTest)
                        UNION
                        (DELETE DeleteTest)
                    );
            ''')
