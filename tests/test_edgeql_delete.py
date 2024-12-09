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


import itertools

import edgedb

from edb.testbase import server as tb


class TestDelete(tb.QueryTestCase):
    SETUP = """
        START MIGRATION TO {
            module default {
                type LinkingType {
                    multi link objs -> AbstractDeleteTest;
                };

                abstract type AbstractDeleteTest {
                    property name -> str;
                };

                type DeleteTest extending AbstractDeleteTest;
                type DeleteTest2 extending AbstractDeleteTest;
            };
        };
        POPULATE MIGRATION;
        COMMIT MIGRATION;
    """

    async def test_edgeql_delete_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot delete non-ObjectType object'):

            await self.con.execute('''\
                DELETE 42;
            ''')

    async def test_edgeql_delete_bad_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'free objects cannot be deleted',
        ):
            await self.con.execute('''\
                WITH foo := {bar := 1}
                DELETE foo
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'free objects cannot be deleted',
        ):
            await self.con.execute('''\
                DELETE std::FreeObject
            ''')

    async def test_edgeql_delete_bad_03(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'delete standard library type',
        ):
            await self.con.execute('''\
                DELETE schema::Object;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'delete standard library type',
        ):
            await self.con.execute('''\
                DELETE {default::LinkingType, schema::Object};
            ''')

    async def test_edgeql_delete_simple_01(self):
        # ensure a clean slate, not part of functionality testing
        await self.con.execute(r"""
            DELETE DeleteTest;
        """)

        await self.con.execute(r"""
            INSERT DeleteTest {
                name := 'delete-test'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT DeleteTest;
            """,
            [{}],
        )

        await self.con.execute(r"""
            DELETE DeleteTest;
        """)

        await self.assert_query_result(
            r"""
                SELECT DeleteTest;
            """,
            [],
        )

    async def test_edgeql_delete_simple_02(self):
        id1 = str((await self.con.query_single(r"""
            SELECT(INSERT DeleteTest {
                name := 'delete-test1'
            }) LIMIT 1;
        """)).id)

        id2 = str((await self.con.query_single(r"""
            SELECT(INSERT DeleteTest {
                name := 'delete-test2'
            }) LIMIT 1;
        """)).id)

        await self.assert_query_result(
            r"""
                DELETE (SELECT DeleteTest
                        FILTER DeleteTest.name = 'bad name');
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                SELECT DeleteTest ORDER BY DeleteTest.name;
            """,
            [{'id': id1}, {'id': id2}],
        )

        await self.assert_query_result(
            r"""
                SELECT (DELETE (SELECT DeleteTest
                        FILTER DeleteTest.name = 'delete-test1'));
            """,
            [{'id': id1}],
        )

        await self.assert_query_result(
            r"""
                SELECT DeleteTest ORDER BY DeleteTest.name;
            """,
            [{'id': id2}],
        )

        await self.assert_query_result(
            r"""
                SELECT (DELETE (SELECT DeleteTest
                        FILTER DeleteTest.name = 'delete-test2'));
            """,
            [{'id': id2}],
        )

        await self.assert_query_result(
            r"""
                SELECT DeleteTest ORDER BY DeleteTest.name;
            """,
            [],
        )

    async def test_edgeql_delete_returning_01(self):
        id1 = str((await self.con.query_single(r"""
            SELECT (INSERT DeleteTest {
                name := 'delete-test1'
            }) LIMIT 1;
        """)).id)

        await self.con.execute(r"""
            INSERT DeleteTest {
                name := 'delete-test2'
            };
            INSERT DeleteTest {
                name := 'delete-test3'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT (DELETE DeleteTest
                        FILTER DeleteTest.name = 'delete-test1');
            """,
            [{'id': id1}],
        )

        await self.assert_query_result(
            r"""
                WITH
                    D := (DELETE DeleteTest
                          FILTER DeleteTest.name = 'delete-test2')
                SELECT D {name};
            """,
            [{'name': 'delete-test2'}],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    (DELETE DeleteTest
                     FILTER DeleteTest.name = 'delete-test3'
                    ).name ++ '--DELETED';
            """,
            ['delete-test3--DELETED'],
        )

    async def test_edgeql_delete_returning_02(self):
        await self.con.execute(r"""
            INSERT DeleteTest {
                name := 'delete-test1'
            };
            INSERT DeleteTest {
                name := 'delete-test2'
            };
            INSERT DeleteTest {
                name := 'delete-test3'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH D := (DELETE DeleteTest)
                SELECT count(D);
            """,
            [3],
        )

    async def test_edgeql_delete_returning_03(self):
        await self.con.execute(r"""
            INSERT DeleteTest {
                name := 'dt1.1'
            };
            INSERT DeleteTest {
                name := 'dt1.2'
            };
            INSERT DeleteTest {
                name := 'dt1.3'
            };
            # create a different object
            INSERT DeleteTest2 {
                name := 'dt2.1'
            };

            INSERT DeleteTest2 {
                name := 'delete test2.2'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    D := (DELETE DeleteTest)
                SELECT DeleteTest2 {
                    name,
                    foo := 'bar'
                } FILTER any(DeleteTest2.name LIKE D.name[:2] ++ '%');
            """,
            [{
                'name': 'dt2.1',
                'foo': 'bar',
            }],
        )

        deleted = await self.con._fetchall(
            r"""
                DELETE DeleteTest2;
            """,
            __typeids__=True,
            __typenames__=True
        )

        self.assertTrue(hasattr(deleted[0], '__tid__'))
        self.assertEqual(deleted[0].__tname__, 'default::DeleteTest2')

    async def test_edgeql_delete_returning_04(self):
        await self.con.execute(r"""
            INSERT DeleteTest {
                name := 'dt1.1'
            };
            INSERT DeleteTest {
                name := 'dt1.2'
            };
            INSERT DeleteTest {
                name := 'dt1.3'
            };
            # create a different object
            INSERT DeleteTest2 {
                name := 'dt2.1'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
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
                SELECT (DELETE DeleteTest2) {name};
            """,
            [{
                'name': 'dt2.1',
            }],
        )

    async def test_edgeql_delete_returning_05(self):
        await self.con.execute(r"""
            INSERT DeleteTest {
                name := 'dt1.1'
            };
            INSERT DeleteTest {
                name := 'dt1.2'
            };
            INSERT DeleteTest {
                name := 'dt1.3'
            };
            # create a different object
            INSERT DeleteTest2 {
                name := 'dt2.1'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
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
                SELECT (DELETE DeleteTest2) {name};
            """,
            [{
                'name': 'dt2.1',
            }],
        )

    async def test_edgeql_delete_sugar_01(self):
        await self.con.execute(r"""
            FOR x IN {'1', '2', '3', '4', '5', '6'}
            UNION (INSERT DeleteTest {
                name := 'sugar delete ' ++ x
            });
        """)

        await self.con.execute(r"""
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
                SELECT DeleteTest.name;
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
            FOR x IN {'1', '2', '3', '4', '5', '6'}
            UNION (INSERT DeleteTest {
                name := 'delete union ' ++ x
            });

            FOR x IN {'7', '8', '9'}
            UNION (INSERT DeleteTest2 {
                name := 'delete union ' ++ x
            });

            INSERT DeleteTest { name := 'not delete union 1' };

            INSERT DeleteTest2 { name := 'not delete union 2' };
        """)

        await self.con.execute(r"""
            WITH
                ToDelete := (
                    (SELECT DeleteTest FILTER .name ILIKE 'delete union%')
                    UNION
                    (SELECT DeleteTest2 FILTER .name ILIKE 'delete union%')
                )
            DELETE ToDelete;
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    DeleteTest
                FILTER
                    .name ILIKE 'delete union%';

            """,
            [],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    DeleteTest {name}
                FILTER
                    .name ILIKE 'not delete union%';

            """,
            [{
                'name': 'not delete union 1'
            }],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    DeleteTest2
                FILTER
                    .name ILIKE 'delete union%';

            """,
            [],
        )

        await self.assert_query_result(
            r"""
                SELECT
                    DeleteTest2 {name}
                FILTER
                    .name ILIKE 'not delete union%';

            """,
            [{
                'name': 'not delete union 2'
            }],
        )

    async def test_edgeql_delete_abstract_01(self):
        await self.con.execute(r"""

            INSERT DeleteTest { name := 'child of abstract 1' };
            INSERT DeleteTest2 { name := 'child of abstract 2' };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    D := (
                        DELETE
                            AbstractDeleteTest
                        FILTER
                            .name ILIKE 'child of abstract%'
                    )
                SELECT D { name } ORDER BY .name;
            """,
            [{
                'name': 'child of abstract 1'
            }, {
                'name': 'child of abstract 2'
            }],
        )

    async def test_edgeql_delete_assert_exists(self):
        await self.con.execute(r"""
            INSERT DeleteTest2 { name := 'x' };
        """)

        await self.assert_query_result(
            r"""
            select assert_exists((delete DeleteTest2 filter .name = 'x'));
            """,
            [{}],
        )

    async def test_edgeql_delete_then_union(self):
        await self.con.execute(r"""
            INSERT DeleteTest2 { name := 'x' };
            INSERT DeleteTest2 { name := 'y' };
        """)

        await self.assert_query_result(
            r"""
            with
            delete1 := assert_exists((delete DeleteTest2 filter .name = 'x')),
            delete2 := assert_exists((delete DeleteTest2 filter .name = 'y')),
            select {delete1, delete2};
            """,
            [{}, {}],
        )

    async def test_edgeql_delete_multi_simultaneous_01(self):
        await self.con.execute(r"""
            with
              a := (insert DeleteTest { name := '1' }),
              b := (insert DeleteTest { name := '2' }),
              c := (insert LinkingType { objs := {a, b} })
            select c;
        """)

        dels = {
            'a': '(DELETE DeleteTest)',
            'b': '(DELETE LinkingType)',
        }

        # We want to try all the different permutations of deletion
        # binding order and order that the variables are referenced in
        # the body. (Somewhat upsettingly, the order that the delete CTEs
        # are included into the SQL union affected the behavior.)
        # All the queries look like some variant on:
        #
        #      with
        #        a := (DELETE DeleteTest),
        #        b := (DELETE LinkingType),
        #      select {a, b};

        for binds, uses in itertools.product(
            list(itertools.permutations(dels.keys())),
            list(itertools.permutations(dels.keys())),
        ):
            bind_q = '\n'.join(
                ' ' * 18 + f'{k} := {dels[k]},' for k in binds
            ).lstrip()
            q = f'''
                with
                  {bind_q}
                select {{{', '.join(uses)}}};
            '''

            async with self._run_and_rollback():
                with self.annotate(binds=binds, uses=uses):
                    await self.con.execute(q)

    async def test_edgeql_delete_multi_simultaneous_02(self):
        populate = r"""
            with
              a := (insert DeleteTest { name := '1' }),
              b := (insert DeleteTest2 { name := '2' }),
              c := (insert LinkingType { objs := {a, b} })
            select c;
        """

        await self.con.execute(populate)
        await self.con.execute(r"""
             with
               a := (DELETE AbstractDeleteTest),
               b := (DELETE LinkingType),
             select {a, b};
        """)

        await self.con.execute(populate)
        await self.con.execute(r"""
             with
               a := (DELETE AbstractDeleteTest),
               b := (DELETE LinkingType),
             select {b, a};
        """)

    async def test_edgeql_delete_where_order_dml(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "INSERT statements cannot be used in a FILTER clause"):
            await self.con.query('''
                delete DeleteTest
                filter
                        (INSERT DeleteTest {
                            name := 't1',
                        })
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "UPDATE statements cannot be used in a FILTER clause"):
            await self.con.query('''
                delete DeleteTest
                filter
                        (UPDATE DeleteTest set {
                            name := 't1',
                        })
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "DELETE statements cannot be used in a FILTER clause"):
            await self.con.query('''
                delete DeleteTest
                filter
                        (DELETE DeleteTest filter .name = 't1')
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "INSERT statements cannot be used in an ORDER BY clause"):
            await self.con.query('''
                delete DeleteTest
                order by
                        (INSERT DeleteTest {
                            name := 't1',
                        })
                limit 1
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "UPDATE statements cannot be used in an ORDER BY clause"):
            await self.con.query('''
                delete DeleteTest
                order by
                        (UPDATE DeleteTest set {
                            name := 't1',
                        })
                limit 1
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "DELETE statements cannot be used in an ORDER BY clause"):
            await self.con.query('''
                delete DeleteTest
                order by
                        (DELETE DeleteTest filter .name = 't1')
                limit 1
            ''')
