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


import uuid

import edgedb

from edb.server import _testbase as tb


class TestDelete(tb.QueryTestCase):
    SETUP = """
        CREATE MIGRATION test::d_delete01 TO eschema $$
            type DeleteTest:
                property name -> str

            type DeleteTest2:
                property name -> str
        $$;

        COMMIT MIGRATION test::d_delete01;
    """

    async def test_edgeql_delete_bad01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r'cannot delete non-ObjectType object'):

            await self.query('''\
                DELETE 42;
            ''')
        pass

    async def test_edgeql_delete_simple01(self):
        # ensure a clean slate, not part of functionality testing
        await self.query(r"""
            DELETE test::DeleteTest;
        """)

        result = await self.query(r"""
            INSERT test::DeleteTest {
                name := 'delete-test'
            };

            DELETE test::DeleteTest;
        """)

        self.assert_data_shape(result, [
            [1],
            [1]
        ])

    async def test_edgeql_delete_simple02(self):
        result = await self.query(r"""
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test1'
            });
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test2'
            });
        """)

        self.assert_data_shape(result, [
            [{'id': uuid.UUID}],
            [{'id': uuid.UUID}],
        ])

        id1 = result[0][0]['id']
        id2 = result[1][0]['id']

        await self.assert_query_result(r"""
            WITH MODULE test
            DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'bad name');

            WITH MODULE test
            SELECT DeleteTest ORDER BY DeleteTest.name;

            WITH MODULE test
            SELECT (DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test1'));

            WITH MODULE test
            SELECT DeleteTest ORDER BY DeleteTest.name;

            WITH MODULE test
            SELECT (DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test2'));

            WITH MODULE test
            SELECT DeleteTest ORDER BY DeleteTest.name;
        """, [
            [0],
            [{'id': id1}, {'id': id2}],

            [{'id': id1}],
            [{'id': id2}],

            [{'id': id2}],
            [],
        ])

    async def test_edgeql_delete_returning01(self):
        result = await self.query(r"""
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test1'
            });
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test2'
            });
            SELECT(INSERT test::DeleteTest {
                name := 'delete-test3'
            });
        """)

        self.assert_data_shape(result, [
            [{'id': uuid.UUID}],
            [{'id': uuid.UUID}],
            [{'id': uuid.UUID}],
        ])

        id1 = result[0][0]['id']

        del_result = await self.query(r"""
            WITH MODULE test
            SELECT (DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test1'));

            WITH
                MODULE test,
                D := (DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test2'))
            SELECT D {name};

            WITH MODULE test
            SELECT
                (DELETE (
                    SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test3'
                )).name ++ '--DELETED';
        """)

        self.assert_data_shape(del_result, [
            [{'id': id1}],
            [{'name': 'delete-test2'}],
            ['delete-test3--DELETED'],
        ])

    async def test_edgeql_delete_returning02(self):
        await self.assert_query_result(r"""
            INSERT test::DeleteTest {
                name := 'delete-test1'
            };
            INSERT test::DeleteTest {
                name := 'delete-test2'
            };
            INSERT test::DeleteTest {
                name := 'delete-test3'
            };

            WITH D := (DELETE test::DeleteTest)
            SELECT count(D);
        """, [
            [1],
            [1],
            [1],
            [3],
        ])

    async def test_edgeql_delete_returning03(self):
        await self.assert_query_result(r"""
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

            WITH
                MODULE test,
                D := (DELETE DeleteTest)
            SELECT DeleteTest2 {
                name,
                foo := 'bar'
            } FILTER DeleteTest2.name LIKE D.name[:2] ++ '%';

            WITH MODULE test
            SELECT (DELETE DeleteTest2) { name };
        """, [
            [1],
            [1],
            [1],
            [1],
            [1],
            [{
                'name': 'dt2.1',
                'foo': 'bar',
            }],
            [{}, {}],
        ])

    async def test_edgeql_delete_returning04(self):
        await self.assert_query_result(r"""
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

            WITH
                MODULE test,
                # make sure that aliased deletion works as an expression
                #
                Q := (DELETE DeleteTest)
            SELECT DeleteTest2 {
                name,
                count := count(Q),
            } FILTER DeleteTest2.name = 'dt2.1';

            WITH MODULE test
            SELECT (DELETE DeleteTest2) {name};
        """, [
            [1],
            [1],
            [1],
            [1],
            [{
                'name': 'dt2.1',
                'count': 3,
            }],
            [{
                'name': 'dt2.1',
            }],
        ])

    async def test_edgeql_delete_returning05(self):
        await self.assert_query_result(r"""
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

            WITH MODULE test
            SELECT (DELETE DeleteTest2) {name};
        """, [
            [1],
            [1],
            [1],
            [1],
            [{
                'name': 'dt2.1',
                'count': 3
            }],
            [{
                'name': 'dt2.1',
            }],
        ])
