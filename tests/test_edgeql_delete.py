##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestDelete(tb.QueryTestCase):
    SETUP = """
        CREATE MIGRATION test::d_delete01 TO eschema $$
            concept DeleteTest:
                link name to str
        $$;

        COMMIT MIGRATION test::d_delete01;
    """

    async def test_edgeql_delete_bad01(self):
        with self.assertRaisesRegex(
                exc.EdgeQLError,
                r'cannot delete non-Concept object',
                position=7):

            await self.query('''\
                DELETE 42;
            ''')
        pass

    async def test_edgeql_delete_simple01(self):
        result = await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'delete-test'
            };
        """)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
            }],
        ])

        del_result = await self.con.execute(r"""
            DELETE test::DeleteTest;
        """)

        self.assert_data_shape(del_result, [
            [{
                'id': result[0][0]['id'],
            }],
        ])

    async def test_edgeql_delete_simple02(self):
        result = await self.con.execute(r"""
            INSERT test::DeleteTest {
                name := 'delete-test1'
            };
            INSERT test::DeleteTest {
                name := 'delete-test2'
            };
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
            DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test1');

            WITH MODULE test
            SELECT DeleteTest ORDER BY DeleteTest.name;

            WITH MODULE test
            DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test2');

            WITH MODULE test
            SELECT DeleteTest ORDER BY DeleteTest.name;
        """, [
            [],
            [{'id': id1}, {'id': id2}],

            [{'id': id1}],
            [{'id': id2}],

            [{'id': id2}],
            [],
        ])

    async def test_edgeql_delete_returning01(self):
        result = await self.con.execute(r"""
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

        self.assert_data_shape(result, [
            [{'id': uuid.UUID}],
            [{'id': uuid.UUID}],
            [{'id': uuid.UUID}],
        ])

        id1 = result[0][0]['id']

        del_result = await self.con.execute(r"""
            WITH MODULE test
            DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test1')
            RETURNING DeleteTest;

            WITH MODULE test
            DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test2')
            RETURNING DeleteTest{name};

            WITH MODULE test
            DELETE (SELECT DeleteTest
                    FILTER DeleteTest.name = 'delete-test3')
            RETURNING DeleteTest.name + '--DELETED';
        """)

        self.assert_data_shape(del_result, [
            [{'id': id1}],
            [{'name': 'delete-test2'}],
            ['delete-test3--DELETED'],
        ])
