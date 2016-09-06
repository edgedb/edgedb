##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid

from edgedb.server import _testbase as tb


class TestDelete(tb.QueryTestCase):
    SETUP = """
        CREATE DELTA test::d_delete01 TO $$
            concept DeleteTest:
                link name to str
        $$;

        COMMIT DELTA test::d_delete01;
    """

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
            DELETE test::DeleteTest
            WHERE test::DeleteTest.name = 'delete-test';
        """)

        self.assert_data_shape(del_result, [
            [{
                'id': result[0][0]['id'],
            }],
        ])
