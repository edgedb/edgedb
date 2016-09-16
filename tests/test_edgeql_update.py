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
        CREATE DELTA test::d_update01 TO $$
            concept Status:
                link name to str

            concept UpdateTest:
                link name to str
                link status to Status
        $$;

        COMMIT DELTA test::d_update01;
    """

    async def test_edgeql_update_simple01(self):
        result = await self.con.execute(r"""
            INSERT test::Status {
                name := 'Open'
            };

            INSERT test::Status {
                name := 'Closed'
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test',
                status := (SELECT Status WHERE Status.name = 'Open')
            };
        """)

        result = await self.con.execute(r"""
            WITH MODULE test
            SELECT UpdateTest {
                name,
                status: {
                    name
                }
            } WHERE UpdateTest.name = 'update-test';
        """)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
                'name': 'update-test',
                'status': {
                    'name': 'Open'
                }
            }],
        ])

        result = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                status := (SELECT Status WHERE Status.name = 'Closed')
            } WHERE UpdateTest.name = 'update-test';
        """)

        result = await self.con.execute(r"""
            WITH MODULE test
            SELECT UpdateTest {
                name,
                status: {
                    name
                }
            } WHERE UpdateTest.name = 'update-test';
        """)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
                'name': 'update-test',
                'status': {
                    'name': 'Closed'
                }
            }],
        ])
