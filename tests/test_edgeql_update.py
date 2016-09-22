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


class TestUpdate(tb.QueryTestCase):
    SETUP = """
        CREATE DELTA test::d_update01 TO $$
            concept Status:
                link name to str

            concept UpdateTest:
                link name to str
                link status to Status
                link comment to str
        $$;

        COMMIT DELTA test::d_update01;

        # populate the test DB

        INSERT test::Status {
            name := 'Open'
        };

        INSERT test::Status {
            name := 'Closed'
        };
    """

    def setUp(self):
        super().setUp()
        self.loop.run_until_complete(self._setup_objects())

    def tearDown(self):
        super().tearDown()
        self.loop.run_until_complete(self.con.execute(r"""
            DELETE test::UpdateTest;
        """))

    async def _setup_objects(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test1',
                status := (SELECT Status WHERE Status.name = 'Open')
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test2',
                comment := 'second',
                status := (SELECT Status WHERE Status.name = 'Open')
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test3',
                comment := 'third',
                status := (SELECT Status WHERE Status.name = 'Closed')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            } ORDER BY UpdateTest.name;
        """)

        self.original = res[-1]

    async def test_edgeql_update_simple01(self):
        orig1 = self.original[0]

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                status := (SELECT Status WHERE Status.name = 'Closed')
            } WHERE UpdateTest.name = 'bad name';

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            } WHERE UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res, [
            [],
            [orig1],
        ])

    async def test_edgeql_update_simple02(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                status := (SELECT Status WHERE Status.name = 'Closed')
            } WHERE UpdateTest.name = 'update-test1';

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            } ORDER BY UpdateTest.name;
        """)

        self.assert_data_shape(res[-1], [
            {
                'id': orig1['id'],
                'name': 'update-test1',
                'status': {
                    'name': 'Closed'
                }
            },
            orig2,
            orig3,
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_simple03(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                comment := 'updated ' + UpdateTest.comment
            } WHERE UpdateTest.name = 'update-test2';

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
            } ORDER BY UpdateTest.name;
        """)

        self.assert_data_shape(res[-1], [
            {
                'id': orig1['id'],
                'name': orig1['name'],
                'comment': orig1['comment'],
            }, {
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'updated second',
            }, {
                'id': orig3['id'],
                'name': orig3['name'],
                'comment': orig3['comment'],
            },
        ])

    async def test_edgeql_update_simple04(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                comment := UpdateTest.comment + "!",
                status := (SELECT Status WHERE Status.name = 'Closed')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            } ORDER BY UpdateTest.name;
        """)

        self.assert_data_shape(res[-1], [
            {
                'id': orig1['id'],
                'name': 'update-test1',
                'comment': None,
                'status': {
                    'name': 'Closed'
                }
            }, {
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'second!',
                'status': {
                    'name': 'Closed'
                }
            }, {
                'id': orig3['id'],
                'name': 'update-test3',
                'comment': 'third!',
                'status': {
                    'name': 'Closed'
                }
            },
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_returning01(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                comment := 'updated ' + UpdateTest.comment
            } WHERE UpdateTest.name = 'update-test2'
            RETURNING UpdateTest {
                name,
                comment,
            };
        """)

        self.assert_data_shape(res[-1], [
            {
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'updated second',
            },
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_returning02(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                comment := UpdateTest.comment + "!",
                status := (SELECT Status WHERE Status.name = 'Closed')
            } RETURNING UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            };
        """)

        # XXX: this relies on the "natural" DB order of items
        self.assert_data_shape(res[-1], [
            {
                'id': orig1['id'],
                'name': 'update-test1',
                'comment': None,
                'status': {
                    'name': 'Closed'
                }
            }, {
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'second!',
                'status': {
                    'name': 'Closed'
                }
            }, {
                'id': orig3['id'],
                'name': 'update-test3',
                'comment': 'third!',
                'status': {
                    'name': 'Closed'
                }
            },
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_returning03(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest {
                comment := 'updated ' + UpdateTest.comment
            } WHERE UpdateTest.name = 'update-test2'
            RETURNING SINGLE UpdateTest {
                name,
                comment,
            };
        """)

        self.assert_data_shape(res[-1], [
            {
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'updated second',
            },
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_returning04(self):
        # XXX: We're expecting an exception since the returning set is
        # not a singleton set. The specific exception needs to be
        # updated once it is implemented.
        #
        with self.assertRaises(exc._base.EdgeDBError):
            res = await self.con.execute(r"""
                WITH MODULE test
                UPDATE UpdateTest {
                    comment := 'updated ' + UpdateTest.comment
                } RETURNING SINGLE UpdateTest {
                    name,
                    comment,
                };
            """)
