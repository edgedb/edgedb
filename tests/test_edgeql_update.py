##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest

from edgedb.server import _testbase as tb


class TestUpdate(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'updates.eschema')

    SETUP = """
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
                status := (SELECT Status FILTER Status.name = 'Open')
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test2',
                comment := 'second',
                status := (SELECT Status FILTER Status.name = 'Open')
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test3',
                comment := 'third',
                status := (SELECT Status FILTER Status.name = 'Closed')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            } ORDER BY .name;
        """)

        self.original = res[-1]

    async def test_edgeql_update_simple01(self):
        orig1 = self.original[0]

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'bad name'
            SET {
                status := (SELECT Status FILTER Status.name = 'Closed')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res, [
            [],
            [orig1],
        ])

    async def test_edgeql_update_simple02(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                status := (SELECT Status FILTER Status.name = 'Closed')
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
                'status': {
                    'name': 'Closed'
                }
            },
            orig2,
            orig3,
        ])

    async def test_edgeql_update_simple03(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test2'
            SET {
                comment := 'updated ' + UpdateTest.comment
            };

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
            UPDATE UpdateTest
            SET {
                comment := UpdateTest.comment + "!",
                status := (SELECT Status FILTER Status.name = 'Closed')
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

    async def test_edgeql_update_returning01(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test2'
            SET {
                comment := 'updated ' + UpdateTest.comment
            } RETURNING UpdateTest {
                name,
                comment,
            };
        """, [
            [{
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'updated second',
            }]
        ])

    async def test_edgeql_update_returning02(self):
        orig1, orig2, orig3 = self.original

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            SET {
                comment := UpdateTest.comment + "!",
                status := (SELECT Status FILTER Status.name = 'Closed')
            } RETURNING UpdateTest {
                name,
                comment,
                status: {
                    name
                }
            };
        """)

        res[-1].sort(key=lambda x: x['name'])
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

    async def test_edgeql_update_returning03(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test2'
            SET {
                comment := 'updated ' + UpdateTest.comment
            } RETURNING SINGLETON UpdateTest {
                name,
                comment,
            };
        """, [
            [{
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'updated second',
            }]
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_returning04(self):
        # XXX: We're expecting an exception since the returning set is
        # not a singleton set. The specific exception needs to be
        # updated once it is implemented.
        #
        with self.assertRaises(ValueError):
            await self.con.execute(r"""
                WITH MODULE test
                UPDATE UpdateTest
                SET {
                    comment := 'updated ' + UpdateTest.comment
                } RETURNING SINGLETON UpdateTest {
                    name,
                    comment,
                };
            """)

    async def test_edgeql_update_returning05(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test2'
            SET {
                comment := 'updated ' + UpdateTest.comment
            } RETURNING 42;
        """, [
            [42],
        ])

    async def test_edgeql_update_returning06(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(r"""
            WITH
                MODULE test,
                U := (
                    UPDATE UpdateTest
                    FILTER UpdateTest.name = 'update-test2'
                    SET {
                        comment := 'updated ' + UpdateTest.comment
                    }
                )
            SELECT Status{name}
            FILTER Status = U.status
            ORDER BY Status.name;
        """, [
            [{'name': 'Open'}],
        ])

    async def test_edgeql_update_returning07(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(r"""
            WITH
                MODULE test,
                Q := (
                    UPDATE UpdateTest
                    SET {
                        comment := UpdateTest.comment + "!",
                        status := (SELECT Status FILTER Status.name = 'Closed')
                    }
                )

            SELECT
                Q {
                    name,
                    comment,
                    status: {
                        name
                    }
                }
            ORDER BY
                Q.name;
        """, [
            [{
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
            }],
        ])

    async def test_edgeql_update_generic01(self):
        status = await self.con.execute(r"""
            WITH MODULE test
            SELECT Status.id
            FILTER Status.name = 'Open';
        """)

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test3'
            SET {
                status := (
                    SELECT Object
                    FILTER Object.id = <uuid>'""" + status[0][0] + r"""'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                status: {
                    name
                }
            } FILTER UpdateTest.name = 'update-test3';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test3',
                'status': {
                    'name': 'Open',
                },
            },
        ])
