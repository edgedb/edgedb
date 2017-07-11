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

        INSERT test::Tag {
            name := 'fun'
        };

        INSERT test::Tag {
            name := 'boring'
        };

        INSERT test::Tag {
            name := 'wow'
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
            [0],
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
            SELECT (
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test2'
                SET {
                    comment := 'updated ' + UpdateTest.comment
                }
            ) {
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
            SELECT (
                UPDATE UpdateTest
                SET {
                    comment := UpdateTest.comment + "!",
                    status := (SELECT Status FILTER Status.name = 'Closed')
                }
            ) {
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
            SELECT SINGLETON (
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test2'
                SET {
                    comment := 'updated ' + UpdateTest.comment
                }
            ) {
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
                SELECT SINGLETON (
                    UPDATE UpdateTest
                    SET {
                        comment := 'updated ' + UpdateTest.comment
                    }
                ) {
                    name,
                    comment,
                };
            """)

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

    async def test_edgeql_update_multiple01(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                tags := (SELECT Tag)
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                tags: {
                    name
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'tags': [{
                    'name': 'boring',
                }, {
                    'name': 'fun',
                }, {
                    'name': 'wow',
                }],
            },
        ])

    async def test_edgeql_update_multiple02(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                tags := (SELECT Tag FILTER Tag.name = 'wow')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                tags: {
                    name
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'tags': [{
                    'name': 'wow',
                }],
            },
        ])

    async def test_edgeql_update_multiple03(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                tags := (SELECT Tag FILTER Tag.name IN ['wow', 'fun'])
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                tags: {
                    name
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'tags': [{
                    'name': 'fun',
                }, {
                    'name': 'wow',
                }],
            },
        ])

    async def test_edgeql_update_multiple04(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                related := (SELECT U2 FILTER U2.name != 'update-test1')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                related: {
                    name
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'related': [{
                    'name': 'update-test2',
                }, {
                    'name': 'update-test3',
                }],
            },
        ])

    async def test_edgeql_update_multiple05(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_tests := (
                    SELECT U2 FILTER U2.name != 'update-test1'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                annotated_tests: {
                    name,
                    @note
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'annotated_tests': [{
                    'name': 'update-test2',
                    '@note': None,
                }, {
                    'name': 'update-test3',
                    '@note': None,
                }],
            },
        ])

    async def test_edgeql_update_multiple06(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_tests := (
                    SELECT U2 {
                        @note := 'note' + U2.name[-1]
                    } FILTER U2.name != 'update-test1'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                annotated_tests: {
                    name,
                    @note
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'annotated_tests': [{
                    'name': 'update-test2',
                    '@note': 'note2',
                }, {
                    'name': 'update-test3',
                    '@note': 'note3',
                }],
            },
        ])

    async def test_edgeql_update_props01(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                weighted_tags := (
                    SELECT Tag {
                        @weight :=
                            1 IF Tag.name = 'boring' ELSE
                            2 IF Tag.name = 'wow' ELSE
                            3
                    }
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                weighted_tags: {
                    name,
                    @weight
                } ORDER BY @weight
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'weighted_tags': [{
                    'name': 'boring',
                    '@weight': 1,
                }, {
                    'name': 'wow',
                    '@weight': 2,
                }, {
                    'name': 'fun',
                    '@weight': 3,
                }],
            },
        ])

    async def test_edgeql_update_props02(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                weighted_tags := (
                    SELECT Tag {@weight := 1} FILTER Tag.name = 'wow')
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                weighted_tags: {
                    name,
                    @weight
                } ORDER BY @weight
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'weighted_tags': [{
                    'name': 'wow',
                    '@weight': 1,
                }],
            },
        ])

    async def test_edgeql_update_props03(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                weighted_tags := (
                    SELECT Tag {
                        @weight := len(Tag.name) % 2 + 1
                    } FILTER Tag.name IN ['wow', 'boring']
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                weighted_tags: {
                    name,
                    @weight
                } ORDER BY @weight
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'weighted_tags': [{
                    'name': 'boring',
                    '@weight': 1,
                }, {
                    'name': 'wow',
                    '@weight': 2,
                }],
            },
        ])

    async def test_edgeql_update_props05(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_status := (
                    SELECT Status {
                        @note := 'Victor'
                    } FILTER Status.name = 'Closed'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                annotated_status: {
                    name,
                    @note
                }
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'annotated_status': {
                    'name': 'Closed',
                    '@note': 'Victor',
                },
            },
        ])

    async def test_edgeql_update_props06(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_status := (
                    SELECT Status {
                        @note := 'Victor'
                    } FILTER Status = UpdateTest.status
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                annotated_status: {
                    name,
                    @note
                }
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'annotated_status': {
                    'name': 'Open',
                    '@note': 'Victor',
                },
            },
        ])

    async def test_edgeql_update_props07(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_status := (
                    SELECT Status FILTER Status.name = 'Open'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                annotated_status: {
                    name,
                    @note
                }
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'annotated_status': {
                    'name': 'Open',
                    '@note': None,
                },
            },
        ])

    async def test_edgeql_update_props08(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_status := (
                    SELECT Status {
                        @note := 'Victor'
                    } FILTER Status.name = 'Open'
                )
            };

            # update again, erasing the 'note' value
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                annotated_status: {
                    @note := <str>{}
                }
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                annotated_status: {
                    name,
                    @note
                }
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'annotated_status': {
                    'name': 'Open',
                    '@note': None,
                },
            },
        ])

    @tb.expected_optimizer_failure
    async def test_edgeql_update_for01(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            FOR x IN {
                (name := 'update-test1', comment := 'foo'),
                (name := 'update-test2', comment := 'bar')
            }
            UPDATE UpdateTest
            FILTER UpdateTest.name = x.name
            SET {
                comment := x.comment
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                comment
            } ORDER BY UpdateTest.name;
        """)

        self.assert_data_shape(res[-1], [
            {
                'name': 'update-test1',
                'comment': 'foo'
            },
            {
                'name': 'update-test2',
                'comment': 'bar'
            },
            {
                'name': 'update-test3',
                'comment': 'third'
            },
        ])
