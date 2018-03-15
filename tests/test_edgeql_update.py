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

    async def test_edgeql_update_simple_01(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            # bad name doesn't exist, so no update is expected
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
            };
        """)

        self.assert_data_shape(res, [
            [0],
            self.original,
        ])

    async def test_edgeql_update_simple_02(self):
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

    async def test_edgeql_update_simple_03(self):
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

    async def test_edgeql_update_simple_04(self):
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

    async def test_edgeql_update_returning_01(self):
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

    async def test_edgeql_update_returning_02(self):
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

    async def test_edgeql_update_returning_03(self):
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

    async def test_edgeql_update_returning_04(self):
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

    async def test_edgeql_update_generic_01(self):
        status = await self.con.execute(r"""
            WITH MODULE test
            SELECT Status{id}
            FILTER Status.name = 'Open';
        """)
        status = status[0][0]['id']

        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test3'
            SET {
                status := (
                    SELECT Status
                    FILTER Status.id = <uuid>'""" + status + r"""'
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

    @unittest.expectedFailure
    async def test_edgeql_update_filter_01(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE (SELECT UpdateTest)
            # this FILTER is irrelevant because UpdateTest is wrapped
            # into a SET OF by SELECT
            FILTER UpdateTest.name = 'bad name'
            SET {
                comment := 'bad test'
            };

            WITH MODULE test
            SELECT UpdateTest.comment;
        """)

        self.assert_data_shape(res, [
            [3],
            ['bad test'] * 3,
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_filter_02(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE ({} ?? UpdateTest)
            # this FILTER is irrelevant because UpdateTest is wrapped
            # into a SET OF by ??
            FILTER UpdateTest.name = 'bad name'
            SET {
                comment := 'bad test'
            };

            WITH MODULE test
            SELECT UpdateTest.comment;
        """)

        self.assert_data_shape(res, [
            [3],
            ['bad test'] * 3,
        ])

    async def test_edgeql_update_multiple_01(self):
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

    async def test_edgeql_update_multiple_02(self):
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

    async def test_edgeql_update_multiple_03(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                tags := (SELECT Tag FILTER Tag.name IN {'wow', 'fun'})
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

    @unittest.expectedFailure
    async def test_edgeql_update_multiple_04(self):
        res = await self.con.execute(r"""
            # first add a tag to UpdateTest
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                tags := (
                    SELECT Tag
                    FILTER Tag.name = 'fun'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                tags: {
                    name
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';

            # now add another tag, but keep the existing one, too
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                tags := UpdateTest.tags UNION (
                    SELECT Tag
                    FILTER Tag.name = 'wow'
                )
            };

            WITH MODULE test
            SELECT UpdateTest {
                name,
                tags: {
                    name
                } ORDER BY .name
            } FILTER UpdateTest.name = 'update-test1';
        """)

        self.assert_data_shape(res, [
            [1],
            [{
                'name': 'update-test1',
                'tags': [{
                    'name': 'fun',
                }],
            }],
            [1],
            [{
                'name': 'update-test1',
                'tags': [{
                    'name': 'fun',
                }, {
                    'name': 'wow',
                }],
            }],
        ])

    async def test_edgeql_update_multiple_05(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                U2 := DETACHED UpdateTest
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

    async def test_edgeql_update_multiple_06(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                U2 := DETACHED UpdateTest
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

    async def test_edgeql_update_multiple_07(self):
        res = await self.con.execute(r"""
            WITH
                MODULE test,
                U2 := DETACHED UpdateTest
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

    @unittest.expectedFailure
    async def test_edgeql_update_multiple_08(self):
        res = await self.con.execute(r"""
            # make tests related to the other 2
            WITH
                MODULE test,
                UT := DETACHED UpdateTest
            UPDATE UpdateTest
            SET {
                related := (SELECT UT FILTER UT != UpdateTest)
            };

            WITH MODULE test
            SELECT UpdateTest{
                name,
                related: {name} ORDER BY .name
            } ORDER BY .name;

            # now update related tests based on existing related tests
            WITH
                MODULE test,
                UT := DETACHED UpdateTest
            UPDATE UpdateTest
            SET {
                # since there are 2 tests in each FILTER, != is
                # guaranteed to be TRUE for at least one of them
                related := (SELECT UT FILTER UT != UpdateTest.related)
            };

            WITH MODULE test
            SELECT UpdateTest{
                name,
                related: {name} ORDER BY .name
            } ORDER BY .name;
        """)

        self.assert_data_shape(res, [
            [3],
            [
                {
                    'name': 'update-test1',
                    'related': [
                        {'name': 'update-test2'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test2',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test3',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test2'},
                    ],
                },
            ],
            [3],
            [
                {
                    'name': 'update-test1',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test2'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test2',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test2'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test3',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test2'},
                        {'name': 'update-test3'},
                    ],
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_multiple_09(self):
        res = await self.con.execute(r"""
            # make tests related to the other 2
            WITH
                MODULE test,
                UT := DETACHED UpdateTest
            UPDATE UpdateTest
            SET {
                related := (SELECT UT FILTER UT != UpdateTest)
            };

            WITH MODULE test
            SELECT UpdateTest{
                name,
                related: {name} ORDER BY .name
            } ORDER BY .name;

            # now update related tests based on existing related tests
            WITH
                MODULE test,
                UT := DETACHED UpdateTest
            UPDATE UpdateTest
            SET {
                # this should make the related test be the same as parent
                related := (SELECT UT FILTER UT NOT IN UpdateTest.related)
            };

            WITH MODULE test
            SELECT UpdateTest{
                name,
                related: {name} ORDER BY .name
            } ORDER BY .name;
        """)

        self.assert_data_shape(res, [
            [3],
            [
                {
                    'name': 'update-test1',
                    'related': [
                        {'name': 'update-test2'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test2',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test3',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test2'},
                    ],
                },
            ],
            [3],
            [
                {
                    'name': 'update-test1',
                    'related': [
                        {'name': 'update-test1'},
                    ],
                },
                {
                    'name': 'update-test2',
                    'related': [
                        {'name': 'update-test2'},
                    ],
                },
                {
                    'name': 'update-test3',
                    'related': [
                        {'name': 'update-test3'},
                    ],
                },
            ],
        ])

    @unittest.expectedFailure
    async def test_edgeql_update_multiple_10(self):
        res = await self.con.execute(r"""
            # make each test related to 'update-test1'
            WITH
                MODULE test,
                UT := DETACHED (
                    SELECT UpdateTest FILTER UpdateTest.name = 'update-test1'
                )
            UPDATE UpdateTest
            SET {
                related := UT
            };

            WITH MODULE test
            SELECT UpdateTest{
                name,
                related: {name} ORDER BY .name
            } ORDER BY .name;

            # now update related tests
            WITH MODULE test
            # there's only one item in the UPDATE set
            UPDATE UpdateTest.related
            SET {
                # every test is .<related to 'update-test1'
                related := UpdateTest.related.<related
            };

            WITH MODULE test
            SELECT UpdateTest{
                name,
                related: {name} ORDER BY .name
            } ORDER BY .name;
        """)

        self.assert_data_shape(res, [
            [3],
            [
                {
                    'name': 'update-test1',
                    'related': [
                        {'name': 'update-test1'},
                    ],
                },
                {
                    'name': 'update-test2',
                    'related': [
                        {'name': 'update-test1'},
                    ],
                },
                {
                    'name': 'update-test3',
                    'related': [
                        {'name': 'update-test1'},
                    ],
                },
            ],
            [1],
            [
                {
                    'name': 'update-test1',
                    'related': [
                        {'name': 'update-test1'},
                        {'name': 'update-test2'},
                        {'name': 'update-test3'},
                    ],
                },
                {
                    'name': 'update-test2',
                    'related': [
                        {'name': 'update-test1'},
                    ],
                },
                {
                    'name': 'update-test3',
                    'related': [
                        {'name': 'update-test1'},
                    ],
                },
            ],
        ])

    async def test_edgeql_update_props_01(self):
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

    async def test_edgeql_update_props_02(self):
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

    async def test_edgeql_update_props_03(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            UPDATE UpdateTest
            FILTER UpdateTest.name = 'update-test1'
            SET {
                weighted_tags := (
                    SELECT Tag {
                        @weight := len(Tag.name) % 2 + 1
                    } FILTER Tag.name IN {'wow', 'boring'}
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

    async def test_edgeql_update_props_05(self):
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

    async def test_edgeql_update_props_06(self):
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

    async def test_edgeql_update_props_07(self):
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

    async def test_edgeql_update_props_08(self):
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

    async def test_edgeql_update_for_01(self):
        res = await self.con.execute(r"""
            WITH MODULE test
            FOR x IN {
                    (name := 'update-test1', comment := 'foo'),
                    (name := 'update-test2', comment := 'bar')
                }
            UNION (
                UPDATE UpdateTest
                FILTER UpdateTest.name = x.name
                SET {
                    comment := x.comment
                }
            );

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
