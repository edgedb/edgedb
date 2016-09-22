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


class TestInsert(tb.QueryTestCase):
    SETUP = """
        CREATE DELTA test::d_insert01 TO $$
            link subordinates:
                linkproperty comment to str

            concept Subordinate:
                required link name to str

            concept InsertTest:
                link name to str
                link l1 to int
                required link l2 to int
                link l3 to str:
                    default: "test"
                link subordinates to Subordinate:
                    mapping: "1*"

            concept DefaultTest1:
                link num to int:
                    default: 42
                link foo to str

            concept DefaultTest2:
                required link num to int:
                    # XXX: circumventing sequence deficiency
                    default:=
                        SELECT DefaultTest.num + 1
                        ORDER BY DefaultTest.num DESC
                        LIMIT 1
                link foo to str
        $$;

        COMMIT DELTA test::d_insert01;
    """

    async def test_edgeql_insert_fail_1(self):
        err = 'missing value for required pointer ' + \
              '{test::InsertTest}.{test::l2}'
        with self.assertRaisesRegex(exc.MissingRequiredPointerError, err):
            await self.con.execute('''
                INSERT test::InsertTest;
            ''')

    async def test_edgeql_insert_simple01(self):
        result = await self.con.execute(r"""
            INSERT test::InsertTest {
                name := 'insert simple 01',
                l2 := 0,
                l3 := 'test'
            };

            INSERT test::InsertTest {
                name := 'insert simple 01',
                l3 := "Test\"1\"",
                l2 := 1
            };

            INSERT test::InsertTest {
                name := 'insert simple 01',
                l3 := 'Test\'2\'',
                l2 := 2
            };

            INSERT test::InsertTest {
                name := 'insert simple 01',
                l3 := '\"Test\'3\'\"',
                l2 := 3
            };

            SELECT
                test::InsertTest {
                    l2, l3
                }
            WHERE
                test::InsertTest.name = 'insert simple 01'
            ORDER BY
                test::InsertTest.l2;
        """)

        self.assert_data_shape(result, [
            [{
                'id': uuid.UUID,
            }],

            [{
                'id': uuid.UUID,
            }],

            [{
                'id': uuid.UUID,
            }],

            [{
                'id': uuid.UUID,
            }],

            [{
                'id': uuid.UUID,
                'l2': 0,
                'l3': 'test',
            }, {
                'id': uuid.UUID,
                'l2': 1,
                'l3': 'Test"1"',
            }, {
                'id': uuid.UUID,
                'l2': 2,
                'l3': "Test'2'",
            }, {
                'id': uuid.UUID,
                'l2': 3,
                'l3': '''"Test'3'"''',
            }]
        ])

    @unittest.expectedFailure
    async def test_edgeql_insert_simple02(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT DefaultTest1;

            INSERT test::DefaultTest1;

            INSERT test::DefaultTest1;

            WITH MODULE test
            SELECT DefaultTest1 { num };
        ''')

        self.assert_data_shape(
            res[-1],
            [{'num': 42}, {'num': 42}, {'num': 42}],
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_simple03(self):
        res = await self.con.execute('''
            INSERT test::DefaultTest2{ num:=0 };

            WITH MODULE test
            INSERT DefaultTest2;

            INSERT test::DefaultTest2;

            INSERT test::DefaultTest2;

            WITH MODULE test
            SELECT DefaultTest2 { num }
            ORDER BY DefaultTest2.num;
        ''')

        self.assert_data_shape(
            res[-1],
            [{'num': 0}, {'num': 1}, {'num': 2}, {'num': 3}],
        )

    async def test_edgeql_insert_nested01(self):
        res = await self.con.execute('''
            INSERT test::Subordinate {
                name := 'subtest 1'
            };

            INSERT test::Subordinate {
                name := 'subtest 2'
            };

            INSERT test::InsertTest {
                name := 'insert nested',
                l2 := 0,
                subordinates := (
                    SELECT test::Subordinate
                    WHERE test::Subordinate.name LIKE 'subtest%'
                )
            };

            SELECT test::InsertTest {
                subordinates: {
                    name,
                    @comment,
                } ORDER BY test::Subordinate.name
            }
            WHERE
                test::InsertTest.name = 'insert nested';
        ''')

        self.assert_data_shape(
            res[-1],
            [{
                'id': uuid.UUID,
                'subordinates': [{
                    'id': uuid.UUID,
                    'name': 'subtest 1',
                    '@comment': None,
                }, {
                    'id': uuid.UUID,
                    'name': 'subtest 2',
                    '@comment': None,
                }]
            }]
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_nested02(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT Subordinate {
                name := 'subtest 3'
            };

            WITH MODULE test
            INSERT Subordinate {
                name := 'subtest 4'
            };

            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 2',
                l2 := 0,
                subordinates := (
                    SELECT Subordinate {
                        @comment:= (SELECT 'comment ' + Subordinate.name)
                    }
                    WHERE Subordinate.name IN ('subtest 3', 'subtest 4')
                )
            };

            WITH MODULE test
            SELECT InsertTest {
                subordinates: {
                    name,
                    @comment,
                } ORDER BY Subordinate.name
            }
            WHERE
                InsertTest.name = 'insert nested 2';
        ''')

        self.assert_data_shape(
            res[-1],
            [{
                'id': uuid.UUID,
                'subordinates': [{
                    'id': uuid.UUID,
                    'name': 'subtest 3',
                    '@comment': 'comment subtest 3',
                }, {
                    'id': uuid.UUID,
                    'name': 'subtest 4',
                    '@comment': 'comment subtest 4',
                }]
            }]
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_nested03(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 3',
                l2 := 0,
                subordinates: Subordinate {
                    name := 'nested sub 3.1'
                }
            };

            WITH MODULE test
            SELECT InsertTest {
                subordinates: {
                    name
                } ORDER BY Subordinate.name
            }
            WHERE
                InsertTest.name = 'insert nested 3';
        ''')

        self.assert_data_shape(
            res[-1],
            [{
                'id': uuid.UUID,
                'subordinates': [{
                    'id': uuid.UUID,
                    'name': 'nested sub 3.1'
                }]
            }]
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_nested04(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 4',
                l2 := 0,
                subordinates: Subordinate {
                    name := 'nested sub 4.1',
                    @comment := 'comment 4.1',
                }
            };

            WITH MODULE test
            SELECT InsertTest {
                subordinates: {
                    name,
                    @comment,
                } ORDER BY Subordinate.name
            }
            WHERE
                InsertTest.name = 'insert nested 4';
        ''')

        self.assert_data_shape(
            res[-1],
            [{
                'id': uuid.UUID,
                'subordinates': [{
                    'id': uuid.UUID,
                    'name': 'nested sub 4.1',
                    '@comment': 'comment 4.1'
                }]
            }]
        )

    async def test_edgeql_insert_nested05(self):
        res = await self.con.execute('''
            INSERT test::Subordinate {
                name := 'only subordinate'
            };

            INSERT test::Subordinate {
                name := 'never subordinate'
            };

            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 5',
                l2 := 0,
                subordinates := (
                    SELECT Subordinate
                    WHERE Subordinate.name = 'only subordinate'
                )
            };

            WITH MODULE test
            SELECT InsertTest {
                name,
                l2,
                subordinates: {
                    name
                }
            } WHERE InsertTest.name = 'insert nested 5';
        ''')

        self.assert_data_shape(
            res[-1],
            [{
                'name': 'insert nested 5',
                'l2': 0,
                'subordinates': [{
                    'name': 'only subordinate'
                }]
            }],
        )

    async def test_edgeql_insert_returning01(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret1',
                num := 1,
            };

            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret2',
                num := 2,
            } RETURNING DefaultTest1 {foo};

            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret3',
                num := 3,
            } RETURNING DefaultTest1.num;
        ''')

        self.assert_data_shape(
            res,
            [
                [{
                    'id': uuid.UUID,
                }],
                [{
                    'foo': 'ret2',
                }],
                [3],
            ]
        )

    async def test_edgeql_insert_returning02(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret1',
                num := 1,
            } RETURNING SINGLE DefaultTest1;

            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret2',
                num := 2,
            } RETURNING SINGLE DefaultTest1 {foo};

            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret3',
                num := 3,
            } RETURNING SINGLE DefaultTest1.num;
        ''')

        self.assert_data_shape(
            res,
            [
                [{
                    'id': uuid.UUID,
                }],
                [{
                    'foo': 'ret2',
                }],
                [3],
            ]
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_returning03(self):
        res = await self.con.execute('''
            INSERT test::Subordinate {
                name := 'sub returning 1'
            };

            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested returning 1',
                l2 := 0,
                subordinates := (
                    SELECT Subordinate
                    WHERE Subordinate.name = 'sub returning 1'
                )
            } RETURNING InsertTest {
                name,
                l2,
                subordinates: {
                    name
                }
            };
        ''')

        self.assert_data_shape(
            res[-1],
            [{
                'name': 'insert returning 1',
                'l2': 0,
                'subordinates': [{
                    'name': 'sub returning 1'
                }]
            }],
        )
