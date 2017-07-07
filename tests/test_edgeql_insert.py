##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path
import unittest
import uuid

from edgedb.client import exceptions as exc
from edgedb.server import _testbase as tb


class TestInsert(tb.QueryTestCase):
    '''The scope of the tests is testing various modes of Object creation.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'insert.eschema')

    TEARDOWN_METHOD = """
        DELETE test::Subordinate;
        DELETE test::InsertTest;
        DELETE test::DefaultTest1;
        DELETE test::DefaultTest2;
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
            FILTER
                test::InsertTest.name = 'insert simple 01'
            ORDER BY
                test::InsertTest.l2;
        """)

        self.assert_data_shape(result, [
            [1],

            [1],

            [1],

            [1],

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

    async def test_edgeql_insert_simple02(self):
        res = await self.con.execute('''
            WITH MODULE test
            INSERT DefaultTest1 { foo := '02' };

            INSERT test::DefaultTest1 { foo := '02' };

            INSERT test::DefaultTest1 { foo := '02' };

            WITH MODULE test
            SELECT DefaultTest1 { num } FILTER DefaultTest1.foo = '02';
        ''')

        self.assert_data_shape(
            res[-1],
            [{'num': 42}, {'num': 42}, {'num': 42}],
        )

    async def test_edgeql_insert_simple03(self):
        res = await self.con.execute('''
            INSERT test::DefaultTest1 { num:=100 };

            WITH MODULE test
            INSERT DefaultTest2;

            INSERT test::DefaultTest1 { num:=101 };

            INSERT test::DefaultTest2;

            INSERT test::DefaultTest1 { num:=102 };

            INSERT test::DefaultTest2;

            WITH MODULE test
            SELECT DefaultTest2 { num }
            ORDER BY DefaultTest2.num;
        ''')

        self.assert_data_shape(
            res[-1],
            [{'num': 101}, {'num': 102}, {'num': 103}],
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
                    FILTER test::Subordinate.name LIKE 'subtest%'
                )
            };

            SELECT test::InsertTest {
                subordinates: {
                    name,
                    @comment,
                } ORDER BY test::InsertTest.subordinates.name
            }
            FILTER
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
                        @comment := (SELECT 'comment ' + Subordinate.name)
                    }
                    FILTER Subordinate.name IN ['subtest 3', 'subtest 4']
                )
            };

            WITH MODULE test
            SELECT InsertTest {
                subordinates: {
                    name,
                    @comment,
                } ORDER BY InsertTest.subordinates.name
            }
            FILTER
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
                } ORDER BY InsertTest.subordinates.name
            }
            FILTER
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
                } ORDER BY InsertTest.subordinates.name
            }
            FILTER
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
                    FILTER Subordinate.name = 'only subordinate'
                )
            };

            WITH MODULE test
            SELECT InsertTest {
                name,
                l2,
                subordinates: {
                    name
                }
            } FILTER InsertTest.name = 'insert nested 5';
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
            SELECT (INSERT DefaultTest1 {
                foo := 'ret2',
                num := 2,
            }) {foo};

            WITH MODULE test
            SELECT (INSERT DefaultTest1 {
                foo := 'ret3',
                num := 3,
            }).num;
        ''')

        self.assert_data_shape(
            res,
            [
                [1],
                [{
                    'foo': 'ret2',
                }],
                [3],
            ]
        )

    async def test_edgeql_insert_returning02(self):
        res = await self.con.execute('''
            WITH MODULE test
            SELECT SINGLETON (INSERT DefaultTest1 {
                foo := 'ret1',
                num := 1,
            });

            WITH MODULE test
            SELECT SINGLETON (INSERT DefaultTest1 {
                foo := 'ret2',
                num := 2,
            }) {foo};

            WITH MODULE test
            SELECT SINGLETON (INSERT DefaultTest1 {
                foo := 'ret3',
                num := 3,
            }).num;
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

    async def test_edgeql_insert_returning03(self):
        res = await self.con.execute('''
            INSERT test::Subordinate {
                name := 'sub returning 3'
            };

            WITH
                MODULE test,
                I := (INSERT InsertTest {
                    name := 'insert nested returning 3',
                    l2 := 0,
                    subordinates := (
                        SELECT Subordinate
                        FILTER Subordinate.name = 'sub returning 3'
                    )
                })
            SELECT I {
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
                'name': 'insert nested returning 3',
                'l2': 0,
                'subordinates': [{
                    'name': 'sub returning 3'
                }]
            }],
        )

    async def test_edgeql_insert_returning04(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT (INSERT DefaultTest1 {
                foo := 'DT returning 5',
                num := 33,
            }) {foo, num};

            WITH
                MODULE test,
                I := (INSERT _ := InsertTest {
                    name := 'IT returning 5',
                    l2 := 9999,
                })
            SELECT
                DefaultTest1 {foo, num}
                FILTER DefaultTest1.num > I.l2;

            WITH
                MODULE test,
                I := (INSERT _ := InsertTest {
                    name := 'IT returning 5',
                    l2 := 9,
                })
            SELECT
                DefaultTest1 {foo, num}
                FILTER DefaultTest1.num > I.l2;
        ''', [
            [{
                'foo': 'DT returning 5',
                'num': 33,
            }],
            [],
            [{
                'foo': 'DT returning 5',
                'num': 33,
            }],
        ])

    @unittest.expectedFailure
    async def test_edgeql_insert_for01(self):
        res = await self.con.execute('''
            WITH MODULE test
            FOR x IN {3, 5, 7, 2}
            INSERT InsertTest {
                name := 'insert for 1',
                l2 := x,
            };

            WITH MODULE test
            FOR Q IN (SELECT InsertTest{foo := 'foo' + <str> InsertTest.l2}
                        FILTER .name = 'insert for 1')
            INSERT InsertTest {
                name := 'insert for 1',
                l2 := 35 % Q.l2,
                l3 := Q.foo,
            };

            WITH MODULE test
            SELECT InsertTest{name, l2, l3}
            FILTER .name = 'insert for 1'
            ORDER BY .l2 THEN .l3;
        ''')

        self.assert_data_shape(
            res[-1],
            # insertion based on existing data
            [{
                'name': 'insert for 1',
                'l2': 0,
                'l3': 'foo5',
            }],
            [{
                'name': 'insert for 1',
                'l2': 0,
                'l3': 'foo7',
            }],
            [{
                'name': 'insert for 1',
                'l2': 1,
                'l3': 'foo2',
            }],
            [{
                'name': 'insert for 1',
                'l2': 2,
                'l3': 'foo3',
            }],
            # inserted based on static data
            [{
                'name': 'insert for 1',
                'l2': 2,
                'l3': 'test',
            }],
            [{
                'name': 'insert for 1',
                'l2': 3,
                'l3': 'test',
            }],
            [{
                'name': 'insert for 1',
                'l2': 5,
                'l3': 'test',
            }],
            [{
                'name': 'insert for 1',
                'l2': 7,
                'l3': 'test',
            }],
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_as_expr01(self):
        res = await self.con.execute(r'''
            # insert several objects, then annotate one of the inserted batch
            #
            WITH MODULE test
            FOR x IN (
                SELECT _i := (
                    FOR y IN {3, 5, 7, 2}
                    INSERT InsertTest {
                        name := 'insert expr 1',
                        l2 := y,
                    }
                ) ORDER BY _i.l2 DESC LIMIT 1
            )
            INSERT Annotation {
                name := 'insert expr 1',
                note := 'largest ' + <str>x.l2,
                subject := x
            };

            WITH MODULE test
            SELECT
                InsertTest {
                    name,
                    l2,
                    <subject: {
                        name,
                        note,
                    }
                }
            FILTER .name = 'insert expr 1'
            ORDER BY .l2;
        ''')

        self.assert_data_shape(
            res[-1],
            # inserted based on static data
            [{
                'name': 'insert expr 1',
                'l2': 2,
                'l3': 'test',
                'subject': [],
            }],
            [{
                'name': 'insert expr 1',
                'l2': 3,
                'l3': 'test',
                'subject': [],
            }],
            [{
                'name': 'insert expr 1',
                'l2': 5,
                'l3': 'test',
                'subject': [],
            }],
            [{
                'name': 'insert expr 1',
                'l2': 7,
                'l3': 'test',
                'subject': [{
                    'name': 'insert expr 1',
                    'note': 'largest 7'
                }]
            }],
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_as_expr02(self):
        res = await self.con.execute(r'''
            # same as above, but refactored differently
            #
            WITH
                MODULE test,
                _i := (
                    FOR x IN {3, 5, 7, 2}
                    INSERT InsertTest {
                        name := 'insert expr 2',
                        l2 := x,
                    }
                ),
                y := (SELECT _i ORDER BY _i.l2 DESC LIMIT 1)
            INSERT Annotation {
                name := 'insert expr 2',
                note := 'largest ' + <str>y.l2,
                subject := y
            };

            WITH MODULE test
            SELECT
                InsertTest {
                    name,
                    l2,
                    <subject: {
                        name,
                        note,
                    }
                }
            FILTER .name = 'insert expr 2'
            ORDER BY .l2;
        ''')

        self.assert_data_shape(
            res[-1],
            # inserted based on static data
            [{
                'name': 'insert expr 2',
                'l2': 2,
                'l3': 'test',
                'subject': [],
            }],
            [{
                'name': 'insert expr 2',
                'l2': 3,
                'l3': 'test',
                'subject': [],
            }],
            [{
                'name': 'insert expr 2',
                'l2': 5,
                'l3': 'test',
                'subject': [],
            }],
            [{
                'name': 'insert expr 2',
                'l2': 7,
                'l3': 'test',
                'subject': [{
                    'name': 'insert expr 2',
                    'note': 'largest 7'
                }]
            }],
        )
