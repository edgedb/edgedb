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


import os.path
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestInsert(tb.QueryTestCase):
    '''The scope of the tests is testing various modes of Object creation.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'insert.esdl')

    async def test_edgeql_insert_fail_1(self):
        err = 'missing value for required property ' + \
              'test::InsertTest.l2'
        with self.assertRaisesRegex(edgedb.MissingRequiredError, err):
            await self.con.execute('''
                INSERT test::InsertTest;
            ''')

    async def test_edgeql_insert_simple_01(self):
        await self.con.execute(r"""
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
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    test::InsertTest {
                        l2, l3
                    }
                FILTER
                    test::InsertTest.name = 'insert simple 01'
                ORDER BY
                    test::InsertTest.l2;
            """,
            [
                {
                    'l2': 0,
                    'l3': 'test',
                },
                {
                    'l2': 1,
                    'l3': 'Test"1"',
                },
                {
                    'l2': 2,
                    'l3': "Test'2'",
                },
                {
                    'l2': 3,
                    'l3': '''"Test'3'"''',
                }
            ]
        )

    async def test_edgeql_insert_simple_02(self):
        await self.con.execute('''
            WITH MODULE test
            INSERT DefaultTest1 { foo := '02' };

            INSERT test::DefaultTest1 { foo := '02' };

            INSERT test::DefaultTest1 { foo := '02' };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest1 { num } FILTER DefaultTest1.foo = '02';
            ''',
            [{'num': 42}, {'num': 42}, {'num': 42}],
        )

    async def test_edgeql_insert_simple_03(self):
        await self.con.execute('''
            INSERT test::DefaultTest1 { num := 100 };

            WITH MODULE test
            INSERT DefaultTest2;

            INSERT test::DefaultTest1 { num := 101 };

            INSERT test::DefaultTest2;

            INSERT test::DefaultTest1 { num := 102 };

            INSERT test::DefaultTest2;
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest2 { num }
                ORDER BY DefaultTest2.num;
            ''',
            [{'num': 101}, {'num': 102}, {'num': 103}],
        )

    async def test_edgeql_insert_nested_01(self):
        await self.con.execute('''
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
        ''')

        await self.assert_query_result(
            r'''
                SELECT test::InsertTest {
                    subordinates: {
                        name,
                        @comment,
                    } ORDER BY test::InsertTest.subordinates.name
                }
                FILTER
                    test::InsertTest.name = 'insert nested';
            ''',
            [{
                'subordinates': [{
                    'name': 'subtest 1',
                    '@comment': None,
                }, {
                    'name': 'subtest 2',
                    '@comment': None,
                }]
            }]
        )

    async def test_edgeql_insert_nested_02(self):
        await self.con.execute('''
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
                        @comment := (SELECT 'comment ' ++ Subordinate.name)
                    }
                    FILTER Subordinate.name IN {'subtest 3', 'subtest 4'}
                )
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InsertTest {
                    subordinates: {
                        name,
                        @comment,
                    } ORDER BY InsertTest.subordinates.name
                }
                FILTER
                    InsertTest.name = 'insert nested 2';
            ''',
            [{
                'subordinates': [{
                    'name': 'subtest 3',
                    '@comment': 'comment subtest 3',
                }, {
                    'name': 'subtest 4',
                    '@comment': 'comment subtest 4',
                }]
            }]
        )

    async def test_edgeql_insert_nested_03(self):
        await self.con.execute('''
            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 3',
                l2 := 0,
                subordinates := (INSERT Subordinate {
                    name := 'nested sub 3.1'
                })
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InsertTest {
                    subordinates: {
                        name
                    } ORDER BY InsertTest.subordinates.name
                }
                FILTER
                    InsertTest.name = 'insert nested 3';
            ''',
            [{
                'subordinates': [{
                    'name': 'nested sub 3.1'
                }]
            }]
        )

    async def test_edgeql_insert_nested_04(self):
        await self.con.execute('''
            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 4',
                l2 := 0,
                subordinates := (INSERT Subordinate {
                    name := 'nested sub 4.1',
                    @comment := 'comment 4.1',
                })
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InsertTest {
                    subordinates: {
                        name,
                        @comment,
                    } ORDER BY InsertTest.subordinates.name
                }
                FILTER
                    InsertTest.name = 'insert nested 4';
            ''',
            [{
                'subordinates': [{
                    'name': 'nested sub 4.1',
                    '@comment': 'comment 4.1'
                }]
            }]
        )

    async def test_edgeql_insert_nested_05(self):
        await self.con.execute('''
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
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InsertTest {
                    name,
                    l2,
                    subordinates: {
                        name
                    }
                } FILTER InsertTest.name = 'insert nested 5';
            ''',
            [{
                'name': 'insert nested 5',
                'l2': 0,
                'subordinates': [{
                    'name': 'only subordinate'
                }]
            }],
        )

    @test.xfail('''
        This test fails with the following error:
        invalid reference to link property in top level shape

        The culprit appears to be specifically the LIMIT 1.
    ''')
    async def test_edgeql_insert_nested_06(self):
        await self.con.execute('''
            WITH MODULE test
            INSERT Subordinate {
                name := 'linkprop test target 6'
            };

            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 6',
                l2 := 0,
                subordinates := (
                    SELECT Subordinate {
                        @comment := 'comment 6'
                    }
                    LIMIT 1
                )
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InsertTest {
                    subordinates: {
                        name,
                        @comment,
                    }
                }
                FILTER
                    InsertTest.name = 'insert nested 6';
            ''',
            [{
                'subordinates': [{
                    'name': 'linkprop test target 6',
                    '@comment': 'comment 6'
                }]
            }]
        )

    async def test_edgeql_insert_nested_07(self):
        with self.assertRaisesRegex(
                edgedb.EdgeQLSyntaxError,
                "unexpected ':'"):
            await self.con.execute('''
                WITH MODULE test
                INSERT InsertTest {
                    subordinates: Subordinate {
                        name := 'nested sub 7.1',
                        @comment := 'comment 7.1',
                    }
                };
            ''')

    async def test_edgeql_insert_nested_08(self):
        await self.assert_query_result(r'''
            WITH
                MODULE test,
                x1 := (
                    INSERT InsertTest {
                        name := 'insert nested 8',
                        l2 := 0,
                        subordinates := (
                            INSERT Subordinate {
                                name := 'nested sub 8.1'
                            }
                        )
                    }
                )
            SELECT x1 {
                name,
                subordinates: {
                    name
                }
            };
        ''', [{
            'name': 'insert nested 8',
            'subordinates': [{'name': 'nested sub 8.1'}]
        }])

    async def test_edgeql_insert_nested_09(self):
        # test a single link with a link property
        await self.con.execute(r'''
            WITH MODULE test
            INSERT InsertTest {
                name := 'insert nested 9',
                l2 := 0,
                sub := (
                    INSERT Subordinate {
                        name := 'nested sub 9',
                        @note := 'sub note 9',
                    }
                )
            }
        ''')

        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT InsertTest {
                name,
                sub: {
                    name,
                    @note
                }
            } FILTER
                .name = 'insert nested 9'
        ''', [{
            'name': 'insert nested 9',
            'sub': {
                'name': 'nested sub 9',
                '@note': 'sub note 9',
            }
        }])

    @test.xfail('''
        edgedb.errors.QueryError: invalid reference to link property
        in top level shape
    ''')
    async def test_edgeql_insert_nested_10(self):
        # test a single link with a link property
        await self.con.execute(r'''
            SET MODULE test;

            INSERT Subordinate {
                name := 'nested sub 10',
            };

            INSERT InsertTest {
                name := 'insert nested 10',
                l2 := 0,
                sub := (
                    SELECT Subordinate {
                        @note := 'sub note 10',
                    }
                    FILTER .name = 'nested sub 10'
                    LIMIT 1
                )
            }
        ''')

        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT InsertTest {
                name,
                sub: {
                    name,
                    @note
                }
            } FILTER
                .name = 'insert nested 10'
        ''', [{
            'name': 'insert nested 10',
            'sub': {
                'name': 'nested sub 10',
                '@note': 'sub note 10',
            }
        }])

    async def test_edgeql_insert_returning_01(self):
        await self.con.execute('''
            WITH MODULE test
            INSERT DefaultTest1 {
                foo := 'ret1',
                num := 1,
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest1 {
                    foo := 'ret2',
                    num := 2,
                }) {foo};
            ''',
            [{
                'foo': 'ret2',
            }],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest1 {
                    foo := 'ret3',
                    num := 3,
                }).num;
            ''',
            [3],
        )

    async def test_edgeql_insert_returning_02(self):
        await self.assert_query_result(
            '''
                WITH MODULE test
                INSERT DefaultTest1 {
                    foo := 'ret1',
                    num := 1,
                };
            ''',
            [{
                'id': uuid.UUID,
            }],
        )

        await self.assert_query_result(
            '''
                WITH MODULE test
                SELECT (INSERT DefaultTest1 {
                    foo := 'ret2',
                    num := 2,
                }) {foo};
            ''',
            [{
                'foo': 'ret2',
            }],
        )

        await self.assert_query_result(
            '''
                WITH MODULE test
                SELECT (INSERT DefaultTest1 {
                    foo := 'ret3',
                    num := 3,
                }).num;
            ''',
            [3],
        )

        obj = await self.con.fetchone(
            '''
                WITH MODULE test
                INSERT DefaultTest1 {
                    foo := 'ret1',
                    num := 1,
                };
            ''',
        )

        self.assertTrue(hasattr(obj, 'id'))
        self.assertTrue(hasattr(obj, '__tid__'))

    async def test_edgeql_insert_returning_03(self):
        await self.con.execute('''
            INSERT test::Subordinate {
                name := 'sub returning 3'
            };
        ''')

        await self.assert_query_result(
            r'''
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
            ''',
            [{
                'name': 'insert nested returning 3',
                'l2': 0,
                'subordinates': [{
                    'name': 'sub returning 3'
                }]
            }],
        )

    async def test_edgeql_insert_returning_04(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest1 {
                    foo := 'DT returning 4',
                    num := 33,
                }) {foo, num};
            ''',
            [{
                'foo': 'DT returning 4',
                'num': 33,
            }],
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    I := (INSERT _ := InsertTest {
                        name := 'IT returning 4',
                        l2 := 9999,
                    })
                SELECT
                    DefaultTest1 {foo, num}
                    FILTER DefaultTest1.num > I.l2;
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
                WITH
                    MODULE test,
                    I := (INSERT _ := InsertTest {
                        name := 'IT returning 4',
                        l2 := 9,
                    })
                SELECT
                    DefaultTest1 {foo, num}
                    FILTER DefaultTest1.num > I.l2;
            ''',
            [{
                'foo': 'DT returning 4',
                'num': 33,
            }],
        )

    async def test_edgeql_insert_returning_05(self):
        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest1 {
                    foo := 'DT returning 5',
                }) {
                    foo,
                    # test that num will show up with the default value
                    num,
                };
            ''',
            [{
                'foo': 'DT returning 5',
                'num': 42,
            }],
        )

    async def test_edgeql_insert_returning_06(self):
        await self.con.execute('''
            INSERT test::Subordinate {
                name := 'DefaultTest5/Sub'
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest5 {
                    name := 'ret6/DT5',
                }) {
                    name,
                    # test that other will show up with the default value
                    other: {
                        name
                    },
                };
            ''',
            [{
                'name': 'ret6/DT5',
                'other': {
                    'name': 'DefaultTest5/Sub',
                }
            }],
        )

    async def test_edgeql_insert_returning_07(self):
        await self.con.execute('''
            INSERT test::Subordinate {
                name := 'DefaultTest5/Sub'
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest6 {
                    name := 'ret7/DT6',
                }) {
                    name,
                    # test that other will show up with the default value
                    other: {
                        name,
                        other: {
                            name
                        },
                    },
                };
            ''',
            [{
                'name': 'ret7/DT6',
                'other': {
                    'name': 'DefaultTest6/5',
                    'other': {
                        'name': 'DefaultTest5/Sub',
                    }
                }
            }],
        )

    async def test_edgeql_insert_returning_08(self):
        await self.con.execute('''
            INSERT test::Subordinate {
                name := 'DefaultTest5/Sub'
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT (INSERT DefaultTest7 {
                    name := 'ret8/DT7',
                }) {
                    name,
                    # test that other will show up with the default value
                    other: {
                        name,
                        other: {
                            name,
                            other: {
                                name,
                            },
                        },
                    },
                };
            ''',
            [{
                'name': 'ret8/DT7',
                'other': {
                    'name': 'DefaultTest7/6',
                    'other': {
                        'name': 'DefaultTest6/5',
                        'other': {
                            'name': 'DefaultTest5/Sub',
                        }
                    }
                }
            }],
        )

    async def test_edgeql_insert_for_01(self):
        await self.con.execute(r'''
            WITH MODULE test
            FOR x IN {3, 5, 7, 2}
            UNION (INSERT InsertTest {
                name := 'insert for 1',
                l2 := x,
            });

            WITH MODULE test
            FOR Q IN {(SELECT InsertTest{foo := 'foo' ++ <str> InsertTest.l2}
                       FILTER .name = 'insert for 1')}
            UNION (INSERT InsertTest {
                name := 'insert for 1',
                l2 := 35 % Q.l2,
                l3 := Q.foo,
            });
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InsertTest{name, l2, l3}
                FILTER .name = 'insert for 1'
                ORDER BY .l2 THEN .l3;
            ''',
            [
                # insertion based on existing data
                {
                    'name': 'insert for 1',
                    'l2': 0,
                    'l3': 'foo5',
                },
                {
                    'name': 'insert for 1',
                    'l2': 0,
                    'l3': 'foo7',
                },
                {
                    'name': 'insert for 1',
                    'l2': 1,
                    'l3': 'foo2',
                },
                {
                    'name': 'insert for 1',
                    'l2': 2,
                    'l3': 'foo3',
                },
                # inserted based on static data
                {
                    'name': 'insert for 1',
                    'l2': 2,
                    'l3': 'test',
                },
                {
                    'name': 'insert for 1',
                    'l2': 3,
                    'l3': 'test',
                },
                {
                    'name': 'insert for 1',
                    'l2': 5,
                    'l3': 'test',
                },
                {
                    'name': 'insert for 1',
                    'l2': 7,
                    'l3': 'test',
                },
            ]
        )

    async def test_edgeql_insert_for_02(self):
        await self.con.execute(r'''
            # create 10 DefaultTest3 objects, each object is defined
            # as having a randomly generated value for 'foo'
            WITH MODULE test
            FOR x IN {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
            UNION (INSERT DefaultTest3);
        ''')

        await self.assert_query_result(
            r'''
                # statistically, randomly generated value for 'foo'
                # should not be identical for all 10 records
                WITH
                    MODULE test,
                    DT3 := DefaultTest3
                SELECT count(
                    DefaultTest3 FILTER DefaultTest3.foo != DT3.foo) > 0;
            ''',
            {True}
        )

    async def test_edgeql_insert_for_03(self):
        await self.con.execute(r'''
            # Create 5 DefaultTest4 objects. The default value for
            # 'bar' is technically evaluated for each object, but
            # because it is deterministic it will be same for all 5
            # new objects.
            WITH MODULE test
            FOR x IN {1, 2, 3, 4, 5}
            UNION (INSERT DefaultTest4);
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest4.bar
                ORDER BY DefaultTest4.bar;
            ''',
            [0, 0, 0, 0, 0]
        )

    async def test_edgeql_insert_default_01(self):
        await self.con.execute(r'''
            # create 10 DefaultTest3 objects, each object is defined
            # as having a randomly generated value for 'foo'
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;

            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
            INSERT test::DefaultTest3;
        ''')

        await self.assert_query_result(
            r'''
                # statistically, randomly generated value for 'foo'
                # should not be identical for all 10 records
                WITH
                    MODULE test,
                    DT3 := DefaultTest3
                SELECT count(
                    DefaultTest3 FILTER DefaultTest3.foo != DT3.foo) > 0;
            ''',
            {True}
        )

    async def test_edgeql_insert_default_02(self):
        await self.con.execute(r'''
            # by default the 'bar' value is simply going to be "indexing" the
            # created objects
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest4 { bar }
                ORDER BY DefaultTest4.bar;
            ''',
            [
                {
                    'bar': 0,
                },
                {
                    'bar': 1,
                },
                {
                    'bar': 2,
                },
                {
                    'bar': 3,
                },
                {
                    'bar': 4,
                }
            ]
        )

    async def test_edgeql_insert_default_03(self):
        await self.con.execute(r'''
            # by default the 'bar' value is simply going to be "indexing" the
            # created objects
            INSERT test::DefaultTest4 { bar:= 10 };
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest4 { bar }
                ORDER BY DefaultTest4.bar;
            ''',
            [
                {
                    'bar': 1,
                },
                {
                    'bar': 2,
                },
                {
                    'bar': 10,
                }
            ]
        )

    async def test_edgeql_insert_default_04(self):
        await self.con.execute(r'''
            # by default the 'bar' value is simply going to be "indexing" the
            # created objects
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4 { bar:= 0 };
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest4 { bar }
                ORDER BY DefaultTest4.bar;
            ''',
            [
                {
                    'bar': 0,
                },
                {
                    'bar': 0,
                },
                {
                    'bar': 1,
                },
                {
                    'bar': 3,
                },
                {
                    'bar': 4,
                }
            ]
        )

    async def test_edgeql_insert_default_05(self):
        # Issue #730
        await self.con.execute(r'''
            # The 'number' property is supposed to be
            # self-incrementing and read-only.
            INSERT test::DefaultTest8;
            INSERT test::DefaultTest8;
            INSERT test::DefaultTest8;
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT DefaultTest8.number;
            ''',
            {1, 2, 3}
        )

    async def test_edgeql_insert_as_expr_01(self):
        await self.con.execute(r'''
            # insert several objects, then annotate one of the inserted batch
            WITH MODULE test
            FOR x IN {(
                    SELECT _i := (
                        FOR y IN {3, 5, 7, 2}
                        UNION (INSERT InsertTest {
                            name := 'insert expr 1',
                            l2 := y,
                        })
                    ) ORDER BY _i.l2 DESC LIMIT 1
                )}
            UNION (INSERT Note {
                name := 'insert expr 1',
                note := 'largest ' ++ <str>x.l2,
                subject := x
            });
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT
                    InsertTest {
                        name,
                        l2,
                        l3,
                        subject := .<subject[IS Note] {
                            name,
                            note,
                        }
                    }
                FILTER .name = 'insert expr 1'
                ORDER BY .l2;
            ''',
            [
                # inserted based on static data
                {
                    'name': 'insert expr 1',
                    'l2': 2,
                    'l3': 'test',
                    'subject': [],
                },
                {
                    'name': 'insert expr 1',
                    'l2': 3,
                    'l3': 'test',
                    'subject': [],
                },
                {
                    'name': 'insert expr 1',
                    'l2': 5,
                    'l3': 'test',
                    'subject': [],
                },
                {
                    'name': 'insert expr 1',
                    'l2': 7,
                    'l3': 'test',
                    'subject': [{
                        'name': 'insert expr 1',
                        'note': 'largest 7'
                    }]
                },
            ]
        )

    async def test_edgeql_insert_polymorphic_01(self):
        await self.con.execute(r'''
            WITH MODULE test
            INSERT Directive {
                args := (INSERT InputValue {
                    val := "something"
                }),
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT Callable {
                    args: {
                        val
                    }
                };
            ''',
            [{
                'args': [{'val': 'something'}],
            }]
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT Field {
                    args: {
                        val
                    }
                };
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT Directive {
                    args: {
                        val
                    }
                };
            ''',
            [{
                'args': [{'val': 'something'}],
            }],
        )

        await self.assert_query_result(
            r'''
                WITH MODULE test
                SELECT InputValue {
                    val
                };
            ''',
            [{
                'val': 'something',
            }],
        )

    async def test_edgeql_insert_linkprops_with_for_01(self):
        await self.con.execute(r"""
            WITH MODULE test
            FOR i IN {'1', '2', '3'} UNION (
                INSERT Subordinate {
                    name := 'linkproptest ' ++ i
                }
            );

            WITH MODULE test
            INSERT InsertTest {
                l2 := 99,
                subordinates := (
                    FOR x IN {('a', '1'), ('b', '2'), ('c', '3')} UNION (
                        SELECT Subordinate {@comment := x.0}
                        FILTER .name[-1] = x.1
                    )
                )
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT InsertTest {
                    l2,
                    subordinates: {
                        name,
                        @comment,
                    } ORDER BY InsertTest.subordinates.name
                } FILTER .l2 = 99;
            """,
            [{
                'l2': 99,
                'subordinates': [
                    {
                        'name': 'linkproptest 1',
                        '@comment': 'a',
                    },
                    {
                        'name': 'linkproptest 2',
                        '@comment': 'b',
                    },
                    {
                        'name': 'linkproptest 3',
                        '@comment': 'c',
                    }
                ],
            }],
        )

    async def test_edgeql_insert_empty_01(self):
        await self.con.execute(r"""
            WITH MODULE test
            INSERT InsertTest {
                l1 := {},
                l2 := 99,
                # l3 has a default value
                l3 := {},
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT InsertTest {
                    l1,
                    l2,
                    l3
                };
            """,
            [{
                'l1': None,
                'l2': 99,
                'l3': None,
            }],
        )

    async def test_edgeql_insert_empty_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid target.*std::datetime.*expecting 'std::int64'"):
            await self.con.execute(r"""
                WITH MODULE test
                INSERT InsertTest {
                    l1 := <datetime>{},
                    l2 := 99,
                };
                """)

    async def test_edgeql_insert_empty_03(self):
        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r"missing value for required property"):
            await self.con.execute(
                r"""
                    WITH MODULE test
                    INSERT InsertTest {
                        l2 := {},
                    };
                """
            )

    async def test_edgeql_insert_empty_04(self):
        await self.con.execute(r"""
            WITH MODULE test
            INSERT InsertTest {
                l2 := 99,
                subordinates := {}
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT InsertTest {
                    l2,
                    subordinates
                };
            """,
            [{
                'l2': 99,
                'subordinates': {},
            }],
        )

    async def test_edgeql_insert_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link.*std::Object.*"
                r"expecting 'test::Subordinate'"):
            await self.con.execute(r"""
                WITH MODULE test
                INSERT InsertTest {
                    l2 := 99,
                    subordinates := <Object>{}
                };
                """)

    async def test_edgeql_insert_abstract(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot insert into abstract object type 'std::Object'",
                _position=23):
            await self.con.execute("""\
                INSERT Object;
            """)

    async def test_edgeql_insert_alias(self):
        await self.con.execute('''
            CREATE ALIAS test::Foo := (SELECT test::InsertTest);
        ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot insert into expression alias 'test::Foo'",
                _position=23):
            await self.con.execute("""\
                INSERT test::Foo;
            """)

    async def test_edgeql_insert_selfref_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'self-referencing INSERTs are not allowed'):
            await self.con.execute(r"""
                WITH MODULE test
                INSERT SelfRef {
                    name := 'myself',
                    ref := SelfRef
                };
            """)

    async def test_edgeql_insert_selfref_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'self-referencing INSERTs are not allowed'):
            await self.con.execute(r"""
                WITH MODULE test
                INSERT SelfRef {
                    name := 'other'
                };

                WITH MODULE test
                INSERT SelfRef {
                    name := 'myself',
                    ref := (
                        SELECT SelfRef
                        FILTER .name = 'other'
                    )
                };
            """)

    async def test_edgeql_insert_selfref_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'self-referencing INSERTs are not allowed'):
            await self.con.execute(r"""
                WITH MODULE test
                INSERT SelfRef {
                    name := 'other'
                };

                WITH MODULE test
                INSERT SelfRef {
                    name := 'myself',
                    ref := (
                        WITH X := SelfRef
                        SELECT X
                        FILTER .name = 'other'
                    )
                };
            """)

    async def test_edgeql_insert_selfref_04(self):
        await self.con.execute(r"""
            WITH MODULE test
            INSERT SelfRef {
                name := 'ok other'
            };

            WITH MODULE test
            INSERT SelfRef {
                name := 'ok myself',
                ref := (
                    SELECT DETACHED SelfRef
                    FILTER .name = 'ok other'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT SelfRef {
                    name,
                    ref: {
                        name
                    }
                } ORDER BY .name;
            """,
            [{
                'name': 'ok myself',
                'ref': [{'name': 'ok other'}],
            }, {
                'name': 'ok other',
                'ref': [],
            }],
        )

    async def test_edgeql_insert_cardinality_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'single'):
            await self.con.execute(r'''
                SET MODULE test;

                INSERT Subordinate { name := 'sub1_cardinality_01'};
                INSERT Subordinate { name := 'sub2_cardinality_01'};
                INSERT Note {
                    name := 'note_cardinality_01',
                    subject := (
                        SELECT Subordinate
                        FILTER .name LIKE '%cardinality_01'
                    )
                };
            ''')

    async def test_edgeql_insert_derived_01(self):
        await self.con.execute(r"""
            INSERT test::DerivedTest {
                name := 'insert derived 01',
                l2 := 0,
            };

            INSERT test::DerivedTest {
                name := 'insert derived 01',
                l3 := "Test\"1\"",
                l2 := 1
            };

            INSERT test::DerivedTest {
                name := 'insert derived 01',
                l3 := 'Test\'2\'',
                l2 := 2
            };

            INSERT test::DerivedTest {
                name := 'insert derived 01',
                l3 := '\"Test\'3\'\"',
                l2 := 3
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    test::DerivedTest {
                        l2, l3
                    }
                FILTER
                    test::DerivedTest.name = 'insert derived 01'
                ORDER BY
                    test::DerivedTest.l2;
            """,
            [
                {
                    'l2': 0,
                    'l3': 'test',
                },
                {
                    'l2': 1,
                    'l3': 'Test"1"',
                },
                {
                    'l2': 2,
                    'l3': "Test'2'",
                },
                {
                    'l2': 3,
                    'l3': '''"Test'3'"''',
                }
            ]
        )

    async def test_edgeql_insert_derived_02(self):
        await self.con.execute(r"""
            WITH MODULE test
            INSERT DerivedTest {
                name := 'insert derived 02',
                l2 := 0,
                sub :=  (
                    INSERT Subordinate {
                        name := 'nested derived sub 02'
                    }
                )
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    DerivedTest {
                        name,
                        sub: {
                            name,
                            @note
                        }
                    }
                FILTER
                    .name = 'insert derived 02'
                ORDER BY
                    .l2;
            """,
            [
                {
                    'name': 'insert derived 02',
                    'sub': {
                        'name': 'nested derived sub 02',
                        '@note': None,
                    },
                },
            ]
        )

    async def test_edgeql_insert_collection_01(self):
        await self.con.execute(r"""
            INSERT test::CollectionTest {
                some_tuple := ('collection_01', 99),
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    CollectionTest {
                        some_tuple
                    }
                FILTER
                    .some_tuple.0 = 'collection_01';
            """,
            [
                {
                    'some_tuple': ['collection_01', 99],
                },
            ]
        )

    async def test_edgeql_insert_collection_02(self):
        await self.con.execute(r"""
            INSERT test::CollectionTest {
                str_array := ['collection_02', '99'],
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    CollectionTest {
                        str_array
                    }
                FILTER
                    .str_array[0] = 'collection_02';
            """,
            [
                {
                    'str_array': ['collection_02', '99'],
                },
            ]
        )

    async def test_edgeql_insert_collection_03(self):
        await self.con.execute(r"""
            INSERT test::CollectionTest {
                float_array := [3, 1234.5],
            };
        """)

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT
                    CollectionTest {
                        float_array
                    }
                FILTER
                    .float_array[0] = 3;
            """,
            [
                {
                    'float_array': [3, 1234.5],
                },
            ]
        )

    async def test_edgeql_insert_in_conditional_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'INSERT statements cannot be used'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT
                    (SELECT Subordinate FILTER .name = 'foo')
                    ??
                    (INSERT Subordinate { name := 'no way' });
            ''')

    async def test_edgeql_insert_in_conditional_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'INSERT statements cannot be used'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT
                    (SELECT Subordinate FILTER .name = 'foo')
                    IF EXISTS Subordinate
                    ELSE (
                        (SELECT Subordinate)
                        UNION
                        (INSERT Subordinate { name := 'no way' })
                    );
            ''')

    async def test_edgeql_insert_correlated_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Subordinate' here"):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT (
                    Subordinate,
                    (INSERT InsertTest {
                        name := 'insert bad',
                        l2 := 0,
                        subordinates := Subordinate
                    })
                );
            ''')

    async def test_edgeql_insert_correlated_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Subordinate' here"):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT (
                    (INSERT InsertTest {
                        name := 'insert bad',
                        l2 := 0,
                        subordinates := Subordinate
                    }),
                    Subordinate,
                );
            ''')
