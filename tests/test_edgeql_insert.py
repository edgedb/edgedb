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
import unittest  # NOQA
import uuid

import edgedb

from edb.server import _testbase as tb


class TestInsert(tb.QueryTestCase):
    '''The scope of the tests is testing various modes of Object creation.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'insert.eschema')

    async def test_edgeql_insert_fail_1(self):
        err = 'missing value for required property ' + \
              'test::InsertTest.l2'
        with self.assertRaisesRegex(edgedb.MissingRequiredError, err):
            await self.query('''
                INSERT test::InsertTest;
            ''')

    async def test_edgeql_insert_simple_01(self):
        result = await self.query(r"""
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
                'l2': 0,
                'l3': 'test',
            }, {
                'l2': 1,
                'l3': 'Test"1"',
            }, {
                'l2': 2,
                'l3': "Test'2'",
            }, {
                'l2': 3,
                'l3': '''"Test'3'"''',
            }]
        ])

    async def test_edgeql_insert_simple_02(self):
        res = await self.query('''
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

    async def test_edgeql_insert_simple_03(self):
        res = await self.query('''
            INSERT test::DefaultTest1 { num := 100 };

            WITH MODULE test
            INSERT DefaultTest2;

            INSERT test::DefaultTest1 { num := 101 };

            INSERT test::DefaultTest2;

            INSERT test::DefaultTest1 { num := 102 };

            INSERT test::DefaultTest2;

            WITH MODULE test
            SELECT DefaultTest2 { num }
            ORDER BY DefaultTest2.num;
        ''')

        self.assert_data_shape(
            res[-1],
            [{'num': 101}, {'num': 102}, {'num': 103}],
        )

    async def test_edgeql_insert_nested_01(self):
        res = await self.query('''
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
        res = await self.query('''
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
        res = await self.query('''
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
                'subordinates': [{
                    'name': 'nested sub 3.1'
                }]
            }]
        )

    async def test_edgeql_insert_nested_04(self):
        res = await self.query('''
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
                'subordinates': [{
                    'name': 'nested sub 4.1',
                    '@comment': 'comment 4.1'
                }]
            }]
        )

    async def test_edgeql_insert_nested_05(self):
        res = await self.query('''
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

    async def test_edgeql_insert_returning_01(self):
        res = await self.query('''
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

    async def test_edgeql_insert_returning_02(self):
        res = await self.query('''
            WITH MODULE test
            SELECT (INSERT DefaultTest1 {
                foo := 'ret1',
                num := 1,
            });

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
                [{
                    'id': uuid.UUID,
                }],
                [{
                    'foo': 'ret2',
                }],
                [3],
            ]
        )

    async def test_edgeql_insert_returning_03(self):
        res = await self.query('''
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

    async def test_edgeql_insert_returning_04(self):
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

    async def test_edgeql_insert_for_01(self):
        res = await self.query(r'''
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

            WITH MODULE test
            SELECT InsertTest{name, l2, l3}
            FILTER .name = 'insert for 1'
            ORDER BY .l2 THEN .l3;
        ''')

        self.assert_data_shape(
            res[-1], [
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
        res = await self.query(r'''
            # create 10 DefaultTest3 objects, each object is defined
            # as having a randomly generated value for 'foo'
            WITH MODULE test
            FOR x IN {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
            UNION (INSERT DefaultTest3);

            # statistically, randomly generated value for 'foo' should not be
            # identical for all 10 records
            WITH
                MODULE test,
                DT3 := DefaultTest3
            SELECT count(
                DefaultTest3 FILTER DefaultTest3.foo != DT3.foo) > 0;
        ''')

        self.assert_data_shape(
            res[-1], [True]
        )

    async def test_edgeql_insert_for_03(self):
        res = await self.query(r'''
            # Create 5 DefaultTest4 objects. The default value for
            # 'bar' is technically evaluated for each object, but
            # because it is deterministic it will be same for all 5
            # new objects.
            WITH MODULE test
            FOR x IN {1, 2, 3, 4, 5}
            UNION (INSERT DefaultTest4);

            WITH MODULE test
            SELECT DefaultTest4.bar
            ORDER BY DefaultTest4.bar;
        ''')

        self.assert_data_shape(
            res[-1], [0, 0, 0, 0, 0]
        )

    async def test_edgeql_insert_default_01(self):
        res = await self.query(r'''
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

            # statistically, randomly generated value for 'foo' should not be
            # identical for all 10 records
            WITH
                MODULE test,
                DT3 := DefaultTest3
            SELECT count(
                DefaultTest3 FILTER DefaultTest3.foo != DT3.foo) > 0;
        ''')

        self.assert_data_shape(
            res[-1], [True]
        )

    async def test_edgeql_insert_default_02(self):
        res = await self.query(r'''
            # by default the 'bar' value is simply going to be "indexing" the
            # created objects
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;

            WITH MODULE test
            SELECT DefaultTest4 { bar }
            ORDER BY DefaultTest4.bar;
        ''')

        self.assert_data_shape(
            res[-1], [{
                'bar': 0,
            }, {
                'bar': 1,
            }, {
                'bar': 2,
            }, {
                'bar': 3,
            }, {
                'bar': 4,
            }]
        )

    async def test_edgeql_insert_default_03(self):
        res = await self.query(r'''
            # by default the 'bar' value is simply going to be "indexing" the
            # created objects
            INSERT test::DefaultTest4 { bar:= 10 };
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;

            WITH MODULE test
            SELECT DefaultTest4 { bar }
            ORDER BY DefaultTest4.bar;
        ''')

        self.assert_data_shape(
            res[-1], [{
                'bar': 1,
            }, {
                'bar': 2,
            }, {
                'bar': 10,
            }]
        )

    async def test_edgeql_insert_default_04(self):
        res = await self.query(r'''
            # by default the 'bar' value is simply going to be "indexing" the
            # created objects
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4 { bar:= 0 };
            INSERT test::DefaultTest4;
            INSERT test::DefaultTest4;

            WITH MODULE test
            SELECT DefaultTest4 { bar }
            ORDER BY DefaultTest4.bar;
        ''')

        self.assert_data_shape(
            res[-1], [{
                'bar': 0,
            }, {
                'bar': 0,
            }, {
                'bar': 1,
            }, {
                'bar': 3,
            }, {
                'bar': 4,
            }]
        )

    @unittest.expectedFailure
    async def test_edgeql_insert_as_expr_01(self):
        res = await self.query(r'''
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
            UNION (INSERT Annotation {
                name := 'insert expr 1',
                note := 'largest ' + <str>x.l2,
                subject := x
            });

            WITH MODULE test
            SELECT
                InsertTest {
                    name,
                    l2,
                    l3,
                    <subject: {
                        name,
                        note,
                    }
                }
            FILTER .name = 'insert expr 1'
            ORDER BY .l2;
        ''')

        self.assert_data_shape(
            res[-1], [
                # inserted based on static data
                {
                    'name': 'insert expr 1',
                    'l2': 2,
                    'l3': 'test',
                    'subject': None,
                },
                {
                    'name': 'insert expr 1',
                    'l2': 3,
                    'l3': 'test',
                    'subject': None,
                },
                {
                    'name': 'insert expr 1',
                    'l2': 5,
                    'l3': 'test',
                    'subject': None,
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

    @unittest.expectedFailure
    async def test_edgeql_insert_polymorphic_01(self):
        res = await self.query(r'''
            WITH MODULE test
            INSERT Directive {
                args: {
                    val := "something"
                },
            };

            WITH MODULE test
            SELECT Callable {
                args: {
                    val
                }
            };

            WITH MODULE test
            SELECT Field {
                args: {
                    val
                }
            };

            WITH MODULE test
            SELECT Directive {
                args: {
                    val
                }
            };

            WITH MODULE test
            SELECT InputValue {
                val
            };
        ''')

        self.assert_data_shape(
            res, [
                [1],
                [{
                    'args': {'val': 'something'},
                }],
                [],
                [{
                    'args': {'val': 'something'},
                }],
                [{
                    'val': 'something',
                }],
            ]
        )

    async def test_edgeql_insert_linkprops_with_for_01(self):
        await self.assert_query_result(r"""
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

            WITH MODULE test
            SELECT InsertTest {
                l2,
                subordinates: {
                    name,
                    @comment,
                } ORDER BY InsertTest.subordinates.name
            } FILTER .l2 = 99;
        """, [
            [{}, ...],
            {1},
            [{
                'l2': 99,
                'subordinates': [{
                    'name': 'linkproptest 1',
                    '@comment': 'a',
                }, {
                    'name': 'linkproptest 2',
                    '@comment': 'b',
                }, {
                    'name': 'linkproptest 3',
                    '@comment': 'c',
                }],
            }],
        ])

    async def test_edgeql_insert_empty_01(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            INSERT InsertTest {
                l1 := {},
                l2 := 99,
                # l3 has a default value
                l3 := {},
            };

            WITH MODULE test
            SELECT InsertTest {
                l1,
                l2,
                l3
            };
        """, [
            {1},
            [{
                'l1': None,
                'l2': 99,
                'l3': None,
            }],
        ])

    async def test_edgeql_insert_empty_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid target.*std::datetime.*expecting 'std::int64'"):
            await self.query(r"""
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
            await self.query(r"""
                WITH MODULE test
                INSERT InsertTest {
                    l2 := {},
                };
                """)

    async def test_edgeql_insert_empty_04(self):
        await self.assert_query_result(r"""
            WITH MODULE test
            INSERT InsertTest {
                l2 := 99,
                subordinates := {}
            };

            WITH MODULE test
            SELECT InsertTest {
                l2,
                subordinates
            };
        """, [
            {1},
            [{
                'l2': 99,
                'subordinates': {},
            }],
        ])

    async def test_edgeql_insert_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link.*std::Object.*"
                r"expecting 'test::Subordinate'"):
            await self.query(r"""
                WITH MODULE test
                INSERT InsertTest {
                    l2 := 99,
                    subordinates := <Object>{}
                };
                """)

    async def test_edgeql_insert_abstract(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot insert: std::Object is abstract",
                position=7):
            await self.query("""\
                INSERT Object;
            """)

    async def test_edgeql_insert_view(self):
        await self.query('''
            CREATE VIEW test::Foo := (SELECT test::InsertTest);
        ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot insert: test::Foo is a view",
                position=7):
            await self.query("""\
                INSERT test::Foo;
            """)
