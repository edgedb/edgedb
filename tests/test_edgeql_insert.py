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


import contextlib
import os.path
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestInsert(tb.QueryTestCase):
    '''The scope of the tests is testing various modes of Object creation.'''
    # NO_FACTOR = True
    WARN_FACTOR = True
    INTERNAL_TESTMODE = False

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'insert.esdl')

    def assertRaisesRegex(self, exc, r, **kwargs):
        if (
            (self.NO_FACTOR or self.WARN_FACTOR)
            and "cannot reference correlated set" in r
        ):
            r = ""
        return super().assertRaisesRegex(exc, r, **kwargs)

    async def test_edgeql_insert_fail_01(self):
        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            r"missing value for required property"
            r" 'l2' of object type 'default::InsertTest'",
        ):
            await self.con.execute('''
                INSERT InsertTest;
            ''')

    async def test_edgeql_insert_fail_02(self):
        with self.assertRaisesRegex(
            edgedb.MissingRequiredError,
            r"missing value for required property"
            r" 'l2' of object type 'default::InsertTest'",
        ):
            await self.con.execute('''
                INSERT InsertTest {
                    l2 := assert_single({})
                };
            ''')

    async def test_edgeql_insert_fail_03(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"modification of computed property"
            r" 'name' of object type 'default::Person2b' is prohibited",
        ):
            await self.con.execute('''
                INSERT Person2b {
                    first := "foo",
                    last := "bar",
                    name := "something else",
                };
            ''')

    async def test_edgeql_insert_fail_04(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"mutation queries must specify values with ':='",
        ):
            await self.con.execute('''
                INSERT Person { name };
            ''')

    async def test_edgeql_insert_fail_05(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "INSERT only works with object types, not arbitrary "
            "expressions"
        ):
            await self.con.execute('''
                INSERT Person.notes { name := "note1" };
            ''')

    async def test_edgeql_insert_fail_06(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"could not resolve partial path",
        ):
            await self.con.execute('''
                INSERT Person { name := .name };
            ''')

    async def test_edgeql_insert_fail_07(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"insert standard library type",
        ):
            await self.con.execute('''
                INSERT schema::Migration { script := 'foo' };
            ''')

    async def test_edgeql_insert_fail_08(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "INSERT only works with object types, not arbitrary "
            "expressions"
        ):
            await self.con.execute("""
                insert Note {name := 'bad note'} union DerivedNote;
            """)

    async def test_edgeql_insert_fail_09(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "INSERT only works with object types, not conditional "
            "expressions"
        ):
            await self.con.execute("""
                insert Note {
                    name := 'bad note'
                } if not exists DerivedNote else DerivedNote;
            """)

    async def test_edgeql_insert_simple_01(self):
        await self.con.execute(r"""
            INSERT InsertTest {
                name := 'insert simple 01',
                l2 := 0,
            };

            INSERT InsertTest {
                name := 'insert simple 01',
                l3 := "Test\"1\"",
                l2 := 1
            };

            INSERT InsertTest {
                name := 'insert simple 01',
                l3 := 'Test\'2\'',
                l2 := 2
            };

            INSERT InsertTest {
                name := 'insert simple 01',
                l3 := '\"Test\'3\'\"',
                l2 := 3
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    InsertTest {
                        l2, l3
                    }
                FILTER
                    InsertTest.name = 'insert simple 01'
                ORDER BY
                    InsertTest.l2;
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
            INSERT DefaultTest1 { foo := '02' };

            INSERT DefaultTest1 { foo := '02' };

            INSERT DefaultTest1 { foo := '02' };
        ''')

        await self.assert_query_result(
            r'''
                SELECT DefaultTest1 { num } FILTER DefaultTest1.foo = '02';
            ''',
            [{'num': 42}, {'num': 42}, {'num': 42}],
        )

    async def test_edgeql_insert_simple_03(self):
        await self.con.execute('''
            INSERT DefaultTest1 { num := 100 };

            INSERT DefaultTest2;

            INSERT DefaultTest1 { num := 101 };

            INSERT DefaultTest2;

            INSERT DefaultTest1 { num := 102 };

            INSERT DefaultTest2;
        ''')

        await self.assert_query_result(
            r'''
                SELECT DefaultTest2 { num }
                ORDER BY DefaultTest2.num;
            ''',
            [{'num': 101}, {'num': 102}, {'num': 103}],
        )

    async def test_edgeql_insert_unused_01(self):
        await self.con.execute(r"""
            with _ := (
                INSERT InsertTest {
                    name := 'insert simple 01',
                    l2 := 0,
                }
            ), select 1;
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    InsertTest {
                        l2
                    }
                FILTER
                    InsertTest.name = 'insert simple 01'
            """,
            [
                {
                    'l2': 0,
                },
            ]
        )

        await self.con.execute(r"""
            with _ := (
                INSERT InsertTest {
                    name := 'insert simple 01',
                    l2 := (select 1 filter true),
                }
            ),
            INSERT InsertTest {
                name := 'insert simple 01',
                l2 := 2,
            }
        """)
        await self.assert_query_result(
            r"""
                SELECT
                    InsertTest {
                        l2
                    }
                FILTER
                    InsertTest.name = 'insert simple 01'
                ORDER BY .l2
            """,
            [
                {'l2': 0},
                {'l2': 1},
                {'l2': 2},
            ]
        )

    async def test_edgeql_insert_nested_01(self):
        await self.con.execute('''
            INSERT Subordinate {
                name := 'subtest 1'
            };

            INSERT Subordinate {
                name := 'subtest 2'
            };

            INSERT InsertTest {
                name := 'insert nested',
                l2 := 0,
                subordinates := (
                    SELECT Subordinate
                    FILTER Subordinate.name LIKE 'subtest%'
                )
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT InsertTest {
                    subordinates: {
                        name,
                        @comment,
                    } ORDER BY InsertTest.subordinates.name
                }
                FILTER
                    InsertTest.name = 'insert nested';
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
            INSERT Subordinate {
                name := 'subtest 3'
            };

            INSERT Subordinate {
                name := 'subtest 4'
            };

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
            INSERT Subordinate {
                name := 'only subordinate'
            };

            INSERT Subordinate {
                name := 'never subordinate'
            };

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

    async def test_edgeql_insert_nested_06(self):
        await self.con.execute('''
            INSERT Subordinate {
                name := 'linkprop test target 6'
            };

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
                "Unexpected 'Subordinate'"):
            await self.con.execute('''
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

    async def test_edgeql_insert_nested_10(self):
        # test a single link with a link property
        await self.con.execute(r'''

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

    async def test_edgeql_insert_nested_11(self):
        await self.con.execute('''
            INSERT Subordinate {
                name := 'linkprop test target 6'
            };
        ''')

        await self.assert_query_result(
            '''
                SELECT (
                    INSERT InsertTest {
                        name := 'insert nested 6',
                        l2 := 0,
                        subordinates := (
                            SELECT (SELECT Subordinate LIMIT 1) {
                                @comment := 'comment 6'
                            }
                        )
                    }
                ) {
                    subordinates: { name, @comment }
                }
            ''',
            [{
                'subordinates': [{
                    'name': 'linkprop test target 6',
                    '@comment': 'comment 6'
                }]
            }]
        )

    async def test_edgeql_insert_nested_12(self):
        # Ugh, set a default value on the link prop
        await self.con.execute('''
            ALTER TYPE InsertTest
              ALTER LINK subordinates
                ALTER PROPERTY comment
                  SET default := "!!!";
        ''')

        await self.con.execute('''
            INSERT Subordinate {
                name := 'linkprop test target 6'
            };
        ''')

        await self.assert_query_result(
            '''
                SELECT (
                    INSERT InsertTest {
                        name := 'insert nested 6',
                        l2 := 0,
                        subordinates := (
                            SELECT Subordinate LIMIT 1
                        )
                    }
                ) {
                    subordinates: { name, @comment }
                }
            ''',
            [{
                'subordinates': [{
                    'name': 'linkprop test target 6',
                    '@comment': '!!!'
                }]
            }]
        )

    async def test_edgeql_insert_returning_01(self):
        await self.con.execute('''
            INSERT DefaultTest1 {
                foo := 'ret1',
                num := 1,
            };
        ''')

        await self.assert_query_result(
            r'''
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
                SELECT (INSERT DefaultTest1 {
                    foo := 'ret3',
                    num := 3,
                }).num;
            ''',
            [3],
        )

        obj = await self.con._fetchall(
            '''
                INSERT DefaultTest1 {
                    foo := 'ret1',
                    num := 1,
                };
            ''',
            __typeids__=True,
            __typenames__=True,
        )

        self.assertTrue(hasattr(obj[0], 'id'))
        self.assertTrue(hasattr(obj[0], '__tid__'))
        self.assertEqual(obj[0].__tname__, 'default::DefaultTest1')

    async def test_edgeql_insert_returning_03(self):
        await self.con.execute('''
            INSERT Subordinate {
                name := 'sub returning 3'
            };
        ''')

        await self.assert_query_result(
            r'''
                WITH
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
                    I := (INSERT InsertTest {
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
                    I := (INSERT InsertTest {
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
            INSERT Subordinate {
                name := 'DefaultTest5/Sub'
            };
        ''')

        await self.assert_query_result(
            r'''
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
            INSERT Subordinate {
                name := 'DefaultTest5/Sub'
            };
        ''')

        await self.assert_query_result(
            r'''
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
            INSERT Subordinate {
                name := 'DefaultTest5/Sub'
            };
        ''')

        await self.assert_query_result(
            r'''
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

    async def test_edgeql_insert_returning_09(self):
        # make sure a WITH bound insert makes it into the returned data
        await self.assert_query_result(
            r'''
                WITH N := (INSERT Note {name := "!" }),
                SELECT ((
                    INSERT Person {
                        name := "Phil Emarg",
                        notes := N,
                    }
                )) { name, notes: {name} };

            ''',
            [{
                'name': 'Phil Emarg',
                'notes': [{'name': '!'}],
            }],
        )

        # make sure it works when *doubly* nested!
        await self.assert_query_result(
            r'''
                WITH S := (INSERT Subordinate { name := "sub" }),
                     N := (INSERT Note {name := "!", subject := S }),
                SELECT ((
                    INSERT Person {
                        name := "Madeline Hatch",
                        notes := N,
                    }
                )) { name, notes: {name, subject[IS Subordinate]: {name}} };

            ''',
            [{
                'name': 'Madeline Hatch',
                'notes': [{'name': '!', 'subject': {'name': "sub"}}],
            }],
        )

        # ... *doubly* nested, but the inner insert is a multi link
        await self.assert_query_result(
            r'''
            WITH N := (INSERT Note {name := "!" }),
                 P := (INSERT Person {
                    name := "Emmanuel Villip",
                    notes := N,
                 }),
            SELECT ((
                INSERT PersonWrapper { person := P }
            )) { person: { name, notes: {name} } };
            ''',
            [{
                'person': {
                    'name': 'Emmanuel Villip',
                    'notes': [{'name': '!'}],
                },
            }],
        )

    async def test_edgeql_insert_returning_10(self):
        # test that subtypes get returned by a nested insert
        await self.assert_query_result(
            r'''
                SELECT
                (INSERT Note {
                     name := "test",
                     subject := (INSERT Subordinate { name := "sub" })})
                { name, subject };
            ''',
            [{
                'name': 'test',
                'subject': {'id': str},
            }],
        )

    async def test_edgeql_insert_returning_11(self):
        await self.con.execute(r'''
            INSERT Note { name := "note", note := "a" };
        ''')

        # test that subtypes get returned by a nested update
        await self.assert_query_result(
            r'''
                SELECT
                (INSERT Person {
                     name := "test",
                     notes := (
                         UPDATE Note FILTER .name = "note"
                         SET { note := "b" }
                     )
                })
                { name, notes: {note} };
            ''',
            [{
                'name': 'test',
                'notes': [{'note': "b"}],
            }],
        )

    async def test_edgeql_insert_returning_12(self):
        await self.con.execute(r'''
            INSERT DerivedNote { name := "note", note := "a" };
        ''')

        # test that subtypes get returned by a nested update
        await self.assert_query_result(
            r'''
                SELECT
                (INSERT Person {
                     name := "test",
                     notes := (
                         UPDATE DerivedNote FILTER .name = "note"
                         SET { note := "b" }
                     )
                })
                { name, notes: {note} };
            ''',
            [{
                'name': 'test',
                'notes': [{'note': "b"}],
            }],
        )

    async def test_edgeql_insert_returning_13(self):
        await self.con.execute(r'''
            INSERT DerivedNote { name := "dnote", note := "a" };
            INSERT DerivedNote { name := "anote", note := "some note" };
        ''')

        # test that subtypes get returned by a nested update
        await self.assert_query_result(
            r'''
            SELECT
            (INSERT Person {
                name := "test",
                notes := assert_distinct({
                    (SELECT Note FILTER .name = "anote"),
                    (INSERT DerivedNote { name := "new note", note := "hi" }),
                    (UPDATE Note FILTER .name = "dnote" SET { note := "b" }),
                })
            })
            { name, notes: {name, note} ORDER BY .name };
            ''',
            [
                {
                    "name": "test",
                    "notes": [
                        {"name": "anote", "note": "some note"},
                        {"name": "dnote", "note": "b"},
                        {"name": "new note", "note": "hi"}
                    ]
                }
            ]
        )

    async def test_edgeql_insert_returning_14(self):
        await self.con.execute(r'''
            INSERT DerivedNote { name := "dnote", note := "a" };
            INSERT DerivedNote { name := "anote", note := "some note" };
        ''')

        # test that subtypes get returned by a nested update
        # same as 13, but test that it happens even after doing a type filter!
        await self.assert_query_result(
            r'''
            SELECT
            (INSERT Person {
                name := "test",
                notes := assert_distinct({
                    (SELECT Note FILTER .name = "anote"),
                    (INSERT DerivedNote { name := "new note", note := "hi" }),
                    (UPDATE Note FILTER .name = "dnote" SET { note := "b" }),
                })
            })
            {
                name,
                dnotes := (SELECT .notes[IS DerivedNote] {name, note}
                           ORDER BY .name)
            }
            ''',
            [
                {
                    "name": "test",
                    "dnotes": [
                        {"name": "anote", "note": "some note"},
                        {"name": "dnote", "note": "b"},
                        {"name": "new note", "note": "hi"}
                    ]
                }
            ]
        )

    async def test_edgeql_insert_returning_15(self):
        await self.con.execute('''
            alter type Person {
                create access policy ok allow all using (true);
            };
        ''')

        # Nested, with a required link that forces an assert_exists
        # to get injected.
        await self.assert_query_result(
            r'''
            WITH P := (INSERT Person {name := "Emmanuel Villip"}),
            SELECT ((
                INSERT PersonWrapper { person := P }
            )) { person: { name } };
            ''',
            [{"person": {"name": "Emmanuel Villip"}}],
        )

    async def test_edgeql_insert_conflict_policy_01(self):
        # Make sure that having an access policy doesn't break using an
        # object constraint in UNLESS CONFLICT.
        await self.con.execute('''
            create type Tgt {
                create access policy test allow all using (true);
            };
            create type Src {
                create link tgt -> Tgt;
                create constraint exclusive on (.tgt);
            };
        ''')

        await self.con.execute('''
            INSERT Src {
                tgt := (SELECT Tgt LIMIT 1)
            }
            UNLESS CONFLICT ON (.tgt)
        ''')

    async def test_edgeql_insert_conflict_policy_02(self):
        # An UNLESS CONFLICT with an invisible conflicting object
        # raises an exception. Notionally this is since the SELECT
        # we do can't find the object, and so we can't fetch it for
        # an ELSE clause.
        await self.con.execute('''
            alter type Person {
                create access policy yes allow all using (true);
                create access policy no deny select using (true);
            };
        ''')

        Q = '''
            insert Person { name := "test" }
            unless conflict on (.name) else (Person);
        '''

        await self.con.execute(Q)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(Q)

        Q = '''
            insert Person {
                name := "test2", note := (insert Note { name := "" }) }
            unless conflict on (.name) else (Person);
        '''

        await self.con.execute(Q)

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(Q)

    async def test_edgeql_insert_policy_cast(self):
        # Test for #6305, where a cast in a global used in an access policy
        # was causing a stray volatility ref to show up in the wrong CTE
        await self.con.execute('''
            create global sub_id -> uuid;
            create global sub := <Subordinate>(global sub_id);
            alter type Note {
                create access policy asdf allow all using (
                    (.subject in global sub) ?? false
                )
            };
        ''')

        sub = await self.con.query_single('''
            insert Subordinate { name := "asdf" };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.AccessPolicyError,
                "violation on insert of default::Note"):
            await self.con.execute('''
                insert Person { notes := (insert Note { name := "" }) };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.AccessPolicyError,
                "violation on insert of default::Note"):
            await self.con.execute('''
                insert Person {
                    notes := (insert Note {
                        name := "",
                        subject := assert_single(
                          (select Subordinate filter .name = 'asdf'))
                    })
                };
            ''')

        await self.con.execute('''
            set global sub_id := <uuid>$0
        ''', sub.id)

        await self.con.execute('''
            insert Person {
                notes := (insert Note {
                    name := "",
                    subject := assert_single(
                      (select Subordinate filter .name = 'asdf'))
                })
            };
        ''')

    async def test_edgeql_insert_for_01(self):
        await self.con.execute(r'''
            FOR x IN {3, 5, 7, 2}
            INSERT InsertTest {
                name := 'insert for 1',
                l2 := x,
            };

            FOR Q IN (SELECT InsertTest{foo := 'foo' ++ <str> InsertTest.l2}
                      FILTER .name = 'insert for 1')
            INSERT InsertTest {
                name := 'insert for 1',
                l2 := 35 % Q.l2,
                l3 := Q.foo,
            };
        ''')

        await self.assert_query_result(
            r'''
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

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_insert_for_02(self):
        await self.con.execute(r'''
            # create 10 DefaultTest3 objects, each object is defined
            # as having a randomly generated value for 'foo'
            FOR x IN {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
            UNION (INSERT DefaultTest3);
        ''')

        await self.assert_query_result(
            r'''
                # statistically, randomly generated value for 'foo'
                # should not be identical for all 10 records
                WITH
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
            FOR x IN {1, 2, 3, 4, 5}
            UNION (INSERT DefaultTest4);
        ''')

        await self.assert_query_result(
            r'''
                SELECT DefaultTest4.bar
                ORDER BY DefaultTest4.bar;
            ''',
            [0, 0, 0, 0, 0]
        )

    async def test_edgeql_insert_for_04(self):
        await self.con.execute(r'''
            INSERT InsertTest {
                name := 'nested-insert-for',
                l2 := 999,
                subordinates := (
                    FOR x IN {('sub1', 'first'), ('sub2', 'second')}
                    INSERT Subordinate {
                        name := x.0,
                        @comment := x.1,
                    }
                )
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT InsertTest {
                    subordinates: {
                        name,
                        @comment,
                    } ORDER BY .name
                }
                FILTER .name = 'nested-insert-for'
            ''',
            [{
                'subordinates': [{
                    'name': 'sub1',
                    '@comment': 'first',
                }, {
                    'name': 'sub2',
                    '@comment': 'second',
                }]
            }]
        )

    async def test_edgeql_insert_for_06(self):
        res = await self.con.query(r'''
            FOR a IN {"a", "b"}
            FOR b IN {"c", "d"}
            INSERT Note {name := b};
        ''')
        self.assertEqual(len(res), 4)

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["c", "c", "d", "d"]
        )

    async def test_edgeql_insert_for_07(self):
        res = await self.con.query(r'''
            FOR a IN {"a", "b"}
            FOR b IN {a++"c", a++"d"}
            INSERT Note {name := b};
        ''')
        self.assertEqual(len(res), 4)

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["ac", "ad", "bc", "bd"]
        )

    async def test_edgeql_insert_for_08(self):
        res = await self.con.query(r'''
            FOR a IN {"a", "b"}
            FOR b IN {"a", "b"}
            FOR c IN {a++b++"a", a++b++"b"}
            INSERT Note {name := c};
        ''')
        self.assertEqual(len(res), 8)

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["aaa", "aab", "aba", "abb", "baa", "bab", "bba", "bbb"]
        )

    async def test_edgeql_insert_for_09(self):
        res = await self.con.query(r'''
            FOR a in {"a", "b"} UNION (
                FOR b in {"a", "b"} UNION (
                    FOR c in {"a", "b"} UNION (
                        INSERT Note {name := a++b++c})));
        ''')
        self.assertEqual(len(res), 8)

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["aaa", "aab", "aba", "abb", "baa", "bab", "bba", "bbb"]
        )

    async def test_edgeql_insert_for_10(self):
        # Nested FOR where the inner-most one isn't referenced
        res = await self.con.query(r'''
            FOR a in {"a", "b"} UNION (
                FOR b in {"a", "b"} UNION (
                    FOR c in {"a", "b"} UNION (
                        INSERT Note {name := a++b})));
        ''')
        self.assertEqual(len(res), 8)

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["aa", "aa", "ab", "ab", "ba", "ba", "bb", "bb"]
        )

    async def test_edgeql_insert_for_11(self):
        # Nested FOR where the inner-most two aren't referenced
        res = await self.con.query(r'''
            FOR a in {"a", "b"} UNION (
                FOR b in {"a", "b"} UNION (
                    FOR c in {"a", "b"} UNION (
                        INSERT Note {name := a})));
        ''')
        self.assertEqual(len(res), 8)

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["a", "a", "a", "a", "b", "b", "b", "b"]
        )

    async def test_edgeql_insert_for_12(self):
        # FOR that has a correlated SELECT and INSERT
        await self.assert_query_result(
            r'''
                FOR a in {"foo", "bar"} UNION (
                    (a,(INSERT Note {name:=a}))
                )
            ''',
            [["bar", {}], ["foo", {}]],
            sort=True,
        )

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["bar", "foo"]
        )

    async def test_edgeql_insert_for_13(self):
        await self.assert_query_result(
            r'''
                FOR a in {"foo", "bar"} UNION (
                    SELECT (INSERT Note {name:=a}) {name}
                )
            ''',
            [{"name": "bar"}, {"name": "foo"}],
            sort=lambda x: x['name']
        )

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["bar", "foo"]
        )

    async def test_edgeql_insert_for_14(self):
        # Nested FOR that has a correlated SELECT and INSERT
        await self.assert_query_result(
            r'''
                FOR a in {"a", "b"} UNION (
                    FOR b in {"c", "d"} UNION (
                        (a, b, (INSERT Note {name:=a++b}).name)
                    )
                )
            ''',
            [
                ["a", "c", "ac"],
                ["a", "d", "ad"],
                ["b", "c", "bc"],
                ["b", "d", "bd"],
            ],
            sort=True
        )

        await self.assert_query_result(
            r'''
                SELECT Note.name
                ORDER BY Note.name;
            ''',
            ["ac", "ad", "bc", "bd"]
        )

    async def test_edgeql_insert_for_15(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob ++ "!",
                               notes := (INSERT Note {name := noob})});
        """)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name DESC",
            [{"name": "Phil Emarg!",
              "notes": [{"name": "Phil Emarg"}]},
             {"name": "Madeline Hatch!",
              "notes": [{"name": "Madeline Hatch"}]}],
        )

    async def test_edgeql_insert_for_16(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob,
                               notes := (
                    FOR suffix in {"?", "!"} UNION (
                        INSERT Note {name := noob ++ suffix}))});
        """)

        await self.assert_query_result(
            """SELECT Person {
               name, notes: {name} ORDER BY .name DESC} ORDER BY .name DESC""",
            [
                {"name": "Phil Emarg",
                 "notes": [{"name": "Phil Emarg?"},
                           {"name": "Phil Emarg!"}]},
                {"name": "Madeline Hatch",
                 "notes": [{"name": "Madeline Hatch?"},
                           {"name": "Madeline Hatch!"}]},
            ],
        )

    async def test_edgeql_insert_for_17(self):
        # same as above, but with a SELECT wrapping the inner FOR,
        # which exposed some issues
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob,
                               notes := (SELECT (
                    FOR suffix in {"?", "!"} UNION (
                        INSERT Note {name := noob ++ suffix})))});
        """)

        await self.assert_query_result(
            """SELECT Person {
               name, notes: {name} ORDER BY .name DESC} ORDER BY .name DESC""",
            [
                {"name": "Phil Emarg",
                 "notes": [{"name": "Phil Emarg?"},
                           {"name": "Phil Emarg!"}]},
                {"name": "Madeline Hatch",
                 "notes": [{"name": "Madeline Hatch?"},
                           {"name": "Madeline Hatch!"}]},
            ],
        )

    async def test_edgeql_insert_for_18(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob ++ "!",
                               note := (INSERT Note {name := noob})});
        """)

        await self.assert_query_result(
            "SELECT Person { name, note: {name} } ORDER BY .name DESC",
            [{"name": "Phil Emarg!",
              "note": {"name": "Phil Emarg"}},
             {"name": "Madeline Hatch!",
              "note": {"name": "Madeline Hatch"}}],
        )

    async def test_edgeql_insert_for_19(self):
        await self.con.execute(r"""
            FOR t IN array_unpack(<array<InsertTest>>[])
            UNION (
                INSERT InsertTest {
                    name := t.name, l2 := t.l2,
                }
            );
        """)

        await self.assert_query_result(
            "SELECT InsertTest",
            [],
        )

    async def test_edgeql_insert_for_20(self):
        await self.con.execute(r"""
            INSERT InsertTest { name := "a", l2 := 1 };
        """)

        await self.con.execute(r"""
            FOR t IN array_unpack([InsertTest])
            UNION (
                INSERT InsertTest {
                    name := t.name ++ "!", l2 := t.l2 + 1,
                }
            );
        """)

        await self.assert_query_result(
            "SELECT InsertTest { name, l2 } ORDER BY .l2",
            [
                {'name': "a", 'l2': 1},
                {'name': "a!", 'l2': 2},
            ],
        )

    async def test_edgeql_insert_for_21(self):
        await self.con.execute(r"""
            FOR t IN array_unpack(<array<tuple<InsertTest>>>[])
            UNION (
                INSERT InsertTest {
                    name := t.0.name, l2 := t.0.l2,
                }
            );
        """)

        await self.assert_query_result(
            "SELECT InsertTest",
            [],
        )

    async def test_edgeql_insert_for_22(self):
        await self.con.execute(r"""
            INSERT InsertTest { name := "a", l2 := 1 };
        """)

        await self.con.execute(r"""
            FOR t IN array_unpack([(InsertTest,)])
            UNION (
                INSERT InsertTest {
                    name := t.0.name ++ "!", l2 := t.0.l2 + 1,
                }
            );
        """)

        await self.assert_query_result(
            "SELECT InsertTest { name, l2 } ORDER BY .l2",
            [
                {'name': "a", 'l2': 1},
                {'name': "a!", 'l2': 2},
            ],
        )

    async def test_edgeql_insert_for_23(self):
        await self.con.execute(r"""
            INSERT Subordinate { name := "a" }
        """)

        await self.assert_query_result(
            """
            for x in {Subordinate, Subordinate} union (
              (x { name }, (insert Note { name := '', subject := x }))
            );
            """,
            [
                [{'name': "a"}, {}],
                [{'name': "a"}, {}],
            ],
        )

        await self.assert_query_result(
            """
            for x in {Subordinate, Subordinate} union (
              (x { name }, (insert InsertTest { l2 := 0, sub := x }))
            );
            """,
            [
                [{'name': "a"}, {}],
                [{'name': "a"}, {}],
            ],
        )

    async def test_edgeql_insert_for_bad_01(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "cannot reference correlated set",
        ):
            await self.con.execute("""
                SELECT (Person,
                        (FOR x in Person UNION (
                             INSERT Note {name := x.name})));
            """)

    async def test_edgeql_insert_for_bad_02(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "cannot reference correlated set",
        ):
            await self.con.execute("""
                SELECT (Person,
                        (FOR x in Person UNION (
                             SELECT (INSERT Note {name := x.name}))));
            """)

    async def test_edgeql_insert_for_bad_03(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "cannot reference correlated set",
        ):
            await self.con.execute("""
                SELECT ((FOR x in Person UNION (
                             INSERT Note {name := x.name})),
                        Person);
            """)

    async def test_edgeql_insert_for_bad_04(self):
        with self.assertRaisesRegex(
            edgedb.errors.QueryError,
            "cannot reference correlated set",
        ):
            await self.con.execute("""
                SELECT (Person,
                        (FOR x in Person UNION (
                             SELECT (
                                 20,
                                 (FOR y in {"hello", "world"} UNION (
                                  INSERT Note {name := y ++ x.name}))))));
            """)

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_insert_default_01(self):
        await self.con.execute(r'''
            # create 10 DefaultTest3 objects, each object is defined
            # as having a randomly generated value for 'foo'
            INSERT DefaultTest3;
            INSERT DefaultTest3;
            INSERT DefaultTest3;
            INSERT DefaultTest3;
            INSERT DefaultTest3;

            INSERT DefaultTest3;
            INSERT DefaultTest3;
            INSERT DefaultTest3;
            INSERT DefaultTest3;
            INSERT DefaultTest3;
        ''')

        await self.assert_query_result(
            r'''
                # statistically, randomly generated value for 'foo'
                # should not be identical for all 10 records
                WITH
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
            INSERT DefaultTest4;
            INSERT DefaultTest4;
            INSERT DefaultTest4;
            INSERT DefaultTest4;
            INSERT DefaultTest4;
        ''')

        await self.assert_query_result(
            r'''
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
            INSERT DefaultTest4 { bar:= 10 };
            INSERT DefaultTest4;
            INSERT DefaultTest4;
        ''')

        await self.assert_query_result(
            r'''
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
            INSERT DefaultTest4;
            INSERT DefaultTest4;
            INSERT DefaultTest4 { bar:= 0 };
            INSERT DefaultTest4;
            INSERT DefaultTest4;
        ''')

        await self.assert_query_result(
            r'''
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
            INSERT DefaultTest8;
            INSERT DefaultTest8;
            INSERT DefaultTest8;
        ''')

        try:
            await self.assert_query_result(
                r'''
                    SELECT DefaultTest8.number;
                ''',
                {1, 2, 3},
            )
        except AssertionError:
            if self.is_repeat:
                await self.assert_query_result(
                    r'''
                        SELECT DefaultTest8.number;
                    ''',
                    {4, 5, 6},
                )
            else:
                raise

    async def test_edgeql_insert_default_06(self):
        res = await self.con.query(r'''
            INSERT DefaultTest1;
        ''')
        assert len(res) == 1
        obj = res[0]
        # The result should not include the default param
        assert not hasattr(obj, 'num')

    async def test_edgeql_insert_default_07(self):
        await self.con.query(
            r"""
            create type Foo {
                create property n -> int32;
                create property a -> str;
                create property b -> str;
                create property c -> str;
            };

            alter type Foo {
                alter property a { set default := 'a=' ++ .b };
                alter property b { set default := 'b=' ++ .c };
                alter property c { set default := 'c=' ++ .a };
            };
        """
        )
        await self.con.query("insert Foo { n := 0, a := 'given' };")
        await self.con.query("insert Foo { n := 1, b := 'given' };")
        await self.con.query("insert Foo { n := 2, c := 'given' };")
        await self.assert_query_result(
            "select Foo { a, b, c } order by .n",
            [
                {"a": "given", "b": "b=c=given", "c": "c=given"},
                {"a": "a=given", "b": "given", "c": "c=a=given"},
                {"a": "a=b=given", "b": "b=given", "c": "given"},
            ],
        )

    async def test_edgeql_insert_default_08(self):
        await self.con.query(
            r"""
            create type Bar {
                create property f -> float64;
                create property g -> float64 {
                    set default := .f
                };
            };
            """
        )
        await self.con.query("insert Bar { f := random() };")
        res = await self.con.query("select Bar { f, g }")
        assert res[0].f == res[0].g

    async def test_edgeql_insert_default_09(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"default expression cannot refer to multi properties",
        ):
            await self.migrate(
                r"""
                type Hello {
                    multi property b -> int32;
                    property a -> int32 {
                        default := count(.b);
                    };
                }
            """
            )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"default expression cannot refer to links",
        ):
            await self.migrate(
                r"""
                type World {
                    property w -> int32;
                }

                type Hello {
                    link world -> World;

                    property hello -> int32 {
                        default := .world.w;
                    };
                }
            """
            )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"default expression cannot refer to links",
        ):
            await self.migrate(
                r"""
                type World {
                    property w -> int32;
                }

                type Hello {
                    multi link world -> World;

                    property hello -> int32 {
                        default := count(.world);
                    };
                }
            """
            )

    @tb.needs_factoring_weakly  # XXX(factor): maybe it shouldn't?
    async def test_edgeql_insert_dunder_default_01(self):
        await self.con.execute(r'''
            INSERT DunderDefaultTest01 { a := 1, c := __default__ };
            INSERT DunderDefaultTest01 { a := 1, c := __default__ + 3 };
            INSERT DunderDefaultTest01 {
                a := 1,
                c := __default__ + __default__,
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT DunderDefaultTest01 { a, b, c };
            ''',
            [
                {'a': 1, 'b': 2, 'c': 1},
                {'a': 1, 'b': 2, 'c': 4},
                {'a': 1, 'b': 2, 'c': 2},
            ]
        )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='No default expression exists',
        ):
            await self.con.execute(r'''
                INSERT DunderDefaultTest01 { a := __default__ };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses __source__',
        ):
            await self.con.execute(r'''
                INSERT DunderDefaultTest01 { a := 1, b := __default__ };
            ''')

    async def test_edgeql_insert_dunder_default_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses DML',
        ):
            await self.con.execute(r'''
                INSERT DunderDefaultTest02_B {
                    default_with_insert := __default__
                };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses DML',
        ):
            await self.con.execute(r'''
                INSERT DunderDefaultTest02_B {
                    default_with_update := __default__
                };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses DML',
        ):
            await self.con.execute(r'''
                INSERT DunderDefaultTest02_B {
                    default_with_delete := __default__
                };
            ''')

        await self.con.execute(r'''
            INSERT DunderDefaultTest02_A { a := 1 };
            INSERT DunderDefaultTest02_A { a := 2 };
            INSERT DunderDefaultTest02_A { a := 3 };
            INSERT DunderDefaultTest02_A { a := 4 };
            INSERT DunderDefaultTest02_B {
                default_with_insert := (
                    select DunderDefaultTest02_A
                    filter DunderDefaultTest02_A.a = 1
                ),
                default_with_update := (
                    select DunderDefaultTest02_A
                    filter DunderDefaultTest02_A.a = 2
                ),
                default_with_delete := (
                    select DunderDefaultTest02_A
                    filter DunderDefaultTest02_A.a = 3
                ),
                default_with_select := __default__
            };
        ''')

        await self.assert_query_result(
            r'''
                SELECT DunderDefaultTest02_B {
                    a := .default_with_select.a
                };
            ''',
            [
                {'a': [4]},
            ]
        )

    async def test_edgeql_insert_as_expr_01(self):
        await self.con.execute(r'''
            # insert several objects, then annotate one of the inserted batch
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
            INSERT Directive {
                args := (INSERT InputValue {
                    val := "something"
                }),
            };
        ''')

        await self.assert_query_result(
            r'''
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
                SELECT InputValue {
                    val
                };
            ''',
            [{
                'val': 'something',
            }],
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_insert_linkprops_with_for_01(self):
        await self.con.execute(r"""
            FOR i IN {'1', '2', '3'} UNION (
                INSERT Subordinate {
                    name := 'linkproptest ' ++ i
                }
            );

            INSERT InsertTest {
                l2 := 99,
                subordinates := DISTINCT(
                    FOR x IN {('a', '1'), ('b', '2'), ('c', '3')} UNION (
                        SELECT Subordinate {@comment := x.0}
                        FILTER .name[-1] = x.1
                    )
                )
            };
        """)

        await self.assert_query_result(
            r"""
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
            INSERT InsertTest {
                l1 := {},
                l2 := 99,
                # l3 has a default value
                l3 := {},
            };
        """)

        await self.assert_query_result(
            r"""
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
                    INSERT InsertTest {
                        l2 := {},
                    };
                """
            )

    async def test_edgeql_insert_empty_04(self):
        await self.con.execute(r"""
            INSERT InsertTest {
                l2 := 99,
                subordinates := {}
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT InsertTest {
                    l2,
                    subordinates
                };
            """,
            [{
                'l2': 99,
                'subordinates': [],
            }],
        )

    async def test_edgeql_insert_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link.*std::Object.*"
                r"expecting 'default::Subordinate'"):
            await self.con.execute(r"""
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
            CREATE ALIAS Foo := (SELECT InsertTest);
        ''')

        with self.assertRaisesRegex(
                edgedb.QueryError,
                r"cannot insert into expression alias 'default::Foo'",
                _position=23):
            await self.con.execute("""\
                INSERT Foo;
            """)

    async def test_edgeql_insert_free_obj(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"free objects cannot be inserted",
            _position=23,
        ):
            await self.con.execute("""\
                INSERT std::FreeObject;
            """)

    async def test_edgeql_insert_selfref_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'self-referencing INSERTs are not allowed'):
            await self.con.execute(r"""
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
                INSERT SelfRef {
                    name := 'other'
                };

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
                INSERT SelfRef {
                    name := 'other'
                };

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
            INSERT SelfRef {
                name := 'ok other'
            };

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
            INSERT DerivedTest {
                name := 'insert derived 01',
                l2 := 0,
            };

            INSERT DerivedTest {
                name := 'insert derived 01',
                l3 := "Test\"1\"",
                l2 := 1
            };

            INSERT DerivedTest {
                name := 'insert derived 01',
                l3 := 'Test\'2\'',
                l2 := 2
            };

            INSERT DerivedTest {
                name := 'insert derived 01',
                l3 := '\"Test\'3\'\"',
                l2 := 3
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    DerivedTest {
                        l2, l3
                    }
                FILTER
                    DerivedTest.name = 'insert derived 01'
                ORDER BY
                    DerivedTest.l2;
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

    async def test_edgeql_insert_tuples_01(self):
        await self.assert_query_result(
            r"""
                with noobs := {
                  ((insert InsertTest { l2 := 1 }), "bar"),
                  ((insert InsertTest { l2 := 2 }), "eggs"),
                },
                select noobs;
            """,
            [
                ({}, "bar"),
                ({}, "eggs"),
            ]
        )

        await self.assert_query_result(
            r"""
                select {
                  ((insert InsertTest { l2 := 1 }), "bar"),
                  ((insert InsertTest { l2 := 2 }), "eggs"),
                }
            """,
            [
                ({}, "bar"),
                ({}, "eggs"),
            ]
        )

    async def test_edgeql_insert_tuples_02(self):
        await self.assert_query_result(
            r"""
                with noobs := {
                  ((insert InsertTest { l2 := 1 }), "bar"),
                  ((insert DerivedTest { l2 := 2 }), "eggs"),
                },
                select noobs;
            """,
            [
                ({}, "bar"),
                ({}, "eggs"),
            ]
        )

        await self.assert_query_result(
            r"""
                select {
                  ((insert InsertTest { l2 := 1 }), "bar"),
                  ((insert DerivedTest { l2 := 2 }), "eggs"),
                }
            """,
            [
                ({}, "bar"),
                ({}, "eggs"),
            ]
        )

    async def test_edgeql_insert_tuples_03(self):
        await self.assert_query_result(
            r"""
                with noobs := {
                  ((insert InsertTest { l2 := 1 }), "bar"),
                  ((insert Person { name := "x" }), "eggs"),
                },
                select noobs;
            """,
            [
                ({}, "bar"),
                ({}, "eggs"),
            ]
        )

        await self.assert_query_result(
            r"""
                select {
                  ((insert InsertTest { l2 := 1 }), "bar"),
                  ((insert Person { name := "y" }), "eggs"),
                }
            """,
            [
                ({}, "bar"),
                ({}, "eggs"),
            ]
        )

    async def test_edgeql_insert_tuples_04(self):
        await self.assert_query_result(
            r"""
            with noobs := {
              ((insert Subordinate { name := "foo" }), "bar"),
              ((insert Subordinate { name := "spam" }), "eggs"),
            },
            select (insert InsertTest {
                l2 := 1,
                subordinates := assert_distinct(
                    noobs.0 { @comment := noobs.1 }),
            }) { subordinates: {name, @comment} order by .name };
            """,
            [
                {
                    "subordinates": [
                        {"name": "foo", "@comment": "bar"},
                        {"name": "spam", "@comment": "eggs"}
                    ]
                }
            ],
        )

        await self.assert_query_result(
            r"""
            select InsertTest { subordinates: {name, @comment} };
            """,
            [
                {
                    "subordinates": [
                        {"name": "foo", "@comment": "bar"},
                        {"name": "spam", "@comment": "eggs"}
                    ]
                }
            ],
        )

    async def test_edgeql_insert_collection_01(self):
        await self.con.execute(r"""
            INSERT CollectionTest {
                some_tuple := ('collection_01', 99),
            };
        """)

        await self.assert_query_result(
            r"""
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
            INSERT CollectionTest {
                str_array := ['collection_02', '99'],
            };
        """)

        await self.assert_query_result(
            r"""
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
            INSERT CollectionTest {
                float_array := [3, 1234.5],
            };
        """)

        await self.assert_query_result(
            r"""
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

    async def test_edgeql_insert_collection_04(self):
        await self.con.execute(r"""
            INSERT CollectionTest {
                some_tuple := ('huh', -1),
                some_multi_tuple := ('foo', 0),
            };

            INSERT CollectionTest {
                some_tuple := ('foo', 0),
                some_multi_tuple := {('foo', 0), ('bar', 1)},
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT count(
                    CollectionTest FILTER ('bar', 1) IN .some_multi_tuple
                );
            """,
            [1],
        )

        await self.assert_query_result(
            r"""
                SELECT count(
                    CollectionTest FILTER .some_tuple IN .some_multi_tuple
                );
            """,
            [1],
        )

        await self.assert_query_result(
            r"""
                SELECT count(
                    CollectionTest FILTER ('foo', '0') IN
                    <tuple<str, str>>.some_multi_tuple
                );
            """,
            [2],
        )

    async def test_edgeql_insert_collection_05(self):
        # Make sure that empty arrays are accepted for inserts even when types
        # are not explicitly specified.
        await self.con.execute(r"""
            INSERT CollectionTest {
                str_array := [],
                float_array := [],
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    CollectionTest {
                        str_array,
                        float_array
                    }
                FILTER
                    len(.float_array) = 0;
            """,
            [
                {
                    'str_array': [],
                    'float_array': [],
                },
            ]
        )

    async def test_edgeql_insert_correlated_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Subordinate' here"):
            await self.con.execute(r'''
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
                SELECT (
                    (INSERT InsertTest {
                        name := 'insert bad',
                        l2 := 0,
                        subordinates := Subordinate
                    }),
                    Subordinate,
                );
            ''')

    async def test_edgeql_insert_correlated_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Person' here"):
            await self.con.execute(r'''
                SELECT (
                    Person,
                    (INSERT Person {name := 'insert bad'}),
                )
            ''')

    async def test_edgeql_insert_unless_conflict_01(self):
        query = r'''
            SELECT
             ((INSERT Person {name := "test"} UNLESS CONFLICT)
              ?? (SELECT Person FILTER .name = "test")) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "test"}],
        )

        await self.assert_query_result(
            query,
            [{"name": "test"}],
        )

        query2 = r'''
            SELECT
             ((INSERT Person {name := <str>$0} UNLESS CONFLICT ON .name)
              ?? (SELECT Person FILTER .name = <str>$0));
        '''

        res = await self.con.query(query2, "test2")
        res2 = await self.con.query(query2, "test2")
        self.assertEqual([x.id for x in res], [x.id for x in res2])

        res3 = await self.con.query(query2, "test3")
        self.assertNotEqual([x.id for x in res], [x.id for x in res3])

    async def test_edgeql_insert_unless_conflict_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "UNLESS CONFLICT argument must be a property"):
            await self.con.query(r'''
                INSERT Person {name := "hello"}
                UNLESS CONFLICT ON 20;
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "UNLESS CONFLICT argument must be a property of "
                "the type being inserted"):
            await self.con.query(r'''
                INSERT Person {name := "hello"}
                UNLESS CONFLICT ON Note.name;
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "UNLESS CONFLICT property must have a "
                "single exclusive constraint"):
            await self.con.query(r'''
                INSERT Note {name := "hello"}
                UNLESS CONFLICT ON .name;
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "object type 'std::Object' has no link or property 'name'"):
            await self.con.query(r'''
                SELECT (
                    INSERT Person {name := "hello"}
                    UNLESS CONFLICT ON .name
                    ELSE DefaultTest1
                ) {name};
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "possibly more than one element returned by an expression "
                "for a computed link 'foo' declared as 'single'"):
            await self.con.query(r'''
                WITH X := (
                        INSERT Person {name := "hello"}
                        UNLESS CONFLICT ON .name
                        ELSE (DETACHED Person)
                    )
                SELECT {
                    single foo := X
                };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "possibly more than one element returned by an expression "
                "for a computed link 'foo' declared as 'single'"):
            await self.con.query(r'''
                WITH X := (
                        INSERT Person {name := "hello"}
                        UNLESS CONFLICT ON .name
                        ELSE Note
                    )
                SELECT {
                    single foo := X
                };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "possibly an empty set returned by an expression for a "
                "computed link 'foo' declared as 'required'"):
            await self.con.query(r'''
                WITH X := (
                        INSERT Person {name := "hello"}
                        UNLESS CONFLICT ON .name
                    )
                SELECT {
                    required foo := X
                };
            ''')

    async def test_edgeql_insert_unless_conflict_03(self):
        query = r'''
            SELECT (
                INSERT Person {name := "test"} UNLESS CONFLICT) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "test"}],
        )

        await self.assert_query_result(
            query,
            [],
        )

    async def test_edgeql_insert_unless_conflict_04(self):
        query = r'''
            SELECT (
                INSERT Person {name := "test"} UNLESS CONFLICT
                ON .name ELSE (SELECT Person)
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "test"}],
        )

        await self.assert_query_result(
            query,
            [{"name": "test"}],
        )

        await self.assert_query_result(
            r'''SELECT Person {name}''',
            [{"name": "test"}],
        )

        query2 = r'''
            INSERT Person {name := <str>$0} UNLESS CONFLICT
            ON .name ELSE (SELECT Person)
        '''

        res = await self.con.query(query2, "test2")
        res2 = await self.con.query(query2, "test2")
        self.assertEqual([x.id for x in res], [x.id for x in res2])

        res3 = await self.con.query(query2, "test3")
        self.assertNotEqual([x.id for x in res], [x.id for x in res3])

    async def test_edgeql_insert_unless_conflict_05(self):
        await self.con.execute(r'''
            INSERT Person { name := "Phil Emarg" }
        ''')

        query = r'''
            SELECT (
                INSERT Person {name := "Emmanuel Villip"} UNLESS CONFLICT
                ON .name ELSE (UPDATE Person SET { tag := "redo" })
            ) {name, tag};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Emmanuel Villip", "tag": None}],
        )

        await self.assert_query_result(
            "SELECT Person {name, tag} ORDER BY .name",
            [{"name": "Emmanuel Villip", "tag": None},
             {"name": "Phil Emarg", "tag": None}],
        )

        await self.assert_query_result(
            query,
            [{"name": "Emmanuel Villip", "tag": "redo"}],
        )

        # Only the correct record should be updated
        await self.assert_query_result(
            "SELECT Person {name, tag} ORDER BY .name",
            [{"name": "Emmanuel Villip", "tag": "redo"},
             {"name": "Phil Emarg", "tag": None}],
            sort=lambda x: x['name']
        )

    async def test_edgeql_insert_unless_conflict_06(self):
        await self.con.execute(r'''
            INSERT Person { name := "Phil Emarg" };
            INSERT Person { name := "Madeline Hatch" };
        ''')

        query = r'''
            SELECT (
                FOR noob in {"Emmanuel Villip", "Madeline Hatch"} UNION (
                    INSERT Person {name := noob} UNLESS CONFLICT
                    ON .name ELSE (UPDATE Person SET { tag := "redo" })
                )
            ) {name, tag} ORDER BY .name;
        '''

        await self.assert_query_result(
            query,
            [{"name": "Emmanuel Villip", "tag": None},
             {"name": "Madeline Hatch", "tag": "redo"}]
        )

        await self.assert_query_result(
            "SELECT Person {name, tag} ORDER BY .name",
            [
                {"name": "Emmanuel Villip", "tag": None},
                {"name": "Madeline Hatch", "tag": "redo"},
                {"name": "Phil Emarg", "tag": None},
            ],
        )

        await self.assert_query_result(
            query,
            [{"name": "Emmanuel Villip", "tag": "redo"},
             {"name": "Madeline Hatch", "tag": "redo"}],
        )

        await self.assert_query_result(
            "SELECT Person {name, tag} ORDER BY .name",
            [
                {"name": "Emmanuel Villip", "tag": "redo"},
                {"name": "Madeline Hatch", "tag": "redo"},
                {"name": "Phil Emarg", "tag": None},
            ],
        )

    async def test_edgeql_insert_unless_conflict_07(self):
        # Test it using default values
        query = r'''
            SELECT (
                INSERT Person UNLESS CONFLICT
                ON .name ELSE (UPDATE Person SET { tag := "redo" })
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Nemo"}],
        )

        await self.assert_query_result(
            "SELECT Person {name, tag}",
            [{"name": "Nemo", "tag": None}]
        )

        await self.assert_query_result(
            query,
            [{"name": "Nemo"}],
        )

        await self.con.execute(r'''
            INSERT Person { name := "Phil Emarg" }
        ''')

        # Only the correct record should be updated
        await self.assert_query_result(
            "SELECT Person {name, tag} ORDER BY .name",
            [{"name": "Nemo", "tag": "redo"},
             {"name": "Phil Emarg", "tag": None}],
        )

    async def test_edgeql_insert_unless_conflict_08(self):
        query = r'''
            SELECT (
                INSERT PersonWrapper {
                    person := (
                        INSERT Person { name := "foo" }
                        UNLESS CONFLICT ON .name ELSE (SELECT Person)
                    )
                }
            ) {id, person};
        '''

        res1 = await self.con.query_single(query)
        res2 = await self.con.query_single(query)

        self.assertNotEqual(res1.id, res2.id)
        self.assertEqual(res1.person.id, res2.person.id)

    async def test_edgeql_insert_unless_conflict_09(self):
        query = r'''
            INSERT Person {
                name := 'Cap',
                tag := 'hero',
            } UNLESS CONFLICT ON .name ELSE (
                UPDATE Person SET {
                    tag := 'super ' ++ .tag
                }
            );
        '''

        await self.con.execute(query)

        await self.assert_query_result(
            "SELECT Person { tag } FILTER .name = 'Cap'",
            [{
                'tag': 'hero'
            }]
        )

        await self.con.execute(query)

        await self.assert_query_result(
            "SELECT Person { tag } FILTER .name = 'Cap'",
            [{
                'tag': 'super hero'
            }]
        )

        await self.con.execute(query)

        await self.assert_query_result(
            "SELECT Person { tag } FILTER .name = 'Cap'",
            [{
                'tag': 'super super hero'
            }]
        )

    async def test_edgeql_insert_unless_conflict_10(self):
        await self.con.execute(r'''
            INSERT Person {
                name := "Foo",
                case_name := "Foo",
            };
        ''')

        await self.assert_query_result(
            r'''
            SELECT (
                INSERT Person {
                    name := "Bar",
                    case_name := "foo",
                }
                UNLESS CONFLICT ON (.case_name)
                ELSE (SELECT Person)
            ) {name, case_name};
            ''',
            [{
                'name': 'Foo', 'case_name': 'Foo',
            }]
        )

    async def test_edgeql_insert_unless_conflict_11(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "self-referencing INSERTs are not allowed"):
            await self.con.execute(r'''
                SELECT (
                    INSERT Person {name := "Madz"}
                    UNLESS CONFLICT ON (.name)
                    ELSE (INSERT Person {name := "Maddy"})
                ) {name};
            ''')

        query = r'''
            SELECT (
                INSERT Person {name := "Madz"}
                UNLESS CONFLICT ON (.name)
                ELSE (DETACHED (INSERT Person {name := "Maddy"}))
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Madz"}],
        )

        await self.assert_query_result(
            query,
            [{"name": "Maddy"}],
        )

    async def test_edgeql_insert_unless_conflict_12(self):
        # An upsert where we don't wrap it in another shape
        query = r'''
            INSERT Person {name := "Emmanuel Villip"} UNLESS CONFLICT
            ON .name ELSE (UPDATE Person SET { tag := "redo" })
        '''

        res1 = await self.con._fetchall(
            query, __typenames__=True,
        )
        res2 = await self.con._fetchall(
            query, __typenames__=True,
        )

        self.assertEqual(list(res1)[0].id, list(res2)[0].id)

    async def test_edgeql_insert_unless_conflict_13(self):
        # An insert-or-select where we don't wrap it in another shape
        query = r'''
            INSERT Person {name := "Emmanuel Villip"} UNLESS CONFLICT
            ON .name ELSE (SELECT Person)
        '''

        res1 = await self.con._fetchall(
            query, __typenames__=True,
        )
        res2 = await self.con._fetchall(
            query, __typenames__=True,
        )

        self.assertEqual(list(res1)[0].id, list(res2)[0].id)

    async def test_edgeql_insert_unless_conflict_14(self):
        query = r'''
            SELECT (
                INSERT Person2a {first := "Phil", last := "Emarg"}
                UNLESS CONFLICT ON (.first, .last) ELSE (SELECT Person2a)
            ) {first, last};
        '''

        await self.assert_query_result(
            query,
            [{"first": "Phil", "last": "Emarg"}],
        )

        await self.assert_query_result(
            query,
            [{"first": "Phil", "last": "Emarg"}],
        )

        await self.assert_query_result(
            r'''SELECT Person2a {first, last}''',
            [{"first": "Phil", "last": "Emarg"}],
        )

    async def test_edgeql_insert_unless_conflict_15(self):
        # test using a tuple object constraint with a link in it
        await self.con.execute(r'''
            INSERT Person {
                name := "Phil Emarg",
            };

            INSERT Person {
                name := "Madeline Hatch",
            };
        ''')

        query = r'''
            SELECT (
                INSERT Person2a {
                    first := "Emmanuel",
                    last := "Villip",
                    bff := (SELECT Person FILTER .name = "Phil Emarg")
                }
                UNLESS CONFLICT ON (.first, .bff) ELSE (SELECT Person2a)
            ) {first, last};
        '''

        await self.assert_query_result(
            query,
            [{"first": "Emmanuel", "last": "Villip"}],
        )

        await self.assert_query_result(
            query,
            [{"first": "Emmanuel", "last": "Villip"}],
        )

        await self.assert_query_result(
            query.replace("Villip", "Vi11ip"),
            [{"first": "Emmanuel", "last": "Villip"}],
        )

        await self.assert_query_result(
            r'''SELECT Person2a {first, last, friend := .bff.name}''',
            [{"first": "Emmanuel", "last": "Villip", "friend": "Phil Emarg"}],
        )

        await self.assert_query_result(
            query.replace("Villip", "Vi11ip").replace(
                "Phil Emarg", "Madeline Hatch"),
            [{"first": "Emmanuel", "last": "Vi11ip"}],
        )

        await self.assert_query_result(
            r'''
                SELECT Person2a {first, last, friend := .bff.name}
                ORDER BY .last
            ''',
            [
                {"first": "Emmanuel", "last": "Vi11ip",
                 "friend": "Madeline Hatch"},
                {"first": "Emmanuel", "last": "Villip",
                 "friend": "Phil Emarg"},
            ],
        )

    @test.not_implemented("""
        We don't support UNLESS CONFLICT with volatile keys yet.
        test_edgeql_insert_unless_conflict_16b tests our error reporting
    """)
    async def test_edgeql_insert_unless_conflict_16(self):
        # unless conflict with a volatile key
        for _ in range(10):
            await self.con.execute(r'''
                DELETE Person;
            ''')

            for _ in range(3):
                res = await self.con.query(r'''
                    INSERT Person { name := <str>math::floor(random() * 2) }
                    UNLESS CONFLICT ON (.name) ELSE (Person)
                ''')
                self.assertEqual(len(res), 1)

    async def test_edgeql_insert_unless_conflict_16b(self):
        async with self.assertRaisesRegexTx(
                edgedb.UnsupportedFeatureError,
                "INSERT UNLESS CONFLICT ON does not support volatile "
                "properties"):
            await self.con.execute('''
                INSERT Person { name := <str>math::floor(random() * 2) }
                UNLESS CONFLICT ON (.name) ELSE (Person)
            ''')

    async def test_edgeql_insert_unless_conflict_17(self):
        await self.con.execute(r'''
            FOR x IN {"1", "2", "3", "4"} UNION (
                INSERT Person { name := x }
            );
        ''')

        await self.assert_query_result(
            r'''
            FOR x IN {"1", "2", "3", "4"} UNION (
                INSERT Person { name := x }
                UNLESS CONFLICT ON (.name)
                ELSE (UPDATE Person SET { tag := "!" })
            );
            ''',
            [{}, {}],
            implicit_limit=2,
        )

        await self.assert_query_result(
            r'''
            SELECT Person.tag
            ''',
            ["!"] * 4,
        )

    async def test_edgeql_insert_unless_conflict_18a(self):
        await self.con.execute(r'''
            INSERT Person { name := "Phil Emarg" };
        ''')

        await self.assert_query_result(
            r'''
            INSERT DerivedPerson { name := "Phil Emarg" } UNLESS CONFLICT;
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            INSERT DerivedPerson { name := "Phil Emarg" }
            UNLESS CONFLICT ON (.name);
            ''',
            [],
        )

    async def test_edgeql_insert_unless_conflict_18b(self):
        await self.con.execute(r'''
            INSERT DerivedPerson { name := "Phil Emarg" };
        ''')

        await self.assert_query_result(
            r'''
            INSERT Person { name := "Phil Emarg" } UNLESS CONFLICT;
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            INSERT Person { name := "Phil Emarg" }
            UNLESS CONFLICT ON (.name);
            ''',
            [],
        )

    async def test_edgeql_insert_unless_conflict_19(self):
        await self.con.execute(r'''
            INSERT DerivedPerson { name := "Phil Emarg", sub_key := "1" };
        ''')

        await self.assert_query_result(
            r'''
            INSERT DerivedPerson { name := "Madeline Hatch", sub_key := "1" }
            UNLESS CONFLICT;
            ''',
            [],
        )

    async def test_edgeql_insert_unless_conflict_20a(self):
        # currently we reject ELSE in these cases
        with self.assertRaisesRegex(
            edgedb.errors.UnsupportedFeatureError,
            "UNLESS CONFLICT can not use ELSE when constraint is from a "
            "parent type",
        ):
            await self.con.execute(r'''
                INSERT DerivedPerson { name := "Madeline Hatch" }
                UNLESS CONFLICT ON (.name) ELSE (SELECT DerivedPerson)
            ''')

    @test.not_implemented("""
        ELSE isn't implemented in inheritance cases
    """)
    async def test_edgeql_insert_unless_conflict_20b(self):
        await self.con.execute(r'''
            INSERT Person { name := "1" };
        ''')

        # This is maybe what we want? Though it would also possibly
        # be reasonable to require filtering Person explicitly?
        # (we'll also want a test case with inheriting from two parents
        await self.assert_query_result(
            r'''
            FOR x IN {"1", "2"} UNION (
                INSERT DerivedPerson { name := x }
                UNLESS CONFLICT ON (.name)
                ELSE (UPDATE Person SET { tag := "!" })
            );
            ''',
            [{}],
        )

        await self.assert_query_result(
            r'''
            SELECT Person {name, tag, sub: Person IS DerivedPerson}
            ORDER BY .name
            ''',
            [
                {'name': "1", 'tag': "!", 'sub': False},
                {'name': "2", 'tag': None, 'sub': True},
            ]
        )

    async def test_edgeql_insert_unless_conflict_21(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Bar {
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz extending Foo, Bar;
        ''')

        await self.con.execute(r'''
            INSERT Foo { name := "foo" };
            INSERT Foo { name := "both" };
            INSERT Bar { name := "bar" };
            INSERT Bar { name := "both" };
        ''')

        await self.assert_query_result(
            r'''
            FOR x IN {"foo", "bar", "both", "asdf"} UNION (
                INSERT Baz { name := x }
                UNLESS CONFLICT ON (.name)
            );
            ''',
            [{}],
        )

    async def test_edgeql_insert_unless_conflict_22(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY foo -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Bar {
                CREATE REQUIRED PROPERTY bar -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz extending Foo, Bar;
        ''')

        await self.con.execute(r'''
            INSERT Foo { foo := "foo" };
            INSERT Bar { bar := "bar" };
        ''')

        await self.assert_query_result(
            r'''
            INSERT Baz { foo := "!", bar := "bar" }
            UNLESS CONFLICT ON (.bar)
            ''',
            [],
        )

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(
                r'''
                INSERT Baz { foo := "!", bar := "bar" }
                UNLESS CONFLICT ON (.foo)
                ''',
            )

        await self.assert_query_result(
            r'''
            INSERT Baz { foo := "foo", bar := "!" }
            UNLESS CONFLICT
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            INSERT Baz { foo := "!", bar := "bar" }
            UNLESS CONFLICT
            ''',
            [],
        )

        await self.assert_query_result(
            r'''
            INSERT Baz { foo := "foo", bar := "bar" }
            UNLESS CONFLICT
            ''',
            [],
        )

    async def test_edgeql_insert_unless_conflict_23(self):
        # Test that scoping of default parameters doesn't get messed up
        obj1 = await self.con.query_single('''
            insert DerivedPerson { sub_key := "foo" };
        ''')
        obj2 = await self.con.query_single('''
            insert DerivedPerson {
                name := "new",
                sub_key := <str>json_get(
                    to_json('{ "sub_key": "foo"}'), 'sub_key')
            }
            unless conflict on .sub_key else (select DerivedPerson);
        ''')
        self.assertEqual(obj1.id, obj2.id)

        obj3 = await self.con.query('''
            with
              raw_data := to_json('[{"sub_key": "foo"}]')
            for item in json_array_unpack(raw_data) union (
                insert DerivedPerson {
                    name := "new",
                    sub_key := <str>json_get(item, 'sub_key')
                }
                unless conflict on .sub_key else (select DerivedPerson)
            );
        ''')
        self.assertEqual(len(obj3), 1)
        self.assertEqual(obj1.id, tuple(obj3)[0].id)

    async def test_edgeql_insert_unless_conflict_24(self):
        await self.con.execute('''
            WITH
                raw_data := to_json('[1,2]')
            for data in {<int64>json_array_unpack(raw_data)} union(
                INSERT Person {
                    note := (INSERT Note { name := 'x' }),
                    name := <str>data,
                }
                UNLESS conflict on .name
            );
        ''')

    async def test_edgeql_insert_unless_conflict_25(self):
        await self.con.execute('''
            create type X {
                create required property n -> str {
                    create constraint exclusive;
                }
            };
            create type Y {
                create required link l -> X {
                    create constraint exclusive;
                }
            };
        ''')

        q = '''
            INSERT Y {
              l := (INSERT X { n := <str>$n } UNLESS CONFLICT ON (.n) ELSE (X))
            }
            UNLESS CONFLICT ON (.l);
        '''
        await self.assert_query_result(
            q, [{}], variables={'n': "1"},
        )
        await self.assert_query_result(
            q, [], variables={'n': "1"},
        )
        await self.con.execute('''
            insert X { n := "2" }
        ''')
        await self.assert_query_result(
            q, [{}], variables={'n': "2"},
        )
        await self.assert_query_result(
            q, [], variables={'n': "2"},
        )

    async def test_edgeql_insert_unless_conflict_26(self):
        # Test unless conflict on a property not actually mentioned
        await self.con.execute('''
            INSERT Person {
              name := "Colin"
            }
            UNLESS CONFLICT ON .case_name
            ELSE (Person)
        ''')

    async def test_edgeql_insert_unless_conflict_27(self):
        DML_Q = '''
            WITH P := (
                insert Person { name := <str>$0 }
                unless conflict on .name else (Person)
            )
            insert Person2a {
                first := <str>$1, last := '', bff := P
            } unless conflict on (.first, .last) else (
                update Person2a set { bff := P }
            )
        '''
        Q = '''
            select Person2a { bff: {name} } filter .first = <str>$0
        '''

        # Try all 4 combinations of which conflict clauses fire
        all_args = [
            ('a', 'x'),
            ('b', 'x'),
            ('b', 'y'),
            ('c', 'y'),
        ]
        for friend, person in all_args:
            await self.assert_query_result(
                DML_Q,
                [{}],
                variables=(friend, person),
                msg=f'insert {(friend, person)}',
            )
            await self.assert_query_result(
                Q,
                [{'bff': {'name': friend}}],
                variables=(person,),
                msg=f'check {(friend, person)}',
            )

    async def test_edgeql_insert_unless_conflict_28(self):
        await self.con.execute('''
            create type T {
                create multi property name -> str {
                    create constraint exclusive; } };
            insert T { name := {'foo', 'bar'} };
        ''')

        await self.assert_query_result(
            '''
            insert T { name := {'baz', 'bar'} } unless conflict
            ''',
            [],
        )

        await self.assert_query_result(
            '''
            select (
                insert T { name := {'baz', 'bar'} }
                unless conflict on (.name) else (T)
            ) { name }
            ''',
            [{'name': {'foo', 'bar'}}],
        )

    async def test_edgeql_insert_unless_conflict_29(self):
        await self.con.execute('''
            with
                sub := <Subordinate>{},
                upsert := (
                    insert InsertTest {
                        l2 := 0,
                        sub_ex := sub
                    }
                    unless conflict
                ),
            insert Note {
                name := '', subject := upsert,
            };
        ''')

    async def test_edgeql_insert_dependent_01(self):
        query = r'''
            SELECT (
                INSERT Person {
                    name :=  "Test",
                    notes := (INSERT Note {name := "tag!" })
                } UNLESS CONFLICT
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            query,
            [],
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_02(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob,
                               notes := (INSERT Note {name := "tag" })});
        """)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name",
            [{"name": "Madeline Hatch", "notes": [{"name": "tag"}]},
             {"name": "Phil Emarg", "notes": [{"name": "tag"}]}]
        )

        # Make sure the notes are distinct
        await self.assert_query_result(
            r'''SELECT count(DISTINCT Person.notes)''',
            [2],
        )

    async def test_edgeql_insert_dependent_03(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {
                    name := noob,
                    notes := (FOR note in {"hello", "world"}
                              UNION (INSERT Note { name := note }))});
        """)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name",
            [{"name": "Madeline Hatch",
              "notes": [{"name": "hello"}, {"name": "world"}]},
             {"name": "Phil Emarg",
              "notes": [{"name": "hello"}, {"name": "world"}]}],
        )

        # Make sure the notes are distinct
        await self.assert_query_result(
            r'''SELECT count(DISTINCT Person.notes)''',
            [4],
        )

    async def test_edgeql_insert_dependent_04(self):
        query = r'''
            SELECT (
                INSERT Person {
                    name :=  "Zendaya",
                    notes := (FOR note in {"hello", "world"}
                              UNION (INSERT Note { name := note }))
                } UNLESS CONFLICT
            ) { name, notes: {name} ORDER BY .name};
        '''

        # Execute twice and then make sure that there weren't any
        # stray side-effects from the second.
        await self.assert_query_result(
            query,
            [{"name": "Zendaya",
              "notes": [{"name": "hello"}, {"name": "world"}]}],
        )
        await self.assert_query_result(
            query,
            [],
        )

        # Make sure only the 2 inserts into Note happened
        await self.assert_query_result(
            r'''SELECT DISTINCT count(Person.notes)''',
            [2],
        )

    async def test_edgeql_insert_dependent_05(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob}
            );
        """)

        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                UPDATE Person FILTER .name = noob
                SET {notes := (INSERT Note { name := "tag" }) }
            );
        """)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name DESC",
            [{"name": "Phil Emarg", "notes": [{"name": "tag"}]},
             {"name": "Madeline Hatch", "notes": [{"name": "tag"}]}],
        )

        # Make sure the notes are distinct
        await self.assert_query_result(
            r'''SELECT count(DISTINCT Person.notes)''',
            [2],
        )

    async def test_edgeql_insert_dependent_06(self):
        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                INSERT Person {name := noob}
            );
        """)

        await self.con.execute(r"""
            FOR noob in {"Phil Emarg", "Madeline Hatch"}
            UNION (
                UPDATE Person FILTER .name = noob
                SET {
                    notes := (FOR note in {"hello", "world"}
                              UNION (INSERT Note { name := note }))
                }
            );
        """)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name DESC",
            [{"name": "Phil Emarg",
              "notes": [{"name": "hello"}, {"name": "world"}]},
             {"name": "Madeline Hatch",
              "notes": [{"name": "hello"}, {"name": "world"}]}],
        )

        # Make sure the notes are distinct
        await self.assert_query_result(
            r'''SELECT count(DISTINCT Person.notes)''',
            [4],
        )

    async def test_edgeql_insert_dependent_07(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "mutations are invalid in a shape's computed expression"):
            await self.con.execute(
                r"""
                    SELECT Person {
                        name,
                        foo := (
                            INSERT Note {name := 'NoteDep07'}
                        ) {
                            name,
                        }
                    };
                """
            )

    async def test_edgeql_insert_dependent_08(self):
        await self.con.execute(r"""
            INSERT Person {
                name := 'PersonDep08'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    foo := (
                        INSERT Note {name := 'NoteDep08'}
                    )
                SELECT Person {
                    name,
                    foo := foo {
                        name,
                    }
                };
            """,
            [
                {
                    'name': 'PersonDep08',
                    'foo': {
                        'name': 'NoteDep08',
                    },
                },
            ]
        )

        await self.assert_query_result(
            r"""
                SELECT Person {
                    name,
                    notes: {
                        name,
                    }
                };
            """,
            [
                {
                    'name': 'PersonDep08',
                    'notes': [],
                },
            ]
        )

        await self.assert_query_result(
            r"""
                SELECT Note {
                    name,
                };
            """,
            [
                {
                    'name': 'NoteDep08',
                },
            ]
        )

    async def test_edgeql_insert_dependent_09(self):
        await self.con.execute(r"""
            INSERT Person {
                name := 'PersonDep09'
            };
        """)

        await self.assert_query_result(
            r"""
                WITH
                    foo := (
                        INSERT Note {name := 'NoteDep09'}
                    )
                SELECT Person {
                    name,
                    # Fake having an actual linked Note
                    notes := foo {
                        name,
                    }
                };
            """,
            [
                {
                    'name': 'PersonDep09',
                    'notes': [{
                        'name': 'NoteDep09',
                    }],
                },
            ]
        )

        await self.assert_query_result(
            r"""
                SELECT Person {
                    name,
                    notes: {
                        name,
                    }
                };
            """,
            [
                {
                    'name': 'PersonDep09',
                    'notes': [],
                },
            ]
        )

        await self.assert_query_result(
            r"""
                SELECT Note {
                    name,
                };
            """,
            [
                {
                    'name': 'NoteDep09',
                },
            ]
        )

    async def test_edgeql_insert_dependent_10(self):
        await self.con.execute(r"""INSERT Note { name := "foo" };""")

        query = r"""
            FOR noob in {"foo", "bar"} UNION (
                INSERT Person { name := noob,
                                notes := (UPDATE Note FILTER .name = noob
                                          SET { name := noob ++ "!"})
                }
                UNLESS CONFLICT
            );
        """

        await self.con.execute(query)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name DESC",
            [{"name": "foo",
              "notes": [{"name": "foo!"}]},
             {"name": "bar",
              "notes": []}],
        )

        await self.con.execute(r"""INSERT Note { name := "bar" };""")

        await self.con.execute(query)

        await self.assert_query_result(
            "SELECT Person { name, notes: {name} } ORDER BY .name",
            [{"name": "bar",
              "notes": []},
             {"name": "foo",
              "notes": [{"name": "foo!"}]}],
        )

        await self.assert_query_result(
            "SELECT Note.name",
            {"foo!", "bar"}
        )

    async def test_edgeql_insert_dependent_11(self):
        # A with-bound insert used in a FOR should only execute once
        await self.con.execute(
            r'''
                WITH N := (INSERT Note {name := "tag!" }),
                FOR name in {"Phil", "Madz"} UNION (
                    INSERT Person {
                        name := name,
                        notes := N,
                    }
                );
            '''
        )

        # Should only be one note
        await self.assert_query_result(
            r'''SELECT Note { name }''',
            [{"name": "tag!"}],
        )

    async def test_edgeql_insert_dependent_12(self):
        # A with-bound insert used in a FOR should only execute once
        # Same as above, but using a single link
        await self.con.execute(
            r'''
                WITH N := (INSERT Note {name := "tag!" }),
                FOR name in {"Phil", "Madz"} UNION (
                    INSERT Person {
                        name := name,
                        note := N,
                    }
                );
            '''
        )

        # Should only be one note
        await self.assert_query_result(
            r'''SELECT Note { name }''',
            [{"name": "tag!"}],
        )

    async def test_edgeql_insert_dependent_13(self):
        # A WITH bound INSERT used in an INSERT UNLESS CONFLICT
        # should execute unconditionally
        query = r'''
        WITH N := (INSERT Note {name := "tag!" }),
        SELECT (
            INSERT Person {
                name := "Test",
                notes := N,
            } UNLESS CONFLICT
        ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            query,
            [],
        )

        # Make sure that two inserts happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [2],
        )

    async def test_edgeql_insert_dependent_14(self):
        # Test the combination of a WITH bound INSERT, a WITH bound
        # FOR loop, and a shape query that references both
        await self.assert_query_result(
            r'''
                WITH N := (INSERT Note {name := "tag!" }),
                    X := (FOR name in {"Phil", "Madz"} UNION (
                        INSERT Person {
                            name := name,
                            notes := N,
                        }
                    )),
                SELECT {
                    x := (SELECT X { name } ORDER BY .name),
                    n := N { name },
                };
            ''',
            [{
                "n": {"name": "tag!"},
                "x": [{"name": "Madz"}, {"name": "Phil"}]
            }],
        )

        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_15(self):
        query = r'''
            SELECT (
                INSERT Person {
                    name := "Test",
                    note := (INSERT Note {name := "tag!" })
                } UNLESS CONFLICT
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            query,
            [],
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_16(self):
        await self.assert_query_result(
            r'''
                SELECT (
                    INSERT Person {
                        name := "Test",
                        note := (INSERT Note {name := "tag!" })
                    } UNLESS CONFLICT
                ) {name};
            ''',
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            r'''
                SELECT (
                    INSERT Person {
                        name := "Test",
                        note := (
                            UPDATE (SELECT Note LIMIT 1)
                            SET { name := "owned" })
                    } UNLESS CONFLICT
                ) {name};
            ''',
            []
        )

        # Make sure the update did not happen
        await self.assert_query_result(
            r'''SELECT Note { name }''',
            [{"name": "tag!"}],
        )

    async def test_edgeql_insert_dependent_17(self):
        query = r'''
            SELECT (
                INSERT Person {
                    name := "Test",
                    note := (INSERT Note {name := "tag!" })
                } UNLESS CONFLICT ON (.name) ELSE (SELECT Person)
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            query,
            [{"name": "Test"}]
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_18(self):
        await self.con.execute('''
            INSERT Person { name := "foo" }
        ''')

        query = r'''
            SELECT (
            FOR name in {"foo", "bar"} UNION (
                SELECT (
                    INSERT Person {
                        name := name,
                        note := (INSERT Note {name := "tag!" })
                    } UNLESS CONFLICT ON (.name) ELSE (SELECT Person)
                ) {name}
            )) ORDER BY .name;
        '''

        await self.assert_query_result(
            query,
            [{"name": "bar"},
             {"name": "foo"}]
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_19(self):
        # same as above but without ELSE
        await self.con.execute('''
            INSERT Person { name := "foo" }
        ''')

        query = r'''
            SELECT (
            FOR name in {"foo", "bar"} UNION (
                SELECT (
                    INSERT Person {
                        name := name,
                        note := (INSERT Note {name := "tag!" })
                    } UNLESS CONFLICT ON (.name)
                ) {name}
            )) ORDER BY .name;
        '''

        await self.assert_query_result(
            query,
            [{"name": "bar"}]
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_20(self):
        # make sure a nested insert gets failed properly when it is
        # nested in a dumb way instead of directly being put in a
        # pointer
        await self.con.execute('''
            INSERT Person { name := "foo" }
        ''')

        query = r'''
            SELECT (
            FOR name in {"foo", "bar"} UNION (
                SELECT (
                    INSERT Person {
                        name := name,
                        tag2 := (INSERT Note {name := "tag!" }).name
                    } UNLESS CONFLICT ON (.name)
                ) {name, tag2}
            )) ORDER BY .name;
        '''

        await self.assert_query_result(
            query,
            [{"name": "bar", "tag2": "tag!"}]
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_21(self):
        # test with an empty set as one of the values
        query = r'''
            SELECT (
                INSERT Person {
                    name := "Test",
                    note := (INSERT Note {name := "tag!" }),
                    multi_prop := {},
                } UNLESS CONFLICT
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            query,
            [],
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_22(self):
        # test with a constraint that has a nontrivial subjectexpr
        await self.assert_query_result(
            r'''
            SELECT (
                INSERT Person {
                    name := "Test",
                    note := (INSERT Note {name := "tag!" }),
                    case_name := "Foo",
                } UNLESS CONFLICT
            ) {name};
            ''',
            [{"name": "Test"}]
        )

        await self.assert_query_result(
            r'''
            SELECT (
                INSERT Person {
                    name := "Test2",
                    note := (INSERT Note {name := "tag!" }),
                    case_name := "foo",
                } UNLESS CONFLICT
            ) {name};
            ''',
            [],
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_23(self):
        await self.con.execute('''
            INSERT Person2a {
                first := "Madeline",
                last := "Hatch1",
            }
        ''')

        # test with something that has an object constraint
        query = r'''
            SELECT (
                INSERT Person2a {
                    first := "Phil",
                    last := "Emarg",
                    note := (INSERT Note {name := "tag!" }),
                } UNLESS CONFLICT
            ) {first, last};
        '''

        await self.assert_query_result(
            query,
            [{"first": "Phil", "last": "Emarg"}]
        )

        await self.assert_query_result(
            query,
            [],
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_24(self):
        # test with something that has a computed constraint
        await self.con.execute('''
            INSERT Person2b {
                first := "Madeline",
                last := "Hatch2",
            }
        ''')

        query = r'''
            SELECT (
                INSERT Person2b {
                    first := "Phil",
                    last := "Emarg",
                    note := (INSERT Note {name := "tag!" }),
                } UNLESS CONFLICT
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Phil Emarg"}]
        )

        await self.assert_query_result(
            query,
            [],
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_25(self):
        await self.con.execute('''
            INSERT Person2b {
                first := "Madeline",
                last := "Hatch3",
            }
        ''')
        # test with something that has a computed constraint using ON
        query = r'''
            SELECT (
                INSERT Person2b {
                    first := "Phil",
                    last := "Emarg",
                    note := (INSERT Note {name := "tag!" }),
                }
                UNLESS CONFLICT ON (.name)
                ELSE (SELECT Person2b)
            ) {name};
        '''

        await self.assert_query_result(
            query,
            [{"name": "Phil Emarg"}]
        )

        await self.assert_query_result(
            query,
            [{"name": "Phil Emarg"}]
        )

        # Make sure only 1 insert into Note happened
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_dependent_26(self):
        # test that it works with an empty value in a computed prop
        await self.con.execute('''
            INSERT Person2b {
                first := "Madeline",
                last := "Hatch4",
            }
        ''')

        query = r'''
            SELECT (
                INSERT Person2b {
                    first := "Phil",
                    note := (INSERT Note {name := "tag!" }),
                } UNLESS CONFLICT
            ) {first, name};
        '''

        await self.assert_query_result(
            query,
            [{"first": "Phil", "name": None}]
        )

        await self.assert_query_result(
            query,
            [{"first": "Phil", "name": None}]
        )

        # No conflict (because last was empty), so two inserts
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [2],
        )

    async def test_edgeql_insert_dependent_27(self):
        # test with two nested single links
        await self.con.execute('''
            CREATE ABSTRACT TYPE Named {
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE DELEGATED CONSTRAINT exclusive;
                };
            };

            CREATE TYPE Foo EXTENDING Named;
            CREATE TYPE Bar EXTENDING Named;

            CREATE TYPE Obj extending Named {
                CREATE LINK foo -> Foo;
                CREATE LINK bar -> Bar;
            };
        ''')

        await self.assert_query_result(
            r'''
                INSERT Obj {
                    name := "obj",
                    foo := (
                        INSERT Foo {name := "foo"}
                        UNLESS CONFLICT ON .name ELSE (SELECT Foo)
                    ),
                    bar := (
                        INSERT Bar {name := "bar"}
                        UNLESS CONFLICT ON .name ELSE (SELECT Bar)
                    ),
                }
                UNLESS CONFLICT ON .name ELSE (SELECT Obj);
            ''',
            [{"id": str}]
        )

        await self.assert_query_result(
            "SELECT Obj {name, foo: {name}, bar: {name}}",
            [{"name": "obj", "foo": {"name": "foo"}, "bar": {"name": "bar"}}],
        )

    async def test_edgeql_insert_dependent_28(self):
        await self.con.execute(r"""
            create type X {
                create required property name -> str {
                    create constraint exclusive
                };
                create multi link notes -> Note;
            };
        """)

        # The Madeline note insert shouldn't happen
        q = r"""
            INSERT X {name := "Madeline Hatch",
                      notes := (INSERT Note {name := "tag" })}
            UNLESS CONFLICT;
        """
        await self.con.query(q)
        await self.con.query(q)

        # Should only be one note
        await self.assert_query_result(
            r'''SELECT count(Note)''',
            [1],
        )

    async def test_edgeql_insert_unless_conflict_self_01(self):
        # It would also be a reasonable semantics for this test to
        # return two objects
        query = r'''
            SELECT (
              FOR x in {"Phil Emarg", "Phil Emarg"} UNION (
                INSERT Person {name := x}
                UNLESS CONFLICT ON (.name)
                ELSE (SELECT Person)
              )
            ) { name }
            ORDER BY .name;
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_unless_conflict_self_02(self):
        # It would also be a reasonable semantics for this test to
        # not fail
        query = r'''
            SELECT (
              (INSERT Person {name := "Emmanuel Villip"} UNLESS CONFLICT),
              (INSERT Person {name := "Emmanuel Villip"} UNLESS CONFLICT),
            )
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_unless_conflict_self_03(self):
        # It would also be a reasonable semantics for this test to
        # not fail
        query = r'''
            INSERT Person {
                name := "Madeline Hatch",
                note := (
                    INSERT Note {
                        name := "wtvr",
                        subject := (
                            DETACHED (
                                INSERT Person { name := "Madeline Hatch" })
                        ),
                     }
                )
            }
            UNLESS CONFLICT;
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_nested_volatile_01(self):
        await self.con.execute('''
            INSERT Subordinate {
                name := 'subtest 1'
            };

            INSERT Subordinate {
                name := 'subtest 2'
            };

            INSERT InsertTest {
                name := 'insert nested',
                l2 := 0,
                subordinates := (
                    SELECT Subordinate {
                        @comment := <str>uuid_generate_v1mc()
                    }
                )
            };
        ''')

        # Each object should get a distinct @comment
        await self.assert_query_result(
            r'''
                SELECT count(DISTINCT InsertTest.subordinates@comment);
            ''',
            [2]
        )

    async def test_edgeql_insert_cross_type_conflict_01a(self):
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_01b(self):
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name}),
                 Z := (B, F),
            SELECT Z;
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_01c(self):
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name}),
            SELECT (SELECT (B, F));
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_01d(self):
        # argh!
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name}),
            SELECT (B, F) FILTER false;
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_01e(self):
        # argh!
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name}),
            SELECT (B, F, <str>{});
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_02(self):
        query = r'''
            WITH name := 'Madeline Hatch',
                 F := (INSERT DerivedPerson {name := name}),
                 B := (INSERT Person {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_03(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (UPDATE Person FILTER .name = 'Bar' SET {name := name}),
                 F := (INSERT Person {name := name})
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_04(self):
        query = r'''
        WITH
             B := (INSERT Person {name := "Foo", case_name := "asdf"}),
             F := (INSERT DerivedPerson {name := "Bar", case_name := "ASDF"}),
        SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "case_name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_05(self):
        query = r'''
        WITH
             B := (INSERT Person {name := "Bar", multi_prop := {"1", "2"}}),
             F := (INSERT DerivedPerson {
                      name := "Foo", multi_prop := {"2","3"}}),
        SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "multi_prop violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_06(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (UPDATE Person FILTER .name = 'Bar'
                       SET {multi_prop += "a"}),
                 F := (INSERT Person {name := name, multi_prop := {"a", "b"}})
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "multi_prop violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_07a(self):
        await self.con.execute('''
            INSERT Person { name := 'Foo' };
        ''')

        query = r'''
            WITH
                 B := (UPDATE Person FILTER .name = 'Foo'
                       SET {name := "Bar"}),
                 F := (INSERT Person {name := "Foo"})
            SELECT (B, F);
        '''

        # This is a bummer, but I guess correct?
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_07b(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar', multi_prop := "a" };
        ''')

        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (UPDATE Person FILTER .name = 'Bar'
                       SET {multi_prop -= "a"}),
                 F := (INSERT Person {name := name, multi_prop := {"a", "b"}})
            SELECT (B, F);
        '''

        # This is a bummer, but I guess correct?
        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "multi_prop violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_08(self):
        # UNLESS CONFLICT doesn't change anything here since inserting
        # the same thing twice in one query still fails
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name} UNLESS CONFLICT),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_09(self):
        # UNLESS CONFLICT doesn't change anything here since inserting
        # the same thing twice in one query still fails
        query = r'''
            WITH name := 'Madeline Hatch',
                 F := (INSERT DerivedPerson {name := name} UNLESS CONFLICT),
                 B := (INSERT Person {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_10(self):
        await self.con.execute('''
            CREATE ABSTRACT TYPE Named {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                }
            };
            CREATE ABSTRACT TYPE Titled {
                CREATE PROPERTY title -> str {
                    CREATE CONSTRAINT exclusive;
                }
            };
            CREATE TYPE Foo EXTENDING Named, Titled;
            CREATE TYPE Bar EXTENDING Named, Titled;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(r'''
                WITH name := 'Madeline Hatch',
                     B := (INSERT Bar {name := name}),
                     F := (INSERT Foo {name := name}),
                SELECT (B, F);
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "title violates exclusivity constraint"):
            await self.con.execute(r'''
                WITH name := 'Madeline Hatch',
                     B := (INSERT Bar {title := name}),
                     F := (INSERT Foo {title := name}),
                SELECT (B, F);
            ''')

    async def test_edgeql_insert_cross_type_conflict_11(self):
        # Should be fine if it is delegated
        await self.con.execute('''
            CREATE ABSTRACT TYPE Named {
                CREATE PROPERTY name -> str {
                    CREATE DELEGATED CONSTRAINT exclusive;
                }
            };
            CREATE TYPE Foo EXTENDING Named;
            CREATE TYPE Bar EXTENDING Named;
        ''')

        await self.con.execute(r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Bar {name := name}),
                 F := (INSERT Foo {name := name}),
            SELECT (B, F);
        ''')

    async def test_edgeql_insert_cross_type_conflict_12(self):
        query = r'''
        WITH
             B := (INSERT Person {name := "foo"}),
             F := (FOR a in {"b", "f"} UNION (
                   FOR b in {"ar", "oo"} UNION (
                       INSERT DerivedPerson {name := a ++ b}
                  ))),
        SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_13(self):
        query = r'''
        WITH
             F := (FOR a in {"b", "f"} UNION (
                   FOR b in {"ar", "oo"} UNION (
                       INSERT DerivedPerson {name := a ++ b}
                  ))),
             B := (INSERT Person {name := "foo"}),
        SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_14(self):
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := <str>random()}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "does not support volatile properties with exclusive"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_15(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE LINK foo -> Foo;
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Bar EXTENDING Foo;
        ''')

        query = r'''
            WITH name := 'Alice'
            INSERT Foo {
                name := name,
                foo := (
                    INSERT Bar {
                        name := name,
                    }
                )
            };
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_16(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE MULTI LINK foo -> Foo;
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Bar EXTENDING Foo;
        ''')

        query = r'''
            WITH name := 'Alice'
            INSERT Foo {
                name := name,
                foo := (
                    INSERT Bar {
                        name := name,
                    }
                )
            };
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_17(self):
        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (INSERT Person {name := name}),
                 F := (INSERT DerivedPerson {name := name}),
                 L := (FOR x IN {F} UNION (INSERT Note {name := "bs"})),
            SELECT (B, L);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_18(self):
        await self.con.execute('''
            create type Foo {
                create property foo -> str {
                    create constraint exclusive on (__subject__ ?? '');
                };
                create property bar -> str;
                create constraint exclusive on (.bar ?? '');
            };
            create type Bar extending Foo;
        ''')

        query = r'''
            SELECT ((insert Foo), (insert Bar));
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_cross_type_conflict_19(self):
        await self.con.execute('''
            create required global break -> bool { set default := false; };
            create type X {
                create property foo -> str {
                    create constraint exclusive;
                };
                create access policy yes allow all using (true);
                create access policy no deny select using (global break);
            };
            create type Y extending X;
        ''')

        await self.con.execute('''
            set global break := true
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.query('''
                select {
                    (insert X { foo := "!" }),
                    (insert Y { foo := "!" }),
                };
            ''')

    async def test_edgeql_insert_update_cross_type_conflict_01a(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH name := 'Madeline Hatch',
                 F := (INSERT Person {name := name}),
                 B := (UPDATE Person FILTER .name = 'Bar' SET {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_01b(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH name := 'Madeline Hatch',
                 F := (INSERT Person {name := name}),
                 B := (UPDATE Person FILTER .name = 'Bar' SET {name := name}),
            SELECT (SELECT (B, F));
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_02(self):
        # this isn't really an insert test
        await self.con.execute('''
            INSERT Person { name := 'Foo' };
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH name := 'Madeline Hatch',
                 B := (UPDATE Person FILTER .name = 'Bar' SET {name := name}),
                 F := (UPDATE Person FILTER .name = 'Foo' SET {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_03(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH
                 F := (INSERT Person {name := 'Bar!'}),
                 B := (UPDATE Person FILTER .name = 'Bar'
                       SET {name := .name ++ "!"}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_04(self):
        await self.con.execute('''
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH
                 F := (INSERT Person {name := 'Bar?'}),
                 B := (UPDATE Person FILTER .name = 'Bar'
                       SET {name := .name ++ "!"}),
            SELECT (B, F);
        '''

        # This should be fine.
        await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_05a(self):
        # this isn't really an insert test
        await self.con.execute('''
            INSERT Person { name := 'Foo' };
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            UPDATE Person FILTER true SET { name := "!" };
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_05b(self):
        # this isn't really an insert test
        await self.con.execute('''
            INSERT Person { name := 'Foo' };
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            WITH P := Person
            UPDATE P FILTER true SET { name := "!" };
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_06(self):
        # this isn't really an insert test
        await self.con.execute('''
            INSERT Person { name := 'Foo' };
            INSERT DerivedPerson { name := 'Bar' };
        ''')

        query = r'''
            UPDATE Person FILTER true SET { multi_prop := "!" };
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "multi_prop violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_07a(self):
        await self.con.execute('''
            INSERT Person2a { first := 'foo', last := 'bar' };
        ''')

        query = r'''
            WITH
                 F := (INSERT DerivedPerson2a {first := 'foo', last := 'baz'}),
                 B := (UPDATE Person2a FILTER .first = 'foo' and .last = 'bar'
                       SET {last := 'baz'}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "Person2a violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_07b(self):
        # this should be fine, though
        await self.con.execute('''
            INSERT Person2a { first := 'foo', last := 'bar' };
            INSERT DerivedPerson2a { first := 'spam', last := 'eggs' };
        ''')

        query = r'''
            UPDATE Person2a SET { first := "!" };
        '''

        await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_08a(self):
        await self.con.execute('''
            INSERT Person2b { first := 'foo', last := 'bar' };
        ''')

        query = r'''
            WITH
                 F := (INSERT DerivedPerson2b {first := 'foo', last := 'baz'}),
                 B := (UPDATE Person2b FILTER .first = 'foo' and .last = 'bar'
                       SET {last := 'baz'}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_08b(self):
        # this should be fine, though
        await self.con.execute('''
            INSERT Person2b { first := 'foo', last := 'bar' };
            INSERT DerivedPerson2b { first := 'spam', last := 'eggs' };
        ''')

        query = r'''
            UPDATE Person2b SET { first := "!" };
        '''

        await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_09a(self):
        # a constraint that is just on the children
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
            };
            CREATE TYPE Bar EXTENDING Foo  {
                ALTER PROPERTY name {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz EXTENDING Bar;

            INSERT Bar { name := "bar" };
            INSERT Baz { name := "baz" };
        ''')

        query = r'''
            UPDATE Foo FILTER true SET { name := "!" };
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_09b(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
            };
            CREATE TYPE Bar EXTENDING Foo  {
                ALTER PROPERTY name {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz EXTENDING Bar;

            INSERT Bar { name := "bar" };
            # INSERT Baz { name := "baz" };
        ''')

        query = r'''
            WITH name := '!',
                 B := (UPDATE Foo FILTER .name = 'bar' SET {name := name}),
                 F := (INSERT Bar {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_09c(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
            };
            CREATE TYPE Bar EXTENDING Foo  {
                ALTER PROPERTY name {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz EXTENDING Bar;

            INSERT Bar { name := "bar" };
            INSERT Baz { name := "baz" };
        ''')

        query = r'''
            WITH name := '!',
                 B := (UPDATE Foo FILTER .name = 'bar' SET {name := name}),
                 Z := (UPDATE Foo FILTER .name = 'baz' SET {name := name}),
            SELECT (B, Z);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_10(self):
        # a constraint that is just on the children
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE MULTI PROPERTY tags -> str;
            };
            CREATE TYPE Bar EXTENDING Foo  {
                ALTER PROPERTY tags {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz EXTENDING Bar;

            INSERT Bar { name := "bar" };
            INSERT Baz { name := "baz" };
        ''')

        query = r'''
            UPDATE Foo FILTER true SET { tags := "!" };
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "tags violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_11(self):
        # a constraint that is on an unrelated type
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
            };
            CREATE TYPE Bar {
                CREATE PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Baz EXTENDING Foo, Bar;

            INSERT Baz { name := "baz" };
        ''')

        query = r'''
            WITH name := '!',
                 F := (INSERT Bar {name := name}),
                 B := (UPDATE Foo FILTER .name = 'baz' SET {name := name}),
            SELECT (B, F);
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_12(self):
        # a constraint that is just on the children, using data only
        # in the children
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE REQUIRED PROPERTY x -> int64;
            };
            CREATE TYPE Bar EXTENDING Foo {
                CREATE REQUIRED PROPERTY y -> int64;
                CREATE CONSTRAINT exclusive on
                    ((__subject__.x + __subject__.y));
            };
            CREATE TYPE Baz EXTENDING Bar;

            INSERT Bar { name := "bar", x := 1, y := 1 };
            INSERT Baz { name := "baz", x := 2, y := 2 };
        ''')

        query = r'''
            UPDATE Foo FILTER true SET { x := - .x };
        '''

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "Bar violates exclusivity constraint"):
            await self.con.execute(query)

        await self.con.execute('''
            UPDATE Foo FILTER .name = 'baz' SET { x := 3 };
        ''')

        # now it should be fine
        await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_13(self):
        # ... make sure we don't try enforcing on non-exclusive constraints
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE REQUIRED PROPERTY x -> int64;
                CREATE CONSTRAINT expression on ((.x >= 0));
            };
            INSERT Foo { name := "bar", x := 1 };
            INSERT Foo { name := "baz", x := 2 };
        ''')

        query1 = r'''
            UPDATE Foo FILTER true SET { name := .name };
        '''
        query2 = r'''
            UPDATE Foo FILTER true SET { x := .x + 1 };
        '''

        await self.con.execute(query1)
        await self.con.execute(query2)

        await self.con.execute('''
            CREATE TYPE Bar EXTENDING Foo;
        ''')
        await self.con.execute(query1)
        await self.con.execute(query2)

    async def test_edgeql_insert_update_cross_type_conflict_14(self):
        await self.con.execute('''
            create type A {
                create property foo -> int64 {
                    create constraint exclusive;
                }
            };
            create type B extending A;
            create type X extending B;
            create type Y extending B;
        ''')

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute('''
                with x := (insert X { foo := 0 }),
                     y := (insert Y { foo := 0 }),
                select {x, y};
            ''')

    async def test_edgeql_insert_update_cross_type_conflict_15(self):
        await self.con.execute('''
            create required global break -> bool { set default := false; };
            create type X {
                create property foo -> str {
                    create constraint exclusive;
                };
                create access policy yes allow all using (true);
                create access policy no deny select using (
                    global break and exists .foo);
            };
            create type Y extending X;
        ''')
        await self.con.query('''
            insert X;
        ''')
        await self.con.query('''
            insert Y;
        ''')
        await self.con.execute('''
            set global break := true
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.query('''
                update X set { foo := "!" };
            ''')

    async def test_edgeql_insert_update_cross_type_conflict_16(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY name -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE TYPE Bar EXTENDING Foo;
            CREATE TYPE Baz EXTENDING Foo;

            INSERT Bar { name := "bar" };
            INSERT Baz { name := "baz" };
        ''')

        query = r'''
            UPDATE {Bar, Baz} FILTER true SET { name := "!" };
        '''

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                "name violates exclusivity constraint"):
            await self.con.execute(query)

    async def test_edgeql_insert_update_cross_type_conflict_17(self):

        await self.con.execute('''
            create type T;
            create type X {
                create multi link l -> T {
                    create property x -> str { create constraint exclusive; }
                };
            };
            create type Y extending X;
            insert X;
            insert Y;
        ''')

        with self.assertRaisesRegex(
                edgedb.UnsupportedFeatureError,
                "do not support exclusive constraints on link properties"):
            await self.con.execute('''
                update X set { l := (insert T { @x := 'x' }) };
            ''')

    async def test_edgeql_insert_and_update_01(self):
        # INSERTing something that would violate a constraint while
        # fixing the violation is still supposed to be an error.
        await self.con.execute('''
            INSERT Person { name := 'foo' };
        ''')

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute('''
                SELECT (
                    (UPDATE Person FILTER .name = 'foo'
                        SET { name := 'foo' }),
                    (INSERT Person { name := 'foo' })
                )
            ''')

    async def test_edgeql_insert_and_delete_01(self):
        # INSERTing something that would violate a constraint while
        # fixing the violation is still supposed to be an error.
        await self.con.execute('''
            INSERT Person { name := 'foo' };
        ''')

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute('''
                SELECT (
                    (DELETE Person FILTER .name = 'foo'),
                    (INSERT Person { name := 'foo' })
                )
            ''')

    async def test_edgeql_insert_and_delete_02(self):
        # Assigning the result of a DELETE as a link during an INSERT
        # should be an error.
        await self.con.execute('''
            INSERT Note { name := 'delete me' };
        ''')

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    r"deletion of default::Note.+ is "
                                    r"prohibited by link target policy"):
            await self.con.execute('''
                INSERT Person {
                    name := 'foo',
                    note := (
                        DELETE Note FILTER .name = 'delete me' LIMIT 1
                    )
                }
            ''')

    async def test_edgeql_insert_cardinality_assertion(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "possibly more than one element returned by an expression "
                "for a link 'sub' declared as 'single'"):
            await self.con.query(r'''
                INSERT InsertTest {
                    l2 := 10,
                    sub := Subordinate,
                }
            ''')

    async def test_edgeql_insert_volatile_01(self):
        await self.con.execute('''
            WITH name := <str>random(),
            INSERT Person { name := name, tag := name };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_02(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                name := x ++ "!",
            INSERT Person { name := name, tag := name };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_03(self):
        await self.con.execute('''
            WITH
                x := "!",
                name := x ++ <str>random(),
            INSERT Person { name := name, tag := name };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_04(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                name := x ++ <str>random(),
            INSERT Person { name := name, tag := name };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_05(self):
        await self.con.execute('''
            WITH name := <str>random(),
            SELECT (INSERT Person { name := name, tag := name });
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_06(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                name := x ++ "!",
            SELECT (INSERT Person { name := name, tag := name });
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_07(self):
        await self.con.execute('''
            WITH
                x := "!",
                name := x ++ <str>random(),
            SELECT (INSERT Person { name := name, tag := name });
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_08(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                name := x ++ <str>random(),
            SELECT (INSERT Person { name := name, tag := name });
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_09(self):
        await self.con.execute('''
            WITH x := <str>random()
            SELECT (
                WITH name := x ++ "!"
                INSERT Person { name := name, tag := name }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_10(self):
        await self.con.execute('''
            WITH x := "!"
            SELECT (
                WITH name := x ++ <str>random()
                INSERT Person { name := name, tag := name }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_11(self):
        await self.con.execute('''
            WITH x := <str>random()
            SELECT (
                WITH name := x ++ <str>random()
                INSERT Person { name := name, tag := name }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_12(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                y := x ++ <str>random(),
            SELECT (
                WITH name := y ++ <str>random()
                INSERT Person { name := name, tag := name }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_volatile_13(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                )
            SELECT (
                INSERT Person {
                    name := x.name ++ "!",
                    tag := x.tag ++ "!",
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_14(self):
        await self.con.execute('''
            WITH
                x := "!",
                y := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
            SELECT (
                INSERT Person {
                    name := x ++ y.name,
                    tag := x ++ y.tag,
                    tag2 := y.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_15(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                y := (
                    WITH name := "!",
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
            SELECT (
                INSERT Person {
                    name := x ++ y.name,
                    tag := x ++ y.tag,
                    tag2 := y.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_16(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                y := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
            SELECT (
                INSERT Person {
                    name := x ++ y.name,
                    tag := x ++ y.tag,
                    tag2 := y.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_17(self):
        await self.con.execute('''
            WITH
                x := "!",
                y := (
                    WITH name := x ++ <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := x }
                ),
            SELECT (
                INSERT Person {
                    name := y.name ++ "!",
                    tag := y.tag ++ "!",
                    tag2 := y.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_18(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                y := (
                    WITH name := x ++ "!",
                    INSERT Person { name := name, tag := name, tag2 := x }
                ),
            SELECT (
                INSERT Person {
                    name := y.name ++ "!",
                    tag := y.tag ++ "!",
                    tag2 := y.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_19(self):
        await self.con.execute('''
            WITH
                x := <str>random(),
                y := (
                    WITH name := x ++ <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := x }
                ),
            SELECT (
                INSERT Person {
                    name := y.name ++ "!",
                    tag := y.tag ++ "!",
                    tag2 := y.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_20(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := "!",
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
                y := <str>random(),
            SELECT (
                INSERT Person {
                    name := x.name ++ y,
                    tag := x.tag ++ y,
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_21(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
                y := "!",
            SELECT (
                INSERT Person {
                    name := x.name ++ y,
                    tag := x.tag ++ y,
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_22(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
                y := <str>random(),
            SELECT (
                INSERT Person {
                    name := x.name ++ y,
                    tag := x.tag ++ y,
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_23(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := "!",
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
                y := x.name ++ <str>random(),
            SELECT (
                INSERT Person {
                    name := y,
                    tag := y,
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_24(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
                y := x.name ++ "!",
            SELECT (
                INSERT Person {
                    name := y,
                    tag := y,
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_25(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person { name := name, tag := name, tag2 := name }
                ),
                y := x.name ++ <str>random(),
            SELECT (
                INSERT Person {
                    name := y,
                    tag := y,
                    tag2 := x.tag2,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_26(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person {
                        name := name,
                        tag := name,
                        tag2 := name,
                    }
                ),
                y := (
                    WITH r := <str>random(),
                    INSERT Person {
                        name := x.name ++ r,
                        tag := x.tag ++ r,
                        tag2 := x.tag,
                    }
                ),
            SELECT (
                WITH r := <str>random(),
                INSERT Person {
                    name := y.name ++ r,
                    tag := y.name ++ r,
                    tag2 := y.tag ++ r,
                }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [3],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [2],
        )

    async def test_edgeql_insert_volatile_27(self):
        await self.con.execute('''
            WITH x := "!"
            INSERT Person {
                name := x,
                tag := x,
                note := (
                    WITH y := <str>random()
                    insert Note { name := y, note := y }
                )
            };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'WITH N := (Note {ok := .name = .note}) SELECT all(N.ok)',
            [True],
        )

    async def test_edgeql_insert_volatile_28(self):
        await self.con.execute('''
            WITH x := <str>random(),
            INSERT Person {
                name := x,
                tag := x,
                note := (
                    WITH y := <str>random()
                    insert Note { name := y, note := y }
                )
            };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'WITH N := (Note {ok := .name = .note}) SELECT all(N.ok)',
            [True],
        )

    async def test_edgeql_insert_volatile_29(self):
        await self.con.execute('''
            WITH x := "!",
            INSERT Person {
                name := x,
                tag := x,
                note := (
                    WITH y := x ++ <str>random()
                    insert Note { name := y, note := y }
                )
            };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'WITH N := (Note {ok := .name = .note}) SELECT all(N.ok)',
            [True],
        )

    async def test_edgeql_insert_volatile_30(self):
        await self.con.execute('''
            WITH x := <str>random(),
            INSERT Person {
                name := x,
                tag := x,
                note := (
                    WITH y := x ++ "!"
                    insert Note { name := y, note := y }
                )
            };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'WITH N := (Note {ok := .name = .note}) SELECT all(N.ok)',
            [True],
        )

    async def test_edgeql_insert_volatile_31(self):
        await self.con.execute('''
            WITH x := <str>random(),
            INSERT Person {
                name := x,
                tag := x,
                note := (
                    WITH y := x ++ <str>random()
                    insert Note { name := y, note := y }
                )
            };
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'WITH N := (Note {ok := .name = .note}) SELECT all(N.ok)',
            [True],
        )

    async def test_edgeql_insert_volatile_32(self):
        await self.con.execute('''
            FOR name in {<str>random(), <str>random()}
            UNION (INSERT Person { name := name, tag := name });
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )

    async def test_edgeql_insert_volatile_33(self):
        await self.con.execute('''
            WITH x := "!"
            FOR y in {<str>random(), <str>random()}
            UNION (
                WITH name := x ++ y
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_34(self):
        await self.con.execute('''
            WITH x := <str>random()
            FOR y in {"A", "B"}
            UNION (
                WITH name := x ++ y
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_35(self):
        await self.con.execute('''
            WITH x := <str>random()
            FOR y in {<str>random(), <str>random()}
            UNION (
                WITH name := x ++ y
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_36(self):
        await self.con.execute('''
            WITH x := "!"
            FOR name in {x ++ <str>random(), x ++ <str>random()}
            UNION (
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_37(self):
        await self.con.execute('''
            WITH x := <str>random()
            FOR name in {x ++ "A", x ++ "B"}
            UNION (
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_38(self):
        await self.con.execute('''
            WITH x := <str>random()
            FOR name in {x ++ <str>random(), x ++ <str>random()}
            UNION (
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_volatile_39(self):
        await self.con.execute('''
            FOR x in {"A", "B"}
            UNION (
                WITH name := x ++ <str>random()
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [2],
        )

    async def test_edgeql_insert_volatile_40(self):
        await self.con.execute('''
            FOR x in {<str>random(), <str>random()}
            UNION (
                WITH name := x ++ "!"
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [2],
        )

    async def test_edgeql_insert_volatile_41(self):
        await self.con.execute('''
            FOR x in {<str>random(), <str>random()}
            UNION (
                WITH name := x ++ <str>random()
                INSERT Person { name := name, tag := name, tag2 := x }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [2],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [2],
        )

    async def test_edgeql_insert_volatile_42(self):
        await self.con.execute('''
            WITH
                x := (
                    WITH name := <str>random(),
                    INSERT Person {
                        name := name,
                        tag := name,
                        tag2 := name,
                    }
                )
            FOR y in {<str>random(), <str>random()}
            UNION (
                WITH name := x.name ++ y
                INSERT Person { name := name, tag := name, tag2 := x.tag2 }
            );
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [3],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.tag2))',
            [1],
        )

    async def test_edgeql_insert_with_freeobject_01(self):
        await self.con.execute('''
            WITH free := { name := "asdf" },
            SELECT (INSERT Person { name := free.name });
        ''')

        await self.assert_query_result(
            'SELECT Person.name = "asdf"',
            [True],
        )

    async def test_edgeql_insert_with_freeobject_02(self):
        await self.con.execute('''
            WITH free := { name := <str>random() },
            SELECT (INSERT Person { name := free.name, tag := free.name });
        ''')

        await self.assert_query_result(
            'WITH P := (Person {ok := .name = .tag}) SELECT all(P.ok)',
            [True],
        )
        await self.assert_query_result(
            'SELECT count(distinct(Person.name))',
            [1],
        )

    async def test_edgeql_insert_multi_exclusive_01(self):
        await self.con.execute('''
            INSERT Person { name := "asdf", multi_prop := "a" };
        ''')

        await self.con.execute('''
            DELETE Person;
        ''')

        await self.con.execute('''
            INSERT Person { name := "asdf", multi_prop := "a" };
        ''')

    @tb.needs_factoring_weakly
    async def test_edgeql_insert_enumerate_01(self):
        await self.assert_query_result(
            r"""
                WITH
                     F := (INSERT Subordinate {name := "!"}),
                     B := (INSERT Subordinate {name := "??"}),
                     Z := enumerate((F, B)),
                SELECT (Z.0, Z.1.0, Z.1.1);
            """,
            [
                [0, {}, {}],
            ]
        )

    async def test_edgeql_insert_nested_and_with_01(self):
        await self.assert_query_result(
            r"""
                WITH
                    New := (
                        INSERT Person {
                            name := "test",
                            notes := (INSERT Note { name := "test" })
                         }
                    ),
                SELECT (
                    INSERT Person2a {
                        first := New.name, last := "!", bff := New,
                    }
                ) {
                    first,
                    bff: {
                        name,
                        notes: { name }
                    }
                };
            """,
            [
                {
                    "first": "test",
                    "bff": {
                        "name": "test",
                        "notes": [{"name": "test"}]
                    },
                }
            ]

        )

    async def test_edgeql_insert_specified_type(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "cannot assign to link '__type__'"):
            await self.con.execute('''
                INSERT Person {
                    __type__ := (introspect Object),
                    name := "test",
                 }
            ''')

    async def test_edgeql_insert_explicit_id_00(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "cannot assign to property 'id'"):
            await self.con.execute('''
                INSERT Person {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test",
                 }
            ''')

    async def test_edgeql_insert_explicit_id_01(self):
        await self.con.execute('''
            configure session set allow_user_specified_id := true
        ''')

        await self.con.execute('''
            INSERT Person {
                id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                name := "test",
             }
        ''')

        await self.assert_query_result(
            r'''
                SELECT Person
            ''',
            [
                {'id': 'ffffffff-ffff-ffff-ffff-ffffffffffff'}
            ]
        )

    async def test_edgeql_insert_explicit_id_02(self):
        await self.con.execute('''
            configure session set allow_user_specified_id := true
        ''')

        await self.con.execute('''
            INSERT Person {
                id := <uuid>to_json('"ffffffff-ffff-ffff-ffff-ffffffffffff"'),
                name := "test",
             }
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                INSERT Person {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test2",
                 }
            ''')

    async def test_edgeql_insert_explicit_id_03(self):
        await self.con.execute('''
            configure session set allow_user_specified_id := true
        ''')

        await self.con.execute('''
            INSERT Person {
                id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                name := "test",
             }
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                INSERT DerivedPerson {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test2",
                 }
            ''')

    async def test_edgeql_insert_explicit_id_04(self):
        await self.con.execute('''
            configure session set allow_user_specified_id := true
        ''')

        await self.con.execute('''
            create required global break -> bool { set default := false; };
            create type X {
                create access policy yes allow all using (true);
                create access policy no deny select using (global break);
            };
            create type Y;
        ''')
        await self.con.query('''
            insert X {
                id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff'
            };
        ''')
        await self.con.execute('''
            set global break := true
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.query('''
                insert Y {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff'
                };
            ''')

    async def test_edgeql_insert_explicit_id_05(self):
        await self.con.execute('''
            configure session set allow_user_specified_id := true
        ''')

        await self.con.execute('''
            INSERT Person {
                id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                name := "test",
             }
        ''')

        await self.assert_query_result(
            r'''
                INSERT Person {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test",
                 } UNLESS CONFLICT
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                INSERT Person {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test",
                 } UNLESS CONFLICT ON (.id)
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                INSERT Note {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test",
                 } UNLESS CONFLICT
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                INSERT Note {
                    id := <uuid>'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    name := "test",
                 } UNLESS CONFLICT ON (.id)
            ''',
            []
        )

    async def test_edgeql_insert_explicit_id_06(self):
        await self.con.execute('''
            configure session set allow_user_specified_id := true
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            "missing value for required property"
        ):
            await self.con.execute(r'''
                INSERT Person {
                    id := <optional uuid>{},
                    name := "test",
                }
            ''')

    async def test_edgeql_insert_optional_cast_01(self):
        await self.assert_query_result(
            r'''
                insert CollectionTest {
                    str_array := <array<str>>to_json('null')
                };
            ''',
            [{}],
        )

    async def test_edgeql_insert_except_constraint_01(self):
        # Test basic behavior of a constraint using except
        await self.con.execute('''
            insert ExceptTest { name := "foo" };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                insert ExceptTest { name := "foo" };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                insert ExceptTest { name := "foo", deleted := false };
            ''')

        await self.con.execute('''
            insert ExceptTest { name := "foo", deleted := true };
        ''')

        await self.con.execute('''
            insert ExceptTest { name := "bar", deleted := true };
        ''')

        await self.con.execute('''
            insert ExceptTest { name := "bar", deleted := true };
        ''')

        await self.con.execute('''
            insert ExceptTest { name := "bar" };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                insert ExceptTest { name := "bar" };
            ''')

        await self.con.execute('''
            insert ExceptTest { name := "baz" };
        ''')

        await self.con.execute('''
            insert ExceptTestSub { name := "bar", deleted := true };
        ''')

        # Now we are going to drop the constraint and then add it back in,
        # nothing should error

        await self.con.execute('''
            alter type ExceptTest {
                drop constraint exclusive on (.name) except (.deleted);
            };
        ''')
        await self.con.execute('''
            alter type ExceptTest {
                create constraint exclusive on (.name) except (.deleted);
            };
        ''')

        # Now drop it, and add something that *will* break, and recreate it
        await self.con.execute('''
            alter type ExceptTest {
                drop constraint exclusive on (.name) except (.deleted);
            };
        ''')
        await self.con.execute('''
            insert ExceptTestSub { name := "baz" };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                alter type ExceptTest {
                    create constraint exclusive on (.name) except (.deleted);
                };
            ''')

    async def test_edgeql_insert_except_constraint_02(self):
        # Test some self conflict insert cases
        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                select {
                    (insert ExceptTest { name := "foo" }),
                    (insert ExceptTestSub { name := "foo" }),
                };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                select {
                    (insert ExceptTest { name := "foo" }),
                    (insert ExceptTestSub { name := "foo", deleted := false }),
                };
            ''')

        await self.con.execute('''
            select {
                (insert ExceptTest { name := "foo" }),
                (insert ExceptTestSub { name := "foo", deleted := true }),
            };
        ''')

    async def test_edgeql_insert_except_constraint_03(self):
        # Test some self conflict update cases
        await self.con.execute('''
            insert ExceptTest { name := "a" };
            insert ExceptTestSub { name := "b" };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                update ExceptTest set { name := "foo" };
            ''')

        await self.con.execute('''
            update ExceptTest set { name := "foo", deleted := true };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                update ExceptTest set { deleted := false };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                "violates exclusivity constraint"):
            await self.con.execute('''
                update ExceptTest set { deleted := {} };
            ''')

    async def test_edgeql_insert_except_constraint_04(self):
        # exclusive constraints with EXCEPT clauses can't narrow cardinality
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "possibly more than one element returned"):
            await self.con.query('''
                select { single x := (select ExceptTest filter .name = 'foo') }
            ''')

    async def test_edgeql_insert_in_free_object_01(self):
        await self.assert_query_result(
            r"""
                select {
                    obj := (
                        INSERT InsertTest {
                            name := 'insert simple 01',
                            l2 := 0,
                        }
                     )
                }
            """,
            [{"obj": {"id": str}}],
        )

        await self.assert_query_result(
            r"""
                select {
                    obj := (
                        INSERT InsertTest {
                            name := 'insert simple 02',
                            l2 := 0,
                        }
                     ) { name, l2 }
                }
            """,
            [{"obj": {'name': "insert simple 02", 'l2': 0}}],
        )

        await self.assert_query_result(
            r"""
                select {
                    objs := (
                        for name in {'one', 'two'} union (
                            INSERT InsertTest {
                                name := name, l2 := 0,
                            }
                        )
                    )
                }
            """,
            [{"objs": [{"id": str}, {"id": str}]}],
        )

    async def test_edgeql_insert_in_free_object_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "mutations are invalid in a shape's computed expression"):
            await self.con.query('''
                select { foo := 1 } {
                    obj := (
                        INSERT InsertTest {
                            name := 'insert simple 02',
                            l2 := 0,
                        }
                     ) { name, l2 }
                }
            ''')
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "mutations are invalid in a shape's computed expression"):
            await self.con.query('''
                select (for x in {1,2} union FreeObject) {
                    obj := (
                        INSERT InsertTest {
                            name := 'insert simple 01',
                            l2 := 0,
                        }
                     )
                };
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "mutations are invalid in a shape's computed expression"):
            await self.con.query('''
                with X := {
                    obj := (
                        INSERT InsertTest {
                            name := 'insert simple 01',
                            l2 := 0,
                        }
                     )
                }, select X;
            ''')

    async def test_edgeql_insert_rebind_with_typenames_01(self):
        await self.assert_query_result(
            '''
            with
              update1 := (insert InsertTest {l2:=1}),
            select (select update1);
            ''',
            [{'id': str}],
            always_typenames=True,
        )

        await self.assert_query_result(
            '''
            with
              update1 := (insert InsertTest {l2:=1}),
            select {update1};
            ''',
            [{'id': str}],
            always_typenames=True,
        )

    async def test_edgeql_insert_pointless_shape_elements_01(self):
        await self.con.execute('''
            insert Person {
                name := "test",
                notes := (select Note { foo := 0 })
            };
        ''')

    async def test_edgeql_insert_bogus_correlation_typenames(self):
        # This was being rejected with a correlation error
        query = r'''
            for l2 in <int64>{} union (
                with
                  subs := (select Subordinate filter .name = '')
                insert InsertTest {
                  subordinates := subs,
                  l2 := l2,
                }
            );
        '''

        await self.con._fetchall(
            query, __typenames__=True,
        )

    async def test_edgeql_insert_single_linkprop(self):
        await self.con.execute('''
            insert Subordinate { name := "1" };
            insert Subordinate { name := "2" };
        ''')

        for _ in range(10):
            await self.con.execute('''
                insert InsertTest {
                    l2 := -1,
                    sub := (select Subordinate { @note := "!" }
                             order by random() limit 1)
                };
            ''')

        await self.assert_query_result(
            '''
            select InsertTest { sub: {name, @note} };
            ''',
            [{"sub": {"name": str, "@note": "!"}}] * 10,
        )

        await self.con.execute('''
            update InsertTest set {
                sub := (select Subordinate { @note := "!" }
                         order by random() limit 1)
            };
        ''')

        await self.assert_query_result(
            '''
            select InsertTest { sub: {name, @note} };
            ''',
            [{"sub": {"name": str, "@note": "!"}}] * 10,
        )

    async def test_edgeql_insert_conditional_01(self):
        await self.assert_query_result(
            '''
            select if <bool>$0 then (
                insert InsertTest { l2 := 2 }
            ) else (
                insert DerivedTest { l2 := 200 }
            )
            ''',
            [{}],
            variables=(True,)
        )

        await self.assert_query_result(
            '''
            select InsertTest { l2, tname := .__type__.name }
            ''',
            [
                {"l2": 2, "tname": "default::InsertTest"},
            ],
        )

        await self.assert_query_result(
            '''
            select if <bool>$0 then (
                insert InsertTest { l2 := 2 }
            ) else (
                insert DerivedTest { l2 := 200 }
            )
            ''',
            [{}],
            variables=(False,)
        )

        await self.assert_query_result(
            '''
            select InsertTest { l2, tname := .__type__.name } order by  .l2
            ''',
            [
                {"l2": 2, "tname": "default::InsertTest"},
                {"l2": 200, "tname": "default::DerivedTest"},
            ],
        )

        await self.assert_query_result(
            '''
            select if array_unpack(<array<bool>>$0) then (
                insert InsertTest { l2 := 2 }
            ) else (
                insert DerivedTest { l2 := 200 }
            )
            ''',
            [{}, {}],
            variables=([True, False],)
        )

        await self.assert_query_result(
            '''
            with go := <bool>$0
            select if go then (
                insert InsertTest { l2 := 100 }
            ) else {}
            ''',
            [{}],
            variables=(True,)
        )

        await self.assert_query_result(
            '''
            select InsertTest { l2, tname := .__type__.name } order by  .l2
            ''',
            [
                {"l2": 2, "tname": "default::InsertTest"},
                {"l2": 2, "tname": "default::InsertTest"},
                {"l2": 100, "tname": "default::InsertTest"},
                {"l2": 200, "tname": "default::DerivedTest"},
                {"l2": 200, "tname": "default::DerivedTest"},
            ],
        )

    async def test_edgeql_insert_conditional_02(self):
        ctxmgr = (
            contextlib.nullcontext() if self.NO_FACTOR
            else self.assertRaisesRegexTx(
                edgedb.errors.QueryError,
                "cannot reference correlated set",
            )
        )
        async with ctxmgr:
            await self.con.execute('''
                select ((if ExceptTest.deleted then (
                    insert InsertTest { l2 := 2 }
                ) else (
                    insert DerivedTest { l2 := 200 }
                )), (select ExceptTest.deleted limit 1));
            ''')

    async def test_edgeql_insert_conditional_03(self):
        await self.assert_query_result(
            '''
            select (for n in array_unpack(<array<int64>>$0) union (
                if n % 2 = 0 then
                  (insert InsertTest { l2 := n }) else {}
            )) { l2 } order by .l2;
            ''',
            [{'l2': 2}, {'l2': 4}],
            variables=([1, 2, 3, 4, 5],),
        )

        await self.assert_query_result(
            '''
            select InsertTest { l2 } order by .l2;
            ''',
            [{'l2': 2}, {'l2': 4}],
        )

    async def test_edgeql_insert_coalesce_01(self):
        await self.assert_query_result(
            '''
            select (select InsertTest filter .l2 = 2) ??
              (insert InsertTest { l2 := 2 });
            ''',
            [{}],
        )

        await self.assert_query_result(
            '''
            select (select InsertTest filter .l2 = 2) ??
              (insert InsertTest { l2 := 2 });
            ''',
            [{}],
        )

        await self.assert_query_result(
            '''
            select count((delete InsertTest))
            ''',
            [1],
        )

    async def test_edgeql_insert_coalesce_02(self):
        await self.assert_query_result(
            '''
            select ((select InsertTest filter .l2 = 2), true) ??
              ((insert InsertTest { l2 := 2 }), false);
            ''',
            [({}, False)],
        )

        await self.assert_query_result(
            '''
            select ((select InsertTest filter .l2 = 2), true) ??
              ((insert InsertTest { l2 := 2 }), false);
            ''',
            [({}, True)],
        )

    async def test_edgeql_insert_coalesce_03(self):
        await self.assert_query_result(
            '''
            select (
                (update InsertTest filter .l2 = 2 set { name := "!" }) ??
                  (insert InsertTest { l2 := 2, name := "?" })
            ) { l2, name }
            ''',
            [{'l2': 2, 'name': "?"}],
        )

        await self.assert_query_result(
            '''
            select (
                (update InsertTest filter .l2 = 2 set { name := "!" }) ??
                  (insert InsertTest { l2 := 2, name := "?" })
            ) { l2, name }
            ''',
            [{'l2': 2, 'name': "!"}],
        )

        await self.assert_query_result(
            '''
            select InsertTest { l2, name }
            ''',
            [{'l2': 2, 'name': "!"}],
        )

        await self.assert_query_result(
            '''
            select count((delete InsertTest))
            ''',
            [1],
        )

    async def test_edgeql_insert_coalesce_04(self):
        Q = '''
        select (for n in array_unpack(<array<int64>>$0) union (
            (update InsertTest filter .l2 = n set { name := "!" }) ??
              (insert InsertTest { l2 := n, name := "?" })
        )) { l2, name, new := .id not in InsertTest.id } order by .l2
        '''

        await self.assert_query_result(
            Q,
            [
                {'l2': 1, 'name': "?", 'new': True},
                {'l2': 2, 'name': "?", 'new': True},
            ],
            variables=([1, 2],)
        )

        await self.assert_query_result(
            Q,
            [
                {'l2': 0, 'name': "?", 'new': True},
                {'l2': 1, 'name': "!", 'new': False},
                {'l2': 2, 'name': "!", 'new': False},
                {'l2': 3, 'name': "?", 'new': True},
            ],
            variables=([0, 1, 2, 3],)
        )

    async def test_edgeql_insert_coalesce_05(self):
        await self.con.execute('''
            insert Subordinate { name := "foo" };
        ''')

        Q = '''
        for sub in Subordinate union (
          (select Note filter .subject = sub) ??
          (insert Note { name := "", subject := sub })
        );
        '''

        await self.assert_query_result(
            Q,
            [{}],
        )
        await self.assert_query_result(
            Q,
            [{}],
        )
        await self.assert_query_result(
            'select count(Note)',
            [1],
        )

        await self.con.execute('''
            insert Subordinate { name := "bar" };
            insert Subordinate { name := "baz" };
        ''')

        await self.assert_query_result(
            Q,
            [{}] * 3,
        )
        await self.assert_query_result(
            Q,
            [{}] * 3,
        )
        await self.assert_query_result(
            'select count(Note)',
            [3],
        )

    async def test_edgeql_insert_coalesce_nulls_01(self):
        Q = '''
        with name := 'name',
             new := (
               (select Person filter .name = name) ??
               (insert Person { name := name})
             ),
        select { new := new }
        '''

        await self.assert_query_result(
            Q,
            [{'new': {}}],
        )

        await self.assert_query_result(
            Q,
            [{'new': {}}],
        )

    async def test_edgeql_insert_coalesce_nulls_02(self):
        Q = '''
        with name := 'name',
             new := (
               (select Person filter .name = name) ??
               (insert Person { name := name})
             ),
        select (
          insert Note { name := '??', subject := new }
        ) { subject }
        '''

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

    async def test_edgeql_insert_coalesce_nulls_03(self):
        await self.con.execute('''
            insert Note { name := 'x' }
        ''')

        Q = '''
        with name := 'name',
             new := (
               (select Person filter .name = name) ??
               (insert Person { name := name})
             ),
        select (update Note filter .name = 'x' set { subject := new })
               { subject }
        '''

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

    async def test_edgeql_insert_coalesce_nulls_04(self):
        Q = '''
        with name := 'name',
             new := (
               (select Note filter .name = name) ??
               (insert Note { name := name })
             ),
        select { new := assert_single(new) }
        '''

        await self.assert_query_result(
            Q,
            [{'new': {}}],
        )

        await self.assert_query_result(
            Q,
            [{'new': {}}],
        )

    async def test_edgeql_insert_coalesce_nulls_05(self):
        Q = '''
        with name := 'name',
             new := (
               (select Note filter .name = name) ??
               (insert Note { name := name})
             ),
        select (
          insert Note { name := '??', subject := assert_single(new) }
        ) { subject }
        '''

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

    async def test_edgeql_insert_coalesce_nulls_06(self):
        await self.con.execute('''
            insert Note { name := 'x' }
        ''')

        Q = '''
        with name := 'name',
             new := (
               (select Note filter .name = name) ??
               (insert Note { name := name })
             ),
        select (update Note filter .name = 'x' set {
                  subject := assert_single(new) })
               { subject }
        '''

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

        await self.assert_query_result(
            Q,
            [{'subject': {}}],
        )

    async def test_edgeql_insert_coalesce_nulls_08(self):
        Q = '''
        with l2 := 420,
        select (
          if <bool>$0 then (
            (delete DerivedTest filter .l2 = l2)
            ??
            (insert DerivedTest {l2 := l2})
          ) else (
            (update Note filter .name = <str>l2 set { note := "note" })
            ??
            (insert Note {name := <str>l2})
          )
        );
        '''

        await self.assert_query_result(
            Q,
            [{}],
            variables=(True,),
        )
        await self.assert_query_result(
            Q,
            [{}],
            variables=(True,),
        )
        await self.assert_query_result(
            'select DerivedTest',
            [],
        )

        await self.assert_query_result(
            Q,
            [{}],
            variables=(False,),
        )
        await self.assert_query_result(
            Q,
            [{}],
            variables=(False,),
        )
        await self.assert_query_result(
            'select Note { note }',
            [{'note': "note"}],
        )

    async def test_edgeql_insert_empty_array_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "expression returns value of indeterminate type"
        ):
            await self.con.execute("""
                insert InsertTest {
                    name := [],
                    l2 := 0,
                };
            """)

    async def test_edgeql_insert_empty_array_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidPropertyTargetError,
            r"invalid target for property 'name' "
            r"of object type 'default::InsertTest': 'array<std::str>' "
            r"\(expecting 'std::str'\)"
        ):
            await self.con.execute("""
                insert InsertTest {
                    name := ['a'] ++ [],
                    l2 := 0,
                };
            """)

    async def test_edgeql_insert_empty_array_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidPropertyTargetError,
            r"invalid target for property 'name' "
            r"of object type 'default::InsertTest': 'std::int64' "
            r"\(expecting 'std::str'\)"
        ):
            await self.con.execute("""
                insert InsertTest {
                    name := array_unpack([1] ++ []),
                    l2 := 0,
                };
            """)

    async def test_edgeql_insert_empty_array_04(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "expression returns value of indeterminate type"
        ):
            await self.con.execute("""
                insert InsertTest {
                    l2 := 0,
                    subordinates := (
                        select Subordinate {
                            @comment := []
                        }
                    )
                };
            """)

    async def test_edgeql_insert_empty_array_05(self):
        await self.assert_query_result("""
            insert Subordinate { name := 'hi' };
            select ( insert InsertTest {
                l2 := 0,
                subordinates := (
                    select Subordinate {
                        @comment := array_join(['a'] ++ [], '')
                    }
                )
            }) { l2, subordinates: { name, @comment } };
            """,
            [{'l2': 0, 'subordinates': [{'name': 'hi', '@comment': 'a'}]}],
        )
