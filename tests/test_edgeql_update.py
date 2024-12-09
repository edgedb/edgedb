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


import json
import os.path
import uuid

import edgedb

from edb.testbase import server as tb
from edb.tools import test


class TestUpdate(tb.QueryTestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'updates.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'updates.edgeql')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.loop.run_until_complete(cls._setup_objects())

    @classmethod
    async def _setup_objects(cls):
        cls.original = await cls.con.query_json(r"""
            SELECT UpdateTest {
                id,
                name,
                comment,
                status: {
                    name
                }
            } ORDER BY .name;
        """)
        # this is used to validate what was updated and was untouched
        cls.original = json.loads(cls.original)

    async def test_edgeql_update_simple_01(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                # bad name doesn't exist, so no update is expected
                FILTER .name = 'bad name'
                SET {
                    status := (SELECT Status FILTER Status.name = 'Closed')
                };
            """,
            []
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    id,
                    name,
                    comment,
                    status: {
                        name
                    }
                } ORDER BY .name;
            """,
            self.original,
        )

    async def test_edgeql_update_simple_02(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    name := 'update-test1-updated',
                    status := (SELECT Status FILTER Status.name = 'Closed')
                };
            """,
            [{}]
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    id,
                    name,
                    comment,
                    status: {
                        name
                    }
                } ORDER BY .name;
            """,
            [
                {
                    'id': orig1['id'],
                    'name': 'update-test1-updated',
                    'status': {
                        'name': 'Closed'
                    }
                },
                orig2,
                orig3,
            ]
        )

    async def test_edgeql_update_simple_03(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test2'
                SET {
                    comment := 'updated ' ++ UpdateTest.comment
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    id,
                    name,
                    comment,
                } ORDER BY .name;
            """,
            [
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
            ]
        )

    async def test_edgeql_update_simple_04(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                SET {
                    comment := UpdateTest.comment ++ "!",
                    status := (SELECT Status FILTER Status.name = 'Closed')
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    id,
                    name,
                    comment,
                    status: {
                        name
                    }
                } ORDER BY .name;
            """,
            [
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
            ]
        )

    async def test_edgeql_update_simple_05(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    name := 'update-test1-updated',
                    status := assert_single((
                        SELECT
                          Status
                        FILTER
                          .name = 'Closed' or .name = 'Foo'
                    ))
                };
            """,
            [{}]
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    id,
                    name,
                    comment,
                    status: {
                        name
                    }
                } ORDER BY .name;
            """,
            [
                {
                    'id': orig1['id'],
                    'name': 'update-test1-updated',
                    'status': {
                        'name': 'Closed'
                    }
                },
                orig2,
                orig3,
            ]
        )

    async def test_edgeql_update_returning_01(self):
        _orig1, orig2, _orig3 = self.original

        await self.assert_query_result(
            r"""
                SELECT (
                    UPDATE UpdateTest
                    FILTER UpdateTest.name = 'update-test2'
                    SET {
                        comment := 'updated ' ++ UpdateTest.comment
                    }
                ) {
                    id,
                    name,
                    comment,
                };
            """,
            [{
                'id': orig2['id'],
                'name': 'update-test2',
                'comment': 'updated second',
            }]
        )

    async def test_edgeql_update_returning_02(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                SELECT (
                    UPDATE UpdateTest
                    SET {
                        comment := UpdateTest.comment ++ "!",
                        status := (SELECT Status FILTER Status.name = 'Closed')
                    }
                ) {
                    id,
                    name,
                    comment,
                    status: {
                        name
                    }
                };
            """,
            [
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
            ],
            sort=lambda x: x['name']
        )

    async def test_edgeql_update_returning_03(self):
        _orig1, _orig2, _orig3 = self.original

        await self.assert_query_result(
            r"""
                WITH
                    U := (
                        UPDATE UpdateTest
                        FILTER UpdateTest.name = 'update-test2'
                        SET {
                            comment := 'updated ' ++ UpdateTest.comment
                        }
                    )
                SELECT Status{name}
                FILTER Status = U.status
                ORDER BY Status.name;
            """,
            [{'name': 'Open'}],
        )

    async def test_edgeql_update_returning_04(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                WITH
                    Q := (
                        UPDATE UpdateTest
                        SET {
                            comment := UpdateTest.comment ++ "!",
                            status := (SELECT
                                Status FILTER Status.name = 'Closed')
                        }
                    )

                SELECT
                    Q {
                        id,
                        name,
                        comment,
                        status: {
                            name
                        }
                    }
                ORDER BY
                    Q.name;
            """,
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
        )

    async def test_edgeql_update_returning_05(self):
        # test that plain INSERT and UPDATE return objects they have
        # manipulated
        try:
            data = []
            data.append(await self.con.query_single(r"""
                INSERT UpdateTest {
                    name := 'ret5.1'
                };
            """))
            data.append(await self.con.query_single(r"""
                INSERT UpdateTest {
                    name := 'ret5.2'
                };
            """))
            data = [str(o.id) for o in data]

            await self.assert_query_result(
                r"""
                    SELECT UpdateTest {
                        id,
                        name
                    }
                    FILTER .name LIKE '%ret5._'
                    ORDER BY .name;
                """,
                [
                    {
                        'id': data[0],
                        'name': 'ret5.1',
                    },
                    {
                        'id': data[1],
                        'name': 'ret5.2',
                    }
                ],
            )

            await self.assert_query_result(
                r"""
                    UPDATE UpdateTest
                    FILTER UpdateTest.name LIKE '%ret5._'
                    SET {
                        name := 'new ' ++ UpdateTest.name
                    };
                """,
                [{'id': data_id} for data_id in sorted(data)],
                sort=lambda x: x['id']
            )

            await self.assert_query_result(
                r"""
                    SELECT UpdateTest {
                        id,
                        name
                    }
                    FILTER .name LIKE '%ret5._'
                    ORDER BY .name;
                """,
                [
                    {
                        'id': data[0],
                        'name': 'new ret5.1',
                    },
                    {
                        'id': data[1],
                        'name': 'new ret5.2',
                    }
                ],
            )

            objs = await self.con._fetchall(
                r"""
                    UPDATE UpdateTest
                    FILTER UpdateTest.name LIKE '%ret5._'
                    SET {
                        name := 'new ' ++ UpdateTest.name
                    };
                """,
                __typenames__=True,
                __typeids__=True
            )
            self.assertTrue(hasattr(objs[0], '__tid__'))
            self.assertEqual(objs[0].__tname__, 'default::UpdateTest')

        finally:
            await self.con.execute(r"""
                DELETE (
                    SELECT UpdateTest
                    FILTER .name LIKE '%ret5._'
                );
            """)

    async def test_edgeql_update_generic_01(self):
        status = await self.con.query_single(r"""
            SELECT Status{id}
            FILTER Status.name = 'Open'
            LIMIT 1;
        """)
        status = str(status.id)

        updated = await self.con.query(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test3'
                SET {
                    status := (
                        SELECT Status
                        FILTER Status.id = <uuid>$status
                    )
                };
            """,
            status=status
        )
        self.assertGreater(len(updated), 0)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    status: {
                        name
                    }
                } FILTER UpdateTest.name = 'update-test3';
            """,
            [
                {
                    'name': 'update-test3',
                    'status': {
                        'name': 'Open',
                    },
                },
            ]
        )

    async def test_edgeql_update_bad_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'free objects cannot be updated',
        ):
            await self.con.execute('''\
                WITH foo := {bar := 1}
                UPDATE foo SET { bar := 2 };
            ''')

    async def test_edgeql_update_bad_02(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r'update standard library type',
        ):
            await self.con.execute('''\
                UPDATE schema::Migration SET { script := 'foo'};
            ''')

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_update_filter_01(self):
        await self.assert_query_result(
            r"""
                UPDATE (SELECT UpdateTest)
                # this FILTER is trivial because UpdateTest is wrapped
                # into a SET OF by SELECT
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    comment := 'bad test'
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest.comment;
            """,
            ['bad test'] * 3,
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_update_filter_02(self):
        await self.assert_query_result(
            r"""
                UPDATE (<UpdateTest>{} ?? UpdateTest)
                # this FILTER is trivial because UpdateTest is wrapped
                # into a SET OF by ??
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    comment := 'bad test'
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest.comment;
            """,
            ['bad test'] * 3,
        )

    async def test_edgeql_update_multiple_01(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    tags := (SELECT Tag)
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    tags: {
                        name
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
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
            ]
        )

    async def test_edgeql_update_multiple_02(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    tags := (SELECT Tag FILTER Tag.name = 'wow')
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    tags: {
                        name
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'tags': [{
                        'name': 'wow',
                    }],
                },
            ]
        )

    async def test_edgeql_update_multiple_03(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    tags := (SELECT Tag FILTER Tag.name IN {'wow', 'fun'})
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    tags: {
                        name
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'tags': [{
                        'name': 'fun',
                    }, {
                        'name': 'wow',
                    }],
                },
            ]
        )

    async def test_edgeql_update_multiple_04(self):
        await self.assert_query_result(
            r"""
                # first add a tag to UpdateTest
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    tags := (
                        SELECT Tag
                        FILTER Tag.name = 'fun'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    tags: {
                        name
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [{
                'name': 'update-test1',
                'tags': [{
                    'name': 'fun',
                }],
            }],
        )

        await self.assert_query_result(
            r"""
                # now add another tag, but keep the existing one, too
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    tags += (
                        SELECT Tag
                        FILTER Tag.name = 'wow'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    tags: {
                        name
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [{
                'name': 'update-test1',
                'tags': [{
                    'name': 'fun',
                }, {
                    'name': 'wow',
                }],
            }],
        )

    async def test_edgeql_update_multiple_05(self):
        await self.assert_query_result(
            r"""
                WITH
                    U2 := UpdateTest
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    related := (SELECT U2 FILTER U2.name != 'update-test1')
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    related: {
                        name
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'related': [{
                        'name': 'update-test2',
                    }, {
                        'name': 'update-test3',
                    }],
                },
            ]
        )

    async def test_edgeql_update_multiple_06(self):
        await self.assert_query_result(
            r"""
                WITH
                    U2 := UpdateTest
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_tests := (
                        SELECT U2 FILTER U2.name != 'update-test1'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_tests: {
                        name,
                        @note
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
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
            ]
        )

    async def test_edgeql_update_multiple_07(self):
        await self.assert_query_result(
            r"""
                WITH
                    U2 := UpdateTest
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_tests := (
                        SELECT U2 {
                            @note := 'note' ++ U2.name[-1]
                        } FILTER U2.name != 'update-test1'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_tests: {
                        name,
                        @note
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
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
            ]
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_update_multiple_08(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-8-1',
            };

            INSERT UpdateTestSubType {
                name := 'update-test-8-2',
            };

            INSERT UpdateTestSubSubType {
                name := 'update-test-8-3',
            };
        """)

        await self.assert_query_result(
            r"""
                # make tests related to the other 2
                WITH
                    UT := (SELECT UpdateTest
                           FILTER .name LIKE 'update-test-8-%')
                UPDATE UpdateTest
                FILTER .name LIKE 'update-test-8-%'
                SET {
                    related := (SELECT UT FILTER UT != UpdateTest)
                };
            """,
            [
                {'id': uuid.UUID},
                {'id': uuid.UUID},
                {'id': uuid.UUID},
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest{
                    name,
                    related: {name} ORDER BY .name
                }
                FILTER .name LIKE 'update-test-8-%'
                ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-8-1',
                    'related': [
                        {'name': 'update-test-8-2'},
                        {'name': 'update-test-8-3'},
                    ],
                },
                {
                    'name': 'update-test-8-2',
                    'related': [
                        {'name': 'update-test-8-1'},
                        {'name': 'update-test-8-3'},
                    ],
                },
                {
                    'name': 'update-test-8-3',
                    'related': [
                        {'name': 'update-test-8-1'},
                        {'name': 'update-test-8-2'},
                    ],
                },
            ],
        )

        await self.assert_query_result(
            r"""
                # now update related tests based on existing related tests
                WITH
                    UT := (SELECT UpdateTest
                           FILTER .name LIKE 'update-test-8-%')
                UPDATE UpdateTest
                FILTER .name LIKE 'update-test-8-%'
                SET {
                    # since there are 2 tests in each FILTER, != is
                    # guaranteed to be TRUE for at least one of them
                    related := (SELECT UT FILTER UT != UpdateTest.related)
                };
            """,
            [
                {'id': uuid.UUID},
                {'id': uuid.UUID},
                {'id': uuid.UUID},
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest{
                    name,
                    related: {name} ORDER BY .name
                }
                FILTER .name LIKE 'update-test-8-%'
                ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-8-1',
                    'related': [
                        {'name': 'update-test-8-1'},
                        {'name': 'update-test-8-2'},
                        {'name': 'update-test-8-3'},
                    ],
                },
                {
                    'name': 'update-test-8-2',
                    'related': [
                        {'name': 'update-test-8-1'},
                        {'name': 'update-test-8-2'},
                        {'name': 'update-test-8-3'},
                    ],
                },
                {
                    'name': 'update-test-8-3',
                    'related': [
                        {'name': 'update-test-8-1'},
                        {'name': 'update-test-8-2'},
                        {'name': 'update-test-8-3'},
                    ],
                },
            ],
        )

    async def test_edgeql_update_multiple_09(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-9-1',
            };

            INSERT UpdateTest {
                name := 'update-test-9-2',
            };

            INSERT UpdateTest {
                name := 'update-test-9-3',
            };
        """)

        await self.assert_query_result(
            r"""
                # make tests related to the other 2
                WITH
                    UT := (SELECT UpdateTest
                           FILTER .name LIKE 'update-test-9-%')
                UPDATE UpdateTest
                FILTER .name LIKE 'update-test-9-%'
                SET {
                    related := (SELECT UT FILTER UT != UpdateTest)
                };
            """,
            [
                {'id': uuid.UUID},
                {'id': uuid.UUID},
                {'id': uuid.UUID},
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest{
                    name,
                    related: {name} ORDER BY .name
                }
                FILTER .name LIKE 'update-test-9-%'
                ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-9-1',
                    'related': [
                        {'name': 'update-test-9-2'},
                        {'name': 'update-test-9-3'},
                    ],
                },
                {
                    'name': 'update-test-9-2',
                    'related': [
                        {'name': 'update-test-9-1'},
                        {'name': 'update-test-9-3'},
                    ],
                },
                {
                    'name': 'update-test-9-3',
                    'related': [
                        {'name': 'update-test-9-1'},
                        {'name': 'update-test-9-2'},
                    ],
                },
            ],
        )

        await self.assert_query_result(
            r"""
                # now update related tests based on existing related tests
                WITH
                    UT := (SELECT UpdateTest
                           FILTER .name LIKE 'update-test-9-%')
                UPDATE UpdateTest
                FILTER .name LIKE 'update-test-9-%'
                SET {
                    # this should make the related test be the same as parent
                    related := (SELECT UT FILTER UT NOT IN UpdateTest.related)
                };
            """,
            [
                {'id': uuid.UUID},
                {'id': uuid.UUID},
                {'id': uuid.UUID},
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest{
                    name,
                    related: {name} ORDER BY .name
                }
                FILTER .name LIKE 'update-test-9-%'
                ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-9-1',
                    'related': [
                        {'name': 'update-test-9-1'},
                    ],
                },
                {
                    'name': 'update-test-9-2',
                    'related': [
                        {'name': 'update-test-9-2'},
                    ],
                },
                {
                    'name': 'update-test-9-3',
                    'related': [
                        {'name': 'update-test-9-3'},
                    ],
                },
            ],
        )

    async def test_edgeql_update_multiple_10(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-10-1',
            };

            INSERT UpdateTest {
                name := 'update-test-10-2',
            };

            INSERT UpdateTest {
                name := 'update-test-10-3',
            };
        """)

        await self.assert_query_result(
            r"""
                # make each test related to 'update-test-10-1'
                WITH
                    UT := (
                        SELECT UpdateTest FILTER .name = 'update-test-10-1'
                    )
                UPDATE UpdateTest
                FILTER .name LIKE 'update-test-10-%'
                SET {
                    related := UT
                };
            """,
            [
                {'id': uuid.UUID},
                {'id': uuid.UUID},
                {'id': uuid.UUID},
            ],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest{
                    name,
                    related: {name} ORDER BY .name
                }
                FILTER .name LIKE 'update-test-10-%'
                ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-10-1',
                    'related': [
                        {'name': 'update-test-10-1'},
                    ],
                },
                {
                    'name': 'update-test-10-2',
                    'related': [
                        {'name': 'update-test-10-1'},
                    ],
                },
                {
                    'name': 'update-test-10-3',
                    'related': [
                        {'name': 'update-test-10-1'},
                    ],
                },
            ],
        )

        await self.assert_query_result(
            r"""
                # now update related tests
                # there's only one item in the UPDATE set
                UPDATE UpdateTest.related
                FILTER .name LIKE 'update-test-10-%'
                SET {
                    # every test is .<related to 'update-test1'
                    related := UpdateTest.related.<related[IS UpdateTest]
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest{
                    name,
                    related: {name} ORDER BY .name
                }
                FILTER .name LIKE 'update-test-10-%'
                ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-10-1',
                    'related': [
                        {'name': 'update-test-10-1'},
                        {'name': 'update-test-10-2'},
                        {'name': 'update-test-10-3'},
                    ],
                },
                {
                    'name': 'update-test-10-2',
                    'related': [
                        {'name': 'update-test-10-1'},
                    ],
                },
                {
                    'name': 'update-test-10-3',
                    'related': [
                        {'name': 'update-test-10-1'},
                    ],
                },
            ],
        )

    async def test_edgeql_update_props_01(self):
        await self.assert_query_result(
            r"""
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
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight
                    } ORDER BY @weight
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
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
            ]
        )

    async def test_edgeql_update_props_02(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags := (
                        SELECT Tag {@weight := 1} FILTER Tag.name = 'wow')
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight
                    } ORDER BY @weight
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': [{
                        'name': 'wow',
                        '@weight': 1,
                    }],
                },
            ]
        )

    async def test_edgeql_update_props_03(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags := (
                        SELECT Tag {
                            @weight := len(Tag.name) % 2 + 1,
                            @note := Tag.name ++ '!',
                        } FILTER Tag.name IN {'wow', 'boring'}
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight,
                        @note,
                    } ORDER BY @weight
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': [{
                        'name': 'boring',
                        '@weight': 1,
                        '@note': 'boring!',
                    }, {
                        'name': 'wow',
                        '@weight': 2,
                        '@note': 'wow!',
                    }],
                },
            ]
        )

        # Check that reassignment erases the link properties.
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags := .weighted_tags {
                        @weight := len(.name) % 2 + 1,
                    },
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight,
                        @note,
                    } ORDER BY @weight
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': [{
                        'name': 'boring',
                        '@weight': 1,
                        '@note': None,
                    }, {
                        'name': 'wow',
                        '@weight': 2,
                        '@note': None,
                    }],
                },
            ]
        )

    async def test_edgeql_update_props_05(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_status := (
                        SELECT Status {
                            @note := 'Victor'
                        } FILTER Status.name = 'Closed'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_status: {
                        name,
                        @note
                    }
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'annotated_status': {
                        'name': 'Closed',
                        '@note': 'Victor',
                    },
                },
            ]
        )

    async def test_edgeql_update_props_06(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_status := (
                        SELECT Status {
                            @note := 'Victor'
                        } FILTER Status = UpdateTest.status
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_status: {
                        name,
                        @note
                    }
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'annotated_status': {
                        'name': 'Open',
                        '@note': 'Victor',
                    },
                },
            ]
        )

    async def test_edgeql_update_props_07(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_status := (
                        SELECT Status FILTER Status.name = 'Open'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_status: {
                        name,
                        @note
                    }
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'annotated_status': {
                        'name': 'Open',
                        '@note': None,
                    },
                },
            ]
        )

    async def test_edgeql_update_props_08(self):
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_status := (
                        SELECT Status {
                            @note := 'Victor'
                        } FILTER Status.name = 'Open'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                # update again, erasing the 'note' value
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_status := .annotated_status {
                        @note := <str>{}
                    }
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_status: {
                        name,
                        @note
                    }
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'annotated_status': {
                        'name': 'Open',
                        '@note': None,
                    },
                },
            ]
        )

    async def test_edgeql_update_props_09(self):
        # Check that we can update a link property on a specific link.

        # Setup some multi links
        await self.con.execute(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags := (SELECT Tag FILTER .name != 'boring')
                };
            """,
        )

        # Update the @weight for Tag 'wow'
        await self.con.execute(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags += (
                        SELECT .weighted_tags {@weight := 1}
                        FILTER .name = 'wow'
                    )
                };
            """,
        )
        # Update the @weight for Tag 'boring', which should do nothing
        # because that Tag is not actually linked.
        await self.con.execute(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags += (
                        SELECT .weighted_tags {@weight := 2}
                        FILTER .name = 'boring'
                    )
                };
            """,
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': [{
                        'name': 'fun',
                        '@weight': None,
                    }, {
                        'name': 'wow',
                        '@weight': 1,
                    }],
                },
            ]
        )

    async def test_edgeql_update_props_10(self):
        # Check that we can update a link property on a specific link.

        # Setup some multi links on several objects
        await self.con.execute(
            r"""
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    weighted_tags := (
                        SELECT Tag {@weight := 2}
                        FILTER .name != 'boring'
                    )
                };

                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test2'
                SET {
                    weighted_tags := (
                        SELECT Tag {@weight := len(.name)}
                        FILTER .name != 'wow'
                    )
                };

                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test3'
                SET {
                    weighted_tags := (
                        SELECT Tag {
                            @weight := 10,
                            @note := 'original'
                        }
                        FILTER .name = 'fun'
                    )
                };
            """,
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight,
                        @note,
                    } ORDER BY .name
                } ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': [{
                        'name': 'fun',
                        '@weight': 2,
                        '@note': None,
                    }, {
                        'name': 'wow',
                        '@weight': 2,
                        '@note': None,
                    }],
                },
                {
                    'name': 'update-test2',
                    'weighted_tags': [{
                        'name': 'boring',
                        '@weight': 6,
                        '@note': None,
                    }, {
                        'name': 'fun',
                        '@weight': 3,
                        '@note': None,
                    }],
                },
                {
                    'name': 'update-test3',
                    'weighted_tags': [{
                        'name': 'fun',
                        '@weight': 10,
                        '@note': 'original',
                    }],
                },
            ]
        )

        # Update the @weight, @note for some tags on all of the
        # UpdateTest objects.
        await self.con.execute(
            r"""
                UPDATE UpdateTest
                SET {
                    weighted_tags += (
                        FOR x IN {
                            (
                                name := 'fun',
                                weight_adj := -1,
                                empty_note := 'new fun'
                            ),
                            (
                                name := 'wow',
                                weight_adj := 5,
                                empty_note := 'new wow'
                            ),
                            (
                                name := 'boring',
                                weight_adj := -2,
                                empty_note := 'new boring'
                            ),
                        } UNION (
                            SELECT .weighted_tags {
                                @weight := @weight + x.weight_adj,
                                @note := @note ?? x.empty_note,
                            }
                            FILTER .name = x.name
                        )
                    )
                };
            """,
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    weighted_tags: {
                        name,
                        @weight,
                        @note,
                    } ORDER BY .name
                } ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': [{
                        'name': 'fun',
                        '@weight': 1,
                        '@note': 'new fun',
                    }, {
                        'name': 'wow',
                        '@weight': 7,
                        '@note': 'new wow',
                    }],
                },
                {
                    'name': 'update-test2',
                    'weighted_tags': [{
                        'name': 'boring',
                        '@weight': 4,
                        '@note': 'new boring',
                    }, {
                        'name': 'fun',
                        '@weight': 2,
                        '@note': 'new fun',
                    }],
                },
                {
                    'name': 'update-test3',
                    'weighted_tags': [{
                        'name': 'fun',
                        '@weight': 9,
                        '@note': 'original',
                    }],
                },
            ]
        )

    async def test_edgeql_update_for_01(self):
        await self.assert_query_result(
            r"""
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
            """,
            [{}, {}],  # since updates are in FOR they return objects
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    comment
                } ORDER BY UpdateTest.name;
            """,
            [
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
            ]
        )

    async def test_edgeql_update_for_02(self):
        await self.assert_query_result(
            r"""
                FOR x IN {
                        'update-test1',
                        'update-test2',
                    }
                UNION (
                    UPDATE UpdateTest
                    FILTER UpdateTest.name = x
                    SET {
                        comment := x ++ "!"
                    }
                );
            """,
            [{}, {}],  # since updates are in FOR they return objects
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    comment
                } ORDER BY UpdateTest.name;
            """,
            [
                {
                    'name': 'update-test1',
                    'comment': 'update-test1!'
                },
                {
                    'name': 'update-test2',
                    'comment': 'update-test2!'
                },
                {
                    'name': 'update-test3',
                    'comment': 'third'
                },
            ]
        )

    async def test_edgeql_update_for_03(self):
        await self.assert_query_result(
            r"""
                FOR x IN {
                        'update-test1',
                        'update-test2',
                    }
                UNION (
                    UPDATE UpdateTest
                    FILTER UpdateTest.name = x
                    SET {
                        str_tags := x
                    }
                );
            """,
            [{}, {}],  # since updates are in FOR they return objects
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    str_tags,
                }
                FILTER
                    .name IN {'update-test1', 'update-test2', 'update-test3'}
                ORDER BY
                    .name;
            """,
            [
                {
                    'name': 'update-test1',
                    'str_tags': ['update-test1'],
                },
                {
                    'name': 'update-test2',
                    'str_tags': ['update-test2'],
                },
                {
                    'name': 'update-test3',
                    'str_tags': [],
                },
            ]
        )

    async def test_edgeql_update_empty_01(self):
        await self.assert_query_result(
            r"""
                # just clear all the comments
                UPDATE UpdateTest
                SET {
                    comment := {}
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest.comment;
            """,
            [],
        )

    async def test_edgeql_update_empty_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid target for property.*std::int64.*expecting .*str'"):
            await self.con.execute(r"""
                # just clear all the comments
                UPDATE UpdateTest
                SET {
                    comment := <int64>{}
                };
            """)

    async def test_edgeql_update_empty_03(self):
        with self.assertRaisesRegex(
                edgedb.MissingRequiredError,
                r"missing value for required property"):
            await self.con.execute(r"""
                # just clear all the comments
                UPDATE UpdateTest
                SET {
                    name := {}
                };
            """)

    async def test_edgeql_update_empty_04(self):
        await self.assert_query_result(
            r"""
                # just clear all the statuses
                UPDATE UpdateTest
                SET {
                    status := {}
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest.status;
            """,
            [],
        )

    async def test_edgeql_update_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link.*std::Object.*"
                r"expecting 'default::Status'"):
            await self.con.execute(r"""
                # just clear all the statuses
                UPDATE UpdateTest
                SET {
                    status := <Object>{}
                };
            """)

    async def test_edgeql_update_cardinality_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'single'):
            await self.con.execute(r'''

                UPDATE UpdateTest
                SET {
                    status := Status
                };
            ''')

    async def test_edgeql_update_cardinality_02(self):
        await self.assert_query_result(r'''
            WITH
                x1 := (
                    UPDATE UpdateTest
                    FILTER .name = 'update-test1'
                    SET {
                        status := (
                            SELECT Status
                            # the ID is non-existent
                            FILTER .id = <uuid>
                                '10000000-aaaa-bbbb-cccc-d00000000000'
                        )
                    }
                )
            SELECT {
                multi x0 := (
                    SELECT x1 {
                        name,
                        status: {
                            name
                        }
                    }
                )
            };
        ''', [{
            'x0': [{'name': 'update-test1', 'status': None}]
        }])

    async def test_edgeql_update_new_01(self):
        # test and UPDATE with a new object
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER .name = 'update-test1'
                SET {
                    tags := (
                        INSERT Tag {
                            name := 'new tag'
                        }
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    tags: {
                        name
                    }
                } FILTER .name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'tags': [{
                        'name': 'new tag',
                    }],
                },
            ]
        )

    async def test_edgeql_update_new_02(self):
        # test and UPDATE with a new object
        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER .name = 'update-test1'
                SET {
                    status := (
                        INSERT Status {
                            name := 'new status'
                        }
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    status: {
                        name
                    }
                } FILTER .name = 'update-test1';
            """,
            [
                {
                    'name': 'update-test1',
                    'status': {
                        'name': 'new status',
                    },
                },
            ]
        )

    async def test_edgeql_update_collection_01(self):
        # test and UPDATE with a collection
        await self.con.execute(
            r"""
                UPDATE CollectionTest
                FILTER .name = 'collection-test1'
                SET {
                    some_tuple := ('coll_01', 1)
                };
            """
        )

        await self.assert_query_result(
            r"""
                SELECT CollectionTest {
                    name,
                    some_tuple,
                } FILTER .name = 'collection-test1';
            """,
            [
                {
                    'name': 'collection-test1',
                    'some_tuple': ['coll_01', 1],
                },
            ]
        )

    async def test_edgeql_update_collection_02(self):
        # test and UPDATE with a collection
        await self.con.execute(
            r"""
                UPDATE CollectionTest
                FILTER .name = 'collection-test1'
                SET {
                    str_array := ['coll_02', '2']
                };
            """
        )

        await self.assert_query_result(
            r"""
                SELECT CollectionTest {
                    name,
                    str_array,
                } FILTER .name = 'collection-test1';
            """,
            [
                {
                    'name': 'collection-test1',
                    'str_array': ['coll_02', '2'],
                },
            ]
        )

    async def test_edgeql_update_correlated_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Status' here"):
            await self.con.execute(r'''
                SELECT (
                    Status,
                    (UPDATE UpdateTest SET {
                        status := Status
                    })
                );
            ''')

    async def test_edgeql_update_correlated_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Status' here"):
            await self.con.execute(r'''
                SELECT (
                    (UPDATE UpdateTest SET {
                        status := Status
                    }),
                    Status,
                );
            ''')

    async def test_edgeql_update_correlated_bad_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'UpdateTest' here"):
            await self.con.execute(r'''
                SELECT (
                    UpdateTest,
                    (UPDATE UpdateTest SET {name := 'update bad'}),
                )
            ''')

    async def test_edgeql_update_protect_readonly_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot update link 'readonly_tag': "
            "it is declared as read-only",
            _position=148,
        ):
            await self.con.execute(r'''
                UPDATE UpdateTest
                FILTER .name = 'update-test-readonly'
                SET {
                    readonly_tag := (SELECT Tag FILTER .name = 'not read-only')
                };
            ''')

    async def test_edgeql_update_protect_readonly_02(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot update property 'readonly_note': "
            "it is declared as read-only",
            _position=148,
        ):
            await self.con.execute(r'''
                UPDATE UpdateTest
                FILTER .name = 'update-test-readonly'
                SET {
                    readonly_note := 'not read-only',
                };
            ''')

    async def test_edgeql_update_protect_readonly_03(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot update property 'id': "
            "it is declared as read-only",
        ):
            await self.con.execute(r'''
                UPDATE UpdateTest
                SET {
                    id := <uuid>'77841036-8e35-49ce-b509-2cafa0c25c4f'
                };
            ''')

    @test.xfail("nested UPDATE not supported")
    async def test_edgeql_update_protect_readonly_04(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot update property 'readonly_note': "
            "it is declared as read-only",
            _position=190,
        ):
            await self.con.execute(r'''
                UPDATE UpdateTest
                FILTER .name = 'update-test-readonly'
                SET {
                    weighted_tags: {
                        @readonly_note := 'not read-only',
                    },
                };
            ''')

    async def test_edgeql_update_append_01(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-append-1',
            };

            INSERT UpdateTest {
                name := 'update-test-append-2',
            };

            INSERT UpdateTest {
                name := 'update-test-append-3',
            };
        """)

        await self.con.execute("""
            WITH
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER .name = 'update-test-append-1'
            SET {
                annotated_tests := (
                    SELECT U2 FILTER .name = 'update-test-append-2'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_tests: {
                        name,
                        @note
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test-append-1';
            """,
            [
                {
                    'name': 'update-test-append-1',
                    'annotated_tests': [{
                        'name': 'update-test-append-2',
                        '@note': None,
                    }],
                },
            ]
        )

        await self.con.execute("""
            WITH
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER .name = 'update-test-append-1'
            SET {
                annotated_tests += (
                    SELECT U2 { @note := 'foo' }
                    FILTER .name = 'update-test-append-3'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_tests: {
                        name,
                        @note
                    } ORDER BY .name
                } FILTER UpdateTest.name = 'update-test-append-1';
            """,
            [
                {
                    'name': 'update-test-append-1',
                    'annotated_tests': [{
                        'name': 'update-test-append-2',
                        '@note': None,
                    }, {
                        'name': 'update-test-append-3',
                        '@note': 'foo',
                    }],
                },
            ]
        )

    async def test_edgeql_update_append_02(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "possibly more than one element returned by an expression"
            " for a link 'annotated_status' declared as 'single'",
            _position=114,
        ):
            await self.con.execute("""
                UPDATE UpdateTest
                FILTER .name = 'foo'
                SET {
                    annotated_status += (
                        SELECT Status FILTER .name = 'status'
                    )
                };
            """)

    async def test_edgeql_append_badness_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"unexpected '\+='",
            _position=90,
        ):
            await self.con.execute("""
                INSERT UpdateTest
                {
                    annotated_status += (
                        SELECT Status FILTER .name = 'status'
                    )
                };
            """)

    async def test_edgeql_append_badness_02(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"unexpected '\+='",
            _position=90,
        ):
            await self.con.execute("""
                SELECT UpdateTest
                {
                    annotated_status += (
                        SELECT Status FILTER .name = 'status'
                    )
                };
            """)

    async def test_edgeql_update_subtract_01(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-subtract-1',
            };

            INSERT UpdateTest {
                name := 'update-test-subtract-2',
            };

            INSERT UpdateTest {
                name := 'update-test-subtract-3',
            };
        """)

        await self.con.execute("""
            WITH
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER .name = 'update-test-subtract-1'
            SET {
                annotated_tests := DISTINCT(
                    FOR v IN {
                        ('update-test-subtract-2', 'one'),
                        ('update-test-subtract-3', 'two'),
                    }
                    UNION (
                        SELECT U2 {
                            @note := v.1,
                        } FILTER .name = v.0
                    )
                )
            };
        """)

        await self.con.execute("""
            WITH
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER .name = 'update-test-subtract-3'
            SET {
                annotated_tests := (
                    FOR v IN {
                        ('update-test-subtract-2', 'one'),
                    }
                    UNION (
                        SELECT U2 {
                            @note := v.1,
                        } FILTER .name = v.0
                    )
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_tests: {
                        name,
                        @note
                    } ORDER BY .name
                } FILTER
                    .name LIKE 'update-test-subtract-%'
                  ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-subtract-1',
                    'annotated_tests': [{
                        'name': 'update-test-subtract-2',
                        '@note': 'one',
                    }, {
                        'name': 'update-test-subtract-3',
                        '@note': 'two',
                    }],
                },
                {
                    'name': 'update-test-subtract-2',
                    'annotated_tests': [],
                },
                {
                    'name': 'update-test-subtract-3',
                    'annotated_tests': [{
                        'name': 'update-test-subtract-2',
                        '@note': 'one',
                    }],
                },
            ]
        )

        await self.con.execute("""
            WITH
                U2 := UpdateTest
            UPDATE UpdateTest
            FILTER .name = 'update-test-subtract-1'
            SET {
                annotated_tests -= (
                    SELECT U2
                    FILTER .name = 'update-test-subtract-2'
                )
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    annotated_tests: {
                        name,
                        @note
                    } ORDER BY .name
                } FILTER
                    .name LIKE 'update-test-subtract-%'
                  ORDER BY .name;
            """,
            [
                {
                    'name': 'update-test-subtract-1',
                    'annotated_tests': [{
                        'name': 'update-test-subtract-3',
                        '@note': 'two',
                    }],
                },
                {
                    'name': 'update-test-subtract-2',
                    'annotated_tests': [],
                },
                {
                    'name': 'update-test-subtract-3',
                    'annotated_tests': [{
                        'name': 'update-test-subtract-2',
                        '@note': 'one',
                    }],
                },
            ]
        )

    async def test_edgeql_update_subtract_02(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-subtract-various',
                annotated_status := (
                    SELECT Status {
                        @note := 'forever',
                    } FILTER .name = 'Closed'
                ),
                comment := 'to remove',
                str_tags := {'1', '2', '3'},
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    annotated_status: {
                        name,
                        @note
                    },
                    comment,
                    str_tags ORDER BY UpdateTest.str_tags
                } FILTER
                    .name = 'update-test-subtract-various';
            """,
            [
                {
                    'annotated_status': {
                        'name': 'Closed',
                        '@note': 'forever',
                    },
                    'comment': 'to remove',
                    'str_tags': ['1', '2', '3'],
                },
            ],
        )

        # Check that singleton links work.
        await self.con.execute("""
            UPDATE UpdateTest
            FILTER .name = 'update-test-subtract-various'
            SET {
                annotated_status -= (SELECT Status FILTER .name = 'Closed')
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    annotated_status: {
                        name,
                        @note
                    },
                } FILTER
                    .name = 'update-test-subtract-various';
            """,
            [
                {
                    'annotated_status': None,
                },
            ],
        )

        # And singleton properties too.
        await self.con.execute("""
            UPDATE UpdateTest
            FILTER .name = 'update-test-subtract-various'
            SET {
                comment -= 'to remove'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    comment,
                } FILTER
                    .name = 'update-test-subtract-various';
            """,
            [
                {
                    'comment': None,
                },
            ],
        )

        # And multi properties as well.
        await self.con.execute("""
            UPDATE UpdateTest
            FILTER .name = 'update-test-subtract-various'
            SET {
                str_tags -= '2'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    str_tags,
                } FILTER
                    .name = 'update-test-subtract-various';
            """,
            [
                {
                    'str_tags': {'1', '3'},
                },
            ],
        )

    async def test_edgeql_update_subtract_non_distinct(self):
        await self.con.execute("""
            INSERT UpdateTest {
                name := 'update-test-subtract-non-distinct-1',
                str_tags := {'1', '1', '2', '3', '2', '2', '3', '4'},
            };
            INSERT UpdateTest {
                name := 'update-test-subtract-non-distinct-2',
                str_tags := {'1', '2', '2', '3', '4', '5'},
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    UpdateTest {
                        str_tags ORDER BY UpdateTest.str_tags
                    }
                FILTER
                    .name LIKE 'update-test-subtract-non-distinct-%'
                ORDER BY
                    .name
            """,
            [
                {
                    'str_tags': ['1', '1', '2', '2', '2', '3', '3', '4'],
                },
                {
                    'str_tags': ['1', '2', '2', '3', '4', '5'],
                },
            ],
        )

        await self.con.execute("""
            UPDATE UpdateTest
            FILTER .name LIKE 'update-test-subtract-non-distinct-%'
            SET {
                str_tags -= {'1', '2', '2', '3', '5'}
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    UpdateTest {
                        str_tags ORDER BY UpdateTest.str_tags,
                    }
                FILTER
                    .name LIKE 'update-test-subtract-non-distinct-%'
                ORDER BY
                    .name
            """,
            [
                {
                    'str_tags': {'1', '2', '3', '4'},
                },
                {
                    'str_tags': {'4'},
                },
            ],
        )

        await self.con.execute("""
            UPDATE UpdateTest
            FILTER .name LIKE 'update-test-subtract-non-distinct-%'
            SET {
                str_tags -= <str>{}
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    UpdateTest {
                        str_tags ORDER BY UpdateTest.str_tags,
                    }
                FILTER
                    .name LIKE 'update-test-subtract-non-distinct-%'
                ORDER BY
                    .name
            """,
            [
                {
                    'str_tags': {'1', '2', '3', '4'},
                },
                {
                    'str_tags': {'4'},
                },
            ],
        )

        await self.con.execute("""
            UPDATE UpdateTest
            FILTER .name LIKE 'update-test-subtract-non-distinct-%'
            SET {
                str_tags -= {'10'}
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT
                    UpdateTest {
                        str_tags ORDER BY UpdateTest.str_tags,
                    }
                FILTER
                    .name LIKE 'update-test-subtract-non-distinct-%'
                ORDER BY
                    .name
            """,
            [
                {
                    'str_tags': {'1', '2', '3', '4'},
                },
                {
                    'str_tags': {'4'},
                },
            ],
        )

    async def test_edgeql_update_subtract_required(self):
        await self.con.execute("""
            INSERT MultiRequiredTest {
                name := 'update-test-subtract-required',
                prop := {'one', 'two'},
                tags := (SELECT Tag FILTER .name IN {'fun', 'wow'}),
            };
        """)

        await self.con.execute("""
            UPDATE MultiRequiredTest
            FILTER .name = 'update-test-subtract-required'
            SET {
                prop -= 'one'
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT MultiRequiredTest {
                    prop,
                } FILTER
                    .name = 'update-test-subtract-required';
            """,
            [
                {
                    'prop': {'two'},
                },
            ],
        )

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required property 'prop'",
        ):
            await self.con.execute("""
                UPDATE MultiRequiredTest
                FILTER .name = 'update-test-subtract-required'
                SET {
                    prop -= 'two'
                };
            """)

        await self.con.execute("""
            UPDATE MultiRequiredTest
            FILTER .name = 'update-test-subtract-required'
            SET {
                tags -= (SELECT Tag FILTER .name = 'fun')
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT MultiRequiredTest {
                    tags: {name}
                } FILTER
                    .name = 'update-test-subtract-required';
            """,
            [
                {
                    'tags': [{'name': 'wow'}],
                },
            ],
        )

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required link 'tags'",
        ):
            await self.con.execute("""
                UPDATE MultiRequiredTest
                FILTER .name = 'update-test-subtract-required'
                SET {
                    tags -= (SELECT Tag FILTER .name = 'wow')
                };
            """)

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError,
            r"missing value for required link 'tags'",
        ):
            await self.con.execute("""
                SELECT (
                    UPDATE MultiRequiredTest
                    FILTER .name = 'update-test-subtract-required'
                    SET {
                        tags -= (SELECT Tag FILTER .name = 'wow')
                    }
                ) FILTER false;
            """)

    async def test_edgeql_update_insert_multi_required_01(self):
        await self.con.execute("""
            insert MultiRequiredTest {
              name := "___",
              prop := "!",
              tags := (
                for i in {(x := "90"), (x := "240")}
                union (
                  insert Tag {
                    name := i.x,
                  }
                )
              ),
            };
        """)

    async def test_edgeql_update_insert_multi_required_02(self):
        await self.con.execute("""
            insert MultiRequiredTest {
              name := "___",
              prop := "!",
              tags := (
                for i in {"90", "240"}
                union (
                  insert Tag {
                    name := i,
                  }
                )
              ),
            };
        """)

    async def test_edgeql_subtract_badness_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"unexpected '-='",
            _position=90,
        ):
            await self.con.execute("""
                INSERT UpdateTest
                {
                    annotated_status -= (
                        SELECT Status FILTER .name = 'status'
                    )
                };
            """)

    async def test_edgeql_update_insert_01(self):
        await self.con.execute('''
            UPDATE UpdateTest SET {
                tags := (INSERT Tag { name := <str>random() })
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT count(UpdateTest.tags);
            """,
            [3],
        )

    async def test_edgeql_update_insert_02(self):
        await self.con.execute('''
            UPDATE UpdateTest SET {
                status := (INSERT Status { name := <str>random() })
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT count(UpdateTest.status);
            """,
            [3],
        )

    @test.xfail('''
        PostgreSQL doesn't allow updating just-inserted record
        in the same query.
    ''')
    async def test_edgeql_update_with_self_insert_01(self):
        await self.con.execute('''
            WITH new_test := (INSERT UpdateTest { name := "new-test" })
            UPDATE new_test
            SET {
                related := (new_test)
            };
        ''')

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    related: {
                        name
                    }
                }
                FILTER .name = "new-test";
            """,
            [
                {
                    'name': 'new-test',
                    'related': [{
                        'name': 'new-test'
                    }]
                },
            ],
        )

    async def test_edgeql_update_inheritance_01(self):
        await self.con.execute('''
            INSERT UpdateTest {
                name := 'update-test-inh-supertype-1',
                related := (
                    SELECT (DETACHED UpdateTest)
                    FILTER .name = 'update-test1'
                )
            };

            INSERT UpdateTestSubType {
                name := 'update-test-inh-subtype-1',
                related := (
                    SELECT (DETACHED UpdateTest)
                    FILTER .name = 'update-test1'
                )
            };
        ''')

        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER .name = 'update-test-inh-subtype-1'
                SET {
                    comment := 'updated',
                    related := (
                        SELECT (DETACHED UpdateTest)
                        FILTER .name = 'update-test2'
                    ),
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    comment,
                    related: {
                        name
                    }
                }
                FILTER .name LIKE 'update-test-inh-%'
                ORDER BY .name
            """,
            [
                {
                    'name': 'update-test-inh-subtype-1',
                    'comment': 'updated',
                    'related': [{
                        'name': 'update-test2'
                    }]
                },
                {
                    'name': 'update-test-inh-supertype-1',
                    'comment': None,
                    'related': [{
                        'name': 'update-test1'
                    }]
                },
            ]
        )

    async def test_edgeql_update_inheritance_02(self):
        await self.con.execute('''
            INSERT UpdateTest {
                name := 'update-test-inh-supertype-2',
                related := (
                    SELECT (DETACHED UpdateTest)
                    FILTER .name = 'update-test2'
                )
            };

            INSERT UpdateTestSubType {
                name := 'update-test-inh-subtype-2',
                comment := 'update-test-inh-02',
                related := (
                    SELECT (DETACHED UpdateTest)
                    FILTER .name = 'update-test2'
                )
            };

            INSERT UpdateTestSubSubType {
                name := 'update-test-inh-subsubtype-2',
                comment := 'update-test-inh-02',
                related := (
                    SELECT (DETACHED UpdateTest)
                    FILTER .name = 'update-test2'
                )
            };
        ''')

        await self.assert_query_result(
            r"""
                UPDATE UpdateTest
                FILTER .comment = 'update-test-inh-02'
                SET {
                    comment := 'updated',
                    related := (
                        SELECT (DETACHED UpdateTest)
                        FILTER .name = 'update-test2'
                    ),
                };
            """,
            [{}, {}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    name,
                    comment,
                    related: {
                        name
                    }
                }
                FILTER .name LIKE 'update-test-inh-%'
                ORDER BY .name
            """,
            [
                {
                    'name': 'update-test-inh-subsubtype-2',
                    'comment': 'updated',
                    'related': [{
                        'name': 'update-test2'
                    }]
                },
                {
                    'name': 'update-test-inh-subtype-2',
                    'comment': 'updated',
                    'related': [{
                        'name': 'update-test2'
                    }]
                },
                {
                    'name': 'update-test-inh-supertype-2',
                    'comment': None,
                    'related': [{
                        'name': 'update-test2'
                    }]
                },
            ]
        )

    async def test_edgeql_update_inheritance_03(self):
        await self.con.execute('''
            INSERT UpdateTestSubSubType {
                name := 'update-test-w-insert',
            };
        ''')

        await self.assert_query_result(
            r"""
                UPDATE UpdateTestSubType
                FILTER .name = 'update-test-w-insert'
                SET {
                    tags := (INSERT Tag { name := "new tag" })
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                SELECT UpdateTest {
                    tags: {name},
                }
                FILTER .name = 'update-test-w-insert'
            """,
            [
                {
                    'tags': [{
                        'name': 'new tag'
                    }]
                },
            ]
        )

    async def test_edgeql_update_add_dupes_01a(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'add-dupes-scalar',
                str_tags := {'foo', 'bar'},
            };
        """)

        await self.assert_query_result(
            r"""
                WITH _ := (
                    UPDATE UpdateTestSubSubType
                    FILTER .name = 'add-dupes-scalar'
                    SET {
                        str_tags += 'foo'
                    }
                )
                SELECT _ { name, str_tags ORDER BY _.str_tags }
            """,
            [{
                'name': 'add-dupes-scalar',
                'str_tags': ['bar', 'foo', 'foo'],
            }]
        )

    @test.xerror(
        "Known collation issue on Heroku Postgres",
        unless=os.getenv("EDGEDB_TEST_BACKEND_VENDOR") != "heroku-postgres"
    )
    async def test_edgeql_update_add_dupes_01b(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'add-dupes-scalar-foo',
                str_tags := {'foo', 'baz'},
            };
            INSERT UpdateTestSubSubType {
                name := 'add-dupes-scalar-bar',
                str_tags := {'bar', 'baz'},
            };
        """)

        await self.assert_query_result(
            r"""
                WITH _ := (
                    FOR x in {'foo', 'bar'} UNION (
                        UPDATE UpdateTestSubSubType
                        FILTER .name = 'add-dupes-scalar-' ++ x
                        SET {
                            str_tags += x
                        }
                    )
                )
                SELECT _ {
                    name,
                    str_tags ORDER BY _.str_tags
                };
            """,
            [
                {
                    'name': 'add-dupes-scalar-foo',
                    'str_tags': ['baz', 'foo', 'foo'],
                },
                {
                    'name': 'add-dupes-scalar-bar',
                    'str_tags': ['bar', 'bar', 'baz'],
                },
            ]
        )

        await self.assert_query_result(
            r"""
                WITH _ := (
                    FOR x in {'foo', 'bar'} UNION (
                        UPDATE UpdateTestSubSubType
                        FILTER .name = 'add-dupes-scalar-' ++ x
                        SET {
                            str_tags += {x, ('baz' if x = 'foo' else <str>{})}
                        }
                    )
                )
                SELECT _ {
                    name,
                    str_tags ORDER BY _.str_tags
                };
            """,
            [
                {
                    'name': 'add-dupes-scalar-foo',
                    'str_tags': ['baz', 'baz', 'foo', 'foo', 'foo'],
                },
                {
                    'name': 'add-dupes-scalar-bar',
                    'str_tags': ['bar', 'bar', 'bar', 'baz'],
                },
            ]
        )

        await self.assert_query_result(
            r"""
                WITH _ := (
                    FOR x in {'foo', 'bar'} UNION (
                        UPDATE UpdateTestSubSubType
                        FILTER .name = 'add-dupes-scalar-' ++ x
                        SET {
                            str_tags += x
                        }
                    )
                )
                SELECT _ {
                    name,
                    str_tags ORDER BY _.str_tags
                }
                 ORDER BY .name;

            """,
            [
                {
                    'name': 'add-dupes-scalar-bar',
                    'str_tags': ['bar', 'bar', 'bar', 'bar', 'baz'],
                },
                {
                    'name': 'add-dupes-scalar-foo',
                    'str_tags': ['baz', 'baz', 'foo', 'foo', 'foo', 'foo'],
                },
            ]
        )

    async def test_edgeql_update_add_dupes_02(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'add-dupes',
                tags := (SELECT Tag FILTER .name IN {'fun', 'wow'})
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    UPDATE UpdateTestSubSubType
                    FILTER .name = 'add-dupes'
                    SET {
                        tags += {
                            (SELECT Tag FILTER .name = 'fun'),
                        }
                    }
                ) {
                    name,
                    tags: { name } ORDER BY .name
                }
            """,
            [{
                'name': 'add-dupes',
                'tags': [
                    {'name': 'fun'},
                    {'name': 'wow'},
                ],
            }]
        )

    async def test_edgeql_update_add_dupes_03(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'add-dupes',
                tags := (SELECT Tag FILTER .name IN {'fun', 'wow'})
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    UPDATE UpdateTestSubSubType
                    FILTER .name = 'add-dupes'
                    SET {
                        tags += {
                            (UPDATE Tag FILTER .name = 'fun' SET {flag := 1}),
                            (UPDATE Tag FILTER .name = 'wow' SET {flag := 2}),
                        }
                    }
                ) {
                    name,
                    tags: { name, flag } ORDER BY .name
                }
            """,
            [{
                'name': 'add-dupes',
                'tags': [
                    {'name': 'fun', 'flag': 1},
                    {'name': 'wow', 'flag': 2},
                ],
            }]
        )

    async def test_edgeql_update_add_dupes_04(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'add-dupes-lprop',
                weighted_tags := (
                    SELECT Tag { @weight := 10 }
                    FILTER .name IN {'fun', 'wow'})
            };
        """)

        await self.assert_query_result(
            r"""
                SELECT (
                    UPDATE UpdateTestSubSubType
                    FILTER .name = 'add-dupes-lprop'
                    SET {
                        weighted_tags += (SELECT Tag {@weight := 20}
                                          FILTER .name = 'fun'),
                    }
                ) {
                    name,
                    weighted_tags: { name, @weight } ORDER BY .name
                }
            """,
            [{
                'name': 'add-dupes-lprop',
                'weighted_tags': [
                    {'name': 'fun', '@weight': 20},
                    {'name': 'wow', '@weight': 10},
                ],
            }]
        )

    async def test_edgeql_update_assert_calls_01(self):
        await self.assert_query_result(
            r"""
            select assert_exists(assert_single((
              select (update UpdateTest filter .name = 'update-test1'
                      set {comment := "test"}) { comment }
            )));
            """,
            [{"comment": "test"}]
        )

    async def test_edgeql_update_covariant_01(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'update-covariant',
            };
        """)

        # Covariant updates should work if the types actually work at
        # runtime
        await self.con.execute("""
            UPDATE UpdateTestSubType
            FILTER .name = "update-covariant"
            SET {
                status := (SELECT Status FILTER .name = "Broke a Type System")
            }
        """)

        # But fail if they don't
        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'status' of object type "
                r"'default::UpdateTestSubSubType': 'default::Status' "
                r"\(expecting 'default::MajorLifeEvent'\)"):
            await self.con.execute("""
                UPDATE UpdateTestSubType
                FILTER .name = "update-covariant"
                SET {
                    status := (SELECT Status FILTER .name = "Open")
                }
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'status' of object type "
                r"'default::UpdateTestSubSubType': 'default::Status' "
                r"\(expecting 'default::MajorLifeEvent'\)"):
            await self.con.execute("""
                UPDATE UpdateTestSubType
                FILTER .name = "update-covariant"
                SET {
                    status := (INSERT Status { name := "Yolo" })
                }
            """)

    async def test_edgeql_update_covariant_02(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'update-covariant',
            };
        """)

        # Covariant updates should work if the types actually work at
        # runtime
        await self.con.execute("""
            UPDATE UpdateTestSubType
            FILTER .name = "update-covariant"
            SET {
                statuses := (
                    SELECT Status FILTER .name = "Broke a Type System")
            }
        """)

        # But fail if they don't
        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'statuses' of object type "
                r"'default::UpdateTestSubSubType': 'default::Status' "
                r"\(expecting 'default::MajorLifeEvent'\)"):
            await self.con.execute("""
                UPDATE UpdateTestSubType
                FILTER .name = "update-covariant"
                SET {
                    statuses := (SELECT Status FILTER .name = "Open")
                }
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'statuses' of object type "
                r"'default::UpdateTestSubSubType': 'default::Status' "
                r"\(expecting 'default::MajorLifeEvent'\)"):
            await self.con.execute("""
                UPDATE UpdateTestSubType
                FILTER .name = "update-covariant"
                SET {
                    statuses := (INSERT Status { name := "Yolo" })
                }
            """)

    async def test_edgeql_update_covariant_03(self):
        await self.con.execute("""
            INSERT UpdateTestSubSubType {
                name := 'update-covariant',
            };
        """)

        # Tests with a multi link, actually using multiple things

        # Covariant updates should work if the types actually work at
        # runtime
        await self.con.execute("""
            UPDATE UpdateTestSubType
            FILTER .name = "update-covariant"
            SET {
                statuses := (SELECT Status FILTER .name IN {
                                 "Broke a Type System",
                                 "Downloaded a Car",
                             })
            }
        """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'statuses' of object type "
                r"'default::UpdateTestSubSubType': 'default::Status' "
                r"\(expecting 'default::MajorLifeEvent'\)"):
            await self.con.execute("""
                UPDATE UpdateTestSubType
                FILTER .name = "update-covariant"
                SET {
                    statuses := (SELECT Status FILTER .name IN {
                                     "Broke a Type System",
                                     "Downloaded a Car",
                                     "Open",
                                 })
                }
            """)

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'statuses' of object type "
                r"'default::UpdateTestSubSubType': 'default::Status' "
                r"\(expecting 'default::MajorLifeEvent'\)"):
            await self.con.execute("""
                UPDATE UpdateTestSubType
                FILTER .name = "update-covariant"
                SET {
                    statuses := (FOR x in {"Foo", "Bar"} UNION (
                                     INSERT Status {name := x}))
                }
            """)

    async def test_edgeql_update_subnavigate_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            r"mutation queries must specify values with ':='",
        ):
            await self.con.execute('''
                UPDATE UpdateTest
                SET {
                    tags: {
                        flag := 1
                    } FILTER .name = 'Tag'
                };
            ''')

    async def test_edgeql_update_cardinality_assertion(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "possibly more than one element returned by an expression "
                "for a link 'status' declared as 'single'"):
            await self.con.query(r'''
                UPDATE UpdateTest
                SET {
                    status := Status,
                }
            ''')

    @test.xfail('''
        Disabling triggers for constraints without any inheritance
        is a performance optimization that we would like to do, but
        it results in different behavior in this case:
          deleting an object while creating a conflicting object *succeeds*
          if we are just using a UNIQUE constraint, but fails with a trigger.

        The cases where there is inheritance would still fail.
        This is all pretty marginal but we need to think about it.
    ''')
    async def test_edgeql_update_and_delete_01(self):
        # Updating something that would violate a constraint while
        # fixing the violation is still supposed to be an error.

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    "violates exclusivity constraint"):
            await self.con.execute('''
                SELECT (
                    (DELETE Tag FILTER .name = 'fun'),
                    (UPDATE Tag FILTER .name = 'wow' SET { name := 'fun' })
                )
            ''')

    async def test_edgeql_update_and_delete_02(self):
        # Assigning the result of a DELETE as a link during an UPDATE
        # should be an error.

        with self.assertRaisesRegex(edgedb.ConstraintViolationError,
                                    r"deletion of default::Tag.+ is "
                                    r"prohibited by link target policy"):
            await self.con.execute('''
                UPDATE UpdateTest
                FILTER .name = 'update-test1'
                SET {
                    tags := (DELETE Tag FILTER .name = 'fun')
                };
            ''')

    async def test_edgeql_update_subtract_backlink_overload_01(self):
        await self.con.execute(r"""
            CREATE TYPE Clash { CREATE LINK statuses -> Status; };
        """)

        await self.con.execute(r"""
            UPDATE UpdateTest FILTER .name = 'update-test1'
            SET { statuses := MajorLifeEvent };
        """)

        await self.assert_query_result(
            r"""
            SELECT (
                UPDATE UpdateTest FILTER .name = "update-test1" SET {
                    statuses -= (SELECT Status
                                 FILTER .name = "Downloaded a Car")
                }
            ) {
                statuses: {
                    name,
                    backlinks := .<statuses[IS UpdateTest].name
                }
            };
            """,
            [
                {
                    "statuses": [
                        {
                            "backlinks": ["update-test1"],
                            "name": "Broke a Type System"
                        }
                    ]
                }
            ]
        )

    async def test_edgeql_update_inject_intersection_01(self):
        await self.con.execute(r"""
            CREATE ABSTRACT TYPE default::I;
            CREATE TYPE default::S EXTENDING default::I {
                CREATE REQUIRED PROPERTY Depth -> std::float64;
            };
            CREATE TYPE default::P {
                CREATE REQUIRED MULTI LINK Items -> default::I;
                CREATE PROPERTY name -> std::str;
            };
        """)

        await self.con._fetchall(
            r"""
                with
                    obj1 := (select P FILTER .name = 'foo'),
                    obj3 := (select obj1.Items limit 1)[is S]
                UPDATE obj3
                SET {
                    Depth := 11.3781298010066
                };
            """,
            __typenames__=True
        )

        await self.con._fetchall(
            r"""
                with
                    obj1 := (select P FILTER .name = 'foo'),
                    obj3 := (select obj1.Items limit 1)[is S]
                DELETE obj3
            """,
            __typenames__=True
        )

    async def test_edgeql_update_inject_intersection_02(self):
        await self.con.execute(r"""
            create alias UpdateTestCommented :=
               (select UpdateTest filter exists .comment);
        """)

        await self.assert_query_result(
            r"""
                update UpdateTestCommented[is UpdateTestSubType]
                set { str_tags += 'lol' };
            """,
            [],
        )

        await self.assert_query_result(
            r"""
                delete UpdateTestCommented[is UpdateTestSubType]
            """,
            [],
        )

    async def test_edgeql_update_volatility_01(self):
        # random should be executed once for each object
        await self.con.execute(r"""
            update UpdateTest set { comment := <str>random() };
        """)

        await self.assert_query_result(
            r"""
                select count(distinct UpdateTest.comment) = count(UpdateTest)
            """,
            [True],
        )

    async def test_edgeql_update_volatility_02(self):
        # random should be executed once for each object
        await self.con.execute(r"""
            update UpdateTest set { str_tags := <str>random() };
        """)

        await self.assert_query_result(
            r"""
                select count(distinct UpdateTest.str_tags) = count(UpdateTest)
            """,
            [True],
        )

    async def test_edgeql_update_poly_overlay_01(self):
        await self.con.execute(r"""
            insert UpdateTestSubType { name := 'update-test4' };
        """)

        await self.assert_query_result(
            r"""
                select (
                  update UpdateTest filter .name = 'update-test4'
                  set { name := '!' }
                ) { c1 := .name, c2 := [is UpdateTestSubType].name };
            """,
            [{"c1": "!", "c2": "!"}]
        )

    async def test_edgeql_update_poly_overlay_02(self):
        await self.con.execute(r"""
            insert UpdateTestSubType { name := 'update-test4' };
        """)

        await self.assert_query_result(
            r"""
                with X := (
                  update UpdateTest filter .name = 'update-test4'
                  set { name := '!' }
                ),
                select X[is UpdateTestSubType] { name };
            """,
            [{"name": "!"}]
        )

    async def test_edgeql_update_where_order_dml(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "INSERT statements cannot be used in a FILTER clause"):
            await self.con.query('''
                    update UpdateTest
                    filter (INSERT UpdateTest {
                                name := 't1',
                            })
                    set { name := '!' }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "UPDATE statements cannot be used in a FILTER clause"):
            await self.con.query('''
                    update UpdateTest
                    filter (UPDATE UpdateTest set {
                            name := 't1',
                        })
                    set { name := '!' }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                "DELETE statements cannot be used in a FILTER clause"):
            await self.con.query('''
                    update UpdateTest
                    filter (DELETE UpdateTest filter .name = 't1')
                    set { name := '!' }
            ''')

    async def test_edgeql_update_union_overlay_01(self):
        await self.assert_query_result(
            r"""
            WITH
                 A := (UPDATE UpdateTest FILTER .name = 'update-test1'
                       SET { comment := "!!!!!1!!!" }),
                 B := (UPDATE UpdateTest FILTER .name = 'update-test2'
                       SET { comment  := "foo" }),
            SELECT assert_exists((SELECT {A, B} {name, comment}))
            ORDER BY .name;
            """,
            [
                {"name": "update-test1", "comment": "!!!!!1!!!"},
                {"name": "update-test2", "comment": "foo"},
            ]
        )

    @test.xfail("""
        We incorrectly return 'foo' as the comment twice.
        See #6222.
    """)
    async def test_edgeql_update_union_overlay_02(self):
        await self.assert_query_result(
            r"""
            WITH
                 A := (SELECT UpdateTest FILTER .name = 'update-test2'),
                 B := (UPDATE UpdateTest FILTER .name = 'update-test2'
                       SET { comment  := "foo" }),
            SELECT assert_exists((SELECT {A, B} {name, comment}))
            ORDER BY .comment;
            """,
            [
                {"name": "update-test2", "comment": "foo"},
                {"name": "update-test2", "comment": "second"},
            ]
        )

    async def test_edgeql_update_dunder_default_01(self):
        await self.con.execute(r"""
            INSERT DunderDefaultTest01 { a := 1, b := 2, c := 3 };
        """)

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='No default expression exists',
        ):
            await self.con.execute(r'''
                UPDATE DunderDefaultTest01 set { a := __default__ };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses __source__',
        ):
            await self.con.execute(r'''
                UPDATE DunderDefaultTest01 set { b := __default__ };
            ''')

        await self.con.execute(r"""
            UPDATE DunderDefaultTest01 set { c := __default__ };
        """)

        await self.assert_query_result(
            r'''
                SELECT DunderDefaultTest01 { a, b, c };
            ''',
            [
                {'a': 1, 'b': 2, 'c': 1},
            ]
        )

    async def test_edgeql_update_dunder_default_02(self):
        await self.con.execute(r'''
            INSERT DunderDefaultTest02_A { a := 1 };
            INSERT DunderDefaultTest02_A { a := 2 };
            INSERT DunderDefaultTest02_A { a := 3 };
            INSERT DunderDefaultTest02_A { a := 4 };
            INSERT DunderDefaultTest02_A { a := 5 };
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
                default_with_select := (
                    select DunderDefaultTest02_A
                    filter DunderDefaultTest02_A.a = 5
                ),
            };
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses DML',
        ):
            await self.con.execute(r'''
                UPDATE DunderDefaultTest02_B set {
                    default_with_insert := __default__
                };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses DML',
        ):
            await self.con.execute(r'''
                UPDATE DunderDefaultTest02_B set {
                    default_with_update := __default__
                };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__default__ cannot be used in this expression",
            _hint='Default expression uses DML',
        ):
            await self.con.execute(r'''
                UPDATE DunderDefaultTest02_B set {
                    default_with_delete := __default__
                };
            ''')

        await self.con.execute(r'''
            UPDATE DunderDefaultTest02_B set {
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

    async def test_edgeql_update_empty_array_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "expression returns value of indeterminate type"
        ):
            await self.con.execute("""
                update UpdateTest
                set {
                    name := [],
                };
            """)

    async def test_edgeql_update_empty_array_02(self):
        with self.assertRaisesRegex(
            edgedb.InvalidPropertyTargetError,
            r"invalid target for property 'name' "
            r"of object type 'default::UpdateTest': 'array<std::str>' "
            r"\(expecting 'std::str'\)"
        ):
            await self.con.execute("""
                update UpdateTest
                set {
                    name := ['a'] ?? [],
                };
            """)

    async def test_edgeql_update_empty_array_03(self):
        with self.assertRaisesRegex(
            edgedb.InvalidPropertyTargetError,
            r"invalid target for property 'name' "
            r"of object type 'default::UpdateTest': 'std::int64' "
            r"\(expecting 'std::str'\)"
        ):
            await self.con.execute("""
                insert UpdateTest {
                    name := array_unpack([1] ?? []),
                };
            """)

    async def test_edgeql_update_empty_array_04(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "expression returns value of indeterminate type"
        ):
            await self.con.execute("""
                update UpdateTest
                set {
                    annotated_status := (
                        select Status {
                            @note := []
                        }
                    )
                };
            """)

    async def test_edgeql_update_empty_array_05(self):
        await self.assert_query_result("""
            select ( update UpdateTest
                set {
                    weighted_tags := (
                        select Tag {
                            @note := array_join(['a'] ++ [], '')
                        }
                    )
                }
            ) { name, weighted_tags: { name, @note } } ;
            """,
            [
                {
                    'name': 'update-test1',
                    'weighted_tags': tb.bag([
                        {'name': 'fun', '@note': 'a'},
                        {'name': 'boring', '@note': 'a'},
                        {'name': 'wow', '@note': 'a'}
                    ]),
                },
                {
                    'name': 'update-test2',
                    'weighted_tags': tb.bag([
                        {'name': 'fun', '@note': 'a'},
                        {'name': 'boring', '@note': 'a'},
                        {'name': 'wow', '@note': 'a'}
                    ]),
                },
                {
                    'name': 'update-test3',
                    'weighted_tags': tb.bag([
                        {'name': 'fun', '@note': 'a'},
                        {'name': 'boring', '@note': 'a'},
                        {'name': 'wow', '@note': 'a'}
                    ]),
                },
            ],
        )
