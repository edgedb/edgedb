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
        cls.original = await cls.con.fetchall_json(r"""
            WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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

    async def test_edgeql_update_returning_01(self):
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
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
        orig1, orig2, orig3 = self.original

        await self.assert_query_result(
            r"""
                WITH
                    MODULE test,
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
                    MODULE test,
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
            data.append(await self.con.fetchone(r"""
                INSERT test::UpdateTest {
                    name := 'ret5.1'
                };
            """))
            data.append(await self.con.fetchone(r"""
                INSERT test::UpdateTest {
                    name := 'ret5.2'
                };
            """))
            data = [str(o.id) for o in data]

            await self.assert_query_result(
                r"""
                    WITH MODULE test
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
                    WITH MODULE test
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
                    WITH MODULE test
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

            objs = await self.con.fetchall(
                r"""
                    WITH MODULE test
                    UPDATE UpdateTest
                    FILTER UpdateTest.name LIKE '%ret5._'
                    SET {
                        name := 'new ' ++ UpdateTest.name
                    };
                """
            )

            self.assertTrue(hasattr(objs[0], '__tid__'))

        finally:
            await self.con.execute(r"""
                DELETE (
                    SELECT test::UpdateTest
                    FILTER .name LIKE '%ret5._'
                );
            """)

    async def test_edgeql_update_generic_01(self):
        status = await self.con.fetchone(r"""
            WITH MODULE test
            SELECT Status{id}
            FILTER Status.name = 'Open'
            LIMIT 1;
        """)
        status = str(status.id)

        updated = await self.con.fetchall(
            r"""
                WITH MODULE test
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
                WITH MODULE test
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

    async def test_edgeql_update_filter_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
                SELECT UpdateTest.comment;
            """,
            ['bad test'] * 3,
        )

    async def test_edgeql_update_filter_02(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
                SELECT UpdateTest.comment;
            """,
            ['bad test'] * 3,
        )

    async def test_edgeql_update_multiple_01(self):
        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    tags := UpdateTest.tags UNION (
                        SELECT Tag
                        FILTER Tag.name = 'wow'
                    )
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                    MODULE test,
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
                WITH MODULE test
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
                    MODULE test,
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
                WITH MODULE test
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
                    MODULE test,
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
                WITH MODULE test
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

    async def test_edgeql_update_multiple_08(self):
        await self.con.execute("""
            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-8-1',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-8-2',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-8-3',
            };
        """)

        await self.assert_query_result(
            r"""
                # make tests related to the other 2
                WITH
                    MODULE test,
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
                WITH MODULE test
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
                    MODULE test,
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
                WITH MODULE test
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
            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-9-1',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-9-2',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-9-3',
            };
        """)

        await self.assert_query_result(
            r"""
                # make tests related to the other 2
                WITH
                    MODULE test,
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
                WITH MODULE test
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
                    MODULE test,
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
                WITH MODULE test
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
            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-10-1',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-10-2',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-10-3',
            };
        """)

        await self.assert_query_result(
            r"""
                # make each test related to 'update-test-10-1'
                WITH
                    MODULE test,
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                    }],
                },
            ]
        )

    async def test_edgeql_update_props_05(self):
        await self.assert_query_result(
            r"""
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
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                # update again, erasing the 'note' value
                WITH MODULE test
                UPDATE UpdateTest
                FILTER UpdateTest.name = 'update-test1'
                SET {
                    annotated_status: {
                        @note := <str>{}
                    }
                };
            """,
            [{}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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

    async def test_edgeql_update_for_01(self):
        await self.assert_query_result(
            r"""
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
            """,
            [{}, {}],  # since updates are in FOR they return objects
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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

    async def test_edgeql_update_empty_01(self):
        await self.assert_query_result(
            r"""
                # just clear all the comments
                WITH MODULE test
                UPDATE UpdateTest
                SET {
                    comment := {}
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT UpdateTest.comment;
            """,
            {},
        )

    async def test_edgeql_update_empty_02(self):
        with self.assertRaisesRegex(
                edgedb.InvalidPropertyTargetError,
                r"invalid target for property.*std::int64.*expecting .*str'"):
            await self.con.execute(r"""
                # just clear all the comments
                WITH MODULE test
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
                WITH MODULE test
                UPDATE UpdateTest
                SET {
                    name := {}
                };
            """)

    async def test_edgeql_update_empty_04(self):
        await self.assert_query_result(
            r"""
                # just clear all the statuses
                WITH MODULE test
                UPDATE UpdateTest
                SET {
                    status := {}
                };
            """,
            [{}, {}, {}],
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
                SELECT UpdateTest.status;
            """,
            {},
        )

    async def test_edgeql_update_empty_05(self):
        with self.assertRaisesRegex(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link.*std::Object.*"
                r"expecting 'test::Status'"):
            await self.con.execute(r"""
                # just clear all the statuses
                WITH MODULE test
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
                SET MODULE test;

                UPDATE UpdateTest
                SET {
                    status := Status
                };
            ''')

    async def test_edgeql_update_cardinality_02(self):
        await self.assert_query_result(r'''
            WITH MODULE test
            SELECT stdgraphql::Query {
                multi x0 := (
                    WITH x1 := (
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
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
                WITH MODULE test
                UPDATE CollectionTest
                FILTER .name = 'collection-test1'
                SET {
                    some_tuple := ('coll_01', 1)
                };
            """
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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
                WITH MODULE test
                UPDATE CollectionTest
                FILTER .name = 'collection-test1'
                SET {
                    str_array := ['coll_02', '2']
                };
            """
        )

        await self.assert_query_result(
            r"""
                WITH MODULE test
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

    async def test_edgeql_update_in_conditional_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'UPDATE statements cannot be used'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT
                    (SELECT UpdateTest)
                    ??
                    (UPDATE UpdateTest SET { name := 'no way' });
            ''')

    async def test_edgeql_update_in_conditional_bad_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                'UPDATE statements cannot be used'):
            await self.con.execute(r'''
                WITH MODULE test
                SELECT
                    (SELECT UpdateTest FILTER .name = 'foo')
                    IF EXISTS UpdateTest
                    ELSE (
                        (SELECT UpdateTest)
                        UNION
                        (UPDATE UpdateTest SET { name := 'no way' })
                    );
            ''')

    async def test_edgeql_update_correlated_bad_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryError,
                "cannot reference correlated set 'Status' here"):
            await self.con.execute(r'''
                WITH MODULE test
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
                WITH MODULE test
                SELECT (
                    (UPDATE UpdateTest SET {
                        status := Status
                    }),
                    Status,
                );
            ''')

    async def test_edgeql_update_protect_readonly_01(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot update link 'readonly_tag': "
            "it is declared as read-only",
            _position=180,
        ):
            await self.con.execute(r'''
                WITH MODULE test
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
            _position=181,
        ):
            await self.con.execute(r'''
                WITH MODULE test
                UPDATE UpdateTest
                FILTER .name = 'update-test-readonly'
                SET {
                    readonly_note := 'not read-only',
                };
            ''')

    async def test_edgeql_update_protect_readonly_03(self):
        with self.assertRaisesRegex(
            edgedb.QueryError,
            "cannot update property 'readonly_note': "
            "it is declared as read-only",
            _position=223,
        ):
            await self.con.execute(r'''
                WITH MODULE test
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
            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-append-1',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-append-2',
            };

            WITH MODULE test
            INSERT UpdateTest {
                name := 'update-test-append-3',
            };
        """)

        await self.con.execute("""
            WITH
                MODULE test,
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
                WITH MODULE test
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
                MODULE test,
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
                WITH MODULE test
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
            " for a computable link 'annotated_status' declared as 'single'",
            _position=147,
        ):
            await self.con.execute("""
                WITH MODULE test
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
            _position=123,
        ):
            await self.con.execute("""
                WITH MODULE test
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
            _position=123,
        ):
            await self.con.execute("""
                WITH MODULE test
                SELECT UpdateTest
                {
                    annotated_status += (
                        SELECT Status FILTER .name = 'status'
                    )
                };
            """)
