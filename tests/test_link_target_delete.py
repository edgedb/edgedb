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


import itertools
import pathlib

import edgedb

from edb import errors

from edb.testbase import lang as tb
from edb.schema import links as s_links
from edb.schema import name as s_name

from edb.testbase import server as stb


class TestLinkTargetDeleteSchema(tb.BaseSchemaLoadTest):
    def test_schema_on_target_delete_01(self):
        schema = self.load_schema("""
            type Object {
                link foo -> Object {
                    on target delete allow
                };

                link bar -> Object;
            };
        """)

        obj = schema.get('default::Object')

        self.assertEqual(
            obj.getptr(schema, s_name.UnqualName('foo')).get_on_target_delete(
                schema),
            s_links.LinkTargetDeleteAction.Allow)

        self.assertEqual(
            obj.getptr(schema, s_name.UnqualName('bar')).get_on_target_delete(
                schema),
            s_links.LinkTargetDeleteAction.Restrict)

    def test_schema_on_target_delete_02(self):
        schema = self.load_schema("""
            type Object {
                link foo -> Object {
                    on target delete allow
                }
            };

            type Object2 extending Object {
                overloaded link foo -> Object {
                    annotation title := "Foo"
                }
            };

            type Object3 extending Object {
                overloaded link foo -> Object {
                    on target delete restrict
                }
            };
        """)

        obj2 = schema.get('default::Object2')
        self.assertEqual(
            obj2.getptr(schema, s_name.UnqualName('foo')).get_on_target_delete(
                schema),
            s_links.LinkTargetDeleteAction.Allow)

        obj3 = schema.get('default::Object3')
        self.assertEqual(
            obj3.getptr(schema, s_name.UnqualName('foo')).get_on_target_delete(
                schema),
            s_links.LinkTargetDeleteAction.Restrict)

    @tb.must_fail(errors.SchemaError,
                  "cannot implicitly resolve the `on target delete` action "
                  "for 'default::C.foo'")
    def test_schema_on_target_delete_03(self):
        """
            type A {
                link foo -> Object {
                    on target delete restrict
                }
            };

            type B {
                link foo -> Object {
                    on target delete allow
                }
            };

            type C extending A, B;
        """


class TestLinkTargetDeleteDeclarative(stb.QueryTestCase):

    SCHEMA = pathlib.Path(__file__).parent / 'schemas' / 'link_tgt_del.esdl'

    # Cannot use transaction isolation, because some
    # tests rely on transactional semantics and cannot
    # run in "nested" transactions.
    TRANSACTION_ISOLATION = False

    async def test_link_on_target_delete_restrict_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'deletion of default::Target1.* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT Target1 FILTER .name = 'Target1.1');
                """)

    async def test_link_on_target_delete_restrict_02(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1Child {
                    name := 'Target1Child.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1Child.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'deletion of default::Target1.* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT Target1Child
                            FILTER .name = 'Target1Child.1');
                """)

    async def test_link_on_target_delete_restrict_03(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source3 {
                    name := 'Source3.1',
                    tgt1_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'deletion of default::Target1 .* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT Target1 FILTER .name = 'Target1.1');
                """)

    async def test_link_on_target_delete_restrict_04(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1Child {
                    name := 'Target1Child.1'
                };

                INSERT Source3 {
                    name := 'Source3.1',
                    tgt1_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1Child.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'deletion of default::Target1.* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT Target1Child
                            FILTER .name = 'Target1Child.1');
                """)

    async def test_link_on_target_delete_restrict_05(self):
        success = False

        async with self._run_and_rollback():
            await self.con.execute(r"""

                INSERT Target1 {
                    name := 'Target1.1'
                };

                # no source, so the deletion should not be a problem
                DELETE Target1;
            """)

            success = True

        self.assertTrue(success)

    async def test_link_on_target_delete_restrict_06(self):
        success = False

        async with self._run_and_rollback():
            await self.con.execute("""

                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                DELETE Source1;
                DELETE Target1;
            """)

            success = True

        self.assertTrue(success)

    async def test_link_on_target_delete_restrict_07(self):
        success = False

        async with self._run_and_rollback():
            await self.con.execute("""

                FOR name IN {'Target1.1', 'Target1.2', 'Target1.3'}
                UNION (
                    INSERT Target1 {
                        name := name
                    });

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_m2m_restrict := (
                        SELECT Target1
                        FILTER
                            .name IN {'Target1.1', 'Target1.2', 'Target1.3'}
                    )
                };

                DELETE Source1;
                DELETE Target1;
            """)
            success = True

        self.assertTrue(success)

    async def test_link_on_target_delete_restrict_08(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt_union_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'deletion of default::Target1.* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT Target1 FILTER .name = 'Target1.1');
                """)

    async def test_link_on_target_delete_deferred_restrict_01(self):
        exception_is_deferred = False

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                'deletion of default::Target1 .* is prohibited by link'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Target1 {
                        name := 'Target1.1'
                    };

                    INSERT Source1 {
                        name := 'Source1.1',
                        tgt1_deferred_restrict := (
                            SELECT Target1
                            FILTER .name = 'Target1.1'
                        )
                    };
                """)

                await self.con.execute("""
                    DELETE (SELECT Target1
                            FILTER .name = 'Target1.1');
                """)

                exception_is_deferred = True

        self.assertTrue(exception_is_deferred)

    async def test_link_on_target_delete_deferred_restrict_02(self):
        exception_is_deferred = False

        with self.assertRaisesRegex(
                edgedb.ConstraintViolationError,
                'deletion of default::Target1 .* is prohibited by link'):

            async with self.con.transaction():
                await self.con.execute("""
                    INSERT Target1 {
                        name := 'Target1.1'
                    };

                    INSERT Source3 {
                        name := 'Source3.1',
                        tgt1_deferred_restrict := (
                            SELECT Target1
                            FILTER .name = 'Target1.1'
                        )
                    };
                """)

                await self.con.execute("""
                    DELETE (SELECT Target1
                            FILTER .name = 'Target1.1');
                """)

                exception_is_deferred = True

        self.assertTrue(exception_is_deferred)

    async def test_link_on_target_delete_deferred_restrict_03(self):
        success = False

        async with self._run_and_rollback():
            await self.con.execute("""

                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_deferred_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                DELETE Named;
            """)

            success = True

        self.assertTrue(success)

    async def test_link_on_target_delete_deferred_restrict_04(self):
        try:
            async with self.con.transaction():
                await self.con.execute(r"""

                    INSERT Target1 {
                        name := 'Target4.1'
                    };

                    INSERT Source1 {
                        name := 'Source4.1',
                        tgt1_deferred_restrict := (
                            SELECT Target1
                            FILTER .name = 'Target4.1'
                        )
                    };

                    # delete the target with deferred trigger
                    DELETE (SELECT Target1
                            FILTER .name = 'Target4.1');

                    # assign a new target to the `tgt1_deferred_restrict`
                    INSERT Target1 {
                        name := 'Target4.2'
                    };

                    UPDATE Source1
                    FILTER Source1.name = 'Source4.1'
                    SET {
                        tgt1_deferred_restrict := (
                            SELECT Target1
                            FILTER .name = 'Target4.2'
                        )
                    };
                """)

            await self.assert_query_result(
                r'''
                    SELECT Target1 { name }
                    FILTER .name = 'Target4.2';
                ''',
                [{'name': 'Target4.2'}]
            )

        finally:
            # cleanup
            await self.con.execute("""
                DELETE (SELECT Source1
                        FILTER .name = 'Source4.1');
                DELETE (SELECT Target1
                        FILTER .name = 'Target4.2');
            """)

    async def test_link_on_target_delete_allow_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_allow := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1 {
                            tgt1_allow: {
                                name
                            }
                        };
                ''',
                [{
                    'tgt1_allow': {'name': 'Target1.1'},
                }]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1 {
                            tgt1_allow: {
                                name
                            }
                        }
                    FILTER
                        .name = 'Source1.1';
                ''',
                [{
                    'tgt1_allow': None,
                }]
            )

    async def test_link_on_target_delete_allow_02(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source3 {
                    name := 'Source3.1',
                    tgt1_allow := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source3 {
                            tgt1_allow: {
                                name
                            }
                        };
                ''',
                [{
                    'tgt1_allow': {'name': 'Target1.1'},
                }]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source3 {
                            tgt1_allow: {
                                name
                            }
                        }
                    FILTER
                        .name = 'Source3.1';
                ''',
                [{
                    'tgt1_allow': None,
                }]
            )

    async def test_link_on_target_delete_allow_03(self):
        async with self._run_and_rollback():
            await self.con.execute("""

                FOR name IN {'Target1.1', 'Target1.2', 'Target1.3'}
                UNION (
                    INSERT Target1 {
                        name := name
                    });

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_m2m_allow := (
                        SELECT Target1
                        FILTER
                            .name IN {'Target1.1', 'Target1.2', 'Target1.3'}
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1 {
                            name,
                            tgt1_m2m_allow: {
                                name
                            } ORDER BY .name
                        }
                    FILTER
                        .name = 'Source1.1';
                ''',
                [{
                    'name': 'Source1.1',
                    'tgt1_m2m_allow': [
                        {'name': 'Target1.1'},
                        {'name': 'Target1.2'},
                        {'name': 'Target1.3'},
                    ],
                }]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1 {
                            name,
                            tgt1_m2m_allow: {
                                name
                            } ORDER BY .name
                        }
                    FILTER
                        .name = 'Source1.1';
                ''',
                [{
                    'name': 'Source1.1',
                    'tgt1_m2m_allow': [
                        {'name': 'Target1.2'},
                        {'name': 'Target1.3'},
                    ],
                }]
            )

            await self.assert_query_result(
                r'''
                    SELECT
                        Target1 {
                            name
                        }
                    FILTER
                        .name LIKE 'Target1%'
                    ORDER BY
                        .name;
                ''',
                [
                    {'name': 'Target1.2'},
                    {'name': 'Target1.3'},
                ]
            )

    async def test_link_on_target_delete_delete_source_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };
                INSERT Target1 {
                    name := 'Target1.2'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                INSERT Source2 {
                    name := 'Source2.1',
                    src1_del_source := (
                        SELECT Source1
                        FILTER .name = 'Source1.1'
                    ),
                    tgt_m2m := (
                        SELECT Target1
                        FILTER .name = 'Target1.2'
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source2 {
                            src1_del_source: {
                                name,
                                tgt1_del_source: {
                                    name
                                }
                            }
                        }
                    FILTER
                        .name = 'Source2.1';
                ''',
                [{
                    'src1_del_source': {
                        'name': 'Source1.1',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    }
                }]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source2
                    FILTER
                        .name = 'Source2.1';
                ''',
                []
            )
            await self.assert_query_result(
                r'''
                    SELECT
                        Source1
                    FILTER
                        .name = 'Source1.1';
                ''',
                [],
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.2');
            """)

    async def test_link_on_target_delete_delete_source_02(self):
        async with self._run_and_rollback():
            await self.con.execute("""

                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                INSERT Source1 {
                    name := 'Source1.2',
                    tgt1_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                INSERT Source2 {
                    name := 'Source2.1',
                    src1_del_source := (
                        SELECT Source1
                        FILTER .name = 'Source1.1'
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source2 {
                            src1_del_source: {
                                name,
                                tgt1_del_source: {
                                    name
                                }
                            }
                        }
                    FILTER
                        .name = 'Source2.1';
                ''',
                [{
                    'src1_del_source': {
                        'name': 'Source1.1',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    }
                }]
            )

            await self.assert_query_result(
                r'''
                SELECT
                    Source1 {
                        name,
                        tgt1_del_source: {
                            name
                        }
                    }
                FILTER
                    .name LIKE 'Source1%';
                ''',
                [
                    {
                        'name': 'Source1.1',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    },
                    {
                        'name': 'Source1.2',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    }
                ]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source2
                    FILTER
                        .name = 'Source2.1';
                ''',
                [],
            )

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1
                    FILTER
                        .name LIKE 'Source1%';
                ''',
                [],
            )

    async def test_link_on_target_delete_delete_source_03(self):
        async with self._run_and_rollback():
            await self.con.execute("""

                FOR name IN {'Target1.1', 'Target1.2', 'Target1.3'}
                UNION (
                    INSERT Target1 {
                        name := name
                    });

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_m2m_del_source := (
                        SELECT Target1
                        FILTER
                            .name IN {'Target1.1', 'Target1.2', 'Target1.3'}
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1 {
                            name,
                            tgt1_m2m_del_source: {
                                name
                            } ORDER BY .name
                        }
                    FILTER
                        .name = 'Source1.1';
                ''',
                [{
                    'name': 'Source1.1',
                    'tgt1_m2m_del_source': [
                        {'name': 'Target1.1'},
                        {'name': 'Target1.2'},
                        {'name': 'Target1.3'},
                    ],
                }]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1 {
                            name,
                            tgt1_m2m_del_source: {
                                name
                            } ORDER BY .name
                        }
                    FILTER
                        .name = 'Source1.1';
                ''',
                []
            )

            await self.assert_query_result(
                r'''
                    SELECT
                        Target1 {
                            name
                        }
                    FILTER
                        .name LIKE 'Target1%'
                    ORDER BY
                        .name;
                ''',
                [
                    {'name': 'Target1.2'},
                    {'name': 'Target1.3'},
                ]
            )

    async def test_link_on_target_delete_delete_source_04(self):
        async with self._run_and_rollback():
            await self.con.execute("""

                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source3 {
                    name := 'Source3.1',
                    tgt1_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                INSERT Source3 {
                    name := 'Source3.2',
                    tgt1_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                INSERT Source2 {
                    name := 'Source2.1',
                    src1_del_source := (
                        SELECT Source3
                        FILTER .name = 'Source3.1'
                    )
                };
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source2 {
                            src1_del_source: {
                                name,
                                tgt1_del_source: {
                                    name
                                }
                            }
                        }
                    FILTER
                        .name = 'Source2.1';
                ''',
                [{
                    'src1_del_source': {
                        'name': 'Source3.1',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    }
                }]
            )

            await self.assert_query_result(
                r'''
                SELECT
                    Source3 {
                        name,
                        tgt1_del_source: {
                            name
                        }
                    }
                FILTER
                    .name LIKE 'Source3%';
                ''',
                [
                    {
                        'name': 'Source3.1',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    },
                    {
                        'name': 'Source3.2',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    }
                ]
            )

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source3
                    FILTER
                        .name LIKE 'Source3%';
                ''',
                [],
            )

            await self.assert_query_result(
                r'''
                    SELECT
                        Source2
                    FILTER
                        .name = 'Source2.1';
                ''',
                [],
            )

    async def test_link_on_target_delete_delete_source_05(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT ChildSource1 {
                    name := 'Source1.1',
                    tgt1_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        ChildSource1
                    FILTER
                        .name = 'Source1.1';
                ''',
                []
            )

    async def test_link_on_target_delete_delete_source_06(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.1'
                };

                INSERT Source1 {
                    name := 'Source1.1',
                    tgt_union_m2m_del_source := (
                        SELECT Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            await self.con.execute("""
                DELETE (SELECT Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(
                r'''
                    SELECT
                        Source1
                    FILTER
                        .name = 'Source1.1';
                ''',
                []
            )

    async def test_link_on_target_delete_loop_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                insert Source1 {
                    name := 'Source1.1',
                    self_del_source := detached (
                        insert Source1 {
                            name := 'Source1.2',
                            self_del_source := detached (
                                insert Source1 { name := 'Source1.3' }
                            )
                        }
                    )
                };
                update Source1 filter .name = 'Source1.3' set {
                    self_del_source := detached (
                        select Source1 filter .name = 'Source1.1'
                    )
                };
            """)

            await self.con.execute("""
                delete Source1 filter .name = 'Source1.1'
            """)

            await self.assert_query_result(
                r'''
                    select Source1
                ''',
                []
            )

    async def test_link_on_source_delete_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_del_target := (
                        INSERT Target1 {
                            name := 'Target1.1',
                            extra_tgt := (detached (
                                INSERT Target1 { name := "t2" })),
                        }
                    )
                };
            """)

            await self.con.execute("""
                DELETE Source1 filter .name = 'Source1.1'
            """)

            await self.assert_query_result(
                r'''
                    SELECT Target1
                    FILTER .name = 'Target1.1';
                ''',
                []
            )

            # Make sure that the link tables get cleared when a policy
            # deletes an object
            await self.con.execute("""
                DELETE Target1 filter .name = 't2'
            """)

    async def test_link_on_source_delete_02(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Source1 {
                    name := 'Source1.1',
                    tgt1_m2m_del_target := {
                        (INSERT Target1 {name := 'Target1.1'}),
                        (INSERT Target1 {name := 'Target1.2'}),
                    }
                };
            """)

            await self.con.execute("""
                DELETE Source1 filter .name = 'Source1.1'
            """)

            await self.assert_query_result(
                r'''
                    SELECT Target1
                ''',
                []
            )

    async def test_link_on_source_delete_03(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT Source1 {
                    name := 'Source1.1',
                    self_del_target := detached (
                        insert Source1 {
                            name := 'Source1.2',
                            self_del_target := detached (
                                insert Source1 { name := 'Source1.3' }
                            )
                        }
                    )
                };
            """)

            await self.con.execute("""
                DELETE Source1 filter .name = 'Source1.1'
            """)

            await self.assert_query_result(
                r'''
                    SELECT Source1
                ''',
                []
            )

    async def test_link_on_source_delete_orphan_01(self):
        # Try all the permutations of parent and child classes
        for src1, src2, tgt in itertools.product(
            ('Source1', 'Source3'),
            ('Source1', 'Source3'),
            ('Target1', 'Target1Child'),
        ):
            async with self._run_and_rollback():
                q = f"""
                    INSERT {src1} {{
                        name := 'Source1.1',
                        tgt1_del_target_orphan := (
                            INSERT {tgt} {{
                                name := 'Target1.1'
                            }}
                        )
                    }};
                    INSERT {src2} {{
                        name := 'Source1.2',
                        tgt1_del_target_orphan := (
                            SELECT Target1 FILTER .name = 'Target1.1'
                        )
                    }};
                """
                await self.con.execute(q)

                await self.con.execute("""
                    DELETE Source1 filter .name = 'Source1.1'
                """)

                await self.assert_query_result(
                    r'''
                        SELECT Target1
                        FILTER .name = 'Target1.1';
                    ''',
                    [{}]
                )

                await self.con.execute("""
                    DELETE Source1 filter .name = 'Source1.2'
                """)

                await self.assert_query_result(
                    r'''
                        SELECT Target1
                        FILTER .name = 'Target1.1';
                    ''',
                    []
                )

    async def test_link_on_source_delete_orphan_02(self):
        # Try all the permutations of parent and child classes
        for src1, src2, tgt1, tgt2 in itertools.product(
            ('Source1', 'Source3'),
            ('Source1', 'Source3'),
            ('Target1', 'Target1Child'),
            ('Target1', 'Target1Child'),
        ):
            async with self._run_and_rollback():
                q = f"""
                    INSERT {src1} {{
                        name := 'Source1.1',
                        tgt1_m2m_del_target_orphan := {{
                            (INSERT {tgt1} {{ name := 'Target1.1'}}),
                            (INSERT {tgt2} {{ name := 'Target1.2'}}),
                        }}
                    }};
                    INSERT {src2} {{
                        name := 'Source1.2',
                        tgt1_m2m_del_target_orphan := (
                            SELECT Target1 FILTER .name = 'Target1.1'
                        )
                    }};
                """
                await self.con.execute(q)

                await self.con.execute("""
                    DELETE Source1 filter .name = 'Source1.1'
                """)

                await self.assert_query_result(
                    r'''
                        SELECT Target1 { name }
                        FILTER .name LIKE 'Target1.%';
                    ''',
                    [{'name': "Target1.1"}]
                )

                await self.con.execute("""
                    DELETE Source1 filter .name = 'Source1.2'
                """)

                await self.assert_query_result(
                    r'''
                        SELECT Target1
                        FILTER .name = 'Target1.1';
                    ''',
                    []
                )

    async def _cycle_test(self):
        await self.con.execute("""
            insert Source1 {
                name := 'Source1.1',
                self_del_target := detached (
                    insert Source1 {
                        name := 'Source1.2',
                        self_del_target := detached (
                            insert Source1 { name := 'Source1.3' }
                        )
                    }
                )
            };
            update Source1 filter .name = 'Source1.3' set {
                self_del_target := detached (
                    select Source1 filter .name = 'Source1.1'
                )
            };
        """)

        await self.con.execute("""
            delete Source1 filter .name = 'Source1.1'
        """)

        await self.assert_query_result(
            r'''
                select Source1
            ''',
            []
        )

    async def test_link_on_source_delete_cycle_01(self):
        async with self._run_and_rollback():
            await self._cycle_test()

    async def test_link_on_source_delete_cycle_02(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                alter type Source1 alter link self_del_target
                on target delete delete source
            """)

            await self._cycle_test()

    async def test_link_on_source_delete_cycle_03(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                alter type Source1 alter link self_del_target
                on target delete allow
            """)

            await self._cycle_test()

    async def test_link_on_source_delete_cycle_04(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                alter type Source1 alter link self_del_target
                on target delete deferred restrict
            """)

            await self._cycle_test()


class TestLinkTargetDeleteMigrations(stb.DDLTestCase):

    SCHEMA = pathlib.Path(__file__).parent / 'schemas' / 'link_tgt_del.esdl'

    async def test_link_on_target_delete_migration_01(self):
        async with self._run_and_rollback():

            schema_f = (pathlib.Path(__file__).parent / 'schemas' /
                        'link_tgt_del_migrated.esdl')

            with open(schema_f) as f:
                schema = f.read()

            await self.migrate(schema)

            await self.con.execute('''
                INSERT Target1 {name := 'Target1_migration_01'};

                INSERT ObjectType4 {
                    foo := (
                        SELECT Target1
                        FILTER .name = 'Target1_migration_01'
                    )
                };

                DELETE (
                    SELECT AbstractObjectType
                    FILTER .foo.name = 'Target1_migration_01'
                );
            ''')

    async def test_link_on_target_delete_migration_02(self):
        async with self._run_and_rollback():

            schema_f = (pathlib.Path(__file__).parent / 'schemas' /
                        'link_tgt_del_migrated.esdl')

            with open(schema_f) as f:
                schema = f.read()

            await self.migrate(schema)

            await self.con.execute("""
                INSERT Target1 {
                    name := 'Target1.m02'
                };

                INSERT Source1 {
                    name := 'Source1.m02',
                    tgt1_deferred_restrict := (
                        SELECT Target1
                        FILTER .name = 'Target1.m02'
                    )
                };
            """)

            # Post-migration the deletion trigger must fire immediately,
            # since the policy is no longer "DEFERRED"
            with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError,
                    'deletion of default::Target1 .* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT Target1 FILTER .name = 'Target1.m02');
                """)
