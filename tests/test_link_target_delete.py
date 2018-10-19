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


import pathlib

from edb.client import exceptions

from edb.lang import _testbase as tb
from edb.lang.schema import error as s_err
from edb.lang.schema import links as s_links

from edb.server import _testbase as stb


class TestLinkTargetDeleteSchema(tb.BaseSchemaTest):
    def test_schema_on_target_delete_01(self):
        schema = self.load_schema("""
            type Object:
                link foo -> Object:
                    on target delete set empty

                link bar -> Object
        """)

        obj = schema.get('test::Object')

        self.assertEqual(obj.getptr(schema, 'foo').on_target_delete,
                         s_links.LinkTargetDeleteAction.SET_EMPTY)

        self.assertEqual(obj.getptr(schema, 'bar').on_target_delete,
                         s_links.LinkTargetDeleteAction.RESTRICT)

    def test_schema_on_target_delete_02(self):
        schema = self.load_schema("""
            type Object:
                link foo -> Object:
                    on target delete set empty

            type Object2 extending Object:
                inherited link foo -> Object:
                    title := "Foo"

            type Object3 extending Object:
                inherited link foo -> Object:
                    on target delete restrict
        """)

        obj2 = schema.get('test::Object2')
        self.assertEqual(obj2.getptr(schema, 'foo').on_target_delete,
                         s_links.LinkTargetDeleteAction.SET_EMPTY)

        obj3 = schema.get('test::Object3')
        self.assertEqual(obj3.getptr(schema, 'foo').on_target_delete,
                         s_links.LinkTargetDeleteAction.RESTRICT)

    @tb.must_fail(s_err.SchemaError,
                  "cannot implicitly resolve the `on target delete` action "
                  "for 'test::C.foo'")
    def test_schema_on_target_delete_03(self):
        """
            type A:
                link foo -> Object:
                    on target delete restrict

            type B:
                link foo -> Object:
                    on target delete set empty

            type C extending A, B
        """


class TestLinkTargetDeleteDeclarative(stb.QueryTestCase):
    SCHEMA = pathlib.Path(__file__).parent / 'schemas' / 'link_tgt_del.eschema'
    ISOLATED_METHODS = False

    async def test_link_on_target_delete_restrict_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::Target1 {
                    name := 'Target1.1'
                };

                INSERT test::Source1 {
                    name := 'Source1.1',
                    tgt1_restrict := (
                        SELECT test::Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
                    'deletion of test::Target1 .* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT test::Target1 FILTER .name = 'Target1.1');
                """)

    async def test_link_on_target_delete_restrict_02(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::Target1Child {
                    name := 'Target1Child.1'
                };

                INSERT test::Source1 {
                    name := 'Source1.1',
                    tgt1_restrict := (
                        SELECT test::Target1
                        FILTER .name = 'Target1Child.1'
                    )
                };
            """)

            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
                    'deletion of test::Target1 .* is prohibited by link'):
                await self.con.execute("""
                    DELETE (SELECT test::Target1Child
                            FILTER .name = 'Target1Child.1');
                """)

    async def test_link_on_target_delete_deferred_restrict_01(self):
            exception_is_deferred = False

            with self.assertRaisesRegex(
                    exceptions.ConstraintViolationError,
                    'deletion of test::Target1 .* is prohibited by link'):

                async with self.con.transaction():
                    await self.con.execute("""
                        INSERT test::Target1 {
                            name := 'Target1.1'
                        };

                        INSERT test::Source1 {
                            name := 'Source1.1',
                            tgt1_deferred_restrict := (
                                SELECT test::Target1
                                FILTER .name = 'Target1.1'
                            )
                        };
                    """)

                    await self.con.execute("""
                        DELETE (SELECT test::Target1
                                FILTER .name = 'Target1.1');
                    """)

                    exception_is_deferred = True

            self.assertTrue(exception_is_deferred)

    async def test_link_on_target_delete_set_empty_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::Target1 {
                    name := 'Target1.1'
                };

                INSERT test::Source1 {
                    name := 'Source1.1',
                    tgt1_set_empty := (
                        SELECT test::Target1
                        FILTER .name = 'Target1.1'
                    )
                };
            """)

            await self.assert_query_result(r'''
                WITH MODULE test
                SELECT
                    Source1 {
                        tgt1_set_empty: {
                            name
                        }
                    };
            ''', [
                [{
                    'tgt1_set_empty': {'name': 'Target1.1'},
                }]
            ])

            await self.con.execute("""
                DELETE (SELECT test::Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(r'''
                WITH MODULE test
                SELECT
                    Source1 {
                        tgt1_set_empty: {
                            name
                        }
                    };
            ''', [
                [{
                    'tgt1_set_empty': None,
                }]
            ])

    async def test_link_on_target_delete_delete_source_01(self):
        async with self._run_and_rollback():
            await self.con.execute("""
                INSERT test::Target1 {
                    name := 'Target1.1'
                };

                INSERT test::Source1 {
                    name := 'Source1.1',
                    tgt1_del_source := (
                        SELECT test::Target1
                        FILTER .name = 'Target1.1'
                    )
                };

                INSERT test::Source2 {
                    name := 'Source2.1',
                    src1_del_source := (
                        SELECT test::Source1
                        FILTER .name = 'Source1.1'
                    )
                };
            """)

            await self.assert_query_result(r'''
                WITH MODULE test
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
            ''', [
                [{
                    'src1_del_source': {
                        'name': 'Source1.1',
                        'tgt1_del_source': {'name': 'Target1.1'},
                    }
                }]
            ])

            await self.con.execute("""
                DELETE (SELECT test::Target1 FILTER .name = 'Target1.1');
            """)

            await self.assert_query_result(r'''
                WITH MODULE test
                SELECT
                    Source2
                FILTER
                    .name = 'Source2.1';
            ''', [
                []
            ])
