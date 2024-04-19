#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2020-present MagicStack Inc. and the EdgeDB authors.
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

from edb.testbase import server as tb


class DumpTestCaseMixin:

    async def ensure_schema_data_integrity(self):
        async for tx in self._run_and_rollback_retrying():
            async with tx:
                await self._ensure_schema_data_integrity()

    async def _ensure_schema_data_integrity(self):
        await self.assert_query_result(
            r'''
            SELECT _ := schema::Module.name
            FILTER _ LIKE 'default%'
            ''',
            {'default', 'default::nested', 'default::back`ticked'},
        )

        # We don't bother to validate these but we need them to work
        await self.con.query('describe schema as sdl')
        await self.con.query('describe schema as ddl')

        # We took a dev version snapshot for 3.0, but then needed to
        # add more stuff to the 3.0 dump tests. It didn't seem worth
        # adding a new dump test for it (both ergonomically and
        # because it would be slower), so just quit early in that case.
        if (
            self._testMethodName
            == 'test_dumpv3_restore_compatibility_3_0_dev_7258'
        ):
            return

        await self.assert_query_result(
            r'''
            select schema::Migration { script, message, generated_by }
            order by exists .parents then exists .parents.parents
            limit 3
            ''',
            [
                {"message": None, "generated_by": None},
                {"message": "test", "generated_by": None},
                {"message": None, "generated_by": "DDLStatement"},
            ],
        )

        await self.assert_query_result(
            r'''
            select schema::Trigger {
                name, scope, kinds, sname := .subject.name
            };
            ''',
            [
                {
                    "name": "log",
                    "scope": "Each",
                    "kinds": ["Insert"],
                    "sname": "default::Foo"
                }
            ]
        )
        await self.assert_query_result(
            r'''
            select schema::Rewrite {
                sname := .subject.source.name ++ "." ++ .subject.name,
                name,
            };
            ''',
            tb.bag([
                {"sname": "default::Log.timestamp", "name": "Insert"},
                {"sname": "default::Log.timestamp", "name": "Update"},
            ]),
        )
        await self.assert_query_result(
            r'''
            select schema::AccessPolicy { name, errmessage }
            filter .name = 'whatever_no';
            ''',
            [{"name": "whatever_no", "errmessage": "aaaaaa"}],
        )

        await self.assert_query_result(
            r'''
            select cfg::Config.allow_user_specified_id;
            ''',
            [True],
        )
        await self.assert_query_result(
            r'''
            select <str>cfg::Config.query_execution_timeout;
            ''',
            ['PT1H20M13S'],
        )


class TestDumpV3(tb.StableDumpTestCase, DumpTestCaseMixin):
    DEFAULT_MODULE = 'test'

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump_v3_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump_v3_setup.edgeql')

    async def test_dump_v3_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump_v3_branch_data(self):
        await self.check_branching(
            include_data=True,
            check_method=DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDumpV3Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dumpv3',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    pass
