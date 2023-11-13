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
        tx = self.con.transaction()
        await tx.start()
        try:
            await self._ensure_schema_data_integrity()
        finally:
            await tx.rollback()

    async def _ensure_schema_data_integrity(self):
        await self.assert_query_result(
            r'''
                select count(L2)
            ''',
            [
                999
            ]
        )

        await self.assert_query_result(
            r'''
                with x := range_unpack(range(1, 1000))
                select all(
                    L2.vec in <v3>[x % 10, math::ln(x), x / 7 % 13]
                )
            ''',
            [
                True
            ]
        )

        # We put pgvector dump tests in v4 dump even though they
        # shipped in 3.0-rc3 (shipping pgvector was a wild ride). It
        # doesn't seem worth adding a second v4 dump test for (both
        # ergonomically and because it would be slower), so just quit
        # early in that case.
        if (
            self._testMethodName
            == 'test_dumpv4_restore_compatibility_3_0'
        ):
            return

        await self.assert_query_result(
            '''
                select cfg::Config {
                    conf := assert_single(.extensions[is ext::_conf::Config] {
                        config_name,
                        objs: { name, value, [is ext::_conf::SubObj].extra,
                                tname := .__type__.name }
                              order by .name,
                    })
                };
            ''',
            [dict(conf=dict(
                config_name='ready',
                objs=[
                    dict(name='1', value='foo', tname='ext::_conf::Obj'),
                    dict(name='2', value='bar', tname='ext::_conf::Obj'),
                    dict(name='3', value='baz', extra=42,
                         tname='ext::_conf::SubObj'),
                    # No SecretObj
                ],
            ))]
        )

        # Secret shouldn't make it
        await self.assert_query_result(
            '''
            select ext::_conf::get_top_secret()
            ''',
            [],
        )


class TestDumpV4(tb.StableDumpTestCase, DumpTestCaseMixin):
    EXTENSIONS = ["pgvector", "_conf", "pgcrypto", "auth"]
    BACKEND_SUPERUSER = True

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump_v4_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump_v4_setup.edgeql')

    async def test_dumpv4_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)


class TestDumpV4Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dumpv4',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    BACKEND_SUPERUSER = True
