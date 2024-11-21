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

    async def ensure_schema_data_integrity(self, include_secrets=False):
        async for tx in self._run_and_rollback_retrying():
            async with tx:
                await self._ensure_schema_data_integrity(
                    include_secrets=include_secrets)

    async def _ensure_schema_data_integrity(self, include_secrets):
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
                    L2.vec in <v3>[x % 10, std::math::ln(x), x / 7 % 13]
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

        if include_secrets:
            secrets = [
                dict(name='4', value='spam', extra=None,
                     tname='ext::_conf::SecretObj')
            ]
        else:
            secrets = []

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
                    *secrets,
                ],
            ))]
        )

        await self.assert_query_result(
            '''
            select ext::_conf::get_top_secret()
            ''',
            ['secret'] if include_secrets else [],
        )

        # __fts_document__ should be repopulated
        await self.assert_query_result(
            r'''
            SELECT fts::search(L3, 'satisfying').object { x }
            ''',
            [
                {
                    'x': 'satisfied customer',
                },
            ],
        )

        if include_secrets:
            await self.assert_query_result(
                '''
                    select cfg::Config.extensions[is ext::auth::AuthConfig] {
                      providers: { name } order by .name
                    };
                ''',
                [{
                    'providers': [
                        {'name': 'builtin::local_emailpassword'},
                        {'name': 'builtin::oauth_apple'},
                        {'name': 'builtin::oauth_azure'},
                        {'name': 'builtin::oauth_github'},
                        {'name': 'builtin::oauth_google'},
                    ]
                }]
            )

        # We didn't specify include_secrets in the dumps we made for
        # 4.0, but the way that smtp config was done then, it got
        # dumped anyway. (The secret wasn't specified.)
        has_smtp = (
            include_secrets
            or self._testMethodName == 'test_dumpv4_restore_compatibility_4_0'
        )

        # N.B: This is not what it looked like in the original
        # dumps. We patched it up during restore starting with 6.0.
        if has_smtp:
            await self.assert_query_result(
                '''
                select cfg::Config {
                    email_providers[is cfg::SMTPProviderConfig]: {
                        name, sender
                    },
                    current_email_provider_name,
                };
                ''',
                [
                    {
                        "email_providers": [
                            {
                                "name": "_default",
                                "sender": "noreply@example.com",
                            }
                        ],
                        "current_email_provider_name": "_default"
                    }
                ],
            )


class TestDumpV4(tb.StableDumpTestCase, DumpTestCaseMixin):
    EXTENSIONS = ["pgvector", "_conf", "pgcrypto", "auth"]
    BACKEND_SUPERUSER = True

    SCHEMA_DEFAULT = os.path.join(os.path.dirname(__file__), 'schemas',
                                  'dump_v4_default.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'dump_v4_setup.edgeql')

    async def test_dump_v4_dump_restore(self):
        await self.check_dump_restore(
            DumpTestCaseMixin.ensure_schema_data_integrity)

    async def test_dump_v4_dump_restore_secrets(self):
        await self.check_dump_restore(
            lambda self: self.ensure_schema_data_integrity(
                include_secrets=True),
            include_secrets=True,
        )

    async def test_dump_v4_branch_data(self):
        await self.check_branching(
            include_data=True,
            check_method=lambda self: self.ensure_schema_data_integrity(
                include_secrets=True))


class TestDumpV4Compat(
    tb.DumpCompatTestCase,
    DumpTestCaseMixin,
    dump_subdir='dumpv4',
    check_method=DumpTestCaseMixin.ensure_schema_data_integrity,
):
    BACKEND_SUPERUSER = True
