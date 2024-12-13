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
import textwrap

import edgedb

import edb.buildmeta
from edb.testbase import server as tb


class TestDDLExtensions(tb.DDLTestCase):
    TRANSACTION_ISOLATION = False
    PARALLELISM_GRANULARITY = 'suite'

    async def _extension_test_01(self):
        await self.con.execute('''
            create extension ltree version '1.0'
        ''')

        await self.assert_query_result(
            '''
                select ext::ltree::nlevel(
                  <ext::ltree::ltree><json><ext::ltree::ltree>'foo.bar');
            ''',
            [2],
        )
        await self.assert_query_result(
            '''
                select ext::ltree::asdf(
                  <ext::ltree::ltree><json><ext::ltree::ltree>'foo.bar');
            ''',
            [3],
        )
        await self.assert_query_result(
            '''
                select <str>(
                  <ext::ltree::ltree><json><ext::ltree::ltree>'foo.bar');
            ''',
            ['foo.bar'],
        )
        await self.assert_query_result(
            '''
                select <ext::ltree::ltree>'foo.bar'
                     = <ext::ltree::ltree>'foo.baz';
            ''',
            [False],
        )
        await self.assert_query_result(
            '''
                select <ext::ltree::ltree>'foo.bar'
                     != <ext::ltree::ltree>'foo.baz';
            ''',
            [True],
        )
        await self.assert_query_result(
            '''
                select <ext::ltree::ltree><json><ext::ltree::ltree>'foo.bar';
            ''',
            [['foo', 'bar']],
            json_only=True,
        )

        await self.con.execute('''
            create type Foo { create property x -> ext::ltree::ltree };
            insert Foo { x := <ext::ltree::ltree>'foo.bar.baz' };
        ''')

        await self.assert_query_result(
            '''
                select Foo.x;
            ''',
            [['foo', 'bar', 'baz']],
            json_only=True,
        )

        await self.con.execute("""
            START MIGRATION TO {
                using extension ltree version '2.0';
                module default {
                    type Foo { property x -> ext::ltree::ltree };
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.assert_query_result(
            '''
                select ext::ltree::hash(<ext::ltree::ltree>'foo.bar');
            ''',
            [-2100566927],
        )
        await self.con.execute('''
            drop type Foo;
            drop extension ltree;
        ''')

    async def test_edgeql_extensions_01(self):
        pg_ver = edb.buildmeta.parse_pg_version(await self.con.query_single('''
            select sys::_postgres_version();
        '''))
        # Skip if postgres is old, since it doesn't have ltree 1.3
        if pg_ver.major < 17:
            self.skipTest('Postgres version too old')

        # Make an extension that wraps a tiny bit of the ltree package.
        await self.con.execute('''
        create extension package ltree VERSION '1.0' {
          set ext_module := "ext::ltree";
          set sql_extensions := ["ltree >=1.2,<1.3"];

          set sql_setup_script := $$
            CREATE FUNCTION edgedb.asdf(val edgedb.ltree) RETURNS int4
             LANGUAGE sql
             STRICT
             IMMUTABLE
            AS $function$
            SELECT edgedb.nlevel(val) + 1
            $function$;
          $$;

          set sql_teardown_script := $$
            DROP FUNCTION edgedb.asdf(edgedb.ltree);
          $$;

          create module ext::ltree;
          create scalar type ext::ltree::ltree extending anyscalar {
            set sql_type := "ltree";
          };
          create cast from ext::ltree::ltree to std::str {
            SET volatility := 'Immutable';
            USING SQL CAST;
          };
          create cast from std::str to ext::ltree::ltree {
            SET volatility := 'Immutable';
            USING SQL CAST;
          };

          # Use a non-trivial json representation just to show that we can.
          create cast from ext::ltree::ltree to std::json {
            SET volatility := 'Immutable';
            USING SQL $$
              select to_jsonb(string_to_array("val"::text, '.'));
            $$
          };
          create cast from std::json to ext::ltree::ltree {
            SET volatility := 'Immutable';
            USING SQL $$
              select string_agg(edgedb.raise_on_null(
                edgedbstd."std|cast@std|json@std|str_f"(z.z),
                'invalid_parameter_value', 'invalid null value in cast'),
              '.')::ltree
              from unnest(
                edgedbstd."std|cast@std|json@array<std||json>_f"("val"))
                as z(z);
            $$
          };
          create function ext::ltree::nlevel(
                v: ext::ltree::ltree) -> std::int32 {
            using sql function 'edgedb.nlevel';
          };
          create function ext::ltree::asdf(v: ext::ltree::ltree) -> std::int32 {
            using sql function 'edgedb.asdf';
          };
        };

        create extension package ltree VERSION '2.0' {
          set ext_module := "ext::ltree";
          set sql_extensions := ["ltree >=1.3,<10.0"];

          set sql_setup_script := $$
            CREATE FUNCTION edgedb.asdf(val edgedb.ltree) RETURNS int4
             LANGUAGE sql
             STRICT
             IMMUTABLE
            AS $function$
            SELECT edgedb.nlevel(val) + 1
            $function$;
          $$;

          set sql_teardown_script := $$
            DROP FUNCTION edgedb.asdf(edgedb.ltree);
          $$;

          create module ext::ltree;
          create scalar type ext::ltree::ltree extending anyscalar {
            set sql_type := "ltree";
          };
          create cast from ext::ltree::ltree to std::str {
            SET volatility := 'Immutable';
            USING SQL CAST;
          };
          create cast from std::str to ext::ltree::ltree {
            SET volatility := 'Immutable';
            USING SQL CAST;
          };

          # Use a non-trivial json representation just to show that we can.
          create cast from ext::ltree::ltree to std::json {
            SET volatility := 'Immutable';
            USING SQL $$
              select to_jsonb(string_to_array("val"::text, '.'));
            $$
          };
          create cast from std::json to ext::ltree::ltree {
            SET volatility := 'Immutable';
            USING SQL $$
              select string_agg(edgedb.raise_on_null(
                edgedbstd."std|cast@std|json@std|str_f"(z.z),
                'invalid_parameter_value', 'invalid null value in cast'),
              '.')::ltree
              from unnest(
                edgedbstd."std|cast@std|json@array<std||json>_f"("val"))
                as z(z);
            $$
          };
          create function ext::ltree::nlevel(
                v: ext::ltree::ltree) -> std::int32 {
            using sql function 'edgedb.nlevel';
          };
          create function ext::ltree::asdf(v: ext::ltree::ltree) -> std::int32 {
            using sql function 'edgedb.asdf';
          };
          create function ext::ltree::hash(v: ext::ltree::ltree) -> std::int32 {
            using sql function 'edgedb.hash_ltree';
          };
        };

        create extension package ltree migration from version '1.0'
             to version '2.0' {
          create function ext::ltree::hash(v: ext::ltree::ltree) -> std::int32 {
            using sql function 'edgedb.hash_ltree';
          };
        };

        ''')
        try:
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_01()
        finally:
            await self.con.execute('''
                drop extension package ltree VERSION '1.0';
                drop extension package ltree VERSION '2.0';
                drop extension package ltree migration from
                  VERSION '1.0' TO VERSION '2.0';
            ''')

    async def _extension_test_02a(self):
        await self.con.execute('''
            create extension varchar
        ''')

        await self.con.execute('''
            create scalar type vc5 extending ext::varchar::varchar<5>;
            create type X {
                create property foo: vc5;
            };
        ''')

        await self.assert_query_result(
            '''
                describe scalar type vc5;
            ''',
            [
                'create scalar type default::vc5 '
                'extending ext::varchar::varchar<5>;'
            ],
        )
        await self.assert_query_result(
            '''
                describe scalar type vc5 as sdl;
            ''',
            ['scalar type default::vc5 extending ext::varchar::varchar<5>;'],
        )

        await self.assert_query_result(
            '''
                select schema::ScalarType { arg_values }
                filter .name = 'default::vc5'
            ''',
            [{'arg_values': ['5']}],
        )

        await self.con.execute('''
            insert X { foo := <vc5>"0123456789" }
        ''')

        await self.assert_query_result(
            '''
                select X.foo
            ''',
            ['01234'],
            json_only=True,
        )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            "parameterized scalar types may not have constraints",
        ):
            await self.con.execute('''
                alter scalar type vc5 create constraint expression on (true);
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "invalid scalar type argument",
        ):
            await self.con.execute('''
                create scalar type fail extending ext::varchar::varchar<foo>;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "does not accept parameters",
        ):
            await self.con.execute('''
                create scalar type yyy extending str<1, 2>;
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            "incorrect number of arguments",
        ):
            await self.con.execute('''
                create scalar type yyy extending ext::varchar::varchar<1, 2>;
            ''')

        # If no params are specified, it just makes a normal scalar type
        await self.con.execute('''
            create scalar type vc extending ext::varchar::varchar {
                create constraint expression on (false);
            };
        ''')
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError,
            "invalid",
        ):
            await self.con.execute('''
                select <str><vc>'a';
            ''')

    async def _extension_test_02b(self):
        await self.con.execute(r"""
            START MIGRATION TO {
                using extension varchar version "1.0";
                module default {
                    scalar type vc5 extending ext::varchar::varchar<5>;
                    type X {
                        foo: vc5;
                    };
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.con.execute('''
            insert X { foo := <vc5>"0123456789" }
        ''')

        await self.assert_query_result(
            '''
                select X.foo
            ''',
            ['01234'],
            json_only=True,
        )

        # Try dropping everything that uses it but not the extension
        async with self._run_and_rollback():
            await self.con.execute(r"""
                START MIGRATION TO {
                    using extension varchar version "1.0";
                    module default {
                    }
                };
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            """)

        # Try dropping everything including the extension
        await self.con.execute(r"""
            START MIGRATION TO {
                module default {
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

    async def test_edgeql_extensions_02(self):
        # Make an extension that wraps some of varchar
        await self.con.execute('''
        create extension package varchar VERSION '1.0' {
          set ext_module := "ext::varchar";
          set sql_extensions := [];
          create module ext::varchar;
          create scalar type ext::varchar::varchar {
            create annotation std::description := 'why are we doing this';
            set id := <uuid>'26dc1396-0196-11ee-a005-ad0eaed0df03';
            set sql_type := "varchar";
            set sql_type_scheme := "varchar({__arg_0__})";
            set num_params := 1;
          };

          create cast from ext::varchar::varchar to std::str {
            SET volatility := 'Immutable';
            USING SQL CAST;
          };
          create cast from std::str to ext::varchar::varchar {
            SET volatility := 'Immutable';
            USING SQL CAST;
          };
          # This is meaningless but I need to test having an array in a cast.
          create cast from ext::varchar::varchar to array<std::float32> {
            SET volatility := 'Immutable';
            USING SQL $$
              select array[0.0]
            $$
          };

          create abstract index ext::varchar::with_param(
              named only lists: int64
          ) {
              set code := ' ((__col__) NULLS FIRST)';
          };

          create type ext::varchar::ParentTest extending std::BaseObject {
              create property foo -> str;
          };
          create type ext::varchar::ChildTest
              extending ext::varchar::ParentTest;
          create type ext::varchar::GrandChildTest
              extending ext::varchar::ChildTest;
        };
        ''')
        try:
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_02a()
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_02b()
        finally:
            await self.con.execute('''
                drop extension package varchar VERSION '1.0'
            ''')

    async def test_edgeql_extensions_03(self):
        await self.con.execute('''
        create extension package ltree_broken VERSION '1.0' {
          set ext_module := "ext::ltree";
          set sql_extensions := ["ltree >=1000.0"];
          create module ltree;
        };
        ''')
        try:
            async with self.assertRaisesRegexTx(
                    edgedb.UnsupportedBackendFeatureError,
                    r"could not find extension satisfying ltree >=1000.0: "
                    r"only found versions 1\."):
                await self.con.execute(r"""
                    CREATE EXTENSION ltree_broken;
                """)
        finally:
            await self.con.execute('''
                drop extension package ltree_broken VERSION '1.0'
            ''')

    async def test_edgeql_extensions_04(self):
        await self.con.execute('''
        create extension package ltree_broken VERSION '1.0' {
          set ext_module := "ext::ltree";
          set sql_extensions := ["loltree >=1.0"];
          create module ltree;
        };
        ''')
        try:
            async with self.assertRaisesRegexTx(
                    edgedb.UnsupportedBackendFeatureError,
                    r"could not find extension satisfying loltree >=1.0: "
                    r"extension not found"):
                await self.con.execute(r"""
                    CREATE EXTENSION ltree_broken;
                """)
        finally:
            await self.con.execute('''
                drop extension package ltree_broken VERSION '1.0'
            ''')

    async def _extension_test_05(self, in_tx):
        await self.con.execute('''
            create extension _conf
        ''')

        # Check that the ids are stable
        await self.assert_query_result(
            '''
            select schema::ObjectType {
                id,
                properties: { name, id } filter .name = 'value'
            } filter .name = 'ext::_conf::Obj'
            ''',
            [
                {
                    "id": "dc7c6ed1-759f-5a70-9bc3-2252b2d3980a",
                    "properties": [
                        {
                            "name": "value",
                            "id": "0dff1c2f-f51b-59fd-bae9-9d66cb963896",
                        },
                    ],
                },
            ],
        )

        Q = '''
            select cfg::%s {
                conf := assert_single(.extensions[is ext::_conf::Config] {
                    config_name,
                    opt_value,
                    obj: { name, value, fixed },
                    objs: { name, value, opt_value,
                            [is ext::_conf::SubObj].extra,
                            tname := .__type__.name }
                          order by .name,
                })
            };
        '''

        async def _check(_cfg_obj='Config', **kwargs):
            q = Q % _cfg_obj
            await self.assert_query_result(
                q,
                [{'conf': kwargs}],
            )

        await _check(
            config_name='',
            objs=[],
        )

        await self.con.execute('''
            configure current database set ext::_conf::Config::config_name :=
                "test";
        ''')

        await _check(
            config_name='test',
            opt_value=None,
            objs=[],
        )

        await self.con.execute('''
            configure current database set ext::_conf::Config::opt_value :=
                "opt!";
        ''')

        await self.con.execute('''
            configure current database set ext::_conf::Config::secret :=
                "foobaz";
        ''')

        await _check(
            config_name='test',
            opt_value='opt!',
            objs=[],
        )

        if not in_tx:
            with self.assertRaisesRegex(
                    edgedb.ConfigurationError, "is not allowed"):
                await self.con.execute('''
                    configure instance set ext::_conf::Config::config_name :=
                        "session!";
                ''')

        await self.con.execute('''
            configure session set ext::_conf::Config::config_name :=
                "session!";
        ''')

        await _check(
            config_name='session!',
            objs=[],
        )

        await self.con.execute('''
            configure session reset ext::_conf::Config::config_name;
        ''')

        await _check(
            config_name='test',
            objs=[],
        )

        await self.con.execute('''
            configure current database insert ext::_conf::Obj {
                name := '1',
                value := 'foo',
            };
        ''')
        await self.con.execute('''
            configure current database insert ext::_conf::Obj {
                name := '2',
                value := 'bar',
                opt_value := 'opt.',
            };
        ''')
        await self.con.execute('''
            configure current database insert ext::_conf::SubObj {
                name := '3',
                value := 'baz',
                extra := 42,
            };
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.ConfigurationError, "invalid setting value"
        ):
            await self.con.execute('''
                configure current database insert ext::_conf::SubObj {
                    name := '3!',
                    value := 'asdf_wrong',
                    extra := 42,
                };
            ''')

        # This is fine, constraint on value is delegated
        await self.con.execute('''
            configure current database insert ext::_conf::SecretObj {
                name := '4',
                value := 'foo',
                secret := '123456',
            };
        ''')

        # But this collides
        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, "value violate"
        ):
            await self.con.execute('''
                configure current database insert ext::_conf::SecretObj {
                    name := '5',
                    value := 'foo',
                };
            ''')

        await self.con.execute('''
            configure current database insert ext::_conf::SecretObj {
                name := '5',
                value := 'quux',
            };
        ''')
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, "protected"
        ):
            await self.con.execute('''
                configure current database insert ext::_conf::SingleObj {
                    name := 'single',
                    value := 'val',
                    fixed := 'variable??',
                };
            ''')

        await self.con.execute('''
            configure current database insert ext::_conf::SingleObj {
                name := 'single',
                value := 'val',
            };
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.ConstraintViolationError, ""
        ):
            await self.con.execute('''
                CONFIGURE CURRENT BRANCH INSERT ext::_conf::SingleObj {
                    name := 'fail',
                    value := '',
                };
            ''')

        await self.con.execute('''
            configure current database set ext::_conf::Config::config_name :=
                "ready";
        ''')

        await _check(
            config_name='ready',
            objs=[
                dict(name='1', value='foo', tname='ext::_conf::Obj',
                     opt_value=None),
                dict(name='2', value='bar', tname='ext::_conf::Obj',
                     opt_value='opt.'),
                dict(name='3', value='baz', extra=42,
                     tname='ext::_conf::SubObj', opt_value=None),
                dict(name='4', value='foo',
                     tname='ext::_conf::SecretObj', opt_value=None),
                dict(name='5', value='quux',
                     tname='ext::_conf::SecretObj', opt_value=None),
            ],
            obj=dict(name='single', value='val', fixed='fixed!'),
        )

        await self.assert_query_result(
            '''
            with c := cfg::Config.extensions[is ext::_conf::Config]
            select ext::_conf::get_secret(
              (select c.objs[is ext::_conf::SecretObj] filter .name = '4'))
            ''',
            ['123456'],
        )
        await self.assert_query_result(
            '''
            select ext::_conf::get_top_secret()
            ''',
            ['foobaz'],
        )

        await self.assert_query_result(
            '''
            select ext::_conf::OK
            ''',
            [True],
        )
        await self.con.execute('''
            configure current database set ext::_conf::Config::secret :=
                "123456";
        ''')
        await self.assert_query_result(
            '''
            select ext::_conf::OK
            ''',
            [False],
        )

        # Make sure secrets are redacted from get_config_json
        cfg_json = await self.con.query_single('''
            select to_str(cfg::get_config_json());
        ''')
        self.assertNotIn('123456', cfg_json, 'secrets not redacted')

        # test not being able to access secrets
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, "because it is secret"
        ):
            await self.con.execute('''
                select cfg::Config {
                    conf := assert_single(.extensions[is ext::_conf::Config] {
                        secret
                    })
                };
            ''')
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, "because it is secret"
        ):
            await self.con.execute('''
                select cfg::Config {
                    conf := assert_single(
                        .extensions[is ext::_conf::Config] {
                        objs: { [is ext::_conf::SecretObj].secret }
                    })
                };
            ''')
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, "because it is secret"
        ):
            await self.con.execute('''
                select ext::_conf::Config.secret
            ''')
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, "because it is secret"
        ):
            await self.con.execute('''
                select ext::_conf::SecretObj.secret
            ''')
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, "because it is secret"
        ):
            await self.con.execute('''
                configure current database reset ext::_conf::SecretObj
                filter .secret = '123456'
            ''')

        if not in_tx:
            # Load the in-memory config state via a HTTP debug endpoint
            # Retry until we see 'ready' is visible
            async for tr in self.try_until_succeeds(ignore=AssertionError):
                async with tr:
                    with self.http_con() as http_con:
                        rdata, _headers, _status = self.http_con_request(
                            http_con,
                            prefix="",
                            path="server-info",
                        )
                        data = json.loads(rdata)
                        if 'databases' not in data:
                            # multi-tenant instance - use the first tenant
                            data = next(iter(data['tenants'].values()))
                        db_data = data['databases'][self.get_database_name()]
                        config = db_data['config']
                        assert (
                            config['ext::_conf::Config::config_name'] == 'ready'
                        )

            self.assertEqual(
                sorted(
                    config['ext::_conf::Config::objs'],
                    key=lambda x: x['name'],
                ),
                [
                    {'_tname': 'ext::_conf::Obj',
                     'name': '1', 'value': 'foo', 'opt_value': None},
                    {'_tname': 'ext::_conf::Obj',
                     'name': '2', 'value': 'bar', 'opt_value': 'opt.'},
                    {'_tname': 'ext::_conf::SubObj',
                     'name': '3', 'value': 'baz', 'extra': 42,
                     'duration_config': 'PT10M',
                     'opt_value': None},
                    {'_tname': 'ext::_conf::SecretObj',
                     'name': '4', 'value': 'foo',
                     'opt_value': None, 'secret': {'redacted': True}},
                    {'_tname': 'ext::_conf::SecretObj',
                     'name': '5', 'value': 'quux',
                     'opt_value': None, 'secret': None},
                ],
            )
            self.assertEqual(
                config['ext::_conf::Config::obj'],
                {'_tname': 'ext::_conf::SingleObj',
                 'name': 'single', 'value': 'val', 'fixed': 'fixed!'},
            )

        val = await self.con.query_single('''
            describe current branch config
        ''')
        test_expected = textwrap.dedent('''\
        CONFIGURE CURRENT BRANCH SET ext::_conf::Config::config_name := \
'ready';
        CONFIGURE CURRENT BRANCH INSERT ext::_conf::SingleObj {
            name := 'single',
            value := 'val',
        };
        CONFIGURE CURRENT BRANCH INSERT ext::_conf::Obj {
            name := '1',
            value := 'foo',
        };
        CONFIGURE CURRENT BRANCH INSERT ext::_conf::Obj {
            name := '2',
            opt_value := 'opt.',
            value := 'bar',
        };
        CONFIGURE CURRENT BRANCH INSERT ext::_conf::SecretObj {
            name := '4',
            secret := {},  # REDACTED
            value := 'foo',
        };
        CONFIGURE CURRENT BRANCH INSERT ext::_conf::SecretObj {
            name := '5',
            secret := {},  # REDACTED
            value := 'quux',
        };
        CONFIGURE CURRENT BRANCH INSERT ext::_conf::SubObj {
            duration_config := <std::duration>'PT10M',
            extra := 42,
            name := '3',
            value := 'baz',
        };
        CONFIGURE CURRENT BRANCH SET ext::_conf::Config::opt_value := 'opt!';
        CONFIGURE CURRENT BRANCH SET ext::_conf::Config::secret := \
{};  # REDACTED
        ''')
        self.assertEqual(val, test_expected)

        await self.con.execute('''
            configure current database reset ext::_conf::Obj
            filter .value like 'ba%'
        ''')
        await self.con.execute('''
            configure current database reset ext::_conf::SecretObj
        ''')

        await _check(
            config_name='ready',
            objs=[
                dict(name='1', value='foo'),
            ],
        )

        await self.con.execute('''
            configure current database reset ext::_conf::Obj
        ''')
        await self.con.execute('''
            configure current database reset ext::_conf::Config::opt_value;
        ''')

        await _check(
            config_name='ready',
            opt_value=None,
            objs=[],
            obj=dict(name='single', value='val'),
        )

        await self.con.execute('''
            configure current database reset ext::_conf::SingleObj
        ''')
        await _check(
            config_name='ready',
            opt_value=None,
            objs=[],
            obj=None,
        )

        await self.con.execute('''
            configure current database reset ext::_conf::Config::secret;
        ''')
        await self.con.execute('''
            configure current database reset ext::_conf::Config::config_name;
        ''')

        await _check(
            config_name='',
            objs=[],
        )

        if not in_tx:
            con2 = await self.connect(database=self.con.dbname)
            try:
                await con2.query('select 1')
                await self.con.execute('''
                    CONFIGURE CURRENT BRANCH INSERT ext::_conf::Obj {
                        name := 'fail',
                        value := '',
                    };
                ''')

                # This needs to fail
                with self.assertRaisesRegex(
                    edgedb.ConstraintViolationError, ""
                ):
                    await self.con.execute('''
                        CONFIGURE CURRENT BRANCH INSERT ext::_conf::Obj {
                            name := 'fail',
                            value := '',
                        };
                        insert Test;
                    ''')

                # The code path by which the above fails is subtle (it
                # gets triggered by config processing code in the
                # server). Make sure that the error properly aborts
                # the whole script.
                await self.assert_query_result(
                    'select count(Test)',
                    [0],
                )

            finally:
                await con2.aclose()

    async def test_edgeql_extensions_05(self):
        # Test config extension
        await self.con.execute('''
            create type Test;
        ''')

        try:
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_05(in_tx=True)
            try:
                await self._extension_test_05(in_tx=False)
            finally:
                await self.con.execute('''
                    drop extension _conf
                ''')
        finally:
            await self.con.execute('''
                drop type Test;
            ''')

    async def _extension_test_06b(self):
        await self.con.execute(r"""
            START MIGRATION TO {
                using extension bar version "2.0";
                module default {
                    function lol() -> str using (ext::bar::fubar())
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.assert_query_result(
            'select lol()',
            ['foobar'],
        )

        # Try dropping everything that uses it but not the extension
        async with self._run_and_rollback():
            await self.con.execute(r"""
                START MIGRATION TO {
                    using extension bar version "2.0";
                    module default {
                    }
                };
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            """)

        # Try dropping it but adding bar
        async with self._run_and_rollback():
            await self.con.execute(r"""
                START MIGRATION TO {
                    using extension bar version "2.0";
                    module default {
                    }
                };
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            """)

        # Try dropping everything including the extension
        await self.con.execute(r"""
            START MIGRATION TO {
                module default {
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        # Try it explicitly specifying an old version. Note
        # that we don't *yet* support upgrading between extension
        # versions; you need to drop it and recreate everything, which
        # obviously is not great.

        await self.con.execute(r"""
            START MIGRATION TO {
                using extension bar version '1.0';
                module default {
                    function lol() -> str using (ext::bar::fubar())
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.assert_query_result(
            'select lol()',
            ['foo?bar'],
        )

        # Try dropping everything including the extension
        await self.con.execute(r"""
            START MIGRATION TO {
                module default {
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        with self.assertRaisesRegex(
            edgedb.SchemaError,
            "cannot install extension 'foo' version 2.0: "
            "version 1.0 is already installed"
        ):
            await self.con.execute(r"""
                START MIGRATION TO {
                    using extension bar version '1.0';
                    using extension foo version '2.0';
                    module default {
                    }
                };
                POPULATE MIGRATION;
                COMMIT MIGRATION;
            """)

    async def test_edgeql_extensions_06(self):
        # Make an extension with dependencies
        await self.con.execute('''
            create extension package foo VERSION '1.0' {
              set ext_module := "ext::foo";
              create module ext::foo;
              create function ext::foo::test() -> str using ("foo?");
            };
            create extension package foo VERSION '2.0' {
              set ext_module := "ext::foo";
              create module ext::foo;
              create function ext::foo::test() -> str using ("foo");
            };
            create extension package bar VERSION '1.0' {
              set ext_module := "ext::bar";
              set dependencies := ["foo>=1.0"];
              create module ext::bar;
              create function ext::bar::fubar() -> str using (
                ext::foo::test() ++ "bar"
              );
            };
            create extension package bar VERSION '2.0' {
              set ext_module := "ext::bar";
              set dependencies := ["foo>=2.0"];
              create module ext::bar;
              create function ext::bar::fubar() -> str using (
                ext::foo::test() ++ "bar"
              );
            };
        ''')
        try:
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_06b()
        finally:
            await self.con.execute('''
                drop extension package bar VERSION '1.0';
                drop extension package foo VERSION '1.0';
                drop extension package bar VERSION '2.0';
                drop extension package foo VERSION '2.0';
            ''')

    async def _extension_test_07(self):
        await self.con.execute(r"""
            START MIGRATION TO {
                using extension asdf version "1.0";
                module default {
                    function lol() -> int64 using (ext::asdf::getver())
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.assert_query_result(
            'select lol()',
            [1],
        )

        await self.con.execute(r"""
            START MIGRATION TO {
                using extension asdf version "3.0";
                module default {
                    function lol() -> int64 using (ext::asdf::getver())
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.assert_query_result(
            'select lol()',
            [3],
        )
        await self.assert_query_result(
            'select ext::asdf::getsver()',
            ['3'],
        )

    async def test_edgeql_extensions_07(self):
        # Make an extension with chained upgrades
        await self.con.execute('''
            create extension package asdf version '1.0' {
                set ext_module := "ext::asdf";
                create module ext::asdf;
                create function ext::asdf::getver() -> int64 using (1);
            };
            create extension package asdf version '2.0' {
                set ext_module := "ext::asdf";
                create module ext::asdf;
                create function ext::asdf::getver() -> int64 using (2);
                create function ext::asdf::getsver() -> str using (
                   <str>ext::asdf::getver());
            };
            create extension package asdf version '3.0' {
                set ext_module := "ext::asdf";
                create module ext::asdf;
                create function ext::asdf::getver() -> int64 using (3);
                create function ext::asdf::getsver() -> str using (
                    <str>ext::asdf::getver());
            };

            create extension package asdf migration
                from version '1.0' to version '2.0' {
                alter function ext::asdf::getver() using (2);
                create function ext::asdf::getsver() -> str using (
                    <str>ext::asdf::getver());
            };
            create extension package asdf migration
                from version '2.0' to version '3.0' {
                alter function ext::asdf::getver() using (3);
            };
        ''')
        try:
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_07()
        finally:
            await self.con.execute('''
                drop extension package asdf version '1.0';
                drop extension package asdf version '2.0';

                drop extension package asdf migration
                from version '1.0' to version '2.0';

                drop extension package asdf version '3.0';
                drop extension package asdf migration
                from version '2.0' to version '3.0';
            ''')

    async def _extension_test_08(self):
        await self.con.execute(r"""
            START MIGRATION TO {
                using extension bar version "1.0";
                module default {
                    function lol() -> str using (ext::bar::fubar())
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)

        await self.assert_query_result(
            'select lol()',
            ['bar'],
        )
        # Direct upgrade command should fail.
        async with self.assertRaisesRegexTx(
            edgedb.SchemaError,
            "cannot create extension 'bar' version '2.0': "
            "it depends on extension foo which has not been created",
        ):
            await self.con.execute(r"""
                alter extension bar to version '2.0';
            """)

        # Migration should work, though, since it will create the dependency.
        await self.con.execute(r"""
            START MIGRATION TO {
                using extension bar version "2.0";
                module default {
                    function lol() -> str using (ext::bar::fubar())
                }
            };
            POPULATE MIGRATION;
            COMMIT MIGRATION;
        """)
        await self.assert_query_result(
            'select lol()',
            ['foobar'],
        )

    async def test_edgeql_extensions_08(self):
        # Make an extension with dependencies
        await self.con.execute('''
            create extension package foo VERSION '2.0' {
              set ext_module := "ext::foo";
              create module ext::foo;
              create function ext::foo::test() -> str using ("foo");
            };
            create extension package bar VERSION '1.0' {
              set ext_module := "ext::bar";
              create module ext::bar;
              create function ext::bar::fubar() -> str using (
                "bar"
              );
            };
            create extension package bar VERSION '2.0' {
              set ext_module := "ext::bar";
              set dependencies := ["foo>=2.0"];
              create module ext::bar;
              create function ext::bar::fubar() -> str using (
                ext::foo::test() ++ "bar"
              );
            };
            create extension package bar migration
                from version '1.0' to version '2.0' {
              alter function ext::bar::fubar() using (
                ext::foo::test() ++ "bar"
              );
            };

        ''')
        try:
            async for tx in self._run_and_rollback_retrying():
                async with tx:
                    await self._extension_test_08()
        finally:
            await self.con.execute('''
                drop extension package bar VERSION '1.0';
                drop extension package bar VERSION '2.0';
                drop extension package foo VERSION '2.0';
                drop extension package bar migration
                  from version '1.0' to version '2.0';
            ''')
