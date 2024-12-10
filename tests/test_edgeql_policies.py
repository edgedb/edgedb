#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2017-present MagicStack Inc. and the EdgeDB authors.
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

import edgedb

from edb.testbase import server as tb
# from edb.tools import test


class TestEdgeQLPolicies(tb.QueryTestCase):
    '''Tests for policies.'''

    NO_FACTOR = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.esdl')

    SETUP = [
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'issues_setup.edgeql'),
        '''
            # These are for testing purposes and don't really model anything
            create required global cur_owner_active -> bool {
                set default := true;
            };
            create required global watchers_active -> bool {
                set default := true;
            };

            create required global filter_owned -> bool {
                set default := false;
            };
            create global cur_user -> str;

            alter type Owned {
                create access policy disable_filter
                  when (not global filter_owned)
                  allow select;

                create access policy cur_owner
                  when (global cur_owner_active)
                  allow all using (.owner.name ?= global cur_user);
            };

            alter type Issue {
                create access policy cur_watchers
                  when (global watchers_active)
                  allow select using (
                      (global cur_user IN __subject__.watchers.name) ?? false
                  )
            };
            create function count_Issue() -> int64 using (count(Issue));

            create type CurOnly extending Dictionary {
                create access policy cur_only allow all
                using (not exists global cur_user or global cur_user ?= .name);
            };
            create type CurOnlyM {
                create multi property name -> str {
                    create constraint exclusive;
                };
                create access policy cur_only allow all
                using (
                    not exists global cur_user
                    or (global cur_user in .name) ?? false
                );
            };

            create type Message {
                create link attachment -> Issue;
                create access policy has_attachment
                    allow all
                    using (count(.attachment) > 0);
            };
        '''
    ]

    async def test_edgeql_policies_01(self):
        await self.con.execute('''
            set global cur_owner_active := false;
        ''')
        await self.con.execute('''
            set global watchers_active := false;
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        await self.assert_query_result(
            r'''
                select Owned { [IS Named].name }
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                select Issue { name }
            ''',
            []
        )

    async def test_edgeql_policies_02a(self):
        await self.con.execute('''
            set global cur_user := 'Yury';
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        await self.assert_query_result(
            r'''
                select Owned { [IS Named].name }
            ''',
            tb.bag([
                {"name": "Release EdgeDB"},
                {"name": "Improve EdgeDB repl output rendering."},
                {"name": "Repl tweak."},
            ])
        )

        await self.assert_query_result(
            r'''
                select Issue { name }
            ''',
            tb.bag([
                {"name": "Release EdgeDB"},
                {"name": "Improve EdgeDB repl output rendering."},
                {"name": "Repl tweak."},
            ])
        )

    async def test_edgeql_policies_02b(self):
        await self.con.execute('''
            alter type Owned reset abstract;
        ''')

        await self.con.execute('''
            set global cur_user := 'Yury';
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        await self.assert_query_result(
            r'''
                select Owned { [IS Named].name }
            ''',
            tb.bag([
                {"name": "Release EdgeDB"},
                {"name": "Improve EdgeDB repl output rendering."},
                {"name": "Repl tweak."},
            ])
        )

        await self.assert_query_result(
            r'''
                select Issue { name }
            ''',
            tb.bag([
                {"name": "Release EdgeDB"},
                {"name": "Improve EdgeDB repl output rendering."},
                {"name": "Repl tweak."},
            ])
        )

    async def test_edgeql_policies_03(self):
        vals = await self.con.query('''
            select Object.id
        ''')
        self.assertEqual(len(vals), len(set(vals)))

        await self.con.execute('''
            create alias foo := Issue;
        ''')

        vals = await self.con.query('''
            select BaseObject.id
        ''')
        self.assertEqual(len(vals), len(set(vals)))

    async def test_edgeql_policies_04(self):
        await self.con.execute('''
            set global cur_user := 'Phil';
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        await self.assert_query_result(
            r'''
                select URL { src := .<references[IS User] }
            ''',
            tb.bag([
                {"src": []}
            ])
        )

        await self.assert_query_result(
            r'''
                select URL { src := .<references }
            ''',
            tb.bag([
                {"src": []}
            ])
        )

    async def test_edgeql_policies_05a(self):
        await self.con.execute('''
            CREATE TYPE Tgt {
                CREATE REQUIRED PROPERTY b -> bool;

                CREATE ACCESS POLICY redact
                    ALLOW SELECT USING (not global filter_owned);
                CREATE ACCESS POLICY dml_always
                    ALLOW UPDATE, INSERT, DELETE;
            };
            CREATE TYPE Ptr {
                CREATE REQUIRED LINK tgt -> Tgt;
                CREATE PROPERTY tb := .tgt.b;
            };
        ''')
        await self.con.query('''
            insert Ptr { tgt := (insert Tgt { b := True }) };
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr { tgt }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr { z := .tgt.b }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr { z := .tgt.id }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"required link 'tgt' of object type 'default::Ptr' is "
                r"hidden by access policy \(while evaluating computed "
                r"property 'tb' of object type 'default::Ptr'\)"):
            await self.con.query('''
                select Ptr { tb }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr.tgt
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr.tgt.b
            ''')

        await self.con.query('''
            delete Ptr
        ''')

        await self.assert_query_result(
            r''' select Ptr { tgt }''',
            [],
        )

        await self.assert_query_result(
            r''' select Ptr.tgt''',
            [],
        )

        await self.assert_query_result(
            r''' select Ptr.tgt.b''',
            [],
        )

    async def test_edgeql_policies_05b(self):
        await self.con.execute('''
            CREATE TYPE Tgt {
                CREATE REQUIRED PROPERTY b -> bool;

                CREATE ACCESS POLICY redact
                    ALLOW SELECT USING (not global filter_owned);
                CREATE ACCESS POLICY dml_always
                    ALLOW UPDATE, INSERT, DELETE;
            };
            CREATE TYPE Ptr {
                CREATE REQUIRED LINK tgt -> Tgt;
                CREATE PROPERTY tb := .tgt.b;
                CREATE ACCESS POLICY redact
                    ALLOW SELECT USING (.tgt.b);
                CREATE ACCESS POLICY dml_always
                    ALLOW UPDATE, INSERT, DELETE;
            };
        ''')
        await self.con.query('''
            insert Ptr { tgt := (insert Tgt { b := True }) };
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr { tgt }
            ''')

    async def test_edgeql_policies_06(self):
        await self.con.execute('''
            CREATE TYPE Tgt {
                CREATE REQUIRED PROPERTY b -> bool;

                CREATE ACCESS POLICY redact
                    ALLOW SELECT USING (not global filter_owned);
                CREATE ACCESS POLICY dml_always
                    ALLOW UPDATE, INSERT, DELETE;
            };
            CREATE TYPE BadTgt;
            CREATE TYPE Ptr {
                CREATE REQUIRED LINK tgt -> Tgt | BadTgt;
            };
        ''')
        await self.con.query('''
            insert Ptr { tgt := (insert Tgt { b := True }) };
        ''')
        await self.con.execute('''
            set global filter_owned := True;
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.CardinalityViolationError,
                r"is hidden by access policy"):
            await self.con.query('''
                select Ptr { tgt }
            ''')

    async def test_edgeql_policies_07(self):
        # test update policies
        await self.con.execute('''
            set global filter_owned := True;
        ''')
        await self.con.execute('''
            set global cur_user := 'Yury';
        ''')

        await self.assert_query_result(
            '''
                select Issue { name } filter .number = '1'
            ''',
            [{"name": "Release EdgeDB"}],
        )

        # Shouldn't work
        await self.assert_query_result(
            '''
                update Issue filter .number = '1' set { name := "!" }
            ''',
            [],
        )

        await self.assert_query_result(
            '''
                delete Issue filter .number = '1'
            ''',
            [],
        )

        await self.assert_query_result(
            '''
                select Issue { name } filter .number = '1'
            ''',
            [{"name": "Release EdgeDB"}],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on update of default::Issue"):
            await self.con.query('''
                update Issue filter .number = "2"
                set { owner := (select User filter .name = 'Elvis') };
            ''')

        # This update *should* work, though
        await self.assert_query_result(
            '''
                update Issue filter .number = '2' set { name := "!" }
            ''',
            [{}],
        )

        await self.assert_query_result(
            '''
                select Issue { name } filter .number = '2'
            ''',
            [{"name": "!"}],
        )

        # Now try updating Named, based on name

        # This one should work
        await self.assert_query_result(
            '''
                update Named filter .name = '!' set { name := "Fix bug" }
            ''',
            [{}],
        )

        await self.assert_query_result(
            '''
                select Issue { name } filter .number = '2'
            ''',
            [{"name": "Fix bug"}],
        )

        # This shouldn't work
        await self.assert_query_result(
            '''
                update Named filter .name = 'Release EdgeDB'
                set { name := "?" }
            ''',
            [],
        )

        await self.assert_query_result(
            '''
                select Issue { name } filter .number = '1'
            ''',
            [{"name": "Release EdgeDB"}],
        )

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                "access policy violation"):
            await self.con.query('''
                INSERT Issue {
                    number := '4',
                    name := 'Regression.',
                    body := 'Fix regression introduced by lexer tweak.',
                    owner := (SELECT User FILTER User.name = 'Elvis'),
                    status := (SELECT Status FILTER Status.name = 'Closed'),
                } UNLESS CONFLICT ON (.number) ELSE Issue;
            ''')

    async def test_edgeql_policies_08(self):
        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                r"possibly an empty set"):
            await self.con.query('''
                WITH Z := (INSERT Issue {
                    number := '4',
                    name := 'Regression.',
                    body := 'Fix regression introduced by lexer tweak.',
                    owner := (SELECT User FILTER User.name = 'Elvis'),
                    status := (SELECT Status FILTER Status.name = 'Closed'),
                } UNLESS CONFLICT ON (.number) ELSE Issue),
                select { required z := Z };
            ''')

    async def test_edgeql_policies_09(self):
        # Create a type that we can write but not view
        await self.con.execute('''
            create type X extending Dictionary {
                create access policy can_insert allow insert;
            };
            insert X { name := "!" };
        ''')

        # We need to raise a constraint violation error even though
        # we are trying to do unless conflict, because we can't see
        # the conflicting object!
        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"name violates exclusivity constraint"):
            await self.con.query('''
                insert X { name := "!" }
                unless conflict on (.name) else (select X)
            ''')

    async def test_edgeql_policies_10(self):
        # see issue https://github.com/edgedb/edgedb/issues/4646
        async with self.assertRaisesRegexTx(edgedb.AccessPolicyError, ''):
            await self.con.execute('insert Message {}')

    async def test_edgeql_policies_11(self):
        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on insert of default::Issue"):
            await self.con.query('''
            insert Issue {
                name := '', body := '',
                status := (select Status filter .name = 'Open'), number := '',
                owner := (insert User { name := "???" }),
            };
            ''')

    async def test_edgeql_policies_12(self):
        await self.con.query('''
            create global cur_user_obj := (
                select User filter .name = global cur_user);
            alter type User {
                create access policy allow_self_obj
                   allow all
                   using (__subject__ ?= global cur_user_obj);
            }
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on insert of default::User"):
            await self.con.query('''
                insert User { name := 'whatever' }
            ''')

    async def test_edgeql_policies_order_01(self):
        await self.con.execute('''
            insert CurOnly { name := "!" }
        ''')
        await self.con.execute('''
            set global cur_user := "?"
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on insert of default::CurOnly"):
            await self.con.query('''
                insert CurOnly { name := "!" }
            ''')

    async def test_edgeql_policies_order_02(self):
        await self.con.execute('''
            insert CurOnly { name := "!" };
            insert CurOnly { name := "?" };
        ''')
        await self.con.execute('''
            set global cur_user := "?"
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on update of default::CurOnly"):
            await self.con.query('''
                update CurOnly set { name := "!" }
            ''')

    async def test_edgeql_policies_order_03(self):
        await self.con.execute('''
            insert CurOnlyM { name := "!" }
        ''')
        await self.con.execute('''
            set global cur_user := "?"
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on insert of default::CurOnlyM"):
            await self.con.query('''
                insert CurOnlyM { name := "!" }
            ''')

    async def test_edgeql_policies_order_04(self):
        await self.con.execute('''
            insert CurOnlyM { name := "!" };
            insert CurOnlyM { name := "?" };
        ''')
        await self.con.execute('''
            set global cur_user := "?"
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on update of default::CurOnlyM"):
            await self.con.query('''
                update CurOnlyM set { name := "!" }
            ''')

    async def test_edgeql_policies_scope_01(self):
        await self.con.execute('''
            create type Foo {
                create required property val -> int64;
            };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.SchemaDefinitionError,
                r'possibly an empty set returned'):
            await self.con.execute('''
                alter type Foo {
                    create access policy pol allow all using(Foo.val > 5);
                };
            ''')

    async def test_edgeql_policies_binding_01(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY val -> int64;
            };
            CREATE TYPE Bar EXTENDING Foo;
            ALTER TYPE Foo {
                CREATE ACCESS POLICY ap0
                    ALLOW ALL USING ((count(Bar) = 0));
            };
        ''')

        await self.con.execute('''
            insert Foo { val := 0 };
            insert Foo { val := 1 };
            insert Bar { val := 10 };
        ''')

        await self.assert_query_result(
            r'''
                select Foo
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                select Bar
            ''',
            []
        )

    async def test_edgeql_policies_binding_02(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY val -> int64;
            };
            CREATE TYPE Bar EXTENDING Foo;
            ALTER TYPE Foo {
                CREATE ACCESS POLICY ins ALLOW INSERT;
                CREATE ACCESS POLICY ap0
                    ALLOW ALL USING (
                        not exists (select Foo filter .val = 1));
            };
        ''')

        await self.con.execute('''
            insert Foo { val := 0 };
            insert Foo { val := 1 };
            insert Bar { val := 10 };
        ''')

        await self.assert_query_result(
            r'''
                select Foo
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                select Bar
            ''',
            []
        )

    async def test_edgeql_policies_binding_03(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY val -> int64;
            };
            CREATE TYPE Bar EXTENDING Foo;
            ALTER TYPE Foo {
                CREATE MULTI LINK bar -> Bar;
            };

            insert Foo { val := 0 };
            insert Foo { val := 1 };
            insert Bar { val := 10 };
            update Foo set { bar := Bar };

            ALTER TYPE Foo {
                CREATE ACCESS POLICY ap0
                    ALLOW ALL USING (not exists .bar);
            };
        ''')

        await self.assert_query_result(
            r'''
                select Foo
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                select Bar
            ''',
            []
        )

    async def test_edgeql_policies_binding_04(self):
        await self.con.execute('''
            CREATE TYPE Foo {
                CREATE REQUIRED PROPERTY val -> int64;
                CREATE MULTI LINK foo -> Foo;
            };
            CREATE TYPE Bar EXTENDING Foo;

            insert Foo { val := 0 };
            insert Foo { val := 1 };
            insert Bar { val := 10 };
            update Foo set { foo := Foo };

            ALTER TYPE Foo {
                CREATE ACCESS POLICY ap0
                    ALLOW ALL USING (not exists .foo);
            };
        ''')

        await self.assert_query_result(
            r'''
                select Foo
            ''',
            []
        )

        await self.assert_query_result(
            r'''
                select Bar
            ''',
            []
        )

    async def test_edgeql_policies_cycle_05(self):
        # cycle is just fine if nonrecursive_access_policies is set
        await self.con.execute("""
            CREATE TYPE Bar {
                CREATE REQUIRED PROPERTY b -> bool;
            };
            CREATE TYPE Foo {
                CREATE LINK bar -> Bar;
                CREATE REQUIRED PROPERTY b -> bool;
                CREATE ACCESS POLICY redact
                    ALLOW ALL USING ((.bar.b ?? false));
            };
            ALTER TYPE Bar {
                CREATE LINK foo -> Foo;
                CREATE ACCESS POLICY redact
                    ALLOW ALL USING ((.foo.b ?? false));
            };
        """)

    async def test_edgeql_policies_missing_prop_01(self):
        await self.con.execute('''
            CREATE TYPE A {
                CREATE PROPERTY ts -> datetime;
                CREATE ACCESS POLICY soft_delete
                    ALLOW SELECT, UPDATE READ, INSERT
                    USING (NOT (EXISTS (.ts)));
                CREATE ACCESS POLICY update_write
                    ALLOW UPDATE WRITE;
            }
        ''')

        await self.con.execute('''
            insert A;
        ''')

    async def test_edgeql_policies_missing_prop_02(self):
        await self.con.execute('''
            CREATE TYPE A {
                CREATE MULTI PROPERTY ts -> datetime;
                CREATE ACCESS POLICY soft_delete
                    ALLOW SELECT, UPDATE READ, INSERT
                    USING (NOT (EXISTS (.ts)));
                CREATE ACCESS POLICY update_write
                    ALLOW UPDATE WRITE;
            }
        ''')

        await self.con.execute('''
            insert A;
        ''')

    async def test_edgeql_policies_delete_union_01(self):
        await self.con.execute('''
            create type T {
                create access policy insert_select
                    allow insert, select;
            };
            create type S;
            insert T;
        ''')

        await self.assert_query_result(
            r'''
                delete {T, S};
            ''',
            []
        )

    async def test_edgeql_policies_multi_object_01(self):
        await self.con.execute('''
            create global bypassAccessPolicies -> bool;
            create type User2 {
                create access policy bypass_access_policies
                    allow all using (
                      (global bypassAccessPolicies ?? false));
                create required property username -> str {
                    create constraint exclusive;
                };
            };
            create global sessionToken -> str;
            create type Session {
                create required property token -> str;
                create required link user -> User2;
            };
            create global session := (select
                Session filter
                    (.token ?= global sessionToken)
            limit
                1
            );
            alter type User2 {
                create access policy admin_has_full_access
                    allow all using (
                      ((global session).user.username ?= 'admin'));
            };
        ''')

        await self.con.execute('''
            set global bypassAccessPolicies := true;
        ''')
        await self.con.execute('''
            insert User2 { username := "admin" }
        ''')
        await self.con.execute('''
            insert User2 { username := "admin" } unless conflict
        ''')

        await self.assert_query_result(
            r'''
                select User2 { username }
            ''',
            [{'username': 'admin'}]
        )

    async def test_edgeql_policies_recursive_01(self):
        await self.con.execute('''
            create type A;
            create type B;
            insert A;
            insert B;
            alter type A {
                create access policy no allow all using (false);
            };
            alter type B {
                create access policy if_a allow all using (exists A);
            };
            create function count_B() -> int64 using (count(B));
        ''')

        # B should be visible if we are using nonrecursive_access_policies
        await self.assert_query_result(
            r'''select count(B)''',
            [1],
        )
        await self.assert_query_result(
            r'''select count_B()''',
            [1],
        )

    async def test_edgeql_policies_insert_type(self):
        await self.con.execute('''
            create type T {
                create access policy ok allow all;
                create access policy asdf deny insert using (
                    .__type__.name ?!= 'default::T')
            };
            create type S extending T;
        ''')

        await self.con.execute('insert T')
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            'access policy violation',
        ):
            await self.con.execute('insert S')

    async def test_edgeql_policies_internal_shape_01(self):
        await self.con.execute('''
            alter type Issue {
                create access policy foo_1 deny all using (
                    not exists (select .watchers { foo := .todo }
                                filter "x" in .foo.name));
                create access policy foo_2 deny all using (
                    not exists (select .watchers { todo }
                                filter "x" in .todo.name));
             };
        ''')

        await self.assert_query_result(
            r'''
                select Issue
            ''',
            [],
        )
        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "access policy violation on insert",
        ):
            await self.con.execute('''
                insert Issue {
                    name := '', body := '', status := {}, number := '',
                    owner := {}};
            ''')

    async def test_edgeql_policies_volatile_01(self):
        await self.con.execute('''
            create type Bar {
                create required property r -> float64;
                create access policy ok allow all;
                create access policy no deny
                    update write, insert using (.r <= 0.5);
            };
        ''')

        for _ in range(10):
            async with self._run_and_rollback():
                try:
                    await self.con.execute('''
                        insert Bar { r := random() };
                    ''')
                except edgedb.AccessPolicyError:
                    # If it failed, nothing to do, keep trying
                    pass
                else:
                    r = (await self.con.query('''
                        select Bar.r
                    '''))[0]
                    self.assertGreater(r, 0.5)

        await self.con.execute('''
            insert Bar { r := 1.0 };
        ''')
        for _ in range(10):
            async with self._run_and_rollback():
                try:
                    await self.con.execute('''
                        update Bar set { r := random() };
                    ''')
                except edgedb.AccessPolicyError:
                    # If it failed, nothing to do, keep trying
                    pass
                else:
                    r = (await self.con.query('''
                        select Bar.r
                    '''))[0]
                    self.assertGreater(r, 0.5)

    async def test_edgeql_policies_volatile_02(self):
        # Same as above but multi
        await self.con.execute('''
            create type Bar {
                create required multi property r -> float64;
                create access policy ok allow all;
                create access policy no deny
                    update write, insert using (all(.r <= 0.5));
            };
        ''')

        for _ in range(10):
            async with self._run_and_rollback():
                try:
                    await self.con.execute('''
                        insert Bar { r := random() };
                    ''')
                except edgedb.AccessPolicyError:
                    # If it failed, nothing to do, keep trying
                    pass
                else:
                    r = (await self.con.query('''
                        select Bar.r
                    '''))[0]
                    self.assertGreater(r, 0.5)

        await self.con.execute('''
            insert Bar { r := 1.0 };
        ''')
        for _ in range(10):
            async with self._run_and_rollback():
                try:
                    await self.con.execute('''
                        update Bar set { r := random() };
                    ''')
                except edgedb.AccessPolicyError:
                    # If it failed, nothing to do, keep trying
                    pass
                else:
                    r = (await self.con.query('''
                        select Bar.r
                    '''))[0]
                    self.assertGreater(r, 0.5)

    async def test_edgeql_policies_messages(self):
        await self.con.execute(
            '''
            create type NoAllows {
                create access policy allow_select
                    allow select;
            };
            create type TwoAllows {
                create required property val -> str;
                create access policy allow_insert_of_a
                    allow insert using (.val = 'a')
                    { set errmessage := 'you can insert a' };
                create access policy allow_insert_of_b
                    allow insert using (.val = 'b');
            };
            create type ThreeDenies {
                create required property val -> str;

                create access policy allow_insert
                    allow insert;

                create access policy deny_starting_with_f
                    deny insert using (.val[0] = 'f')
                    { set errmessage := 'val cannot start with f' };

                create access policy deny_foo
                    deny insert using (.val = 'foo')
                    { set errmessage := 'val cannot be foo' };

                create access policy deny_bar
                    deny insert using (.val = 'bar');
            };
        '''
        )

        await self.con.execute("insert TwoAllows { val := 'a' };")

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            r"access policy violation on insert of default::NoAllows$",
        ):
            await self.con.query('insert NoAllows')

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError, r"\(you can insert a\)$"
        ):
            await self.con.query("insert TwoAllows { val := 'c' }")

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "access policy violation.*val cannot.*val cannot",
        ):
            await self.con.query("insert ThreeDenies { val := 'foo' }")

        async with self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            "access policy violation on insert of default::ThreeDenies$",
        ):
            await self.con.query("insert ThreeDenies { val := 'bar' }")

    async def test_edgeql_policies_namespace(self):
        # ... we were accidentally skipping some important fixups in
        # access policy compilation
        await self.con.execute(
            '''
            create type X {
                create access policy foo
                allow all using (
                  count((
                    WITH X := {1, 2}
                    SELECT (X, (FOR x in {X} UNION (SELECT x)))
                  )) = 2);
            };
            insert X;
            '''
        )
        await self.assert_query_result(
            r'''select X''',
            [{}],
        )

    async def test_edgeql_policies_function_01(self):
        await self.con.execute('''
            set global filter_owned := true;
        ''')
        await self.assert_query_result(
            r'''select (count(Issue), count_Issue())''',
            [(0, 0)],
        )

        await self.con.execute('''
            configure session set apply_access_policies := false;
        ''')
        await self.assert_query_result(
            r'''select (count(Issue), count_Issue())''',
            [(4, 4)],
        )

    async def test_edgeql_policies_complex_01(self):
        await self.migrate(
            """
            abstract type Auditable {
                access policy auditable_default
                    allow all ;
                access policy auditable_prohibit_hard_deletes
                    deny delete  {
                        errmessage := 'hard deletes are disallowed';
                    };
                delegated constraint std::expression on
                    ((.updated_at >= .created_at))
                    except (NOT (EXISTS (.updated_at)));
                delegated constraint std::expression on
                    ((.deleted_at > .created_at))
                    except (NOT (EXISTS (.deleted_at)));
                required property created_at: std::datetime {
                    default := (std::datetime_of_statement());
                    readonly := true;
                };
                property deleted_at: std::datetime;
                required property uid: std::str {
                    default := <str>random();
                    readonly := true;
                    constraint std::exclusive;
                };
                property updated_at: std::datetime {
                    rewrite
                        update
                        using (std::datetime_of_statement());
                };
            };
            type Avatar extending default::Auditable {
                link owner := (.<avatar[is default::Member]);
                required property url: std::str;
            };
            type Member extending default::Auditable {
                link avatar: default::Avatar {
                    on source delete delete target if orphan;
                    on target delete allow;
                    constraint std::exclusive;
                };
            };
            """
        )
        await self.con.execute(
            '''
            update Avatar set {deleted_at:=datetime_of_statement()};
            '''
        )

    async def test_edgeql_policies_optional_leakage_01(self):
        await self.con.execute(
            '''
            CREATE GLOBAL current_user -> uuid;
            CREATE TYPE Org {
                CREATE REQUIRED PROPERTY domain -> str {
                    CREATE CONSTRAINT exclusive;
                };
                CREATE PROPERTY name -> str;
            };
            CREATE TYPE User2 {
                CREATE REQUIRED LINK org -> Org;
                CREATE REQUIRED PROPERTY email -> str {
                    CREATE CONSTRAINT exclusive;
                };
            };
            CREATE GLOBAL current_user_object := (SELECT
                User2
            FILTER
                (.id = GLOBAL current_user)
            );
            CREATE TYPE Src {
                CREATE REQUIRED SINGLE LINK org -> Org;
                CREATE SINGLE LINK user -> User2;

                CREATE ACCESS POLICY deny_no
                    DENY ALL USING (
                      (((GLOBAL current_user_object).org != .org) ?? true));
                CREATE ACCESS POLICY yes
                    ALLOW ALL USING (SELECT
                        ((GLOBAL current_user = .user.id) ?? false)
                    );
            };
            '''
        )

        await self.con.execute('''
            configure session set apply_access_policies := false;
        ''')

        await self.con.execute('''
            insert User2 {
                email:= "a@a.com",
                org:=(insert Org {domain:="a.com"}),
            };
        ''')
        res = await self.con.query_single('''
            insert User2 {
                email:= "b@b.com",
                org:=(insert Org {domain:="b.com"}),
            };
        ''')
        await self.con.execute('''
            insert Src {
                org := (select Org filter .domain = "a.com")
            };
        ''')

        await self.con.execute('''
            configure session set apply_access_policies := true;
        ''')

        await self.con.execute(f'''
            set global current_user := <uuid>'{res.id}';
        ''')

        await self.assert_query_result(
            r'''select Src''',
            [],
        )

    async def test_edgeql_policies_parent_update_01(self):
        await self.con.execute('''
            CREATE ABSTRACT TYPE Base {
                CREATE PROPERTY name: std::str;
                CREATE ACCESS POLICY sel_ins
                    ALLOW SELECT, INSERT USING (true);
            };
            CREATE TYPE Child EXTENDING Base;

            INSERT Child;
        ''')

        await self.assert_query_result(
            '''
            update Base set { name := '!!!' }
            ''',
            [],
        )

        await self.assert_query_result(
            '''
            delete Base
            ''',
            [],
        )

        await self.con.execute('''
            ALTER TYPE Base {
                CREATE ACCESS POLICY upd_read
                    ALLOW UPDATE READ USING (true);
            };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidValueError,
                r"access policy violation on update"):
            await self.con.query('''
                update Base set { name := '!!!' }
            ''')

    async def test_edgeql_policies_empty_cast_01(self):
        obj = await self.con._fetchall(
            '''
                SELECT <Issue>{}
            ''',
            __typenames__=True,
        )
        self.assertEqual(obj, [])

    async def test_edgeql_policies_global_01(self):
        # GH issue #6404

        clan_and_global = '''
            type Clan {
                access policy allow_select_players
                    allow select
                    using (
                        global current_player.clan.id ?= .id
                    );
            };
            global current_player_id: uuid;
            global current_player := (
                select Player filter .id = global current_player_id
            );
        '''

        await self.migrate(
            '''
            type Principal;
            type Player extending Principal {
                required link clan: Clan;
            }
            ''' + clan_and_global
        )

        await self.migrate(
            '''
            type Player {
                required link clan: Clan;
            }
            ''' + clan_and_global
        )

    async def test_edgeql_policies_global_02(self):
        await self.con.execute('''
            create type T {
                create access policy ok allow all;
                create access policy no deny select;
            };
            insert T;
            create global foo := (select T limit 1);
            create type S {
                create access policy ok allow all using (exists global foo)
            };
        ''')

        await self.assert_query_result(
            r'''
            select { s := S, foo := global foo };
            ''',
            [{"s": [], "foo": None}]
        )
