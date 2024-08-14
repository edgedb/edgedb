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


import dataclasses
import os.path
import unittest

import edgedb

from edb.testbase import server as tb


class TestEdgeQLGlobals(tb.QueryTestCase):
    '''Tests for globals.'''

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    SETUP = [
        os.path.join(os.path.dirname(__file__), 'schemas',
                     'cards_setup.edgeql'),
        '''
            create global cur_user -> str;
            create required global def_cur_user -> str {
                set default := 'Alice'
            };
            create global def_cur_user_excited := (
                global def_cur_user ++ '!'
            );
            create global cur_card -> str {
                set default := 'Dragon'
            };
            create global banned_cards -> array<str>;

            create alias ACurUser := (
                select User filter .name = global cur_user);

            create global CurUser := (
                select User filter .name = global cur_user);

            create function get_current_user() -> OPTIONAL User using (
                select User filter .name = global cur_user
            );

            create function get_all_vars() -> tuple<str, str, str> using (
                (global cur_user ?? "", global def_cur_user,
                 global cur_card ?? "")
            );

            create function get_current_legal_cards() -> SET OF Card using (
                select get_current_user().deck
                filter .name not in array_unpack(global banned_cards)
            );

            create global tupleStr -> tuple<str, int64>;
            create global tupleNums -> tuple<int64, int64>;
            create global arrayTuple -> array<tuple<int64, int64>>;
            create global arrayTuple2 -> array<tuple<str, array<int64>>>;
            create global arrayTuple3 ->
              array<tuple<tuple<str, bool>, array<int64>>>;
        '''
    ]

    @classmethod
    def setUpClass(cls):
        if cls.get_set_up() == 'inplace':
            raise unittest.SkipTest(
                'globals schema broken with in place setup'
            )
        super().setUpClass()

    async def test_edgeql_globals_errors_01(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"possibly an empty set returned",
        ):
            await self.con.execute('''
                set global def_cur_user := {};
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError,
            r"possibly more than one element returned",
        ):
            await self.con.execute('''
                set global def_cur_user := {"foo", "bar"};
            ''')

    async def test_edgeql_globals_01(self):
        await self.con.execute('''
            set global cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''
                select global cur_user
            ''',
            ['Bob'],
        )

    async def test_edgeql_globals_02(self):
        await self.con.execute('''
            set global cur_user := "Bob";
            set global def_cur_user := "Dave";
        ''')

        await self.assert_query_result(
            r'''
                select (global cur_user, global def_cur_user,
                        global def_cur_user_excited)
            ''',
            [['Bob', 'Dave', 'Dave!']],
        )

    async def test_edgeql_globals_03(self):
        # test the behaviors of an optional with a default
        await self.assert_query_result(
            r'''select global cur_card''',
            ['Dragon']
        )

        await self.con.execute('''
            set global cur_card := 'foo'
        ''')

        await self.assert_query_result(
            r'''select global cur_card''',
            ['foo']
        )

        # setting it to {} actually sets it to {}
        await self.con.execute('''
            set global cur_card := {}
        ''')

        await self.assert_query_result(
            r'''select global cur_card''',
            []
        )

        # and RESET puts it back to the default
        await self.con.execute('''
            reset global cur_card
        ''')
        await self.assert_query_result(
            r'''select global cur_card''',
            ['Dragon']
        )

    async def test_edgeql_globals_04(self):
        await self.assert_query_result(
            r'''select ACurUser { name }''',
            []
        )

        await self.assert_query_result(
            r'''select global CurUser { name }''',
            []
        )

        await self.con.execute('''
            set global cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''select ACurUser { name }''',
            [
                {'name': 'Bob'}
            ]
        )

        await self.assert_query_result(
            r'''select global CurUser { name }''',
            [
                {'name': 'Bob'}
            ]
        )

    async def test_edgeql_globals_05(self):
        await self.con.execute('''
            set global cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''select get_current_user() { name }''',
            [
                {'name': 'Bob'}
            ]
        )

    async def test_edgeql_globals_06(self):
        await self.con.execute('''
            set global cur_user := "Alice";
        ''')
        await self.con.execute('''
            set global banned_cards := ["Dragon"];
        ''')

        await self.assert_query_result(
            r'''select get_current_legal_cards().name''',
            {"Imp", "Bog monster", "Giant turtle"},
        )

    async def test_edgeql_globals_07(self):
        await self.con.execute('''
            set global def_cur_user := "Bob";
        ''')
        await self.con.execute('''
            set global cur_user := "Alice";
        ''')
        await self.con.execute('''
            set global banned_cards := ["Dragon"];
        ''')

        await self.con.execute('''
            alter function get_current_user() using (
                select User filter .name = global def_cur_user
            );
        ''')

        await self.assert_query_result(
            r'''select get_current_legal_cards().name''',
            {"Dwarf", "Bog monster", "Golem", "Giant turtle"}
        )

    async def test_edgeql_globals_08(self):
        await self.assert_query_result(
            r'''select get_all_vars()''',
            [["", "Alice", "Dragon"]],
        )

        await self.con.execute('''
            set global cur_card := "Imp";
        ''')

        await self.assert_query_result(
            r'''select get_all_vars()''',
            [["", "Alice", "Imp"]],
        )

        await self.con.execute('''
            set global cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''select get_all_vars()''',
            [["Bob", "Alice", "Imp"]],
        )

        await self.con.execute('''
            set global def_cur_user := "Carol";
        ''')

        await self.assert_query_result(
            r'''select get_all_vars()''',
            [["Bob", "Carol", "Imp"]],
        )

        await self.con.execute('''
            set global cur_card := {};
        ''')

        await self.assert_query_result(
            r'''select get_all_vars()''',
            [["Bob", "Carol", ""]],
        )

    async def test_edgeql_globals_09(self):
        # Test that overloaded functions work
        await self.con.execute('''
            create function is_active(x: Named) -> bool using (false);
            create function is_active(x: User) -> bool using (
                select x.name ?= global cur_user);
            create function is_active(x: Card) -> bool using (
                select x.name not in array_unpack(global banned_cards));
            create function is_active2(x: Named) -> bool using (is_active(x));
        ''')

        await self.con.execute('''
            set global cur_user := "Bob";
        ''')
        await self.con.execute('''
            set global banned_cards := ["Dragon"];
        ''')

        results = tb.bag([
            {"active": True, "name": "Imp"},
            {"active": False, "name": "Dragon"},
            {"active": True, "name": "Bog monster"},
            {"active": True, "name": "Giant turtle"},
            {"active": True, "name": "Dwarf"},
            {"active": True, "name": "Golem"},
            {"active": True, "name": "Sprite"},
            {"active": True, "name": "Giant eagle"},
            {"active": False, "name": "Alice"},
            {"active": True, "name": "Bob"},
            {"active": False, "name": "Carol"},
            {"active": False, "name": "Dave"},
            {"active": True, "name": "Djinn"},
            {"active": False, "name": "1st"},
            {"active": False, "name": "2nd"},
            {"active": False, "name": "3rd"},
        ])

        await self.assert_query_result(
            r'''
                select Named { name, required active := is_active(Named) }
            ''',
            results
        )
        await self.assert_query_result(
            r'''
                select Named { name, required active := is_active2(Named) }
            ''',
            results
        )

        # swap the function to use def_cur_user and make sure it still works
        await self.con.execute('''
            alter function is_active(x: User) using (
                select x.name = global def_cur_user);
        ''')

        await self.con.execute('''
            set global cur_user := "Carol";
        ''')
        await self.con.execute('''
            set global def_cur_user := "Bob";
        ''')

        await self.assert_query_result(
            r'''
                select Named { name, required active := is_active(Named) }
            ''',
            results
        )

        # An indirect call, to make sure it gets update that way
        await self.assert_query_result(
            r'''
                select Named { name, required active := is_active2(Named) }
            ''',
            results
        )

    async def test_edgeql_globals_10(self):
        await self.con.execute('''
            set global cur_user := "test";
        ''')
        await self.con.execute('''
            set global cur_user := global cur_user ++ "!";
        ''')
        await self.con.execute('''
            set global cur_user := global cur_user ++ "!";
            set global cur_user := global cur_user ++ "!";
        ''')

        await self.assert_query_result(
            r'''select global cur_user''',
            ['test!!!']
        )

    async def test_edgeql_globals_11(self):
        await self.con.execute('''
            set global cur_user := "";
        ''')
        await self.assert_query_result(
            r'''select global cur_user''',
            ['']
        )

        await self.con.execute('''
            set global cur_user := {};
        ''')
        await self.assert_query_result(
            r'''select global cur_user''',
            []
        )

    async def test_edgeql_globals_12(self):
        await self.con.execute('''
            create global Ug := User
        ''')

        await self.assert_query_result(
            r'''select count((global Ug).deck)''',
            [9]
        )

        await self.assert_query_result(
            r'''select count(((global Ug).id, (global Ug).name))''',
            [16]
        )

    async def test_edgeql_globals_13(self):
        await self.con.execute('''
            create global my_var -> str;
        ''')

        await self.con.execute('''
            set global my_var := <str>$my_param
        ''', my_param='hello')

        await self.assert_query_result(
            r'''select global my_var''',
            ['hello']
        )

        with self.assertRaisesRegex(
            edgedb.QueryError,
            'non-constant expression in CONFIGURE DATABASE SET',
        ):
            await self.con.execute(
                '''
                configure current database
                    set query_execution_timeout := <duration>$0
                ''',
                my_param='1 sec',
            )

    async def test_edgeql_globals_14(self):
        with self.assertRaisesRegex(
            edgedb.ConfigurationError,
            "global 'def_cur_user_excited' is computed from an expression "
            "and cannot be modified",
        ):
            await self.con.execute(
                '''
                set global def_cur_user_excited := 'yay!'
                ''',
            )

    async def test_edgeql_globals_15(self):
        await self.con.execute('''
            create global foo := 1;
        ''')

        with self.assertRaisesRegex(
            edgedb.ConfigurationError,
            "global 'def_cur_user_excited' is computed from an expression "
            "and cannot be modified",
        ):
            await self.con.execute(
                '''
                reset global def_cur_user_excited
                ''',
            )

    async def test_edgeql_globals_client_01(self):
        con = edgedb.create_async_client(
            **self.get_connect_args(database=self.con.dbname)
        )
        try:
            globs = dict(
                cur_user='Alice',
            )
            scon = con.with_globals(**globs)
            res = await scon.query_single(
                f'select {{ cur_user := global cur_user }}'
            )
            dres = dataclasses.asdict(res)
            self.assertEqual(dres, {'cur_user': 'Alice'})
        finally:
            await con.aclose()

    async def test_edgeql_globals_client_02(self):
        con = edgedb.create_async_client(
            **self.get_connect_args(database=self.con.dbname)
        )
        try:
            globs = dict(
                cur_user=1,  # wrong type
            )
            scon = con.with_globals(**globs)
            with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r"invalid input for state argument  default::cur_user := 1 "
                r"\(expected str, got int\)",
            ):
                await scon.query_single(
                    f'select {{ cur_user := global cur_user }}'
                )
        finally:
            await con.aclose()

    async def test_edgeql_globals_client_03(self):
        con = edgedb.create_async_client(
            **self.get_connect_args(database=self.con.dbname)
        )
        try:
            globs = dict(
                def_cur_user_excited='yay!',  # computed
            )
            scon = con.with_globals(**globs)
            with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"got {'default::def_cur_user_excited'}, "
                r"extra {'default::def_cur_user_excited'}",
            ):
                await scon.query_single(
                    f'select {{'
                    f'    def_cur_user_excited := global def_cur_user_excited'
                    f'}}'
                )
        finally:
            await con.aclose()

    async def test_edgeql_globals_client_04(self):
        con = edgedb.create_async_client(
            **self.get_connect_args(database=self.con.dbname)
        )
        try:
            globs = dict(
                imaginary='!',  # doesn't exist
            )
            scon = con.with_globals(**globs)
            with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"got {'default::imaginary'}, "
                r"extra {'default::imaginary'}",
            ):
                await scon.query_single(
                    f'select {{ imaginary := global imaginary }}'
                )
        finally:
            await con.aclose()

    async def test_edgeql_globals_state_cardinality(self):
        await self.con.execute('''
            set global cur_user := {};
        ''')
        state = self.con._protocol.last_state
        await self.con.execute('''
            alter global cur_user {
                set default := 'Bob';
                set required;
            }
        ''')
        # Use the previous state that has no data for the required global
        self.con._protocol.last_state = state
        with self.assertRaises(edgedb.CardinalityViolationError):
            await self.con.execute("select global cur_user")
        self.con._protocol.last_state = None

    async def test_edgeql_globals_composite(self):
        # Test various composite global variables.

        # HACK: Using with_globals on testbase.Connection doesn't
        # work, and I timed out on understanding why; I got the state
        # plumbed into the real client library code, where the state
        # codec was not encoding it.
        # It isn't actually important for that to work, so for now
        # we create a connection with the real honest client library.
        con = edgedb.create_async_client(
            **self.get_connect_args(database=self.con.dbname)
        )
        try:
            globs = dict(
                tupleStr=('foo', 42),
                tupleNums=(1, 2),
                arrayTuple=[(10, 20), (30, 40)],
                arrayTuple2=[('a', [1]), ('b', [3, 4])],
                arrayTuple3=[(('a', True), [1]), (('b', False), [3, 4])],
            )
            scon = con.with_globals(**globs)
            chunks = [f'{name} := global {name}' for name in globs]
            res = await scon.query_single(f'select {{ {", ".join(chunks)} }}')
            dres = dataclasses.asdict(res)
            self.assertEqual(dres, globs)
        finally:
            await con.aclose()

    async def test_edgeql_globals_schema_types_01(self):
        # Non-computed globals don't add a schema type
        await self.con.execute('''
            create global best_card -> str;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            set global best_card := 'Dragon';
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            drop global best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            create module my_mod;
            create global my_mod::best_card -> str;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            set global my_mod::best_card := 'Dragon';
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            drop global my_mod::best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

    async def test_edgeql_globals_schema_types_02(self):
        # Computed scalar global adds a type
        await self.con.execute('''
            create global best_card := 'Dragon';
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            [{'name': 'default::best_card'}]
        )

        await self.con.execute('''
            drop global best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            create module my_mod;
            create global my_mod::best_card := 'Dragon';
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            [{'name': 'my_mod::best_card'}]
        )

        await self.con.execute('''
            drop global my_mod::best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

    async def test_edgeql_globals_schema_types_03(self):
        # Computed object global adds a type
        await self.con.execute('''
            create global best_card := (
                select Card filter .name = 'Dragon' limit 1
            );
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            [{'name': 'default::best_card'}]
        )

        await self.con.execute('''
            drop global best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            create module my_mod;
            create global my_mod::best_card := (
                select Card filter .name = 'Dragon' limit 1
            );
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            [{'name': 'my_mod::best_card'}]
        )

        await self.con.execute('''
            drop global my_mod::best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

    async def test_edgeql_globals_schema_types_04(self):
        # Computed object global with shape adds two types:
        # - one for the global
        # - one for the shape
        await self.con.execute('''
            create global best_card := (
                select Card {name}
                filter .name = 'Dragon' limit 1
            );
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%"
            order by .name;
            ''',
            [
                {'name': 'default::__best_card'},
                {'name': 'default::best_card'},
            ]
        )

        await self.con.execute('''
            drop global best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )

        await self.con.execute('''
            create module my_mod;
            create global my_mod::best_card := (
                select Card {name}
                filter .name = 'Dragon' limit 1
            );
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%"
            order by .name;
            ''',
            [
                {'name': 'my_mod::__best_card'},
                {'name': 'my_mod::best_card'},
            ]
        )

        await self.con.execute('''
            drop global my_mod::best_card;
        ''')
        await self.assert_query_result(
            r'''
            with module schema select Type { name }
            filter .name ilike "%best_card%";
            ''',
            []
        )
