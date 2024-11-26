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


import os.path

import edgedb

from edb.testbase import server as tb


class TestRewrites(tb.QueryTestCase):

    NO_FACTOR = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas', 'movies.esdl')
    # Setting up some rewrites makes the tests run a bit faster
    # because we don't need to recompile the delta scripts for it.
    SETUP = [
        """
        create type Asdf {
          create property title -> str {
            create rewrite update using ('updated');
          };
        };
        alter type Asdf {
          alter property title {
            create rewrite insert using ('inserted');
          };
        };

        create abstract type Resource {
          create required property name: str {
            create rewrite insert, update using (str_trim(.name));
          };
        };

        create type Project extending Resource;

        create type Document extending Resource {
          create property text: str;
          create required property textUpdatedAt: std::datetime {
            set default := (std::datetime_of_statement());
            create rewrite update using ((
              IF __specified__.text
              THEN std::datetime_of_statement()
              ELSE __old__.textUpdatedAt
            ));
          };
        };

        alter type Content {
          drop access policy filter_title;
          drop access policy dml;
        };
    """
    ]

    # TO TEST:
    # * Trigger interactions
    # * multi (once supported)

    async def test_edgeql_rewrites_01(self):
        # basic overriding of properties
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using ('inserted');
                create rewrite update using ('updated');
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')

        # title is required, but we don't specify it
        await self.con.execute('insert Movie { }')

        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "inserted"}, {"title": "inserted"}],
        )

        await self.con.execute('update Movie set { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "updated"}, {"title": "updated"}],
        )

    async def test_edgeql_rewrites_02(self):
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using (.title);
                create rewrite update using (.title);
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title := "Whiplash" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash"}],
        )

        async with self.assertRaisesRegexTx(
            edgedb.MissingRequiredError, r"missing value for required property"
        ):
            await self.con.execute('insert Movie { }')

        # if title is specified
        # __subject__.title refers to the new value
        await self.con.execute('update Movie set { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather"}],
        )

        # if title is not specified
        # __subject__.title refers to the existing value
        await self.con.execute('update Movie set { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather"}],
        )

    async def test_edgeql_rewrites_03(self):
        # interaction with default
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite insert using (.title ++ ' (new)');
                set default := 'untitled';
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash (new)"}, {"title": "untitled (new)"}],
        )

    async def test_edgeql_rewrites_04(self):
        # __specified__

        await self.con.execute(
            '''
            alter type Content alter property title set optional;
            alter type Movie {
              create property title_specified: bool {
                create rewrite insert using (__specified__.title);
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('insert Movie')
        await self.assert_query_result(
            'select Movie { title, title_specified }',
            [
                {"title_specified": True},
                {"title_specified": False},
            ],
        )

    async def test_edgeql_rewrites_05(self):
        # Rewrites should also be applied to children types.

        await self.con.execute(
            '''
            alter type Content {
              create property updated_at -> str {
                create rewrite update using ('just now');
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title, updated_at }',
            [{"title": "The Godfather", "updated_at": None}],
        )

        await self.con.execute(
            'update Movie set { title:= "The Godfather II" }'
        )
        await self.assert_query_result(
            'select Movie { title, updated_at }',
            [{"title": "The Godfather II", "updated_at": "just now"}],
        )

    async def test_edgeql_rewrites_06(self):
        # Rewrites override parent overrides.

        await self.con.execute(
            '''
            alter type Content {
              alter property title {
                set optional;
                create rewrite update
                    using (__subject__.title ++ ' - content updated');
              };
            };
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - movie updated');
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')
        await self.con.execute('insert Content { title:= "Harry Potter" }')

        await self.con.execute('update Content set { title := .title }')
        await self.assert_query_result(
            'select Content { title } order by .title',
            [
                {"title": "Harry Potter - content updated"},
                {"title": "The Godfather - movie updated"},
            ],
        )

    async def test_edgeql_rewrites_07(self):
        await self.con.execute(
            '''
            alter type Content alter property title set optional;
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - updated');
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "The Godfather" }')

        await self.con.execute('update Movie set { title:= "Whiplash" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash - updated"}],
        )

        await self.con.execute('update Movie set { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "Whiplash - updated - updated"}],
        )

    async def test_edgeql_rewrites_08(self):
        # __old__
        await self.con.execute(
            '''
            alter type Movie {
              alter property title {
                create rewrite update
                    using (__subject__.title ++ ' - ' ++ __old__.title);
              };
            };
            '''
        )

        await self.con.execute('insert Movie { title:= "Whiplash" }')
        await self.con.execute('update Movie set { title:= "The Godfather" }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather - Whiplash"}],
        )

        await self.con.execute('update Movie set { }')
        await self.assert_query_result(
            'select Movie { title }',
            [{"title": "The Godfather - Whiplash - The Godfather - Whiplash"}],
        )

        async with self.assertRaisesRegexTx(
            edgedb.InvalidReferenceError,
            r"__old__ cannot be used in this expression"
        ):
            await self.con.execute(
                '''
                alter type Movie {
                  alter property title {
                    create rewrite insert using (__old__.title);
                  };
                };
                '''
            )

    async def test_edgeql_rewrites_09(self):
        # a common use case
        await self.con.execute(
            '''
            alter type Content {
              create property title_updated -> str {
                set default := 'never';
                create rewrite update using (
                    'just now'
                    if __specified__.title
                    else __old__.title_updated
                );
              };
            };
            '''
        )

        await self.con.execute('insert Content { title := "Harry Potter" }')
        await self.con.execute('insert Movie { title := "Whiplash" }')
        await self.con.execute(
            '''
            insert Movie {
                title := "The Godfather", title_updated := "a long time ago"
            }
            '''
        )

        await self.assert_query_result(
            'select Movie { title, title_updated } order by .title',
            [
                {"title": "The Godfather", "title_updated": "a long time ago"},
                {"title": "Whiplash", "title_updated": "never"},
            ],
        )

        await self.con.execute(
            '''
            update Content filter .title = "Whiplash" set { title := "Up" }
            '''
        )

        await self.assert_query_result(
            'select Movie { title, title_updated } order by .title',
            [
                {"title": "The Godfather", "title_updated": "a long time ago"},
                {"title": "Up", "title_updated": "just now"},
            ],
        )

    async def test_edgeql_rewrites_10(self):
        # Rewrites need to obey the covariant check
        await self.con.execute('insert Movie { title:= "Whiplash" }')

        await self.con.execute(
            '''
            create type Collection {
                create property name -> str;
                create link elements -> Content;
            };
            create type Library extending Collection {
                alter link elements set type Book;
            };


            alter type Collection {
              alter link elements {
                create rewrite update
                  using ((select Content filter .title = 'Whiplash' limit 1));
              };
            };
            '''
        )

        await self.con.execute('insert Library')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Collection set { elements := {} }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Library set { elements := {} }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Collection set { name := "x" }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Library set { name := "x" }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Collection set { }')

        async with self.assertRaisesRegexTx(
                edgedb.InvalidLinkTargetError,
                r"invalid target for link 'elements"):
            await self.con.execute('update Library set { }')

    async def test_edgeql_rewrites_11(self):
        # Update triggers child overrides on unknown fields

        await self.con.execute(
            '''
            alter type Movie {
              alter property release_year {
                create rewrite update
                    using (__subject__.release_year + 1);
              };
            };
            '''
        )

        await self.con.execute('''
            insert Movie { title := "The Godfather", release_year := 1972 }
        ''')

        await self.con.execute('''
            update Content set { title := .title ++ "!"}
        ''')

        await self.assert_query_result(
            'select Movie { title, release_year }',
            [
                {"title": "The Godfather!", "release_year": 1973},
            ],
        )

    async def test_edgeql_rewrites_12(self):
        # Update triggers child overrides on unknown fields
        # inherited from an unrelated base

        await self.con.execute(
            '''
            create type Counted {
              create required property count -> int64 {
                set default := 0;
                create rewrite update
                    using (__subject__.count + 1);
              }
            };

            alter type Movie extending Counted;
            '''
        )

        await self.con.execute('''
            insert Movie { title := "The Godfather" }
        ''')

        await self.con.execute('''
            update Content set { title := .title ++ "!"}
        ''')
        await self.con.execute('''
            update Content set { title := .title ++ "!"}
        ''')

        await self.assert_query_result(
            'select Movie { title, count }',
            [
                {"title": "The Godfather!!", "count": 2},
            ],
        )

    async def test_edgeql_rewrites_13(self):
        await self.con.execute(
            '''
            alter type Movie {
              alter property release_year {
                create rewrite insert using (__subject__.release_year + 1);
                create rewrite update using (__subject__.release_year + 1);
              };
              create access policy ok allow all;
              create access policy no_even deny insert, update write using
                ((.release_year ?? 0) % 2 = 0);
            };
            '''
        )

        async with self.assertRaisesRegexTx(edgedb.AccessPolicyError, ''):
            await self.con.execute('''
                insert Movie {
                    title := "The Godfather", release_year := 1971 };
            ''')

        await self.con.execute('''
            insert Movie { title := "The Godfather", release_year := 1972 };
        ''')

        await self.assert_query_result(
            'select Movie { title, release_year }',
            [
                {"title": "The Godfather", "release_year": 1973},
            ],
        )

        async with self.assertRaisesRegexTx(edgedb.AccessPolicyError, ''):
            await self.con.execute('''
                update Movie set { release_year := 101 };
            ''')

        await self.con.execute('''
            update Movie set { release_year := 100 };
        ''')

        await self.assert_query_result(
            'select Movie { title, release_year }',
            [
                {"title": "The Godfather", "release_year": 101},
            ],
        )

    async def test_edgeql_rewrites_14(self):
        await self.con.execute(
            '''
            create type Foo {
                create property r -> float64;
                create property x -> float64 {
                    create rewrite insert, update using (.r * 2);
                };
             };
            '''
        )

        await self.con.execute('''
            insert Foo { r := random() };
        ''')

        await self.assert_query_result(
            'select Foo { z := (.r * 2 = .x) }',
            [
                {"z": True},
            ],
        )

        await self.con.execute('''
            update Foo set { r := random() };
        ''')

        await self.assert_query_result(
            'select Foo { z := (.r * 2 = .x) }',
            [
                {"z": True},
            ],
        )

    async def test_edgeql_rewrites_15(self):
        await self.con.execute('''
            create type X {
                create property foo -> float64 {
                    create rewrite insert, update using (random())
                }
            };
        ''')

        await self.con.execute('''
            for _ in {1, 2} union (insert X);
        ''')

        await self.assert_query_result(
            'select count(distinct X.foo);',
            [2],
        )

        await self.con.execute('''
            update X set {};
        ''')

        await self.assert_query_result(
            'select count(distinct X.foo);',
            [2],
        )

    async def test_edgeql_rewrites_16(self):
        await self.con.execute('''
            create type X {
                create required property bar -> int64;
                create property foo -> float64 {
                    create rewrite insert, update using (.bar + random())
                }
            };
        ''')

        await self.con.execute('''
            for x in {1, 2} union (insert X { bar := x } );
        ''')

        await self.assert_query_result(
            'select math::floor(X.foo);',
            {1, 2},
        )

        await self.con.execute('''
            for x in {1, 2} union (
                update X filter .bar = x set { bar := x*2 } );
        ''')

        await self.assert_query_result(
            'select math::floor(X.foo);',
            {2, 4},
        )

        await self.assert_query_result(
            'select count(distinct (X { z := .foo - math::floor(.foo) }).z)',
            [2],
        )

    async def test_edgeql_rewrites_17(self):
        # Test stuff that *references* multi properties
        check = 'select (S.sum, S.delta)'

        # XXX: I bet it doesn't work for defaults
        await self.con.execute('''
            create type S {
                create multi property vals -> int64;
                create property sum -> int64 {
                    create rewrite insert, update using (sum(.vals))
                };
                create property delta -> int64 {
                    create rewrite insert using (sum(.vals));
                    create rewrite update using
                      (sum(.vals) - sum(__old__.vals))
                };
            };
        ''')

        await self.con.execute('''
            insert S { vals := {1, 2, 3} }
        ''')
        await self.assert_query_result(check, [(6, 6)])

        await self.con.execute('''
            update S set { vals := {4, 5, 6} }
        ''')
        await self.assert_query_result(check, [(15, 9)])

        await self.con.execute('''
            update S set { vals += {3, 4} }
        ''')
        await self.assert_query_result(check, [(22, 7)])

        await self.con.execute('''
            update S set { vals -= 5 }
        ''')
        await self.assert_query_result(check, [(17, -5)])

    async def test_edgeql_rewrites_18(self):
        # Rewrites with DML in them
        await self.con.execute('''
            create type Tgt { create property name -> str };
            create type D {
                create property name -> str;
                create link t -> Tgt {
                    create rewrite insert, update using (
                        insert Tgt { name := __subject__.name }
                    );
                };
            };
        ''')

        await self.con.execute('''
            insert D { name := "foo" }
        ''')

        await self.con.execute('''
            for x in {'bar', 'baz'} union (
                insert D { name := x }
            )
        ''')

        await self.assert_query_result(
            '''
                select D { name, tname := .t.name }
            ''',
            tb.bag([
                {'name': "foo", 'tname': "foo"},
                {'name': "bar", 'tname': "bar"},
                {'name': "baz", 'tname': "baz"},
            ]),
        )

        await self.con.execute('''
            update D filter .name = 'foo' set { name := "spam" }
        ''')

        await self.con.execute('''
            for x in {('bar', 'eggs'), ('baz', 'ham')} union (
                update D filter .name = x.0 set { name := x.1 }
            )
        ''')

        await self.assert_query_result(
            '''
                select D { name, tname := .t.name }
            ''',
            tb.bag([
                {'name': "spam", 'tname': "spam"},
                {'name': "eggs", 'tname': "eggs"},
                {'name': "ham", 'tname': "ham"},
            ]),
        )

        await self.con.execute('''
            update D set { name := .name ++ "!" }
        ''')

        await self.assert_query_result(
            '''
                select D { name, tname := .t.name }
            ''',
            tb.bag([
                {'name': "spam!", 'tname': "spam!"},
                {'name': "eggs!", 'tname': "eggs!"},
                {'name': "ham!", 'tname': "ham!"},
            ]),
        )

    async def test_edgeql_rewrites_19(self):
        # Rewrites with DML in them, and the DML has a chained rewrite
        await self.con.execute('''
            create type Tgt { create property name -> str {
                create rewrite insert using (.name ++ '!')
            } };
            create type D {
                create property name -> str;
                create link t -> Tgt {
                    create rewrite insert, update using (
                        insert Tgt { name := __subject__.name }
                    );
                };
            };
        ''')

        await self.con.execute('''
            insert D { name := "foo" }
        ''')

        await self.con.execute('''
            for x in {'bar', 'baz'} union (
                insert D { name := x }
            )
        ''')

        await self.assert_query_result(
            '''
                select D { name, tname := .t.name }
            ''',
            tb.bag([
                {'name': "foo", 'tname': "foo!"},
                {'name': "bar", 'tname': "bar!"},
                {'name': "baz", 'tname': "baz!"},
            ]),
        )

        await self.con.execute('''
            update D filter .name = 'foo' set { name := "spam" }
        ''')

        await self.con.execute('''
            for x in {('bar', 'eggs'), ('baz', 'ham')} union (
                update D filter .name = x.0 set { name := x.1 }
            )
        ''')

        await self.assert_query_result(
            '''
                select D { name, tname := .t.name }
            ''',
            tb.bag([
                {'name': "spam", 'tname': "spam!"},
                {'name': "eggs", 'tname': "eggs!"},
                {'name': "ham", 'tname': "ham!"},
            ]),
        )

        await self.con.execute('''
            update D set { name := .name ++ "!" }
        ''')

        await self.assert_query_result(
            '''
                select D { name, tname := .t.name }
            ''',
            tb.bag([
                {'name': "spam!", 'tname': "spam!!"},
                {'name': "eggs!", 'tname': "eggs!!"},
                {'name': "ham!", 'tname': "ham!!"},
            ]),
        )

    async def test_edgeql_rewrites_20(self):
        async with self.assertRaisesRegexTx(
            edgedb.QueryError, r"rewrite rule cycle"
        ):
            await self.con.execute('''
                create type Recursive;
                alter type Recursive {
                    create link rec -> Recursive {
                        create rewrite insert using (insert Recursive) } };
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.QueryError, r"rewrite rule cycle"
        ):
            await self.con.execute('''
                create type Foo;
                create type Bar {
                    create link rec -> Foo {
                        create rewrite insert using (insert Foo) } };
                alter type Foo {
                    create link rec -> Bar {
                        create rewrite insert using (insert Bar) } };
            ''')

    async def test_edgeql_rewrites_21(self):
        await self.con.execute('''
            create type Conflicted;
            alter type Conflicted {
                create property a -> str {
                    create rewrite insert using ('nope') };

                create constraint exclusive on (.a);
            };
            create type SubConflicted extending Conflicted;
        ''')

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"INSERT UNLESS CONFLICT cannot be used on .* have a rewrite rule"
        ):
            await self.con.execute('''
                INSERT Conflicted
                UNLESS CONFLICT ON (.a)
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"INSERT UNLESS CONFLICT cannot be used on .* have a rewrite rule"
        ):
            await self.con.execute('''
                INSERT Conflicted { a := 'hello' }
                UNLESS CONFLICT ON (.a)
            ''')

        async with self.assertRaisesRegexTx(
            edgedb.UnsupportedFeatureError,
            r"INSERT UNLESS CONFLICT cannot be used on .* have a rewrite rule"
        ):
            await self.con.execute('''
                INSERT Conflicted { a := 'hello' }
                UNLESS CONFLICT
            ''')

    async def test_edgeql_rewrites_22(self):
        await self.con.execute(
            '''
            insert Project { name := ' hello ' };
            '''
        )
        await self.assert_query_result(
            '''
                select Project { name }
            ''',
            [
                {'name': 'hello'},
            ],
        )

        await self.con.execute(
            '''
            update Project set { name := ' world ' };
            '''
        )
        await self.assert_query_result(
            '''
                select Project { name }
            ''',
            [
                {'name': 'world'},
            ],
        )

        await self.con.execute(
            '''
            alter type Project {
                alter property name {
                    create rewrite insert, update using ('hidden');
                };
            };
            '''
        )

        await self.con.execute(
            '''
            insert Project { name := ' hey ' };
            '''
        )
        await self.assert_query_result(
            '''
                select Project { name }
            ''',
            tb.bag(
                [
                    {'name': 'world'},
                    {'name': 'hidden'},
                ]
            ),
        )

        await self.con.execute(
            '''
            update Project set { name := ' hoy ' };
            '''
        )
        await self.assert_query_result(
            '''
                select Project { name }
            ''',
            tb.bag(
                [
                    {'name': 'hidden'},
                    {'name': 'hidden'},
                ]
            ),
        )

    async def test_edgeql_rewrites_23(self):
        await self.con.execute(
            '''
            alter type Person {
                alter property first_name {
                    create rewrite update using ('updated');
                };
            };
            insert Person { first_name := 'initial' };
            '''
        )
        await self.assert_query_result(
            'select Person { first_name }',
            [{'first_name': 'initial'}],
        )

        await self.con.execute(
            '''
            with A := Person
            update A set {};
            '''
        )
        await self.assert_query_result(
            'select Person { first_name }',
            [{'first_name': 'updated'}],
        )

    async def test_edgeql_rewrites_24(self):
        await self.con.execute(
            '''
            create type X {
                create property tup -> tuple<int64, str> {
                    create rewrite insert, update using ((1, '2'));
                };
            };
            insert X;
            '''
        )
        await self.assert_query_result(
            'select X { tup }',
            [{'tup': (1, '2')}],
        )
        await self.con.execute(
            '''
            update X set {};
            '''
        )
        await self.assert_query_result(
            'select X { tup }',
            [{'tup': (1, '2')}],
        )

    async def test_edgeql_rewrites_25(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"rewrite expression is of invalid type",
        ):
            await self.con.execute(
                '''
                create type X {
                    create property foo -> str {
                        create rewrite insert using (10);
                    };
                };
                '''
            )

        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"rewrite expression may not include a shape",
        ):
            await self.con.execute(
                '''
                create type X {
                    create link foo -> std::Object {
                        create rewrite insert using (
                            (select std::Object { __type__: {name} })
                        );
                    };
                };
                '''
            )

    async def test_edgeql_rewrites_26(self):
        async with self.assertRaisesRegexTx(
            edgedb.SchemaDefinitionError,
            r"rewrites on link properties are not supported",
        ):
            await self.con.execute(
                '''
                create type X {
                    create link foo -> std::Object {
                        create property bar: int32 {
                            create rewrite insert using ('hello');
                        };
                    };
                };
                '''
            )

    async def test_edgeql_rewrites_27(self):
        await self.con.execute(
            '''
            create type Foo {
                create property will_be_true: bool {
                    create rewrite update using (__subject__ = __old__);
                };
            };
            insert Foo { will_be_true := false };
            '''
        )
        await self.assert_query_result(
            'select Foo { will_be_true }',
            [{'will_be_true': False}]
        )
        await self.con.execute('update Foo set { };')
        await self.assert_query_result(
            'select Foo { will_be_true }',
            [{'will_be_true': True}]
        )

    async def test_edgeql_rewrites_28(self):
        await self.con.execute(
            '''
            create type Address {
                create property coordinates: tuple<lat: float32, lng: float32>;
                create property updated_at: str {
                    create rewrite insert using ('now')
                };
            };
            insert Address {
                coordinates := (
                    lat := <std::float32>40.07987,
                    lng := <std::float32>20.56509
                )
            };
            '''
        )
        await self.assert_query_result(
            'select Address { coordinates, updated_at }',
            [
                {
                    'coordinates': {'lat': 40.07987, 'lng': 20.56509},
                    'updated_at': 'now'
                }
            ]
        )

    async def test_edgeql_rewrites_29(self):
        # see https://github.com/edgedb/edgedb/issues/7048

        # these tests check that subject of an update rewrite is the child
        # object and not parent that is being updated
        await self.con.execute(
            '''
            update std::Object set { };
            '''
        )

        await self.con.execute(
            '''
            update Project set { name := '## redacted ##' }
            '''
        )
