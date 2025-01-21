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


class TestTriggers(tb.QueryTestCase):
    '''The scope of the tests is testing various modes of Object creation.'''

    NO_FACTOR = True

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'insert.esdl')

    SETUP = [
        '''
            alter type InsertTest {
                alter property name set required;
                alter property l2 set optional;
            };
            alter type Note {
                create multi property notes -> str;
            };
        ''',
    ]

    # TODO: Possible additional tests:
    # * more multi?
    # * more access policies?
    # * more use of __old__?
    # * the interaction of __old__ and access policies!!
    # * update on union

    async def do_basic_work(self):
        """Do a standardized set of DML operations.

        We'll run this with different triggers and observe the results
        """

        # This is multiple queries instead of one so that if one of them
        # errors, it is immediately obvious which.
        #
        # ... I forgot this at some point and merged them, so now I am
        # adding a note.
        await self.con.execute('''
            insert InsertTest { name := "a" };
        ''')

        await self.con.execute('''
            select {
              (insert InsertTest { name := "b" }),
              (update InsertTest filter .name = 'a'
               set { name := 'a!' }),
            };
        ''')

        await self.con.execute('''
            select {
              (insert InsertTest { name := "c" }),
              (insert DerivedTest { name := "d" }),
              (update InsertTest filter .name = 'b'
               set { name := 'b!' }),
              (delete InsertTest filter .name = "a!"),
            };
        ''')

        await self.con.execute('''
            select {
              (for x in {'e', 'f'} union (insert DerivedTest { name := x })),
              (delete InsertTest filter .name = "b!"),
             };
        ''')

        await self.con.execute('''
            update InsertTest filter .name = 'd'
            set { name := .name ++ '!' };
        ''')

        await self.con.execute('''
            for x in {'c', 'e'} union (
                update InsertTest filter .name = x
                set { name := x ++ '!' }
            );
        ''')

        await self.con.execute('''
            select {
              (update DerivedTest filter .name = 'f'
               set { name := 'f!' }),
              (delete DerivedTest filter .name = 'd!'),
            };
        ''')

        await self.con.execute('''
            delete InsertTest;
        ''')

    async def assert_notes(self, data):
        q = r"""
            select (group Note by .name)
            { name := .key.name, notes := .elements.note }
            order by .name;
        """
        # print(await self.con.query(q))
        await self.assert_query_result(q, data)

    # Actual tests now

    async def test_edgeql_triggers_insert_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "insert", 'notes': set("abcdef")},
        ])

        # We should still be able to insert Note normally
        await self.con.execute('''
            select {
              (insert InsertTest { name := "foo" }),
              (insert Note { name := "manual", note := "!" }),
            };
        ''')

    async def test_edgeql_triggers_update_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_upd after update for each do (
                insert Note {
                  name := "update",
                  note := (__old__.name ?? "") ++ " -> " ++ (__new__.name??"")
                }
              )
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "update", 'notes': set(f'{x} -> {x}!' for x in "abcdef")},
        ])

    async def test_edgeql_triggers_update_02(self):
        # This is the same as update_01 except the trigger is on DerivedTest
        await self.con.execute('''
            alter type DerivedTest {
              create trigger log_upd after update for each do (
                insert Note {
                  name := "update",
                  note := (__old__.name ?? "") ++ " -> " ++ (__new__.name??"")
                }
              )
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "update", 'notes': set(f'{x} -> {x}!' for x in "def")},
        ])

    async def test_edgeql_triggers_delete_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_del after delete for each do (
                insert Note { name := "delete", note := __old__.name }
              )
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "delete", 'notes': set(f'{x}!' for x in "abcdef")},
        ])

    async def test_edgeql_triggers_delete_02(self):
        # This is the same as delete_01 except the trigger is on DerivedTest
        await self.con.execute('''
            alter type DerivedTest {
              create trigger log_del after delete for each do (
                insert Note { name := "delete", note := __old__.name }
              )
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "delete", 'notes': set(f'{x}!' for x in "def")},
        ])

    async def test_edgeql_triggers_mixed_01(self):
        # Install triggers for everything
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_del after delete for each do (
                insert Note { name := "delete", note := __old__.name }
              );
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
              create trigger log_upd after update for each do (
                insert Note {
                  name := "update",
                  note := (__old__.name ?? "") ++ " -> " ++ (__new__.name??"")
                }
              )
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "delete", 'notes': set(f'{x}!' for x in "abcdef")},
            {'name': "insert", 'notes': set("abcdef")},
            {'name': "update", 'notes': set(f'{x} -> {x}!' for x in "abcdef")},
        ])

    async def test_edgeql_triggers_mixed_02(self):
        # Install double and triple triggers
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after insert, update for each do (
                insert Note { name := "new", note := __new__.name }
              );
              create trigger log_old after delete, update for each do (
                insert Note { name := "old", note := __old__.name }
              );
              create trigger log_all after delete, update, insert for each do (
                insert Note { name := "all", note := "." }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {
                'name': "all",
                'notes': ["."] * 18,
            },
            {
                'name': "new",
                'notes': set("abcdef") | {f'{x}!' for x in "abcdef"}
            },
            {
                'name': "old",
                'notes': set("abcdef") | {f'{x}!' for x in "abcdef"}
            },
        ])

    # MULTI!

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_triggers_multi_insert_01(self):
        await self.con.execute('''
            alter type InsertTest {
              alter property name set multi;

              create trigger log after insert for each do (
                insert Note {
                    name := "insert", note := assert_single(__new__.name) }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "insert", 'notes': set("abcdef")},
        ])

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_triggers_multi_mixed_01(self):
        # Install triggers for everything
        await self.con.execute('''
            alter type InsertTest {
              alter property name set multi;

              create trigger log_del after delete for each do (
                insert Note {
                  name := "delete",
                  note := assert_single(__old__.name)
                }
              );
              create trigger log after insert for each do (
                insert Note {
                  name := "insert",
                  note := assert_single(__new__.name)
                }
              );
              create trigger log_upd after update for each do (
                insert Note {
                  name := "update",
                  note := assert_single(
                    (__old__.name ?? "") ++ " -> " ++ (__new__.name??""))
                }
              )
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "delete", 'notes': set(f'{x}!' for x in "abcdef")},
            {'name': "insert", 'notes': set("abcdef")},
            {'name': "update", 'notes': set(f'{x} -> {x}!' for x in "abcdef")},
        ])

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_triggers_multi_mixed_02(self):
        # Install double and triple triggers
        await self.con.execute('''
            alter type InsertTest {
              alter property name set multi;

              create trigger log after insert, update for each do (
                insert Note {
                  name := "new",
                  note := assert_single(__new__.name)
                }
              );
              create trigger log_old after delete, update for each do (
                insert Note {
                  name := "old",
                  note := assert_single(__old__.name)
                }
              );
              create trigger log_all after delete, update, insert for each do (
                insert Note { name := "all", note := "." }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {
                'name': "all",
                'notes': ["."] * 18,
            },
            {
                'name': "new",
                'notes': set("abcdef") | {f'{x}!' for x in "abcdef"}
            },
            {
                'name': "old",
                'notes': set("abcdef") | {f'{x}!' for x in "abcdef"}
            },
        ])

    async def test_edgeql_triggers_mixed_all_01(self):
        # Install FOR ALL triggers for everything
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_del after delete for all do (
                insert Note { name := "delete", notes := __old__.name }
              );
              create trigger log_ins after insert for all do (
                insert Note { name := "insert", notes := __new__.name }
              );
              create trigger log_upd after update for all do (
                insert Note {
                  name := "update",
                  notes := __old__.name,
                  subject := (insert DerivedNote {
                    name := "", notes := __new__.name
                  })
                }
              )
            };
        ''')

        await self.do_basic_work()

        res = tb.bag([
            {"name": "insert", "notes": ["a"], "subject": None},
            {"name": "insert", "notes": ["b"], "subject": None},
            {"name": "insert", "notes": {"c", "d"}, "subject": None},
            {"name": "insert", "notes": {"e", "f"}, "subject": None},

            {"name": "update", "notes": ["a"], "subject": {"notes": ["a!"]}},
            {"name": "update", "notes": ["b"], "subject": {"notes": ["b!"]}},
            {"name": "update", "notes": ["d"], "subject": {"notes": ["d!"]}},
            {
                "name": "update",
                "notes": {"c", "e"},
                "subject": {"notes": {"c!", "e!"}}
            },
            {"name": "update", "notes": ["f"], "subject": {"notes": ["f!"]}},

            {"name": "delete", "notes": ["b!"], "subject": None},
            {"name": "delete", "notes": ["d!"], "subject": None},
            {"name": "delete", "notes": ["a!"], "subject": None},
            {"name": "delete", "notes": {"c!", "e!", "f!"}, "subject": None},
        ])

        await self.assert_query_result(
            '''
            select Note { name, notes, subject[is Note]: {notes} }
            filter .name != "";
            ''',
            res,
        )

    async def test_edgeql_triggers_mixed_all_02(self):
        # Install FOR ALL triggers for everything
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_new after insert, update for all do (
                insert Note { name := "new", notes := __new__.name }
              );
              create trigger log_old after delete, update for all do (
                insert Note { name := "old", notes := __old__.name }
              );
              create trigger log_all after delete, update, insert for all do (
                insert Note { name := "all", notes := "." }
              );
            };
        ''')

        await self.do_basic_work()

        res = tb.bag([
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "all", "notes": {"."}},
            {"name": "new", "notes": {"a"}},
            {"name": "new", "notes": {"c!", "e!"}},
            {"name": "new", "notes": {"d!"}},
            {"name": "new", "notes": {"e", "f"}},
            {"name": "new", "notes": {"f!"}},
            {"name": "new", "notes": {"b", "a!"}},
            {"name": "new", "notes": {"b!", "d", "c"}},
            {"name": "old", "notes": {"f", "d!"}},
            {"name": "old", "notes": {"a"}},
            {"name": "old", "notes": {"b", "a!"}},
            {"name": "old", "notes": {"b!"}},
            {"name": "old", "notes": {"d"}},
            {"name": "old", "notes": {"c", "e"}},
            {"name": "old", "notes": {"c!", "e!", "f!"}},
        ])

        await self.assert_query_result(
            '''
            select Note { name, notes } order by .name
            ''',
            res,
        )

    async def test_edgeql_triggers_enforce_errors_01(self):
        # Simulate a global constraint that we can't do with constraints:
        # ensure the *count* of subordinates in each InsertTest is unique
        # This is woefully inefficient though.
        # (A better approach would be to enforce that a _count field
        # matches the count (using policies, triggers, or best of all
        # rewrite rules), and then having an exclusive constraint on that.)
        await self.con.execute('''
            alter type InsertTest {
              create trigger check_distinct after insert, update for all do (
                assert_distinct(
                  (InsertTest { cnt := count(.subordinates) }.cnt),
                  message := "subordinate counts collide",
                )
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest { name := "0" };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                insert InsertTest { name := "1" };
            ''')

        await self.con.query('''
            insert InsertTest {
                name := "1",
                subordinates := (insert Subordinate { name := "a" }),
            };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                insert InsertTest {
                    name := "2",
                    subordinates := (insert Subordinate { name := "b" }),
                };
            ''')

        await self.con.query('''
            insert InsertTest {
                name := "2",
                subordinates := {
                  (insert Subordinate { name := "b" }),
                  (insert Subordinate { name := "c" }),
                }
            }
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "0"
                set { subordinates := (insert Subordinate { name := "d" }) }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "0"
                set { subordinates += (insert Subordinate { name := "d" }) }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "1"
                set { subordinates += (insert Subordinate { name := "d" }) }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "1"
                set { subordinates -= .subordinates }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "1"
                set { subordinates -= .subordinates }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "2"
                set { subordinates -= (select Subordinate filter .name = "b") }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name = "2"
                set { subordinates := (select Subordinate filter .name = "b") }
            ''')

        async with self.assertRaisesRegexTx(
                edgedb.ConstraintViolationError,
                r"subordinate counts collide"):
            await self.con.query('''
                update InsertTest filter .name in {"1", "2"}
                set { subordinates := Subordinate }
            ''')

        await self.con.query('''
            with c := (select Subordinate filter .name = "c"),
            select {
                (update InsertTest filter .name = "1"
                set { subordinates += c }),
                (update InsertTest filter .name = "2"
                set { subordinates -= c }),
            };
        ''')

        await self.con.query('''
            with c := (select Subordinate filter .name = "c"),
            select {
                (update InsertTest filter .name = "2"
                set { subordinates -= c }),
                (update InsertTest filter .name = "1"
                set { subordinates += c }),
            };
        ''')

    async def test_edgeql_triggers_enforce_errors_02(self):
        # Simulate a multi-table constraint that we can't do with constraints:
        # ensure the *sum* of the val fields in subordinates is zero
        # To do this we put triggers on both InsertTest for inserts and updates
        # and Subordinate for updates.
        await self.con.execute('''
            alter type InsertTest {
              create trigger check_subs after insert, update for each do (
                select assert(
                  sum(__new__.subordinates.val) = 0,
                  message := "subordinate sum is not zero for "++__new__.name,
                )
              );
            };
            alter type Subordinate {
              # use for all so that we semi-join deduplicate the InsertTests
              # before checking
              # Use a shape to drive the error check for fun (testing).
              create trigger check_subs after update for all do (
                (__new__.<subordinates[is InsertTest]) {
                  fail := assert(
                    sum(.subordinates.val) = 0,
                    message := "subordinate sum is not zero for " ++ .name,
                  )
                }
              );
            };
            create function sub(i: str) -> set of Subordinate using (
              select Subordinate filter .name = i
            );
        ''')

        await self.con.query('''
            for x in range_unpack(range(-10, 10)) union (
                insert Subordinate { name := <str>x, val := x }
            );
        ''')

        await self.con.query('''
            insert InsertTest { name := "a" }
        ''')

        err = lambda name: self.assertRaisesRegexTx(
            edgedb.InvalidValueError,
            f"subordinate sum is not zero for {name}"
        )

        await self.con.query('''
            insert InsertTest {
                name := "b",
                subordinates := assert_distinct(sub({"1", "2", "-3"}))
            }
        ''')

        async with err("c"):
            await self.con.query('''
                insert InsertTest {
                    name := "c", subordinates := assert_distinct(sub("1"))
                }
            ''')

        async with err("c"):
            await self.con.query('''
                insert InsertTest {
                    name := "c",
                    subordinates := assert_distinct(sub({"1", "-2"})),
                }
            ''')

        # Try some updates
        async with err("b"):
            await self.con.query('''
                update InsertTest filter .name = "b" set {
                    subordinates := assert_distinct(sub({"1", "2", "3"}))
                }
            ''')

        async with err("b"):
            await self.con.query('''
                update InsertTest filter .name = "b" set {
                    subordinates += assert_distinct(sub({"4", "-2"}))
                }
            ''')

        async with err("b"):
            await self.con.query('''
                update InsertTest filter .name = "b" set {
                    subordinates -= assert_distinct(sub({"2"}))
                }
            ''')

        await self.con.query('''
            update InsertTest filter .name = "b" set {
                subordinates += assert_distinct(sub({"-4", "-2", "6", "2"}))
            }
        ''')

        await self.con.query('''
            update InsertTest filter .name = "b" set {
                subordinates -= assert_distinct(
                  sub({"1", "6", "-4", "-3", "5"}))
            }
        ''')

        await self.con.query('''
            update InsertTest filter .name = "b" set {
                subordinates := assert_distinct(sub({"-1", "-2", "3"}))
            };
        ''')

        # Now try updating Subordinate
        async with err("b"):
            await self.con.query('''
                update Subordinate filter .name = "3"
                set { val := -3 };
            ''')

        await self.con.query('''
            insert InsertTest {
                name := "b2",
                subordinates := assert_distinct(sub({"-1", "-2", "3"}))
            }
        ''')

        async with err("b"):
            await self.con.query('''
                update Subordinate filter .name = "3"
                set { val := -3 };
            ''')

        # This one *should* work, though, since the sums still work out
        await self.con.query('''
            update Subordinate filter .name in {"-1", "-2", "3"}
            set { val := - .val };
        ''')
        # ... and set it back
        await self.con.query('''
            update Subordinate filter .name in {"-1", "-2", "3"}
            set { val := - .val };
        ''')

        # Now create a new InsertTest that uses one of those vals
        await self.con.query('''
            insert InsertTest {
                name := "d",
                subordinates := assert_distinct(sub({"3", "-3"}))
            }
        ''')

        # And now it *shouldn't* work
        async with err("d"):
            await self.con.query('''
                update Subordinate filter .name in {"-1", "-2", "3"}
                set { val := - .val };
            ''')

        # Make sure they fire with typename injection on
        async with err("a"):
            await self.con._fetchall('''
                update InsertTest filter .name = "a"
                set { subordinates := assert_distinct(sub("1")) };
            ''', __typenames__=True)

        async with err("c"):
            await self.con._fetchall('''
                insert InsertTest {
                    name := "c", subordinates := assert_distinct(sub("1"))
                }
            ''', __typenames__=True)

    async def test_edgeql_triggers_policies_01(self):
        # It is OK to see the newly created object during a trigger,
        # even if you shouldn't otherwise. (Much like with overlays
        # normally.)
        await self.con.execute('''
            alter type InsertTest {
              create access policy ins_ok allow insert;
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
            };
        ''')

        await self.con.query('''
            insert InsertTest {
                name := "x",
            }
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            [{'note': "x"}],
        )

    async def test_edgeql_triggers_policies_02(self):
        # But you *can't* see things by accessing them through the
        # "normal channels"
        await self.con.execute('''
            alter type InsertTest {
              create access policy ins_ok allow insert;
              create trigger log after insert for each do (
                insert Note {
                  name := "insert", note := <str>count(InsertTest)
                }
              );
            };
        ''')

        await self.con.query('''
            insert InsertTest {name := "x"};
            insert InsertTest {name := "y"};
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            tb.bag([
                {'note': "0"},
                {'note': "0"},
            ]),
        )

    async def test_edgeql_triggers_policies_03(self):
        await self.con.execute('''
            alter type Note {
              create access policy ok allow all;
              create access policy no_x deny insert using (
                (.note like 'x%') ?? false);
            };
            alter type InsertTest {
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
            };
        ''')

        await self.con.query('''
            insert InsertTest {name := "y"};
        ''')

        async with self.assertRaisesRegexTx(edgedb.AccessPolicyError, ''):
            await self.con.query('''
                insert InsertTest {name := "x"};
            ''')

    async def test_edgeql_triggers_policies_04(self):
        await self.con.execute('''
            alter type InsertTest {
              create access policy ok allow all;
              create access policy no_x deny select
                using (.name = 'xx');

              create trigger log after update for all do (
                insert Note {
                  name := "update",
                  note := <str>count(__old__)++"/"++<str>count(__new__)++"/"
                          ++<str>count(InsertTest),
                }
              );
            };
        ''')

        await self.con.query('''
            insert InsertTest {name := "x"};
            insert InsertTest {name := "y"};
        ''')

        await self.con.query('''
            update InsertTest set {name := .name ++ "x"};
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            tb.bag([
                {'note': "2/2/1"},
            ]),
        )

    async def test_edgeql_triggers_policies_05(self):
        await self.con.execute('''
            alter type Subordinate {
              create access policy ok allow all;
              create access policy no deny select using (
                any(.<subordinates[is InsertTest].l2 < 0)
              );
            };
            insert Subordinate { name := "foo" };
            insert Subordinate { name := "bar" };
            insert InsertTest { name := "x", subordinates := Subordinate };

            alter type InsertTest {
              create trigger log after update for each do (
                insert Note {
                  name := "update",
                  note := <str>count(__old__.subordinates)++
                          "/"++<str>count(__new__.subordinates)++
                          "/"++<str>count(InsertTest.subordinates),
                }
              );
            };
        ''')

        await self.con.execute('''
            update InsertTest set { l2 := -1 };
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            [{'note': "2/2/0"}],
        )

    async def test_edgeql_triggers_policies_06(self):
        await self.con.execute('''
            alter type Subordinate {
              create access policy ok allow all;
              create access policy no deny select;
            };

            alter type InsertTest {
              create trigger log after insert for each do (
                insert Note {
                  name := "insert",
                  note := <str>count(__new__.subordinates)
                }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest {
                name := "!",
                subordinates := {
                  (insert Subordinate { name := "foo" }),
                  (insert Subordinate { name := "bar" }),
                },
            };
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            [{'note': "2"}],
        )

    async def test_edgeql_triggers_policies_07(self):
        # Do some weird unioning + source rvar forcing in the trigger
        await self.con.execute('''
            alter type Subordinate {
              create access policy ok allow all;
              create access policy no deny select using (
                any(.<subordinates[is InsertTest].l2 < 0)
              );
            };
            insert Subordinate { name := "foo" };
            insert Subordinate { name := "bar" };
            insert InsertTest { name := "x", subordinates := Subordinate };

            alter type InsertTest {
              create trigger log after update for each do (
                insert Note {
                  name := "update",
                  note := <str>count(assert_exists((select {
                      __old__.subordinates,
                      __new__.subordinates,
                      InsertTest.subordinates,
                  } filter true)).name),
                }
              );
            };
        ''')

        await self.con.execute('''
            update InsertTest set { l2 := -1 };
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            [{'note': "4"}],
        )

    async def test_edgeql_triggers_policies_08(self):
        # Do some weird unioning + source rvar forcing in the trigger
        await self.con.execute('''
            alter type Subordinate {
              create access policy ok allow all;
              create access policy no deny select using (
                any(.<subordinates[is InsertTest].l2 < 0)
              );
              create access policy no_val deny select using (
                .val ?= -1
              )
            };
            insert Subordinate { name := "foo" };
            insert Subordinate { name := "bar" };
            insert InsertTest { name := "x", subordinates := Subordinate };

            alter type InsertTest {
              create trigger log after update for each do (
                insert Note {
                  name := "update",
                  note := <str>count(assert_exists((select {
                      (insert Subordinate { name := "lol", val := -1 }),
                      __old__.subordinates,
                      __new__.subordinates,
                  } filter true)).name),
                }
              );
            };
        ''')

        await self.con.execute('''
            update InsertTest set { l2 := -1 };
        ''')

        await self.assert_query_result(
            '''
            select Note { note }
            ''',
            [{'note': "5"}],
        )

    async def test_edgeql_triggers_chain_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
            };
            alter type Note {
              create trigger log after insert for each do (
                insert Subordinate { val := 1, name := __new__.note }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_notes([
            {'name': "insert", 'notes': set("abcdef")},
        ])

        await self.assert_query_result(
            '''
            select Subordinate.name
            ''',
            set("abcdef"),
        )

    async def test_edgeql_triggers_chain_02(self):
        async with self.assertRaisesRegexTx(
                edgedb.SchemaDefinitionError,
                'trigger on default::InsertTest after insert is recursive'):
            await self.con.execute('''
                alter type InsertTest {
                  create trigger log after insert for each do (
                    insert InsertTest { name := __new__.name ++ "!" }
                  );
                };
            ''')

    async def test_edgeql_triggers_chain_03(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
            };
            alter type Note {
              create trigger log after insert for each do (
                insert Subordinate { val := 1, name := __new__.note }
              );
            };
        ''')

        async with self.assertRaisesRegexTx(
                edgedb.QueryError,
                'would need to be executed in multiple stages on default::Note '
                'after insert'):
            await self.con.execute('''
                select {
                    (insert InsertTest { name := "foo" }),
                    (insert Note { name := "foo" }),
                }
            ''')

    async def test_edgeql_triggers_chain_04(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after update for each do (
                insert InsertTest { name := __new__.name ++ "!" }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest { name := "test" }
        ''')

        await self.con.execute('''
            update InsertTest
            filter InsertTest.name = "test"
            set { name := "updated" }
        ''')

        await self.assert_query_result(
            '''
            select InsertTest { name }
            ''',
            [{'name': 'updated'}, {'name': 'updated!'}],
        )

    async def test_edgeql_triggers_chain_05(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after insert for each do (
                insert Note { name := "insert", note := __new__.name }
              );
            };
        ''')

        await self.con.execute('''
            select {
                (insert Note { name := "foo" }),
            }
        ''')

        await self.assert_notes([
            {'name': "foo", 'notes': set()},
        ])

        await self.con.execute('''
            select {
                (insert InsertTest { name := "foo_insert" }),
                (
                  update Note
                  filter Note.name = "foo"
                  set { name := "foo_update" }
                ),
            }
        ''')

        await self.assert_notes([
            {'name': "foo_update", 'notes': set()},
            {'name': "insert", 'notes': set(["foo_insert"])},
        ])

    async def test_edgeql_triggers_tricky_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log after insert for each do (
                with X := (insert Note{ name := "x", subject := __new__.sub }),
                insert Note { name := "y", note := <str>count(X.subject) }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest {
                name := "test", sub := (insert Subordinate { name := "!" })
            }
        ''')

        await self.assert_query_result(
            '''
            select Note { name, note, subject } order by .name
            ''',
            [
                {'name': 'x', 'note': None, 'subject': {'id': str}},
                {'name': 'y', 'note': "1", 'subject': None},
            ]
        )

    async def test_edgeql_triggers_old_link_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_upd after update for each do (
                insert Note { name := "upd", note := __old__.__type__.name }
              );
              create trigger log_del after delete for each do (
                insert Note { name := "del", note := __old__.__type__.name }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest {
                name := "test",
            };
            update InsertTest set {};
            delete InsertTest;
        ''')

        await self.assert_query_result(
            '''
            select Note { name, note } order by .name
            ''',
            [
                {'name': 'del', 'note': "default::InsertTest"},
                {'name': 'upd', 'note': "default::InsertTest"},
            ]
        )

    async def test_edgeql_triggers_old_link_02(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_upd after update for each do (
                insert Note {
                    name := "upd", note := <str>count(__old__.subordinates) }
              );
              create trigger log_del after delete for each do (
                insert Note {
                    name := "del", note := <str>count(__old__.subordinates) }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest {
                name := "test",
                subordinates := (insert Subordinate { name := "foo" }),
            };
            update InsertTest set {};
            delete InsertTest;
        ''')

        await self.assert_query_result(
            '''
            select Note { name, note } order by .name
            ''',
            [
                {'name': 'del', 'note': "1"},
                {'name': 'upd', 'note': "1"},
            ]
        )

    async def test_edgeql_triggers_when_01(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_new after insert, update for each
              when (__new__.name not in {'a', 'f!'})
              do (
                insert Note { name := "new", note := __new__.name }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_query_result(
            '''
            select Note.note
            ''',
            tb.bag(['a!', 'b', 'b!', 'c', 'c!', 'd', 'd!', 'e', 'e!', 'f']),
        )

    @tb.ignore_warnings('more than one.* in a FILTER clause')
    async def test_edgeql_triggers_when_02(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_new after insert, update for each
              when (__new__.name = {'a', 'f!'})
              do (
                insert Note { name := "new", note := __new__.name }
              );
            };
        ''')

        await self.do_basic_work()

        await self.assert_query_result(
            '''
            select Note.note
            ''',
            tb.bag(['a', 'f!']),
        )

    async def test_edgeql_triggers_when_03(self):
        # Install FOR ALL triggers for everything
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_new after insert, update for all
              when (count(__new__) >= 2)
              do (
                insert Note { name := "new", notes := __new__.name }
              );
              create trigger log_old after delete, update for all
              when (count(__old__) >= 2)
              do (
                insert Note { name := "old", notes := __old__.name }
              );
            };
        ''')

        await self.do_basic_work()

        res = tb.bag([
            {"name": "new", "notes": {"c!", "e!"}},
            {"name": "new", "notes": {"e", "f"}},
            {"name": "new", "notes": {"b", "a!"}},
            {"name": "new", "notes": {"b!", "d", "c"}},
            {"name": "old", "notes": {"f", "d!"}},
            {"name": "old", "notes": {"b", "a!"}},
            {"name": "old", "notes": {"c", "e"}},
            {"name": "old", "notes": {"c!", "e!", "f!"}},
        ])

        await self.assert_query_result(
            '''
            select Note { name, notes } order by .name
            ''',
            res,
        )

    async def test_edgeql_triggers_when_04(self):
        await self.con.execute('''
            alter type InsertTest {
              create trigger log_new after insert, update for each
              when (__new__.l2 < 0)
              do (
                insert Note { name := "new", note := __new__.name }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest { name := "a" };
        ''')
        await self.con.execute('''
            insert InsertTest { name := "b", l2 := 10 };
        ''')
        await self.con.execute('''
            insert InsertTest { name := "c", l2 := -42 };
        ''')

        await self.assert_query_result(
            '''
            select Note.note
            ''',
            ['c'],
        )

    async def test_edgeql_triggers_when_bad(self):
        async with self.assertRaisesRegexTx(
                edgedb.SchemaDefinitionError,
                r"data-modifying statements are not allowed"):
            await self.con.query('''
                alter type InsertTest {
                  create trigger log_new after insert, update for each
                  when (exists (insert Note { name := "!" }))
                  do (
                    insert Note { name := "new", note := __new__.name }
                  );
                };
            ''')

    async def test_edgeql_triggers_cached_global_01(self):
        # Install FOR ALL triggers for everything
        await self.con.execute('''
            create alias CA := count(InsertTest);
            create global CG := count(InsertTest);
            create type X {
                create access policy asdf allow all using (global CG > 0)
            };
            alter type InsertTest {
              create trigger log after insert for each
              do (
                insert Note {
                    name := <str>assert_single(CA),
                    note := <str>(global CG),
                }
              );
            };
        ''')

        await self.con.execute('''
            insert InsertTest { name := <str>((global CG)) };
        ''')
        await self.assert_query_result(
            '''
            select Note { name, note }
            ''',
            [
                {'name': '1', 'note': '1'},
            ],
        )
