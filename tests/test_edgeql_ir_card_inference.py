#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2012-present MagicStack Inc. and the EdgeDB authors.
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
import textwrap

from edb import errors
from edb.testbase import lang as tb

from edb.edgeql import compiler
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser


class TestEdgeQLCardinalityInference(tb.BaseEdgeQLCompilerTest):
    """Unit tests for cardinality inference."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_ir_inference.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse_query(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                modaliases={None: 'default'},
            ),
        )

        if not expected:
            return

        # The expected cardinality is either given for the whole query
        # (by default) or for a specific element of the top-level
        # shape. In case of the specific element the name of the shape
        # element must be given followed by ":" and then the
        # cardinality.
        exp = textwrap.dedent(expected).strip(' \n').split(':')

        if len(exp) == 1:
            field = None
            expected_cardinality = qltypes.Cardinality(exp[0])
        elif len(exp) == 2:
            field = exp[0].strip()
            expected_cardinality = qltypes.Cardinality(exp[1].strip())
        else:
            raise ValueError(
                f'unrecognized expected specification: {expected!r}')

        if field is not None:
            shape = ir.expr.expr.result.shape
            for el, _ in shape:
                if str(el.path_id.rptr_name()).endswith(field):
                    card = el.expr.ptrref.out_cardinality
                    self.assertEqual(card, expected_cardinality,
                                     'unexpected cardinality:\n' + source)
                    break
            else:
                raise AssertionError(f'shape field not found: {field!r}')

        else:
            self.assertEqual(ir.cardinality, expected_cardinality,
                             'unexpected cardinality:\n' + source)

    def test_edgeql_ir_card_inference_00(self):
        """
        SELECT Card
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_01(self):
        """
        SELECT Card FILTER Card.name = 'Djinn'
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_02(self):
        """
        SELECT Card FILTER 'Djinn' = Card.name
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_03(self):
        """
        SELECT Card FILTER 'foo' = 'foo' AND 'Djinn' = Card.name
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_04(self):
        """
        SELECT Card FILTER 'foo' = 'foo' OR 'Djinn' = Card.name
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_05(self):
        """
        SELECT Card FILTER Card.id = <uuid>'...'
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_06(self):
        """
        WITH C2 := Card
        SELECT Card FILTER Card = (SELECT C2 FILTER C2.name = 'Djinn')
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_07(self):
        """
        WITH C2 := DETACHED Card
        SELECT Card FILTER Card = (SELECT C2 FILTER C2.name = 'Djinn')
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_08(self):
        """
        SELECT Card LIMIT 1
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_09(self):
        """
        SELECT Card FILTER Card.<deck[IS User].name = 'Bob'
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_10(self):
        """
        SELECT 1
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_11(self):
        """
        SELECT {1, 2, 3}
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_12(self):
        """
        SELECT {1, 2, 3, Card.cost}
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_13(self):
        """
        SELECT array_agg({1, 2, 3})
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_14(self):
        """
        SELECT array_agg(Card.cost)
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_15(self):
        """
        SELECT to_str(Card.cost)
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_16(self):
        """
        SELECT to_str((SELECT Card.cost LIMIT 1))
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_17(self):
        """
        SELECT to_str({1, (SELECT Card.cost LIMIT 1)})
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_18(self):
        """
        SELECT to_str(1)
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_19(self):
        """
        SELECT 1 + 2
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_20(self):
        """
        SELECT 1 + (2 UNION 3)
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_21(self):
        """
        SELECT 1 + Card.cost
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_22(self):
        """
        SELECT (SELECT Card LIMIT 1).cost ?? 99
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_23(self):
        """
        SELECT (SELECT Card LIMIT 1).element ?? (SELECT User LIMIT 1).name
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_24(self):
        """
        SELECT (SELECT Card LIMIT 1).element ?= 'fire'
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_25(self):
        """
        SELECT Named {
            name
        }
% OK %
        name: ONE
        """

    def test_edgeql_ir_card_inference_26(self):
        """
        SELECT User {
            foo := .name
        }
% OK %
        foo: ONE
        """

    def test_edgeql_ir_card_inference_27(self):
        """
        SELECT User {
            foo := 'prefix_' ++ .name
        }
% OK %
        foo: ONE
        """

    def test_edgeql_ir_card_inference_28(self):
        """
        SELECT User {
            deck_cost
        }
% OK %
        deck_cost: ONE
        """

    def test_edgeql_ir_card_inference_29(self):
        """
        SELECT User {
            dc := sum(.deck.cost)
        }
% OK %
        dc: ONE
        """

    def test_edgeql_ir_card_inference_30(self):
        """
        SELECT User {
            deck
        }
% OK %
        deck: MANY
        """

    def test_edgeql_ir_card_inference_31(self):
        """
        SELECT Card {
            owners
        }
% OK %
        owners: MANY
        """

    def test_edgeql_ir_card_inference_32(self):
        """
        WITH
            A := (SELECT Award LIMIT 1)
        # the "awards" are exclusive
        SELECT A.<awards[IS User]
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_33(self):
        """
        SELECT Award {
            # the "awards" are exclusive
            recipient := .<awards[IS User]
        }
% OK %
        recipient: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_34(self):
        """
        SELECT Award {
            rec
        }
% OK %
        rec: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_35(self):
        """
        SELECT AwardAlias {
            recipient
        }
% OK %
        recipient: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_36(self):
        """
        SELECT Eert {
            parent
        }
% OK %
        parent: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_36b(self):
        """
        SELECT Eert {
            asdf := .<children[is Eert]
        }
% OK %
        asdf: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_36c(self):
        """
        SELECT Eert {
            asdf := .<children[is Asdf]
        }
% OK %
        asdf: MANY
        """

    def test_edgeql_ir_card_inference_36d(self):
        """
        SELECT Eert {
            asdf := .<children[is Object]
        }
% OK %
        asdf: MANY
        """

    def test_edgeql_ir_card_inference_37(self):
        """
        SELECT Report {
            user_name := .user.name
        }
% OK %
        user_name: ONE
        """

    def test_edgeql_ir_card_inference_38(self):
        """
        SELECT Report {
            name := .user.name
        }
% OK %
        name: ONE
        """

    @tb.must_fail(errors.QueryError,
                  "possibly an empty set", line=3, col=13)
    def test_edgeql_ir_card_inference_39(self):
        """
        SELECT Report {
            name := <str>{}
        }
        """

    @tb.must_fail(errors.QueryError,
                  "possibly more than one element", line=3, col=13)
    def test_edgeql_ir_card_inference_40(self):
        """
        SELECT Report {
            single foo := User.name
        }
        """

    def test_edgeql_ir_card_inference_41(self):
        """
        SELECT User.deck@count
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_42(self):
        """
        SELECT Report.user@note
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_43(self):
        """
        SELECT User {
            foo := .deck@count
        }
% OK %
        foo: MANY
        """

    def test_edgeql_ir_card_inference_44(self):
        """
        SELECT Report {
            foo := .user@note
        }
% OK %
        foo: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_45(self):
        """
        SELECT Report {
            subtitle := 'aaa'
        }
% OK %
        subtitle: ONE
        """

    def test_edgeql_ir_card_inference_46(self):
        """
        SELECT Named {
            as_card := Named[IS Card]
        }
% OK %
        as_card: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_47(self):
        """
        SELECT User {
            foo := EXISTS(.friends)
        }
% OK %
        foo: ONE
        """

    def test_edgeql_ir_card_inference_48(self):
        """
        SELECT Card {
            o_name := .owners.name,
        }
% OK %
        o_name: MANY
        """

    def test_edgeql_ir_card_inference_49(self):
        """
        SELECT User {
            name,
            fire_deck := (
                SELECT User.deck {name, element}
                FILTER .element = 'Fire'
                ORDER BY .name
            ).name
        }
% OK %
        fire_deck: MANY
        """

    def test_edgeql_ir_card_inference_50(self):
        """
        INSERT User {name := "Timmy"}
        UNLESS CONFLICT
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_51(self):
        """
        INSERT User {name := "Johnny"}
        UNLESS CONFLICT ON (.name)
        ELSE User
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_52(self):
        """
        INSERT User {name := "Spike"}
        UNLESS CONFLICT ON (.name)
        ELSE Card
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_53(self):
        """
        INSERT User {name := "Madz"}
        UNLESS CONFLICT ON (.name)
        ELSE (DETACHED (INSERT User {name := "Madz2"}))
% OK %
        ONE
        """

    # some tests of object constraints
    def test_edgeql_ir_card_inference_54(self):
        """
        SELECT Person FILTER .first = "Phil" AND .last = "Emarg"
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_55(self):
        """
        SELECT Person FILTER .first = "Phil"
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_56(self):
        """
        SELECT Person FILTER .email = "test@example.com"
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_57(self):
        """
        SELECT Person { first } FILTER .p = 7 AND .q = 3
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_58(self):
        """
        SELECT Person FILTER .last = "Hatch" AND .first = "Madeline"
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_59(self):
        """
        SELECT Person FILTER .p = 7 AND .q = 3 AND .first = "???"
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_60(self):
        """
        SELECT Person
        FILTER .p = 12 AND .card = (SELECT Card FILTER .name = 'Imp')
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_60b(self):
        """
        SELECT Person
        FILTER .p = 12 AND .card.name = 'Imp'
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_61(self):
        """
        SELECT Person FILTER .first = "Phil" OR .last = "Emarg"
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_62(self):
        """
        SELECT Person FILTER .p = 7 AND .q = 3 AND .last = "Whatever"
% OK %
        AT_MOST_ONE
        """

    @tb.must_fail(errors.QueryError,
                  "possibly more than one element")
    def test_edgeql_ir_card_inference_63(self):
        """
        WITH X := User { busted := (SELECT 1 ORDER BY {1,2}) },
        SELECT X
        """

    def test_edgeql_ir_card_inference_64(self):
        """
        SELECT (FOR x IN {1,2} UNION (SELECT User { m := x })) { m }
% OK %
        m: ONE
        """

    def test_edgeql_ir_card_inference_65(self):
        """
        SELECT (SELECT User { multi m := 1 }) { m }
% OK %
        m: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_66(self):
        """
        WITH Z := (SELECT (SELECT User) ORDER BY .name), SELECT Z
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_67(self):
        """
        SELECT { o := (SELECT (SELECT User) ORDER BY .name) }
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_68(self):
        """
        SELECT 1 FILTER false
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_69(self):
        """
        SELECT {1, 2} FILTER false
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_70(self):
        """
        SELECT (1, 'a')
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_71(self):
        """
        SELECT (1, Card.name)
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_71b(self):
        """
        SELECT ((1, Card {name}),).0
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_72(self):
        """
        SELECT {a := 42}
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_73(self):
        # Make sure that a union of free shapes still ends up
        # with cardinality AT_LEAST_ONE.
        """
        FOR x IN {0, 1} UNION {a := x}
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_74(self):
        """
        SELECT taking_opt_returning_non_opt("foo")
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_75(self):
        """
        SELECT taking_opt_returning_non_opt(<str>{})
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_76(self):
        """
        SELECT taking_non_opt_returning_opt("foo")
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_77(self):
        """
        SELECT taking_non_opt_returning_opt(<str>{})
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_78(self):
        """
        SELECT len("foo")
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_79(self):
        """
        SELECT len(<str>{})
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_80(self):
        """
        WITH s := {1, 2, 3}
        SELECT max(s)
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_81(self):
        """
        SELECT max(Person.p)
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_82(self):
        """
        SELECT assert_single(Person.p)
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_83(self):
        """
        SELECT Card {
            element := assert_single(.element ++ "1")
        }
% OK %
        element: ONE
        """

    def test_edgeql_ir_card_inference_84(self):
        """
        SELECT array_get([1, 2, 3], {0, 2})
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_85(self):
        """
        SELECT User { optional multi m := 1 }
% OK %
        m: MANY
        """

    def test_edgeql_ir_card_inference_86(self):
        """
        SELECT User { required multi m := 1 }
% OK %
        m: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_87(self):
        """
        SELECT User { optional m := 1 }
% OK %
        m: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_88(self):
        """
        SELECT User { m := assert_distinct(1) }
% OK %
        m: ONE
        """

    def test_edgeql_ir_card_inference_89(self):
        """
        SELECT User { m := assert_distinct(Card) }
% OK %
        m: MANY
        """

    def test_edgeql_ir_card_inference_90(self):
        """
        SELECT User { m := assert_distinct(assert_exists(Card)) }
% OK %
        m: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_91(self):
        """
        SELECT User {
            m := assert_distinct(assert_exists(assert_single(Card)))
        }
% OK %
        m: ONE
        """

    def test_edgeql_ir_card_inference_92(self):
        """
        WITH
            inserted := (INSERT Award { name := <str>$0 }),
            all := (inserted UNION (SELECT Award)),
        SELECT DISTINCT (all { name })
        ORDER BY .name ASC
% OK %
        name: ONE
        """

    def test_edgeql_ir_card_inference_93(self):
        """
        SELECT (User { friends: { required bs := .name } },
                User.friends.name ?? 'a')
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_94(self):
        """
        SELECT User { foo := enumerate(.name) }
% OK %
        foo: ONE
        """

    def test_edgeql_ir_card_inference_95(self):
        """
        WITH x := User
        SELECT (
            WITH y := x
            SELECT (y,).0
        )
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_96(self):
        """
        SELECT (
            (SELECT User),
            (User,).0,
        )
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_97(self):
        """
        SELECT (
            (User,).0,
            (User,).0,
        )
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_98(self):
        """
        SELECT (Card.name ?? "N/A", Card.element ?? "N/A")
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_99(self):
        """
        SELECT {1, 2} LIMIT 1
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_100(self):
        """
        SELECT assert_exists(User) LIMIT 1
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_101(self):
        """
        SELECT 1 LIMIT 0
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_102(self):
        """
        SELECT 1 LIMIT (SELECT count(User))
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_103(self):
        """
        SELECT {1, 2} LIMIT (SELECT count(User))
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_104(self):
        """
        SELECT 1 OFFSET 2
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_105(self):
        """
        select User
        filter .avatar.name = 'Dragon'
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_106(self):
        """
        select User
        filter .unique_avatar.name = 'Dragon'
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_107(self):
        """
        WITH
          __scope_0_Hero := DETACHED default::User
        UPDATE __scope_0_Hero
        FILTER (__scope_0_Hero.name = "Spider-Man")
        SET {
          name := ("The Amazing " ++ __scope_0_Hero.name)
        }
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_108(self):
        """
        WITH
          __scope_0_Hero := DETACHED default::User
        SELECT __scope_0_Hero
        FILTER (__scope_0_Hero.name = "Spider-Man")
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_109(self):
        """
        select User
        filter (detached (select User limit 1)).name = 'Alice'
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_110(self):
        """
        with z := (select User { asdf := .name })
        select (
            even := z.asdf,
            elements := count(z)
        )
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_111(self):
        """
        with z := (select User { asdf := {.name} })
        select (
            even := z.asdf,
            elements := count(z)
        )
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_112(self):
        """
        select <str>to_json('null')
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_113(self):
        """
        select <array<str>>[]
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_114(self):
        """
        select 1 + (2 intersect 3)
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_115(self):
        """
        select 1 + (2 intersect {3, 4})
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_116(self):
        """
        select 1 + ({2, 3} intersect {3, 4})
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_117(self):
        """
        select 1 + ({2, 3} intersect <int64>{})
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_118(self):
        """
        select 1 + (2 except 3)
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_119(self):
        """
        select 1 + (2 except {3, 4})
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_120(self):
        """
        select 1 + ({2, 3} except {3, 4})
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_121(self):
        """
        with X := {User, User},
        select X filter .name = 'Alice'
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_122(self):
        """
        with X := {User, User},
        update X filter .name = 'Alice' set { }
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_123(self):
        """
        select Card { req_awards }
% OK %
        req_awards: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_124(self):
        """
        select Card { x := .req_awards }
% OK %
        x: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_125(self):
        """
        select Card { required x := .req_awards }
% OK %
        x: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_126(self):
        """
        select Card { req_tags }
% OK %
        req_tags: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_127(self):
        """
        select Card { x := .req_tags }
% OK %
        x: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_128(self):
        """
        select Card { required x := .req_tags }
% OK %
        x: AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_129(self):
        """
        select assert(<bool>{})
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_130(self):
        """
        select assert(<bool>{}, message := {'uh', 'oh'})
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_131(self):
        """
        select assert(true, message := {'uh', 'oh'})
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_132(self):
        """
        select distinct <str>{}
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_133(self):
        """
        select distinct 1
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_134(self):
        """
        select distinct {1, 2}
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_135(self):
        """
        <str>{} if true else {'foo', 'bar'}
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_136(self):
        """
        <str>{} if true else 'foo'
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_137(self):
        """
        'bar' if true else 'foo'
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_138(self):
        """
        assert_exists(1, message := {"uh", "oh"})
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_139(self):
        """
        if <bool>$0 then
            (insert User { name := "test" })
        else
            (insert User { name := "???" })
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_140(self):
        """
        if <bool>$0 then
            (insert User { name := "test" })
        else
            {(insert User { name := "???" }), (insert User { name := "!!!" })}
% OK %
        AT_LEAST_ONE
        """

    def test_edgeql_ir_card_inference_141(self):
        """
        if <bool>$0 then
            (insert User { name := "test" })
        else
            <User>{}
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_142(self):
        """
        select Named { [is Card].element }
% OK %
        element: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_143(self):
        """
        select Named { element := [is Card].element }
% OK %
        element: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_144(self):
        """
        select (
          select assert_exists(Named) { [is Card].element } limit 1).element
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_145(self):
        """
        select Named { [is Named].name }
% OK %
        name: ONE
        """

    def test_edgeql_ir_card_inference_146(self):
        """
        select User { [is Named].name }
% OK %
        name: ONE
        """

    @tb.must_fail(errors.QueryError,
                  "possibly an empty set returned")
    def test_edgeql_ir_card_inference_147(self):
        """
        select Named { [is User].name }
        """

    @tb.must_fail(errors.QueryError,
                  "possibly an empty set returned")
    def test_edgeql_ir_card_inference_148(self):
        """
        select Named { name := [is User].name }
        """

    @tb.must_fail(errors.QueryError,
                  "possibly an empty set returned")
    def test_edgeql_ir_card_inference_149(self):
        """
        select Named { [is schema::Object].name }
        """

    @tb.must_fail(errors.QueryError,
                  "possibly an empty set returned")
    def test_edgeql_ir_card_inference_150(self):
        """
        select User { [is schema::Object].name }
        """

    def test_edgeql_ir_card_inference_151(self):
        # lnk has a *delegated* constraint
        """
        select Tgt { back := .<lnk[is Src] }
% OK %
        back: MANY
        """

    def test_edgeql_ir_card_inference_152(self):
        """
        select Tgt { back := .<lnk[is SrcSub1] }
% OK %
        back: AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_153(self):
        # Constraint is delegated, shouldn't apply here
        """
        select Named filter .name = ''
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_154(self):
        # Constraint is delegated, shouldn't apply here
        """
        select Named2 filter .name = ''
% OK %
        MANY
        """

    def test_edgeql_ir_card_inference_155(self):
        # But should apply to this subtype
        """
        select Named2Sub filter .name = ''
% OK %
        AT_MOST_ONE
        """

    def test_edgeql_ir_card_inference_156(self):
        """
        select global Alice
% OK %
        AT_MOST_ONE
        """
