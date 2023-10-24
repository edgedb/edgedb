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
import textwrap

from edb import errors
from edb.testbase import lang as tb

from edb.edgeql import compiler
from edb.edgeql import qltypes
from edb.edgeql import parser as qlparser


class TestEdgeQLMultiplicityInference(tb.BaseEdgeQLCompilerTest):
    """Unit tests for multiplicity inference."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_ir_inference.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse_query(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                modaliases={None: 'default'},
            )
        )

        # The expected multiplicity is given for the whole query.
        exp = textwrap.dedent(expected).strip(' \n')
        expected_multiplicity = qltypes.Multiplicity(exp)
        self.assertEqual(ir.multiplicity, expected_multiplicity,
                         'unexpected multiplicity:\n' + source)

    def test_edgeql_ir_mult_inference_00(self):
        """
        SELECT Card
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_01(self):
        """
        SELECT Card.id
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_02(self):
        """
        SELECT User.name
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_03(self):
        # Unconstrained property
        """
        SELECT User.deck_cost
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_04(self):
        """
        SELECT Card FILTER Card.name = 'Djinn'
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_05(self):
        """
        SELECT Card LIMIT 1
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_06(self):
        """
        SELECT 1
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_07(self):
        """
        SELECT {1, 2}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_08(self):
        """
        SELECT {1, 1}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_09(self):
        """
        SELECT User.deck
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_10(self):
        """
        SELECT Card.cost
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_11(self):
        """
        SELECT Card.owners
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_12(self):
        """
        SELECT {Card, User}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_13(self):
        """
        SELECT 1 + 2
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_14a(self):
        """
        SELECT 1 + {2, 3}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_14b(self):
        """
        SELECT 0 * {2, 3}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_15(self):
        """
        SELECT {1, 2} + {2, 3}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_16(self):
        """
        SELECT 'pre_' ++ Card.name
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_17(self):
        """
        SELECT User.name ++ Card.name
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_18(self):
        """
        SELECT (1, {'a', 'b'})
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_19(self):
        """
        SELECT (1, Card.name)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_20(self):
        """
        SELECT [1, {1, 2}]
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_21(self):
        """
        SELECT ['card', Card.name]
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_22(self):
        """
        SELECT User.name ++ Card.name
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_23(self):
        """
        SELECT to_str(1)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_24(self):
        """
        WITH
            C := (SELECT Card FILTER .name = 'Imp')
        SELECT str_split(<str>C.id, '')
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_25(self):
        # Any time a function returns a set for any reason the
        # multiplicity cannot be reliably inferred.
        #
        # We don't know what a set-returning function really does.
        #
        # We also don't know that an element-wise function doesn't end
        # up with collisions.
        """
        SELECT str_split(<str>Card.id, '')
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_26(self):
        # Any time a function returns a set for any reason the
        # multiplicity cannot be reliably inferred.
        #
        # We don't know what a set-returning function really does.
        #
        # We also don't know that an element-wise function doesn't end
        # up with collisions.
        """
        SELECT array_unpack(str_split(<str>Card.id, ''))
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_27(self):
        """
        SELECT count(Card)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_28(self):
        """
        SELECT 1 IN {1, 2, 3}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_29(self):
        """
        SELECT 1 IN {1, 1, 3}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_30(self):
        """
        SELECT {1, 2} IN {1, 2, 3}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_31(self):
        """
        SELECT Card.name IN {'Imp', 'Dragon'}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_32(self):
        """
        SELECT <str>{1, 2, 3}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_33(self):
        """
        SELECT <str>{1, 1, 3}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_34(self):
        """
        SELECT <str>Card.id
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_35(self):
        """
        SELECT <json>User.name
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_36(self):
        """
        SELECT <str>Card.cost
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_37(self):
        """
        SELECT User.deck[IS SpecialCard]
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_38(self):
        """
        SELECT Award.<awards[IS User]
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_39(self):
        """
        SELECT (1, Card.name).0
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_40(self):
        """
        SELECT (1, Card.name).1
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_41(self):
        """
        SELECT ['card', Card.name][0]
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_42(self):
        # It's probably impractical to even try to infer that we're
        # only fetching a unique array element here.
        """
        SELECT ['card', Card.name][1]
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_43(self):
        """
        SELECT DISTINCT Card.element
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_44(self):
        """
        SELECT User {
            friends_of_friends := .friends.friends,
            others := (
                SELECT WaterOrEarthCard.owners
            )
        }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_45(self):
        """
        SELECT Award {
            owner := .<awards[IS User]
        }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_46(self):
        """
        SELECT User {
            card_names := .deck.name,
            card_elements := DISTINCT .deck.element,
            deck: {
                el := User.deck.element[:2]
            }
        }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_47(self):
        """
        SELECT 1 IS str
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_48(self):
        """
        SELECT Award IS Named
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_49(self):
        """
        WITH
            A := (
                SELECT Award FILTER .name = 'Wow'
            )
        SELECT A IS Named
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_50(self):
        """
        SELECT Award.name IS str
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_51(self):
        """
        SELECT INTROSPECT TYPEOF User.deck
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_52(self):
        """
        SELECT (INTROSPECT TYPEOF User.deck).name
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_53(self):
        """
        SELECT User {
            card_elements := .deck.element
        }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_54(self):
        """
        SELECT User {
            foo := {1, 1, 2}
        }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_55a(self):
        """
        FOR x IN {'fire', 'water'}
        UNION (
            SELECT Card
            FILTER .element = x
        )
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_55b(self):
        """
        FOR letter IN {'I', 'B'}
        UNION (
            SELECT Card
            FILTER .name[0] = letter
        )
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_56(self):
        """
        SELECT User {
            wishlist := (
                FOR x IN {'fire', 'water'}
                UNION (
                    SELECT Card
                    FILTER .element = x
                )
            )
        }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_57(self):
        """
        SELECT enumerate({2, 2})
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_58(self):
        """
        SELECT enumerate(Card)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_59(self):
        """
        FOR x IN {enumerate({'fire', 'water'})}
        UNION (
            SELECT Card
            FILTER .element = x.1
        )
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_59a(self):
        """
        FOR x IN {enumerate({'fire', 'water'})}
        UNION (
            SELECT (
                SELECT Card
                FILTER .element = x.1
            )
        )
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_60(self):
        """
        FOR x IN {
            enumerate(
                DISTINCT array_unpack(['fire', 'water']))
        }
        UNION (
            SELECT Card
            FILTER .element = x.1
        )
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_61(self):
        """
        FOR x IN {
            enumerate(
                array_unpack(['A', 'B']))
        }
        UNION (
            INSERT Card {
                name := x.1,
                element := 'test',
                cost := 0,
                req_awards := {}, # wtvr
                req_tags := {}, # wtvr
            }
        )
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_62(self):
        """
        SELECT Card UNION SpecialCard
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_63(self):
        """
        FOR card IN {enumerate(Card)}
        UNION (SELECT card.1)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_64(self):
        """
        FOR card IN {Card}
        UNION card
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_65(self):
        """
        WITH C := <Card>{}
        FOR card IN {C}
        UNION card
% OK %
        EMPTY
        """

    def test_edgeql_ir_mult_inference_66(self):
        """
        FOR card IN {Card, SpecialCard}
        UNION card
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_67(self):
        """
        SELECT
            (SELECT User FILTER .name = "foo")
            ??
            (SELECT User FILTER .name = "bar")
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_68(self):
        """
        SELECT
            (SELECT User FILTER .name = "foo")
            ??
            {
                User,
                User,
            }
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_69(self):
        """
        SELECT
            {
                (INSERT User { name := "a" }),
                (INSERT User { name := "b" }),
            }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_70(self):
        """
        WITH
            X1 := Card {
                z := (.<deck[IS User],)
            }
        SELECT X1 {
            foo := .z.0
        }.foo
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_71(self):
        """
        FOR card IN {assert_distinct(Card UNION SpecialCard)}
        UNION card
% OK %
        UNIQUE
        """

    @tb.must_fail(errors.QueryError,
                  r"possibly not a distinct set.+computed link 'bad_link'",
                  line=3, col=13)
    def test_edgeql_ir_mult_inference_error_01(self):
        """
        SELECT User {
            bad_link := {Card, Card},
            name,
        }
        """

    @tb.must_fail(errors.QueryError,
                  r"possibly not a distinct set.+computed link 'bad_link'",
                  line=5, col=13)
    def test_edgeql_ir_mult_inference_error_02(self):
        """
        WITH
            A := {Card, Card}
        SELECT User {
            bad_link := A,
            name,
        }
        """

    def test_edgeql_ir_mult_inference_72(self):
        """
        SELECT ()
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_73(self):
        """
        SELECT {(), ()}
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_74(self):
        """
        SELECT <array<str>>[]
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_75(self):
        """
        SELECT <str>{}
% OK %
        EMPTY
        """

    def test_edgeql_ir_mult_inference_76(self):
        """
        SELECT (Card, User).1
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_77(self):
        """
        for x in {1, 2} union { foo := 10 }
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_78(self):
        """
        with F := { foo := 10 }
        for x in {1, 2} union F
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_79(self):
        """
        for x in {1, 2, 3} union (with z := x, select z)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_80(self):
        """
        for x in {1,2} union (for y in {3, 4} union x)
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_81(self):
        """
        for x in {1,2} union (for y in {3, 4} union y)
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_82(self):
        """
        select 1 union 1
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_83(self):
        """
        select 1 + (2 intersect 3)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_84(self):
        """
        select 1 + (2 intersect {3, 3})
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_85(self):
        """
        select 1 + ({2, 2} intersect {3, 3})
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_86(self):
        """
        select {2, 2} intersect <int64>{}
% OK %
        EMPTY
        """

    def test_edgeql_ir_mult_inference_87(self):
        """
        select 1 + (2 except 3)
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_88(self):
        """
        select 1 + (2 except {3, 3})
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_89(self):
        """
        select 1 + ({2, 2} except {3, 3})
% OK %
        DUPLICATE
        """

    def test_edgeql_ir_mult_inference_90(self):
        """
        if <bool>$0 then
            (insert User { name := "test" })
        else
            (insert User { name := "???" })
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_91(self):
        """
        if <bool>$0 then
            (insert User { name := "test" })
        else
            {(insert User { name := "???" }), (insert User { name := "!!!" })}
% OK %
        UNIQUE
        """

    def test_edgeql_ir_mult_inference_92(self):
        """
        if <bool>$0 then
            (insert User { name := "test" })
        else
            <User>{}
% OK %
        UNIQUE
        """
