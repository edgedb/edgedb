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
from edb.tools import test


class TestEdgeQLMultiplicityInference(tb.BaseEdgeQLCompilerTest):
    """Unit tests for multiplicity inference."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards_ir_inference.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                validate_multiplicity=True
            )
        )

        # The expected multiplicity is given for the whole query.
        exp = textwrap.dedent(expected).strip(' \n')
        expected_multiplicity = qltypes.Multiplicity(exp)
        self.assertEqual(ir.multiplicity, expected_multiplicity,
                         'unexpected multiplicity:\n' + source)

    def test_edgeql_ir_mult_inference_00(self):
        """
        WITH MODULE test
        SELECT Card
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_01(self):
        """
        WITH MODULE test
        SELECT Card.id
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_02(self):
        """
        WITH MODULE test
        SELECT User.name
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_03(self):
        # Unconstrained property
        """
        WITH MODULE test
        SELECT User.deck_cost
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_04(self):
        """
        WITH MODULE test
        SELECT Card FILTER Card.name = 'Djinn'
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_05(self):
        """
        WITH MODULE test
        SELECT Card LIMIT 1
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_06(self):
        """
        SELECT 1
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_07(self):
        """
        SELECT {1, 2}
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_08(self):
        """
        SELECT {1, 1}
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_09(self):
        """
        WITH MODULE test
        SELECT User.deck
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_10(self):
        """
        WITH MODULE test
        SELECT Card.cost
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_11(self):
        """
        WITH MODULE test
        SELECT Card.owners
% OK %
        ONE
        """

    @test.xfail('''
        Ideally this should be inferred as ONE because Card and User
        sets are non-intersecting (one is not a subset of the
        other).

        Currently, this is not taken into account.
    ''')
    def test_edgeql_ir_mult_inference_12(self):
        """
        WITH MODULE test
        SELECT {Card, User}
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_13(self):
        """
        SELECT 1 + 2
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_14(self):
        """
        SELECT 1 + {2, 3}
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_15(self):
        """
        SELECT {1, 2} + {2, 3}
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_16(self):
        """
        WITH MODULE test
        SELECT 'pre_' ++ Card.name
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_17(self):
        """
        WITH MODULE test
        SELECT User.name ++ Card.name
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_18(self):
        """
        SELECT (1, {'a', 'b'})
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_19(self):
        """
        WITH MODULE test
        SELECT (1, Card.name)
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_20(self):
        """
        SELECT [1, {1, 2}]
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_21(self):
        """
        WITH MODULE test
        SELECT ['card', Card.name]
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_22(self):
        """
        WITH MODULE test
        SELECT User.name ++ Card.name
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_23(self):
        """
        SELECT to_str(1)
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_24(self):
        """
        WITH
            MODULE test,
            C := (SELECT Card FILTER .name = 'Imp')
        SELECT str_split(<str>C.id, '')
% OK %
        ONE
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
        WITH MODULE test
        SELECT str_split(<str>Card.id, '')
% OK %
        MANY
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
        WITH MODULE test
        SELECT array_unpack(str_split(<str>Card.id, ''))
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_27(self):
        """
        WITH MODULE test
        SELECT count(Card)
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_28(self):
        """
        WITH MODULE test
        SELECT 1 IN {1, 2, 3}
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_29(self):
        """
        WITH MODULE test
        SELECT 1 IN {1, 1, 3}
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_30(self):
        """
        WITH MODULE test
        SELECT {1, 2} IN {1, 2, 3}
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_31(self):
        """
        WITH MODULE test
        SELECT Card.name IN {'Imp', 'Dragon'}
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_32(self):
        """
        SELECT <str>{1, 2, 3}
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_33(self):
        """
        SELECT <str>{1, 1, 3}
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_34(self):
        """
        WITH MODULE test
        SELECT <str>Card.id
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_35(self):
        """
        WITH MODULE test
        SELECT <json>User.name
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_36(self):
        """
        WITH MODULE test
        SELECT <str>Card.cost
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_37(self):
        """
        WITH MODULE test
        SELECT User.deck[IS SpecialCard]
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_38(self):
        """
        WITH MODULE test
        SELECT Award.<awards[IS User]
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_39(self):
        """
        WITH MODULE test
        SELECT (1, Card.name).0
% OK %
        MANY
        """

    @test.xfail('''
        Ideally this should be inferred as ONE because that tuple
        element is Card.name and that's unique.
    ''')
    def test_edgeql_ir_mult_inference_40(self):
        """
        WITH MODULE test
        SELECT (1, Card.name).1
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_41(self):
        """
        WITH MODULE test
        SELECT ['card', Card.name][0]
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_42(self):
        # It's probably impractical to even try to infer that we're
        # only fetching a unique array element here.
        """
        WITH MODULE test
        SELECT ['card', Card.name][1]
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_43(self):
        """
        WITH MODULE test
        SELECT DISTINCT Card.element
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_44(self):
        """
        WITH MODULE test
        SELECT User {
            friends_of_friends := .friends.friends,
            others := (
                SELECT WaterOrEarthCard.owners
            )
        }
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_45(self):
        """
        WITH MODULE test
        SELECT Award {
            owner := .<awards[IS User]
        }
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_46(self):
        """
        WITH MODULE test
        SELECT User {
            card_names := .deck.name,
            card_elements := DISTINCT .deck.element,
            deck: {
                el := User.deck.element[:2]
            }
        }
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_47(self):
        """
        SELECT 1 IS str
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_48(self):
        """
        WITH MODULE test
        SELECT Award IS Named
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_49(self):
        """
        WITH
            MODULE test,
            A := (
                SELECT Award FILTER .name = 'Wow'
            )
        SELECT A IS Named
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_50(self):
        """
        WITH MODULE test
        SELECT Award.name IS str
% OK %
        MANY
        """

    def test_edgeql_ir_mult_inference_51(self):
        """
        WITH MODULE test
        SELECT INTROSPECT TYPEOF User.deck
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_52(self):
        """
        WITH MODULE test
        SELECT (INTROSPECT TYPEOF User.deck).name
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_53(self):
        """
        WITH MODULE test
        SELECT User {
            card_elements := .deck.element
        }
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_54(self):
        """
        WITH MODULE test
        SELECT User {
            foo := {1, 1, 2}
        }
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_55(self):
        """
        WITH MODULE test
        FOR x IN {'fire', 'water'}
        UNION (
            SELECT Card
            FILTER .element = x
        )
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_56(self):
        """
        WITH MODULE test
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
        ONE
        """

    def test_edgeql_ir_mult_inference_57(self):
        """
        SELECT enumerate({2, 2})
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_58(self):
        """
        WITH MODULE test
        SELECT enumerate(Card)
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_59(self):
        """
        WITH MODULE test
        FOR x IN {enumerate({'fire', 'water'})}
        UNION (
            SELECT Card
            FILTER .element = x.1
        )
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_60(self):
        """
        WITH MODULE test
        FOR x IN {
            enumerate(
                DISTINCT array_unpack(['fire', 'water']))
        }
        UNION (
            SELECT Card
            FILTER .element = x.1
        )
% OK %
        ONE
        """

    def test_edgeql_ir_mult_inference_61(self):
        """
        WITH MODULE test
        FOR x IN {
            enumerate(
                array_unpack(['A', 'B']))
        }
        UNION (
            INSERT Card {
                name := x.1,
                element := 'test',
                cost := 0,
            }
        )
% OK %
        ONE
        """

    @tb.must_fail(errors.QueryError,
                  r"possibly not a strict set.+computable bad_link",
                  line=4, col=13)
    def test_edgeql_ir_mult_inference_error_01(self):
        """
        WITH MODULE test
        SELECT User {
            bad_link := {Card, Card},
            name,
        }
        """

    @tb.must_fail(errors.QueryError,
                  r"possibly not a strict set.+computable bad_link",
                  line=6, col=13)
    def test_edgeql_ir_mult_inference_error_02(self):
        """
        WITH
            MODULE test,
            A := {Card, Card}
        SELECT User {
            bad_link := A,
            name,
        }
        """
