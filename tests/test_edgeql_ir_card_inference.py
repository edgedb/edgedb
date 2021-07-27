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
        qltree = qlparser.parse(source)
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
                    card = el.rptr.ptrref.out_cardinality
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
        ELSE (INSERT User {name := "Madz2"})
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
        SELECT Person FILTER .p = 7 AND .q = 3
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

    def test_edgeql_ir_card_inference_72(self):
        """
        SELECT {a := 42}
% OK %
        ONE
        """

    def test_edgeql_ir_card_inference_73(self):
        # Make sure that a union of anonymous shapes still ends up
        # with cardinality AT_LEAST_ONE.
        """
        FOR x IN {0, 1} UNION {a := x}
% OK %
        AT_LEAST_ONE
        """
