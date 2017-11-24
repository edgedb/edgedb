##
# Copyright (c) 2012-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import difflib
import os.path
import textwrap

from edgedb.lang import _testbase as tb

from edgedb.lang.edgeql import compiler


class TestEdgeQLIRScopeTree(tb.BaseEdgeQLCompilerTest):
    """Unit tests for scope tree logic."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.eschema')

    def run_test(self, *, source, spec, expected):
        ir = compiler.compile_to_ir(source, self.schema)

        path_scope = textwrap.indent(ir.path_scope.pformat(), '    ')
        expected_scope = textwrap.indent(
            textwrap.dedent(expected).strip(' \n'), '    ')

        if path_scope != expected_scope:
            diff = '\n'.join(difflib.context_diff(
                expected_scope.split('\n'), path_scope.split('\n')))

            self.fail(
                f'Scope tree does not match the expected result.'
                f'\nEXPECTED:\n{expected_scope}\nACTUAL:\n{path_scope}'
                f'\nDIFF:\n{diff}')

    def test_edgeql_ir_scope_tree_01(self):
        """
        WITH MODULE test
        SELECT (Card, Card.id)
% OK %
        "FENCE": {
            "(test::Card)",
            "(test::Card).>(std::id)[IS std::uuid]"
        }
        """

    def test_edgeql_ir_scope_tree_02(self):
        """
        WITH MODULE test
        SELECT Card{name, cost}
% OK %
        "FENCE": {
            "(test::Card)",
            "FENCE": {
                "(test::Card).>(std::id)[IS std::uuid]"
            },
            "FENCE": {
                "(test::Card).>(test::name)[IS std::str]"
            },
            "FENCE": {
                "(test::Card).>(test::cost)[IS std::int]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_03(self):
        """
        WITH MODULE test
        SELECT (
            Card {
                owner := (SELECT Card.<deck)
            },
            Card.<deck
        )

% OK %
        "FENCE": {
            "(test::Card)",
            "FENCE": {
                "(test::Card).>(std::id)[IS std::uuid]"
            },
            "FENCE": {
                "(__expr__::expr~1)",
                "(test::Card).>(test::owner)[IS test::User]"
            },
            "(test::Card).<(test::deck)[IS test::User]"
        }
        """

    def test_edgeql_ir_scope_tree_04(self):
        """
        WITH MODULE test
        SELECT (
            Card.<deck,
            Card {
                owner := (SELECT Card.<deck)
            }
        )

% OK %
        "FENCE": {
            "(test::Card).<(test::deck)[IS test::User]",
            "(test::Card)",
            "FENCE": {
                "(test::Card).>(std::id)[IS std::uuid]"
            },
            "FENCE": {
                "(__expr__::expr~1)",
                "(test::Card).>(test::owner)[IS test::User]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_05(self):
        """
        WITH
            MODULE test,
            U := User
        SELECT (
            Card {
                users := U
            },
            User
        )

% OK %
        "FENCE": {
            "FENCE": {
                "(test::User)"
            },
            "(test::Card)",
            "FENCE": {
                "(test::Card).>(std::id)[IS std::uuid]"
            },
            "FENCE": {
                "(__view__::U)",
                "(__expr__::expr~1)",
                "(test::Card).>(test::users)[IS test::User]"
            },
            "(test::User)"
        }
        """

    def test_edgeql_ir_scope_tree_06(self):
        """
        WITH MODULE test
        SELECT count(User) + count(User)

% OK %
        "FENCE": {
            "FENCE": {
                "(test::User)"
            },
            "FENCE": {
                "(test::User)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_07(self):
        """
        WITH MODULE test
        SELECT User.deck

% OK %
        "FENCE": {
            "(test::User).>(test::deck)[IS test::Card]": {
                "(test::User)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_08(self):
        """
        WITH MODULE test
        SELECT (User.friends, User.friends@nickname)

% OK %
        "FENCE": {
            "(test::User).>(test::friends)[IS test::User]",
            "(test::User).>(test::friends)[IS test::User]\
@(test::nickname)[IS std::str]",
            "(test::User)"
        }
        """

    def test_edgeql_ir_scope_tree_09(self):
        """
        WITH MODULE test
        SELECT User.friends
        FILTER User.name = 'Bob'

% OK %
        "FENCE": {
            "(test::User).>(test::friends)[IS test::User]",
            "FENCE": {
                "(test::User).>(test::name)[IS std::str]"
            },
            "(test::User)"
        }
        """

    def test_edgeql_ir_scope_tree_10(self):
        """
        WITH MODULE test
        SELECT (Card.element ?? <str>Card.cost, count(Card))

% OK %
        "FENCE": {
            "BRANCH": {
                "(test::Card).>(test::element)[IS std::str]",
                "BRANCH": {
                    "FENCE": {
                        "(test::Card).>(test::cost)[IS std::int]"
                    }
                }
            },
            "(test::Card)"
        }
        """

    def test_edgeql_ir_scope_tree_11(self):
        """
        SELECT schema::Node {
            schema::Array.element_type: {
                name
            }
        }

% OK %
        "FENCE": {
            "(schema::Node)",
            "FENCE": {
                "(schema::Node).>(std::id)[IS std::uuid]"
            },
            "FENCE": {
                "(schema::Node).>(__type__::optindirection)\
[IS schema::Array].>(schema::element_type)[IS schema::Node]": {
                    "(schema::Node).>(__type__::optindirection)\
[IS schema::Array]"
                },
                "FENCE": {
                    "(schema::Node).>(__type__::optindirection)\
[IS schema::Array].>(schema::element_type)[IS schema::Node]\
.>(std::id)[IS std::uuid]"
                },
                "FENCE": {
                    "(schema::Node).>(__type__::optindirection)\
[IS schema::Array].>(schema::element_type)[IS schema::Node]\
.>(schema::name)[IS std::str]"
                }
            }
        }
        """
