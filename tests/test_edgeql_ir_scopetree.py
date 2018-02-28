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

        path_scope = textwrap.indent(ir.expr.path_scope.pformat(), '    ')
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
            "(test::Card)"
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
            "(test::Card).<(test::deck)[IS test::User]",
            "FENCE": {
                "(test::Card).>(test::owner)[IS test::User]"
            }
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
            "(test::Card)",
            "(test::User)",
            "FENCE": {
                "(_::__view__|U@@w~1)": {
                    "(test::User)"
                },
                "(test::Card).>(test::users)[IS test::User]"
            }
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
        SELECT (SELECT User FILTER User.name = 'Bob').friends

% OK %
        "FENCE": {
            "(__expr__::expr~3).>(test::friends)[IS test::User]": {
                "(__expr__::expr~3)": {
                    "FENCE": {
                        "(test::User)",
                        "FENCE": {
                            "(test::User).>(test::name)[IS std::str]"
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_10(self):
        """
        WITH MODULE test
        SELECT (Card.element ?? <str>Card.cost, count(Card))

% OK %
        "FENCE": {
            "(test::Card).>(test::element)[IS std::str] [OPT]",
            "FENCE": {
                "(test::Card).>(test::cost)[IS std::int]"
            },
            "(test::Card)"
        }
        """

    def test_edgeql_ir_scope_tree_11(self):
        """
        SELECT schema::Type {
            schema::Array.element_type: {
                name
            }
        }

% OK %
        "FENCE": {
            "(schema::Type)",
            "FENCE": {
                "(schema::Type).>(schema::element_type)[IS schema::Type]"
            },
            "FENCE": {
                "(schema::Type).>(__type__::indirection)[IS schema::Array]\
.>(schema::element_type)[IS schema::Type]": {
                    "(schema::Type).>(__type__::indirection)[IS schema::Array]"
                },
                "(schema::Type).>(schema::element_type)[IS schema::Type]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_12(self):
        """
        SELECT (
            SELECT
                schema::Type {
                    foo := "!"
                }
        ) { foo }

% OK %
        "FENCE": {
            "FENCE": {
                "(schema::Type)"
            },
            "(__expr__::expr~3)",
            "FENCE": {
                "(__expr__::expr~3).>(schema::foo)[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_13(self):
        """
        WITH MODULE test
        SELECT User {
            friends := (User.friends.name
                        IF EXISTS User.friends
                        ELSE User.deck.name)
        }
        ORDER BY User.name

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "FENCE": {
                    "(test::User).>(test::friends)[IS test::User]"
                },
                "FENCE": {
                    "(test::User).>(test::friends)[IS test::User]\
.>(test::name)[IS std::str]": {
                        "(test::User).>(test::friends)[IS test::User]"
                    }
                },
                "FENCE": {
                    "(test::User).>(test::deck)[IS test::Card]\
.>(test::name)[IS std::str]": {
                        "(test::User).>(test::deck)[IS test::Card]"
                    }
                }
            },
            "FENCE": {
                "(test::User).>(test::name)[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_14(self):
        """
        WITH MODULE test
        SELECT Card.owners

% OK %
        "FENCE": {
            "(test::Card).>(test::owners)[IS test::User]": {
                "(test::Card)",
                "FENCE": {
                    "(test::Card).<(test::deck)[IS test::User]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_15(self):
        """
        WITH MODULE test
        SELECT (
            (SELECT Card FILTER Card.element = 'Air'),
            (SELECT Card FILTER Card.element = 'Earth')
        )

% OK %
        "FENCE": {
            "FENCE": {
                "(test::Card)",
                "FENCE": {
                    "(test::Card).>(test::element)[IS std::str]"
                }
            },
            "FENCE": {
                "(test::Card)",
                "FENCE": {
                    "(test::Card).>(test::element)[IS std::str]"
                }
            },
            "(__expr__::expr~3)",
            "(__expr__::expr~6)"
        }
        """

    def test_edgeql_ir_scope_tree_16(self):
        """
        WITH MODULE test,
            U := (
                SELECT User {
                    cards := (
                        SELECT Card {
                            foo := 1 + random()
                        } FILTER Card = User.deck
                    )
                } FILTER .name = 'Dave'
            )
        SELECT
            U.cards.foo

% OK %
        "FENCE": {
            "(_::__view__|U@@w~1).>(test::cards)[IS test::Card]\
.>(test::foo)[IS std::float]": {
                "(_::__view__|U@@w~1).>(test::cards)[IS test::Card]": {
                    "(_::__view__|U@@w~1)": {
                        "(test::User)",
                        "FENCE": {
                            "(test::User).>(test::name)[IS std::str]"
                        }
                    },
                    "FENCE": {
                        "(test::Card)",
                        "FENCE": {
                            "(_::__view__|U@@w~1).>(test::deck)[IS test::Card]"
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_17(self):
        """
        WITH MODULE test
        SELECT
            Card
        ORDER BY
            (SELECT Card.name)

% OK %
        "FENCE": {
            "(test::Card)",
            "FENCE": {
                "FENCE": {
                    "(test::Card).>(test::name)[IS std::str]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_18(self):
        """
        WITH MODULE test
        SELECT User
        ORDER BY (
            (SELECT User.friends
             FILTER User.friends@nickname = 'Firefighter'
             LIMIT 1).name
        )

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(__expr__::expr~5).>(test::name)[IS std::str]": {
                    "(__expr__::expr~5)": {
                        "FENCE": {
                            "FENCE": {
                                "(test::User).>(test::friends)[IS test::User]",
                                "FENCE": {
                                    "(test::User)\
.>(test::friends)[IS test::User]@(test::nickname)[IS std::str]"
                                }
                            }
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_19(self):
        """
        WITH MODULE test
        SELECT
            x := (
                User.friends.deck_cost / count(User.friends.deck),
            )
        ORDER BY
            x.0

% OK %
        "FENCE": {
            "(_::__view__|x@@w~1)": {
                "(test::User).>(test::friends)[IS test::User]\
.>(test::deck_cost)[IS std::int]": {
                    "FENCE": {
                        "FENCE": {
                            "(test::User).>(test::friends)\
[IS test::User].>(test::deck)[IS test::Card].>(test::cost)[IS std::int]": {
                                "(test::User).>(test::friends)\
[IS test::User].>(test::deck)[IS test::Card]"
                            }
                        }
                    }
                },
                "FENCE": {
                    "(test::User).>(test::friends)[IS test::User]\
.>(test::deck)[IS test::Card]"
                },
                "(test::User).>(test::friends)[IS test::User]": {
                    "(test::User)",
                    "(test::User)"
                }
            },
            "FENCE": {
                "(_::__view__|x@@w~1).>(__tuple__::0)[IS std::float]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_20(self):
        """
        WITH
            MODULE test
        SELECT
            Card.name + <str>count(Card.owners)

% OK %
        "FENCE": {
            "(test::Card).>(test::name)[IS std::str]",
            "FENCE": {
                "(test::Card).>(test::owners)[IS test::User]": {
                    "FENCE": {
                        "(test::Card).<(test::deck)[IS test::User]"
                    }
                }
            },
            "(test::Card)"
        }
        """

    def test_edgeql_ir_scope_tree_21(self):
        """
        WITH
            MODULE test
        SELECT
            Card.element + ' ' + (SELECT Card).name

% OK %
        "FENCE": {
            "(test::Card).>(test::element)[IS std::str]",
            "(__expr__::expr~3).>(test::name)[IS std::str]": {
                "(__expr__::expr~3)"
            },
            "(test::Card)"
        }
        """

    def test_edgeql_ir_scope_tree_22(self):
        """
        WITH MODULE test
        SELECT User {
            name,
            deck: {
                name
            } ORDER BY @count
        } FILTER .deck.cost = .deck@count

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(test::User).>(test::deck)[IS test::Card]"
            },
            "FENCE": {
                "(test::User).>(test::deck)[IS test::Card]",
                "FENCE": {
                    "(test::User).>(test::deck)[IS test::Card]\
@(test::count)[IS std::int]"
                }
            },
            "FENCE": {
                "(test::User).>(test::deck)[IS test::Card]\
.>(test::cost)[IS std::int]",
                "(test::User).>(test::deck)[IS test::Card]\
@(test::count)[IS std::int]",
                "(test::User).>(test::deck)[IS test::Card]": {
                    "FENCE": {
                        "(test::User).>(test::deck)[IS test::Card]"
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_23(self):
        """
        WITH MODULE test
        SELECT User {
            name,
            deck := (SELECT x := User.deck ORDER BY x.name)
        }

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(_::__view__|x@@w~2)",
                "FENCE": {
                    "(_::__view__|x@@w~2).>(test::name)[IS std::str]"
                },
                "(test::User).>(test::deck)[IS test::Card]"
            }
        }
        """
