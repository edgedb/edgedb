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


import difflib
import os.path
import re
import textwrap

from edb import errors

from edb.testbase import lang as tb

from edb.edgeql import compiler
from edb.edgeql import parser as qlparser


class TestEdgeQLIRScopeTree(tb.BaseEdgeQLCompilerTest):
    """Unit tests for scope tree logic."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    UUID_RE = re.compile(
        r'[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}'
        r'-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}'
    )

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse(source)
        ir = compiler.compile_ast_to_ir(qltree, self.schema)

        path_scope = self.UUID_RE.sub(
            '@SID@',
            textwrap.indent(ir.scope_tree.pformat(), '    '),
        )
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
            "(test::Card).>id[IS std::uuid]"
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
                owner := (SELECT Card.<deck[IS User])
            },
            Card.<deck[IS User]
        )

% OK %
        "FENCE": {
            "(test::Card)",
            "(test::Card).<deck[IS __derived__::(@SID@)]\
.>indirection[IS test::User]": {
                "(test::Card).<deck[IS __derived__::(@SID@)]"
            },
            "FENCE": {
                "(test::Card).>owner[IS test::User]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_04(self):
        """
        WITH MODULE test
        SELECT (
            Card.<deck[IS User],
            Card {
                owner := (SELECT Card.<deck[IS User])
            }
        )

% OK %
        "FENCE": {
            "(test::Card)",
            "(test::Card).<deck[IS __derived__::(@SID@)]\
.>indirection[IS test::User]": {
                "(test::Card).<deck[IS __derived__::(@SID@)]"
            },
            "FENCE": {
                "(test::Card).>owner[IS test::User]"
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
                "(__derived__::__derived__|U@@w~1)": {
                    "FENCE": {
                        "(test::User)"
                    }
                },
                "(test::Card).>users[IS test::User]"
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
            "(test::User).>deck[IS test::Card]": {
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
            "(test::User)",
            "(test::User).>friends[IS test::User]",
            "(test::User).>friends[IS test::User]\
@nickname[IS std::str]"
        }
        """

    def test_edgeql_ir_scope_tree_09(self):
        """
        WITH MODULE test
        SELECT (SELECT User FILTER User.name = 'Bob').friends

% OK %
        "FENCE": {
            "(__derived__::expr~5).>friends[IS test::User]": {
                "(__derived__::expr~5)": {
                    "FENCE": {
                        "(test::User)",
                        "FENCE": {
                            "(test::User).>name[IS std::str]"
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
            "(test::Card)",
            "(test::Card).>element[IS std::str] [OPT]",
            "FENCE": {
                "(test::Card).>cost[IS std::int64]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_11(self):
        """
        SELECT schema::Type {
            [IS schema::Array].element_type: {
                name
            }
        }

% OK %
        "FENCE": {
            "(schema::Type)",
            "FENCE": {
                "(schema::Type).>element_type[IS schema::Type]",
                "(schema::Type).>indirection[IS schema::Array]\
.>element_type[IS schema::Type]": {
                    "(schema::Type).>indirection[IS schema::Array]"
                }
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
            "(__derived__::expr~3)",
            "FENCE": {
                "(__derived__::expr~3).>foo[IS std::str]"
            },
            "FENCE": {
                "(schema::Type)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_13(self):
        """
        WITH MODULE test
        SELECT User {
            friends := (User.friends
                        IF EXISTS User.friends
                        ELSE User.deck.<deck[IS User])
        }
        ORDER BY User.name

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(test::User).>friends[IS test::User]",
                "FENCE": {
                    "(test::User).>deck[IS test::Card].\
<deck[IS __derived__::(@SID@)].>indirection[IS test::User]": {
                        "(test::User).>deck[IS test::Card].\
<deck[IS __derived__::(@SID@)]": {
                            "(test::User).>deck[IS test::Card]"
                        }
                    }
                },
                "FENCE": {
                    "(test::User).>friends[IS test::User]"
                },
                "FENCE": {
                    "(test::User).>friends[IS test::User]"
                }
            },
            "FENCE": {
                "(test::User).>name[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_14(self):
        """
        WITH MODULE test
        SELECT Card.owners

% OK %
        "FENCE": {
            "(test::Card).>owners[IS test::User]": {
                "BRANCH": {
                    "(test::Card)"
                },
                "FENCE": {
                    "ns~1@@(test::Card).<deck[IS __derived__::(@SID@)]\
.>indirection[IS test::User]": {
                        "ns~1@@(test::Card).<deck[IS __derived__::(@SID@)]": {
                            "(test::Card)"
                        }
                    }
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
            "(__derived__::expr~10)",
            "(__derived__::expr~5)",
            "FENCE": {
                "(test::Card)",
                "FENCE": {
                    "(test::Card).>element[IS std::str]"
                }
            },
            "FENCE": {
                "(test::Card)",
                "FENCE": {
                    "(test::Card).>element[IS std::str]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_16(self):
        # Apparent misplaced "(__derived__::__derived__|U@@w~1)" in the FILTER
        # fence is due to a alias_map replacement artifact.
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
            "(__derived__::__derived__|U@@w~1).>cards[IS test::Card]\
.>foo[IS std::float64]": {
                "BRANCH": {
                    "(__derived__::__derived__|U@@w~1)\
.>cards[IS test::Card]": {
                        "BRANCH": {
                            "(__derived__::__derived__|U@@w~1)"
                        },
                        "FENCE": {
                            "(test::Card)",
                            "FENCE": {
                                "(__derived__::__derived__|U@@w~1)\
.>deck[IS test::Card]": {
                                    "(__derived__::__derived__|U@@w~1)": {
                                        "FENCE": {
                                            "(test::User)",
                                            "FENCE": {
                                                "(test::User)\
.>name[IS std::str]"
                                            }
                                        }
                                    }
                                }
                            }
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
                    "(test::Card).>name[IS std::str]"
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
                "(__derived__::expr~7).>name[IS std::str]": {
                    "(__derived__::expr~7)": {
                        "FENCE": {
                            "FENCE": {
                                "(test::User).>friends[IS test::User]",
                                "FENCE": {
                                    "(test::User)\
.>friends[IS test::User]@nickname[IS std::str]"
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
            "(__derived__::__derived__|x@@w~1)": {
                "FENCE": {
                    "(test::User).>friends[IS test::User]": {
                        "(test::User)"
                    },
                    "(test::User).>friends[IS test::User]\
.>deck_cost[IS std::int64]": {
                        "FENCE": {
                            "FENCE": {
                                "FENCE": {
                                    "ns~2@@(test::User).>friends\
[IS test::User].>deck[IS test::Card].>cost[IS std::int64]": {
                                        "ns~2@@(test::User).>friends\
[IS test::User].>deck[IS test::Card]"
                                    }
                                }
                            }
                        }
                    },
                    "FENCE": {
                        "(test::User).>friends[IS test::User]\
.>deck[IS test::Card]"
                    }
                }
            },
            "FENCE": {
                "(__derived__::__derived__|x@@w~1)\
.>0[IS std::float64]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_20(self):
        """
        WITH
            MODULE test
        SELECT
            Card.name ++ <str>count(Card.owners)

% OK %
        "FENCE": {
            "(test::Card)",
            "(test::Card).>name[IS std::str]",
            "FENCE": {
                "(test::Card).>owners[IS test::User]": {
                    "FENCE": {
                        "ns~1@@(test::Card).<deck[IS __derived__::(@SID@)]\
.>indirection[IS test::User]": {
                            "ns~1@@(test::Card).<deck[IS __derived__::(@SID@)]"
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_21(self):
        """
        WITH
            MODULE test
        SELECT
            Card.element ++ ' ' ++ (SELECT Card).name

% OK %
        "FENCE": {
            "(__derived__::expr~6).>name[IS std::str]": {
                "(__derived__::expr~6)"
            },
            "(test::Card)",
            "(test::Card).>element[IS std::str]"
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
                "(test::User).>deck[IS test::Card]",
                "(test::User).>deck[IS test::Card]",
                "FENCE": {
                    "(test::User).>deck[IS test::Card]@count[IS std::int64]"
                }
            },
            "FENCE": {
                "(test::User).>deck[IS test::Card]": {
                    "FENCE": {
                        "(test::User).>deck[IS test::Card]",
                        "FENCE": {
                            "(test::User).>deck[IS test::Card]\
@count[IS std::int64]"
                        }
                    },
                    "FENCE": {
                        "(test::User).>deck[IS test::Card]",
                        "FENCE": {
                            "(test::User).>deck[IS test::Card]\
@count[IS std::int64]"
                        }
                    }
                },
                "(test::User).>deck[IS test::Card].>cost[IS std::int64]",
                "(test::User).>deck[IS test::Card]@count[IS std::int64]"
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
                "(__derived__::__derived__|x@@w~1)": {
                    "FENCE": {
                        "(test::User).>deck[IS test::Card]"
                    }
                },
                "(test::User).>deck[IS test::Card]",
                "FENCE": {
                    "(__derived__::__derived__|x@@w~1)\
.>name[IS std::str]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_24(self):
        """
        WITH
            MODULE test,
            A := {1, 2}
        SELECT _ := (User{name, a := A}, A)

% OK %
        "FENCE": {
            "(__derived__::__derived__|_@@w~2)": {
                "FENCE": {
                    "(__derived__::__derived__|A@@w~1)",
                    "(test::User)"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_25(self):
        """
        WITH MODULE test
        SELECT User {
            select_deck := (
                FOR letter IN {'I', 'B'}
                UNION (
                    SELECT User.deck {
                        name,
                        @letter := letter
                    }
                    FILTER User.deck.name[0] = letter
                )
            )
        } FILTER .name = 'Alice'

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(__derived__::__derived__|letter@@w~1)",
                "(test::User).>select_deck[IS test::Card]",
                "FENCE": {
                    "(test::User).>deck[IS test::Card]",
                    "FENCE": {
                        "(test::User).>deck[IS test::Card].>name[IS std::str]"
                    }
                }
            },
            "FENCE": {
                "(test::User).>name[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_26(self):
        """
        WITH MODULE test
        SELECT User {
            select_deck := (
                FOR letter IN {'I', 'B'}
                UNION foo := (
                    SELECT User.deck {
                        name,
                        @letter := letter
                    }
                    FILTER User.deck.name[0] = letter
                )
            )
        } FILTER .name = 'Alice'

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(__derived__::__derived__|foo@@w~2)": {
                    "FENCE": {
                        "(test::User).>deck[IS test::Card]",
                        "FENCE": {
                            "(test::User).>deck[IS test::Card]\
.>name[IS std::str]"
                        }
                    }
                },
                "(__derived__::__derived__|letter@@w~1)",
                "(test::User).>select_deck[IS test::Card]"
            },
            "FENCE": {
                "(test::User).>name[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_27(self):
        """
        WITH MODULE test
        INSERT User {
            name := 'Carol',
            deck := (
                SELECT Card {@count := 5 - Card.cost} FILTER .element != 'Fire'
            )
        }

% OK %
        "FENCE": {
            "(test::User)",
            "FENCE": {
                "(test::Card)",
                "(test::User).>deck[IS test::Card]",
                "FENCE": {
                    "(test::Card).>element[IS std::str]"
                },
                "FENCE": {
                    "(test::User).>deck[IS test::Card]\
.>cost[IS std::int64]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_28(self):
        """
        WITH MODULE test
        SELECT <str>count((WITH A := Card SELECT A.owners)) ++ Card.name

% OK %
        "FENCE": {
            "(test::Card)",
            "(test::Card).>name[IS std::str]",
            "FENCE": {
                "(__derived__::__derived__|A@@w~1).>owners[IS test::User]": {
                    "BRANCH": {
                        "(__derived__::__derived__|A@@w~1)"
                    },
                    "FENCE": {
                        "ns~2@@(__derived__::__derived__|A@@w~1)\
.<deck[IS __derived__::(@SID@)].>indirection[IS test::User]": {
                            "ns~2@@(__derived__::__derived__|A@@w~1)\
.<deck[IS __derived__::(@SID@)]": {
                                "(__derived__::__derived__|A@@w~1)"
                            }
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_29(self):
        """
        WITH
            MODULE test
        SELECT Card {
            name,
            alice := (SELECT User FILTER User.name = 'Alice')
        } FILTER Card.alice != User AND Card.name = 'Bog monster'

% OK %
        "FENCE": {
            "(test::Card)",
            "FENCE": {
                "(test::Card).>alice[IS test::User]",
                "(test::User)",
                "FENCE": {
                    "(test::User).>name[IS std::str]"
                }
            },
            "FENCE": {
                "(test::Card).>alice[IS test::User]": {
                    "FENCE": {
                        "(test::User)",
                        "FENCE": {
                            "(test::User).>name[IS std::str]"
                        }
                    }
                },
                "(test::Card).>name[IS std::str]",
                "(test::User)"
            }
        }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User.name' changes the interpretation",
                  line=4, col=9)
    def test_edgeql_ir_scope_tree_bad_01(self):
        """
        WITH MODULE test
        SELECT User.deck
        FILTER User.name
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User' changes the interpretation",
                  line=4, col=9)
    def test_edgeql_ir_scope_tree_bad_02(self):
        """
        WITH MODULE test
        SELECT User.deck
        FILTER User.deck@count
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User' changes the interpretation",
                  line=3, col=35)
    def test_edgeql_ir_scope_tree_bad_03(self):
        """
        WITH MODULE test
        SELECT User.deck { foo := User }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User.name' changes the interpretation",
                  line=3, col=40)
    def test_edgeql_ir_scope_tree_bad_04(self):
        """
        WITH MODULE test
        UPDATE User.deck SET { name := User.name }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'U.r' changes the interpretation",
                  line=7, col=58)
    def test_edgeql_ir_scope_tree_bad_05(self):
        """
        WITH
            MODULE test,
            U := User {id, r := random()}
        SELECT
            (
                users := array_agg((SELECT U.id ORDER BY U.r LIMIT 10))
            )
        """
