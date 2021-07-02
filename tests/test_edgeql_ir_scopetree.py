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
import textwrap

from edb import errors

from edb.testbase import lang as tb

from edb.edgeql import compiler
from edb.edgeql import parser as qlparser


class TestEdgeQLIRScopeTree(tb.BaseEdgeQLCompilerTest):
    """Unit tests for scope tree logic."""

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'cards.esdl')

    def run_test(self, *, source, spec, expected):
        qltree = qlparser.parse(source)
        ir = compiler.compile_ast_to_ir(
            qltree,
            self.schema,
            options=compiler.CompilerOptions(
                apply_query_rewrites=False,
                modaliases={None: 'default'},
            )
        )

        root = ir.scope_tree
        if len(root.children) != 1:
            self.fail(
                f'Scope tree root is expected to have only one child, got'
                f' {len(root.children)}'
                f' \n{root.pformat()}'
            )

        scope_tree = next(iter(root.children))
        path_scope = textwrap.indent(
            scope_tree.pformat(), '    ')
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
        SELECT (Card, Card.id)
% OK %
        "FENCE": {
            "(default::Card)",
            "(default::Card).>id[IS std::uuid]"
        }
        """

    def test_edgeql_ir_scope_tree_02(self):
        """
        SELECT Card{name, cost}
% OK %
        "FENCE": {
            "(default::Card)"
        }
        """

    def test_edgeql_ir_scope_tree_03(self):
        """
        SELECT (
            Card {
                owner := (SELECT Card.<deck[IS User])
            },
            Card.<deck[IS User]
        )

% OK %
        "FENCE": {
            "(default::Card)",
            "(default::Card).<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                "(default::Card).<deck[IS __derived__::(opaque: default:User)]"
            },
            "FENCE": {
                "(default::Card).>owner[IS default::User]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_04(self):
        """
        SELECT (
            Card.<deck[IS User],
            Card {
                owner := (SELECT Card.<deck[IS User])
            }
        )

% OK %
        "FENCE": {
            "(default::Card).<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                "(default::Card).<deck[IS __derived__::(opaque: default:User)]"
            },
            "(default::Card)",
            "FENCE": {
                "(default::Card).>owner[IS default::User]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_05(self):
        """
        WITH
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
                "FENCE": {
                    "ns~1@@(default::User)"
                }
            },
            "(default::Card)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "tmpns~1@@(__derived__::__derived__|U@w~1)"
                        }
                    }
                }
            },
            "(default::User)",
            "FENCE": {
                "ns~2@tmpns~1@@(__derived__::__derived__|U@w~1)",
                "(default::Card).>users[IS default::User]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_06(self):
        """
        SELECT count(User) + count(User)

% OK %
        "FENCE": {
            "FENCE": {
                "(default::User)"
            },
            "FENCE": {
                "(default::User)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_07(self):
        """
        SELECT User.deck

% OK %
        "FENCE": {
            "(default::User).>deck[IS default::Card]": {
                "(default::User)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_08(self):
        """
        SELECT (User.friends, User.friends@nickname)

% OK %
        "FENCE": {
            "(default::User).>friends[IS default::User]",
            "(default::User)",
            "(default::User).>friends[IS default::User]@nickname[IS std::str]"
        }
        """

    def test_edgeql_ir_scope_tree_09(self):
        """
        SELECT (SELECT User FILTER User.name = 'Bob').friends

% OK %
        "FENCE": {
            "FENCE": {
                "FENCE": {
                    "(default::User)",
                    "FENCE": {
                        "(default::User).>name[IS std::str]"
                    }
                }
            },
            "(__derived__::expr~6).>friends[IS default::User]": {
                "(__derived__::expr~6)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_10(self):
        """
        SELECT (Card.element ?? <str>Card.cost, count(Card))

% OK %
        "FENCE": {
            "FENCE": {
                "(default::Card).>cost[IS std::int64]"
            },
            "(default::Card) [OPT]",
            "(default::Card).>element[IS std::str] [OPT]"
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
                "FENCE": {
                    "(schema::Type).>element_type[IS schema::Type]"
                }
            },
            "FENCE": {
                "(schema::Type).>indirection[IS schema::Array]\
.>element_type[IS schema::Type]": {
                    "(schema::Type).>indirection[IS schema::Array]"
                },
                "(schema::Type).>element_type[IS schema::Type]"
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
            "(__derived__::expr~3)",
            "FENCE": {
                "(__derived__::expr~3).>foo[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_13(self):
        """
        SELECT User {
            friends := (User.friends
                        IF EXISTS User.friends
                        ELSE User.deck.<deck[IS User])
        }
        ORDER BY User.name

% OK %
        "FENCE": {
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "FENCE": {
                                "(default::User).>friends[IS default::User]"
                            },
                            "FENCE": {
                                "(default::User).>deck[IS default::Card]\
.<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                                    "(default::User).>deck[IS default::Card]\
.<deck[IS __derived__::(opaque: default:User)]": {
                                        "(default::User)\
.>deck[IS default::Card]"
                                    }
                                }
                            },
                            "FENCE": {
                                "(default::User).>friends[IS default::User]"
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "FENCE": {
                    "(default::User).>friends[IS default::User]"
                },
                "FENCE": {
                    "(default::User).>deck[IS default::Card]\
.<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                        "(default::User).>deck[IS default::Card]\
.<deck[IS __derived__::(opaque: default:User)]": {
                            "(default::User).>deck[IS default::Card]"
                        }
                    }
                },
                "FENCE": {
                    "(default::User).>friends[IS default::User]"
                },
                "(default::User).>friends[IS default::User]",
                "ns~1@tmpns~1@@(__derived__::expr~13)"
            },
            "FENCE": {
                "(default::User).>name[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_14(self):
        """
        SELECT Card.owners

% OK %
        "FENCE": {
            "(default::Card).>owners[IS default::User]": {
                "CBRANCH": {
                    "(default::Card)"
                },
                "FENCE": {
                    "ns~1@@(default::Card)\
.<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                        "(default::Card)\
.<deck[IS __derived__::(opaque: default:User)]": {
                            "(default::Card)"
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_15(self):
        """
        SELECT (
            (SELECT Card FILTER Card.element = 'Air'),
            (SELECT Card FILTER Card.element = 'Earth')
        )

% OK %
        "FENCE": {
            "FENCE": {
                "(default::Card)",
                "FENCE": {
                    "(default::Card).>element[IS std::str]"
                }
            },
            "FENCE": {
                "(default::Card)",
                "FENCE": {
                    "(default::Card).>element[IS std::str]"
                }
            },
            "(__derived__::expr~5)",
            "(__derived__::expr~10)"
        }
        """

    def test_edgeql_ir_scope_tree_16(self):
        # Apparent misplaced "(__derived__::__derived__|U@w~1)" in the FILTER
        # fence is due to a alias_map replacement artifact.
        """
        WITH U := (
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
            "FENCE": {
                "FENCE": {
                    "ns~1@@(default::User)",
                    "FENCE": {
                        "FENCE": {
                            "FENCE": {
                                "FENCE": {
                                    "ns~1@@(default::Card)",
                                    "FENCE": {
                                        "ns~1@@(default::User)\
.>deck[IS default::Card]"
                                    }
                                }
                            }
                        }
                    },
                    "FENCE": {
                        "ns~1@@(default::User).>name[IS std::str]"
                    },
                    "(__derived__::__derived__|U@w~1)",
                    "FENCE": {
                        "ns~1@ns~4@@(default::Card)",
                        "FENCE": {
                            "ns~1@@(default::User).>deck[IS default::Card]"
                        },
                        "ns~1@@(default::User).>cards[IS default::Card]"
                    }
                }
            },
            "FENCE": {
                "(__derived__::__derived__|U@w~1).>cards[IS default::Card]\
.>foo[IS std::float64]": {
                    "CBRANCH": {
                        "(__derived__::__derived__|U@w~1)\
.>cards[IS default::Card]": {
                            "CBRANCH": {
                                "(__derived__::__derived__|U@w~1)"
                            },
                            "FENCE": {
                                "ns~2@@(default::Card)",
                                "FENCE": {
                                    "(__derived__::__derived__|U@w~1)\
.>deck[IS default::Card]": {
                                        "(__derived__::__derived__|U@w~1)"
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
        SELECT
            Card
        ORDER BY
            (SELECT Card.name)

% OK %
        "FENCE": {
            "(default::Card)",
            "FENCE": {
                "FENCE": {
                    "(default::Card).>name[IS std::str]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_18(self):
        """
        SELECT User
        ORDER BY (
            (SELECT User.friends
             FILTER User.friends@nickname = 'Firefighter'
             LIMIT 1).name
        )

% OK %
        "FENCE": {
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "(default::User).>friends[IS default::User]",
                            "FENCE": {
                                "(default::User)\
.>friends[IS default::User]@nickname[IS std::str]"
                            }
                        }
                    }
                },
                "(__derived__::expr~8).>name[IS std::str]": {
                    "(__derived__::expr~8)"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_19(self):
        """
        SELECT
            x := (
                User.friends.deck_cost / count(User.friends.deck),
            )
        ORDER BY
            x.0

% OK %
        "FENCE": {
            "FENCE": {
                "FENCE": {
                    "ns~1@@(default::User).>friends[IS default::User]": {
                        "ns~1@@(default::User)"
                    },
                    "ns~1@@(default::User).>friends[IS default::User]\
.>deck_cost[IS std::int64]": {
                        "FENCE": {
                            "FENCE": {
                                "FENCE": {
                                    "ns~1@@(default::User)\
.>friends[IS default::User].>deck[IS default::Card].>cost[IS std::int64]": {
                                        "ns~1@@(default::User)\
.>friends[IS default::User].>deck[IS default::Card]"
                                    }
                                }
                            }
                        }
                    },
                    "FENCE": {
                        "ns~1@@(default::User).>friends[IS default::User]\
.>deck[IS default::Card]"
                    }
                }
            },
            "(__derived__::__derived__|x@w~1)",
            "FENCE": {
                "(__derived__::__derived__|x@w~1).>0[IS std::float64]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_20(self):
        """
        SELECT
            Card.name ++ <str>count(Card.owners)

% OK %
        "FENCE": {
            "(default::Card)",
            "(default::Card).>name[IS std::str]",
            "FENCE": {
                "(default::Card).>owners[IS default::User]": {
                    "FENCE": {
                        "(default::Card)\
.<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                            "(default::Card)\
.<deck[IS __derived__::(opaque: default:User)]"
                        }
                    }
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_21(self):
        """
        SELECT
            Card.element ++ ' ' ++ (SELECT Card).name

% OK %
        "FENCE": {
            "(default::Card)",
            "(default::Card).>element[IS std::str]",
            "(__derived__::expr~7).>name[IS std::str]": {
                "(__derived__::expr~7)"
            }
        }
        """

    def test_edgeql_ir_scope_tree_22(self):
        """
        SELECT User {
            name,
            deck: {
                name
            } ORDER BY @count
        } FILTER .deck.cost = .deck@count

% OK %
        "FENCE": {
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "(default::User).>deck[IS default::Card]",
                            "FENCE": {
                                "(default::User)\
.>deck[IS default::Card]@count[IS std::int64]"
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "(default::User).>deck[IS default::Card]",
                "FENCE": {
                    "(default::User)\
.>deck[IS default::Card]@count[IS std::int64]"
                },
                "(default::User).>deck[IS default::Card]"
            },
            "FENCE": {
                "(default::User).>deck[IS default::Card]": {
                    "FENCE": {
                        "(default::User).>deck[IS default::Card]",
                        "FENCE": {
                            "(default::User)\
.>deck[IS default::Card]@count[IS std::int64]"
                        }
                    },
                    "FENCE": {
                        "(default::User).>deck[IS default::Card]",
                        "FENCE": {
                            "(default::User)\
.>deck[IS default::Card]@count[IS std::int64]"
                        }
                    }
                },
                "(default::User).>deck[IS default::Card].>cost[IS std::int64]",
                "(default::User).>deck[IS default::Card]@count[IS std::int64]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_23(self):
        """
        SELECT User {
            name,
            deck := (SELECT x := User.deck ORDER BY x.name)
        }

% OK %
        "FENCE": {
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "FENCE": {
                                "FENCE": {
                                    "(default::User).>deck[IS default::Card]"
                                }
                            },
                            "tmpns~1@@(__derived__::__derived__|x@w~1)",
                            "FENCE": {
                                "tmpns~1@@(__derived__::__derived__|x@w~1)\
.>name[IS std::str]"
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "(default::User).>deck[IS default::Card]"
                    }
                },
                "ns~2@tmpns~1@@(__derived__::__derived__|x@w~2)",
                "FENCE": {
                    "ns~2@tmpns~1@@(__derived__::__derived__|x@w~2)\
.>name[IS std::str]"
                },
                "(default::User).>deck[IS default::Card]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_24(self):
        """
        WITH
            A := {1, 2}
        SELECT _ := (User{name, a := A}, A)

% OK %
        "FENCE": {
            "FENCE": {
                "FENCE": {
                    "ns~2@@(default::User)",
                    "ns~2@@(__derived__::__derived__|A@w~1)"
                }
            },
            "(__derived__::__derived__|_@w~2)"
        }
        """

    def test_edgeql_ir_scope_tree_25(self):
        """
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
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "tmpns~1@@(__derived__::__derived__|letter@w~1)",
                            "FENCE": {
                                "FENCE": {
                                    "(default::User).>deck[IS default::Card]",
                                    "FENCE": {
                                        "(default::User)\
.>deck[IS default::Card].>name[IS std::str]"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "ns~2@tmpns~1@@(__derived__::__derived__|letter@w~1)",
                "FENCE": {
                    "FENCE": {
                        "(default::User).>deck[IS default::Card]",
                        "FENCE": {
                            "(default::User).>deck[IS default::Card]\
.>name[IS std::str]"
                        }
                    }
                },
                "(default::User).>select_deck[IS default::Card]",
                "ns~2@tmpns~1@@(__derived__::expr~26)"
            },
            "FENCE": {
                "(default::User).>name[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_26(self):
        """
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
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "tmpns~1@@(__derived__::__derived__|letter@w~1)",
                            "FENCE": {
                                "FENCE": {
                                    "FENCE": {
                                        "(default::User)\
.>deck[IS default::Card]",
                                        "FENCE": {
                                            "(default::User)\
.>deck[IS default::Card].>name[IS std::str]"
                                        }
                                    }
                                },
                                "tmpns~1@@(__derived__::__derived__|foo@w~2)"
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "ns~3@tmpns~1@@(__derived__::__derived__|letter@w~1)",
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "(default::User).>deck[IS default::Card]",
                            "FENCE": {
                                "(default::User).>deck[IS default::Card]\
.>name[IS std::str]"
                            }
                        }
                    }
                },
                "(default::User).>select_deck[IS default::Card]",
                "ns~3@tmpns~1@@(__derived__::__derived__|foo@w~2)"
            },
            "FENCE": {
                "(default::User).>name[IS std::str]"
            }
        }
        """

    def test_edgeql_ir_scope_tree_27(self):
        """
        INSERT User {
            name := 'Carol',
            deck := (
                SELECT Card {@count := 5 - Card.cost} FILTER .element != 'Fire'
            )
        }

% OK %
        "FENCE": {
            "(default::User)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "tmpns~1@@(default::Card)",
                            "FENCE": {
                                "FENCE": {
                                    "FENCE": {
                                        "FENCE": {
                                            "tmpns~1@@(default::Card)\
.>cost[IS std::int64]"
                                        }
                                    }
                                }
                            },
                            "FENCE": {
                                "tmpns~1@@(default::Card)\
.>element[IS std::str]"
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "ns~3@tmpns~1@@(default::Card)",
                "FENCE": {
                    "ns~3@tmpns~1@@(default::Card).>element[IS std::str]"
                },
                "(default::User).>deck[IS default::Card]",
                "FENCE": {
                    "ns~3@tmpns~1@@(default::Card).>cost[IS std::int64]"
                }
            }
        }
        """

    def test_edgeql_ir_scope_tree_28(self):
        """
        SELECT <str>count((WITH A := Card SELECT A.owners)) ++ Card.name

% OK %
        "FENCE": {
            "FENCE": {
                "FENCE": {
                    "(__derived__::__derived__|A@w~1)\
.>owners[IS default::User]": {
                        "CBRANCH": {
                            "(__derived__::__derived__|A@w~1)"
                        },
                        "FENCE": {
                            "ns~2@@(__derived__::__derived__|A@w~1)\
.<deck[IS __derived__::(opaque: default:User)]\
.>indirection[IS default::User]": {
                                "(__derived__::__derived__|A@w~1)\
.<deck[IS __derived__::(opaque: default:User)]": {
                                    "(__derived__::__derived__|A@w~1)"
                                }
                            }
                        }
                    }
                }
            },
            "(default::Card)",
            "(default::Card).>name[IS std::str]"
        }
        """

    def test_edgeql_ir_scope_tree_29(self):
        """
        SELECT Card {
            name,
            alice := (SELECT User FILTER User.name = 'Alice')
        } FILTER Card.alice != User AND Card.name = 'Bog monster'

% OK %
        "FENCE": {
            "(default::Card)",
            "FENCE": {
                "FENCE": {
                    "FENCE": {
                        "FENCE": {
                            "tmpns~1@@(default::User)",
                            "FENCE": {
                                "tmpns~1@@(default::User).>name[IS std::str]"
                            }
                        }
                    }
                }
            },
            "FENCE": {
                "ns~1@tmpns~1@@(default::User)",
                "FENCE": {
                    "ns~1@tmpns~1@@(default::User).>name[IS std::str]"
                },
                "(default::Card).>alice[IS default::User]"
            },
            "FENCE": {
                "(default::Card).>alice[IS default::User]": {
                    "FENCE": {
                        "ns~2@tmpns~1@@(default::User)",
                        "FENCE": {
                            "ns~2@tmpns~1@@(default::User).>name[IS std::str]"
                        }
                    }
                },
                "(default::User)",
                "(default::Card).>name[IS std::str]"
            }
        }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User.name' changes the interpretation",
                  line=3, col=9)
    def test_edgeql_ir_scope_tree_bad_01(self):
        """
        SELECT User.deck
        FILTER User.name
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User' changes the interpretation",
                  line=3, col=9)
    def test_edgeql_ir_scope_tree_bad_02(self):
        """
        SELECT User.deck
        FILTER User.deck@count
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User' changes the interpretation",
                  line=2, col=35)
    def test_edgeql_ir_scope_tree_bad_03(self):
        """
        SELECT User.deck { foo := User }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'User.name' changes the interpretation",
                  line=2, col=40)
    def test_edgeql_ir_scope_tree_bad_04(self):
        """
        UPDATE User.deck SET { name := User.name }
        """

    @tb.must_fail(errors.QueryError,
                  "reference to 'U.r' changes the interpretation",
                  line=6, col=58)
    def test_edgeql_ir_scope_tree_bad_05(self):
        """
        WITH
            U := User {id, r := random()}
        SELECT
            (
                users := array_agg((SELECT U.id ORDER BY U.r LIMIT 10))
            )
        """
