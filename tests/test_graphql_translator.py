##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import os
import re
import textwrap
import unittest  # NOQA

from edgedb.lang import _testbase as tb
from edgedb.lang.common import markup
from edgedb.lang import graphql as edge_graphql
from edgedb.lang import edgeql as edge_edgeql
from edgedb.lang.graphql.errors import GraphQLValidationError
from edgedb.lang.schema import declarative as s_decl
from edgedb.lang.schema import std as s_std


def with_variables(**kwargs):
    kwargs = {'$' + name: val for name, val in kwargs.items()}

    def wrap(func):
        tb._set_spec(func, 'variables', kwargs)
        return func
    return wrap


class BaseSchemaTestMeta(tb.ParserTestMeta):
    @classmethod
    def __prepare__(mcls, name, bases, **kwargs):
        return collections.OrderedDict()

    def __new__(mcls, name, bases, dct):
        decls = []

        for n, v in dct.items():
            m = re.match(r'^SCHEMA(?:_(\w+))?', n)
            if m:
                module_name = (m.group(1) or 'test').lower().replace(
                    '__', '.')
                schema_text = textwrap.dedent(v)
                decls.append((module_name, schema_text))
        dct['_decls'] = decls

        return super().__new__(mcls, name, bases, dct)


class TranslatorTest(tb.BaseSyntaxTest, metaclass=BaseSchemaTestMeta):
    re_filter = re.compile(r'''[\s,;]+''')
    re_eql_filter = re.compile(r'''[\s'"();,]+|(\#.*?\n)''')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema = s_std.load_std_schema()
        cls.schema = s_std.load_graphql_schema(cls.schema)
        s_decl.parse_module_declarations(cls.schema, cls._decls)

    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get('DEBUG_GRAPHQL'))
        if debug:
            print('\n--- GRAPHQL ---')
            markup.dump_code(textwrap.dedent(source).strip(), lexer='graphql')

        translation = edge_graphql.translate(self.schema, source,
                                             spec.get('variables'))

        if debug:
            print('\n--- EDGEQL ---')
            markup.dump_code(translation, lexer='edgeql')

        self.assert_equal(expected, translation)

        # make sure that resulting EdgeQL is valid and can be parsed
        #
        eqlast = edge_edgeql.parse_block(translation)
        eqlgen = edge_edgeql.generate_source(eqlast)

        self.assert_equal(translation, eqlgen, re_filter=self.re_eql_filter)


class TestGraphQLTranslation(TranslatorTest):
    SCHEMA_TEST = r"""
        abstract concept NamedObject:
            required link name to str

        concept UserGroup extending NamedObject:
            link settings to Setting:
                mapping := '1*'

        concept Setting extending NamedObject:
            required link value to str

        concept Profile extending NamedObject:
            required link value to str

        concept User extending NamedObject:
            required link active to bool
            link groups to UserGroup:
                mapping := '**'
            required link age to int
            required link score to float
            link profile to Profile:
                mapping := '*1'
    """

    SCHEMA_MOD2 = r"""
        import test

        concept Person extending test::User
    """

    SCHEMA_123LIB = r"""
        concept Foo:
            link `select` to str
            link after to str
    """

    def test_graphql_translation_query_01(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT
            (graphql::Query) {
                User := (SELECT
                    (test::User) {
                        name,
                        groups: {
                            id,
                            name
                        }
                    }
                )
            };
        """

    def test_graphql_translation_query_02(self):
        r"""
        query users @edgedb(module: "test") {
            User {
                id,
                name,
                groups {
                    id
                    name
                }
            }
        }

        query settings @edgedb(module: "test") {
            Setting {
                name,
                value,
            }
        }

% OK %

        # query settings
        SELECT (graphql::Query) {
            Setting := (SELECT
                (test::Setting) {
                    name,
                    value
                })
        };

        # query users
        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                    groups: {
                        id,
                        name
                    }
                })
        };

        """

    @tb.must_fail(GraphQLValidationError, "field 'Bogus' is invalid",
                  line=3, col=13)
    def test_graphql_translation_query_03(self):
        r"""
        query @edgedb(module: "test") {
            Bogus {
                name,
                groups {
                    id
                    name
                }
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, "field 'bogus' is invalid for User",
                  line=5, col=17)
    def test_graphql_translation_query_04(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name,
                bogus,
                groups {
                    id
                    name
                }
            }
        }
        """

    @tb.must_fail(GraphQLValidationError,
                  "field 'age' is invalid for NamedObject", line=5, col=17)
    def test_graphql_translation_query_05(self):
        r"""
        query @edgedb(module: "test") {
            NamedObject {
                name,
                age,
                groups {
                    id
                    name
                }
            }
        }
        """

    def test_graphql_translation_query_06(self):
        r"""
        query mixed @edgedb(module: "test") {
            User {
                name
            }
            Setting {
                name,
            }
        }

% OK %

        # query mixed
        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    name
                }),
            Setting := (SELECT
                (test::Setting) {
                    name
                })
        };

        """
    def test_graphql_translation_fragment_01(self):
        r"""
        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                name,
                groups {
                    ... groupFrag
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                })
        };
        """

    def test_graphql_translation_fragment_02(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... userFrag2
        }

        fragment userFrag2 on User @edgedb(module: "test") {
            groups {
                ... groupFrag
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                })
        };
        """

    def test_graphql_translation_fragment_03(self):
        r"""
        fragment userFrag2 on User @edgedb(module: "test") {
            groups {
                ... groupFrag
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... {
                    name
                    ... userFrag2
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                })
        };
        """

    def test_graphql_translation_fragment_04(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... {
                groups {
                    ... groupFrag
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                })
        };
        """

    def test_graphql_translation_directives_01(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @include(if: true),
                groups @include(if: false) {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                })
        };
        """

    def test_graphql_translation_directives_02(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @skip(if: true),
                groups @skip(if: false) {
                    id @skip(if: false)
                    name @skip(if: true)
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    groups: {
                        id,
                    }
                })
        };
        """

    def test_graphql_translation_directives_03(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @skip(if: true), @include(if: true),

                groups @skip(if: false), @include(if: true) {
                    id @skip(if: false), @include(if: true)
                    name @skip(if: true), @include(if: true)
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    groups: {
                        id,
                    }
                })
        };
        """

    def test_graphql_translation_directives_04(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... {
                groups @include(if: false) {
                    ... groupFrag
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                })
        };
        """

    def test_graphql_translation_directives_05(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... @skip(if: true) {
                groups {
                    ... groupFrag
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                })
        };
        """

    def test_graphql_translation_directives_06(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: true)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                    }
                })
        };
        """

    @with_variables(nogroup=False)
    def test_graphql_translation_directives_07(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            name
        }

        query ($nogroup: Boolean = false) @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %
        # critical variables: $nogroup=False

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        name,
                        id,
                    }
                })
        };
        """

    @with_variables(nogroup=True, irrelevant='foo')
    def test_graphql_translation_directives_08(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            name
        }

        query ($nogroup: Boolean = false) @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %
        # critical variables: $nogroup=True

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                    }
                })
        };
        """

    @with_variables(nogroup=True, novalue=False)
    def test_graphql_translation_directives_09(self):
        r"""
        fragment userFrag1 on User @edgedb(module: "test") {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            name
        }

        query users($nogroup: Boolean = false) @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

        query settings($novalue: Boolean = false) @edgedb(module: "test") {
            Setting {
                name
                value @skip(if: $novalue)
            }
        }

% OK %

        # query settings
        # critical variables: $novalue=False

        SELECT (graphql::Query) {
            Setting := (SELECT
                (test::Setting){
                    name,
                    value
                })
        };

        # query users
        # critical variables: $nogroup=True

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    name,
                    groups: {
                        id,
                    }
                })
        };
        """

    @tb.must_fail(GraphQLValidationError, line=4, col=22)
    def test_graphql_translation_directives_10(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @include(if: "true"),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=4, col=22)
    def test_graphql_translation_directives_11(self):
        r"""
        query ($val: String = "true") @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    def test_graphql_translation_arguments_01(self):
        r"""
        query @edgedb(module: "test") {
            User(name: "John") {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            FILTER
                ((test::User).name = 'John'))
        };
        """

    def test_graphql_translation_arguments_02(self):
        r"""
        query @edgedb(module: "test") {
            User(name: "John", active: true) {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            FILTER
                (
                    ((test::User).name = 'John') AND
                    ((test::User).active = True)
                ))
        };
        """

    def test_graphql_translation_arguments_03(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name,
                groups(name: "admin") {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    } FILTER ((test::User).groups.name = 'admin')
                })
        };
        """

    def test_graphql_translation_variables_01(self):
        r"""
        query($name: String) @edgedb(module: "test") {
            User(name: $name) {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            FILTER
                ((test::User).name = $name))
        };
        """

    def test_graphql_translation_variables_03(self):
        r"""
        query($val: Int = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    id,
                }
            FILTER
                ((test::User).score = $val))
        };
        """

    def test_graphql_translation_variables_04(self):
        r"""
        query($val: Boolean = true) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                groups @skip(if: $val) {
                    id
                    name
                }
            }
        }

% OK %

        # critical variables: $val=True

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                })
        };
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_05(self):
        r"""
        query($val: Boolean! = true) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_06(self):
        r"""
        query($val: Boolean!) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    def test_graphql_translation_variables_07(self):
        r"""
        query($val: String = "John") @edgedb(module: "test") {
            User(name: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    id,
                }
            FILTER
                ((test::User).name = $val))
        };
        """

    def test_graphql_translation_variables_08(self):
        r"""
        query($val: Int = 20) @edgedb(module: "test") {
            User(age: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    id,
                }
            FILTER
                ((test::User).age = $val))
        };
        """

    def test_graphql_translation_variables_09(self):
        r"""
        query($val: Float = 3.5) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    id,
                }
            FILTER
                ((test::User).score = $val))
        };
        """

    def test_graphql_translation_variables_10(self):
        r"""
        query($val: Int = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    id,
                }
            FILTER
                ((test::User).score = $val))
        };
        """

    def test_graphql_translation_variables_11(self):
        r"""
        query($val: Float = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    id,
                }
            FILTER
                ((test::User).score = $val))
        };
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_12(self):
        r"""
        query($val: Boolean = 1) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_13(self):
        r"""
        query($val: Boolean = "1") @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_14(self):
        r"""
        query($val: Boolean = 1.3) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_15(self):
        r"""
        query($val: String = 1) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_16(self):
        r"""
        query($val: String = 1.1) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_17(self):
        r"""
        query($val: String = true) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_18(self):
        r"""
        query($val: Int = 1.1) @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_19(self):
        r"""
        query($val: Int = "1") @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_20(self):
        r"""
        query($val: Int = true) @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_21(self):
        r"""
        query($val: Float = "1") @edgedb(module: "test") {
            User(score: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_22(self):
        r"""
        query($val: Float = true) @edgedb(module: "test") {
            User(score: $val) {
                id
            }
        }
        """

    def test_graphql_translation_variables_23(self):
        r"""
        query($val: ID = "1") @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    name,
                }
            FILTER
                ((test::User).id = $val))
        };
        """

    def test_graphql_translation_variables_24(self):
        r"""
        query($val: ID = 1) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    name,
                }
            FILTER
                ((test::User).id = $val))
        };
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_25(self):
        r"""
        query($val: ID = 1.1) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_26(self):
        r"""
        query($val: ID = true) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_27(self):
        r"""
        query($val: [String] = "Foo") @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    @with_variables(val='Foo')
    def test_graphql_translation_variables_28(self):
        r"""
        query($val: [String]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_29(self):
        r"""
        query($val: [String]!) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_30(self):
        r"""
        query($val: String!) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables_31(self):
        r"""
        query($val: [String] = ["Foo", 123]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    @with_variables(val=["Foo", 123])
    def test_graphql_translation_variables_32(self):
        r"""
        query($val: [String]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    def test_graphql_translation_enum_01(self):
        r"""
        query @edgedb(module: "test") {
            # this is an ENUM that gets simply converted to a string in EdgeQL
            UserGroup(name: admin) {
                id,
                name,
            }
        }

% OK %

        SELECT (graphql::Query) {
            UserGroup := (SELECT
                (test::UserGroup){
                    id,
                    name,
                }
            FILTER
                ((test::UserGroup).name = 'admin'))
        };
        """

    def test_graphql_translation_arg_type_01(self):
        r"""
        query @edgedb(module: "test") {
            User(name: "John") {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).name = 'John'))
        };
        """

    def test_graphql_translation_arg_type_02(self):
        r"""
        query @edgedb(module: "test") {
            User(age: 20) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).age = 20))
        };
        """

    def test_graphql_translation_arg_type_03(self):
        r"""
        query @edgedb(module: "test") {
            User(score: 3.5) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).score = 3.5))
        };
        """

    def test_graphql_translation_arg_type_04(self):
        r"""
        query @edgedb(module: "test") {
            User(score: 3) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).score = 3))
        };
        """

    @with_variables(val="John")
    def test_graphql_translation_arg_type_05(self):
        r"""
        query($val: String!) @edgedb(module: "test") {
            User(name: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).name = $val))
        };
        """

    @with_variables(val=20)
    def test_graphql_translation_arg_type_06(self):
        r"""
        query($val: Int!) @edgedb(module: "test") {
            User(age: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).age = $val))
        };
        """

    @with_variables(val=3.5)
    def test_graphql_translation_arg_type_07(self):
        r"""
        query($val: Float!) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).score = $val))
        };
        """

    @with_variables(val=3)
    def test_graphql_translation_arg_type_08(self):
        r"""
        query($val: Int!) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                }
            FILTER
                ((test::User).score = $val))
        };
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type_17(self):
        r"""
        query @edgedb(module: "test") {
            User(name: 42) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type_18(self):
        r"""
        query @edgedb(module: "test") {
            User(age: 20.5) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type_19(self):
        r"""
        query @edgedb(module: "test") {
            User(score: "3.5") {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type_20(self):
        r"""
        query @edgedb(module: "test") {
            User(active: 0) {
                id,
            }
        }
        """

    def test_graphql_translation_fragment_type_01(self):
        r"""
        fragment userFrag on User @edgedb(module: "test") {
            id,
            name,
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_fragment_type_02(self):
        r"""
        fragment namedFrag on NamedObject @edgedb(module: "test") {
            id,
            name,
        }

        query @edgedb(module: "test") {
            User {
                ... namedFrag
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_fragment_type_03(self):
        r"""
        fragment namedFrag on NamedObject @edgedb(module: "test") {
            id,
            name,
        }

        fragment userFrag on User @edgedb(module: "test") {
            ... namedFrag
            age
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                    age,
                })
        };
        """

    @tb.must_fail(GraphQLValidationError,
                  'UserGroup and User are not related', line=9, col=17)
    def test_graphql_translation_fragment_type_04(self):
        r"""
        fragment userFrag on User @edgedb(module: "test") {
            id,
            name,
        }

        query @edgedb(module: "test") {
            UserGroup {
                ... userFrag
            }
        }
        """

    @tb.must_fail(GraphQLValidationError,
                  'UserGroup and User are not related', line=8, col=13)
    def test_graphql_translation_fragment_type_05(self):
        r"""
        fragment userFrag on User @edgedb(module: "test") {
            id,
            name,
        }

        fragment groupFrag on UserGroup @edgedb(module: "test") {
            ... userFrag
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag
                groups {
                    ... groupFrag
                }
            }
        }
        """

    def test_graphql_translation_fragment_type_06(self):
        r"""
        fragment userFrag on User @edgedb(module: "test") {
           name,
           age,
        }

        query @edgedb(module: "test") {
            NamedObject {
                id,
                ... userFrag
            }
        }

% OK %

        SELECT (graphql::Query) {
            NamedObject := (SELECT
                (test::NamedObject){
                    id,
                    (test::User).name,
                    (test::User).age,
                })
        };
        """

    def test_graphql_translation_fragment_type_07(self):
        r"""
        fragment frag on NamedObject @edgedb(module: "test") {
            id,
            name,
        }

        query @edgedb(module: "test") {
            NamedObject {
                ... frag
            }
        }

% OK %

        SELECT (graphql::Query) {
            NamedObject := (SELECT
                (test::NamedObject){
                    id,
                    name,
                })
        };
        """

    @tb.must_fail(GraphQLValidationError, r'field \S+ is invalid for',
                  line=5, col=13)
    def test_graphql_translation_fragment_type_08(self):
        r"""
        fragment frag on NamedObject @edgedb(module: "test") {
            id,
            name,
            age,
        }

        query @edgedb(module: "test") {
            User {
                ... frag
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, r'field \S+ is invalid for',
                  line=7, col=21)
    def test_graphql_translation_fragment_type_09(self):
        r"""
        query @edgedb(module: "test") {
            User {
                ... on NamedObject @edgedb(module: "test") {
                    id,
                    name,
                    age,
                }
            }
        }
        """

    def test_graphql_translation_fragment_type_10(self):
        r"""
        fragment namedFrag on NamedObject @edgedb(module: "test") {
            id,
            name,
            ... userFrag
        }

        fragment userFrag on User @edgedb(module: "test") {
            age
        }

        query @edgedb(module: "test") {
            NamedObject {
                ... namedFrag
            }
        }

% OK %

        SELECT (graphql::Query) {
            NamedObject := (SELECT
                (test::NamedObject){
                    id,
                    name,
                    (test::User).age,
                })
        };
        """

    def test_graphql_translation_fragment_type_11(self):
        r"""
        fragment namedFrag on NamedObject @edgedb(module: "test") {
            id,
            name,
            ... userFrag
        }

        fragment userFrag on User @edgedb(module: "test") {
            age
        }

        query @edgedb(module: "test") {
            User {
                ... namedFrag
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                    (test::User).age,
                })
        };
        """

    def test_graphql_translation_fragment_type_12(self):
        r"""
        query @edgedb(module: "test") {
            NamedObject {
                ... on User {
                    age
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            NamedObject := (SELECT
                (test::NamedObject) {
                    (test::User).age
                })
        };
        """

    def test_graphql_translation_import_01(self):
        r"""
        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "mod2") {
            Person {
                id
                name
                groups {
                    ... groupFrag
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            Person := (SELECT
                (mod2::Person) {
                    id,
                    name,
                    groups: {
                        id,
                        name,
                    }
                })
        };
        """

    @tb.must_fail(GraphQLValidationError, line=8, col=13)
    def test_graphql_translation_import_02(self):
        r"""
        fragment groupFrag on UserGroup @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            Person {
                id
                name
                groups {
                    ... groupFrag
                }
            }
        }
        """

    def test_graphql_translation_duplicates_01(self):
        r"""
        query @edgedb(module: "test") {
            User {
                id
                name
                name
                name
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_duplicates_02(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @include(if: true)
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    name,
                    id,
                })
        };
        """

    def test_graphql_translation_duplicates_03(self):
        r"""
        query @edgedb(module: "test") {
            User {
                ... @skip(if: false) {
                    name @include(if: true)
                }
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    name,
                    id,
                })
        };
        """

    def test_graphql_translation_duplicates_04(self):
        r"""
        fragment f1 on User @edgedb(module: "test") {
            name @include(if: true)
        }

        fragment f2 on User @edgedb(module: "test") {
            id
            name @include(if: true)
            ... f1
        }

        query @edgedb(module: "test") {
            User {
                ... f2
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User){
                    id,
                    name,
                })
        };
        """

    @tb.must_fail(GraphQLValidationError, line=6, col=17)
    def test_graphql_translation_duplicates_05(self):
        r"""
        query @edgedb(module: "test") {
            User {
                id
                name
                name @include(if: true)
                name @skip(if: false)
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=8, col=17)
    def test_graphql_translation_duplicates_06(self):
        r"""
        query @edgedb(module: "test") {
            User {
                ... @skip(if: false) {
                    name @include(if: true)
                }
                id
                name
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=13)
    def test_graphql_translation_duplicates_07(self):
        r"""
        fragment f1 on User @edgedb(module: "test") {
            name @skip(if: false)
        }

        fragment f2 on User @edgedb(module: "test") {
            id
            name @include(if: true)
            ... f1
        }

        query @edgedb(module: "test") {
            User {
                ... f2
                id
                name @include(if: true)
            }
        }
        """

    def test_graphql_translation_quoting_01(self):
        r"""
        query @edgedb(module: "123lib") {
            Foo(select: "bar") {
                select
                after
            }
        }

% OK %

        SELECT (graphql::Query) {
            Foo := (SELECT
                (`123lib`::Foo){
                    `select`,
                    after
                }
            FILTER
                ((`123lib`::Foo).`select` = 'bar'))
        };
        """

    def test_graphql_translation_typename_01(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name
                __typename
                groups {
                    id
                    name
                    __typename
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            User := (SELECT
                (test::User) {
                    name,
                    __typename := (test::User).__class__.name,
                    groups: {
                        id,
                        name,
                        __typename := (test::User).groups.__class__.name
                    }
                })
        };
        """

    def test_graphql_translation_typename_02(self):
        r"""
        query {
            __typename
        }

% OK %

        SELECT (graphql::Query) {
            __typename := 'Query'
        };
        """

    def test_graphql_translation_schema_01(self):
        r"""
        query @edgedb(module: "test") {
            __schema {
                __typename
            }
        }

% OK %

        SELECT (graphql::Query) {
            __schema := (
                SELECT (graphql::Query) {
                    __typename := '__Schema'
                }
            )
        };
        """

    def test_graphql_translation_schema_02(self):
        r"""
        query @edgedb(module: "test") {
            __schema {
                __typename
            }
            __schema {
                __typename
            }
        }

% OK %

        SELECT (graphql::Query) {
            __schema:= (
                SELECT (graphql::Query) {
                    __typename := '__Schema'
                }
            )
        };
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=22)
    def test_graphql_translation_schema_03(self):
        r"""
        query @edgedb(module: "test") {
            __schema(name: "foo") {
                __typename
            }
        }
        """

    def test_graphql_translation_schema_04(self):
        r"""
        query @edgedb(module: "test") {
            __schema {
                directives {
                    name
                    description
                    locations
                    args {
                        name
                        description
                        defaultValue
                    }
                }
            }
        }

% OK %

        SELECT (graphql::Query) {
            __schema := (SELECT (graphql::Query) {
                directives := (SELECT (graphql::Directive) {
                    name,
                    description,
                    locations,
                    args: {
                        name,
                        description,
                        defaultValue
                    }
                })
            })
        };
        """

    def test_graphql_translation_type_01(self):
        r"""
        query @edgedb(module: "test") {
            __type(name: "User") {
                __typename
            }
        }

% OK %

        SELECT (graphql::Query) {
            __type:= (
                SELECT (graphql::Query) {
                    __typename := '__Type'
                }
            )
        };
        """
