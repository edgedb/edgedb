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


import collections
import os
import re
import textwrap
import unittest  # NOQA

from edb.lang import _testbase as tb
from edb.lang.common import markup
from edb.lang import graphql as edge_graphql
from edb.lang import edgeql as edge_edgeql
from edb.lang.graphql.errors import GraphQLCoreError
from edb.lang.schema import declarative as s_decl
from edb.lang.schema import std as s_std


def with_variables(**kwargs):
    kwargs = {'$' + name: val for name, val in kwargs.items()}

    def wrap(func):
        tb._set_spec(func, 'variables', kwargs)
        return func
    return wrap


def with_operation(name):
    def wrap(func):
        tb._set_spec(func, 'operation_name', name)
        return func
    return wrap


def translate_only(func):
    tb._set_spec(func, 'translate_only', True)
    return func


class BaseSchemaTestMeta(tb.DocTestMeta):
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
    re_eql_filter = re.compile(r'''(\#.*?\n)''')

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.schema = s_std.load_std_schema()
        cls.schema = s_std.load_graphql_schema(cls.schema)
        cls.schema = s_std.load_default_schema(cls.schema)
        s_decl.parse_module_declarations(cls.schema, cls._decls)

    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get('DEBUG_GRAPHQL'))
        if debug:
            print('\n--- GRAPHQL ---')
            markup.dump_code(textwrap.dedent(source).strip(), lexer='graphql')

        translation = edge_graphql.translate(
            self.schema, source,
            variables=spec.get('variables'),
            operation_name=spec.get('operation_name'),
        )

        if debug:
            print('\n--- EDGEQL ---')
            markup.dump_code(translation, lexer='edgeql')

        # some tests don't compare the output
        if not spec.get('translate_only'):
            self.assert_equal(expected, translation)

        # make sure that resulting EdgeQL is valid and can be parsed
        eqlast = edge_edgeql.parse_block(translation)
        eqlgen = edge_edgeql.generate_source(eqlast)

        self.assert_equal(translation, eqlgen, re_filter=self.re_eql_filter)


class TestGraphQLTranslation(TranslatorTest):
    SCHEMA_TEST = r"""
        abstract type NamedObject:
            required property name -> str

        type UserGroup extending NamedObject:
            link settings -> Setting:
                cardinality := '1*'

        type Setting extending NamedObject:
            required property value -> str

        type Profile extending NamedObject:
            required property value -> str
            property tags -> array<str>
            property odd -> array<int64>:
                cardinality := '1*'

        type User extending NamedObject:
            required property active -> bool
            link groups -> UserGroup:
                cardinality := '**'
            required property age -> int64
            required property score -> float64
            link profile -> Profile:
                cardinality := '*1'

        type Person extending test::User

        type Foo:
            property `select` -> str
            property after -> str
    """

    def test_graphql_translation_query_01(self):
        r"""
        query {
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
            graphql::Query {
                User := (SELECT
                    test::User {
                        name,
                        groups: {
                            id,
                            name
                        }
                    }
                )
            };
        """

    @with_operation('users')
    def test_graphql_translation_query_02(self):
        r"""
        query users {
            User {
                id,
                name,
                groups {
                    id
                    name
                }
            }
        }

        query settings {
            Setting {
                name,
                value,
            }
        }

% OK %

        # query users
        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                    groups: {
                        id,
                        name
                    }
                })
        };
        """

    @with_operation('settings')
    def test_graphql_translation_query_03(self):
        r"""
        query users {
            User {
                id,
                name,
                groups {
                    id
                    name
                }
            }
        }

        query settings {
            Setting {
                name,
                value,
            }
        }

% OK %

        # query settings
        SELECT graphql::Query {
            Setting := (SELECT
                test::Setting {
                    name,
                    value
                })
        };
        """

    @tb.must_fail(GraphQLCoreError,
                  'Cannot query field "Bogus" on type "Query"',
                  line=3, col=13)
    def test_graphql_translation_query_04(self):
        r"""
        query {
            Bogus {
                name,
                groups {
                    id
                    name
                }
            }
        }
        """

    @tb.must_fail(GraphQLCoreError,
                  'Cannot query field "bogus" on type "User"',
                  line=5, col=17)
    def test_graphql_translation_query_05(self):
        r"""
        query {
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

    @tb.must_fail(GraphQLCoreError,
                  'Cannot query field "age" on type "NamedObject"',
                  line=5, col=17)
    def test_graphql_translation_query_06(self):
        r"""
        query {
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

    def test_graphql_translation_query_07(self):
        r"""
        query mixed {
            User {
                name
            }
            Setting {
                name,
            }
        }

% OK %

        # query mixed
        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name
                }),
            Setting := (SELECT
                test::Setting {
                    name
                })
        };

        """

    def test_graphql_translation_query_08(self):
        r"""
        query {
            foo: User {
                name,
                groups {
                    id
                    name
                }
            }
            bar: User {
                spam: name,
                ham: groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            foo := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            ),
            bar := (SELECT
                test::User {
                    spam := (SELECT
                        test::User.name
                    ),
                    ham := (SELECT
                        test::User.groups {
                            id,
                            name
                        }
                    )
                }
            )
        };
        """

    def test_graphql_translation_fragment_01(self):
        r"""
        fragment groupFrag on UserGroup {
            id
            name
        }

        query {
            User {
                name,
                groups {
                    ... groupFrag
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
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
        fragment userFrag1 on User {
            name
            ... userFrag2
        }

        fragment userFrag2 on User {
            groups {
                ... groupFrag
            }
        }

        fragment groupFrag on UserGroup {
            id
            name
        }

        query {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
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
        fragment userFrag2 on User {
            groups {
                ... groupFrag
            }
        }

        fragment groupFrag on UserGroup {
            id
            name
        }

        query {
            User {
                ... on User {
                    name
                    ... userFrag2
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
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
        fragment userFrag1 on User {
            name
            ... {
                groups {
                    ... groupFrag
                }
            }
        }

        fragment groupFrag on UserGroup {
            id
            name
        }

        query {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
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
        query {
            User {
                name @include(if: true),
                groups @include(if: false) {
                    id
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                })
        };
        """

    def test_graphql_translation_directives_02(self):
        r"""
        query {
            User {
                name @skip(if: true),
                groups @skip(if: false) {
                    id @skip(if: false)
                    name @skip(if: true)
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    groups: {
                        id,
                    }
                })
        };
        """

    def test_graphql_translation_directives_03(self):
        r"""
        query {
            User {
                name @skip(if: true), @include(if: true),

                groups @skip(if: false), @include(if: true) {
                    id @skip(if: false), @include(if: true)
                    name @skip(if: true), @include(if: true)
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    groups: {
                        id,
                    }
                })
        };
        """

    def test_graphql_translation_directives_04(self):
        r"""
        fragment userFrag1 on User {
            name
            ... {
                groups @include(if: false) {
                    ... groupFrag
                }
            }
        }

        fragment groupFrag on UserGroup {
            id
            name
        }

        query {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                })
        };
        """

    def test_graphql_translation_directives_05(self):
        r"""
        fragment userFrag1 on User {
            name
            ... @skip(if: true) {
                groups {
                    ... groupFrag
                }
            }
        }

        fragment groupFrag on UserGroup {
            id
            name
        }

        query {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                })
        };
        """

    def test_graphql_translation_directives_06(self):
        r"""
        fragment userFrag1 on User {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: true)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup {
            name
        }

        query {
            User {
                ... userFrag1
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
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
        fragment userFrag1 on User {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup {
            name
        }

        query ($nogroup: Boolean = false) {
            User {
                ... userFrag1
            }
        }

% OK %
        # critical variables: $nogroup=False

        SELECT graphql::Query {
            User := (SELECT
                test::User {
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
        fragment userFrag1 on User {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup {
            name
        }

        query ($nogroup: Boolean = false) {
            User {
                ... userFrag1
            }
        }

% OK %
        # critical variables: $nogroup=True

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                    }
                })
        };
        """

    @with_operation('settings')
    @with_variables(nogroup=True, novalue=False)
    def test_graphql_translation_directives_09(self):
        r"""
        fragment userFrag1 on User {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup {
            name
        }

        query users($nogroup: Boolean = false) {
            User {
                ... userFrag1
            }
        }

        query settings($novalue: Boolean = false) {
            Setting {
                name
                value @skip(if: $novalue)
            }
        }

% OK %

        # query settings
        # critical variables: $novalue=False

        SELECT graphql::Query {
            Setting := (SELECT
                test::Setting {
                    name,
                    value
                })
        };
        """

    @with_operation('users')
    @with_variables(nogroup=True, novalue=False)
    def test_graphql_translation_directives_10(self):
        r"""
        fragment userFrag1 on User {
            name
            ... {
                groups {
                    ... groupFrag @skip(if: $nogroup)
                    id
                }
            }
        }

        fragment groupFrag on UserGroup {
            name
        }

        query users($nogroup: Boolean = false) {
            User {
                ... userFrag1
            }
        }

        query settings($novalue: Boolean = false) {
            Setting {
                name
                value @skip(if: $novalue)
            }
        }

% OK %

        # query users
        # critical variables: $nogroup=True

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                    }
                })
        };
        """

    @tb.must_fail(GraphQLCoreError, 'invalid value "true"', line=4, col=35)
    def test_graphql_translation_directives_11(self):
        r"""
        query {
            User {
                name @include(if: "true"),
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, '', line=2, col=16)
    def test_graphql_translation_directives_12(self):
        r"""
        query ($val: String = "true") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    def test_graphql_translation_arguments_01(self):
        r"""
        query {
            User(name: "John") {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            FILTER
                (test::User.name = 'John'))
        };
        """

    def test_graphql_translation_arguments_02(self):
        r"""
        query {
            User(name: "John", active: true) {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            FILTER
                (
                    (test::User.name = 'John') AND
                    (test::User.active = True)
                ))
        };
        """

    def test_graphql_translation_arguments_03(self):
        r"""
        query {
            User {
                name,
                groups(name: "admin") {
                    id
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                        name
                    } FILTER (test::User.groups.name = 'admin')
                })
        };
        """

    def test_graphql_translation_arguments_04(self):
        r"""
        query {
            User(id: "8598d268-4efa-11e8-9955-8f9c15d57680") {
                name,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name
                } FILTER (
                    <str>test::User.id =
                        '8598d268-4efa-11e8-9955-8f9c15d57680'
                )
            )
        };
        """

    def test_graphql_translation_variables_01(self):
        r"""
        query($name: String) {
            User(name: $name) {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            FILTER
                (test::User.name = $name))
        };
        """

    @unittest.expectedFailure
    def test_graphql_translation_variables_03(self):
        r"""
        query($val: Int = 3) {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = $val))
        };
        """

    def test_graphql_translation_variables_04(self):
        r"""
        query($val: Boolean = true) {
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

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                })
        };
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=32)
    def test_graphql_translation_variables_05(self):
        r"""
        query($val: Boolean! = true) {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=15)
    def test_graphql_translation_variables_06(self):
        r"""
        query($val: Boolean!) {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    def test_graphql_translation_variables_07(self):
        r"""
        query($val: String = "John") {
            User(name: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.name = $val))
        };
        """

    def test_graphql_translation_variables_08(self):
        r"""
        query($val: Int = 20) {
            User(age: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.age = $val))
        };
        """

    def test_graphql_translation_variables_09(self):
        r"""
        query($val: Float = 3.5) {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = $val))
        };
        """

    def test_graphql_translation_variables_11(self):
        r"""
        query($val: Float = 3) {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = $val))
        };
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=31)
    def test_graphql_translation_variables_12(self):
        r"""
        query($val: Boolean = 1) {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=31)
    def test_graphql_translation_variables_13(self):
        r"""
        query($val: Boolean = "1") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=31)
    def test_graphql_translation_variables_14(self):
        r"""
        query($val: Boolean = 1.3) {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=30)
    def test_graphql_translation_variables_15(self):
        r"""
        query($val: String = 1) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=30)
    def test_graphql_translation_variables_16(self):
        r"""
        query($val: String = 1.1) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=30)
    def test_graphql_translation_variables_17(self):
        r"""
        query($val: String = true) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=27)
    def test_graphql_translation_variables_18(self):
        r"""
        query($val: Int = 1.1) {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=27)
    def test_graphql_translation_variables_19(self):
        r"""
        query($val: Int = "1") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=27)
    def test_graphql_translation_variables_20(self):
        r"""
        query($val: Int = true) {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=29)
    def test_graphql_translation_variables_21(self):
        r"""
        query($val: Float = "1") {
            User(score: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=29)
    def test_graphql_translation_variables_22(self):
        r"""
        query($val: Float = true) {
            User(score: $val) {
                id
            }
        }
        """

    def test_graphql_translation_variables_23(self):
        r"""
        query($val: ID = "1") {
            User(id: $val) {
                name
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                }
            FILTER
                (<str>test::User.id = $val))
        };
        """

    def test_graphql_translation_variables_24(self):
        r"""
        query($val: ID = 1) {
            User(id: $val) {
                name
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                }
            FILTER
                (<str>test::User.id = $val))
        };
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=26)
    def test_graphql_translation_variables_25(self):
        r"""
        query($val: ID = 1.1) {
            User(id: $val) {
                name
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=26)
    def test_graphql_translation_variables_26(self):
        r"""
        query($val: ID = true) {
            User(id: $val) {
                name
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=15)
    def test_graphql_translation_variables_27(self):
        r"""
        query($val: [String] = "Foo") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=15)
    @with_variables(val='Foo')
    def test_graphql_translation_variables_28(self):
        r"""
        query($val: [String]) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=15)
    def test_graphql_translation_variables_29(self):
        r"""
        query($val: [String]!) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=15)
    def test_graphql_translation_variables_30(self):
        r"""
        query($val: String!) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=32)
    def test_graphql_translation_variables_31(self):
        r"""
        query($val: [String] = ["Foo", 123]) {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=2, col=15)
    @with_variables(val=["Foo", 123])
    def test_graphql_translation_variables_32(self):
        r"""
        query($val: [String]) {
            User(name: $val) {
                id
            }
        }
        """

    @unittest.expectedFailure
    def test_graphql_translation_enum_01(self):
        r"""
        query {
            # this is an ENUM that gets simply converted to a string in EdgeQL
            UserGroup(name: admin) {
                id,
                name,
            }
        }

% OK %

        SELECT graphql::Query {
            UserGroup := (SELECT
                test::UserGroup {
                    id,
                    name,
                }
            FILTER
                (test::UserGroup.name = 'admin'))
        };
        """

    def test_graphql_translation_arg_type_01(self):
        r"""
        query {
            User(name: "John") {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.name = 'John'))
        };
        """

    def test_graphql_translation_arg_type_02(self):
        r"""
        query {
            User(age: 20) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.age = 20))
        };
        """

    def test_graphql_translation_arg_type_03(self):
        r"""
        query {
            User(score: 3.5) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = 3.5))
        };
        """

    def test_graphql_translation_arg_type_04(self):
        r"""
        query {
            User(score: 3) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = 3))
        };
        """

    @with_variables(val="John")
    def test_graphql_translation_arg_type_05(self):
        r"""
        query($val: String!) {
            User(name: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.name = $val))
        };
        """

    @with_variables(val=20)
    def test_graphql_translation_arg_type_06(self):
        r"""
        query($val: Int!) {
            User(age: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.age = $val))
        };
        """

    @with_variables(val=3.5)
    def test_graphql_translation_arg_type_07(self):
        r"""
        query($val: Float!) {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = $val))
        };
        """

    @unittest.expectedFailure
    @with_variables(val=3)
    def test_graphql_translation_arg_type_08(self):
        r"""
        query($val: Int!) {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                }
            FILTER
                (test::User.score = $val))
        };
        """

    @tb.must_fail(GraphQLCoreError, line=3, col=24)
    def test_graphql_translation_arg_type_17(self):
        r"""
        query {
            User(name: 42) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=3, col=23)
    def test_graphql_translation_arg_type_18(self):
        r"""
        query {
            User(age: 20.5) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=3, col=25)
    def test_graphql_translation_arg_type_19(self):
        r"""
        query {
            User(score: "3.5") {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLCoreError, line=3, col=26)
    def test_graphql_translation_arg_type_20(self):
        r"""
        query {
            User(active: 0) {
                id,
            }
        }
        """

    def test_graphql_translation_fragment_type_01(self):
        r"""
        fragment userFrag on User {
            id,
            name,
        }

        query {
            User {
                ... userFrag
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_fragment_type_02(self):
        r"""
        fragment namedFrag on NamedObject {
            id,
            name,
        }

        query {
            User {
                ... namedFrag
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_fragment_type_03(self):
        r"""
        fragment namedFrag on NamedObject {
            id,
            name,
        }

        fragment userFrag on User {
            ... namedFrag
            age
        }

        query {
            User {
                ... userFrag
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                    age,
                })
        };
        """

    @tb.must_fail(
        GraphQLCoreError,
        r'userFrag cannot be spread.*?UserGroup can never be of type User',
        line=9, col=17)
    def test_graphql_translation_fragment_type_04(self):
        r"""
        fragment userFrag on User {
            id,
            name,
        }

        query {
            UserGroup {
                ... userFrag
            }
        }
        """

    @tb.must_fail(
        GraphQLCoreError,
        r'userFrag cannot be spread.*?UserGroup can never be of type User',
        line=8, col=13)
    def test_graphql_translation_fragment_type_05(self):
        r"""
        fragment userFrag on User {
            id,
            name,
        }

        fragment groupFrag on UserGroup {
            ... userFrag
        }

        query {
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
        fragment userFrag on User {
           name,
           age,
        }

        query {
            NamedObject {
                id,
                ... userFrag
            }
        }

% OK %

        SELECT graphql::Query {
            NamedObject := (SELECT
                test::NamedObject {
                    id,
                    [IS test::User].name,
                    [IS test::User].age,
                })
        };
        """

    def test_graphql_translation_fragment_type_07(self):
        r"""
        fragment frag on NamedObject {
            id,
            name,
        }

        query {
            NamedObject {
                ... frag
            }
        }

% OK %

        SELECT graphql::Query {
            NamedObject := (SELECT
                test::NamedObject {
                    id,
                    name,
                })
        };
        """

    @tb.must_fail(GraphQLCoreError,
                  'Cannot query field "age" on type "NamedObject"',
                  line=5, col=13)
    def test_graphql_translation_fragment_type_08(self):
        r"""
        fragment frag on NamedObject {
            id,
            name,
            age,
        }

        query {
            User {
                ... frag
            }
        }
        """

    @tb.must_fail(GraphQLCoreError,
                  'Cannot query field "age" on type "NamedObject"',
                  line=7, col=21)
    def test_graphql_translation_fragment_type_09(self):
        r"""
        query {
            User {
                ... on NamedObject {
                    id,
                    name,
                    age,
                }
            }
        }
        """

    def test_graphql_translation_fragment_type_10(self):
        r"""
        fragment namedFrag on NamedObject {
            id,
            name,
            ... userFrag
        }

        fragment userFrag on User {
            age
        }

        query {
            NamedObject {
                ... namedFrag
            }
        }

% OK %

        SELECT graphql::Query {
            NamedObject := (SELECT
                test::NamedObject {
                    id,
                    name,
                    [IS test::User].age,
                })
        };
        """

    def test_graphql_translation_fragment_type_11(self):
        r"""
        fragment namedFrag on NamedObject {
            id,
            name,
            ... userFrag
        }

        fragment userFrag on User {
            age
        }

        query {
            User {
                ... namedFrag
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                    [IS test::User].age,
                })
        };
        """

    def test_graphql_translation_fragment_type_12(self):
        r"""
        query {
            NamedObject {
                ... on User {
                    age
                }
            }
        }

% OK %

        SELECT graphql::Query {
            NamedObject := (SELECT
                test::NamedObject {
                    [IS test::User].age
                })
        };
        """

    def test_graphql_translation_duplicates_01(self):
        r"""
        query {
            User {
                id
                name
                name
                name
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_duplicates_02(self):
        r"""
        query {
            User {
                name @include(if: true)
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    id,
                })
        };
        """

    def test_graphql_translation_duplicates_03(self):
        r"""
        query {
            User {
                ... on User @skip(if: false) {
                    name @include(if: true)
                }
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    id,
                })
        };
        """

    def test_graphql_translation_duplicates_04(self):
        r"""
        fragment f1 on User {
            name @include(if: true)
        }

        fragment f2 on User {
            id
            name @include(if: true)
            ... f1
        }

        query {
            User {
                ... f2
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_duplicates_05(self):
        r"""
        query {
            User {
                id
                name
                name @include(if: true)
                name @skip(if: false)
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                })
        };
        """

    # graphql parser has an issue here
    @unittest.expectedFailure
    def test_graphql_translation_duplicates_06(self):
        r"""
        query {
            User {
                ... @skip(if: false) {
                    name @include(if: true)
                }
                id
                name
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    id,
                })
        };
        """

    def test_graphql_translation_duplicates_07(self):
        r"""
        fragment f1 on User {
            name @skip(if: false)
        }

        fragment f2 on User {
            id
            name @include(if: true)
            ... f1
        }

        query {
            User {
                ... f2
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    id,
                    name,
                })
        };
        """

    def test_graphql_translation_quoting_01(self):
        r"""
        query {
            Foo(select: "bar") {
                select
                after
            }
        }

% OK %

        SELECT graphql::Query {
            Foo := (SELECT
                test::Foo {
                    `select`,
                    after
                }
            FILTER
                (test::Foo.`select` = 'bar'))
        };
        """

    def test_graphql_translation_typename_01(self):
        r"""
        query {
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

        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    __typename := graphql::short_name(
                        test::User.__type__.name),
                    groups: {
                        id,
                        name,
                        __typename := graphql::short_name(
                            test::User.groups.__type__.name)
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

        SELECT graphql::Query {
            __typename
        };
        """

    def test_graphql_translation_typename_03(self):
        r"""
        query {
            foo: __typename
            User {
                name
                bar: __typename
            }
        }

% OK %

        SELECT graphql::Query {
            foo := graphql::short_name(std::str.__type__.name),
            User := (SELECT
                test::User {
                    name,
                    bar := graphql::short_name(test::User.__type__.name)
                }
            )
        };
        """

    def test_graphql_translation_schema_01(self):
        r"""
        query {
            __schema {
                __typename
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "__typename": "__Schema"
            }'
        };
        """

    def test_graphql_translation_schema_02(self):
        r"""
        query {
            __schema {
                __typename
            }
            __schema {
                __typename
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "__typename": "__Schema"
            }'
        };
        """

    @tb.must_fail(GraphQLCoreError, line=3, col=22)
    def test_graphql_translation_schema_03(self):
        r"""
        query {
            __schema(name: "foo") {
                __typename
            }
        }
        """

    def test_graphql_translation_schema_04(self):
        r"""
        query {
            __schema {
                directives {
                    name
                    description
                    locations
                    args {
                        name
                        description
                        type {
                            kind
                            name
                            ofType {
                                kind
                                name
                            }
                        }
                    }
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "directives": [
                    {
                        "name": "include",
                        "description":
                            "Directs the executor to include this
                            field or fragment only when the `if`
                            argument is true.",
                        "locations": [
                            "FIELD",
                            "FRAGMENT_SPREAD",
                            "INLINE_FRAGMENT"
                        ],
                        "args": [
                            {
                                "name": "if",
                                "description": "Included when true.",
                                "type": {
                                    "kind": "NON_NULL",
                                    "name": null,
                                    "ofType": {
                                        "kind": "SCALAR",
                                        "name": "Boolean"
                                    }
                                }
                            }
                        ]
                    },
                    {
                        "name": "skip",
                        "description":
                            "Directs the executor to skip this field
                            or fragment when the `if` argument is
                            true.",
                        "locations": [
                            "FIELD",
                            "FRAGMENT_SPREAD",
                            "INLINE_FRAGMENT"
                        ],
                        "args": [
                            {
                                "name": "if",
                                "description": "Skipped when true.",
                                "type": {
                                    "kind": "NON_NULL",
                                    "name": null,
                                    "ofType": {
                                        "kind": "SCALAR",
                                        "name": "Boolean"
                                    }
                                }
                            }
                        ]
                    },
                    {
                        "name": "deprecated",
                        "description":
                            "Marks an element of a GraphQL schema as
                            no longer supported.",
                        "locations": [
                            "FIELD_DEFINITION",
                            "ENUM_VALUE"
                        ],
                        "args": [
                            {
                                "name": "reason",
                                "description":

                                    "Explains why this element was
                                    deprecated, usually also including
                                    a suggestion for how toaccess
                                    supported similar data. Formatted
                                    in [Markdown](https://daringfireba
                                    ll.net/projects/markdown/).",

                                "type": {
                                    "kind": "SCALAR",
                                    "name": "String",
                                    "ofType": null
                                }
                            }
                        ]
                    }
                ]
            }'
        };
        """

    def test_graphql_translation_schema_05(self):
        r"""
        query {
            __schema {
                mutationType {
                    name
                    description
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "mutationType": null
            }'
        };
        """

    def test_graphql_translation_schema_06(self):
        r"""
        query {
            __schema {
                queryType {
                    kind
                    name
                    description
                    interfaces {
                        name
                    }
                    possibleTypes {
                        name
                    }
                    enumValues {
                        name
                    }
                    inputFields {
                        name
                    }
                    ofType {
                        name
                    }
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "queryType": {
                    "kind": "OBJECT",
                    "name": "Query",
                    "description": null,
                    "interfaces": [],
                    "possibleTypes": null,
                    "enumValues": null,
                    "inputFields": null,
                    "ofType": null
                }
            }'
        };
        """

    def test_graphql_translation_schema_07(self):
        r"""
        query {
            __schema {
                types {
                    kind
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "types": [
                    {"kind": "OBJECT", "name": "Query"},
                    {"kind": "SCALAR", "name": "String"},
                    {"kind": "SCALAR", "name": "ID"},
                    {"kind": "INTERFACE", "name": "Foo"},
                    {"kind": "INTERFACE", "name": "NamedObject"},
                    {"kind": "INTERFACE", "name": "Object"},
                    {"kind": "SCALAR", "name": "Boolean"},
                    {"kind": "SCALAR", "name": "Int"},
                    {"kind": "SCALAR", "name": "Float"},
                    {"kind": "INTERFACE", "name": "Person"},
                    {"kind": "INTERFACE", "name": "UserGroup"},
                    {"kind": "INTERFACE", "name": "Setting"},
                    {"kind": "INTERFACE", "name": "Profile"},
                    {"kind": "INTERFACE", "name": "User"},
                    {"kind": "OBJECT", "name": "__Schema"},
                    {"kind": "OBJECT", "name": "__Type"},
                    {"kind": "ENUM", "name": "__TypeKind"},
                    {"kind": "OBJECT", "name": "__Field"},
                    {"kind": "OBJECT", "name": "__InputValue"},
                    {"kind": "OBJECT", "name": "__EnumValue"},
                    {"kind": "OBJECT", "name": "__Directive"},
                    {"kind": "ENUM", "name": "__DirectiveLocation"},
                    {"kind": "OBJECT", "name": "UserGroupType"},
                    {"kind": "OBJECT", "name": "SettingType"},
                    {"kind": "OBJECT", "name": "ProfileType"},
                    {"kind": "OBJECT", "name": "UserType"},
                    {"kind": "OBJECT", "name": "PersonType"},
                    {"kind": "OBJECT", "name": "FooType"}
                ]
            }'
        };
        """

    def test_graphql_translation_schema_08(self):
        r"""
        query {
            Foo : __schema {
                types {
                    kind
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            Foo := <json>'{
                "types": [
                    {"kind": "OBJECT", "name": "Query"},
                    {"kind": "SCALAR", "name": "String"},
                    {"kind": "SCALAR", "name": "ID"},
                    {"kind": "INTERFACE", "name": "Foo"},
                    {"kind": "INTERFACE", "name": "NamedObject"},
                    {"kind": "INTERFACE", "name": "Object"},
                    {"kind": "SCALAR", "name": "Boolean"},
                    {"kind": "SCALAR", "name": "Int"},
                    {"kind": "SCALAR", "name": "Float"},
                    {"kind": "INTERFACE", "name": "Person"},
                    {"kind": "INTERFACE", "name": "UserGroup"},
                    {"kind": "INTERFACE", "name": "Setting"},
                    {"kind": "INTERFACE", "name": "Profile"},
                    {"kind": "INTERFACE", "name": "User"},
                    {"kind": "OBJECT", "name": "__Schema"},
                    {"kind": "OBJECT", "name": "__Type"},
                    {"kind": "ENUM", "name": "__TypeKind"},
                    {"kind": "OBJECT", "name": "__Field"},
                    {"kind": "OBJECT", "name": "__InputValue"},
                    {"kind": "OBJECT", "name": "__EnumValue"},
                    {"kind": "OBJECT", "name": "__Directive"},
                    {"kind": "ENUM", "name": "__DirectiveLocation"},
                    {"kind": "OBJECT", "name": "UserGroupType"},
                    {"kind": "OBJECT", "name": "SettingType"},
                    {"kind": "OBJECT", "name": "ProfileType"},
                    {"kind": "OBJECT", "name": "UserType"},
                    {"kind": "OBJECT", "name": "PersonType"},
                    {"kind": "OBJECT", "name": "FooType"}
                ]
            }'
        };
        """

    def test_graphql_translation_schema_09(self):
        # this is specifically testing some issues with quoting in
        # json "\"blah\"" during introspection conversions
        r"""
        query {
            __schema {
                directives {
                    name
                    args {
                        name
                        defaultValue
                    }
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __schema := <json>'{
                "directives": [
                    {
                        "name": "include",
                        "args": [{
                            "name": "if",
                            "defaultValue": null
                        }]
                    },
                    {
                        "name": "skip",
                        "args": [{
                            "name": "if",
                            "defaultValue": null
                        }]
                    },
                    {
                        "name": "deprecated",
                        "args": [{
                            "name": "reason",
                            "defaultValue": "\\"No longer supported\\""
                        }]
                    },
                ]
            }'
        };
        """

    def test_graphql_translation_schema_10(self):
        r"""

        query {
            User {
                name,
                groups {
                    id
                    name
                }
            }
            __schema {
                types {
                    kind
                    name
                }
            }
        }

% OK %
        SELECT graphql::Query {
            User := (SELECT
                test::User {
                    name,
                    groups: {
                        id,
                        name
                    }
                }
            ),
            __schema := <json>'{
                "types": [
                    {"kind": "OBJECT", "name": "Query"},
                    {"kind": "SCALAR", "name": "String"},
                    {"kind": "SCALAR", "name": "ID"},
                    {"kind": "INTERFACE", "name": "Foo"},
                    {"kind": "INTERFACE", "name": "NamedObject"},
                    {"kind": "INTERFACE", "name": "Object"},
                    {"kind": "SCALAR", "name": "Boolean"},
                    {"kind": "SCALAR", "name": "Int"},
                    {"kind": "SCALAR", "name": "Float"},
                    {"kind": "INTERFACE", "name": "Person"},
                    {"kind": "INTERFACE", "name": "UserGroup"},
                    {"kind": "INTERFACE", "name": "Setting"},
                    {"kind": "INTERFACE", "name": "Profile"},
                    {"kind": "INTERFACE", "name": "User"},
                    {"kind": "OBJECT", "name": "__Schema"},
                    {"kind": "OBJECT", "name": "__Type"},
                    {"kind": "ENUM", "name": "__TypeKind"},
                    {"kind": "OBJECT", "name": "__Field"},
                    {"kind": "OBJECT", "name": "__InputValue"},
                    {"kind": "OBJECT", "name": "__EnumValue"},
                    {"kind": "OBJECT", "name": "__Directive"},
                    {"kind": "ENUM", "name": "__DirectiveLocation"},
                    {"kind": "OBJECT", "name": "UserGroupType"},
                    {"kind": "OBJECT", "name": "SettingType"},
                    {"kind": "OBJECT", "name": "ProfileType"},
                    {"kind": "OBJECT", "name": "UserType"},
                    {"kind": "OBJECT", "name": "PersonType"},
                    {"kind": "OBJECT", "name": "FooType"}
                ]
            }'
        };
        """

    def test_graphql_translation_type_01(self):
        r"""
        query {
            __type(name: "User") {
                __typename
                name
                kind
            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "name": "User",
                "kind": "INTERFACE"
            }'
        };
        """

    def test_graphql_translation_type_02(self):
        r"""
        query {
            __type(name: "UserType") {
                __typename
                kind
                name
                description
                interfaces {
                    name
                }
                possibleTypes {
                    name
                }
                enumValues {
                    name
                }
                inputFields {
                    name
                }
                ofType {
                    name
                }

            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "kind": "OBJECT"
                "name": "UserType",
                "description": null,
                "interfaces": [
                    {
                        "name": "User"
                    },
                    {
                        "name": "NamedObject"
                    },
                    {
                        "name": "Object"
                    }
                ],
                "possibleTypes": null,
                "enumValues": null,
                "inputFields": null,
                "ofType": null
            }'
        };
    """

    def test_graphql_translation_type_03(self):
        r"""
        query {
            __type(name: "User") {
                __typename
                kind
                name
                description
                interfaces {
                    name
                }
                possibleTypes {
                    name
                }
                enumValues {
                    name
                }
                inputFields {
                    name
                }
                ofType {
                    name
                }

            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "kind": "INTERFACE"
                "name": "User",
                "description": null,
                "interfaces": null,
                "possibleTypes": [
                    {
                        "name": "UserType"
                    },
                    {
                        "name": "PersonType"
                    }
                ],
                "enumValues": null,
                "inputFields": null,
                "ofType": null
            }'
        };
    """

    def test_graphql_translation_type_04(self):
        r"""
        query {
            __type(name: "UserGroup") {
                __typename
                name
                kind
                fields {
                    __typename
                    name
                    description
                    type {
                        __typename
                        name
                        kind
                        ofType {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                                ofType {
                                    __typename
                                    name
                                    kind
                                    ofType {
                                        __typename
                                        name
                                        kind
                                        ofType {
                                            name
                                        }
                                    }
                                }
                            }
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "name": "UserGroup",
                "kind": "INTERFACE",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "settings",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": null,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "Setting",
                                    "kind": "INTERFACE",
                                    "ofType": null
                                }
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    }
                ]
            }'
        };
        """

    def test_graphql_translation_type_05(self):
        r"""
        fragment _t on __Type {
            __typename
            name
            kind
        }

        query {
            __type(name: "UserGroupType") {
                ..._t
                fields {
                    __typename
                    name
                    description
                    type {
                        ..._t
                        ofType {
                            ..._t
                            ofType {
                                ..._t
                                ofType {
                                    ..._t
                                    ofType {
                                        ..._t
                                        ofType {
                                            name
                                        }
                                    }
                                }
                            }
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "name": "UserGroupType",
                "kind": "OBJECT",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "settings",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": null,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "Setting",
                                    "kind": "INTERFACE",
                                    "ofType": null
                                }
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    }
                ]
            }'
        };
        """

    def test_graphql_translation_type_06(self):
        r"""
        query {
            __type(name: "ProfileType") {
                __typename
                name
                kind
                fields {
                    __typename
                    name
                    description
                    type {
                        __typename
                        name
                        kind
                        ofType {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                                ofType {
                                    __typename
                                    name
                                    kind
                                    ofType {
                                        __typename
                                        name
                                        kind
                                        ofType {
                                            name
                                        }
                                    }
                                }
                            }
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "name": "ProfileType",
                "kind": "OBJECT",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "odd",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": null,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": null,
                                        "kind": "NON_NULL",
                                        "ofType": {
                                            "__typename": "__Type",
                                            "name": "Int",
                                            "kind": "SCALAR",
                                            "ofType": null
                                        }
                                    }
                                }
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "tags",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "LIST",
                            "ofType": {
                                "__typename": "__Type",
                                "name": null,
                                "kind": "NON_NULL",
                                "ofType": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR",
                                    "ofType": null
                                }
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "value",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR",
                                "ofType": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    }
                ]
            }'
        };

        """

    def test_graphql_translation_type_07(self):
        r"""
        fragment _t on __Type {
            __typename
            name
            kind
        }

        fragment _f on __Type {
            fields {
                __typename
                name
                description
                type {
                    ..._t
                    ofType {
                        ..._t
                    }
                }
                isDeprecated
                deprecationReason
            }
        }

        fragment _T on __Type {
                    __typename
                    kind
                    name
                    description
                    ..._f
                    interfaces {
                        ..._t
                    }
                    possibleTypes {
                        name
                    }
                    enumValues {
                        name
                    }
                    inputFields {
                        name
                    }
                    ofType {
                        name
                    }
        }

        query {
            __type(name: "NamedObject") {
                __typename
                kind
                name
                description
                ..._f
                interfaces {
                    ..._T
                }
                possibleTypes {
                    ..._T
                }
                enumValues {
                    name
                }
                inputFields {
                    name
                }
                ofType {
                    name
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "kind": "INTERFACE",
                "name": "NamedObject",
                "description": null,
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "ID",
                                "kind": "SCALAR"
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": null,
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "ofType": {
                                "__typename": "__Type",
                                "name": "String",
                                "kind": "SCALAR"
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    }
                ],
                "interfaces": null,
                "possibleTypes": [
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "UserGroupType",
                        "description": null,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "settings",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": null,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "UserGroup",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            }
                        ],
                        "possibleTypes": null,
                        "enumValues": null,
                        "inputFields": null,
                        "ofType": null
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "SettingType",
                        "description": null,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "value",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "Setting",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            }
                        ],
                        "possibleTypes": null,
                        "enumValues": null,
                        "inputFields": null,
                        "ofType": null
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "ProfileType",
                        "description": null,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "odd",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": null,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "tags",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": null,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "value",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "Profile",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            }
                        ],
                        "possibleTypes": null,
                        "enumValues": null,
                        "inputFields": null,
                        "ofType": null
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "UserType",
                        "description": null,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "active",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Boolean",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "age",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Int",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "groups",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": null,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "profile",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "Profile",
                                    "kind": "INTERFACE",
                                    "ofType": null
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "score",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Float",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "User",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            }
                        ],
                        "possibleTypes": null,
                        "enumValues": null,
                        "inputFields": null,
                        "ofType": null
                    },
                    {
                        "__typename": "__Type",
                        "kind": "OBJECT",
                        "name": "PersonType",
                        "description": null,
                        "fields": [
                            {
                                "__typename": "__Field",
                                "name": "active",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Boolean",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "age",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Int",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "groups",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "LIST",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": null,
                                        "kind": "NON_NULL"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "id",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "ID",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "name",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "String",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "profile",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "Profile",
                                    "kind": "INTERFACE",
                                    "ofType": null
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            },
                            {
                                "__typename": "__Field",
                                "name": "score",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": null,
                                    "kind": "NON_NULL",
                                    "ofType": {
                                        "__typename": "__Type",
                                        "name": "Float",
                                        "kind": "SCALAR"
                                    }
                                },
                                "isDeprecated": false,
                                "deprecationReason": null
                            }
                        ],
                        "interfaces": [
                            {
                                "__typename": "__Type",
                                "name": "Person",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "User",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "NamedObject",
                                "kind": "INTERFACE"
                            },
                            {
                                "__typename": "__Type",
                                "name": "Object",
                                "kind": "INTERFACE"
                            }
                        ],
                        "possibleTypes": null,
                        "enumValues": null,
                        "inputFields": null,
                        "ofType": null
                    }
                ],
                "enumValues": null,
                "inputFields": null,
                "ofType": null
            }'
        };
    """

    def test_graphql_translation_type_08(self):
        r"""
        query {
            __type(name: "UserGroupType") {
                __typename
                name
                kind
                fields {
                    __typename
                    name
                    description
                    args {
                        name
                        description
                        type {
                            __typename
                            name
                            kind
                            ofType {
                                __typename
                                name
                                kind
                            }
                        }
                        defaultValue
                    }
                    type {
                        __typename
                        name
                        kind
                        fields {name}
                        ofType {
                            name
                            kind
                            fields {name}
                        }
                    }
                    isDeprecated
                    deprecationReason
                }
            }
        }

% OK %

        SELECT graphql::Query {
            __type := <json>'{
                "__typename": "__Type",
                "name": "UserGroupType",
                "kind": "OBJECT",
                "fields": [
                    {
                        "__typename": "__Field",
                        "name": "id",
                        "description": null,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "fields": null,
                            "ofType": {
                                "name": "ID",
                                "kind": "SCALAR",
                                "fields": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "name",
                        "description": null,
                        "args": [],
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "NON_NULL",
                            "fields": null,
                            "ofType": {
                                "name": "String",
                                "kind": "SCALAR",
                                "fields": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    },
                    {
                        "__typename": "__Field",
                        "name": "settings",
                        "description": null,
                        "args": [
                            {
                                "name": "id",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "ID",
                                    "kind": "SCALAR"
                                    "ofType": null
                                },
                                "defaultValue": null
                            },
                            {
                                "name": "name",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR"
                                    "ofType": null
                                },
                                "defaultValue": null
                            },
                            {
                                "name": "value",
                                "description": null,
                                "type": {
                                    "__typename": "__Type",
                                    "name": "String",
                                    "kind": "SCALAR"
                                    "ofType": null
                                },
                                "defaultValue": null
                            }
                        ],
                        "type": {
                            "__typename": "__Type",
                            "name": null,
                            "kind": "LIST",
                            "fields": null,
                            "ofType": {
                                "name": null,
                                "kind": "NON_NULL",
                                "fields": null
                            }
                        },
                        "isDeprecated": false,
                        "deprecationReason": null
                    }
                ]
            }'
        };
        """

    @translate_only
    def test_graphql_translation_introspection_01(self):
        r"""
        query IntrospectionQuery {
            __schema {
              queryType { name }
              mutationType { name }
              subscriptionType { name }
              types {
                ...FullType
              }
              directives {
                name
                description
                locations
                args {
                  ...InputValue
                }
              }
            }
          }

          fragment FullType on __Type {
            kind
            name
            description
            fields(includeDeprecated: true) {
              name
              description
              args {
                ...InputValue
              }
              type {
                ...TypeRef
              }
              isDeprecated
              deprecationReason
            }
            inputFields {
              ...InputValue
            }
            interfaces {
              ...TypeRef
            }
            enumValues(includeDeprecated: true) {
              name
              description
              isDeprecated
              deprecationReason
            }
            possibleTypes {
              ...TypeRef
            }
          }

          fragment InputValue on __InputValue {
            name
            description
            type { ...TypeRef }
            defaultValue
          }

          fragment TypeRef on __Type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        """
