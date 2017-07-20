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

    def test_graphql_translation_query01(self):
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
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            };
        """

    def test_graphql_translation_query02(self):
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
        SELECT
            (test::Setting) {
                name,
                value
            };

        # query users
        SELECT
            (test::User){
                id,
                name,
                groups: {
                    id,
                    name
                }
            };

        """

    @tb.must_fail(GraphQLValidationError, line=3, col=13)
    def test_graphql_translation_query03(self):
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

    @tb.must_fail(GraphQLValidationError, line=5, col=17)
    def test_graphql_translation_query04(self):
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

    @tb.must_fail(GraphQLValidationError, line=5, col=17)
    def test_graphql_translation_query05(self):
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

    def test_graphql_translation_fragment01(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            };
        """

    def test_graphql_translation_fragment02(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            };
        """

    def test_graphql_translation_fragment03(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            };
        """

    def test_graphql_translation_fragment04(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            };
        """

    def test_graphql_translation_directives01(self):
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

        SELECT
            (test::User) {
                name,
            };
        """

    def test_graphql_translation_directives02(self):
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

        SELECT
            (test::User) {
                groups: {
                    id,
                }
            };
        """

    def test_graphql_translation_directives03(self):
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

        SELECT
            (test::User) {
                groups: {
                    id,
                }
            };
        """

    def test_graphql_translation_directives04(self):
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

        SELECT
            (test::User) {
                name,
            };
        """

    def test_graphql_translation_directives05(self):
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

        SELECT
            (test::User) {
                name,
            };
        """

    def test_graphql_translation_directives06(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                }
            };
        """

    @with_variables(nogroup=False)
    def test_graphql_translation_directives07(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    name,
                    id,
                }
            };
        """

    @with_variables(nogroup=True, irrelevant='foo')
    def test_graphql_translation_directives08(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                }
            };
        """

    @with_variables(nogroup=True, novalue=False)
    def test_graphql_translation_directives09(self):
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

        SELECT
            (test::Setting){
                name,
                value
            };

        # query users
        # critical variables: $nogroup=True

        SELECT
            (test::User){
                name,
                groups: {
                    id,
                }
            };
        """

    @tb.must_fail(GraphQLValidationError, line=4, col=22)
    def test_graphql_translation_directives10(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @include(if: "true"),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=4, col=22)
    def test_graphql_translation_directives11(self):
        r"""
        query ($val: String = "true") @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    def test_graphql_translation_arguments01(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            ((test::User).name = 'John');
        """

    def test_graphql_translation_arguments02(self):
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

        SELECT
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
            );
        """

    def test_graphql_translation_arguments03(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                } FILTER ((test::User).groups.name = 'admin')
            };
        """

    def test_graphql_translation_arguments04(self):
        r"""
        query @edgedb(module: "test") {
            User(groups__name: "admin") {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            ((test::User).groups.name = 'admin');
        """

    def test_graphql_translation_arguments05(self):
        r"""
        query @edgedb(module: "test") {
            User(groups__name__in: ["admin", "support"]) {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            std::array_contains(['admin', 'support'],
                                (test::User).groups.name)

        """

    def test_graphql_translation_arguments06(self):
        r"""
        query @edgedb(module: "test") {
            User(groups__name__ni: ["admin", "support"]) {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            NOT std::array_contains(['admin', 'support'],
                                    (test::User).groups.name)
        """

    def test_graphql_translation_arguments07(self):
        r"""
        query @edgedb(module: "test") {
            User(groups__name__ne: "admin") {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            ((test::User).groups.name != 'admin');
        """

    def test_graphql_translation_arguments08(self):
        r"""
        query @edgedb(module: "test") {
            User(groups__name__eq: "admin") {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            ((test::User).groups.name = 'admin');
        """

    def test_graphql_translation_arguments09(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name,
                groups {
                    id
                    name
                    settings(name__in: ["level", "description"]) {
                        name
                        value
                    }
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name,
                    settings: {
                        name,
                        value
                    } FILTER
                        std::array_contains(
                            ['level', 'description'],
                            (test::User).groups.settings.name)
                }
            };
        """

    def test_graphql_translation_variables01(self):
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

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                }
            }
        FILTER
            ((test::User).name = $name);
        """

    def test_graphql_translation_variables02(self):
        r"""
        query(
            $names: [String],
            $groups: [String],
            $setting: String
        ) @edgedb(module: "test") {
            User(name__in: $names, groups__name__in: $groups) {
                name,
                groups {
                    id
                    name
                    settings(name: $setting) {
                        name
                        value
                    }
                }
            }
        }

% OK %

        SELECT
            (test::User) {
                name,
                groups: {
                    id,
                    name
                    settings: {
                        name,
                        value
                    } FILTER
                        ((test::User).groups.settings.name = $setting)
                }
            }
        FILTER
            (
                std::array_contains($names, (test::User).name) AND
                std::array_contains($groups, (test::User).groups.name)
            );
        """

    def test_graphql_translation_variables03(self):
        r"""
        query($val: Int = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User) {
                id,
            }
        FILTER
            ((test::User).score = $val);
        """

    def test_graphql_translation_variables04(self):
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

        SELECT
            (test::User) {
                name,
            };
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables05(self):
        r"""
        query($val: Boolean! = true) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables06(self):
        r"""
        query($val: Boolean!) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    def test_graphql_translation_variables07(self):
        r"""
        query($val: String = "John") @edgedb(module: "test") {
            User(name: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User) {
                id,
            }
        FILTER
            ((test::User).name = $val);
        """

    def test_graphql_translation_variables08(self):
        r"""
        query($val: Int = 20) @edgedb(module: "test") {
            User(age: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User) {
                id,
            }
        FILTER
            ((test::User).age = $val);
        """

    def test_graphql_translation_variables09(self):
        r"""
        query($val: Float = 3.5) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User) {
                id,
            }
        FILTER
            ((test::User).score = $val);
        """

    def test_graphql_translation_variables10(self):
        r"""
        query($val: Int = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User) {
                id,
            }
        FILTER
            ((test::User).score = $val);
        """

    def test_graphql_translation_variables11(self):
        r"""
        query($val: Float = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User) {
                id,
            }
        FILTER
            ((test::User).score = $val);
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables12(self):
        r"""
        query($val: Boolean = 1) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables13(self):
        r"""
        query($val: Boolean = "1") @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables14(self):
        r"""
        query($val: Boolean = 1.3) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables15(self):
        r"""
        query($val: String = 1) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables16(self):
        r"""
        query($val: String = 1.1) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables17(self):
        r"""
        query($val: String = true) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables18(self):
        r"""
        query($val: Int = 1.1) @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables19(self):
        r"""
        query($val: Int = "1") @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables20(self):
        r"""
        query($val: Int = true) @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables21(self):
        r"""
        query($val: Float = "1") @edgedb(module: "test") {
            User(score: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables22(self):
        r"""
        query($val: Float = true) @edgedb(module: "test") {
            User(score: $val) {
                id
            }
        }
        """

    def test_graphql_translation_variables23(self):
        r"""
        query($val: ID = "1") @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }

% OK %

        SELECT
            (test::User){
                name,
            }
        FILTER
            ((test::User).id = $val);
        """

    def test_graphql_translation_variables24(self):
        r"""
        query($val: ID = 1) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }

% OK %

        SELECT
            (test::User){
                name,
            }
        FILTER
            ((test::User).id = $val);
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables25(self):
        r"""
        query($val: ID = 1.1) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables26(self):
        r"""
        query($val: ID = true) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables27(self):
        r"""
        query($val: [String] = "Foo") @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    @with_variables(val='Foo')
    def test_graphql_translation_variables28(self):
        r"""
        query($val: [String]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables29(self):
        r"""
        query($val: [String]!) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables30(self):
        r"""
        query($val: String!) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    def test_graphql_translation_variables31(self):
        r"""
        query($val: [String] = ["Foo", 123]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=2, col=15)
    @with_variables(val=["Foo", 123])
    def test_graphql_translation_variables32(self):
        r"""
        query($val: [String]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    def test_graphql_translation_variables33(self):
        r"""
        query($val: [String] = ["Foo", "Bar"]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains($val, (test::User).name);

        """

    def test_graphql_translation_enum01(self):
        r"""
        query @edgedb(module: "test") {
            # this is an ENUM that gets simply converted to a string in EdgeQL
            UserGroup(name: admin) {
                id,
                name,
            }
        }

% OK %

        SELECT
            (test::UserGroup){
                id,
                name,
            }
        FILTER
            ((test::UserGroup).name = 'admin');
        """

    def test_graphql_translation_arg_type01(self):
        r"""
        query @edgedb(module: "test") {
            User(name: "John") {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).name = 'John');
        """

    def test_graphql_translation_arg_type02(self):
        r"""
        query @edgedb(module: "test") {
            User(age: 20) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).age = 20);
        """

    def test_graphql_translation_arg_type03(self):
        r"""
        query @edgedb(module: "test") {
            User(score: 3.5) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).score = 3.5);
        """

    def test_graphql_translation_arg_type04(self):
        r"""
        query @edgedb(module: "test") {
            User(score: 3) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).score = 3);
        """

    @with_variables(val="John")
    def test_graphql_translation_arg_type05(self):
        r"""
        query($val: String!) @edgedb(module: "test") {
            User(name: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).name = $val);
        """

    @with_variables(val=20)
    def test_graphql_translation_arg_type06(self):
        r"""
        query($val: Int!) @edgedb(module: "test") {
            User(age: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).age = $val);
        """

    @with_variables(val=3.5)
    def test_graphql_translation_arg_type07(self):
        r"""
        query($val: Float!) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).score = $val);
        """

    @with_variables(val=3)
    def test_graphql_translation_arg_type08(self):
        r"""
        query($val: Int!) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            ((test::User).score = $val);
        """

    def test_graphql_translation_arg_type09(self):
        r"""
        query @edgedb(module: "test") {
            User(name__in: ["John", "Jane"]) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains(['John', 'Jane'], (test::User).name);
        """

    def test_graphql_translation_arg_type10(self):
        r"""
        query @edgedb(module: "test") {
            User(age__in: [10, 20, 30, 40]) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains([10, 20, 30, 40], (test::User).age);
        """

    def test_graphql_translation_arg_type11(self):
        r"""
        query @edgedb(module: "test") {
            User(score__in: [3.5, 3.6, 3.7]) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains([3.5, 3.6, 3.7], (test::User).score);
        """

    def test_graphql_translation_arg_type12(self):
        r"""
        query @edgedb(module: "test") {
            User(score__in: [1, 2, 3]) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains([1, 2, 3], (test::User).score);
        """

    @with_variables(val=["John", "Jane"])
    def test_graphql_translation_arg_type13(self):
        r"""
        query($val: [String]!) @edgedb(module: "test") {
            User(name__in: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains($val, (test::User).name);
        """

    @with_variables(val=[10, 20])
    def test_graphql_translation_arg_type14(self):
        r"""
        query($val: [Int]!) @edgedb(module: "test") {
            User(age__in: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains($val, (test::User).age);
        """

    @with_variables(val=[3, 3.5, 4])
    def test_graphql_translation_arg_type15(self):
        r"""
        query($val: [Float]!) @edgedb(module: "test") {
            User(score__in: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains($val, (test::User).score);
        """

    @with_variables(val=[1, 2, 3])
    def test_graphql_translation_arg_type16(self):
        r"""
        query($val: [Int]!) @edgedb(module: "test") {
            User(score__in: $val) {
                id,
            }
        }

% OK %

        SELECT
            (test::User){
                id,
            }
        FILTER
            std::array_contains($val, (test::User).score);
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type17(self):
        r"""
        query @edgedb(module: "test") {
            User(name: 42) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type18(self):
        r"""
        query @edgedb(module: "test") {
            User(age: 20.5) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type19(self):
        r"""
        query @edgedb(module: "test") {
            User(score: "3.5") {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type20(self):
        r"""
        query @edgedb(module: "test") {
            User(active: 0) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type21(self):
        r"""
        query @edgedb(module: "test") {
            User(name__in: ["John", 42]) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type22(self):
        r"""
        query @edgedb(module: "test") {
            User(age__in: [1, 20.5]) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type23(self):
        r"""
        query @edgedb(module: "test") {
            User(score__in: [1, "3.5"]) {
                id,
            }
        }
        """

    @tb.must_fail(GraphQLValidationError, line=3, col=18)
    def test_graphql_translation_arg_type24(self):
        r"""
        query @edgedb(module: "test") {
            User(active__in: [true, 0]) {
                id,
            }
        }
        """

    def test_graphql_translation_fragment_type01(self):
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

        SELECT
            (test::User){
                id,
                name,
            };
        """

    def test_graphql_translation_fragment_type02(self):
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

        SELECT
            (test::User){
                id,
                name,
            };
        """

    def test_graphql_translation_fragment_type03(self):
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

        SELECT
            (test::User){
                id,
                name,
                age,
            };
        """

    @tb.must_fail(GraphQLValidationError, 'are not related', line=9, col=17)
    def test_graphql_translation_fragment_type04(self):
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

    @tb.must_fail(GraphQLValidationError, 'are not related', line=8, col=13)
    def test_graphql_translation_fragment_type05(self):
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

    def test_graphql_translation_fragment_type06(self):
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

        SELECT
            (test::NamedObject){
                id,
                (test::User).name,
                (test::User).age,
            };
        """

    def test_graphql_translation_fragment_type07(self):
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

        SELECT
            (test::NamedObject){
                id,
                name,
            };
        """

    @tb.must_fail(GraphQLValidationError, r'field \S+ is invalid for',
                  line=5, col=13)
    def test_graphql_translation_fragment_type08(self):
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
    def test_graphql_translation_fragment_type09(self):
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

    def test_graphql_translation_fragment_type10(self):
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

        SELECT
            (test::NamedObject){
                id,
                name,
                (test::User).age,
            };
        """

    def test_graphql_translation_fragment_type11(self):
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

        SELECT
            (test::User){
                id,
                name,
                (test::User).age,
            };
        """

    def test_graphql_translation_fragment_type12(self):
        r"""
        query @edgedb(module: "test") {
            NamedObject {
                ... on User {
                    age
                }
            }
        }

% OK %

        SELECT
            (test::NamedObject) {
                (test::User).age
            }
        """

    def test_graphql_translation_import01(self):
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

        SELECT
            (mod2::Person) {
                id,
                name,
                groups: {
                    id,
                    name,
                }
            };
        """

    @tb.must_fail(GraphQLValidationError, line=8, col=13)
    def test_graphql_translation_import02(self):
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

% OK %

        SELECT
            [mod2.Person]{
                id,
                name,
                groups{
                    id,
                    name,
                ],
            };
        """

    def test_graphql_translation_duplicates01(self):
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

        SELECT
            (test::User){
                id,
                name,
            };
        """

    def test_graphql_translation_duplicates02(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @include(if: true)
                id
                name @include(if: true)
            }
        }

% OK %

        SELECT
            (test::User){
                name,
                id,
            };
        """

    def test_graphql_translation_duplicates03(self):
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

        SELECT
            (test::User){
                name,
                id,
            };
        """

    def test_graphql_translation_duplicates04(self):
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

        SELECT
            (test::User){
                id,
                name,
            };
        """

    @tb.must_fail(GraphQLValidationError, line=6, col=17)
    def test_graphql_translation_duplicates05(self):
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
    def test_graphql_translation_duplicates06(self):
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
    def test_graphql_translation_duplicates07(self):
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

    def test_graphql_translation_quoting01(self):
        r"""
        query @edgedb(module: "123lib") {
            Foo(select: "bar") {
                select
                after
            }
        }

% OK %

        SELECT
            (`123lib`::Foo){
                `select`,
                after
            }
        FILTER
            ((`123lib`::Foo).`select` = 'bar');
        """

    def test_graphql_translation_delete01(self):
        r"""
        mutation @edgedb(module: "test") {
            delete__User {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (
            DELETE
                (SELECT (test::User))
        ) {
            name,
            groups: {
                id,
                name
            }
        };
        """

    def test_graphql_translation_delete02(self):
        r"""
        mutation @edgedb(module: "test") {
            delete__User(name: "John") {
                name,
                groups {
                    id
                    name
                }
            }
        }

% OK %

        SELECT (
            DELETE
                (SELECT (test::User)
                 FILTER
                    ((test::User).name = 'John'))
        ) {
            name,
            groups: {
                id,
                name
            }
        };
        """

    def test_graphql_translation_delete03(self):
        r"""
        mutation delete @edgedb(module: "test") {
            delete__User(name: "John", active: true) {
                id,
            }
        }

% OK %

        # mutation delete
        SELECT (
            DELETE
                (SELECT (test::User)
                 FILTER
                    (
                        ((test::User).name = 'John') AND
                        ((test::User).active = True)
                    )
                )
        ) {
            id,
        };
        """

    def test_graphql_translation_insert01(self):
        r"""
        mutation @edgedb(module: "test") {
            insert__UserGroup(__data: {
                name: "new"
            }) {
                id,
                name,
            }
        }

% OK %

        SELECT (
            INSERT
                test::UserGroup {
                    name := 'new'
                }
        ) {
            id,
            name
        };
        """

    def test_graphql_translation_insert02(self):
        r"""
        mutation insert @edgedb(module: "test") {
            insert__User(__data: {
                name: "John",
                active: true,
                age: 25,
                score: 3.14
            }) {
                id,
            }
        }

% OK %

        # mutation insert
        SELECT (
            INSERT
                test::User {
                    name := 'John',
                    active := True,
                    age := 25,
                    score := 3.14
                }
        ) {
            id,
        };
        """

    def test_graphql_translation_insert03(self):
        r"""
        mutation insert @edgedb(module: "test") {
            insert__User(__data: {
                name: "John",
                active: true,
                age: 25,
                score: 3.14,
                groups__id: "21e16e2e-e445-494c-acfc-cc9378620501"
            }) {
                id,
                groups {
                    name
                }
            }
        }

% OK %

        # mutation insert
        SELECT (
            INSERT
                test::User {
                    name := 'John',
                    active := True,
                    age := 25,
                    score := 3.14,
                    groups := (
                        SELECT (std::Object)
                        FILTER (
                            (std::Object).id =
                                <std::uuid>'21e16e2e-e445-494c-acfc-cc9378620501'
                        )
                    )
                }
        ) {
            id,
            groups: {
                name
            }
        };
        """

    def test_graphql_translation_insert04(self):
        r"""
        mutation @edgedb(module: "test") {
            insert__User(__data: {
                name: "John",
                active: true,
                age: 25,
                score: 3.14,
                groups__id: [
                    "21e16e2e-e445-494c-acfc-cc9378620501",
                    "fd5f4ad8-2e8c-4224-9243-361d61dee856"
                ]
            }) {
                name,
                id,
                groups {
                    name
                }
            }
        }

% OK %

        SELECT (
            INSERT
                test::User {
                    name := 'John',
                    active := True,
                    age := 25,
                    score := 3.14,
                    groups := (
                        SELECT (std::Object)
                        FILTER (
                            (std::Object).id IN
                                [<std::uuid>'21e16e2e-e445-494c-acfc-cc9378620501',
                                 <std::uuid>'fd5f4ad8-2e8c-4224-9243-361d61dee856']
                        )
                    )
                }
        ) {
            name,
            id,
            groups: {
                name
            }
        };
        """

    def test_graphql_translation_insert05(self):
        r"""
        # this creates a nested user + profile
        #
        mutation @edgedb(module: "test") {
            insert__User(__data: {
                name: "John",
                active: true,
                age: 25,
                score: 3.14,
                profile: {
                    name: "New Profile",
                    value: "default"
                }
            }) {
                name,
                id,
                profile {
                    name,
                    value
                }
            }
        }

% OK %

        SELECT (
            INSERT
                test::User {
                    name := 'John',
                    active := True,
                    age := 25,
                    score := 3.14,
                    profile: {
                        name := 'New Profile',
                        value := 'default'
                    }
                }
        ) {
            name,
            id,
            profile: {
                name,
                value
            }
        };
        """

    def test_graphql_translation_update01(self):
        r"""
        mutation @edgedb(module: "test") {
            update__User(__data: {
                name: "Jonathan",
            },
            name: "John"
            ) {
                name,
                id,
            }
        }

% OK %

        SELECT (
            UPDATE
                test::User
            FILTER
                ((test::User).name = 'John')
            SET {
                name := 'Jonathan'
            }
        ) {
            name,
            id
        };
        """

    def test_graphql_translation_update02(self):
        r"""
        mutation special_update @edgedb(module: "test") {
            update__User(__data: {
                name: "Jonathan",
                groups__id: "21e16e2e-e445-494c-acfc-cc9378620501"
            },
            name: "John"
            ) {
                name,
                id,
                groups {
                    name
                }
            }
        }

% OK %

        # mutation special_update
        SELECT (
            UPDATE
                test::User
            FILTER
                ((test::User).name = 'John')
            SET {
                name := 'Jonathan',
                groups := (
                    SELECT (std::Object)
                    FILTER (
                        (std::Object).id =
                            <std::uuid>'21e16e2e-e445-494c-acfc-cc9378620501'
                    )
                )
            }
        ) {
            name,
            id,
            groups: {
                name
            }
        };
        """

    def test_graphql_translation_update03(self):
        r"""
        mutation special_update @edgedb(module: "test") {
            update__User(__data: {
                name: "Jonathan",
                groups__id: [
                    "21e16e2e-e445-494c-acfc-cc9378620501",
                    "fd5f4ad8-2e8c-4224-9243-361d61dee856"
                ]
            },
            name: "John"
            ) {
                name,
                id,
                groups {
                    name
                }
            }
        }

% OK %

        # mutation special_update
        SELECT (
            UPDATE
                test::User
            FILTER
                ((test::User).name = 'John')
            SET {
                name := 'Jonathan',
                groups := (
                    SELECT (std::Object)
                    FILTER (
                        (std::Object).id IN
                            [<std::uuid>'21e16e2e-e445-494c-acfc-cc9378620501',
                             <std::uuid>'fd5f4ad8-2e8c-4224-9243-361d61dee856']
                    )
                )
            }
        ) {
            name,
            id,
            groups: {
                name
            }
        };
        """
