##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re
import textwrap

from edgedb.lang import _testbase as lang_tb
from edgedb.lang.common import markup
from edgedb.lang import graphql as edge_graphql
from edgedb.lang.graphql.errors import GraphQLValidationError

from edgedb.lang.schema import declarative as s_decl


def with_variables(**kwargs):
    kwargs = {'$' + name: val for name, val in kwargs.items()}

    def wrap(func):
        lang_tb._set_spec(func, 'variables', kwargs)
        return func
    return wrap


class TranslatorTest(lang_tb.BaseParserTest):
    re_filter = re.compile(r'''[\s,]+''')

    def assert_equal(self, expected, result):
        expected_stripped = self.re_filter.sub('', expected).lower()
        result_stripped = self.re_filter.sub('', result).lower()

        assert expected_stripped == result_stripped, \
            '[test]expected: {}\n[test] != returned: {}'.format(
                expected, result)

    def run_test(self, *, source, spec, expected=None):
        debug = bool(os.environ.get('DEBUG_GRAPHQL'))
        if debug:
            print('\n--- GRAPHQL ---')
            markup.dump_code(textwrap.dedent(source).strip(), lexer='graphql')

        result = edge_graphql.translate(self.schema, source,
                                        spec.get('variables'))

        if debug:
            print('\n--- EDGEQL ---')
            markup.dump_code(result, lexer='edgeql')

        self.assert_equal(expected, result)

    def setUp(self):
        schema_text = textwrap.dedent(self.SCHEMA)
        self.schema = s_decl.parse_module_declarations(
            [('test', schema_text)])


class TestGraphQLTranslation(TranslatorTest):
    SCHEMA = r"""
        abstract concept NamedObject:
            required link name -> str

        concept Group extends NamedObject:
            link settings -> Setting:
                mapping: 1*

        concept Setting extends NamedObject:
            required link value -> str

        concept User extends NamedObject:
            required link active -> bool
            link groups -> Group:
                mapping: **
            required link age -> int
            required link score -> float
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
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
        USING
            NAMESPACE test
        SELECT
            Setting[
                name,
                value
            ]

        # query users
        USING
            NAMESPACE test
        SELECT
            User[
                id,
                name,
                groups[
                    id,
                    name
                ]
            ]

        """

    @lang_tb.must_fail(GraphQLValidationError)
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

    @lang_tb.must_fail(GraphQLValidationError)
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

    @lang_tb.must_fail(GraphQLValidationError)
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
        fragment groupFrag on Group @edgedb(module: "test") {
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
        """

    def test_graphql_translation_fragment03(self):
        r"""
        fragment userFrag2 on User @edgedb(module: "test") {
            groups {
                ... groupFrag
            }
        }

        fragment groupFrag on Group @edgedb(module: "test") {
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                groups[
                    id,
                ]
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                groups[
                    id,
                ]
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            id
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            name
        }

        query @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                ]
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            name
        }

        query ($nogroup: Boolean = false) @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %
        # critical variables: $nogroup=False
        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    name,
                    id,
                ]
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
            name
        }

        query ($nogroup: Boolean = false) @edgedb(module: "test") {
            User {
                ... userFrag1
            }
        }

% OK %
        # critical variables: $nogroup=True
        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                ]
            ]
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

        fragment groupFrag on Group @edgedb(module: "test") {
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
        USING
            NAMESPACE test
        SELECT
            Setting[
                name,
                value
            ]

        # query users
        # critical variables: $nogroup=True
        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                ]
            ]
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_directives10(self):
        r"""
        query @edgedb(module: "test") {
            User {
                name @include(if: "true"),
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
        WHERE
            (User.name = 'John')
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
        WHERE
            (
                (User.name = 'John') AND
                (User.active = True)
            )
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                (groups WHERE (User.groups.name = 'admin')) [
                    id,
                    name
                ]
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                    name
                ]
            ]
        WHERE
            (User.groups.name = 'admin')
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                    name
                ]
            ]
        WHERE
            (User.groups.name IN ('admin', 'support'))
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                    name
                ]
            ]
        WHERE
            (User.groups.name NOT IN ('admin', 'support'))
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                    name
                ]
            ]
        WHERE
            (User.groups.name != 'admin')
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                    name
                ]
            ]
        WHERE
            (User.groups.name = 'admin')
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups [
                    id,
                    name,
                    (settings WHERE
                            (User.groups.settings.name IN
                                ('level', 'description'))) [
                        name,
                        value
                    ]
                ]
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                ]
            ]
        WHERE
            (User.name = $name)
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
                groups[
                    id,
                    name
                    (settings WHERE
                            (User.groups.settings.name = $setting)) [
                        name,
                        value
                    ]
                ]
            ]
        WHERE
            (
                (User.name IN $names) AND
                (User.groups.name IN $groups)
            )
        """

    def test_graphql_translation_variables03(self):
        r"""
        query($val: Int = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = $val)
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
        USING
            NAMESPACE test
        SELECT
            User[
                name,
            ]
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables05(self):
        r"""
        query($val: Boolean! = true) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.name = $val)
        """

    def test_graphql_translation_variables08(self):
        r"""
        query($val: Int = 20) @edgedb(module: "test") {
            User(age: $val) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.age = $val)
        """

    def test_graphql_translation_variables09(self):
        r"""
        query($val: Float = 3.5) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = $val)
        """

    def test_graphql_translation_variables10(self):
        r"""
        query($val: Int = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = $val)
        """

    def test_graphql_translation_variables11(self):
        r"""
        query($val: Float = 3) @edgedb(module: "test") {
            User(score: $val) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = $val)
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables12(self):
        r"""
        query($val: Boolean = 1) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables13(self):
        r"""
        query($val: Boolean = "1") @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables14(self):
        r"""
        query($val: Boolean = 1.3) @edgedb(module: "test") {
            User {
                name @include(if: $val),
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables15(self):
        r"""
        query($val: String = 1) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables16(self):
        r"""
        query($val: String = 1.1) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables17(self):
        r"""
        query($val: String = true) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables18(self):
        r"""
        query($val: Int = 1.1) @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables19(self):
        r"""
        query($val: Int = "1") @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables20(self):
        r"""
        query($val: Int = true) @edgedb(module: "test") {
            User(age: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables21(self):
        r"""
        query($val: Float = "1") @edgedb(module: "test") {
            User(score: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
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

        USING
            NAMESPACE test
        SELECT
            User[
                name,
            ]
        WHERE
            (User.id = $val)
        """

    def test_graphql_translation_variables24(self):
        r"""
        query($val: ID = 1) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                name,
            ]
        WHERE
            (User.id = $val)
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables25(self):
        r"""
        query($val: ID = 1.1) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables26(self):
        r"""
        query($val: ID = true) @edgedb(module: "test") {
            User(id: $val) {
                name
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables27(self):
        r"""
        query($val: [String] = "Foo") @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    @with_variables(val='Foo')
    def test_graphql_translation_variables28(self):
        r"""
        query($val: [String]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables29(self):
        r"""
        query($val: [String]!) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables30(self):
        r"""
        query($val: String!) @edgedb(module: "test") {
            User(name: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_variables31(self):
        r"""
        query($val: [String] = ["Foo", 123]) @edgedb(module: "test") {
            User(name__in: $val) {
                id
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.name IN $val)

        """

    def test_graphql_translation_enum01(self):
        r"""
        query @edgedb(module: "test") {
            # this is an ENUM that gets simply converted to a string in EdgeQL
            Group(name: admin) {
                id,
                name,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            Group[
                id,
                name,
            ]
        WHERE
            (Group.name = 'admin')
        """

    def test_graphql_translation_arg_type01(self):
        r"""
        query @edgedb(module: "test") {
            User(name: "John") {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.name = 'John')
        """

    def test_graphql_translation_arg_type02(self):
        r"""
        query @edgedb(module: "test") {
            User(age: 20) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.age = 20)
        """

    def test_graphql_translation_arg_type03(self):
        r"""
        query @edgedb(module: "test") {
            User(score: 3.5) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = 3.5)
        """

    def test_graphql_translation_arg_type04(self):
        r"""
        query @edgedb(module: "test") {
            User(score: 3) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = 3)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.name = $val)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.age = $val)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = $val)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score = $val)
        """

    def test_graphql_translation_arg_type09(self):
        r"""
        query @edgedb(module: "test") {
            User(name__in: ["John", "Jane"]) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.name IN ('John', 'Jane'))
        """

    def test_graphql_translation_arg_type10(self):
        r"""
        query @edgedb(module: "test") {
            User(age__in: [10, 20, 30, 40]) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.age IN (10, 20, 30, 40))
        """

    def test_graphql_translation_arg_type11(self):
        r"""
        query @edgedb(module: "test") {
            User(score__in: [3.5, 3.6, 3.7]) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score IN (3.5, 3.6, 3.7))
        """

    def test_graphql_translation_arg_type12(self):
        r"""
        query @edgedb(module: "test") {
            User(score__in: [1, 2, 3]) {
                id,
            }
        }

% OK %

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score IN (1, 2, 3))
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.name IN $val)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.age IN $val)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score IN $val)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
            ]
        WHERE
            (User.score IN $val)
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type17(self):
        r"""
        query @edgedb(module: "test") {
            User(name: 42) {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type18(self):
        r"""
        query @edgedb(module: "test") {
            User(age: 20.5) {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type19(self):
        r"""
        query @edgedb(module: "test") {
            User(score: "3.5") {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type20(self):
        r"""
        query @edgedb(module: "test") {
            User(active: 0) {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type21(self):
        r"""
        query @edgedb(module: "test") {
            User(name__in: ["John", 42]) {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type22(self):
        r"""
        query @edgedb(module: "test") {
            User(age__in: [1, 20.5]) {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_arg_type23(self):
        r"""
        query @edgedb(module: "test") {
            User(score__in: [1, "3.5"]) {
                id,
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
                name,
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
                name,
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
                name,
                age,
            ]
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_fragment_type04(self):
        r"""
        fragment userFrag on User @edgedb(module: "test") {
            id,
            name,
        }

        query @edgedb(module: "test") {
            Group {
                ... userFrag
            }
        }
        """

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_fragment_type05(self):
        r"""
        fragment userFrag on User @edgedb(module: "test") {
            id,
            name,
        }

        fragment groupFrag on Group @edgedb(module: "test") {
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

        USING
            NAMESPACE test
        SELECT
            NamedObject[
                id,
                name,
                age,
            ]
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

        USING
            NAMESPACE test
        SELECT
            NamedObject[
                id,
                name,
            ]
        """

    @lang_tb.must_fail(GraphQLValidationError)
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

    @lang_tb.must_fail(GraphQLValidationError)
    def test_graphql_translation_fragment_type09(self):
        r"""
        query @edgedb(module: "test") {
            User {
                ... on NamedObject {
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

        USING
            NAMESPACE test
        SELECT
            NamedObject[
                id,
                name,
                age,
            ]
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

        USING
            NAMESPACE test
        SELECT
            User[
                id,
                name,
                age,
            ]
        """
