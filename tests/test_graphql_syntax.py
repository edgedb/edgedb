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


import re

from edb import errors

from edb.lang import _testbase as tb
from edb.lang.graphql import generate_source as gql_to_source
from edb.lang.graphql.parser import parser as gql_parser


class GraphQLSyntaxTest(tb.BaseSyntaxTest):
    re_filter = re.compile(r'''[\s,]+|(\#.*?\n)''')
    parser_debug_flag = 'DEBUG_GRAPHQL'
    markup_dump_lexer = 'graphql'
    ast_to_source = gql_to_source

    def get_parser(self, *, spec):
        return gql_parser.GraphQLParser()


class TestGraphQLParser(GraphQLSyntaxTest):
    def test_graphql_syntax_empty01(self):
        """"""

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=1, col=1)
    def test_graphql_syntax_empty02(self):
        """\v"""

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=1, col=1)
    def test_graphql_syntax_empty03(self):
        """\f"""

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=1, col=1)
    def test_graphql_syntax_empty04(self):
        """\xa0"""

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=2, col=1)
    def test_graphql_syntax_empty05(self):
        """\r\n;"""

    @tb.must_fail(errors.GraphQLSyntaxError, line=1, col=2)
    def test_graphql_syntax_empty06(self):
        '''"'''

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=10)
    def test_graphql_syntax_empty07(self):
        """
        "
        "
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected token', line=1, col=1)
    def test_graphql_syntax_empty08(self):
        """..."""

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=22)
    def test_graphql_syntax_string01(self):
        """
        { field(arg:"\b") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=22)
    def test_graphql_syntax_string02(self):
        R"""
        { field(arg:"\x") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=22)
    def test_graphql_syntax_string03(self):
        R"""
        { field(arg:"\u1") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=22)
    def test_graphql_syntax_string04(self):
        R"""
        { field(arg:"\u0XX1") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=22)
    def test_graphql_syntax_string05(self):
        R"""
        { field(arg:"\uXXXX") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=25)
    def test_graphql_syntax_string06(self):
        R"""
        { field(arg:"foo\uFXXX") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=22)
    def test_graphql_syntax_string07(self):
        R"""
        { field(arg:"\uXXXF") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=2, col=34)
    def test_graphql_syntax_string08(self):
        R"""
        { field(arg:"\uFEFF\n") };
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=29)
    def test_graphql_syntax_string09(self):
        """
        { field(arg:"foo') }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=3, col=23)
    def test_graphql_syntax_string10(self):
        r"""
        { field(
            arg:"foo \
        ) }
        """

    def test_graphql_syntax_string11(self):
        r"""
        { field(arg: "\\/ \\\/") }

% OK %

        { field(arg: "\\/ \\/") }
        """

    def test_graphql_syntax_string12(self):
        r"""
        { field(arg: "\\\\x") }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, line=2, col=25)
    def test_graphql_syntax_string13(self):
        r"""
        { field(arg: "\\\x") }
        """

    def test_graphql_syntax_string14(self):
        r"""
        { field(arg: "\\'") }
        """

    def test_graphql_syntax_string15(self):
        r"""
        { field(arg: "\\\n \\\\n") }
        """

    def test_graphql_syntax_short01(self):
        """{id}"""

    def test_graphql_syntax_short02(self):
        """
        {id, name, description}
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'short form is not allowed here',
                  line=2, col=9)
    def test_graphql_syntax_short03(self):
        """
        {id}
        {name}
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'short form is not allowed here',
                  line=3, col=9)
    def test_graphql_syntax_short04(self):
        """
        query {id}
        {name}
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'Unexpected token', line=2, col=18)
    def test_graphql_syntax_short05(self):
        """
        { field: {} }
        """

    def test_graphql_syntax_field01(self):
        """
        {
            id
        }
        """

    def test_graphql_syntax_field02(self):
        """
        {
            foo: id
        }
        """

    def test_graphql_syntax_field03(self):
        """
        {
            name(q: "bar")
        }
        """

    def test_graphql_syntax_field04(self):
        """
        {
            foo: id(q: 42)
        }
        """

    def test_graphql_syntax_field05(self):
        """
        {
            foo: name(q: 42, w: "bar")
        }
        """

    def test_graphql_syntax_field06(self):
        """
        {
            foo: name (q: 42, w: "bar") @skip(if: true)
        }
        """

    def test_graphql_syntax_field07(self):
        """
        {
            foo: name (q: 42, w: "bar") @skip(if: false), @include(if: true)
        }
        """

    def test_graphql_syntax_inline_fragment01(self):
        """
        {
            ...{
                foo
            }
        }
        """

    def test_graphql_syntax_inline_fragment02(self):
        """
        {
            ... @skip(if: true) {
                foo
            }
        }
        """

    def test_graphql_syntax_inline_fragment03(self):
        """
        {
            ... @skip(if: true), @include(if: true) {
                foo
            }
        }
        """

    def test_graphql_syntax_inline_fragment04(self):
        """
        {
            ... on User {
                foo
            }
        }
        """

    def test_graphql_syntax_inline_fragment05(self):
        """
        {
            ... on User @skip(if: true), @include(if: true) {
                foo
            }
        }
        """

    def test_graphql_syntax_fragment01(self):
        """
        fragment friendFields on User {
            id
            name
            profilePic(size: 50)
        }

        { ... friendFields }
        """

    def test_graphql_syntax_fragment02(self):
        """
        fragment friendFields on User @skip(if: false), @include(if: true) {
            id
            name
            profilePic(size: 50)
        }

        { ... friendFields }
        """

    def test_graphql_syntax_fragment03(self):
        """
        fragment someFields on User { id }

        {
            ...someFields @skip(if: true)
        }
        """

    def test_graphql_syntax_fragment04(self):
        """
        fragment someFields on User { id }

        {
            ...someFields @skip(if: true), @include(if: false)
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'Unexpected token', line=3, col=28)
    def test_graphql_syntax_fragment05(self):
        """
        { ...MissingOn }
        fragment MissingOn Type {name}
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'undefined fragment', line=2, col=10)
    def test_graphql_syntax_fragment06(self):
        """
        {...Missing}
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'unused fragment', line=2, col=9)
    def test_graphql_syntax_fragment07(self):
        """
        fragment Missing on Type {name}
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'cycle in fragment definitions',
                  line=2, col=9)
    def test_graphql_syntax_fragment08(self):
        """
        fragment cyclceFrag on Type {
            ...cyclceFrag
        }

        {... cyclceFrag}
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'cycle in fragment definitions',
                  line=2, col=9)
    def test_graphql_syntax_fragment09(self):
        """
        fragment cyclceFrag on Type {
            ...otherFrag
        }

        fragment otherFrag on Type {
            ...cyclceFrag
        }

        {... cyclceFrag}
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'cycle in fragment definitions',
                  line=2, col=9)
    def test_graphql_syntax_fragment10(self):
        """
        fragment A on Type {...B}
        fragment B on Type {...C}
        fragment C on Type {...D}
        fragment D on Type {...A}

        {... C}
        """

    def test_graphql_syntax_query01(self):
        """
        query getZuckProfile {
            id
            name
        }
        """

    def test_graphql_syntax_query02(self):
        """
        query getZuckProfile($devicePicSize: Int) {
            id
            name
        }
        """

    def test_graphql_syntax_query03(self):
        """
        query getZuckProfile($devicePicSize: Int) @skip(if: true) {
            id
            name
        }
        """

    def test_graphql_syntax_query04(self):
        """
        query noFragments {
            user(id: 4) {
                friends(first: 10) {
                    id
                    name
                    profilePic(size: 50)
                }
                mutualFriends(first: 10) {
                    id
                    name
                    profilePic(size: 50)
                }
            }
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'Unexpected token', line=2, col=23)
    def test_graphql_syntax_query05(self):
        r"""
        query myquery on type { field }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=2, col=32)
    def test_graphql_syntax_query06(self):
        r"""
        query myquery { field };
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=2, col=25)
    def test_graphql_syntax_query07(self):
        r"""
        query myQuery { \a }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected token', line=2, col=9)
    def test_graphql_syntax_query08(self):
        """
        notanoperation Foo { field }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  r'operation with name \S+ already exists',
                  line=3, col=9)
    def test_graphql_syntax_query09(self):
        """
        query myQuery { id }
        query myQuery { id }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'unnamed operation is not allowed here',
                  line=2, col=9)
    def test_graphql_syntax_query10(self):
        """
        query { id }
        query myQuery { id }
        """

    def test_graphql_syntax_mutation01(self):
        """
        mutation {
            likeStory(storyID: 12345) {
                story {
                    likeCount
                }
            }
        }
        """

    def test_graphql_syntax_mutation02(self):
        """
        mutation ($storyId: Int) {
            likeStory(storyID: $storyId) {
                story {
                    likeCount
                }
            }
        }
        """

    def test_graphql_syntax_mutation03(self):
        """
        mutation ($storyId: Int, $likes: Int) @include(if: $likes) {
            likeStory(storyID: $storyId, likeCount: $likes) {
                story {
                    likeCount
                }
            }
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'operation', line=3, col=9)
    def test_graphql_syntax_mutation04(self):
        """
        mutation myQuery { id }
        query myQuery { id }
        """

    def test_graphql_syntax_subscription01(self):
        """
        subscription {
            id
            name
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'operation', line=3, col=9)
    def test_graphql_syntax_subscription02(self):
        """
        mutation myQuery { id }
        subscription myQuery { id }
        """

    def test_graphql_syntax_values01(self):
        """
        {
            user(id: 4) {
                friends(first: 10) {
                    id
                    name
                    profilePic(size: 50)
                }
            }
        }
        """

    def test_graphql_syntax_values02(self):
        """
        {
            foo(id: 4) {
                id
                bar(x: 23.1, y: -42.1, z: -999)
            }
        }
        """

    def test_graphql_syntax_values03(self):
        """
        {
            foo(id: 4) {
                id
                bar(x: 2.31e-08, y: -4.21e+33, z: -9e+12)
            }
        }
        """

    def test_graphql_syntax_values04(self):
        # graphql escapes: \", \\, \/, \b, \f, \n, \r, \t
        r"""
        {
            foo(id: 4) {
                id
                bar(name: "\"something\"",
                    more: "",
                    description: "\\\/\b\f\n\r\t 'blah' спам")
            }
        }
% OK %
        {
            foo(id: 4) {
                id
                bar(name: "\"something\"",
                    more: "",
                    description: "\\/\b\f\n\r\t 'blah' спам")
            }
        }
        """

    def test_graphql_syntax_values05(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(param: MOBILE_WEB)
            }
        }
        """

    def test_graphql_syntax_values06(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(array: [])
            }
        }
        """

    def test_graphql_syntax_values07(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(array: [1, "two", 3])
            }
        }
        """

    def test_graphql_syntax_values08(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(array: {})
            }
        }
        """

    def test_graphql_syntax_values09(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(map: {
                        home: "416 123 4567"
                        work: "416 123 4567"
                    })
            }
        }
        """

    def test_graphql_syntax_values10(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(map: {
                        messy: [1, "two", [], [3, {}, 4]]
                        home: "416 123 4567"
                        work: "416 123 4567"
                        nested: {
                            deeper: [{
                                stuff: 42
                            }, {
                                spam: "ham"
                            }]
                        }
                    })
            }
        }
        """

    def test_graphql_syntax_values11(self):
        """
        query getZuckProfile($devicePicSize: Int = 42) {
            user(id: 4) {
                id
                name
                profilePic(size: $devicePicSize)
            }
        }
        """

    def test_graphql_syntax_values12(self):
        r"""
        query myQuery($special: Int = 42) {
            foo(id: 4) {
                id
                bar(map: {
                        messy: [1, "two", [], [3, {}, 4]]
                        home: "416 123 4567"
                        work: "416 123 4567"
                        nested: {
                            deeper: [{
                                stuff: $special
                            }, {
                                spam: "ham"
                            }]
                        }
                    })
            }
        }
        """

    def test_graphql_syntax_values13(self):
        r"""
        {
            foo(id: null) {
                id
                bar(param: NULL)
            }
        }
        """

    def test_graphql_syntax_values14(self):
        r"""
        {
            foo(id: NULL) {
                id
                bar(param: null)
            }
        }
        """

    def test_graphql_syntax_values15(self):
        r"""
        query myQuery($var: Int) {
            field(complex: { a: { b: [ $var ] } })
        }
        """

    def test_graphql_syntax_values16(self):
        r"""
        query Foo($x: Complex = { a: { b: [ "var" ] } }) {
            field
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, r"undefined variable '\$var'",
                  line=2, col=45)
    def test_graphql_syntax_values17(self):
        r"""
        query Foo($x: Complex = { a: { b: [ $var ] } }) {
            field
        }
        """

    def test_graphql_syntax_values18(self):
        r"""
        {
            fieldWithNullableStringInput(input: null)
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=3, col=49)
    def test_graphql_syntax_values19(self):
        r"""
        {
            fieldWithNullableStringInput(input: .123)
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=3, col=49)
    def test_graphql_syntax_values20(self):
        r"""
        {
            fieldWithNullableStringInput(input: 0123)
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'Unexpected', line=3, col=49)
    def test_graphql_syntax_values21(self):
        r"""
        {
            fieldWithNullableStringInput(input: +123)
        }
        """

    def test_graphql_syntax_values22(self):
        r"""
        {
            foo(bar: ["spam", "ham"]) {
                id
                name
            }
        }
        """

    def test_graphql_syntax_var01(self):
        r"""
        query ($name: String!) {
            User(name: $name) {
                id
                name
            }
        }
        """

    def test_graphql_syntax_var02(self):
        r"""
        query A($atOtherHomes: Boolean) {
          ...HouseTrainedFragment
        }

        query B($atOtherHomes: Boolean) {
          ...HouseTrainedFragment
        }

        fragment HouseTrainedFragment on Base {
          dog {
            isHousetrained(atOtherHomes: $atOtherHomes)
          }
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, r"undefined variable '\$var'",
                  line=3, col=49)
    def test_graphql_syntax_scope01(self):
        r"""
        {
            fieldWithNullableStringInput(input: $var)
        }
        """

    def test_graphql_syntax_scope02(self):
        r"""
        fragment goodVar on User {name(first: $var)}

        query ($var: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, r"undefined variable '\$bad'",
                  line=3, col=46)
    def test_graphql_syntax_scope03(self):
        r"""
        fragment goodVar on User {name(first: $var)}
        fragment badVar on User {name(first: $bad)}

        query ($var: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
            ... badVar
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, r"undefined variable '\$bad'",
                  line=10, col=53)
    def test_graphql_syntax_scope04(self):
        r"""
        fragment goodVar on User {
            name(first: $var)
            ... midVar
        }
        fragment midVar on User {
            id
            ... badVar
        }
        fragment badVar on User {description(first: $bad)}

        query ($var: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
        }
        """

    def test_graphql_syntax_scope05(self):
        r"""
        fragment goodVar on User {
            name(first: $var)
            ... midVar
        }
        fragment midVar on User {
            id
            ... badVar
        }
        fragment badVar on User {description(first: $bad)}

        query ($var: String, $bad: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, r"undefined variable '\$bad'",
                  line=10, col=53)
    def test_graphql_syntax_scope06(self):
        r"""
        fragment goodVar on User {
            name(first: $var)
            ... midVar
        }
        fragment midVar on User {
            id
            ... badVar
        }
        fragment badVar on User {description(first: $bad)}

        query goodQuery ($var: String, $bad: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
        }
        query badQuery {
            ... midVar
        }
        """

    def test_graphql_syntax_names01(self):
        r"""
        {
            on
            fragment
            query
            mutation
            subscription
            true
            false
            null
        }
        """

    def test_graphql_syntax_names02(self):
        r"""
        {
            on: on_ok
            fragment: fragment_ok
            query: query_ok
            mutation: mutation_ok
            subscription: subscription_ok
            true: true_ok
            false: false_ok
            null: null_ok
        }
        """

    def test_graphql_syntax_names03(self):
        r"""
        {
            on_ok: on
            fragment_ok: fragment
            query_ok: query
            mutation_ok: mutation
            subscription_ok: subscription
            true_ok: true
            false_ok: false
            null_ok: null
        }
        """

    def test_graphql_syntax_names04(self):
        r"""
        {
            foo(someObj: {
                on: 42
                fragment: 42
                query: 42
                mutation: 42
                subscription: 42
                true: 42
                false: 42
                null: 42
            }) {
                id
            }
        }
        """

    def test_graphql_syntax_names05(self):
        r"""
        {
            foo(
                on: 42
                fragment: 42
                query: 42
                mutation: 42
                subscription: 42
                true: 42
                false: 42
                null: 42
            ) {
                id
            }
        }
        """

    def test_graphql_syntax_names06(self):
        r"""
        fragment name_on on on {id}
        fragment name_fragment on fragment {id}
        fragment name_query on query {id}
        fragment name_mutation on mutation {id}
        fragment name_subscription on subscription {id}
        fragment name_true on true {id}
        fragment name_false on false {id}
        fragment name_null on null {id}

        {
            ... name_on
            ... name_fragment
            ... name_query
            ... name_mutation
            ... name_subscription
            ... name_true
            ... name_false
            ... name_null
        }
        """

    def test_graphql_syntax_names07(self):
        r"""
        fragment fragment on fragmentFoo {id}
        fragment query on queryFoo {id}
        fragment mutation on mutationFoo {id}
        fragment subscription on subscriptionFoo {id}
        fragment true on trueFoo {id}
        fragment false on falseFoo {id}
        fragment null on nullFoo {id}

        {
            ... fragment
            ... query
            ... mutation
            ... subscription
            ... true
            ... false
            ... null
        }
        """

    def test_graphql_syntax_names08(self):
        r"""
        query A { ... on on {id} }
        query B { ... on fragment {id} }
        query C { ... on query {id} }
        query D { ... on mutation {id} }
        query E { ... on subscription {id} }
        query F { ... on true {id} }
        query G { ... on false {id} }
        query H { ... on null {id} }
        """

    def test_graphql_syntax_names09(self):
        r"""
        # fragment not_on on Foo {name}
        # fragment fragment on Foo {name}
        # fragment query on Foo {name}
        # fragment mutation on Foo {name}
        # fragment subscription on Foo {name}
        # fragment true on Foo {name}
        fragment false on Foo {name}
        fragment null on Foo {name}

        # query A { ... not_on on on {id} }
        # query B { ... fragment on fragmentFoo {id} }
        # query C { ... query on queryFoo {id} }
        # query D { ... mutation on mutationFoo {id} }
        # query E { ... subscription on subscriptionFoo {id} }
        # query F { ... true on trueFoo {id} }
        query G { ... false on falseFoo {id} }
        query H { ... null on nullFoo {id} }
        """

    def test_graphql_syntax_names10(self):
        r"""
        query (
            $on: on = on
            $fragment: fragment = fragment
            $query: query = query
            $mutation: mutation = mutation
            $subscription: subscription = subscription
            $true: true = true
            $false: false = false
            $null: null = NULL
        ) {
            id
        }
        """

    def test_graphql_syntax_names11(self):
        r"""
        fragment someFragment on Foo {id}

        query A { ...someFragment @on }
        query B { ...someFragment @fragment }
        query C { ...someFragment @query }
        query D { ...someFragment @mutation }
        query E { ...someFragment @subscription }
        query F { ...someFragment @true }
        query G { ...someFragment @false }
        query H { ...someFragment @null }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'Unexpected token', line=2, col=21)
    def test_graphql_syntax_names12(self):
        r"""
        { ... on on on {id} }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'Unexpected token', line=2, col=18)
    def test_graphql_syntax_names13(self):
        r"""
        fragment on on on {id}
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'Unexpected token', line=2, col=18)
    def test_graphql_syntax_names14(self):
        r"""
        { ... on }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'variabledefinition', line=2, col=32)
    def test_graphql_syntax_names15(self):
        r"""
        query myQuery($x: Int, $x: Int) { id }
        """

    @tb.must_fail(errors.GraphQLSyntaxError,
                  'variabledefinition', line=2, col=32)
    def test_graphql_syntax_names16(self):
        r"""
        query myQuery($x: Int, $x: Float) { id }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'argument', line=3, col=23)
    def test_graphql_syntax_names17(self):
        r"""
        {
            foo(x: 1, x: 2)
        }
        """

    @tb.must_fail(errors.GraphQLSyntaxError, 'argument', line=3, col=23)
    def test_graphql_syntax_names18(self):
        r"""
        {
            foo(x: 1, x: "one")
        }
        """

    def test_graphql_syntax_comments01(self):
        """
        # some comment
        query noFragments {
            user(id: 4) {
                friends(first: 10) {  # end of line comment
                    # user id
                    id
                    # full name
                    name
                    # avatar
                    profilePic(size: 50)
                }
                mutualFriends(
                    # commenting on arguments
                    first: 10
                ) {
                    id
                    name
                    profilePic(size: 50)
                }
            }
        }
        """
