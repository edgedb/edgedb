##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re

from edgedb.lang.common import markup
from edgedb.lang.graphql import codegen
from edgedb.lang.graphql.parser import parser
from edgedb.server import _testbase as tb


class ParserTest(tb.BaseParserTest):
    re_filter = re.compile(r'[\s,]+|(#.*?\n)')
    parser_cls = parser.GraphQLParser

    def get_parser(self, *, spec):
        return self.__class__.parser_cls()

    def assert_equal(self, expected, result):
        expected_stripped = self.re_filter.sub('', expected).lower()
        result_stripped = self.re_filter.sub('', result).lower()

        assert expected_stripped == result_stripped, \
            '[test]expected: {}\n[test] != returned: {}'.format(
                expected, result)

    def run_test(self, *, source, spec):
        debug = bool(os.environ.get('DEBUG_GRAPHQL'))

        if debug:
            markup.dump_code(source, lexer='graphql')

        p = self.get_parser(spec=spec)

        esast = p.parse(source)

        if debug:
            markup.dump(esast)

        processed_src = codegen.GraphQLSourceGenerator.to_source(esast)

        if debug:
            markup.dump_code(processed_src, lexer='graphql')

        expected_src = source

        self.assert_equal(expected_src, processed_src)


class TestGraphQLParser(ParserTest):
    def test_graphql_parser_empty01(self):
        """"""

    def test_graphql_parser_short01(self):
        """{id}"""

    def test_graphql_parser_short02(self):
        """
        {id, name, description}
        """

    def test_graphql_parser_field01(self):
        """
        {
            id
        }
        """

    def test_graphql_parser_field02(self):
        """
        {
            foo: id
        }
        """

    def test_graphql_parser_field03(self):
        """
        {
            name(q: "bar")
        }
        """

    def test_graphql_parser_field04(self):
        """
        {
            foo: id(q: 42)
        }
        """

    def test_graphql_parser_field05(self):
        """
        {
            foo: name(q: 42, w: "bar")
        }
        """

    def test_graphql_parser_field06(self):
        """
        {
            foo: name (q: 42, w: "bar") @skip(if: true)
        }
        """

    def test_graphql_parser_field07(self):
        """
        {
            foo: name (q: 42, w: "bar") @skip(if: false), @include(if: true)
        }
        """

    def test_graphql_parser_spread01(self):
        """
        {
            ...someFields
        }
        """

    def test_graphql_parser_spread02(self):
        """
        {
            ...someFields @skip(if: true)
        }
        """

    def test_graphql_parser_spread03(self):
        """
        {
            ...someFields @skip(if: true), @include(if: false)
        }
        """

    def test_graphql_parser_inline_fragment01(self):
        """
        {
            ...{
                foo
            }
        }
       """

    def test_graphql_parser_inline_fragment02(self):
        """
        {
            ... @skip(if: true) {
                foo
            }
        }
       """

    def test_graphql_parser_inline_fragment03(self):
        """
        {
            ... @skip(if: true), @include(if: true) {
                foo
            }
        }
       """

    def test_graphql_parser_inline_fragment04(self):
        """
        {
            ... on User {
                foo
            }
        }
       """

    def test_graphql_parser_inline_fragment05(self):
        """
        {
            ... on User @skip(if: true), @include(if: true) {
                foo
            }
        }
       """

    def test_graphql_parser_fragment01(self):
        """
        fragment friendFields on User {
            id
            name
            profilePic(size: 50)
        }
       """

    def test_graphql_parser_fragment02(self):
        """
        fragment friendFields on User @skip(if: false), @include(if: true) {
            id
            name
            profilePic(size: 50)
        }
       """

    def test_graphql_parser_query01(self):
        """
        query getZuckProfile {
            id
            name
        }
       """

    def test_graphql_parser_query02(self):
        """
        query getZuckProfile($devicePicSize: Int) {
            id
            name
        }
       """

    def test_graphql_parser_query03(self):
        """
        query getZuckProfile($devicePicSize: Int) @skip(if: true) {
            id
            name
        }
       """

    def test_graphql_parser_query04(self):
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

    def test_graphql_parser_mutation01(self):
        """
        mutation {
            likeStory(storyID: 12345) {
                story {
                    likeCount
                }
            }
        }
       """

    def test_graphql_parser_mutation02(self):
        """
        mutation ($storyId: Int) {
            likeStory(storyID: $storyId) {
                story {
                    likeCount
                }
            }
        }
       """

    def test_graphql_parser_mutation03(self):
        """
        mutation ($storyId: Int, $likes: Int) @include(if: $likes) {
            likeStory(storyID: $storyId, likeCount: $likes) {
                story {
                    likeCount
                }
            }
        }
       """

    def test_graphql_parser_values01(self):
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

    def test_graphql_parser_values02(self):
        """
        {
            foo(id: 4) {
                id
                bar(x: 23.1, y: -42.1, z: -999)
            }
        }
       """

    def test_graphql_parser_values03(self):
        """
        {
            foo(id: 4) {
                id
                bar(x: 2.31e-08, y: -4.21e+33, z: -9e+12)
            }
        }
       """

    def test_graphql_parser_values04(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(name: "\"something\"",
                    description: "\\\/\b\f\n\r\t 'blah' \u279b")
            }
        }
       """

    def test_graphql_parser_values05(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(param: MOBILE_WEB)
            }
        }
       """

    def test_graphql_parser_values06(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(array: [])
            }
        }
       """

    def test_graphql_parser_values07(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(array: [1, "two", 3])
            }
        }
       """

    def test_graphql_parser_values08(self):
        r"""
        {
            foo(id: 4) {
                id
                bar(array: {})
            }
        }
       """

    def test_graphql_parser_values09(self):
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

    def test_graphql_parser_values10(self):
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

    def test_graphql_parser_values11(self):
        """
        query getZuckProfile($devicePicSize: Int 42) {
            user(id: 4) {
                id
                name
                profilePic(size: $devicePicSize)
            }
        }
       """

    def test_graphql_parser_values12(self):
        r"""
        query myQuery($special: Int 42) {
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

    def test_graphql_parser_names01(self):
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

    def test_graphql_parser_names02(self):
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

    def test_graphql_parser_names03(self):
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

    def test_graphql_parser_names04(self):
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

    def test_graphql_parser_names05(self):
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

    def test_graphql_parser_names06(self):
        r"""
        fragment name_on on on {id}
        fragment name_fragment on fragment {id}
        fragment name_query on query {id}
        fragment name_mutation on mutation {id}
        fragment name_subscription on subscription {id}
        fragment name_true on true {id}
        fragment name_false on false {id}
        fragment name_null on null {id}
        """

    def test_graphql_parser_names07(self):
        r"""
        fragment fragment on fragmentFoo {id}
        fragment query on queryFoo {id}
        fragment mutation on mutationFoo {id}
        fragment subscription on subscriptionFoo {id}
        fragment true on trueFoo {id}
        fragment false on falseFoo {id}
        fragment null on nullFoo {id}
        """

    def test_graphql_parser_names08(self):
        r"""
        query { ... on on {id} }
        query { ... on fragment {id} }
        query { ... on query {id} }
        query { ... on mutation {id} }
        query { ... on subscription {id} }
        query { ... on true {id} }
        query { ... on false {id} }
        query { ... on null {id} }
        """

    def test_graphql_parser_names09(self):
        r"""
        query { ... not_on on on {id} }
        query { ... fragment on fragmentFoo {id} }
        query { ... query on queryFoo {id} }
        query { ... mutation on mutationFoo {id} }
        query { ... subscription on subscriptionFoo {id} }
        query { ... true on trueFoo {id} }
        query { ... false on falseFoo {id} }
        query { ... null on nullFoo {id} }
        """

    def test_graphql_parser_names10(self):
        r"""
        query (
            $on: on on
            $fragment: fragment fragment
            $query: query query
            $mutation: mutation mutation
            $subscription: subscription subscription
            $true: true true
            $false: false false
            $null: null NULL
        ) {
            id
        }
        """

    def test_graphql_parser_names11(self):
        r"""
        query { ...someFragment @on }
        query { ...someFragment @fragment }
        query { ...someFragment @query }
        query { ...someFragment @mutation }
        query { ...someFragment @subscription }
        query { ...someFragment @true }
        query { ...someFragment @false }
        query { ...someFragment @null }
        """

    def test_graphql_parser_comments01(self):
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
