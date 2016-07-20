##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.graphql import _testbase as tb
from edgedb.lang.graphql.parser.errors import (GraphQLParserError,
                                               GraphQLUniquenessError)
from edgedb.lang.common.lexer import UnknownTokenError


class TestGraphQLParser(tb.ParserTest):
    def test_graphql_parser_empty01(self):
        """"""

    @tb.must_fail(UnknownTokenError, line=1, col=1)
    def test_graphql_parser_empty02(self):
        """\v"""

    @tb.must_fail(UnknownTokenError, line=1, col=1)
    def test_graphql_parser_empty03(self):
        """\f"""

    @tb.must_fail(UnknownTokenError, line=1, col=1)
    def test_graphql_parser_empty04(self):
        """\xa0"""

    @tb.must_fail(UnknownTokenError, line=2, col=1)
    def test_graphql_parser_empty05(self):
        """\r\n;"""

    @tb.must_fail(UnknownTokenError, line=1, col=1)
    def test_graphql_parser_empty06(self):
        '''"'''

    @tb.must_fail(UnknownTokenError, line=2, col=9)
    def test_graphql_parser_empty07(self):
        """
        "
        "
        """

    @tb.must_fail(GraphQLParserError, line=1, col=1)
    def test_graphql_parser_empty08(self):
        """..."""

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string01(self):
        # XXX: the string isn't parsed, but the error is too obscure
        """
        { field(arg:"\b") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string02(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\x") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string03(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\u1") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string04(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\u0XX1") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string05(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\uXXXX") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string06(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\uFXXX") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=21)
    def test_graphql_parser_string07(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\uXXXF") }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=34)
    def test_graphql_parser_string08(self):
        # XXX: the string isn't parsed, but the error is too obscure
        R"""
        { field(arg:"\uFEFF\n") };
        """

    def test_graphql_parser_short01(self):
        """{id}"""

    def test_graphql_parser_short02(self):
        """
        {id, name, description}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_short03(self):
        """
        {id}
        {name}
        """

    @tb.must_fail(GraphQLParserError, line=3, col=9)
    def test_graphql_parser_short04(self):
        """
        query {id}
        {name}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=18)
    def test_graphql_parser_short05(self):
        """
        { field: {} }
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

        { ... friendFields }
        """

    def test_graphql_parser_fragment02(self):
        """
        fragment friendFields on User @skip(if: false), @include(if: true) {
            id
            name
            profilePic(size: 50)
        }

        { ... friendFields }
        """

    def test_graphql_parser_fragment03(self):
        """
        fragment someFields on User { id }

        {
            ...someFields @skip(if: true)
        }
        """

    def test_graphql_parser_fragment04(self):
        """
        fragment someFields on User { id }

        {
            ...someFields @skip(if: true), @include(if: false)
        }
        """

    @tb.must_fail(GraphQLParserError, line=3, col=28)
    def test_graphql_parser_fragment05(self):
        """
        { ...MissingOn }
        fragment MissingOn Type {name}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_fragment06(self):
        """
        {...Missing}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_fragment07(self):
        """
        fragment Missing on Type {name}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_fragment08(self):
        """
        fragment cyclceFrag on Type {
            ...cyclceFrag
        }

        {... cyclceFrag}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_fragment09(self):
        """
        fragment cyclceFrag on Type {
            ...otherFrag
        }

        fragment otherFrag on Type {
            ...cyclceFrag
        }

        {... cyclceFrag}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_fragment10(self):
        """
        fragment A on Type {...B}
        fragment B on Type {...C}
        fragment C on Type {...D}
        fragment D on Type {...A}

        {... C}
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

    @tb.must_fail(GraphQLParserError, line=2, col=23)
    def test_graphql_parser_query05(self):
        r"""
        query myquery on type { field }
        """

    @tb.must_fail(UnknownTokenError, line=2, col=32)
    def test_graphql_parser_query06(self):
        r"""
        query myquery { field };
        """

    @tb.must_fail(UnknownTokenError, line=2, col=25)
    def test_graphql_parser_query07(self):
        r"""
        query myQuery { \a }
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_query08(self):
        """
        notanoperation Foo { field }
        """

    @tb.must_fail(GraphQLUniquenessError, line=3, col=9)
    def test_graphql_parser_query09(self):
        """
        query myQuery { id }
        query myQuery { id }
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

    @tb.must_fail(GraphQLUniquenessError, line=3, col=9)
    def test_graphql_parser_mutation04(self):
        """
        mutation myQuery { id }
        query myQuery { id }
        """

    def test_graphql_parser_subscription01(self):
        """
        subscription {
            id
            name
        }
        """

    @tb.must_fail(GraphQLUniquenessError, line=3, col=9)
    def test_graphql_parser_subscription02(self):
        """
        mutation myQuery { id }
        subscription myQuery { id }
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
                    more: "",
                    description: "\\\/\b\f\n\r\t 'blah' спам")
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
        query getZuckProfile($devicePicSize: Int = 42) {
            user(id: 4) {
                id
                name
                profilePic(size: $devicePicSize)
            }
        }
        """

    def test_graphql_parser_values12(self):
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

    @tb.must_fail(GraphQLParserError, line=3, col=21)
    def test_graphql_parser_values13(self):
        r"""
        {
            foo(id: null) {
                id
                bar(param: NULL)
            }
        }
        """

    @tb.must_fail(GraphQLParserError, line=5, col=28)
    def test_graphql_parser_values14(self):
        r"""
        {
            foo(id: NULL) {
                id
                bar(param: null)
            }
        }
        """

    def test_graphql_parser_values15(self):
        r"""
        query myQuery($var: Int) {
            field(complex: { a: { b: [ $var ] } })
        }
        """

    def test_graphql_parser_values16(self):
        r"""
        query Foo($x: Complex = { a: { b: [ "var" ] } }) {
            field
        }
        """

    @tb.must_fail(GraphQLParserError, line=2, col=45)
    def test_graphql_parser_values17(self):
        r"""
        query Foo($x: Complex = { a: { b: [ $var ] } }) {
            field
        }
        """

    @tb.must_fail(GraphQLParserError, line=3, col=49)
    def test_graphql_parser_values18(self):
        r"""
        {
            fieldWithNullableStringInput(input: null)
        }
        """

    @tb.must_fail(UnknownTokenError, line=3, col=49)
    def test_graphql_parser_values19(self):
        r"""
        {
            fieldWithNullableStringInput(input: .123)
        }
        """

    @tb.must_fail(UnknownTokenError, line=3, col=49)
    def test_graphql_parser_values20(self):
        r"""
        {
            fieldWithNullableStringInput(input: 0123)
        }
        """

    @tb.must_fail(UnknownTokenError, line=3, col=49)
    def test_graphql_parser_values21(self):
        r"""
        {
            fieldWithNullableStringInput(input: +123)
        }
        """

    @tb.must_fail(GraphQLParserError, line=2, col=9)
    def test_graphql_parser_scope01(self):
        r"""
        {
            fieldWithNullableStringInput(input: $var)
        }
        """

    def test_graphql_parser_scope02(self):
        r"""
        fragment goodVar on User {name(first: $var)}

        query ($var: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
        }
        """

    @tb.must_fail(GraphQLParserError, line=5, col=9)
    def test_graphql_parser_scope03(self):
        r"""
        fragment goodVar on User {name(first: $var)}
        fragment badVar on User {name(first: $bad)}

        query ($var: String) {
            fieldWithNullableStringInput(input: $var)
            ... goodVar
            ... badVar
        }
        """

    @tb.must_fail(GraphQLParserError, line=12, col=9)
    def test_graphql_parser_scope04(self):
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

    def test_graphql_parser_scope05(self):
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

    @tb.must_fail(GraphQLParserError, line=16, col=9)
    def test_graphql_parser_scope06(self):
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
        query badQuery {
            ... midVar
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

    def test_graphql_parser_names07(self):
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
        # fragment not_on on Foo {name}
        # fragment fragment on Foo {name}
        # fragment query on Foo {name}
        # fragment mutation on Foo {name}
        # fragment subscription on Foo {name}
        # fragment true on Foo {name}
        fragment false on Foo {name}
        fragment null on Foo {name}

        # query { ... not_on on on {id} }
        # query { ... fragment on fragmentFoo {id} }
        # query { ... query on queryFoo {id} }
        # query { ... mutation on mutationFoo {id} }
        # query { ... subscription on subscriptionFoo {id} }
        # query { ... true on trueFoo {id} }
        query { ... false on falseFoo {id} }
        query { ... null on nullFoo {id} }
        """

    def test_graphql_parser_names10(self):
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

    def test_graphql_parser_names11(self):
        r"""
        fragment someFragment on Foo {id}

        query { ...someFragment @on }
        query { ...someFragment @fragment }
        query { ...someFragment @query }
        query { ...someFragment @mutation }
        query { ...someFragment @subscription }
        query { ...someFragment @true }
        query { ...someFragment @false }
        query { ...someFragment @null }
        """

    @tb.must_fail(GraphQLParserError, line=2, col=21)
    def test_graphql_parser_names12(self):
        r"""
        { ... on on on {id} }
        """

    @tb.must_fail(GraphQLParserError, line=2, col=18)
    def test_graphql_parser_names13(self):
        r"""
        fragment on on on {id}
        """

    @tb.must_fail(GraphQLParserError, line=2, col=18)
    def test_graphql_parser_names14(self):
        r"""
        { ... on }
        """

    @tb.must_fail(GraphQLUniquenessError, line=2, col=32)
    def test_graphql_parser_names15(self):
        r"""
        query myQuery($x: Int, $x: Int) { id }
        """

    @tb.must_fail(GraphQLUniquenessError, line=2, col=32)
    def test_graphql_parser_names16(self):
        r"""
        query myQuery($x: Int, $x: Float) { id }
        """

    @tb.must_fail(GraphQLUniquenessError, line=3, col=23)
    def test_graphql_parser_names17(self):
        r"""
        {
            foo(x: 1, x: 2)
        }
        """

    @tb.must_fail(GraphQLUniquenessError, line=3, col=23)
    def test_graphql_parser_names18(self):
        r"""
        {
            foo(x: 1, x: "one")
        }
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
