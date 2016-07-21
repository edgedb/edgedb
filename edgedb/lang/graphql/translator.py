##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.graphql import ast as gqlast
from edgedb.lang.graphql import parser as gqlparser


class GraphQLTranslator:
    def translate(self, gqltree):
        import edgedb.lang.common.markup
        edgedb.lang.common.markup.dump(gqltree)
        1/0


def translate(graphql):
    parser = gqlparser.GraphQLParser()
    gqltree = parser.parse(graphql)
    return GraphQLTranslator().translate(gqltree)
