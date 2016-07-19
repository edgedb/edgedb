##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.parsing import ParserError
from edgedb.lang.graphql.errors import GraphQLError


class GraphQLParserError(GraphQLError, ParserError):
    @classmethod
    def from_parsed(cls, msg, node):
        return GraphQLParserError(msg.format(node), context=node.context)


class GraphQLUniquenessError(GraphQLParserError):
    @classmethod
    def from_ast(cls, node, entity=None):
        if entity is None:
            entity = node.__class__.__name__.lower()

        return GraphQLUniquenessError(
            "{} with name '{}' already exists".format(entity, node.name),
            context=node.context)
