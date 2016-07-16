##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.parsing import ParserError
from edgedb.lang.graphql.errors import GraphQLError


class GraphQLParserError(GraphQLError, ParserError):
    pass
