##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError


class GraphQLError(EdgeDBError):
    pass


class GraphQLValidationError(GraphQLError):
    pass
