##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError, add_context


class GraphQLError(EdgeDBError):
    pass


class GraphQLValidationError(GraphQLError):
    def __init__(self, msg, *, context=None):
        super().__init__(msg)

        if context:
            add_context(self, context)
            self.line = context.start.line
            self.col = context.start.column
        else:
            self.line = self.col = self.context = None
