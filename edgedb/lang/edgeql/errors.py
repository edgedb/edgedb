##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.parsing import ParserError


class EdgeQLError(EdgeDBError):
    pass


class EdgeQLQueryError(ParserError, EdgeQLError):
    pass


class EdgeQLExpressionError(EdgeQLError):
    pass


class EdgeQLReferenceError(EdgeQLError):
    def __init__(self, msg, *, hint=None, details=None, source=None, pointer=None):
        super().__init__(msg, hint=hint, details=details)
        self.source = source
        self.pointer = pointer
