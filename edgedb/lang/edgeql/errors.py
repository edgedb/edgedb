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
    def __str__(self):
        import edgedb.lang.common.markup
        res = self.args[0]
        return res + '\n' + edgedb.lang.common.markup.dumps(self.__sx_error_contexts__)


class EdgeQLExpressionError(EdgeQLError):
    pass


class EdgeQLReferenceError(EdgeQLError):
    def __init__(self, msg, *, hint=None, details=None, source=None, pointer=None):
        super().__init__(msg, hint=hint, details=details)
        self.source = source
        self.pointer = pointer
