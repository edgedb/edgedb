##
# Copyright (c) 2010-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBSyntaxError
from edgedb.lang.common.parsing import ParserError


class EdgeQLError(EdgeDBSyntaxError):
    code = '42601'


class EdgeQLSyntaxError(ParserError, EdgeDBSyntaxError):
    code = '42602'


class EdgeQLExpressionError(EdgeQLError):
    pass


class EdgeQLReferenceError(EdgeQLError):
    def __init__(self, msg, *, hint=None, details=None, source=None,
                 pointer=None, context=None):
        super().__init__(msg, hint=hint, details=details, context=context)
        self.source = source
        self.pointer = pointer
