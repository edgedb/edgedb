##
# Copyright (c) 2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common.parsing import ParserError


class CaosQLError(EdgeDBError):
    pass


class CaosQLQueryError(ParserError, CaosQLError):
    pass


class CaosQLExpressionError(CaosQLError):
    pass


class CaosQLReferenceError(CaosQLError):
    def __init__(self, msg, *, hint=None, details=None, source=None, pointer=None):
        super().__init__(msg, hint=hint, details=details)
        self.source = source
        self.pointer = pointer
