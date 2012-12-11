##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic import MetamagicError
from metamagic.utils.parsing import ParserError


class CaosQLError(MetamagicError):
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
