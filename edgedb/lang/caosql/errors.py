##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import SemantixError
from semantix.utils.parsing import ParserError


class CaosQLError(SemantixError):
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
