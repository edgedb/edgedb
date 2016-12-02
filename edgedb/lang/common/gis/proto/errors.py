##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.exceptions import EdgeDBError


class GeometryError(EdgeDBError):
    def __init__(self, msg=None, *, hint=None, details=None, errvalue=None):
        super().__init__(msg, hint=hint, details=details)
        self.errvalue = errvalue
