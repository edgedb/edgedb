##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.exceptions import MetamagicError


class GeometryError(MetamagicError):
    def __init__(self, msg=None, *, hint=None, details=None, errvalue=None):
        super().__init__(msg, hint=hint, details=details)
        self.errvalue = errvalue
