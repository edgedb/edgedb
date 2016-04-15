##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.exceptions import MetamagicError


class SchemaError(MetamagicError):
    def __init__(self, msg=None, *, hint=None, details=None, context=None):
        super().__init__(msg, hint=hint, details=details)
        self.context = context


class SchemaNameError(SchemaError):
    pass


class NoPrototypeError(SchemaError):
    pass
