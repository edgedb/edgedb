##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common.exceptions import EdgeDBError


class SchemaError(EdgeDBError):
    def __init__(self, msg=None, *, hint=None, details=None, context=None):
        super().__init__(msg, hint=hint, details=details)
        self.context = context


class SchemaNameError(SchemaError):
    pass


class NoClassError(SchemaError):
    pass
