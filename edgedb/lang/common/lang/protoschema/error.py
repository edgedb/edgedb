##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import SemantixError


class SchemaError(SemantixError):
    def __init__(self, msg=None, *, hint=None, details=None, context=None):
        super().__init__(msg, hint=hint, details=details)
        self.context = context


class SchemaNameError(SchemaError):
    pass
