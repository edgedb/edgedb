##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos.proto import ImportContext
from .semantics import Semantics
from .delta import Delta


class Semantics(Semantics):
    def get_import_context_class(self):
        return ImportContext
