##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import proto as caos_proto
from semantix.utils.lang import protoschema

from .semantics import Semantics
from .delta import Delta


class Semantics(Semantics):
    def get_import_context_class(self):
        return caos_proto.ImportContext

    @classmethod
    def get_module_class(cls):
        return caos_proto.SchemaModule
