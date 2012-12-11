##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.caos import proto as caos_proto
from metamagic.utils.lang import protoschema
from metamagic.utils.lang.yaml.schema import CachingSchema

from .semantics import Semantics
from .delta import Delta


class Semantics(Semantics, CachingSchema):
    def get_import_context_class(self):
        return caos_proto.ImportContext

    @classmethod
    def get_module_class(cls):
        return caos_proto.SchemaModule

    @classmethod
    def normalize_code(cls, module_data, imports):
        protomod = dict(module_data)['__sx_prototypes__']
        schema = caos_proto.get_global_proto_schema()
        protomod.normalize(imports)

    @classmethod
    def get_implicit_imports(cls):
        # cls.get_module_class().get_schema_class().get_builtins_module()
        return ('metamagic.caos.builtins',)
