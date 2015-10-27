##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importkit.yaml.schema import CachingSchema

from metamagic.caos import proto as caos_proto
from metamagic.caos import protoschema

from .. import ExpressionText
from .semantics import Semantics
from .delta import Delta


SCHEMA_VERSION = 7


class Delta(Delta):
    @classmethod
    def get_tags(cls):
        return {
            '!expr': (
                ['tag:yaml.org,2002:str'],
                lambda loader, node: ExpressionText(node.value)
            )
        }


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

    @classmethod
    def get_schema_magic(cls):
        return SCHEMA_VERSION

    @classmethod
    def get_tags(cls):
        return {
            '!expr': (
                ['tag:yaml.org,2002:str'],
                lambda loader, node: ExpressionText(node.value)
            )
        }
