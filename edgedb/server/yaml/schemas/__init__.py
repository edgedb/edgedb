##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from importkit.yaml.schema import CachingSchema

from metamagic.caos.schemaloaders import import_ as sl

from .. import ExpressionText
from .semantics import Semantics
from .delta import Delta


SCHEMA_VERSION = 11


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
    @classmethod
    def get_module_class(cls):
        return sl.SchemaModule

    @classmethod
    def normalize_code(cls, module_data, imports):
        protomod = dict(module_data)['__sx_prototypes__']
        protomod.normalize(imports)

    @classmethod
    def get_implicit_imports(cls):
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
