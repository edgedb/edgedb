##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import validator


class YamlValidationError(Exception):
    pass


class Base(validator.Schema):
    def get_import_context_class(self):
        pass

    @classmethod
    def get_module_class(cls):
        raise NotImplementedError

    @classmethod
    def get_implicit_imports(cls):
        return ()

    @classmethod
    def normalize_code(cls, module_data, imports):
        pass


class ModuleSchemaBase(Base):
    pass


class Schema(Base):
    _schema_base_cls = Base

    def check(self, node):
        if not hasattr(node, 'tags'):
            node.tags = [node.tag]
        else:
            node.tags.append(node.tag)
        tag = 'tag:metamagic.sprymix.com,2009/metamagic/class/derive:{}.{}'
        node.tag = tag.format(self._schema_base_cls.__module__, self._schema_base_cls.__name__)
        return node


class ModuleSchema(Schema):
    _schema_base_cls = ModuleSchemaBase


class CachingSchema:
    pass
