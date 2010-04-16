##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib

from .map import MappingType
from ..error import SchemaValidationError


class ClassType(MappingType):
    def __init__(self, schema):
        super().__init__(schema)

    def load(self, dct):
        dct['mapping'] = {'=': {'type': 'map', 'required': True, 'mapping': dct['fields']}}
        dct['min-length'] = 1
        dct['max-length'] = 1
        super().load(dct)

    def check(self, node):
        clsname = node.value[0][0].value

        try:
            mod, _, name = clsname.rpartition('.')
            cls = getattr(importlib.import_module(mod), name)
        except (ImportError, AttributeError):
            raise SchemaValidationError('could not find class %s' % clsname, node)

        if hasattr(cls, 'get_yaml_validator_config'):
            config = cls.get_yaml_validator_config()
            if config:
                self.keys['=']['type'].load_keys(config)

        node = super().check(node)

        clsdict = node.value[0][1]

        tag = 'tag:semantix.sprymix.com,2009/semantix/object/create:%s' % clsname
        self.push_tag(clsdict, tag)

        return clsdict
