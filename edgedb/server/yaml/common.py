##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.lang import yaml
from semantix.utils.datastructures import StructMeta


class StructMeta(StructMeta):
    def get_yaml_validator_config(cls):
        result = {}

        for f, desc in cls._fields.items():
            field = {}

            types = cls.get_yaml_descriptors(desc.type)

            if len(types) == 1:
                field = types[0]
            else:
                field['type'] = 'choice'
                field['choice'] = types

            result[f] = field

        return result

    def get_yaml_descriptors(cls, types):
        result = []

        for type in types:
            field = {}

            if issubclass(type, yaml.Object):
                field['type'] = 'map'
                field['object'] = '%s.%s' % (type.__module__, type.__name__)

                if hasattr(type, 'get_yaml_validator_config'):
                    field['mapping'] = type.get_yaml_validator_config()
                else:
                    field['mapping'] = {'=': {'type': 'any'}}
            elif issubclass(type, int):
                field = {'type': 'int'}
            elif issubclass(type, str):
                field = {'type': 'str'}
            else:
                field = {'type': 'any'}

            result.append(field)

        return result

    def represent(cls, data):
        result = {}

        for f, desc in cls._fields.items():
            result[f] = getattr(data, f)

        return result
