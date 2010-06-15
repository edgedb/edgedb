##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid


from semantix.caos import types as caos_types


def check_type(variable, type):
    if not isinstance(type, str):
        raise Exception('check_type: type parameter must be string')

    if variable is None:
        return True

    if type == 'str':
        return isinstance(variable, str)

    if type == 'int':
        return isinstance(variable, int)

    if type == 'float':
        return isinstance(variable, float)

    if type == 'bool':
        return isinstance(variable, bool)

    if type == 'list':
        return isinstance(variable, list)

    if type == 'uuid':
        return isinstance(variable, uuid.UUID)

    if type == 'none':
        return variable is None

    raise Exception('check_type: checking on unknown type: %s' % type)


class DatasourceError(Exception):
    pass


class Datasource(object):
    @classmethod
    def prepare_class(cls, context, descriptor):
        cls.descriptor = descriptor

    def __init__(self):
        self.params = self.descriptor.get('params', None)

    def describe_output(self):
        raise NotImplementedError

    def check_type(self, value, type):
        return check_type(value, type)

    def _filter_params(self, params, filters=None):
        if self.params is None:
            return {}

        filtered = {}

        for name, config in self.params.items():
            value = None

            if name in params:
                value = params[name]
                if not self.check_type(value, config['type']):
                    raise DatasourceError('datatype check failed, param: @name=%s, @value=%s, expected type: %s' %
                                          (name, value, config['type']))
            else:
                if 'default' in config:
                    value = config['default']
                else:
                    raise DatasourceError('expected required param: @name=%s' % name)

            filtered[name] = value

        if filters:
            for name, value in filters.items():
                filtered['__filter%s' % name] = value

        return filtered

    def fetch(self, *, _filters=None, _sort=None, **params):
        raise NotImplementedError


class CaosDatasource(Datasource):
    def __init__(self, session):
        super().__init__()
        self.session = session

    def check_type(self, value, type):
        if type == 'any' or value is None:
            return True

        type = self.session.schema.get(type)
        if isinstance(value, type):
            return True
        elif isinstance(type, caos_types.AtomClass) and issubclass(type, value.__class__):
            return True
        return False

