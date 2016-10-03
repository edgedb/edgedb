##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import copyreg
import uuid

from importkit import meta as lang_meta
from importkit.import_ import get_object


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


class DatasourceMeta(type):
    pass


class Datasource(metaclass=DatasourceMeta):
    @classmethod
    def prepare_class(cls, context, descriptor):
        cls.descriptor = descriptor

    def __init__(self):
        self.params = self.descriptor.get('params', None)

    def describe_output(self):
        raise NotImplementedError

    def check_type(self, name, value, type):
        if check_type(value, type):
            return value
        else:
            raise ValueError('invalid parameter type')

    def _filter_params(self, params, filters=None):
        if self.params is None:
            return {}

        filtered = {}

        for name, config in self.params.items():
            value = None

            if name in params:
                value = params[name]

                try:
                    value = self.check_type(name, value, config['type'])
                except ValueError as e:
                    raise DatasourceError(
                        'datatype check failed, param: @name={}, @value={}, '
                        'expected type: {}'.format(
                            name, value, config['type'])) from e
            else:
                if 'default' in config:
                    value = self.coerce_default_value(
                        name, config['default'], config['type'])
                else:
                    raise DatasourceError(
                        'expected required param: @name=%s' % name)

            filtered[name] = value

        if filters:
            extra_filters = filters.copy()

            for name, value in filters.items():
                if name not in self.params:
                    filtered['__filter%s' % name] = value
                else:
                    filtered[name] = self.check_type(
                        name, value, self.params[name]['type'])
                    del extra_filters[name]
        else:
            extra_filters = None

        return filtered, extra_filters

    def fetch(self, *, _filters=None, _sort=None, **params):
        raise NotImplementedError

    def coerce_default_value(self, name, value, type):
        return value


def _restore_datasource(metacls, name, module, bases, scls):
    bases = tuple(get_object(b) for b in bases)
    dct = metacls.__prepare__(name, bases)
    dct['__module__'] = module
    result = metacls(name, bases, dct)
    result.descriptor = scls
    return result


def reduce_datasource(cls, restore=_restore_datasource):
    bases = tuple(
        '{}.{}'.format(b.__module__, b.__name__) for b in cls.__bases__)
    mro = type(cls).__mro__
    for metacls in mro:
        if not issubclass(metacls, lang_meta.Object):
            break
    else:
        raise TypeError('{!r} is not a field component class')
    return restore, (
        metacls, cls.__name__, cls.__module__, bases, cls.descriptor)


copyreg.pickle(DatasourceMeta, reduce_datasource)


class Result:
    def __init__(self, result, total_records):
        self.result = result
        self.total_records = total_records

    def __iter__(self):
        return iter(self.result)

    def get_total(self):
        return self.total_records

    def __mm_serialize__(self):
        return list(self.result)
