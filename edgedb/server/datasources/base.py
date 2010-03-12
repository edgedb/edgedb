##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils import type_utils


class DatasourceError(Exception):
    pass

class Datasource(object):
    @classmethod
    def init_class(cls, descriptor):
        cls.descriptor = descriptor


    def __init__(self):
        self.params = self.descriptor.get('params', None)


    def _filter_params(self, params):
        if self.params is None:
            return {}

        filtered = {}

        for name, config in self.params.items():
            value = None

            if name in params:
                value = params[name]
                if not type_utils.check(value, config['type']):
                    raise DatasourceError('datatype check failed, param: @name=%s, @value=%s, expected type: %s' %
                                          (name, value, config['type']))
            else:
                if 'default' in config:
                    value = config['default']
                else:
                    raise DatasourceError('expected required param: @name=%s' % name)

            filtered[name] = value

        return filtered


    def fetch(self, **params):
        raise NotImplemented
