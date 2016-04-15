##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .error import SchemaNameError
from metamagic.utils.algos.persistent_hash import persistent_hash


class SchemaName(str):
    __slots__ = ('module', 'name')

    def __new__(cls, name, module=None):
        if not name:
            raise NameError('name must not be empty')

        if isinstance(name, SchemaName):
            _name = name.name
            _module = name.module
        else:
            _module, _, _name = name.rpartition('.')

            if not _module:
                if not module:
                    err = 'improperly formed name: module is not specified: %s' % name
                    raise SchemaNameError(err)
                else:
                    _module = module

        result = super().__new__(cls, _module + '.' + _name)
        result.name = _name
        result.module = _module

        return result

    def __repr__(self):
        return '<SchemaName %s>' % self

    def persistent_hash(self):
        return persistent_hash(self)

    @staticmethod
    def is_qualified(name):
        return isinstance(name, SchemaName) or '.' in name
