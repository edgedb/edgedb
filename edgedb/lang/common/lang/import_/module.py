##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types


class Module(types.ModuleType):
    pass


class BaseProxyModule:
    def __init__(self, name, module):
        self.__name__ = name
        self.__wrapped__ = module


class LightProxyModule(BaseProxyModule):
    """Light ProxyModule object, does not keep track of wrapped
    module's attributes, so if there are any references to them in
    the code then it may be broken after reload.
    """

    def __setattr__(self, name, value):
        if name not in ('__name__', '__wrapped__'):
            return setattr(self.__wrapped__, name, value)
        else:
            return object.__setattr__(self, name, value)

    def __getattribute__(self, name):
        if name in ('__name__', '__repr__', '__wrapped__'):
            return object.__getattribute__(self, name)

        wrapped = object.__getattribute__(self, '__wrapped__')
        return getattr(wrapped, name)

    def __repr__(self):
        return '<%s "%s">' % (object.__getattribute__(self, '__class__').__name__, self.__name__)


class ModuleInfo:
    def __init__(self, module):
        for attr in ('__name__', '__package__', '__path__', '__file__'):
            setattr(self, attr, getattr(module, attr, None))
