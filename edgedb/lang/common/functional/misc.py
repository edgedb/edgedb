##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .tools import *


__all__ = ['hybridmethod', 'cachedproperty']


class hybridmethod(Decorator):
    def __call__(self, *args, **kwargs):
        return self._func_(*args, **kwargs)


class cachedproperty(BaseDecorator):
    def __init__(self, func):
        super().__init__(func)
        self.__name__ = func.__name__

    def __get__(self, obj, cls=None):
        assert obj
        value = self._func_(obj)
        obj.__dict__[self.__name__] = value
        return value
