##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import abc
import types


def decorate(wrapper, wrapped):
    for attr in ('__module__', '__name__', '__doc__', '__annotations__'):
        if hasattr(wrapped, attr):
            setattr(wrapper, attr, getattr(wrapped, attr))

    if isinstance(wrapped, types.FunctionType) and wrapped.__dict__:
        wrapper.__dict__.update(wrapped.__dict__)


class BaseDecorator:
    def __init__(self, func):
        self._func_ = func


class Decorator(BaseDecorator, metaclass=abc.ABCMeta):
    def __init__(self, func):
        self._func_ = func
        decorate(self, func)

        self._instance_caller = {}
        self._class_caller = {}

    def __get__(self, obj, cls=None):
        if obj:
            _id = id(obj)
            if _id not in self._instance_caller:
                self._instance_caller[_id] = lambda *args, **kwargs: \
                                                    self.instance_call(obj, *args, **kwargs)

                decorate(self._instance_caller[_id], self._func_)
            return self._instance_caller[_id]

        else:
            _id = id(cls)
            if _id not in self._class_caller:
                self._class_caller[_id] = lambda *args, **kwargs: \
                                                 self.class_call(cls, *args, **kwargs)
                decorate(self._class_caller[_id], self._func_)
            return self._class_caller[_id]

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def instance_call(self, instance, *args, **kwargs):
        return self(instance, *args, **kwargs)

    def class_call(self, cls, *args, **kwargs):
        return self(cls, *args, **kwargs)


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
