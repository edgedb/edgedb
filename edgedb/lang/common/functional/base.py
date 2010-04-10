##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import abc
import types
import functools
import threading


_lock = threading.Lock()


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
            try:
                return obj._decorator_cache_[self][obj][self.__name__]

            except (KeyError, AttributeError):
                with _lock:
                    if not hasattr(obj, '_decorator_cache_'):
                        setattr(obj, '_decorator_cache_', {self: {obj: {}}})

                    elif self not in obj._decorator_cache_:
                        obj._decorator_cache_[self] = {obj: {}}

                    elif obj not in obj._decorator_cache_[self]:
                        obj._decorator_cache_[self][obj] = {}

                    if not self.__name__ in obj._decorator_cache_[self][obj]:
                        wrapper = functools.partial(self.instance_call, obj)
                        decorate(wrapper, self._func_)

                        obj._decorator_cache_[self][obj][self.__name__] = wrapper

                    return obj._decorator_cache_[self][obj][self.__name__]

        else:
            try:
                return cls._decorator_cache_[self][cls][self.__name__]

            except (KeyError, AttributeError):
                with _lock:
                    if not hasattr(cls, '_decorator_cache_'):
                        setattr(cls, '_decorator_cache_', {self: {cls: {}}})

                    elif self not in cls._decorator_cache_:
                        cls._decorator_cache_[self] = {cls: {}}

                    elif cls not in cls._decorator_cache_[self]:
                        cls._decorator_cache_[self][cls] = {}

                    if not self.__name__ in cls._decorator_cache_[self][cls]:
                        wrapper = functools.partial(self.class_call, cls)
                        decorate(wrapper, self._func_)

                        cls._decorator_cache_[self][cls][self.__name__] = wrapper

                    return cls._decorator_cache_[self][cls][self.__name__]

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
