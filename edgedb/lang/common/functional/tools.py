##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc
import types
import inspect
import functools
import threading


__all__ = ['get_argsspec', 'apply_decorator', 'decorate', 'Decorator', 'BaseDecorator']


def decorate(wrapper, wrapped):
    for attr in ('__module__', '__name__', '__doc__'):
        if hasattr(wrapped, attr):
            setattr(wrapper, attr, getattr(wrapped, attr))

    if isinstance(wrapped, types.FunctionType):
        if wrapped.__dict__:
            wrapper.__dict__.update(wrapped.__dict__)

        if hasattr(wrapped, '_args_spec_'):
            setattr(wrapper, '_args_spec_', getattr(wrapped, '_args_spec_'))

        else:
            setattr(wrapper, '_args_spec_', inspect.getfullargspec(wrapped))

        setattr(wrapper, '_func_', wrapped)


class BaseDecorator:
    def __init__(self, func):
        self._func_ = func


_lock = threading.Lock()
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


def get_argsspec(func):
    try:
        return getattr(func, '_args_spec_')
    except AttributeError:
        return inspect.getfullargspec(func)


def apply_decorator(func, *, decorate_function=None, decorate_class=None):
    if inspect.isfunction(func):
        if decorate_function:
            return decorate_function(func)
        else:
            raise TypeError('Unable to decorate function %s' % func.__name__)

    if inspect.isclass(func):
        if decorate_class:
            return decorate_class(func)
        else:
            raise TypeError('Unable to decorate class %s' % func.__name__)

    if isinstance(func, classmethod):
        return classmethod(apply_decorator(func.__func__, decorate_function=decorate_function,
                                           decorate_class=decorate_class))

    if isinstance(func, staticmethod):
        return staticmethod(apply_decorator(func.__func__, decorate_function=decorate_function,
                                            decorate_class=decorate_class))

    if isinstance(func, property):
        funcs = []
        for name in 'fget', 'fset', 'fdel':
            f = getattr(func, name, None)
            if f:
                f = apply_decorator(f, decorate_function=decorate_function,
                                    decorate_class=decorate_class)
            funcs.append(f)
        return property(*funcs)

    if isinstance(func, BaseDecorator):
        top = func
        while isinstance(func, BaseDecorator):
            host = func
            func = func._func_
        host._func_ = apply_decorator(host._func_, decorate_function=decorate_function,
                                      decorate_class=decorate_class)
        return top

    return func
