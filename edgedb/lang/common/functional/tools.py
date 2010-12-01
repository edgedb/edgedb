##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
import inspect
import functools
import weakref

from semantix.utils import abc
from semantix.exceptions import SemantixError


__all__ = ['descent_decoration', 'get_argsspec', 'apply_decorator', 'decorate', 'isdecorated',
           'Decorator', 'BaseDecorator', 'NonDecoratable', 'callable']


class NonDecoratable:
    pass


def decorate(wrapper, wrapped):
    if isinstance(wrapped, type) and issubclass(wrapped, NonDecoratable):
        raise SemantixError('Unable to decorate %r as a subclass of NonDecoratable' % wrapped)

    elif isinstance(wrapped, NonDecoratable):
        raise SemantixError('Unable to decorate %r as an instance of NonDecoratable' % wrapped)

    for attr in ('__module__', '__name__', '__doc__'):
        if hasattr(wrapped, attr):
            setattr(wrapper, attr, getattr(wrapped, attr))

    if isinstance(wrapped, types.FunctionType):
        if wrapped.__dict__:
            wrapper.__dict__.update(wrapped.__dict__)

        if hasattr(wrapped, '_args_spec_'):
            setattr(wrapper, '_args_spec_', wrapped._args_spec_)

        else:
            setattr(wrapper, '_args_spec_', inspect.getfullargspec(wrapped))

        if not hasattr(wrapper, '_func_'):
            setattr(wrapper, '_func_', wrapped)


def callable(obj):
    return any('__call__' in cls.__dict__ for cls in type(obj).__mro__)


def isdecorated(func):
    return (isinstance(func, types.FunctionType) and hasattr(func, '_args_spec_') \
                                                            and hasattr(func, '_func_')) \
            or isinstance(func, BaseDecorator)


class BaseDecorator:
    def __init__(self, func):
        self._func_ = func


_marker = object()
class Decorator(BaseDecorator, metaclass=abc.AbstractMeta):
    _cache = weakref.WeakKeyDictionary()

    def __new__(cls, func=_marker, *args, __completed__=False, **kwargs):
        if __completed__ or (not args and not kwargs and callable(func)):
            return super().__new__(cls)

        if func is not _marker:
            args = (func,) + args

        return cls.decorate(args, kwargs)

    @classmethod
    def decorate(cls, args, kwargs):
        def wrap(func):
            return cls(func, *args, __completed__=True, **kwargs)

        return wrap

    def __init__(self, func, *args, __completed__=None, **kwargs):
        BaseDecorator.__init__(self, func)

        if args or kwargs:
            self.handle_args(*args, **kwargs)

        decorate(self, func)

    def handle_args(self, *args, **kwargs):
        raise SemantixError('decorator %r does not support any arguments' % self.__class__.__name__)

    def __get__(self, obj, cls=None):
        if obj:
            target = obj
            method = self.instance_call

        else:
            target = cls
            method = self.class_call

        try:
            return Decorator._cache[target][self, self.__name__]

        except KeyError:
            if target not in Decorator._cache:
                Decorator._cache[target] = {}

            targetref = weakref.ref(target)
            def wrapper(*args, **kwargs):
                return method(targetref(), *args, **kwargs)

            decorate(wrapper, self._func_)

            Decorator._cache[target][self, self.__name__] = wrapper
            return wrapper

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


def descent_decoration(obj, *, on_function=None, on_class=None, _short=False):
    assert on_function or on_class

    if inspect.isfunction(obj):
        if _short:
            on_function(obj)
        else:
            if hasattr(obj, '_func_') and on_function:
                on_function(obj._func_)
        return

    if inspect.isclass(obj) and on_class:
        on_class(obj)
        return

    if isinstance(obj, classmethod) or isinstance(obj, staticmethod):
        descent_decoration(obj.__func__, on_function=on_function, on_class=on_class, _short=True)
        return

    if isinstance(obj, property):
        for name in 'fget', 'fset', 'fdel':
            _obj = getattr(obj, name, None)
            if _obj:
                descent_decoration(_obj, on_function=on_function, on_class=on_class, _short=True)
        return

    if isinstance(obj, BaseDecorator):
        descent_decoration(obj._func_, on_function=on_function, on_class=on_class, _short=True)
        return


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
