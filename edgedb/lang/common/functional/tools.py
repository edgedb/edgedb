##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import abc
import sys
import types
import inspect
import weakref

from semantix.exceptions import SemantixError


__all__ = ['get_argsspec', 'apply_decorator', 'decorate', 'isdecorated',
           'Decorator', 'BaseDecorator', 'NonDecoratable', 'callable',
           'unwrap']


class NonDecoratable:
    pass


WRAPPER_ASSIGNMENTS = {'__module__', '__name__', '__doc__', '__annotations__'}
def decorate(wrapper, wrapped, *, assigned=WRAPPER_ASSIGNMENTS):
    if isinstance(wrapped, type) and issubclass(wrapped, NonDecoratable):
        raise TypeError('Unable to decorate %r as a subclass of NonDecoratable' % wrapped)

    elif isinstance(wrapped, NonDecoratable):
        raise TypeError('Unable to decorate %r as an instance of NonDecoratable' % wrapped)

    for attr in assigned:
        if hasattr(wrapped, attr):
            setattr(wrapper, attr, getattr(wrapped, attr))

    if isinstance(wrapped, types.FunctionType):
        if wrapped.__dict__:
            wrapper.__dict__.update(wrapped.__dict__)

    wrapper.__wrapped__ = wrapped
    return wrapper


def callable(obj):
    return any('__call__' in cls.__dict__ for cls in type(obj).__mro__)


def isdecorated(func):
    return callable(func) and (isinstance(func, BaseDecorator) or hasattr(func, '__wrapped__'))


class BaseDecorator(metaclass=abc.ABCMeta):
    def __init__(self, func):
        self.__wrapped__ = func

BaseDecorator.register(staticmethod)
BaseDecorator.register(classmethod)


_marker = object()
class Decorator(BaseDecorator):
    _cache = weakref.WeakKeyDictionary()

    def __new__(cls, func=_marker, *args, __completed__=False, **kwargs):
        if not __completed__ and func is not _marker and callable(func) and (args or kwargs):
            original_function = unwrap(func, True)
            frame = sys._getframe(1)
            try:
                while frame and frame.f_code.co_filename != original_function.__code__.co_filename:
                    frame = frame.f_back

                if frame and frame.f_lineno >= original_function.__code__.co_firstlineno:
                    __completed__ = True

            finally:
                del frame

        if __completed__ or (not args and not kwargs and callable(func)):
            try:
                decorated = cls.decorate(func, *args, **kwargs)
            except NotImplementedError:
                pass
            else:
                if decorated is not None:
                    return decorated
            return super().__new__(cls)

        if func is not _marker:
            args = (func,) + args

        return (lambda func: cls(func, *args, __completed__=True, **kwargs))

    @classmethod
    def decorate(cls, func, *args, **kwargs):
        raise NotImplementedError

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

            decorate(wrapper, self.__wrapped__)

            Decorator._cache[target][self, self.__name__] = wrapper
            return wrapper

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def instance_call(self, instance, *args, **kwargs):
        return self(instance, *args, **kwargs)

    def class_call(self, cls, *args, **kwargs):
        return self(cls, *args, **kwargs)


def unwrap(func, deep=False):
    def _unwrap(func):
        if not isdecorated(func):
            raise TypeError('function %r is not decorated' % func)

        try:
            return func.__wrapped__
        except AttributeError:
            try:
                return func.__func__
            except AttributeError:
                pass

        raise TypeError('unable to unwrap decorated function %r' % func)

    if deep:
        while isdecorated(func):
            func = _unwrap(func)
        return func

    return _unwrap(func)


def get_argsspec(func):
    if isdecorated(func):
        func = unwrap(func, True)
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
        return classmethod(apply_decorator(func.__func__,
                                           decorate_function=decorate_function,
                                           decorate_class=decorate_class))

    if isinstance(func, staticmethod):
        return staticmethod(apply_decorator(func.__func__,
                                            decorate_function=decorate_function,
                                            decorate_class=decorate_class))

    if isinstance(func, property):
        funcs = []
        for name in 'fget', 'fset', 'fdel':
            f = getattr(func, name, None)
            if f:
                f = apply_decorator(f,
                                    decorate_function=decorate_function,
                                    decorate_class=decorate_class)
            funcs.append(f)
        return property(*funcs)

    if isinstance(func, BaseDecorator):
        top = func
        while isinstance(func, BaseDecorator):
            host = func
            func = func.__wrapped__
        host.__wrapped__ = apply_decorator(host.__wrapped__,
                                           decorate_function=decorate_function,
                                           decorate_class=decorate_class)
        return top

    return func
