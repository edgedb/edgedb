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
from types import MethodType as _method
from functools import partial

from semantix.exceptions import SemantixError
from .signature import signature as _signature


__all__ = ('get_argsspec', 'apply_decorator', 'decorate', 'isdecorated',
           'Decorator', 'BaseDecorator', 'NonDecoratable', 'get_signature',
           'unwrap', 'hybridmethod', 'cachedproperty', 'in_class')


class NonDecoratable:
    pass


def in_class():
    frame = sys._getframe()
    try:
        while frame.f_back:
            frame = frame.f_back

            if type(frame.f_locals.get('__locals__')) is dict and \
                    type(frame.f_locals.get('__module__')) is str and \
                    frame.f_locals is not frame.f_globals:

                return True
    finally:
        del frame

    return False


def get_signature(func):
    try:
        while func.__wrapped__:
            func = func.__wrapped__
    except AttributeError:
        pass

    return _signature(func)


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


def isdecorated(func):
    return (callable(func) \
                and (hasattr(func, '__wrapped__') or hasattr(func, '__func__') \
                                                        or isinstance(func, BaseDecorator))) or \
            isinstance(func, (staticmethod, classmethod))


class BaseDecorator(metaclass=abc.ABCMeta):
    __slots__ = ('__wrapped__',)

    def __init__(self, func):
        self.__wrapped__ = func


_marker = object()
class Decorator(BaseDecorator):
    def __new__(cls, func=_marker, *args, __sx_completed__=False, **kwargs):
        if not __sx_completed__ and func is not _marker and callable(func) and (args or kwargs):
            original_function = unwrap(func)
            frame = sys._getframe(1)
            try:
                while frame and frame.f_code.co_filename != original_function.__code__.co_filename:
                    frame = frame.f_back

                if frame and frame.f_lineno >= original_function.__code__.co_firstlineno:
                    __sx_completed__ = True

            finally:
                del frame

        if __sx_completed__ or (not args and not kwargs and callable(func)):
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

        return (lambda func: cls(func, *args, __sx_completed__=True, **kwargs))

    @classmethod
    def decorate(cls, func, *args, **kwargs):
        raise NotImplementedError

    def __init__(self, func, *args, __sx_completed__=None, **kwargs):
        BaseDecorator.__init__(self, func)

        if args or kwargs:
            self.handle_args(*args, **kwargs)

        decorate(self, func)

    def handle_args(self, *args, **kwargs):
        raise SemantixError('decorator %r does not support any arguments' % self.__class__.__name__)

    def __get__(self, obj, cls=None):
        if obj is None:
            target = cls
            method = self.class_call
        else:
            target = obj
            method = self.instance_call

        wrapper = partial(method, target)
        decorate(wrapper, self.__wrapped__)
        return wrapper

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def instance_call(self, instance, *args, **kwargs):
        return self(instance, *args, **kwargs)

    def class_call(self, cls, *args, **kwargs):
        return self(cls, *args, **kwargs)


def _unwrap_once(func):
    try:
        return func.__wrapped__
    except AttributeError:
        try:
            return func.__func__
        except AttributeError:
            pass

    raise TypeError('unable to unwrap decorated function {!r}'.format(func))


def unwrap(func, *, verify=False):
    '''Extracts the inner-most callable of a decorated callable.
    It does this by following ``__wrapped__`` attributes on regular
    functions (that's how ``functools.wraps`` provides the reference
    to the decorated function), then tries ``__file__`` (works for
    ``@staticmethod`` and ``@classmethod``.

    Example:

    .. code-block:: python

        def extract(func):
            func = unwrap(func)
            print(func.__name__)

        def some_other_decorator(func):
            """A decorator that doesn't save the decorated function's
            name.  So its name will be set to '__wrapper__'"""
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            wrapper.__wrapped__ = func
            return wrapper

        @extract
        @some_other_decorator
        def test():
            pass

    After executing the above example, 'test' will be printed.

    :param bool verify: If set to ``True``, checks that the ``unwrap`` was
                        called from the same file where the ``func`` was
                        defined.  This ensures that we found the inner-most
                        decorated function, with the one exception: it won't
                        detect an error, if the ``func`` and the code that
                        calls ``unwrap`` are in the same file.
    '''

    orig_func = func
    while isdecorated(orig_func):
        orig_func = _unwrap_once(orig_func)

    if verify:
        orig_file = orig_func.__code__.co_filename

        frame = sys._getframe()
        try:
            while frame is not None:
                file = frame.f_code.co_filename
                if file == orig_file:
                    break
                frame = frame.f_back
            else:
                raise RuntimeError('unable to unwrap callable {!r}'.format(func))
        finally:
            del frame

    return orig_func


def get_argsspec(func):
    if isdecorated(func):
        func = unwrap(func)
    return inspect.getfullargspec(func)


class hybridmethod(BaseDecorator):
    def __get__(self, obj, cls=None):
        if obj is None:
            return _method(self.__wrapped__, cls)
        else:
            return _method(self.__wrapped__, obj)


class cachedproperty(BaseDecorator):
    def __init__(self, func):
        super().__init__(func)
        self.__name__ = func.__name__

    def __get__(self, obj, cls=None):
        assert obj
        value = self.__wrapped__(obj)
        obj.__dict__[self.__name__] = value
        return value


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
