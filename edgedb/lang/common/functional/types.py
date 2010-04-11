##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect
import warnings
import itertools

from .base import BaseDecorator, decorate
from semantix.exceptions import SemantixError
import semantix.utils.functional


__all__ = ['checktypes']


class ChecktypeExempt:
    __slots__ = ()


class CheckerError(SemantixError):
    pass


class ChecktypesError(SemantixError):
    pass


class MetaChecker(type):
    registry = []

    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)

        if 'can_handle' in dct:
            MetaChecker.registry.append(cls)

        if '__slots__' not in dct:
            raise CheckerError('Invalid type checker %s: missing __slots__' % name)

        if not isinstance(dct['__slots__'], tuple):
            raise CheckerError('Invalid type checker %s: __slots__ must be a tuple' % name)

        return cls


class Checker(metaclass=MetaChecker):
    __slots__ = ('target',)

    def __init__(self, target=None):
        self.target = target

    @classmethod
    def get(cls, target):
        if isinstance(target, Checker):
            return target

        for checker in MetaChecker.registry:
            if checker.can_handle(target):
                return checker(target)

        raise CheckerError('Unable to find proper checker for %r' % target)


class LambdaChecker(Checker):
    __slots__ = ()

    def check(self, value, func, arg_name):
        if not self.target(value):
            raise TypeError('Invalid function %s argument %s' % \
                            (getattr(func, '__name__', func), arg_name))

    @classmethod
    def can_handle(cls, target):
        return inspect.isfunction(target) and target.__name__ == '<lambda>'


class TupleChecker(Checker):
    __slots__ = ('checkers',)

    def __init__(self, targets):
        super().__init__(targets)
        self.checkers = [Checker.get(target) for target in targets]

    def check(self, value, func, arg_name):
        for checker in self.checkers:
            try:
                checker.check(value, func, arg_name)

            except TypeError:
                pass

            else:
                return

        raise TypeError('Invalid function %s argument %s' % \
                        (getattr(func, '__name__', func), arg_name))

    @classmethod
    def can_handle(cls, target):
        return isinstance(target, tuple)


class TypeChecker(Checker):
    __slots__ = ()

    def check(self, value, func, arg_name):
        if not isinstance(value, self.target):
            raise TypeError('Unexpected type of function %s argument %s: expected %s, got %s' % \
                            (getattr(func, '__name__', func), arg_name,
                             self.target.__name__, type(value).__name__))

    @classmethod
    def can_handle(cls, target):
        return inspect.isclass(target)


class FunctionValidator:
    MAX_REPR_LEN = 100

    @classmethod
    def repr(cls, value):
        string = repr(value)
        if len(string) >= cls.MAX_REPR_LEN:
            string = string[:cls.MAX_REPR_LEN-3] + '...'
        return string

    @classmethod
    def get_argsspec(cls, func):
        try:
            return getattr(func, '_args_spec_')
        except AttributeError:
            return inspect.getfullargspec(func)

    @classmethod
    def get_checkers(cls, func, args_spec):
        return {arg: Checker.get(target) for arg, target in args_spec.annotations.items()}

    @classmethod
    def check_value(cls, checker, value, func, arg_name):
        if value is not None and not isinstance(value, ChecktypeExempt):
            checker.check(value, func, arg_name)

    @classmethod
    def check_defaults(cls, func, args_spec, checkers):
        defaults_checks = []
        if args_spec.defaults:
            defaults_checks.append(zip(reversed(args_spec.args), reversed(args_spec.defaults)))
        if args_spec.kwonlydefaults:
            defaults_checks.append(args_spec.kwonlydefaults.items())

        if defaults_checks:
            for arg_name, arg_default in itertools.chain(*defaults_checks):
                if arg_name in checkers:
                    cls.check_value(checkers[arg_name], arg_default, func, arg_name)

    @classmethod
    def validate_kwonly(cls, func, args, args_spec):
        if len(args) > len(args_spec):
            raise TypeError('%s() takes exactly %d positional argument(s) (%d given)' % \
                                            (func.__name__, len(args_spec.args), len(args)))

    @classmethod
    def check_args(cls, func, args, kwargs, args_spec, checkers):
        cls.validate_kwonly(func, args, args_spec)

        for arg_name, arg_value in itertools.chain(zip(args_spec.args, args), kwargs.items()):
            if arg_name in checkers:
                cls.check_value(checkers[arg_name], arg_value, func, arg_name)

    @classmethod
    def check_result(cls, func, result, checkers):
        if 'return' in checkers:
            cls.check_value(checkers['return'], result, func, 'return')

    @classmethod
    def try_apply_decorator(cls, func, decorate_function=None, decorate_class=None):
        if inspect.isfunction(func) and decorate_function:
            return decorate_function(func)

        if inspect.isclass(func) and decorate_class:
            return decorate_class(func)

        if isinstance(func, classmethod):
            return classmethod(cls.try_apply_decorator(func.__func__,
                                                       decorate_function, decorate_class))

        if isinstance(func, staticmethod):
            return staticmethod(cls.try_apply_decorator(func.__func__,
                                                        decorate_function, decorate_class))

        if isinstance(func, property):
            funcs = []
            for name in 'fget', 'fset', 'fdel':
                f = getattr(func, name, None)
                if f:
                    f = cls.try_apply_decorator(f, decorate_function, decorate_class)
                funcs.append(f)
            return property(*funcs)

        if isinstance(func, BaseDecorator):
            top = func
            while isinstance(func, BaseDecorator):
                host = func
                func = func._func_
            host._func_ = cls.try_apply_decorator(host._func_, decorate_function, decorate_class)
            return top

        return func

    @classmethod
    def checktypes_function(cls, func):
        assert inspect.isfunction(func)

        args_spec = cls.get_argsspec(func)

        if not args_spec.annotations:
            return func

        checkers = cls.get_checkers(func, args_spec)
        cls.check_defaults(func, args_spec, checkers)

        def wrapper(*args, **kwargs):
            cls.check_args(func, args, kwargs, args_spec, checkers)
            result = func(*args, **kwargs)
            cls.check_result(func, result, checkers)
            return result

        decorate(wrapper, func)
        return wrapper

    @classmethod
    def checktypes_class(cls, target_cls):
        assert inspect.isclass(target_cls)

        for name, object in target_cls.__dict__.items():
            patched = cls.try_apply_decorator(object, cls.checktypes_function, cls.checktypes_class)
            if patched is not object:
                setattr(target_cls, name, patched)

        return target_cls


class checktypes:
    def __new__(cls, func):
        if not __debug__:
            return func

        if inspect.isfunction(func):
            args_spec = FunctionValidator.get_argsspec(func)
            if not args_spec.annotations:
                warnings.warn('No annotation for function %s while using @checktypes on it' % \
                                                                                    func.__name__)
                return func

        return FunctionValidator.try_apply_decorator(func, FunctionValidator.checktypes_function,
                                                     FunctionValidator.checktypes_class)
