##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect
import warnings
import itertools
import functools

from .base import BaseDecorator


from semantix.exceptions import SemantixError
import semantix.utils.functional


__all__ = ['checktypes']


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

        return cls


class Checker(metaclass=MetaChecker):
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


class TupleChecker(Checker):
    def __init__(self, targets):
        super().__init__(targets)
        self.checkers = [Checker.get(target) for target in targets]

    def check(self, value):
        return any(checker.check(value) for checker in self.checkers)

    @classmethod
    def can_handle(cls, target):
        return isinstance(target, tuple)


class TypeChecker(Checker):
    def check(self, value):
        return isinstance(value, self.target)

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
        return inspect.getfullargspec(func)

    @classmethod
    def get_checkers(cls, func, args_spec):
        return {arg: Checker.get(target) for arg, target in args_spec.annotations.items()}

    @classmethod
    def check_defaults(cls, func, args_spec, checkers):
        defaults_checks = []
        if args_spec.defaults:
            defaults_checks.append(zip(reversed(args_spec.args), reversed(args_spec.defaults)))
        if args_spec.kwonlydefaults:
            defaults_checks.append(args_spec.kwonlydefaults.items())

        if defaults_checks:
            for arg_name, arg_default in itertools.chain(*defaults_checks):
                if arg_default is not None and arg_name in checkers \
                                                    and not checkers[arg_name].check(arg_default):
                    raise TypeError('Default value for "%s" argument of "%s()" ' \
                                    'function has an invalid value %s' % \
                                    (arg_name, func.__name__, cls.repr(arg_default)))

    @classmethod
    def check_args(cls, func, args, kwargs, args_spec, checkers):
        if len(args) > len(args_spec):
            raise TypeError('%s() takes exactly %d positional argument(s) (%d given)' % \
                                            (func.__name__, len(args_spec.args), len(args)))

        for arg_name, arg_value in itertools.chain(zip(args_spec.args, args), kwargs.items()):
            if arg_value is None:
                continue

            if arg_name in checkers and not checkers[arg_name].check(arg_value):
                raise TypeError('"%s" argument of "%s()" function has an invalid value %s' % \
                                                    (arg_name, func.__name__, cls.repr(arg_value)))

    @classmethod
    def check_result(cls, func, result, checkers):
        if result is not None and 'return' in checkers:
            if not checkers['return'].check(result):
                raise TypeError('Function "%s()" returned an invalid value %s' % \
                                                                (func.__name__, cls.repr(result)))

    @classmethod
    def try_apply_decorator(cls, func):
        if hasattr(func, '__call__') and hasattr(func, '__annotations__') \
                            and func.__annotations__ and not isinstance(func, BaseDecorator):
            return cls.checktypes_function(func)

        if inspect.isclass(func):
            return cls.checktypes_class(func)

        if isinstance(func, classmethod):
            return classmethod(cls.try_apply_decorator(func.__func__))

        if isinstance(func, staticmethod):
            return staticmethod(cls.try_apply_decorator(func.__func__))

        if isinstance(func, BaseDecorator):
            top = func
            while isinstance(func, BaseDecorator):
                host = func
                func = func._func_
            host._func_ = cls.try_apply_decorator(host._func_)
            return top

        return func

    @classmethod
    def checktypes_function(cls, func):
        assert inspect.isfunction(func)

        if not func.__annotations__:
            return func

        args_spec = cls.get_argsspec(func)
        checkers = cls.get_checkers(func, args_spec)
        cls.check_defaults(func, args_spec, checkers)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cls.check_args(func, args, kwargs, args_spec, checkers)
            result = func(*args, **kwargs)
            cls.check_result(func, result, checkers)
            return result

        return wrapper

    @classmethod
    def checktypes_class(cls, target_cls):
        assert inspect.isclass(target_cls)

        for name, object in target_cls.__dict__.items():
            patched = cls.try_apply_decorator(object)
            if patched is not object:
                setattr(target_cls, name, patched)

        return target_cls


class checktypes:
    def __new__(cls, func):
        if not __debug__:
            return func

        if inspect.isfunction(func):
            if not func.__annotations__:
                warnings.warn('No annotation for function %s while using @checktypes on it' % \
                                                                                    func.__name__)
                return func

        return FunctionValidator.try_apply_decorator(func)
