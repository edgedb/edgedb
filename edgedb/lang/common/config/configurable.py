##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import itertools
import inspect

from semantix.utils.functional import decorate, get_argsspec, apply_decorator
from semantix.utils.functional.types import Checker, FunctionValidator, checktypes, \
                                            ChecktypeExempt, TypeChecker, CombinedChecker

from .cvalue import cvalue
from .exceptions import ConfigError


__all__ = 'ConfigurableMeta', 'configurable'


class ConfigurableMeta(type):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        fullname = cls.__module__ + '.' + cls.__name__
        decorate_method = functools.partial(configurable, basename=fullname, bindto=cls)

        for attrname, attrval in tuple(cls.__dict__.items()):
            if isinstance(attrval, cvalue):
                attrval._set_name(fullname + '.' + attrname)
                attrval._bind(cls)
                attrval._validate()

            else:
                try:
                    new = apply_decorator(attrval, decorate_function=decorate_method)
                except TypeError:
                    pass
                else:
                    if new is not attrval:
                        setattr(cls, attrname, new)


_Marker = object()


def configurable(func, *, basename=None, bindto=None):
    if basename is None and func.__module__ == '__main__':
        raise ConfigError('Unable to determine module\'s path')

    assert inspect.isfunction(func)

    funcname = (func.__module__ + '.' + func.__name__) if basename is None \
                                                    else (basename + '.' + func.__name__)
    bindto = func if  bindto is None else bindto
    todecorate = False

    args_spec = get_argsspec(func)
    checkers = {}

    defaults = itertools.chain(
                           zip(reversed(args_spec.args), reversed(args_spec.defaults)) \
                                            if args_spec.defaults else (),

                           args_spec.kwonlydefaults.items() \
                                            if args_spec.kwonlydefaults else ()
                         )

    for arg_name, arg_default in defaults:
        if isinstance(arg_default, cvalue):
            assert not arg_default.bound_to, 'argument was processed twice'

            todecorate = True

#            if arg_default._abstract:
#                raise TypeError('Abstract cvalue may be defined only as a class property: ' \
#                                'got %r cvalue defined for %r function' % \
#                                (arg_name, func.__name__))

            arg_default._set_name(funcname + '.' + arg_name)
            arg_default._bind(bindto)
            arg_default._owner = func

            if arg_default._validator:
                assert isinstance(arg_default._validator, Checker)
                checkers[arg_name] = arg_default._validator

            arg_default._validate()

    if not todecorate:
        return func

    def wrapper(*args, **kwargs):
        FunctionValidator.validate_kwonly(func, args, args_spec)

        args = list(args)
        if args_spec.defaults:
            try:
                flatten_args_spec = getattr(func, '_flatten_args_spec_')
            except AttributeError:
                flatten_args_spec = tuple(enumerate(reversed(tuple(
                                        itertools.zip_longest(
                                            reversed(tuple(args_spec.args) \
                                                                if args_spec.args else ()),

                                            reversed(tuple(args_spec.defaults) \
                                                                if args_spec.defaults else ()),

                                            fillvalue=_Marker
                                        )
                                    ))))

                setattr(func, '_flatten_args_spec_', flatten_args_spec)

            for i, (arg_name, default) in flatten_args_spec:
                if i >= len(args):
                    if default is _Marker:
                        raise TypeError('%s argument is required' % arg_name)

                    if isinstance(default, cvalue):
                        while isinstance(default, cvalue):
                            default = default._get_value()
                    args.append(default)

        if args_spec.kwonlydefaults:
            for def_name, def_value in args_spec.kwonlydefaults.items():
                if not def_name in kwargs and isinstance(def_value, cvalue):
                    while isinstance(def_value, cvalue):
                        def_value = def_value._get_value()
                    kwargs[def_name] = def_value

        FunctionValidator.check_args(func, args, kwargs, args_spec, checkers)
        return func(*args, **kwargs)

    decorate(wrapper, func)
    return wrapper
