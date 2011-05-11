##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect
import collections
import itertools


class Argument:
    __slots__ = ('name', 'position', 'default', 'keyword_only', 'annotation')

    def __init__(self, name, position, *, has_default=False,
                 default=None, keyword_only=False, has_annotation=False,
                 annotation=None):

        self.name = name
        self.position = position
        if has_default:
            self.default = default
        self.keyword_only = keyword_only
        if has_annotation:
            self.annotation = annotation

    def __repr__(self):
        return '<%s at 0x%x %r pos:%s>' % (self.__class__.__name__, id(self),
                                           self.name, self.position)

class PositionalArgument(Argument):
    __slots__ = ()


class VarArgument(Argument):
    __slots__ = ()


class VarKeywordArgument(Argument):
    __slots__ = ()


class KeywordOnlyArgument(Argument):
    __slots__ = ()


class BoundArguments:
    __slots__ = ('_args', '_kwargs', '_varargs', '_varkwargs')

    def __init__(self, args, kwargs, varargs, varkwargs):
        self._args = args
        self._kwargs = kwargs
        self._varargs = varargs
        self._varkwargs = varkwargs

    @property
    def args(self):
        return tuple(self._args.values()) + tuple(self._varargs)

    @property
    def kwargs(self):
        return dict(itertools.chain(self._kwargs.items(), self._varkwargs.items()))


class Signature:
    __slots__ = ('name', 'args', 'kwargs', 'kwonlyargs', 'vararg', 'varkwarg',
                 'map')

    def __init__(self, func):
        func_code = func.__code__
        self.name = func.__name__

        # XXX replace with own implementation
        argspec = inspect.getfullargspec(func)[:4]


        self.args = []
        self.kwargs = []
        self.kwonlyargs = []
        self.vararg = None
        self.varkwarg = None


        # Parameter information.
        pos_count = func_code.co_argcount
        keyword_only_count = func_code.co_kwonlyargcount
        positional = argspec[0]
        keyword_only = func_code.co_varnames[pos_count:
                                                pos_count+keyword_only_count]

        fxn_defaults = func.__defaults__
        if fxn_defaults:
            pos_default_count = len(fxn_defaults)
        else:
            pos_default_count = 0

        idx = 0

        # Non-keyword-only parameters w/o defaults.
        non_default_count = pos_count - pos_default_count
        for name in positional[:non_default_count]:
            has_annotation, annotation = self._find_annotation(func, name)
            self.args.append(PositionalArgument(name, idx, has_default=False,
                                                has_annotation=has_annotation,
                                                annotation=annotation))
            idx += 1

        # ... w/ defaults.
        for offset, name in enumerate(positional[non_default_count:]):
            has_annotation, annotation = self._find_annotation(func, name)
            default_value = fxn_defaults[offset]
            self.kwargs.append(PositionalArgument(name, idx,
                                                  has_default=True, default=default_value,
                                                  has_annotation=has_annotation,
                                                  annotation=annotation))
            idx += 1

        # *args
        if func_code.co_flags & 0x04:
            name = func_code.co_varnames[pos_count + keyword_only_count]
            has_annotation, annotation = self._find_annotation(func, name)
            self.vararg = VarArgument(name, idx,
                                      has_annotation=has_annotation,
                                      annotation=annotation)
            idx += 1

        # Keyword-only parameters.
        for name in keyword_only:
            has_annotation, annotation = self._find_annotation(func, name)

            has_default, default_value = False, None
            if func.__kwdefaults__ and name in func.__kwdefaults__:
                has_default = True
                default_value = func.__kwdefaults__[name]

            self.kwonlyargs.append(KeywordOnlyArgument(name, idx, keyword_only=True,
                                                       has_default=has_default,
                                                       default=default_value,
                                                       has_annotation=has_annotation,
                                                       annotation=annotation))
            idx += 1

        # **kwargs
        if func_code.co_flags & 0x08:
            index = pos_count + keyword_only_count
            if func_code.co_flags & 0x04:
                index += 1

            name = func_code.co_varnames[index]
            has_annotation, annotation = self._find_annotation(func, name)

            self.varkwarg = VarKeywordArgument(name, idx,
                                               has_annotation=has_annotation,
                                               annotation=annotation)

        self.map = {}
        for arg in itertools.chain(self.args, self.kwargs, self.kwonlyargs):
            self.map[arg.name] = arg


    def _find_annotation(self, func, name):
        try:
            return True, func.__annotations__[name]
        except KeyError:
            return False, None

    def bind(self, *arg_values, **kwarg_values):
        args = collections.OrderedDict()
        kwargs = {}
        varargs = []
        varkwargs = {}

        positional = itertools.chain(self.args, self.kwargs)
        idx = 0
        for arg_value in arg_values:
            try:
                spec = next(positional)
            except StopIteration:
                if self.vararg:
                    varargs = arg_values[idx:]
                    break
                else:
                    raise TypeError('too many positional arguments')

            args[spec.name] = arg_value
            idx += 1

        while idx < len(self.args):
            spec = next(positional)
            idx += 1

            if spec.name not in kwarg_values:
                raise TypeError('missing value for %r positional argument' % spec.name)

            args[spec.name] = kwarg_values.pop(spec.name)

        for kwarg_name, kwarg_value in kwarg_values.items():
            if kwarg_name in args:
                raise TypeError('too many values for %r argument' % kwarg_name)

            if kwarg_name in self.map:
                kwargs[kwarg_name] = kwarg_value
            else:
                if self.varkwarg:
                    varkwargs[kwarg_name] = kwarg_value
                else:
                    raise TypeError('unknown argument %r' % kwarg_name)

        for arg in itertools.chain(self.kwargs, self.kwonlyargs):
            if arg.name not in args and arg.name not in kwargs and not hasattr(arg, 'default'):
                raise TypeError('missing value for %r argument' % arg.name)

        return BoundArguments(args, kwargs, varargs, varkwargs)


def signature(func):
    try:
        return func.__signature__
    except AttributeError:
        sig = Signature(func)
        func.__signature__ = sig
        return sig
