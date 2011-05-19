##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import inspect
import collections
import itertools


_void = object()


class Parameter:
    __slots__ = ('name', 'position', 'default', 'keyword_only', 'annotation')

    def __init__(self, name, position, *,
                 default=_void, annotation=_void, keyword_only=False):

        self.name = name
        self.position = position
        if default is not _void:
            self.default = default
        self.keyword_only = keyword_only
        if annotation is not _void:
            self.annotation = annotation

    def __repr__(self):
        return '<%s at 0x%x %r pos:%s>' % (self.__class__.__name__, id(self),
                                           self.name, self.position)


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


class KwonlyArgumentError(TypeError):
    pass


class Signature:
    __slots__ = ('name', 'args', 'kwargs', 'kwonlyargs', 'vararg',
                 'varkwarg', 'map', 'return_annotation')

    def __init__(self, func):
        self.name = func.__name__
        self.args = []
        self.kwargs = []
        self.kwonlyargs = []
        self.vararg = None
        self.varkwarg = None

        argspec = inspect.getfullargspec(func)[:4]
        func_code = func.__code__

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
            self.args.append(Parameter(name, idx,
                                       annotation=self._find_annotation(func, name)))
            idx += 1

        # ... w/ defaults.
        for offset, name in enumerate(positional[non_default_count:]):
            self.kwargs.append(Parameter(name, idx,
                                         default=fxn_defaults[offset],
                                         annotation=self._find_annotation(func, name)))
            idx += 1

        # *args
        if func_code.co_flags & 0x04:
            name = func_code.co_varnames[pos_count + keyword_only_count]
            self.vararg = Parameter(name, idx,
                                    annotation=self._find_annotation(func, name))
            idx += 1

        # Keyword-only parameters.
        for name in keyword_only:
            default_value = _void
            if func.__kwdefaults__ and name in func.__kwdefaults__:
                default_value = func.__kwdefaults__[name]

            self.kwonlyargs.append(Parameter(name, idx, keyword_only=True,
                                             default=default_value,
                                             annotation=self._find_annotation(func, name)))
            idx += 1

        # **kwargs
        if func_code.co_flags & 0x08:
            index = pos_count + keyword_only_count
            if func_code.co_flags & 0x04:
                index += 1

            name = func_code.co_varnames[index]
            self.varkwarg = Parameter(name, idx,
                                      annotation=self._find_annotation(func, name))

        self.map = {arg.name: arg for arg in self}

        # Return annotation.
        if 'return' in func.__annotations__:
            self.return_annotation = func.__annotations__['return']

    def __iter__(self):
        chain = [self.args, self.kwargs]
        if self.vararg:
            chain.append((self.vararg,))
        chain.append(self.kwonlyargs)
        if self.varkwarg:
            chain.append((self.varkwarg,))
        return itertools.chain(*chain)

    def _find_annotation(self, func, name):
        try:
            return func.__annotations__[name]
        except KeyError:
            return _void

    def bind(self, *arg_values, kwarg_values=None, kwarg_only_values=None, loose_kwargs=False):
        """
            @param arg_values           Function positional arguments in a tuple

            @param kwarg_values         Should be an instance of OrderedDict, to
                                        preserve right argument order

            @param kwarg_only_values    A dict of values that should be bound only
                                        to keyword-only arguments
        """

        args = collections.OrderedDict()
        kwargs = {}
        varargs = []
        varkwargs = {}

        if kwarg_values is None:
            kwarg_values = {}

        if kwarg_only_values is None:
            kwarg_only_values = {}

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

        for kwarg_name, kwarg_value in itertools.chain(kwarg_values.items(),
                                                       kwarg_only_values.items()):
            if kwarg_name in args:
                raise TypeError('too many values for %r argument' % kwarg_name)

            if kwarg_name in self.map:
                if kwarg_only_values:
                    if kwarg_name in kwarg_values and self.map[kwarg_name].keyword_only:
                        raise KwonlyArgumentError('%r argument must be passed as a keyword-only ' \
                                                  'argument' % kwarg_name)

                    if kwarg_name in kwarg_only_values and not self.map[kwarg_name].keyword_only:
                        raise KwonlyArgumentError('non keyword-only argument %r was passed as a ' \
                                                  'keyword-only argument' % kwarg_name)

                kwargs[kwarg_name] = kwarg_value
            else:
                if self.varkwarg:
                    varkwargs[kwarg_name] = kwarg_value
                else:
                    if not loose_kwargs:
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
