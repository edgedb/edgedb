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


class BindError(TypeError):
    pass


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
        return '<{} at 0x{:x} {!r} pos:{}>'.format(self.__class__.__name__, id(self),
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
        if self._varargs:
            return tuple(self._args.values()) + self._varargs
        else:
            return tuple(self._args.values())

    @property
    def kwargs(self):
        if self._varkwargs:
            return dict(itertools.chain(self._kwargs.items(), self._varkwargs.items()))
        else:
            return dict(self._kwargs)


class Signature:
    __slots__ = ('name', 'args', 'kwonlyargs', 'vararg', 'varkwarg', '_map', 'return_annotation')

    def __init__(self, func):
        self.name = func.__name__
        self.args = []
        self.kwonlyargs = []
        self.vararg = None
        self.varkwarg = None

        argspec = inspect.getfullargspec(func)[:4]
        func_code = func.__code__

        # Parameter information.
        pos_count = func_code.co_argcount
        keyword_only_count = func_code.co_kwonlyargcount
        positional = argspec[0]
        keyword_only = func_code.co_varnames[pos_count:(pos_count+keyword_only_count)]

        fxn_defaults = func.__defaults__
        if fxn_defaults:
            pos_default_count = len(fxn_defaults)
        else:
            pos_default_count = 0

        idx = 0

        # Non-keyword-only parameters w/o defaults.
        non_default_count = pos_count - pos_default_count
        for name in positional[:non_default_count]:
            self.args.append(Parameter(name, idx, annotation=self._find_annotation(func, name)))
            idx += 1

        # ... w/ defaults.
        for offset, name in enumerate(positional[non_default_count:]):
            self.args.append(Parameter(name, idx,
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

        self.args = tuple(self.args)
        self.kwonlyargs = tuple(self.kwonlyargs)
        self._map = {arg.name: arg for arg in self}

        # Return annotation.
        if 'return' in func.__annotations__:
            self.return_annotation = func.__annotations__['return']

    def __getitem__(self, arg_name):
        return self._map[arg_name]

    def __iter__(self):
        for arg in self.args:
            yield arg
        if self.vararg:
            yield self.vararg
        for arg in self.kwonlyargs:
            yield arg
        if self.varkwarg:
            yield self.varkwarg

    def _find_annotation(self, func, name):
        try:
            return func.__annotations__[name]
        except KeyError:
            return _void

    def render_args(self, for_apply=False):
        result = []

        def render_arg(arg):
            if for_apply:
                return '{}={}'.format(arg.name, arg.name)
            else:
                try:
                    default = arg.default
                except AttributeError:
                    return arg.name
                else:
                    return '{}={!r}'.format(arg.name, default)

        for arg in self.args:
            result.append(render_arg(arg))

        if self.vararg:
            result.append('*{}'.format(self.vararg.name))

        if self.kwonlyargs:
            if not self.vararg and not for_apply:
                result.append('*')

            for arg in self.kwonlyargs:
                result.append(render_arg(arg))

        if self.varkwarg:
            result.append('**{}'.format(self.varkwarg.name))

        return ', '.join(result)

    def bind(self, *args, **kwargs):
        _args = collections.OrderedDict()
        _kwargs = {}
        _varargs = None
        _varkwargs = None

        arg_specs = iter(self.args)
        arg_specs_ex = ()
        arg_vals = iter(args)
        while True:
            try:
                arg_val = next(arg_vals)
            except StopIteration:
                try:
                    arg_spec = next(arg_specs)
                except StopIteration:
                    break
                else:
                    if hasattr(arg_spec, 'default') or arg_spec.name in kwargs:
                        arg_specs_ex = (arg_spec,)
                        break
                    else:
                        raise BindError('bind: {func}: {arg!r} parameter lacking default value'. \
                                        format(func=self.name, arg=arg_spec.name))
            else:
                try:
                    arg_spec = next(arg_specs)
                except StopIteration:
                    if self.vararg:
                        _varargs = (arg_val,) + tuple(arg_vals)
                        break
                    else:
                        raise BindError('bind: {func}: too many positional arguments'. \
                                        format(func=self.name))
                else:
                    if arg_spec.name in kwargs:
                        raise BindError('bind: {func}: multiple values for keyword argument {arg!r}'. \
                                        format(arg=arg_spec.name, func=self.name))
                    _args[arg_spec.name] = arg_val

        for arg_spec in itertools.chain(arg_specs_ex, arg_specs, self.kwonlyargs):
            arg_name = arg_spec.name
            try:
                arg_val = kwargs[arg_name]
            except KeyError:
                if not hasattr(arg_spec, 'default'):
                    raise BindError('bind: {func}: {arg!r} parameter lacking default value'. \
                                    format(arg=arg_name, func=self.name))
            else:
                _kwargs[arg_name] = arg_val
                kwargs.pop(arg_name)

        if kwargs:
            if self.varkwarg:
                _varkwargs = kwargs
            else:
                raise BindError('bind: {func}: too many keyword arguments'. \
                                format(func=self.name))

        return BoundArguments(_args, _kwargs, _varargs, _varkwargs)


def signature(func):
    try:
        return func.__signature__
    except AttributeError:
        sig = Signature(func)
        func.__signature__ = sig
        return sig
