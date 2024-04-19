# mypy: ignore-errors

# Portions copyright 2019-present MagicStack Inc. and the EdgeDB authors.
# Portions copyright 2001-2019 Python Software Foundation.
# License: PSFL.

"""A replacement for functools.singledispatch() that records usage.

See README.md in this package for more details.
"""


from __future__ import annotations
from typing import (
    Any,
    Callable,
    Tuple,
    TypeVar,
    Dict,
    TYPE_CHECKING,
)

import functools
import sys
import threading


# Aliases to minimize code changes in the vendored singledispatch below.
get_cache_token = functools.get_cache_token
update_wrapper = functools.update_wrapper
_find_impl = functools._find_impl


profiling_in_progress = threading.Event()


if TYPE_CHECKING:
    T = TypeVar("T", bound=Callable[..., Any])
    ModulePath = str
    LineNo = int
    FunctionName = str
    FunctionID = Tuple[ModulePath, LineNo, FunctionName]
    Caller = FunctionID
    Callee = FunctionID
    CallCount = int
    CallCounts = Dict[Caller, Dict[Callee, CallCount]]


done_dispatches: Dict[FunctionID, CallCounts] = {}


# Taken from Python 3.8.0+ (051ff526b5dc2c40c4a53d87089740358822edfa)
# The only change is the `wrapper` function implementation, replaced with
# `sd_wrapper` for clarity.
def tracing_singledispatch(func: T) -> T:
    """Single-dispatch generic function decorator.

    Transforms a function into a generic function, which can have different
    behaviours depending upon the type of its first argument. The decorated
    function acts as the default implementation, and additional
    implementations can be registered using the register() attribute of the
    generic function.
    """
    # There are many programs that use functools without singledispatch, so we
    # trade-off making singledispatch marginally slower for the benefit of
    # making start-up of such applications slightly faster.
    import types
    import weakref

    registry = {}
    dispatch_cache = weakref.WeakKeyDictionary()
    cache_token = None

    def dispatch(cls):
        """generic_func.dispatch(cls) -> <function implementation>

        Runs the dispatch algorithm to return the best available implementation
        for the given *cls* registered on *generic_func*.

        """
        nonlocal cache_token
        if cache_token is not None:
            current_token = get_cache_token()
            if cache_token != current_token:
                dispatch_cache.clear()
                cache_token = current_token
        try:
            impl = dispatch_cache[cls]
        except KeyError:
            try:
                impl = registry[cls]
            except KeyError:
                impl = _find_impl(cls, registry)
            dispatch_cache[cls] = impl
        return impl

    def register(cls, func=None):
        """generic_func.register(cls, func) -> func

        Registers a new implementation for the given *cls* on a *generic_func*.

        """
        nonlocal cache_token
        if func is None:
            if isinstance(cls, type):
                return lambda f: register(cls, f)
            ann = getattr(cls, '__annotations__', {})
            if not ann:
                raise TypeError(
                    f"Invalid first argument to `register()`: {cls!r}. "
                    f"Use either `@register(some_class)` or plain `@register` "
                    f"on an annotated function."
                )
            func = cls

            # only import typing if annotation parsing is necessary
            from typing import get_type_hints
            argname, cls = next(iter(get_type_hints(func).items()))
            if not isinstance(cls, type):
                raise TypeError(
                    f"Invalid annotation for {argname!r}. "
                    f"{cls!r} is not a class."
                )
        registry[cls] = func
        if cache_token is None and hasattr(cls, '__abstractmethods__'):
            cache_token = get_cache_token()
        dispatch_cache.clear()
        return func

    def sd_wrapper(*args, **kw):
        if not args:
            raise TypeError(f'{funcname} requires at least '
                            '1 positional argument')

        impl = dispatch(args[0].__class__)
        if profiling_in_progress.is_set():
            caller = sys._getframe().f_back.f_code
            caller_id = (
                caller.co_filename,
                caller.co_firstlineno,
                caller.co_name,
            )
            impl_id = (
                impl.__code__.co_filename,
                impl.__code__.co_firstlineno,
                impl.__code__.co_name,
            )
            our_dispatches = done_dispatches.setdefault(func_id, {})
            caller_dispatches = our_dispatches.setdefault(caller_id, {})
            caller_dispatches[impl_id] = caller_dispatches.get(impl_id, 0) + 1
        return impl(*args, **kw)

    funcname = getattr(func, '__name__', 'singledispatch function')
    registry[object] = func
    _fcode = func.__code__
    func_id = (_fcode.co_filename, _fcode.co_firstlineno, _fcode.co_name)
    wrapper = sd_wrapper
    wrapper.register = register
    wrapper.dispatch = dispatch
    wrapper.registry = types.MappingProxyType(registry)
    wrapper._clear_cache = dispatch_cache.clear
    update_wrapper(wrapper, func)
    return wrapper


def patch_functools() -> None:
    functools.singledispatch = tracing_singledispatch
