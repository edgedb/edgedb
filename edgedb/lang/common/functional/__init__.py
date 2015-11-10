##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools

from .adapter import Adapter, AdapterError
from .types import checktypes, ischecktypes
from .tools import *


def deprecated(func=None, *, msg=None):
    """Marks a callable as a deprecated feature.

    Usage:

    .. code-block:: python

        @deprecated
        def foo():
            ...

        @deprecated(msg='bar is deprecated; use foo() instead')
        def bar():
            ...
    """

    def wrap(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            w = msg
            if not w:
                w = '{}.{} is deprecated'.format(func.__module__, func.__qualname__)
            import warnings
            warnings.warn(w, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)
        return wrapper

    if func:
        return wrap(func)
    else:
        return wrap
