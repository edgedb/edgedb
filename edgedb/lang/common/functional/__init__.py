##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import functools

from .adapter import Adapter, AdapterError  # NOQA
from .types import checktypes, ischecktypes  # NOQA
from .tools import *  # NOQA


def deprecated(func=None, *, msg=None):
    """Mark a callable as a deprecated feature.

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
                w = '{}.{} is deprecated'.format(
                    func.__module__, func.__qualname__)
            import warnings
            warnings.warn(w, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    if func:
        return wrap(func)
    else:
        return wrap
