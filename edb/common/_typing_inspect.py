# The MIT License (MIT)
#
# Portions Copyright (c) 2017-2019 Ivan Levkivskyi
# Portions Copyright (c) 2021 MagicStack Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
This is a micro-implementation of a subset of `typing-inspect` API that Gel
relies on that only works on Python 3.9+.
"""

from __future__ import annotations

import collections
from types import GenericAlias, UnionType  # type: ignore
from typing import _GenericAlias  # type: ignore
from typing import Any, ClassVar, Generic, Optional, Tuple, TypeVar, Union


__all__ = [
    "is_classvar",
    "is_typevar",
    "is_generic_type",
    "is_union_type",
    "is_tuple_type",
    "get_args",
    "get_generic_bases",
    "get_parameters",
    "get_origin",
]


def is_classvar(t) -> bool:
    return t is ClassVar or _is_genericalias(t) and t.__origin__ is ClassVar


def is_typevar(t) -> bool:
    return type(t) is TypeVar


def is_generic_type(t) -> bool:
    return (
        isinstance(t, type)
        and issubclass(t, Generic)  # type: ignore
        or _is_genericalias(t)
        and t.__origin__
        not in (Union, tuple, ClassVar, collections.abc.Callable)
    )


def is_union_type(t) -> bool:
    return (
        t is Union
        or (_is_genericalias(t) and t.__origin__ is Union)
        or isinstance(t, UnionType)
    )


def is_tuple_type(t) -> bool:
    return (
        t is Tuple
        or _is_genericalias(t)
        and t.__origin__ is tuple
        or isinstance(t, type)
        and issubclass(t, Generic)  # type: ignore
        and issubclass(t, tuple)
    )


def get_args(t, evaluate: bool = True) -> Any:
    if evaluate is not None and not evaluate:
        raise ValueError("evaluate can only be True in Python >= 3.7")
    if _is_genericalias(t) or isinstance(t, UnionType):
        res = t.__args__
        if (
            get_origin(t) is collections.abc.Callable
            and res[0] is not Ellipsis
        ):
            res = (list(res[:-1]), res[-1])
        return res
    return ()


def get_generic_bases(t) -> Tuple[type, ...]:
    return getattr(t, "__orig_bases__", ())


def get_parameters(t) -> Tuple[TypeVar, ...]:
    if (
        _is_genericalias(t)
        or isinstance(t, type)
        and issubclass(t, Generic)  # type: ignore
        and t is not Generic
    ):
        return t.__parameters__
    else:
        return ()


def get_origin(t) -> Optional[type]:
    if _is_genericalias(t):
        return t.__origin__ if t.__origin__ is not ClassVar else None
    if t is Generic:
        return Generic  # type: ignore
    return None


def _is_genericalias(t) -> bool:
    return isinstance(t, (GenericAlias, _GenericAlias))
