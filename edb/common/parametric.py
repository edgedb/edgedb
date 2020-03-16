#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations
from typing import *

import functools
import types


__all__ = [
    "ParametricType",
    "SingleParameter",
    "KeyValueParameter",
]


class ParametricType:
    types: ClassVar[Optional[Tuple[type, ...]]] = None
    _forward_refs: ClassVar[Dict[str, Tuple[int, str]]] = {}

    def __init__(self) -> None:
        if self._forward_refs:
            raise TypeError(
                f"{type(self)!r} unresolved type parameters"
            )
        if self.types is None:
            raise TypeError(
                f"{type(self)!r} must be parametrized to instantiate"
            )

        super().__init__()

    @classmethod
    @functools.lru_cache()
    def __class_getitem__(
        cls, params: Union[Union[type, str], Tuple[Union[type, str], ...]]
    ) -> Type[ParametricType]:
        """Return a dynamic subclass parametrized with `params`.

        We cannot use `_GenericAlias` provided by `Generic[T]` because the
        default `__class_getitem__` on `_GenericAlias` is not a real type and
        so it doesn't retain information on generics on the class.  Even on
        the object, it adds the relevant `__orig_class__` link too late, after
        `__init__()` is called.  That means we wouldn't be able to type-check
        in the initializer using built-in `Generic[T]`.
        """
        if cls.types is not None:
            raise TypeError(f"{cls!r} is already parametrized")

        if not isinstance(params, tuple):
            params = (params,)
        params_str = ", ".join(_type_repr(a) for a in params)
        name = f"{cls.__name__}[{params_str}]"
        bases = (cls,)
        type_dict: Dict[str, Any] = {
            "types": params,
            "__module__": cls.__module__,
        }
        forward_refs: Dict[str, Tuple[int, str]] = {}
        tuple_to_attr: Dict[int, str] = {}
        if issubclass(cls, SingleParameter):
            if len(params) != 1:
                raise TypeError(f"{cls!r} expects one type parameter")
            type_dict["type"] = params[0]
            tuple_to_attr[0] = "type"
        elif issubclass(cls, KeyValueParameter):
            if len(params) != 2:
                raise TypeError(f"{cls!r} expects two type parameters")
            type_dict["keytype"] = params[0]
            tuple_to_attr[0] = "keytype"
            type_dict["valuetype"] = params[1]
            tuple_to_attr[1] = "valuetype"
        if not all(isinstance(param, type) for param in params):
            if all(type(param) == TypeVar for param in params):
                # All parameters are type variables: return the regular generic
                # alias to allow proper subclassing.
                generic = super(ParametricType, cls)
                return generic.__class_getitem__(params)  # type: ignore
            else:
                forward_refs = {
                    param: (i, tuple_to_attr[i])
                    for i, param in enumerate(params)
                    if isinstance(param, str)
                }

                if not forward_refs:
                    raise TypeError(
                        f"{cls!r} expects types as type parameters")

        result = type(name, bases, type_dict)
        assert issubclass(result, ParametricType)
        result._forward_refs = forward_refs
        return result

    @classmethod
    def is_fully_resolved(cls) -> bool:
        return not cls._forward_refs

    @classmethod
    def resolve_types(cls, globalns: Dict[str, Any]) -> None:
        if cls.types is None:
            raise TypeError(
                f"{cls!r} is not parametrized"
            )

        if not cls._forward_refs:
            return

        types = list(cls.types)

        for ut, (idx, attr) in cls._forward_refs.items():
            t = eval(ut, globalns, {})
            if isinstance(t, type):
                types[idx] = t
                setattr(cls, attr, t)
            else:
                raise TypeError(
                    f"{cls!r} expects types as type parameters, got {t!r:.100}"
                )

        cls.types = tuple(types)
        cls._forward_refs = {}

    def __reduce__(self) -> Tuple[Any, ...]:
        raise NotImplementedError(
            'must implement explicit __reduce__ for ParametricType subclass'
        )


class SingleParameter:
    type: ClassVar[type]


class KeyValueParameter:
    keytype: ClassVar[type]
    valuetype: ClassVar[type]


def _type_repr(obj: Any) -> str:
    if isinstance(obj, type):
        if obj.__module__ == "builtins":
            return obj.__qualname__
        return f"{obj.__module__}.{obj.__qualname__}"
    if isinstance(obj, types.FunctionType):
        return obj.__name__
    return repr(obj)
