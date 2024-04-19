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
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    Dict,
    get_type_hints,
)

import functools
import types
import sys

from edb.common import typing_inspect


__all__ = [
    "ParametricType",
    "SingleParametricType",
    "KeyValueParametricType",
]


T = TypeVar("T")
V = TypeVar("V")


try:
    from types import GenericAlias
except ImportError:
    from typing import _GenericAlias as GenericAlias  # type: ignore


class ParametricType:

    types: ClassVar[Optional[Tuple[type, ...]]] = None
    orig_args: ClassVar[Optional[Tuple[type, ...]]] = None
    _forward_refs: ClassVar[Dict[str, Tuple[int, str]]] = {}
    _type_param_map: ClassVar[Dict[Any, str]] = {}
    _non_type_params: ClassVar[Dict[int, type]] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if cls.types is not None:
            return
        elif ParametricType in cls.__bases__:
            cls._init_parametric_base()
        elif any(issubclass(b, ParametricType) for b in cls.__bases__):
            cls._init_parametric_user()

    @classmethod
    def _init_parametric_base(cls) -> None:
        """Initialize a direct subclass of ParametricType"""

        # Direct subclasses of ParametricType must declare
        # ClassVar attributes corresponding to the Generic type vars.
        # For example:
        #     class P(ParametricType, Generic[T, V]):
        #         t: ClassVar[Type[T]]
        #         v: ClassVar[Type[V]]

        params = getattr(cls, '__parameters__', None)

        if not params:
            raise TypeError(
                f'{cls} must be declared as Generic'
            )

        mod = sys.modules[cls.__module__]
        annos = get_type_hints(cls, mod.__dict__)
        param_map = {}

        for attr, t in annos.items():
            if not typing_inspect.is_classvar(t):
                continue

            args = typing_inspect.get_args(t)
            # ClassVar constructor should have the check, but be extra safe.
            assert len(args) == 1

            arg = args[0]
            if typing_inspect.get_origin(arg) is not type:
                continue

            arg_args = typing_inspect.get_args(arg)
            # Likewise, rely on Type checking its stuff in the constructor
            assert len(arg_args) == 1

            if not typing_inspect.is_typevar(arg_args[0]):
                continue

            if arg_args[0] in params:
                param_map[arg_args[0]] = attr

        for param in params:
            if param not in param_map:
                raise TypeError(
                    f'{cls.__name__}: missing ClassVar for'
                    f' generic parameter {param}'
                )

        cls._type_param_map = param_map

    @classmethod
    def _init_parametric_user(cls) -> None:
        """Initialize an indirect descendant of ParametricType."""

        # For ParametricType grandchildren we have to deal with possible
        # TypeVar remapping and generally check for type sanity.

        ob = getattr(cls, '__orig_bases__', ())
        generic_params: list[type] = []

        for b in ob:
            if (
                isinstance(b, type)
                and not isinstance(b, GenericAlias)
                and issubclass(b, ParametricType)
                and b is not ParametricType
            ):
                raise TypeError(
                    f'{cls.__name__}: missing one or more type arguments for'
                    f' base {b.__name__!r}'
                )

            if not typing_inspect.is_generic_type(b):
                continue

            org = typing_inspect.get_origin(b)
            if not isinstance(org, type):
                continue
            if not issubclass(org, ParametricType):
                generic_params.extend(getattr(b, '__parameters__', ()))
                continue

            base_params = getattr(org, '__parameters__', ())
            base_non_type_params = getattr(org, '_non_type_params', {})
            args = typing_inspect.get_args(b)
            expected = len(base_params)
            if len(args) != expected:
                raise TypeError(
                    f'{b.__name__} expects {expected} type arguments'
                    f' got {len(args)}'
                )

            base_map = dict(cls._type_param_map)
            subclass_map = {}

            for i, arg in enumerate(args):
                if i in base_non_type_params:
                    continue
                if not typing_inspect.is_typevar(arg):
                    raise TypeError(
                        f'{b.__name__} expects all arguments to be'
                        f' TypeVars'
                    )

                base_typevar = base_params[i]
                attr = base_map.get(base_typevar)
                if attr is not None:
                    subclass_map[arg] = attr

            if len(subclass_map) != len(base_map):
                raise TypeError(
                    f'{cls.__name__}: missing one or more type arguments for'
                    f' base {org.__name__!r}'
                )

            cls._type_param_map = subclass_map

        cls._non_type_params = {
            i: p for i, p in enumerate(generic_params)
            if p not in cls._type_param_map
        }

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
        all_params = params
        type_params = []
        for i, param in enumerate(all_params):
            if i not in cls._non_type_params:
                type_params.append(param)
        params_str = ", ".join(_type_repr(a) for a in all_params)
        name = f"{cls.__name__}[{params_str}]"
        bases = (cls,)
        type_dict: Dict[str, Any] = {
            "types": tuple(type_params),
            "orig_args": all_params,
            "__module__": cls.__module__,
        }
        forward_refs: Dict[str, Tuple[int, str]] = {}
        tuple_to_attr: Dict[int, str] = {}

        if cls._type_param_map:
            gen_params = getattr(cls, '__parameters__', ())
            for i, gen_param in enumerate(gen_params):
                attr = cls._type_param_map.get(gen_param)
                if attr:
                    tuple_to_attr[i] = attr

            expected = len(gen_params)
            actual = len(params)
            if expected != actual:
                raise TypeError(
                    f"type {cls.__name__!r} expects {expected} type"
                    f" parameter{'s' if expected != 1 else ''},"
                    f" got {actual}"
                )

            for i, attr in tuple_to_attr.items():
                type_dict[attr] = all_params[i]

        if not all(isinstance(param, type) for param in type_params):
            if all(
                type(param) is TypeVar  # type: ignore[comparison-overlap]
                for param in type_params
            ):
                # All parameters are type variables: return the regular generic
                # alias to allow proper subclassing.
                generic = super(ParametricType, cls)
                return generic.__class_getitem__(all_params)  # type: ignore
            else:
                forward_refs = {
                    param: (i, tuple_to_attr[i])
                    for i, param in enumerate(type_params)
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
            if isinstance(t, type) and not isinstance(t, GenericAlias):
                types[idx] = t
                setattr(cls, attr, t)
            else:
                raise TypeError(
                    f"{cls!r} expects types as type parameters, got {t!r:.100}"
                )

        cls.types = tuple(types)
        cls._forward_refs = {}

    @classmethod
    def is_anon_parametrized(cls) -> bool:
        return cls.__name__.endswith(']')

    def __reduce__(self) -> Tuple[Any, ...]:
        raise NotImplementedError(
            'must implement explicit __reduce__ for ParametricType subclass'
        )


class SingleParametricType(ParametricType, Generic[T]):

    type: ClassVar[Type[T]]  # type: ignore


class KeyValueParametricType(ParametricType, Generic[T, V]):

    keytype: ClassVar[Type[T]]  # type: ignore
    valuetype: ClassVar[Type[V]]  # type: ignore


def _type_repr(obj: Any) -> str:
    if isinstance(obj, type):
        if obj.__module__ == "builtins":
            return obj.__qualname__
        return f"{obj.__module__}.{obj.__qualname__}"
    if isinstance(obj, types.FunctionType):
        return obj.__name__
    return repr(obj)
