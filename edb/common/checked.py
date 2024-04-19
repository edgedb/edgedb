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
    AbstractSet,
    Iterable,
    Iterator,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
    Dict,
    List,
    Set,
    cast,
    overload,
)

import collections.abc
import itertools
import types

from edb.common import debug
from edb.common import parametric


__all__ = [
    "CheckedList",
    "CheckedDict",
    "CheckedSet",
    "FrozenCheckedList",
    "FrozenCheckedSet",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class ParametricContainer:

    types: ClassVar[Optional[Tuple[type, ...]]] = None

    def __reduce__(self) -> Tuple[Any, ...]:
        assert self.types is not None, f'missing parameters in {type(self)}'
        cls: Type[ParametricContainer] = self.__class__
        container = getattr(self, "_container", ())
        if cls.__name__.endswith("]"):
            # Parametrized type.
            cls = cls.__bases__[0]
        else:
            # A subclass of a parametrized type.
            return cls, (container,)

        args = self.types[0] if len(self.types) == 1 else self.types
        return cls.__restore__, (args, container)

    @classmethod
    def __restore__(
        cls, params: Tuple[type, ...], data: Iterable[Any]
    ) -> ParametricContainer:
        return cls[params](data)  # type: ignore


class AbstractCheckedList(Generic[T]):
    type: type
    _container: List[T]

    @classmethod
    def _check_type(cls, value: Any) -> T:
        """Ensure `value` is of type T and return it."""
        if not isinstance(value, cls.type):
            raise ValueError(
                f"{cls!r} accepts only values of type {cls.type!r}, "
                f"got {type(value)!r}"
            )
        return cast(T, value)

    def __init__(self, iterable: Iterable[T] = ()) -> None:
        pass

    def __lt__(self, other: List[T]) -> bool:
        return self._container < self._cast(other)

    def __le__(self, other: List[T]) -> bool:
        return self._container <= self._cast(other)

    def __gt__(self, other: List[T]) -> bool:
        return self._container > self._cast(other)

    def __ge__(self, other: List[T]) -> bool:
        return self._container >= self._cast(other)

    def _cast(self, other: List[T]) -> List[T]:
        if isinstance(other, (CheckedList, FrozenCheckedList)):
            return other._container

        return other

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (CheckedList, FrozenCheckedList)):
            other = other._container
        return self._container == other

    def __str__(self) -> str:
        return repr(self._container)

    def __repr__(self) -> str:
        return f"{_type_repr(type(self))}({repr(self._container)})"


class FrozenCheckedList(
    ParametricContainer,
    parametric.SingleParametricType[T],
    AbstractCheckedList[T],
    Sequence[T],
):
    def __init__(self, iterable: Iterable[T] = ()) -> None:
        super().__init__()
        self._container = [self._check_type(element) for element in iterable]
        self._hash_cache = -1

    def __hash__(self) -> int:
        if self._hash_cache == -1:
            self._hash_cache = hash(tuple(self._container))
        return self._hash_cache

    #
    # Sequence
    #

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> FrozenCheckedList[T]: ...

    def __getitem__(self, index: Union[int, slice]) -> Any:
        if isinstance(index, slice):
            return self.__class__(self._container[index])

        return self._container[index]

    def __len__(self) -> int:
        return len(self._container)

    #
    # List-specific
    #

    def __add__(self, other: Iterable[T]) -> FrozenCheckedList[T]:
        return self.__class__(itertools.chain(self, other))

    def __radd__(self, other: Iterable[T]) -> FrozenCheckedList[T]:
        return self.__class__(itertools.chain(other, self))

    def __mul__(self, n: int) -> FrozenCheckedList[T]:
        return self.__class__(self._container * n)

    __rmul__ = __mul__


class CheckedList(
    ParametricContainer,
    parametric.SingleParametricType[T],
    AbstractCheckedList[T],
    MutableSequence[T],
):
    def __init__(self, iterable: Iterable[T] = ()) -> None:
        super().__init__()
        self._container = [self._check_type(element) for element in iterable]

    #
    # Sequence
    #

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> CheckedList[T]: ...

    def __getitem__(self, index: Union[int, slice]) -> Any:
        if isinstance(index, slice):
            return self.__class__(self._container[index])

        return self._container[index]

    #
    # MutableSequence
    #

    @overload
    def __setitem__(self, index: int, value: T) -> None: ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[T]) -> None: ...

    def __setitem__(self, index: Union[int, slice], value: Any) -> None:
        if isinstance(index, int):
            self._container[index] = self._check_type(value)
            return

        _slice = index
        self._container[_slice] = filter(self._check_type, value)

    @overload
    def __delitem__(self, index: int) -> None: ...

    @overload
    def __delitem__(self, index: slice) -> None: ...

    def __delitem__(self, index: Union[int, slice]) -> None:
        del self._container[index]

    def insert(self, index: int, value: T) -> None:
        self._container.insert(index, self._check_type(value))

    def __len__(self) -> int:
        return len(self._container)

    #
    # List-specific
    #

    def __add__(self, other: Iterable[T]) -> CheckedList[T]:
        return self.__class__(itertools.chain(self, other))

    def __radd__(self, other: Iterable[T]) -> CheckedList[T]:
        return self.__class__(itertools.chain(other, self))

    def __iadd__(self, other: Iterable[T]) -> CheckedList[T]:
        self._container.extend(filter(self._check_type, other))
        return self

    def __mul__(self, n: int) -> CheckedList[T]:
        return self.__class__(self._container * n)

    __rmul__ = __mul__

    def __imul__(self, n: int) -> CheckedList[T]:
        self._container *= n
        return self

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        self._container.sort(key=key, reverse=reverse)


class AbstractCheckedSet(AbstractSet[T]):
    type: type
    _container: AbstractSet[T]

    def __init__(self, iterable: Iterable[T] = ()) -> None:
        pass

    @classmethod
    def _check_type(cls, value: Any) -> T:
        """Ensure `value` is of type T and return it."""
        if not isinstance(value, cls.type):
            raise ValueError(
                f"{cls!r} accepts only values of type {cls.type!r}, "
                f"got {type(value)!r}"
            )
        return cast(T, value)

    def _cast(self, other: Any) -> AbstractSet[T]:
        if isinstance(other, (FrozenCheckedSet, CheckedSet)):
            return other._container

        if isinstance(other, collections.abc.Set):
            return other

        return set(other)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (CheckedSet, FrozenCheckedSet)):
            other = other._container
        return self._container == other

    def __str__(self) -> str:
        return repr(self._container)

    def __repr__(self) -> str:
        return f"{_type_repr(type(self))}({repr(self._container)})"

    #
    # collections.abc.Set aka typing.AbstractSet
    #

    def __contains__(self, value: Any) -> bool:
        return value in self._container

    def __iter__(self) -> Iterator[T]:
        return iter(self._container)

    def __len__(self) -> int:
        return len(self._container)

    #
    # Specific to set() and frozenset()
    #

    def issubset(self, other: AbstractSet[Any]) -> bool:
        return self.__le__(other)

    def issuperset(self, other: AbstractSet[Any]) -> bool:
        return self.__ge__(other)


class FrozenCheckedSet(
    ParametricContainer,
    parametric.SingleParametricType[T],
    AbstractCheckedSet[T],
):
    def __init__(self, iterable: Iterable[T] = ()) -> None:
        super().__init__()
        self._container = {self._check_type(element) for element in iterable}
        self._hash_cache = -1

    def __hash__(self) -> int:
        if self._hash_cache == -1:
            self._hash_cache = hash(frozenset(self._container))
        return self._hash_cache

    #
    # Replaced mixins of collections.abc.Set
    #

    # NOTE: The type ignores on function signatures below are because we are
    # deliberately breaking the Liskov Substitute Principle: we want the type
    # checker to warn the user if a checked set of a type is __or__'d, or
    # __and__'d  with a set of an incompatible type.  If the user wanted this,
    # they should convert the checked set into a regular set or a differently
    # typed checked set first.

    def __and__(self, other: AbstractSet[T]) -> FrozenCheckedSet[T]:
        other_set = self._cast(other)
        for elem in other_set:
            # We need the explicit type check to reject nonsensical
            # & operations that must always result in an empty new set.
            self._check_type(elem)
        return self.__class__(other_set & self._container)

    __rand__ = __and__

    def __or__(  # type: ignore
        self, other: AbstractSet[T]
    ) -> FrozenCheckedSet[T]:
        other_set = self._cast(other)
        return self.__class__(other_set | self._container)

    __ror__ = __or__

    def __sub__(self, other: AbstractSet[T]) -> FrozenCheckedSet[T]:
        other_set = self._cast(other)
        for elem in other_set:
            # We need the explicit type check to reject nonsensical
            # - operations that always return the original checked set.
            self._check_type(elem)
        return self.__class__(self._container - other_set)

    def __rsub__(self, other: AbstractSet[T]) -> FrozenCheckedSet[T]:
        other_set = self._cast(other)
        return self.__class__(other_set - self._container)

    def __xor__(  # type: ignore
        self, other: AbstractSet[T]
    ) -> FrozenCheckedSet[T]:
        other_set = self._cast(other)
        return self.__class__(self._container ^ other_set)

    __rxor__ = __xor__

    #
    # Specific to set() and frozenset()
    #

    union = __and__
    intersection = __or__
    difference = __sub__
    symmetric_difference = __xor__


class CheckedSet(
    ParametricContainer,
    parametric.SingleParametricType[T],
    AbstractCheckedSet[T],
    MutableSet[T],
):
    _container: Set[T]

    def __init__(self, iterable: Iterable[T] = ()) -> None:
        super().__init__()
        self._container = {self._check_type(element) for element in iterable}

    #
    # Replaced mixins of collections.abc.Set
    #

    # NOTE: The type ignores on function signatures below are because we are
    # deliberately breaking the Liskov Substitute Principle: we want the type
    # checker to warn the user if a checked set of a type is __or__'d, or
    # __and__'d  with a set of an incompatible type.  If the user wanted this,
    # they should convert the checked set into a regular set or a differently
    # typed checked set first.

    def __and__(self, other: AbstractSet[T]) -> CheckedSet[T]:
        other_set = self._cast(other)
        for elem in other_set:
            # We need the explicit type check to reject nonsensical
            # & operations that must always result in an empty new set.
            self._check_type(elem)
        return self.__class__(other_set & self._container)

    __rand__ = __and__

    def __or__(self, other: AbstractSet[T]) -> CheckedSet[T]:  # type: ignore
        other_set = self._cast(other)
        return self.__class__(other_set | self._container)

    __ror__ = __or__

    def __sub__(self, other: AbstractSet[T]) -> CheckedSet[T]:
        other_set = self._cast(other)
        for elem in other_set:
            # We need the explicit type check to reject nonsensical
            # - operations that always return the original checked set.
            self._check_type(elem)
        return self.__class__(self._container - other_set)

    def __rsub__(self, other: AbstractSet[T]) -> CheckedSet[T]:
        other_set = self._cast(other)
        return self.__class__(other_set - self._container)

    def __xor__(self, other: AbstractSet[T]) -> CheckedSet[T]:  # type: ignore
        other_set = self._cast(other)
        return self.__class__(self._container ^ other_set)

    __rxor__ = __xor__

    #
    # MutableSet
    #

    def add(self, value: T) -> None:
        self._container.add(self._check_type(value))

    def discard(self, value: T) -> None:
        self._container.discard(self._check_type(value))

    #
    # Replaced mixins of collections.abc.MutableSet
    #

    def __ior__(self, other: AbstractSet[T]) -> CheckedSet[T]:  # type: ignore
        self._container |= set(filter(self._check_type, other))
        return self

    def __iand__(self, other: AbstractSet[T]) -> CheckedSet[T]:
        # We do the type check here to reject nonsensical
        # & operations that always clear the checked set.
        self._container &= set(filter(self._check_type, other))
        return self

    def __ixor__(self, other: AbstractSet[T]) -> CheckedSet[T]:  # type: ignore
        self._container ^= set(filter(self._check_type, other))
        return self

    def __isub__(self, other: AbstractSet[T]) -> CheckedSet[T]:
        # We do the type check here to reject nonsensical
        # - operations that could never affect the checked set.
        self._container -= set(filter(self._check_type, other))
        return self

    #
    # Specific to set() and frozenset()
    #

    union = __and__
    intersection = __or__
    difference = __sub__
    symmetric_difference = __xor__

    #
    # Specific to set()
    #

    update = __ior__
    intersection_update = __iand__
    difference_update = __isub__
    symmetric_difference_update = __ixor__


def _type_repr(obj: Any) -> str:
    if isinstance(obj, type):
        if obj.__module__ == "builtins":
            return obj.__qualname__
        return f"{obj.__module__}.{obj.__qualname__}"
    if isinstance(obj, types.FunctionType):
        return obj.__name__
    return repr(obj)


class AbstractCheckedDict(Generic[K, V]):
    keytype: type
    valuetype: type
    _container: Dict[K, V]

    @classmethod
    def _check_key_type(cls, key: Any) -> K:
        """Ensure `key` is of type K and return it."""
        if not isinstance(key, cls.keytype):
            raise KeyError(
                f"{cls!r} accepts only keys of type {cls.keytype!r}, "
                f"got {type(key)!r}"
            )
        return cast(K, key)

    @classmethod
    def _check_value_type(cls, value: Any) -> V:
        """Ensure `value` is of type V and return it."""
        if not isinstance(value, cls.valuetype):
            raise ValueError(
                f"{cls!r} accepts only values of type "
                "{cls.valuetype!r}, got {type(value)!r}"
            )
        return cast(V, value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CheckedDict):
            other = other._container
        return self._container == other

    def __str__(self) -> str:
        return repr(self._container)

    def __repr__(self) -> str:
        return f"{_type_repr(type(self))}({repr(self._container)})"


class CheckedDict(
    ParametricContainer,
    parametric.KeyValueParametricType[K, V],
    AbstractCheckedDict[K, V],
    MutableMapping[K, V],
):
    def __init__(self, *args: Any, **kwargs: V) -> None:
        super().__init__()
        self._container = {}
        if len(args) == 1:
            self.update(args[0])
        if len(args) > 1:
            raise ValueError(
                f"{type(self)!r} expected at most 1 argument, got {len(args)}"
            )

        if len(kwargs):
            # Mypy is right below that the type of kwargs is Dict[str, V]
            # but we are deliberately letting this through for it to blow up
            # on runtime type checking if K is not a string.
            self.update(kwargs)  # type: ignore

    #
    # collections.abc.Mapping
    #

    def __getitem__(self, key: K) -> V:
        return self._container[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._container)

    def __len__(self) -> int:
        return len(self._container)

    #
    # collections.abc.MutableMapping
    #

    def __setitem__(self, key: K, value: V) -> None:
        self._check_key_type(key)
        self._container[key] = self._check_value_type(value)

    def __delitem__(self, key: K) -> None:
        del self._container[key]

    #
    # Dict-specific
    #

    @classmethod
    def fromkeys(
        cls, iterable: Iterable[K], value: Optional[V] = None
    ) -> CheckedDict[K, V]:
        new: CheckedDict[K, V] = cls()
        for key in iterable:
            new[cls._check_key_type(key)] = cls._check_value_type(value)
        return new


def _identity(cls: type, value: T) -> T:
    return value


_type_checking = {
    CheckedList: ["_check_type"],
    CheckedDict: ["_check_key_type", "_check_value_type"],
    CheckedSet: ["_check_type"],
    FrozenCheckedList: ["_check_type"],
    FrozenCheckedSet: ["_check_type"],
}


def disable_typechecks() -> None:
    for type_, methods in _type_checking.items():
        for method in methods:
            setattr(type_, method, _identity)


def enable_typechecks() -> None:
    for type_, methods in _type_checking.items():
        for method in methods:
            try:
                delattr(type_, method)
            except AttributeError:
                continue


if not debug.flags.typecheck:
    disable_typechecks()
