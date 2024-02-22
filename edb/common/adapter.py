#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Optional, Tuple, Type, TypeVar, Dict, Set


T = TypeVar("T")
Adapter_T = TypeVar("Adapter_T", bound="Adapter")


class AdapterError(Exception):
    pass


_adapters: Dict[Any, Dict[type, Adapter]] = {}


class Adapter(type):
    __edb_adaptee__: Optional[type]

    def __new__(
        mcls: Type[Adapter_T],
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        adapts: Optional[type] = None,
        **kwargs: Any,
    ) -> Adapter_T:

        if adapts is not None:
            bases = bases + (adapts, )

        clsdict['__edb_adaptee__'] = adapts

        result = super().__new__(mcls, name, bases, clsdict, **kwargs)

        if adapts is not None:
            assert issubclass(mcls, Adapter) and mcls is not Adapter

            try:
                adapters = _adapters[mcls]
            except KeyError:
                adapters = _adapters[mcls] = {}

            assert adapts not in adapters
            adapters[adapts] = result

        return result

    def __init__(
        cls,
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        adapts: Optional[type] = None,
        **kwargs: Any,
    ):
        super().__init__(name, bases, clsdict, **kwargs)

    @classmethod
    def _match_adapter(
        mcls,
        obj: type,
        adaptee: type,
        adapter: Adapter,
    ) -> Optional[Adapter]:
        if issubclass(obj, adapter) and obj is not adapter:
            # mypy bug below
            return obj  # type: ignore
        elif issubclass(obj, adaptee):
            return adapter
        else:
            return None

    @classmethod
    def _get_adapter(
        mcls,
        reversed_mro: Tuple[type, ...],
    ) -> Optional[Adapter]:
        adapters = _adapters.get(mcls)
        if adapters is None:
            return None

        result = None
        seen: Set[Adapter] = set()
        for base in reversed_mro:
            for adaptee, adapter in adapters.items():
                found = mcls._match_adapter(base, adaptee, adapter)

                if found and found not in seen:
                    result = found
                    seen.add(found)

        return result

    @classmethod
    def get_adapter(mcls, obj: Any) -> Optional[Adapter]:
        mro = obj.__mro__

        reversed_mro = tuple(reversed(mro))

        result = mcls._get_adapter(reversed_mro)
        if result is not None:
            return result

        for mc in mcls.__subclasses__(mcls):
            result = mc._get_adapter(reversed_mro)
            if result is not None:
                return result

        return None

    @classmethod
    def adapt(mcls, obj: T) -> T:
        adapter = mcls.get_adapter(obj.__class__)
        if adapter is None:
            raise AdapterError(
                'could not find {}.{} adapter for {}'.format(
                    mcls.__module__, mcls.__name__, obj.__class__.__name__))
        elif adapter is not obj.__class__:  # type: ignore
            return adapter.adapt(obj)
        else:
            return obj

    def get_adaptee(cls) -> type:
        adaptee = cls.__edb_adaptee__
        if adaptee is None:
            raise LookupError(f'adapter {cls} has no adaptee type')
        return adaptee

    def has_adaptee(cls) -> bool:
        return cls.__edb_adaptee__ is not None
