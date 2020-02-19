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
from typing import *


T = TypeVar("T")
Adapter_T = TypeVar("Adapter_T", bound="Adapter")


class AdapterError(Exception):
    pass


class Adapter(type):
    adapters: ClassVar[Dict[Any, Dict[type, Adapter]]] = {}
    instance_adapters: ClassVar[Dict[Any, Dict[type, Adapter]]] = {}
    _transparent_adapter_subclass: ClassVar[bool] = False
    __edb_adaptee__: Optional[type]

    def __new__(
        mcls: Type[Adapter_T],
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        adapts: Optional[type] = None,
        adapts_instances_of: Optional[type] = None,
        pure: bool = False,
        adapterargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Adapter_T:

        if adapts is not None and adapts_instances_of is not None:
            msg = 'adapter class: adapts and adapts_instances_of args are ' + \
                  'mutually exclusive'
            raise AdapterError(msg)

        collection = None

        if adapts is not None and not pure:
            bases = bases + (adapts, )

        if adapts_instances_of is not None:
            pure = True
            adapts = adapts_instances_of
            collection = Adapter.instance_adapters
        else:
            collection = Adapter.adapters

        result = cast(
            Adapter_T,
            super().__new__(  # type: ignore
                mcls,
                name,
                bases,
                clsdict,
                **kwargs,
            ),
        )

        if adapts is not None:
            assert issubclass(mcls, Adapter) and mcls is not Adapter

            registry_key = mcls.get_registry_key(adapterargs)
            adapters: Dict[Any, Adapter]

            try:
                adapters = collection[registry_key]
            except KeyError:
                adapters = collection[registry_key] = {}

            mcls.register_adapter(adapters, adapts, result)

        result.__edb_adaptee__ = adapts
        return result

    def __init__(
        cls,
        name: str,
        bases: Tuple[type, ...],
        clsdict: Dict[str, Any],
        *,
        adapts: Optional[type] = None,
        adapts_instances_of: Optional[type] = None,
        pure: bool = False,
        adapterargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(name, bases, clsdict, **kwargs)  # type: ignore

    @classmethod
    def register_adapter(
        mcls,
        registry: Dict[type, Adapter],
        adaptee: type,
        adapter: Adapter,
    ) -> None:
        assert adaptee not in registry
        registry[adaptee] = adapter

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
        collection: Dict[Any, Dict[type, Adapter]],
        kwargs: Dict[str, Any],
    ) -> Optional[Adapter]:
        registry_key = mcls.get_registry_key(kwargs)

        adapters = collection.get(registry_key)
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
    def get_adapter(mcls, obj: Any, **kwargs: Any) -> Optional[Adapter]:
        if isinstance(obj, type):
            collection = Adapter.adapters
            mro = obj.__mro__
        else:
            collection = Adapter.instance_adapters
            mro = type(obj).__mro__

        reversed_mro = tuple(reversed(mro))

        result = mcls._get_adapter(reversed_mro, collection, kwargs)
        if result is not None:
            return result

        for mc in mcls.__subclasses__(mcls):
            result = mc._get_adapter(reversed_mro, collection, kwargs)
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
        elif adapter is not obj.__class__:
            return adapter.adapt(obj)
        else:
            return obj

    @classmethod
    def get_registry_key(
        mcls,
        adapterargs: Optional[Dict[str, Any]],
    ) -> Tuple[Type[Adapter], Optional[FrozenSet[Tuple[str, Any]]]]:
        if mcls._transparent_adapter_subclass:
            for parent in mcls.__mro__[1:]:
                if (issubclass(parent, Adapter) and
                        not parent._transparent_adapter_subclass):
                    registry_holder = parent
                    break
            else:
                raise TypeError(
                    'cannot find Adapter metaclass for mcls'
                )
        else:
            registry_holder = mcls

        if adapterargs:
            return (registry_holder, frozenset(adapterargs.items()))
        else:
            return (registry_holder, None)

    def get_adaptee(cls) -> type:
        adaptee = cls.__edb_adaptee__
        if adaptee is None:
            raise LookupError(f'adapter {cls} has no adaptee type')
        return adaptee

    def has_adaptee(cls) -> bool:
        return cls.__edb_adaptee__ is not None
