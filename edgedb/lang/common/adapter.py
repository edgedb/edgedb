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


class AdapterError(Exception):
    pass


class Adapter(type):
    adapters = {}
    instance_adapters = {}
    _transparent_adapter_subclass = False

    def __new__(
            mcls, name, bases, clsdict, *, adapts=None,
            adapts_instances_of=None, pure=False, adapterargs=None, **kwargs):

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

        result = super().__new__(mcls, name, bases, clsdict, **kwargs)
        if adapts is not None:
            assert issubclass(mcls, Adapter) and mcls is not Adapter

            registry_key = mcls.get_registry_key(adapterargs)

            try:
                adapters = collection[registry_key]
            except KeyError:
                adapters = collection[registry_key] = {}

            mcls.register_adapter(adapters, adapts, result)

        result.__sx_adaptee__ = adapts
        return result

    def __init__(
            cls, name, bases, clsdict, *, adapts=None,
            adapts_instances_of=None, pure=False, adapterargs=None, **kwargs):
        super().__init__(name, bases, clsdict, **kwargs)

    @classmethod
    def register_adapter(mcls, registry, adaptee, adapter):
        assert adaptee not in registry
        registry[adaptee] = adapter

    @classmethod
    def match_adapter(mcls, obj, adaptee, adapter):
        if issubclass(obj, adapter) and obj is not adapter:
            return obj
        elif issubclass(obj, adaptee):
            return adapter

    @classmethod
    def _get_adapter(mcls, obj, reversed_mro, collection, kwargs):
        registry_key = mcls.get_registry_key(kwargs)

        adapters = collection.get(registry_key)
        if adapters is None:
            return

        result = None
        seen = set()
        for base in reversed_mro:
            for adaptee, adapter in adapters.items():
                found = mcls.match_adapter(base, adaptee, adapter)

                if found and found not in seen:
                    result = found
                    seen.add(found)

        if result is not None:
            return result

    @classmethod
    def get_adapter(mcls, obj, **kwargs):
        if isinstance(obj, type):
            collection = Adapter.adapters
            mro = obj.__mro__
        else:
            collection = Adapter.instance_adapters
            mro = type(obj).__mro__

        reversed_mro = tuple(reversed(mro))

        result = mcls._get_adapter(obj, reversed_mro, collection, kwargs)
        if result is not None:
            return result

        for mc in mcls.__subclasses__(mcls):
            result = mc._get_adapter(obj, reversed_mro, collection, kwargs)
            if result is not None:
                return result

    @classmethod
    def adapt(mcls, obj):
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
    def get_registry_key(mcls, adapterargs):
        if mcls._transparent_adapter_subclass:
            for parent in mcls.__mro__[1:]:
                if not parent._transparent_adapter_subclass:
                    registry_holder = parent
                    break
        else:
            registry_holder = mcls

        if adapterargs:
            return (registry_holder, frozenset(adapterargs.items()))
        else:
            return registry_holder

    def get_adaptee(cls):
        return cls.__sx_adaptee__
