##
# Copyright (c) 2008-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class AdapterError(Exception):
    pass


class Adapter(type):
    adapters = {}

    def __new__(mcls, name, bases, clsdict, *, adapts=None, adapts_instances_of=None, pure=False,
                                                            adapterargs=None, **kwargs):

        if adapts is not None and adapts_instances_of is not None:
            msg = 'adapter class: adapts and adapts_instances_of args are mutually exclusive'
            raise AdapterError(msg)

        if adapts_instances_of is not None:
            pure = True
            adapts = adapts_instances_of

        if adapts is not None and not pure:
            bases = bases + (adapts,)

        result = super().__new__(mcls, name, bases, clsdict, **kwargs)
        if adapts is not None:
            assert issubclass(mcls, Adapter) and mcls is not Adapter

            registry_key = mcls.get_registry_key(adapterargs)

            try:
                adapters = Adapter.adapters[registry_key]
            except KeyError:
                adapters = Adapter.adapters[registry_key] = {}

            mcls.register_adapter(adapters, adapts, adapts_instances_of is not None, result)

        result.__sx_adaptee__ = adapts

        return result

    def __init__(cls, name, bases, clsdict, *, adapts=None, adapts_instances_of=None, pure=False,
                                                            adapterargs=None, **kwargs):
        super().__init__(name, bases, clsdict, **kwargs)

    @classmethod
    def register_adapter(mcls, registry, adaptee, adapting_instances, adapter):
        assert (adaptee, adapting_instances) not in registry
        registry[(adaptee, adapting_instances)] = adapter

    @classmethod
    def match_adapter(mcls, obj, adaptee, adapting_instances, adapter):
        if adapting_instances:
            if isinstance(obj, adaptee):
                return adapter
        elif isinstance(obj, type):
            if issubclass(obj, adapter) and obj is not adapter:
                return obj
            elif issubclass(obj, adaptee):
                return adapter

    @classmethod
    def get_adapter(mcls, cls, **kwargs):
        for mc in [mcls] + mcls.__subclasses__(mcls):
            registry_key = mc.get_registry_key(kwargs)
            adapters = Adapter.adapters.get(registry_key)

            if adapters is not None:
                for (adaptee, adapting_instances), adapter in adapters.items():
                    if adapting_instances:
                        result = mcls.match_adapter(cls, adaptee, adapting_instances, adapter)
                        if result is not None:
                            return result
                    else:
                        mro = getattr(cls, '__mro__', (cls,))

                        for c in mro:
                            result = mcls.match_adapter(c, adaptee, adapting_instances, adapter)
                            if result is not None:
                                return result

            elif mc is not mcls:
                adapter = mc.get_adapter(cls, **kwargs)
                if adapter:
                    return adapter

    @classmethod
    def adapt(mcls, obj):
        adapter = mcls.get_adapter(obj.__class__)
        if adapter is None:
            raise AdapterError('could not find %s.%s adapter for %s' % \
                               (mcls.__module__, mcls.__name__, obj.__class__.__name__))
        elif adapter is not obj.__class__:
            return adapter.adapt(obj)
        else:
            return obj

    @classmethod
    def get_registry_key(mcls, adapterargs):
        if adapterargs:
            return (mcls, frozenset(adapterargs.items()))
        else:
            return mcls

    def get_adaptee(cls):
        return cls.__sx_adaptee__


class MultiAdapter(Adapter):
    @classmethod
    def register_adapter(mcls, registry, adaptee, adapting_instances, adapter):
        try:
            registry[adaptee, adapting_instances] += (adapter,)
        except KeyError:
            registry[adaptee, adapting_instances] = (adapter,)

    @classmethod
    def match_adapter(mcls, obj, adaptee, adapting_instances, adapter):
        if adapting_instances:
            if isinstance(obj, adaptee):
                return adapter
        elif isinstance(obj, type):
            if issubclass(obj, adapter) and obj not in adapter:
                return (obj,)
            elif issubclass(obj, adaptee):
                return adapter
