##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class AdapterError(Exception):
    pass


class Adapter(type):
    adapters = {}

    def __new__(mcls, name, bases, clsdict, *, adapts=None):
        if adapts:
            bases = bases + (adapts,)

        result = super().__new__(mcls, name, bases, clsdict)
        if adapts:
            assert issubclass(mcls, Adapter) and mcls is not Adapter

            adapters = Adapter.adapters.get(mcls)
            if not adapters:
                Adapter.adapters[mcls] = adapters = {}

            assert adapts not in adapters
            adapters[adapts] = result

        return result

    def __init__(cls, name, bases, clsdict, *, adapts=None):
        super().__init__(name, bases, clsdict)

    @classmethod
    def get_adapter(mcls, cls):
        for mc in [mcls] + mcls.__subclasses__(mcls):
            adapters = Adapter.adapters.get(mc)

            if adapters:
                for adaptee, adapter in adapters.items():
                    for c in cls.mro():
                        if issubclass(c, adapter):
                            return c
                        elif issubclass(c, adaptee):
                            return adapter
            else:
                adapter = mc.get_adapter(cls)
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
