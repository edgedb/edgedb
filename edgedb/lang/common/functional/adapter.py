##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class AdapterError(Exception):
    pass


class Adapter(type):
    adapters = {}

    def __new__(mcls, name, bases, clsdict, *, adapts=None, **kwargs):
        if adapts is not None:
            bases = bases + (adapts,)

        result = super().__new__(mcls, name, bases, clsdict, **kwargs)
        if adapts is not None:
            assert issubclass(mcls, Adapter) and mcls is not Adapter

            adapters = Adapter.adapters.get(mcls)
            if not adapters:
                Adapter.adapters[mcls] = adapters = {}

            assert adapts not in adapters
            adapters[adapts] = result

        result.__sx_adaptee__ = adapts

        return result

    def __init__(cls, name, bases, clsdict, *, adapts=None, **kwargs):
        super().__init__(name, bases, clsdict, **kwargs)

    @classmethod
    def get_adapter(mcls, cls):
        for mc in [mcls] + mcls.__subclasses__(mcls):
            adapters = Adapter.adapters.get(mc)

            if adapters is not None:
                for adaptee, adapter in adapters.items():
                    for c in cls.mro():
                        if issubclass(c, adapter):
                            return c
                        elif issubclass(c, adaptee):
                            return adapter
            elif mc is not mcls:
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

    def get_adaptee(cls):
        return cls.__sx_adaptee__
