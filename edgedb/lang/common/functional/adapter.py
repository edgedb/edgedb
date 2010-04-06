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
    def adapt(mcls, obj):
        adapters = Adapter.adapters.get(mcls)

        if adapters:
            for adaptee, adapter in adapters.items():
                for cls in obj.__class__.mro():
                    if issubclass(cls, adapter):
                        return obj
                    elif issubclass(cls, adaptee):
                        return adapter.adapt(obj)

        raise AdapterError('could not find %s.%s adapter for %s' % \
                             (mcls.__module__, mcls.__name__, obj.__class__.__name__))
