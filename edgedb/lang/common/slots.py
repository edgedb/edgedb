##
# Copyright (c) 2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import abc


class SlotsMeta(type):
    def __init__(cls, name, bases, dct):
        if '__slots__' not in dct:
            raise TypeError('{}.{} class must have __slots__ defined'. \
                            format(cls.__module__, cls.__name__))

        if type(dct['__slots__']) is not tuple:
            raise TypeError('{}.{} class __slots__ must be a tuple'. \
                            format(cls.__module__, cls.__name__))

        all_slots = set()
        for base in reversed(cls.__mro__):
            try:
                slots = set(base.__slots__)
            except AttributeError:
                pass
            else:
                common = all_slots.intersection(slots)
                if common:
                    raise TypeError('{}.{}: __slots__ intersection detected: {!r}'. \
                                    format(cls.__module__, cls.__name__, common))
                all_slots.update(slots)

        return super().__init__(name, bases, dct)


class SlotsAbstractMeta(SlotsMeta, abc.AbstractMeta):
    pass
