##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import abc


def validate_class(name, bases, dct):
    if '__slots__' not in dct:
        raise TypeError('%s(%s) class: must have __slots__ defined' % \
                                        (name, ', '.join(base.__name__ for base in bases)))

    if type(dct['__slots__']) is not tuple:
        raise TypeError('%s(%s) class: __slots__ must be of type tuple' % \
                                        (name, ', '.join(base.__name__ for base in bases)))


class SlotsMeta(type):
    def __new__(mcls, name, bases, dct):
        validate_class(name, bases,dct)
        return super().__new__(mcls, name, bases, dct)


class SlotsAbstractMeta(SlotsMeta, abc.AbstractMeta):
    pass
