##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .cvalue import cvalue, _no_default


__all__ = 'ConfigurableMeta', 'Configurable'


class ConfigurableMeta(type):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        for attrname, attrval in tuple(cls.__dict__.items()):
            if not callable(attrval) and isinstance(attrval, cvalue):
                attrval._owner = cls
                attrval._set_name(attrname)

                if attrval._default is not _no_default:
                    attrval._validate(attrval._default, attrval.fullname, 'class definition')


class Configurable(metaclass=ConfigurableMeta):
    # For compatibility with objects that use __slots__.  Zero impact
    # on normal objects with __dict__.
    #
    __slots__ = ()
