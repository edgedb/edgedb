##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .cvalue import cvalue, _no_default


__all__ = 'ConfigurableMeta', 'Configurable'


class ConfigurableMeta(type):
    def __new__(mcls, name, bases, dct):
        dct['__sx_configurable__'] = True
        return super().__new__(mcls, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)

        for attrname, attrval in dct.items():
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
