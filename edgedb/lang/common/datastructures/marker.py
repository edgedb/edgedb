##
# Copyright (c) 2008-2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class MarkerMeta(type):
    def __repr__(cls):
        repr_ = cls.__repr__
        if repr_ is object.__repr__:
            repr_ = type.__repr__
        return repr_(cls)

    def __str__(cls):
        return cls.__name__


class Marker(metaclass=MarkerMeta):
    def __init__(self):
        raise TypeError('%r cannot be instantiated' % self.__class__.__name__)

    def __str__(cls):
        return cls.__name__

    __repr__ = __str__


class _VoidMeta(MarkerMeta):
    def __bool__(cls):
        return False


class Void(Marker, metaclass=_VoidMeta):
    pass
