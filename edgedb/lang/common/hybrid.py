##
# Copyright (c) 2008-2010, 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types


class method:
    __slots__ = ('__wrapped__', )

    def __init__(self, func):
        self.__wrapped__ = func

    def __get__(self, obj, cls=None):
        if obj is None:
            return types.MethodType(self.__wrapped__, cls)
        else:
            return types.MethodType(self.__wrapped__, obj)
