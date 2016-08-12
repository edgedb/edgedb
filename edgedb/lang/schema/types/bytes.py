##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


class Bytes(bytes):
    pass


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping

_add_impl('std::bytes', Bytes)
_add_map(Bytes, 'std::bytes')
_add_map(bytes, 'std::bytes')


class BytesTypeInfo(s_types.TypeInfo, type=Bytes):
    def bytop(self, other: bytes) -> 'std::bytes':
        pass

    __add__ = bytop
    __radd__ = bytop
