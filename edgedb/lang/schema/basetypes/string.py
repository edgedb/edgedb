##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Str(str):
    pass


_add_impl('std::str', Str)
_add_map(Str, 'std::str')
_add_map(str, 'std::str')


class StdStr(s_types.SchemaClass, name='std::str'):
    pass


class StrTypeInfo(s_types.TypeInfo, type=Str):
    def strop(self, other: str) -> StdStr:
        pass

    __add__ = strop
    __radd__ = strop
