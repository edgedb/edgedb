##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class Str(str):
    pass

_add_impl('metamagic.caos.builtins.str', Str)
_add_map(Str, 'metamagic.caos.builtins.str')
_add_map(str, 'metamagic.caos.builtins.str')


class StrTypeInfo(s_types.TypeInfo, type=Str):
    def strop(self, other: str) -> 'metamagic.caos.builtins.str':
        pass

    __add__ = strop
    __radd__ = strop
