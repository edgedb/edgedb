##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class NoneType:
    def __mm_serialize__(self):
        return None


_add_impl('std.none', NoneType)
_add_map(NoneType, 'std.none')
_add_map(type(None), 'std.none')
