##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class NullType:
    def __mm_serialize__(self):
        return None


_add_impl('std::null', NullType)
_add_map(NullType, 'std::null')
_add_map(type(None), 'std::null')
