##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class JSON(bytes):
    pass


_add_impl('std::json', JSON)
_add_map(JSON, 'std::json')
