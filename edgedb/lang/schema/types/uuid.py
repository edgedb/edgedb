##
# Copyright (c) 2008-2010 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import uuid


from edgedb.lang.common import exceptions as edgedb_error
from edgedb.lang.common.algos.persistent_hash import persistent_hash

from . import base as s_types


_add_impl = s_types.BaseTypeMeta.add_implementation
_add_map = s_types.BaseTypeMeta.add_mapping


class UUID(uuid.UUID):
    def __init__(self, value, *, hex=None, bytes=None, bytes_le=None,
                 fields=None, int=None, version=None):
        try:
            if isinstance(value, uuid.UUID):
                int = value.int
                super().__init__(hex, bytes, bytes_le, fields, int, version)
            else:
                hex = value
                super().__init__(hex, bytes, bytes_le, fields, int, version)

        except ValueError as e:
            raise edgedb_error.AtomValueError(e.args[0]) from e

    def persistent_hash(self):
        return persistent_hash(self.int)


_add_impl('std::uuid', UUID)
_add_map(UUID, 'std::uuid')
_add_map(uuid.UUID, 'std::uuid')
