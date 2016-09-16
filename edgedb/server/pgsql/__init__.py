##
# Copyright (c) 2008-2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from . import ast  # NOQA
from . import backend
from . import codegen  # NOQA
from . import common  # NOQA


async def open_database(pgconn):
    await pgconn.set_builtin_type_codec(
        'hstore', schema='edgedb', codec_name='pg_contrib.hstore')

    bk = backend.Backend(pgconn)
    await bk.getschema()
    return bk
