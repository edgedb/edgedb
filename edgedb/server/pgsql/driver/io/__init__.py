##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import importlib
from itertools import cycle, chain

import postgresql.types
from postgresql.types import io as pg_io


io_modules = {
    'datetime': {
        postgresql.types.INTERVALOID,
        postgresql.types.TIMESTAMPTZOID
    }
}


module_io = dict(
    chain.from_iterable((
        zip(x[1], cycle((x[0],))) for x in io_modules.items()
    ))
)


def resolve(oid):
    io = module_io.get(oid)
    if io is None:
        return pg_io.resolve(oid)
    if io.__class__ is str:
        module_io.update(importlib.import_module(__name__ + '.' + io).oid_to_io)
        io = module_io[oid]

    return io
