##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools

from edgedb.lang.ir import ast as irast

from edgedb.server.pgsql import ast as pgast

from . import context


@functools.singledispatch
def compile(
        ir: irast.Base, *, ctx: context.CompilerContextLevel) -> pgast.Base:
    raise NotImplementedError(
        f'no IR compiler handler for {ir.__class__}')
