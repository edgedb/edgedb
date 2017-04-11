##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools

from edgedb.lang.edgeql import ast as qlast
from edgedb.lang.ir import ast as irast

from . import context


@functools.singledispatch
def compile(node: qlast.Base, *, ctx: context.ContextLevel) -> irast.Base:
    raise NotImplementedError(
        f'no EdgeQL compiler handler for {node.__class__}')
