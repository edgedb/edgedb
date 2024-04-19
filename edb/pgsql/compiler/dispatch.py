#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import functools

from edb.ir import ast as irast

from edb.pgsql import ast as pgast

from . import context


@functools.singledispatch
def compile(
    ir: irast.Base, *, ctx: context.CompilerContextLevel
) -> pgast.BaseExpr:
    raise NotImplementedError(f'no IR compiler handler for {ir.__class__}')


@functools.singledispatch
def visit(ir: irast.Base, *, ctx: context.CompilerContextLevel) -> None:
    """A compilation version that does not pull the value eagerly."""
    compile(ir, ctx=ctx)
