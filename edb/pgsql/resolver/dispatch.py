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
import typing

from edb.pgsql import ast as pgast

from . import context

Base_T = typing.TypeVar('Base_T', bound=pgast.Base)
BaseRelation_T = typing.TypeVar('BaseRelation_T', bound=pgast.BaseRelation)


@functools.singledispatch
def _resolve(
    ir: pgast.Base, *, ctx: context.ResolverContextLevel
) -> pgast.Base:
    raise ValueError(f'no SQL resolve handler for {ir.__class__}')


def resolve(ir: Base_T, *, ctx: context.ResolverContextLevel) -> Base_T:
    res = _resolve(ir, ctx=ctx)
    return typing.cast(Base_T, res)


def resolve_opt(
    ir: typing.Optional[Base_T], *, ctx: context.ResolverContextLevel
) -> typing.Optional[Base_T]:
    if not ir:
        return None
    return resolve(ir, ctx=ctx)


def resolve_list(
    exprs: typing.Sequence[Base_T], *, ctx: context.ResolverContextLevel
) -> typing.List[Base_T]:
    return [resolve(e, ctx=ctx) for e in exprs]


def resolve_opt_list(
    exprs: typing.Optional[typing.List[Base_T]],
    *,
    ctx: context.ResolverContextLevel,
) -> typing.Optional[typing.List[Base_T]]:
    if not exprs:
        return None
    return resolve_list(exprs, ctx=ctx)


@functools.singledispatch
def _resolve_relation(
    ir: pgast.BaseRelation, *, ctx: context.ResolverContextLevel
) -> typing.Tuple[pgast.BaseRelation, context.Table]:
    raise ValueError(f'no SQL resolve handler for {ir.__class__}')


def resolve_relation(
    ir: BaseRelation_T, *, ctx: context.ResolverContextLevel
) -> typing.Tuple[BaseRelation_T, context.Table]:
    res, tab = _resolve_relation(ir, ctx=ctx)
    return typing.cast(BaseRelation_T, res), tab


@_resolve.register
def _resolve_BaseRelation(
    rel: pgast.BaseRelation, *, ctx: context.ResolverContextLevel
) -> pgast.BaseRelation:
    rel, _ = resolve_relation(rel, ctx=ctx)
    return rel
