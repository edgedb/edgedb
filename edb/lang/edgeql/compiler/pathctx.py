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


"""EdgeQL compiler path scope helpers."""


import typing

from edb import errors

from edb.lang.ir import ast as irast

from edb.lang.schema import objects as s_obj

from . import context


def get_path_id(stype: s_obj.Object, *,
                typename: typing.Optional[str]=None,
                ctx: context.CompilerContext) -> irast.PathId:
    return irast.PathId.from_type(
        ctx.env.schema, stype,
        typename=typename,
        namespace=ctx.path_id_namespace)


def register_set_in_scope(
        ir_set: irast.Set, *,
        path_scope: irast.ScopeTreeNode=None,
        ctx: context.CompilerContext) -> None:
    if path_scope is None:
        path_scope = ctx.path_scope

    try:
        path_scope.attach_path(ir_set.path_id)
    except irast.InvalidScopeConfiguration as e:
        raise errors.EdgeQLSyntaxError(
            e.args[0], context=ir_set.context) from e


def assign_set_scope(
        ir_set: irast.Set, scope: typing.Optional[irast.ScopeTreeNode], *,
        ctx: context.ContextLevel) -> irast.Set:
    if scope is None:
        ir_set.path_scope_id = None
    else:
        if scope.unique_id is None:
            scope.unique_id = ctx.scope_id_ctr.nextval()
        ir_set.path_scope_id = scope.unique_id
        if scope.find_child(ir_set.path_id):
            raise RuntimeError('scoped set must not contain itself')

    return ir_set


def get_set_scope(
        ir_set: irast.Set, *,
        ctx: context.ContextLevel) -> typing.Optional[irast.ScopeTreeNode]:
    if ir_set.path_scope_id is None:
        return None
    else:
        return ctx.path_scope.root.find_by_unique_id(ir_set.path_scope_id)


def mark_path_as_optional(
        path_id: irast.PathId, *,
        ctx: context.CompilerContext) -> None:
    ctx.path_scope.mark_as_optional(path_id)


def set_path_alias(
        path_id: irast.PathId, alias: irast.PathId, *,
        ctx: context.CompilerContext) -> None:
    ctx.path_scope.set_alias(path_id, alias)
