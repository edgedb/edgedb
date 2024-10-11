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


from __future__ import annotations

from typing import Literal, Optional, AbstractSet

from edb import errors

from edb.ir import ast as irast
from edb.ir import typeutils as irtyputils

from edb.schema import name as s_name
from edb.schema import pointers as s_pointers
from edb.schema import types as s_types

from . import context


def get_path_id(
    stype: s_types.Type,
    *,
    typename: Optional[s_name.QualName] = None,
    ctx: context.ContextLevel,
) -> irast.PathId:
    return irast.PathId.from_type(
        ctx.env.schema,
        stype,
        typename=typename,
        env=ctx.env,
        namespace=ctx.path_id_namespace)


def get_tuple_indirection_path_id(
    tuple_path_id: irast.PathId,
    element_name: str,
    element_type: s_types.Type,
    *,
    ctx: context.ContextLevel,
) -> irast.PathId:

    ctx.env.schema, src_t = irtyputils.ir_typeref_to_type(
        ctx.env.schema, tuple_path_id.target)
    ptrcls = irast.TupleIndirectionLink(
        src_t,
        element_type,
        element_name=element_name,
    )

    ptrref = irtyputils.ptrref_from_ptrcls(
        schema=ctx.env.schema,
        ptrcls=ptrcls,
        cache=ctx.env.ptr_ref_cache,
        typeref_cache=ctx.env.type_ref_cache,
    )

    return tuple_path_id.extend(ptrref=ptrref)


def get_expression_path_id(
    stype: s_types.Type,
    alias: Optional[str] = None,
    *,
    ctx: context.ContextLevel,
) -> irast.PathId:
    if alias is None:
        alias = ctx.aliases.get('expr')
    typename = s_name.QualName(module='__derived__', name=alias)
    return get_path_id(stype, typename=typename, ctx=ctx)


def register_set_in_scope(
    ir_set: irast.Set,
    *,
    path_scope: Optional[irast.ScopeTreeNode] = None,
    optional: bool = False,
    ctx: context.ContextLevel,
) -> None:
    if path_scope is None:
        path_scope = ctx.path_scope

    path_scope.attach_path(
        ir_set.path_id,
        optional=optional,
        span=ir_set.span,
        ctx=ctx,
    )


def assign_set_scope(
    ir_set: irast.Set,
    scope: Optional[irast.ScopeTreeNode],
    *,
    ctx: context.ContextLevel,
) -> irast.Set:
    if scope is None:
        ir_set.path_scope_id = None
    else:
        if scope.unique_id is None:
            scope.unique_id = ctx.scope_id_ctr.nextval()
            ctx.env.scope_tree_nodes[scope.unique_id] = scope
        ir_set.path_scope_id = scope.unique_id
        if scope.find_child(ir_set.path_id):
            raise RuntimeError('scoped set must not contain itself')

    return ir_set


def get_set_scope(
    ir_set: irast.Set,
    *,
    ctx: context.ContextLevel,
) -> Optional[irast.ScopeTreeNode]:
    if ir_set.path_scope_id is None:
        return None
    else:
        scope = ctx.env.scope_tree_nodes.get(ir_set.path_scope_id)
        if scope is None:
            raise errors.InternalServerError(
                f'dangling scope pointer to node with uid'
                f':{ir_set.path_scope_id} in {ir_set!r}'
            )
        return scope


def extend_path_id(
    path_id: irast.PathId,
    *,
    ptrcls: s_pointers.PointerLike,
    direction: s_pointers.PointerDirection = (
        s_pointers.PointerDirection.Outbound),
    ns: AbstractSet[str] = frozenset(),
    ctx: context.ContextLevel,
) -> irast.PathId:
    """A wrapper over :meth:`ir.pathid.PathId.extend` that also ensures
       the cardinality of *ptrcls* is known at the end of compilation.
    """

    ptrref = irtyputils.ptrref_from_ptrcls(
        schema=ctx.env.schema,
        ptrcls=ptrcls,
        cache=ctx.env.ptr_ref_cache,
        typeref_cache=ctx.env.type_ref_cache,
    )

    return path_id.extend(ptrref=ptrref, direction=direction, ns=ns)


def ban_inserting_path(
    path_id: irast.PathId,
    *,
    location: Literal['body'] | Literal['else'],
    ctx: context.ContextLevel,
) -> None:

    ctx.inserting_paths = ctx.inserting_paths.copy()
    ctx.inserting_paths[path_id] = location


def path_is_inserting(
    path_id: irast.PathId, *, ctx: context.ContextLevel
) -> bool:

    node = ctx.path_scope.find_visible(path_id)
    return bool(
        node
        and node.path_id
        and ctx.inserting_paths.get(node.path_id) == 'body'
    )
