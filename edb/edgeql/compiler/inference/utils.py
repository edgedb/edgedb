#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

"""Common utilities used in inferers."""


from __future__ import annotations
from typing import Optional

from edb import errors
from edb.ir import ast as irast

from . import context as inf_ctx


def get_set_scope(
    ir_set: irast.Set,
    scope_tree: irast.ScopeTreeNode,
    ctx: inf_ctx.InfCtx,
) -> irast.ScopeTreeNode:

    if ir_set.path_scope_id:
        new_scope = ctx.env.scope_tree_nodes.get(ir_set.path_scope_id)
        if new_scope is None:
            raise errors.InternalServerError(
                f'dangling scope pointer to node with uid'
                f':{ir_set.path_scope_id} in {ir_set!r}'
            )
    else:
        new_scope = scope_tree

    return new_scope


def find_visible(
    ir: irast.Set,
    scope_tree: irast.ScopeTreeNode,
) -> Optional[irast.ScopeTreeNode]:
    # We want to look one fence up from whatever our current fence is.
    # (Most of the time, scope_tree will be a fence, so this is equivalent
    # to parent_fence, but sometimes it will be a branch.)
    outer_fence = scope_tree.fence.parent_fence
    if outer_fence is not None:
        if scope_tree.namespaces:
            path_id = ir.path_id.strip_namespace(scope_tree.namespaces)
        else:
            path_id = ir.path_id

        return outer_fence.find_visible(path_id)
    else:
        return None
