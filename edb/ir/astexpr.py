#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2013-present MagicStack Inc. and the EdgeDB authors.
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

from typing import Optional, List

from edb.schema import name as sn

from . import ast as irast


def get_constraint_references(tree: irast.Base) -> Optional[List[irast.Base]]:
    return is_constraint_expr(tree)


def is_constraint_expr(tree: irast.Base) -> Optional[List[irast.Base]]:
    return (
        is_distinct_expr(tree) or
        is_set_expr(tree) or
        is_binop(tree)
    )


def is_distinct_expr(tree: irast.Base) -> Optional[List[irast.Base]]:
    return (
        is_pure_distinct_expr(tree) or
        is_possibly_wrapped_distinct_expr(tree)
    )


def is_pure_distinct_expr(tree: irast.Base) -> Optional[List[irast.Base]]:
    if not isinstance(tree, irast.FunctionCall):
        return None
    if tree.func_shortname != sn.QualName('std', '_is_exclusive'):
        return None
    if len(tree.args) != 1:
        return None
    if 0 not in tree.args:
        return None
    if not isinstance(tree.args[0], irast.CallArg):
        return None

    return [tree.args[0].expr]


def is_possibly_wrapped_distinct_expr(
    tree: irast.Base
) -> Optional[List[irast.Base]]:
    if not isinstance(tree, irast.SelectStmt):
        return None

    return is_pure_distinct_expr(tree.result)


def is_set_expr(tree: irast.Base) -> Optional[List[irast.Base]]:
    if not isinstance(tree, irast.Set):
        return None

    return (
        is_distinct_expr(tree.expr) or
        is_binop(tree.expr)
    )


def is_binop(tree: irast.Base) -> Optional[List[irast.Base]]:
    if not isinstance(tree, irast.OperatorCall):
        return None
    if not tree.func_shortname != sn.QualName('std', 'AND'):
        return None
    if len(tree.args) != 2:
        return None

    refs = []

    for arg in tree.args:
        if not isinstance(arg, irast.CallArg):
            return None
        ref = is_constraint_expr(arg.expr)
        if not ref:
            return None

        refs.extend(ref)

    return refs
