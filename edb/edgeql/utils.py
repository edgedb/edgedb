#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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

import copy
import itertools
from typing import Any, Optional, Mapping, Dict, List, Set

from edb import errors
from edb.common import ast
from edb.schema import schema as s_schema
from edb.schema import functions as s_func

from . import ast as qlast


FREE_SHAPE_EXPR = qlast.DetachedExpr(
    expr=qlast.Path(
        steps=[qlast.ObjectRef(module='std', name='FreeObject')],
        allow_factoring=True,
    ),
)


class ParameterInliner(ast.NodeTransformer):

    def __init__(self, args_map: Mapping[str, qlast.Base]) -> None:
        super().__init__()
        self.args_map = args_map

    def visit_Path(self, node: qlast.Path) -> qlast.Base:
        if len(node.steps) != 1 or not isinstance(
            node.steps[0], qlast.ObjectRef
        ):
            self.visit(node.steps[0])
            return node

        ref: qlast.ObjectRef = node.steps[0]
        try:
            arg = self.args_map[ref.name]
        except KeyError:
            return node

        arg = copy.deepcopy(arg)
        return arg


def inline_parameters(
    ql_expr: qlast.Base, args: Mapping[str, qlast.Base]
) -> None:

    inliner = ParameterInliner(args)
    inliner.visit(ql_expr)


def index_parameters(
    ql_args: List[qlast.Base],
    *,
    parameters: s_func.ParameterLikeList,
    schema: s_schema.Schema
) -> Dict[str, qlast.Base]:

    result: Dict[str, qlast.Base] = {}
    varargs: Optional[List[qlast.Expr]] = None
    variadic = parameters.find_variadic(schema)
    variadic_num = variadic.get_num(schema) if variadic else -1  # type: ignore

    params = parameters.objects(schema)

    if not variadic and len(ql_args) > len(params):
        # In error message we discount the implicit __subject__ param.
        raise errors.SchemaDefinitionError(
            f'Expected {len(params) - 1} arguments, but found '
            f'{len(ql_args) - 1}',
            span=ql_args[-1].span,
            details='Did you mean to use ON (...) for specifying the subject?',
        )

    e: qlast.Expr
    p: s_func.ParameterLike
    for iter in itertools.zip_longest(
        enumerate(ql_args), params, fillvalue=None
    ):
        (i, e), p = iter  # type: ignore
        if isinstance(e, qlast.SelectQuery):
            e = e.result

        if variadic and variadic_num == i:
            assert varargs is None
            varargs = []
            result[p.get_parameter_name(schema)] = qlast.Array(
                elements=varargs
            )

        if varargs is not None:
            varargs.append(e)
        else:
            result[p.get_parameter_name(schema)] = e

    return result


class AnchorInliner(ast.NodeTransformer):

    def __init__(self, anchors: Mapping[str, qlast.Base]) -> None:
        super().__init__()
        self.anchors = anchors

    def visit_Path(self, node: qlast.Path) -> qlast.Path:
        if not node.steps:
            return node

        step0 = node.steps[0]

        if isinstance(step0, qlast.Anchor):
            node.steps[0] = self.anchors[step0.name]  # type: ignore
        elif isinstance(step0, qlast.ObjectRef) and step0.name in self.anchors:
            node.steps[0] = self.anchors[step0.name]  # type: ignore

        return node


def inline_anchors(
    ql_expr: qlast.Base, anchors: Mapping[Any, qlast.Base]
) -> None:

    inliner = AnchorInliner(anchors)
    inliner.visit(ql_expr)


def find_paths(ql: qlast.Base) -> List[qlast.Path]:
    return ast.find_children(ql, qlast.Path)


def find_subject_ptrs(ast: qlast.Base) -> Set[str]:
    ptrs = set()
    for path in find_paths(ast):
        if path.partial:
            p = path.steps[0]
        elif is_anchor(path.steps[0], '__subject__') and len(path.steps) > 1:
            p = path.steps[1]
        else:
            continue

        if isinstance(p, qlast.Ptr):
            ptrs.add(p.name)
    return ptrs


def is_anchor(expr: qlast.PathElement, name: str) -> bool:
    return isinstance(expr, qlast.Anchor) and expr.name == name


def subject_paths_substitute(
    ast: qlast.Base_T,
    subject_ptrs: Dict[str, qlast.Expr],
) -> qlast.Base_T:
    ast = copy.deepcopy(ast)
    for path in find_paths(ast):
        if path.partial and isinstance(path.steps[0], qlast.Ptr):
            path.steps[0] = subject_paths_substitute(
                subject_ptrs[path.steps[0].name],
                subject_ptrs,
            )
        elif (
            is_anchor(path.steps[0], '__subject__')
            and len(path.steps)
            and isinstance(path.steps[1], qlast.Ptr)
        ):
            path.steps[0:2] = [subject_paths_substitute(
                subject_ptrs[path.steps[1].name],
                subject_ptrs,
            )]
    return ast


def subject_substitute(
    ast: qlast.Base_T, new_subject: qlast.Expr
) -> qlast.Base_T:
    ast = copy.deepcopy(ast)
    # If the subject is a path (usually will be), graft the path
    # elements directly to avoid an extra SelectStmt/Set in the IR,
    # which can result in worse codegen (unnecessary semijoins, for
    # example).
    # TODO: Unify other substitution functions.
    if isinstance(new_subject, qlast.Path):
        new_partial = new_subject.partial
        new_head = new_subject.steps
    else:
        new_partial = False
        new_head = [new_subject]

    for path in find_paths(ast):
        if is_anchor(path.steps[0], '__subject__'):
            path.steps[0:1] = new_head
            path.partial = new_partial
        elif path.partial:
            path.steps[0:0] = new_head
            path.partial = new_partial
    return ast


def is_enum(type_name: qlast.TypeName):
    return (
        isinstance(type_name.maintype, (qlast.TypeName, qlast.ObjectRef))
        and type_name.maintype.name == "enum"
        and type_name.subtypes
    )
