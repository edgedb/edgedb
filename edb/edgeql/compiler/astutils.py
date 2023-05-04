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


"""EdgeQL compiler helpers for AST classification and basic transforms."""


from __future__ import annotations
from typing import *

from edb.common import ast
from edb.common import view_patterns

from edb.edgeql import ast as qlast


def extend_binop(
    binop: Optional[qlast.Expr],
    *exprs: qlast.Expr,
    op: str = 'AND',
) -> qlast.Expr:
    exprlist = list(exprs)

    if binop is None:
        result = exprlist.pop(0)
    else:
        result = binop

    for expr in exprlist:
        if expr is not None and expr is not result:
            result = qlast.BinOp(
                left=result,
                right=expr,
                op=op,
            )

    return result


def ensure_ql_query(expr: qlast.Expr) -> qlast.Query:

    # a sanity check added after refactoring AST
    assert isinstance(expr, qlast.Expr)

    if not isinstance(expr, qlast.Query):
        expr = qlast.SelectQuery(
            result=expr,
            implicit=True,
        )
    return expr


def ensure_ql_select(expr: qlast.Expr) -> qlast.SelectQuery:
    if not isinstance(expr, qlast.SelectQuery):
        expr = qlast.SelectQuery(
            result=expr,
            implicit=True,
        )
    return expr


def is_ql_empty_set(expr: qlast.Expr) -> bool:
    return isinstance(expr, qlast.Set) and len(expr.elements) == 0


def is_ql_path(qlexpr: qlast.Expr) -> bool:
    if isinstance(qlexpr, qlast.Shape):
        if qlexpr.expr:
            qlexpr = qlexpr.expr

    if not isinstance(qlexpr, qlast.Path):
        return False

    start = qlexpr.steps[0]

    return isinstance(start, (qlast.Source, qlast.ObjectRef, qlast.Ptr))


def is_nontrivial_shape_element(shape_el: qlast.ShapeElement) -> bool:
    return bool(
        shape_el.where
        or shape_el.orderby
        or shape_el.offset
        or shape_el.limit
        or shape_el.compexpr
        or (
            shape_el.elements and
            any(is_nontrivial_shape_element(el) for el in shape_el.elements)
        )
    )


def extend_path(expr: qlast.Expr, field: str) -> qlast.Path:
    step = qlast.Ptr(ptr=qlast.ObjectRef(name=field))

    if isinstance(expr, qlast.Path):
        return qlast.Path(
            steps=[*expr.steps, step],
            partial=expr.partial,
        )
    else:
        return qlast.Path(steps=[expr, step])


class FindParams(ast.NodeVisitor):
    """Visitor to find all the parameters.

    The annoying bit is that we also need all the modaliases.
    """
    def __init__(self, modaliases: Dict[Optional[str], str]) -> None:
        super().__init__()
        self.params: List[
            Tuple[qlast.TypeCast, Dict[Optional[str], str]]] = []
        self.modaliases = modaliases

    def visit_Command(self, n: qlast.Command) -> None:
        self._visit_with_stmt(n)

    def visit_Query(self, n: qlast.Query) -> None:
        self._visit_with_stmt(n)

    def _visit_with_stmt(self, n: qlast.Statement) -> None:
        old = self.modaliases
        for with_entry in (n.aliases or ()):
            if isinstance(with_entry, qlast.ModuleAliasDecl):
                self.modaliases = self.modaliases.copy()
                self.modaliases[with_entry.alias] = with_entry.module
            else:
                self.visit(with_entry)

        # The memoization will prevent us from redoing the aliases
        self.generic_visit(n)
        self.modaliases = old

    def visit_TypeCast(self, n: qlast.TypeCast) -> None:
        if isinstance(n.expr, qlast.Parameter):
            self.params.append((n, self.modaliases))
        self.generic_visit(n)


def find_parameters(
    ql: qlast.Base, modaliases: Dict[Optional[str], str]
) -> List[Tuple[qlast.TypeCast, Dict[Optional[str], str]]]:
    """Get all query parameters"""
    v = FindParams(modaliases)
    v.visit(ql)
    return v.params


class alias_view(
    view_patterns.ViewPattern[tuple[str, list[qlast.PathElement]]],
    targets=(qlast.Base,),
):
    @staticmethod
    def match(obj: object) -> tuple[str, list[qlast.PathElement]]:
        match obj:
            case qlast.Path(
                steps=[qlast.ObjectRef(module=None, name=alias), *rest],
                partial=False,
            ):
                return alias, rest
        raise view_patterns.NoMatch
