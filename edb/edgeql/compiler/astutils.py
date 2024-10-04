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
from dataclasses import dataclass, field
from typing import Optional, Tuple, Dict, List, TYPE_CHECKING

from edb.common import ast
from edb.common import view_patterns

from edb.edgeql import ast as qlast
from edb.edgeql import qltypes

from edb.schema import name as sn

if TYPE_CHECKING:

    from edb.schema import functions as s_func

    from . import context


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


def is_ql_empty_array(expr: qlast.Expr) -> bool:
    return isinstance(expr, qlast.Array) and len(expr.elements) == 0


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
    step = qlast.Ptr(name=field)

    if isinstance(expr, qlast.Path):
        return qlast.Path(
            steps=[*expr.steps, step],
            partial=expr.partial,
        )
    else:
        return qlast.Path(steps=[expr, step])


@dataclass
class Params:
    cast_params: List[
        Tuple[qlast.TypeCast, Dict[Optional[str], str]]
    ] = field(default_factory=list)
    shaped_params: List[
        Tuple[qlast.Parameter, qlast.Shape]
    ] = field(default_factory=list)
    loose_params: List[qlast.Parameter] = field(default_factory=list)


class FindParams(ast.NodeVisitor):
    """Visitor to find all the parameters.

    The annoying bit is that we also need all the modaliases.
    """
    def __init__(self, modaliases: Dict[Optional[str], str]) -> None:
        super().__init__()
        self.params: Params = Params()
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
            self.params.cast_params.append((n, self.modaliases))
        elif isinstance(n.expr, qlast.Shape):
            if isinstance(n.expr.expr, qlast.Parameter):
                self.params.shaped_params.append((n.expr.expr, n.expr))
            else:
                self.generic_visit(n)
        else:
            self.generic_visit(n)

    def visit_Parameter(self, n: qlast.Parameter) -> None:
        self.params.loose_params.append(n)

    def visit_CreateFunction(self, n: qlast.CreateFunction) -> None:
        pass

    def visit_CreateConstraint(self, n: qlast.CreateFunction) -> None:
        pass


def find_parameters(
    ql: qlast.Base, modaliases: Dict[Optional[str], str]
) -> Params:
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


def contains_dml(
    ql_expr: qlast.Base,
    *,
    ctx: context.ContextLevel
    ) -> bool:
    """Check whether a expression contains any DML in a subtree."""
    # If this ends up being a perf problem, we can use a visitor
    # directly and cache.
    dml_types = (qlast.InsertQuery, qlast.UpdateQuery, qlast.DeleteQuery)
    if isinstance(ql_expr, dml_types):
        return True

    res = ast.find_children(
        ql_expr, qlast.Base,
        lambda x: (
            isinstance(x, dml_types)
            or (isinstance(x, qlast.IRAnchor) and x.has_dml)
            or (
                isinstance(x, qlast.FunctionCall)
                and any(
                    (
                        func.get_volatility(ctx.env.schema)
                        == qltypes.Volatility.Modifying
                    )
                    for func in _get_functions_from_call(x, ctx=ctx)
                )
            )
        ),
        terminate_early=True,
    )

    return bool(res)


def _get_functions_from_call(
    expr: qlast.FunctionCall,
    *,
    ctx: context.ContextLevel,
) -> tuple[s_func.Function, ...]:
    funcname: sn.Name
    if isinstance(expr.func, str):
        funcname = sn.UnqualName(expr.func)
    else:
        funcname = sn.QualName(*expr.func)

    return ctx.env.schema.get_functions(
        funcname,
        default=(),
        module_aliases=ctx.modaliases,
    )
