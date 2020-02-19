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

from edb.edgeql import ast as qlast

from edb.schema import schema as s_schema
from edb.schema import types as s_types
from edb.schema import utils as s_utils


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


def ensure_qlstmt(expr: qlast.Expr) -> qlast.Statement:
    if not isinstance(expr, qlast.Statement):
        expr = qlast.SelectQuery(
            result=expr,
            implicit=True,
        )
    return expr


def is_ql_empty_set(expr: qlast.Expr) -> bool:
    return isinstance(expr, qlast.Set) and len(expr.elements) == 0


def is_ql_path(qlexpr: qlast.Expr) -> bool:
    if isinstance(qlexpr, qlast.Shape):
        qlexpr = qlexpr.expr

    if not isinstance(qlexpr, qlast.Path):
        return False

    start = qlexpr.steps[0]

    return isinstance(start, (qlast.Source, qlast.ObjectRef, qlast.Ptr))


def type_to_ql_typeref(t: s_types.Type, *,
                       schema: s_schema.Schema) -> qlast.TypeExpr:
    return s_utils.typeref_to_ast(schema, t)
