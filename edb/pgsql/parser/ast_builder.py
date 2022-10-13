#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2010-present MagicStack Inc. and the EdgeDB authors.
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


from typing import *

from edb.pgsql import ast as pgast
from edb.edgeql import ast as qlast
from edb.pgsql.parser.exceptions import PSqlUnsupportedError

# Node = bool | str | int | float | List[Any] | dict[str, Any]
Node = Any
T = TypeVar("T")
U = TypeVar("U")


def build_queries(node: Node) -> List[pgast.Query]:
    return [_build_query(node["stmt"]) for node in node["stmts"]]


def _maybe(node: Node, name: str, builder: Callable[[Node], T]) -> Optional[T]:
    if name in node:
        return builder(node[name])
    return None


def _ident(t: T) -> U:
    return t  # type: ignore


def _list(
    node: Node,
    name: str,
    builder: Callable[[Node], T],
    mapper: Callable[[T], U] = _ident,
) -> List[U]:
    return [mapper(builder(n)) for n in node[name]]


def _maybe_list(
    node: Node,
    name: str,
    builder: Callable[[Node], T],
    mapper: Callable[[T], U] = _ident,
) -> Optional[List[U]]:
    return _list(node, name, builder, mapper) if name in node else None


def _enum(
    node: Node,
    builders: dict[str, Callable[[Node], T]],
    fallbacks: Sequence[Callable[[Node], T]] = (),
) -> T:
    for name in builders:
        if name in node:
            builder = builders[name]
            return builder(node[name])
    for fallback in fallbacks:
        try:
            return fallback(node)
        except PSqlUnsupportedError:
            pass
    raise PSqlUnsupportedError(f"unknown enum: {node}")


def _build_any(node: Node) -> Any:
    return node


def _build_str(node: Node) -> str:
    node = _unwrap(node, "String")
    node = _unwrap(node, "str")
    return str(node)


def _build_bool(node: Node) -> bool:
    return node


def _bool_or_false(node: Node, name: str) -> bool:
    return node[name] if name in node else False


def _unwrap(node: Node, name: str) -> pgast.Query:
    if isinstance(node, dict) and name in node:
        return node[name]
    return node


def _probe(n: Node, keys: List[str | int]) -> bool:
    for key in keys:
        contained = key in n if isinstance(key, str) else key < len(n)
        if contained:
            n = n[key]
        else:
            return False
    return True


def _as_column_ref(name: str) -> pgast.ColumnRef:
    return pgast.ColumnRef(
        name=(name,),
    )


def _build_query(node: Node) -> pgast.Query:
    return _enum(
        node,
        {
            "SelectStmt": _build_select_stmt,
            "InsertStmt": _build_insert_stmt,
            "UpdateStmt": _build_update_stmt,
            "DeleteStmt": _build_delete_stmt,
        },
    )


def _build_select_stmt(n: Node) -> pgast.SelectStmt:
    op = _maybe(n, "op", _build_str)
    if op:
        op = op[6:]
        if op == "NONE":
            op = None
    return pgast.SelectStmt(
        distinct_clause=_maybe_list(n, "distinct_clause", _build_any),
        target_list=_maybe_list(n, "targetList", _build_res_target) or [],
        from_clause=_maybe_list(n, "fromClause", _build_base_range_var) or [],
        where_clause=_maybe(n, "whereClause", _build_base_expr),
        group_clause=_maybe_list(n, "groupClause", _build_base),
        having=_maybe(n, "having", _build_base_expr),
        window_clause=_maybe_list(n, "windowClause", _build_base),
        values=_maybe_list(n, "valuesLists", _build_base_expr),
        sort_clause=_maybe_list(n, "sortClause", _build_sort_by),
        limit_offset=_maybe(n, "limitOffset", _build_base_expr),
        limit_count=_maybe(n, "limitCount", _build_base_expr),
        locking_clause=_maybe_list(n, "sortClause", _build_any),
        op=op,
        all=n["all"] if "all" in n else False,
        larg=_maybe(n, "larg", _build_select_stmt),
        rarg=_maybe(n, "rarg", _build_select_stmt),
        ctes=_maybe(n, "withClause", _build_ctes),
    )


def _build_insert_stmt(n: Node) -> pgast.InsertStmt:
    return pgast.InsertStmt(
        relation=_maybe(n, "relation", _build_rel_range_var),
        returning_list=_maybe_list(n, "returningList", _build_res_target)
        or [],
        cols=_maybe_list(n, "cols", _build_insert_target),
        select_stmt=_maybe(n, "selectStmt", _build_query),
        on_conflict=_maybe(n, "on_conflict", _build_on_conflict),
        ctes=_maybe(n, "withClause", _build_ctes),
    )


def _build_update_stmt(n: Node) -> pgast.UpdateStmt:
    return pgast.UpdateStmt(
        relation=_maybe(n, "relation", _build_rel_range_var),
        targets=_build_targets(n, "targetList") or [],
        where_clause=_maybe(n, "whereClause", _build_base_expr),
        from_clause=_maybe_list(n, "fromClause", _build_base_range_var) or [],
    )


def _build_delete_stmt(n: Node) -> pgast.DeleteStmt:
    return pgast.DeleteStmt(
        relation=_maybe(n, "relation", _build_rel_range_var),
        returning_list=_maybe_list(n, "returningList", _build_res_target)
        or [],
        where_clause=_maybe(n, "whereClause", _build_base_expr),
        using_clause=_maybe_list(n, "usingClause", _build_base_range_var)
        or [],
    )


def _build_base(n: Node) -> pgast.Base:
    return _enum(
        n,
        {
            "CommonTableExpr": _build_cte,
        },
        [_build_base_expr],  # type: ignore
    )


def _build_base_expr(node: Node) -> pgast.BaseExpr:
    return _enum(
        node,
        {
            "ResTarget": _build_res_target,
            "FuncCall": _build_func_call,
            "List": _build_implicit_row,
            "A_Expr": _build_a_expr,
            "A_ArrayExpr": _build_array_expr,
            "A_Const": _build_const,
            "BoolExpr": _build_bool_expr,
            "CaseExpr": _build_case_expr,
            "TypeCast": _build_type_cast,
            "NullTest": _build_null_test,
            "BooleanTest": _build_boolean_test,
            "RowExpr": _build_row_expr,
            "SubLink": _build_sub_link,
            "ParamRef": _build_param_ref,
            "SetToDefault": _build_keyword("DEFAULT"),
        },
        [_build_base_range_var, _build_indirection_op],  # type: ignore
    )


def _build_indirection_op(n: Node) -> pgast.IndirectionOp:
    return _enum(
        n,
        {
            'A_Indices': _build_index_or_slice,
            'Star': lambda _: pgast.Star(),
            'ColumnRef': _build_column_ref,
        },
    )


def _build_ctes(n: Node) -> List[pgast.CommonTableExpr]:
    return _list(n, "ctes", _build_cte)


def _build_cte(n: Node) -> pgast.CommonTableExpr:
    n = _unwrap(n, "CommonTableExpr")

    materialized = None
    if n["ctematerialized"] == "CTEMaterializeAlways":
        materialized = True
    elif n["ctematerialized"] == "CTEMaterializeNever":
        materialized = False

    return pgast.CommonTableExpr(
        name=n["ctename"],
        query=_build_query(n["ctequery"]),
        recursive=_bool_or_false(n, "cterecursive"),
        aliascolnames=_maybe_list(
            n, "aliascolnames", _build_str  # type: ignore
        ),
        materialized=materialized,
    )


def _build_keyword(name: str) -> Callable[[Node], pgast.Keyword]:
    return lambda _: pgast.Keyword(name=name)


def _build_param_ref(n: Node) -> pgast.BaseParamRef:
    return pgast.ParamRef(number=n["number"])


def _build_sub_link(n: Node) -> pgast.SubLink:
    typ = n["subLinkType"]
    if typ == "EXISTS_SUBLINK":
        type = pgast.SubLinkType.EXISTS
    elif typ == "NOT_EXISTS_SUBLINK":
        type = pgast.SubLinkType.NOT_EXISTS
    elif typ == "ALL_SUBLINK":
        type = pgast.SubLinkType.ALL
    elif typ == "ANY_SUBLINK":
        type = pgast.SubLinkType.ANY
    elif typ == "EXPR_SUBLINK":
        type = pgast.SubLinkType.EXPR
    else:
        raise PSqlUnsupportedError(f"unknown SubLink type: `{typ}`")

    return pgast.SubLink(
        type=type,
        expr=_build_query(n["subselect"]),
        test_expr=_maybe(n, 'testexpr', _build_base_expr),
    )


def _build_row_expr(n: Node) -> pgast.ImplicitRowExpr:
    return pgast.ImplicitRowExpr(args=_list(n, "args", _build_base_expr))


def _build_boolean_test(n: Node) -> pgast.BooleanTest:
    return pgast.BooleanTest(
        arg=_build_base_expr(n["arg"]),
        negated=n["booltesttype"].startswith("IS_NOT"),
        is_true=n["booltesttype"].endswith("TRUE"),
    )


def _build_null_test(n: Node) -> pgast.NullTest:
    return pgast.NullTest(
        arg=_build_base_expr(n["arg"]),
        negated=n["nulltesttype"] == "IS_NOT_NULL",
    )


def _build_type_cast(n: Node) -> pgast.TypeCast:
    return pgast.TypeCast(
        arg=_build_base_expr(n["arg"]),
        type_name=_build_type_name(n["typeName"]),
    )


def _build_type_name(n: Node) -> pgast.TypeName:
    return pgast.TypeName(
        name=tuple(_list(n, "names", _build_str)),
        setof=_bool_or_false(n, "setof"),
        typmods=None,
        array_bounds=None,
    )


def _build_case_expr(n: Node) -> pgast.CaseExpr:
    return pgast.CaseExpr(
        arg=_maybe(n, "arg", _build_base_expr),
        args=_list(n, "args", _build_case_when),
        defresult=_maybe(n, "defresult", _build_base_expr),
    )


def _build_case_when(n: Node) -> pgast.CaseWhen:
    n = _unwrap(n, "CaseWhen")
    return pgast.CaseWhen(
        expr=_build_base_expr(n["expr"]),
        result=_build_base_expr(n["result"]),
    )


def _build_bool_expr(n: Node) -> pgast.Expr:
    name = _build_str(n["boolop"])[0:-5]
    res = pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=name,
        lexpr=_build_base_expr(n["args"].pop(0)),
        rexpr=_build_base_expr(n["args"].pop(0)),
    )
    while len(n["args"]) > 0:
        res = pgast.Expr(
            kind=pgast.ExprKind.OP,
            name=_build_str(n["boolop"])[0:-5],
            lexpr=res,
            rexpr=_build_base_expr(n["args"].pop(0)),
        )
    return res


def _build_base_range_var(n: Node) -> pgast.BaseRangeVar:
    return _enum(
        n,
        {
            "RangeVar": _build_rel_range_var,
            "JoinExpr": _build_join_expr,
            "RangeFunction": _build_range_function,
            "RangeSubselect": _build_range_subselect,
        },
    )


def _build_const(n: Node) -> pgast.BaseConstant:
    return _enum(
        n["val"],
        {
            "Integer": _build_integer,
            "Float": lambda n: pgast.NumericConstant(val=n["str"]),
            "String": _build_string_constant,
            "Null": lambda n: pgast.NullConstant(),
        },
    )


def _build_integer(n: Node) -> pgast.NumericConstant:
    return pgast.NumericConstant(val=str(n["ival"]))


def _build_string_constant(n: Node) -> pgast.StringConstant:
    return pgast.StringConstant(val=_build_str(n))


def _build_range_subselect(n: Node) -> pgast.RangeSubselect:
    return pgast.RangeSubselect(
        alias=_maybe(n, "alias", _build_alias) or pgast.Alias(aliasname=""),
        lateral=_bool_or_false(n, "lateral"),
        subquery=_build_query(n["subquery"]),
    )


def _build_range_function(n: Node) -> pgast.RangeFunction:
    return pgast.RangeFunction(
        alias=_maybe(n, "alias", _build_alias) or pgast.Alias(aliasname=""),
        lateral=_bool_or_false(n, "lateral"),
        with_ordinality=_bool_or_false(n, "with_ordinality"),
        is_rowsfrom=_bool_or_false(n, "is_rowsfrom"),
        functions=_build_implicit_row(n["functions"]).args,  # type: ignore
    )


def _build_join_expr(n: Node) -> pgast.JoinExpr:
    return pgast.JoinExpr(
        alias=_maybe(n, "alias", _build_alias) or pgast.Alias(aliasname=""),
        type=n["jointype"][5:],
        larg=_build_base_expr(n["larg"]),
        rarg=_build_base_expr(n["rarg"]),
        using_clause=_maybe_list(n, "usingClause", _build_str, _as_column_ref),
        quals=_maybe(n, "quals", _build_base_expr),
    )


def _build_rel_range_var(n: Node) -> pgast.RelRangeVar:
    return pgast.RelRangeVar(
        alias=_maybe(n, "alias", _build_alias) or pgast.Alias(aliasname=""),
        relation=_build_base_relation(n),
        include_inherited=_bool_or_false(n, "inh"),
    )


def _build_alias(n: Node) -> pgast.Alias:
    return pgast.Alias(
        aliasname=_build_str(n["aliasname"]),
        colnames=_maybe_list(n, "colnames", _build_str),
    )


def _build_base_relation(node: Node) -> pgast.BaseRelation:
    return pgast.Relation(name=_maybe(node, "relname", _build_str))


def _build_implicit_row(n: Node) -> pgast.ImplicitRowExpr:
    if isinstance(n, list):
        n = n[0]
    n = _unwrap(n, "List")

    return pgast.ImplicitRowExpr(
        args=[_build_base_expr(e) for e in n["items"] if len(e) > 0]
    )


def _build_array_expr(n: Node) -> pgast.ArrayExpr:
    return pgast.ArrayExpr(elements=_list(n, "elements", _build_base_expr))


def _build_a_expr(n: Node) -> pgast.Expr:
    if n["kind"] == "AEXPR_OP":
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name=_build_str(n["name"][0]),
            lexpr=_maybe(n, "lexpr", _build_base_expr),
            rexpr=_maybe(n, "rexpr", _build_base_expr),
        )
    elif n["kind"] == "AEXPR_LIKE":
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name="LIKE",
            lexpr=_maybe(n, "lexpr", _build_base_expr),
            rexpr=_maybe(n, "rexpr", _build_base_expr),
        )
    elif n["kind"] == "AEXPR_IN":
        return pgast.Expr(
            kind=pgast.ExprKind.OP,
            name="IN",
            lexpr=_maybe(n, "lexpr", _build_base_expr),
            rexpr=_maybe(n, "rexpr", _build_base_expr),
        )
    else:
        raise PSqlUnsupportedError(f'unknown ExprKind: {n["kind"]}')


def _build_func_call(n: Node) -> pgast.FuncCall:
    n = _unwrap(n, "FuncCall")
    return pgast.FuncCall(
        name=tuple(_list(n, "funcname", _build_str)),
        args=_maybe_list(n, "args", _build_base_expr) or [],
        agg_order=_maybe_list(n, "aggOrder", _build_sort_by),
        agg_filter=_maybe(n, "aggFilter", _build_base_expr),
        agg_star=_bool_or_false(n, "agg_star"),
        agg_distinct=_bool_or_false(n, "agg_distinct"),
        over=_maybe(n, "over", _build_window_def),
        with_ordinality=_bool_or_false(n, "withOrdinality"),
    )


def _build_index_or_slice(n: Node) -> pgast.Slice | pgast.Index:
    if n['is_slice']:
        return pgast.Slice(
            lidx=_build_base_expr(n['lidx']),
            ridx=_build_base_expr(n['uidx']),
        )
    else:
        return pgast.Index(
            idx=_build_base_expr(n['lidx']),
        )


def _build_res_target(n: Node) -> pgast.ResTarget:
    n = _unwrap(n, "ResTarget")
    return pgast.ResTarget(
        name=_maybe(n, "name", _build_str),
        indirection=_maybe_list(n, "indirection", _build_indirection_op),
        val=_build_base_expr(n["val"]),
    )


def _build_insert_target(n: Node) -> pgast.InsertTarget:
    n = _unwrap(n, "ResTarget")
    return pgast.InsertTarget(
        name=_build_str(n['name']),
    )


def _build_update_target(n: Node) -> pgast.UpdateTarget:
    n = _unwrap(n, "ResTarget")
    return pgast.UpdateTarget(
        name=_build_str(n['name']),
        val=_build_base_expr(n['val']),
        indirection=_maybe_list(n, "indirection", _build_indirection_op),
    )


def _build_window_def(n: Node) -> pgast.WindowDef:
    return pgast.WindowDef(
        name=_maybe(n, "name", _build_str),
        refname=_maybe(n, "refname", _build_str),
        partition_clause=_maybe_list(n, "partitionClause", _build_base_expr),
        order_clause=_maybe_list(n, "orderClause", _build_sort_by),
        frame_options=None,
        start_offset=_maybe(n, "startOffset", _build_base_expr),
        end_offset=_maybe(n, "endOffset", _build_base_expr),
    )


def _build_sort_by(n: Node) -> pgast.SortBy:
    n = _unwrap(n, "SortBy")
    return pgast.SortBy(
        node=_build_base_expr(n["node"]),
        dir=_maybe(n, "sortby_dir", _build_sort_order),
        nulls=_maybe(n, "sortby_nulls", _build_nones_order),
    )


def _build_nones_order(n: Node) -> qlast.NonesOrder:
    if n == "SORTBY_NULLS_FIRST":
        return qlast.NonesFirst
    return qlast.NonesLast


def _build_sort_order(n: Node) -> qlast.SortOrder:
    if n == "SORTBY_DESC":
        return qlast.SortOrder.Desc
    return qlast.SortOrder.Asc


def _build_targets(
    n: Node, key: str
) -> Optional[List[pgast.UpdateTarget | pgast.MultiAssignRef]]:
    if _probe(n, [key, 0, "ResTarget", "val", "MultiAssignRef"]):
        return [_build_multi_assign_ref(n[key])]
    else:
        return _maybe_list(n, key, _build_update_target)


def _build_multi_assign_ref(targets: List[Node]) -> pgast.MultiAssignRef:
    mar = targets[0]['ResTarget']['val']['MultiAssignRef']

    return pgast.MultiAssignRef(
        source=_build_base_expr(mar['source']),
        columns=[
            _as_column_ref(target['ResTarget']['name']) for target in targets
        ],
    )


def _build_column_ref(node: Node) -> pgast.ColumnRef:
    return pgast.ColumnRef(
        name=_list(node, "fields", _build_string_or_star),
        optional=_maybe(node, "optional", _build_bool),
    )


def _build_infer_clause(n: Node) -> pgast.InferClause:
    return pgast.InferClause(
        index_elems=_maybe_list(n, "indexElems", _build_str),
        where_clause=_maybe(n, "whereClause", _build_base_expr),
        conname=_maybe(n, "conname", _build_str),
    )


def _build_on_conflict(n: Node) -> pgast.OnConflictClause:
    return pgast.OnConflictClause(
        action=_build_str(n["action"]),
        infer=_maybe(n, "infer", _build_infer_clause),
        target_list=_build_targets(n, "targetList"),
        where=_maybe(n, "where", _build_base_expr),
    )


def _build_string_or_star(node: Node) -> pgast.Star | str:
    def star(_: Node) -> pgast.Star | str:
        return pgast.Star()

    return _enum(node, {"String": _build_str, "A_Star": star})
