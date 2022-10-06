from typing import *

from edb.pgsql import ast as pgast

# Node = bool | str | int | float | List[Any] | dict[str, Any]
Node = Any
T = TypeVar("T")

def _maybe(node: Node, name: str, builder: Callable[[Node], T]) -> Optional[T]:
    if name in node:
        return builder(node[name])
    return None

def _list(node: Node, name: str, builder: Callable[[Node], T]) -> List[T]:
    return [builder(n) for n in node[name]]


def _maybe_list(node: Node, name: str, builder: Callable[[Node], T]) -> Optional[List[T]]:
    return _list(node, name, builder) if name in node else None    

def _enum(node: Node, builders: dict[str, Callable[[Node], T]], fallbacks: List[Callable[[Node], T]] = []) -> T:
    for name in builders:
        if name in node:
            builder = builders[name]
            return builder(node[name])
    for fallback in fallbacks:
        try:
            return fallback(node)
        except:
            pass
    raise BaseException(f'unknown enum: {node}')

def build_any(node: Node) -> Any:
    return node

def build_str(node: Node) -> str:
    node = unwrap(node, 'String')
    node = unwrap(node, 'str')
    return str(node)

def build_bool(node: Node) -> bool:
    return node == True

def maybe_bool(node: Node, name: str) -> bool:
    return node[name] == True if name in node else False

def build_queries(node: Node) -> List[pgast.Query]:
    return [build_query(node) for node in node['stmts']]

def unwrap(node: Node, name: str) -> pgast.Query:
    if isinstance(node, dict) and name in node:
        return node[name]
    return node

def build_query(node: Node) -> pgast.Query:
    return _enum(node['stmt'], {
        'SelectStmt': build_select_stmt,
    })


def build_select_stmt(node: Node) -> pgast.SelectStmt:
    return pgast.SelectStmt(
        distinct_clause=_maybe_list(node, 'distinct_clause', build_any),
        target_list=_list(node, 'targetList', build_res_target),
        from_clause=_list(node, 'fromClause', build_base_range_var),
        where_clause=_maybe(node, 'whereClause', build_base_expr),
        group_clause=_maybe_list(node, 'groupClause', build_base),
        having=_maybe(node, 'having', build_base_expr),
        window_clause=_maybe_list(node, 'windowClause', build_base),
        values=_maybe_list(node, 'windowClause', build_base),
        sort_clause=_maybe_list(node, 'sortClause', build_sort_by),
        limit_offset=_maybe(node, 'limitOffset', build_base_expr),
        limit_count=_maybe(node, 'limitCount', build_base_expr),
        locking_clause=_maybe_list(node, 'sortClause', build_any),
        op=_maybe(node, 'op', build_str),
        all=node['all'] if 'all' in node else False,
        larg=_maybe(node, 'larg', build_query),
        rarg=_maybe(node, 'rarg', build_query),
    )

def build_base_expr(node: Node) -> pgast.BaseExpr:
    return _enum(node, {
        'ResTarget': build_res_target,
        'ColumnRef': build_column_ref,
        'FuncCall': build_func_call,
        'A_Expr': build_a_expr,
        'List': build_array,
        'A_Const': build_const,
    }, [build_base_range_var])

def build_base_range_var(n: Node) -> pgast.BaseRangeVar:
    return _enum(n, {
        'RangeVar': build_rel_range_var,
        'JoinExpr': build_join_expr,
        'RangeFunction': build_range_function
    })

def build_const(n: Node) -> pgast.BaseConstant:
    return _enum(n['val'], {
        'Integer': build_integer,
        'Float': lambda n: pgast.NumericConstant(val=n['str']),
        'String': build_string_constant,
        'Null': lambda n: pgast.NullConstant(),
    })

def build_integer(n: Node) -> pgast.NumericConstant:
    return pgast.NumericConstant(val=str(n['ival']))

def build_string_constant(n: Node) -> pgast.StringConstant:
    return pgast.StringConstant(
        val=build_str(n)
    )


def build_range_function(n: Node) -> pgast.RangeFunction:
    return pgast.RangeFunction(
        lateral = maybe_bool(n, 'lateral'),
        with_ordinality = maybe_bool(n, 'with_ordinality'),
        is_rowsfrom = maybe_bool(n, 'is_rowsfrom'),
        functions = _list(n, 'functions', build_func_call),
    )

def build_join_expr(n: Node) -> pgast.JoinExpr:
    return pgast.JoinExpr(
        type=n['jointype'],
        larg=build_base_expr(n['larg']),
        rarg=build_base_expr(n['rarg']),
        using_clause=_maybe_list(n, 'usingClause', build_base_expr),
        quals=_maybe(n, 'quals', build_base_expr),
    )

def build_rel_range_var(n: Node) -> pgast.RelRangeVar:
    return pgast.RelRangeVar(
        relation=build_base_relation(n),
        include_inherited=build_bool(n['inh'])
    )

def build_base_relation(node: Node) -> pgast.BaseRelation:
    return pgast.Relation(
        name=_maybe(node, 'relname', build_str)
    )

def build_array(n: Node) -> pgast.ArrayExpr:
    return pgast.ArrayExpr(
        elements=_list(n, 'items', build_base_expr)
    )

def build_a_expr(n: Node) -> pgast.Expr:
    if n['kind'] != 'AEXPR_OP':
        raise BaseException(f'unknown ExprKind: {n["kind"]}')
    return pgast.Expr(
        kind=pgast.ExprKind.OP,
        name=build_str(n['name'][0]),
        lexpr=_maybe(n, 'lexpr', build_base_expr),
        rexpr=_maybe(n, 'rexpr', build_base_expr),
    )

def build_func_call(n: Node) -> pgast.FuncCall:
    n = unwrap(n, 'FuncCall')
    return pgast.FuncCall(
        name=tuple(_list(n, 'funcname', build_str)),
        args=_maybe_list(n, 'args', build_base_expr) or [],
        agg_order=_maybe_list(n, 'aggOrder', build_sort_by),
        agg_filter=_maybe(n, 'aggFilter', build_base_expr),
        agg_star=maybe_bool(n, 'agg_star'),
        agg_distinct=maybe_bool(n, 'agg_distinct'),
        over=_maybe(n, 'over', build_window_def),
        with_ordinality=maybe_bool(n, 'withOrdinality'),
    )

def build_res_target(n: Node) -> pgast.ResTarget:
    n = unwrap(n, 'ResTarget')
    return pgast.ResTarget(
        name=_maybe(n, 'name', build_str),
        indirection=_maybe_list(n, 'indirection', build_any),
        val=build_base_expr(n['val'])
    )
    

def build_base(node: Node) -> pgast.Base:
    return None

def build_window_def(node: Node) -> pgast.WindowDef:
    return None

def build_sort_by(node: Node) -> pgast.SortBy:
    return None

def build_column_ref(node: Node) -> pgast.ColumnRef:
    return pgast.ColumnRef(
        name=_list(node, 'fields', build_string_or_star),
        optional=_maybe(node, 'optional', build_bool),
    )

def build_string_or_star(node: Node) -> pgast.Star | str:
    return _enum(node, {
        'String': build_str,
        'A_Star': lambda n: pgast.Star()
    })

