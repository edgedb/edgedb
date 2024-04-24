from typing import Callable, List, Optional

from .data_ops import (
    BackLinkExpr,
    BindingExpr,
    BoolVal,
    DetachedExpr,
    Expr,
    FilterOrderExpr,
    FreeVarExpr,
    LinkPropProjExpr,
    ObjectProjExpr,
    TpIntersectExpr,
    OptionalForExpr,
    next_name,
    StrLabel,
)
from . import data_ops as e
from . import expr_ops as eops
from .expr_ops import (
    abstract_over_expr,
    appears_in_expr,
    instantiate_expr,
    is_path,
    iterative_subst_expr_for_expr,
    map_expr,
    operate_under_binding,
)
from .query_ops import QueryLevel, map_query, map_sub_and_semisub_queries


def all_prefixes_of_a_path(expr: Expr) -> List[Expr]:
    match expr:
        case FreeVarExpr(_):
            return [expr]
        case LinkPropProjExpr(subject=subject, linkprop=_):
            return [*all_prefixes_of_a_path(subject), expr]
        case ObjectProjExpr(subject=subject, label=_):
            return [*all_prefixes_of_a_path(subject), expr]
        case e.TpIntersectExpr(subject=subject, tp=_):
            return [*all_prefixes_of_a_path(subject), expr]
        case BackLinkExpr(subject=subject, label=_):
            return [*all_prefixes_of_a_path(subject), expr]
        case _:
            raise ValueError("not a path", expr)


def path_lexicographic_key(e: Expr) -> str:
    match e:
        case FreeVarExpr(s):
            return s
        case LinkPropProjExpr(subject=subject, linkprop=linkprop):
            return path_lexicographic_key(subject) + "@" + linkprop
        case ObjectProjExpr(subject=subject, label=label):
            return path_lexicographic_key(subject) + "." + label
        case TpIntersectExpr(subject=subject, tp=_):
            return path_lexicographic_key(subject) + "[is _]"
        case BackLinkExpr(subject=subject, label=label):
            return path_lexicographic_key(subject) + ".<" + label
        case _:
            raise ValueError("not a path")


def get_all_paths(e: Expr) -> List[Expr]:
    all_paths: List[Expr] = []

    def populate(sub: Expr) -> Optional[Expr]:
        nonlocal all_paths
        if isinstance(sub, DetachedExpr):  # skip detached
            return sub
        if is_path(sub):
            all_paths = [*all_paths, sub]
            return sub
        else:
            return None

    map_expr(populate, e)
    return all_paths


def get_all_pre_top_level_paths(e: Expr, dbschema: e.TcCtx) -> List[Expr]:
    all_paths: List[Expr] = []

    def populate(sub: Expr, level: QueryLevel) -> Optional[Expr]:
        nonlocal all_paths
        if isinstance(sub, DetachedExpr):  # skip detached
            return sub
        if is_path(sub) and (
            level == QueryLevel.TOP_LEVEL or level == QueryLevel.SEMI_SUBQUERY
        ):
            all_paths = [*all_paths, sub]
            return sub
        else:
            return None

    map_query(populate, e, dbschema)
    return all_paths


def get_all_proper_top_level_paths(e: Expr, dbschema: e.TcCtx) -> List[Expr]:
    definite_top_paths: List[Expr] = []
    semi_sub_paths: List[List[Expr]] = []
    sub_paths: List[Expr] = []
    sub_sub_paths: List[List[Expr]] = []

    def populate(sub: Expr, level: QueryLevel) -> Optional[Expr]:
        nonlocal definite_top_paths, semi_sub_paths, sub_paths, sub_sub_paths
        if isinstance(sub, DetachedExpr):  # skip detached
            return sub
        if level == QueryLevel.TOP_LEVEL and is_path(sub):
            definite_top_paths = [*definite_top_paths, sub]
            return sub
        elif level == QueryLevel.SEMI_SUBQUERY:
            this_semi_sub_paths = get_all_pre_top_level_paths(sub, dbschema)
            semi_sub_paths = [*semi_sub_paths, this_semi_sub_paths]
            this_sub_paths = [
                p for p in get_all_paths(sub) if p not in this_semi_sub_paths
            ]
            sub_sub_paths = [*sub_sub_paths, this_sub_paths]
            return sub  # also cut off here
        elif level == QueryLevel.SUBQUERY:
            sub_paths = [*sub_paths, *get_all_paths(sub)]
            return sub  # also cut off here as paths inside subqueries
        else:
            return None

    map_query(populate, e, dbschema)

    selected_semi_sub_paths = []
    for i, cluster in enumerate(semi_sub_paths):
        for candidate in cluster:
            prefixes = all_prefixes_of_a_path(candidate)
            to_check = (
                definite_top_paths
                + sub_paths
                + [
                    p
                    for spl in (semi_sub_paths[:i] + semi_sub_paths[i + 1 :])
                    for p in spl
                ]
                + [
                    p
                    for spl in (sub_sub_paths[:i] + sub_sub_paths[i + 1 :])
                    for p in spl
                ]
            )
            if any(
                [
                    appears_in_expr(prefix, ck)
                    for prefix in prefixes
                    for ck in to_check
                ]
            ):
                selected_semi_sub_paths.append(candidate)

    # all top_paths will show up finally,
    # we need to filter out those paths in semi_sub
    # whose prefixes (including itself) appears solely in the same subquery
    return definite_top_paths + selected_semi_sub_paths


def common_longest_path_prefix(e1: Expr, e2: Expr) -> Optional[Expr]:
    pending = None

    def find_longest(pp1: List[Expr], pp2: List[Expr]) -> Optional[Expr]:
        nonlocal pending
        match (pp1, pp2):
            case ([], []):
                return pending
            case ([], _):
                return pending
            case (_, []):
                return pending
            case ([p1this, *p1next], [p2this, *p2next]):
                if p1this == p2this:
                    pending = p1this
                return find_longest([*p1next], [*p2next])
        raise ValueError("should not happen")

    return find_longest(all_prefixes_of_a_path(e1), all_prefixes_of_a_path(e2))


def common_longest_path_prefix_in_set(test_set: List[Expr]) -> List[Expr]:
    result: List[Expr] = []
    for s in test_set:
        for t in test_set:
            optional = common_longest_path_prefix(s, t)
            if optional:
                result.append(optional)
    return result


def separate_common_longest_path_prefix_in_set(
    base_set: List[Expr], compare_set: List[Expr]
) -> List[Expr]:
    result: List[Expr] = []
    for s in base_set:
        for t in compare_set:
            optional = common_longest_path_prefix(s, t)
            if optional:
                result.append(optional)
    return result


def toppath_for_factoring(expr: Expr, dbschema: e.TcCtx) -> List[Expr]:
    all_paths = get_all_paths(expr)
    top_level_paths = get_all_proper_top_level_paths(expr, dbschema)
    clpp_a = common_longest_path_prefix_in_set(top_level_paths)
    c_i = [
        separate_common_longest_path_prefix_in_set(top_level_paths, [b])
        for b in all_paths
    ]
    c_all = [p for c in c_i for p in c]
    d = []
    for p in [*clpp_a, *c_all]:
        match p:
            case e.LinkPropProjExpr(subject=subject, linkprop=_):
                match subject:
                    case e.ObjectProjExpr(subject=subject, label=_):
                        d.append(subject)
                    case e.TpIntersectExpr(
                        subject=(e.BackLinkExpr(subject=subject, label=_)),
                        tp=_,
                    ):
                        d.append(subject)
                    case e.BackLinkExpr(subject=subject, label=_):
                        d.append(subject)
                    case _:
                        pass
            case _:
                pass
    all_factoring_paths = c_all + clpp_a + d

    # remove from all_factoring_paths those paths that only occurred once
    excluding_paths = [
        p
        for p in top_level_paths
        if eops.count_appearances_in_expr(p, expr) == 1
    ]
    all_factoring_paths = [
        p for p in all_factoring_paths if p not in excluding_paths
    ]

    return sorted(list(set(all_factoring_paths)), key=path_lexicographic_key)


def trace_input_output(func):
    def wrapper(e, s):
        indent = "| " * wrapper.depth
        print(f"{indent}input: {e} ")
        wrapper.depth += 1
        result = func(e, s)
        wrapper.depth -= 1
        print(f"{indent}output: {result}")
        return result

    wrapper.depth = 0
    return wrapper


def sub_select_hoist(top_e: Expr, dbschema: e.TcCtx) -> Expr:
    def sub_select_hoist_map_func(e: Expr) -> Expr:
        if isinstance(e, BindingExpr):
            new_fresh_name = next_name()
            return abstract_over_expr(
                select_hoist(
                    instantiate_expr(FreeVarExpr(new_fresh_name), e), dbschema
                ),
                new_fresh_name,
            )
        else:
            return select_hoist(e, dbschema)

    return map_sub_and_semisub_queries(
        sub_select_hoist_map_func, top_e, dbschema
    )


def select_hoist(expr: Expr, dbschema: e.TcCtx) -> Expr:

    top_paths = toppath_for_factoring(expr, dbschema)
    fresh_names: List[str] = [next_name() for p in top_paths]
    fresh_vars: List[Expr] = [FreeVarExpr(n) for n in fresh_names]
    for_paths = [
        iterative_subst_expr_for_expr(fresh_vars[:i], top_paths[:i], p_i)
        for (i, p_i) in enumerate(top_paths)
    ]

    inner_e: Expr
    post_process_transform: Callable[[Expr], Expr]
    match expr:
        # only perform special factoring if there is an order
        case FilterOrderExpr(
            subject=subject, filter=filter, order=order
        ) if order:
            bindname = next_name()
            inner_e = OptionalForExpr(
                FilterOrderExpr(
                    subject=sub_select_hoist(
                        iterative_subst_expr_for_expr(
                            fresh_vars, top_paths, subject
                        ),
                        dbschema,
                    ),
                    filter=operate_under_binding(
                        filter,
                        lambda filter: select_hoist(
                            iterative_subst_expr_for_expr(
                                fresh_vars, top_paths, filter
                            ),
                            dbschema,
                        ),
                    ),
                    order={},
                ),
                abstract_over_expr(
                    e.ShapedExprExpr(
                        expr=e.FreeObjectExpr(),
                        shape=e.ShapeExpr(
                            shape={
                                StrLabel(
                                    "__edgedb_reserved_subject__"
                                ): abstract_over_expr(FreeVarExpr(bindname)),
                                **{
                                    StrLabel(l): abstract_over_expr(
                                        select_hoist(
                                            iterative_subst_expr_for_expr(
                                                fresh_vars,
                                                top_paths,
                                                instantiate_expr(
                                                    FreeVarExpr(bindname), o
                                                ),
                                            ),
                                            dbschema,
                                        )
                                    )
                                    for (l, o) in order.items()
                                },
                            }
                        ),
                    ),
                    bindname,
                ),
            )

            def post_processing(expr: Expr) -> Expr:
                bindname = next_name()
                return ObjectProjExpr(
                    subject=FilterOrderExpr(
                        subject=expr,
                        filter=abstract_over_expr(BoolVal(True)),
                        order={
                            l: abstract_over_expr(
                                ObjectProjExpr(
                                    subject=FreeVarExpr(bindname), label=l
                                ),
                                bindname,
                            )
                            for (l, o) in order.items()
                        },
                    ),
                    label="__edgedb_reserved_subject__",
                )

            post_process_transform = post_processing
        case _:
            after_e = iterative_subst_expr_for_expr(
                fresh_vars, top_paths, expr
            )
            inner_e = sub_select_hoist(after_e, dbschema)

            def id_transform(x):
                return x

            post_process_transform = id_transform

    result = inner_e
    for i in reversed(list(range(len(for_paths)))):
        result = OptionalForExpr(
            for_paths[i], abstract_over_expr(result, fresh_names[i])
        )

    result = post_process_transform(result)
    return result
