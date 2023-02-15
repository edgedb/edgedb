

from .data_ops import *
from .expr_ops import *

from .query_ops import *
def is_path(e : Expr) -> bool:
    match e:
        case FreeVarExpr(_):
            return True
        case LinkPropProjExpr(subject=subject, linkprop=linkprop):
            return is_path(subject)
        case ObjectProjExpr(subject=subject, label=label):
            return is_path(subject)
        case _:
            return False

def all_prefixes_of_a_path(e : Expr) -> List[Expr]:
    match e:
        case FreeVarExpr(_):
            return [e]
        case LinkPropProjExpr(subject=subject, linkprop=linkprop):
            return [*all_prefixes_of_a_path(subject), e]
        case ObjectProjExpr(subject=subject, label=label):
            return [*all_prefixes_of_a_path(subject), e]
        case _:
            raise ValueError("not a path", e)
        
def path_lexicographic_key(e : Expr) -> str:
    match e:
        case FreeVarExpr(s):
            return s
        case LinkPropProjExpr(subject=subject, linkprop=linkprop):
            return path_lexicographic_key(subject) + "@" + linkprop
        case ObjectProjExpr(subject=subject, label=label):
            return path_lexicographic_key(subject) + "." + label
        case _:
            raise ValueError("not a path")

def get_all_paths(e : Expr) -> List[Expr]:
    all_paths : List[Expr] = []
    def populate(sub : Expr, level : int) -> Optional[Expr]:
        nonlocal all_paths
        if isinstance(sub, DetachedExpr): # skip detached
            return sub
        if is_path(sub):
            all_paths = [*all_paths, sub]
            return sub
        else:
            return None
    map_expr(populate, e)
    return all_paths

def get_all_pre_top_level_paths(e : Expr, dbschema : DBSchema) -> List[Expr]:
    all_paths : List[Expr] = []
    def populate(sub : Expr, level : QueryLevel) -> Optional[Expr]:
        nonlocal all_paths
        if isinstance(sub, DetachedExpr): # skip detached
            return sub
        if is_path(sub) and (level == QueryLevel.TOP_LEVEL or level == QueryLevel.SEMI_SUBQUERY):
            all_paths = [*all_paths, sub]
            return sub
        else:
            return None
    map_query(populate, e, dbschema)
    return all_paths
    

def get_all_proper_top_level_paths(e : Expr, dbschema : DBSchema) -> List[Expr]:
    top_paths : List[Expr] = []
    semi_sub_paths : List[List[Expr]] = []
    def populate(sub : Expr, level : QueryLevel) -> Optional[Expr]:
        nonlocal top_paths, semi_sub_paths
        if isinstance(sub, DetachedExpr): # skip detached
            return sub
        if level == QueryLevel.TOP_LEVEL and is_path(sub):
            top_paths = [*top_paths, sub]
            return sub
        elif level == QueryLevel.SEMI_SUBQUERY:
            semi_sub_paths = [*semi_sub_paths, get_all_pre_top_level_paths(sub, dbschema)]
            return sub # also cut off here
        elif level == QueryLevel.SUBQUERY:
            return sub # also cut off here as paths inside subqueries
        else:
            return None
    map_query(populate, e, dbschema)

    selected_semi_sub_paths = []
    for (i, cluster) in enumerate(semi_sub_paths):
        for (candidate) in cluster:
            prefixes = all_prefixes_of_a_path(candidate)
            to_check = top_paths + [p for l in (semi_sub_paths[:i] + semi_sub_paths[i+1:]) for p in l]
            if any([appears_in_expr(prefix, ck) for prefix in prefixes for ck in to_check]):
                selected_semi_sub_paths.append(candidate)

    # all top_paths will show up finally, we need to filter out those paths in semi_sub 
    # whose prefixes (including itself) appears solely in the same subquery
    return top_paths + selected_semi_sub_paths

def common_longest_path_prefix(e1 : Expr, e2 : Expr) -> Optional[Expr]:
    pending = None
    def find_longest(pp1 : List[Expr], pp2 : List[Expr]) -> Optional[Expr]:
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

def common_longest_path_prefix_in_set(test_set : List[Expr]) -> List[Expr]:
    result : List[Expr] = []
    for s in test_set:
        for t in test_set:
            optional = common_longest_path_prefix(s, t)
            if optional:
                result.append(optional)
    return result






    
def toppath_for_factoring(e : Expr, dbschema : DBSchema) -> List[Expr]:
    all_paths = get_all_paths(e)
    top_level_paths = get_all_proper_top_level_paths(e, dbschema)
    c_i = [common_longest_path_prefix_in_set(all_paths + [b]) for b in top_level_paths]
    return sorted(list(set([p for c in c_i for p in c])), key=path_lexicographic_key)


def select_hoist(e : Expr, dbschema : DBSchema) -> Expr:
    top_paths = toppath_for_factoring(e, dbschema)
    fresh_names : List[str] = [next_name() for p in top_paths]
    fresh_vars : List[Expr] = [FreeVarExpr(n) for n in fresh_names]
    after_e = iterative_subst_expr_for_expr(fresh_vars, top_paths, e)
    def sub_select_hoist(e : Expr) -> Expr:
        if isinstance(e, BindingExpr):
            new_fresh_name = next_name()
            return abstract_over_expr(
                        select_hoist(
                            instantiate_expr(
                                FreeVarExpr(new_fresh_name), e
                            ), dbschema
                        ), 
                        new_fresh_name)
        else:
            return select_hoist(e, dbschema)
    after_after_e = map_sub_and_semisub_queries(sub_select_hoist, after_e, dbschema)
    for_paths = [iterative_subst_expr_for_expr(fresh_vars[:i-1], top_paths[:i-1], p_i) for (i, p_i) in enumerate(top_paths)]

    result = after_after_e
    for i in reversed(list(range(len(for_paths)))):
        result = ForExpr(for_paths[i], abstract_over_expr(result, fresh_names[i]))

    return result
