

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
        case BackLinkExpr(subject=subject, label=label):
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
        case BackLinkExpr(subject=subject, label=label):
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
        case BackLinkExpr(subject=subject, label=label):
            return path_lexicographic_key(subject) + ".<" + label
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
    sub_paths : List[Expr] = []
    def populate(sub : Expr, level : QueryLevel) -> Optional[Expr]:
        nonlocal top_paths, semi_sub_paths, sub_paths
        if isinstance(sub, DetachedExpr): # skip detached
            return sub
        if level == QueryLevel.TOP_LEVEL and is_path(sub):
            top_paths = [*top_paths, sub]
            return sub
        elif level == QueryLevel.SEMI_SUBQUERY:
            semi_sub_paths = [*semi_sub_paths, get_all_pre_top_level_paths(sub, dbschema)]
            return sub # also cut off here
        elif level == QueryLevel.SUBQUERY:
            sub_paths = [*sub_paths, *get_all_paths(sub)]
            return sub # also cut off here as paths inside subqueries
        else:
            return None
    map_query(populate, e, dbschema)
    # print("Semi sub paths are", semi_sub_paths)

    selected_semi_sub_paths = []
    for (i, cluster) in enumerate(semi_sub_paths):
        for (candidate) in cluster:
            prefixes = all_prefixes_of_a_path(candidate)
            to_check = top_paths + sub_paths + [p for l in (semi_sub_paths[:i] + semi_sub_paths[i+1:]) for p in l]
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
    # print("All Proper Top Level Paths", top_level_paths)
    c_i = [common_longest_path_prefix_in_set(all_paths + [b]) for b in top_level_paths]
    return sorted(list(set([p for c in c_i for p in c])), key=path_lexicographic_key)

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

def sub_select_hoist(e : Expr, dbschema : DBSchema) -> Expr:
    def sub_select_hoist_map_func(e : Expr) -> Expr:
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
    return map_sub_and_semisub_queries(sub_select_hoist_map_func, e, dbschema)

# @trace_input_output
def select_hoist(e : Expr, dbschema : DBSchema) -> Expr:
    top_paths = toppath_for_factoring(e, dbschema)
    fresh_names : List[str] = [next_name() for p in top_paths]
    # print("Paths and Names:", top_paths, fresh_names)
    fresh_vars : List[Expr] = [FreeVarExpr(n) for n in fresh_names]
    for_paths = [iterative_subst_expr_for_expr(fresh_vars[:i], top_paths[:i], p_i) for (i, p_i) in enumerate(top_paths)]

    inner_e : Expr 
    post_process_transform : Callable[[Expr], Expr] 
    match e :
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            bindname = next_name() 
            inner_e = WithExpr(FilterOrderExpr(subject=sub_select_hoist(iterative_subst_expr_for_expr(fresh_vars, top_paths, subject), dbschema), 
                                               filter=operate_under_binding(filter, lambda filter:select_hoist(iterative_subst_expr_for_expr(fresh_vars, top_paths, filter), dbschema)), 
                                               order= abstract_over_expr(ObjectExpr({}))
                                                      ),
                abstract_over_expr(
                    ObjectExpr({
                        StrLabel("subject") : FreeVarExpr(bindname), 
                        StrLabel("order") : select_hoist(iterative_subst_expr_for_expr(fresh_vars, top_paths, instantiate_expr(FreeVarExpr(bindname), order)), dbschema),
                    }), bindname))
            def post_processing(e: Expr) -> Expr: 
                bindname = next_name()
                return ObjectProjExpr(
                    subject=FilterOrderExpr(subject=e, 
                                            filter=abstract_over_expr(BoolVal(True)), 
                                            order=
                            abstract_over_expr(ObjectProjExpr(subject=FreeVarExpr(bindname), label="order"), bindname)
                                            ),
                    label="subject"
                )
            post_process_transform = post_processing
        case _: 
            after_e = iterative_subst_expr_for_expr(fresh_vars, top_paths, e)
            inner_e = sub_select_hoist(after_e, dbschema)
            post_process_transform = lambda x: x


    result = inner_e
    for i in reversed(list(range(len(for_paths)))):
        # print ("abstracting over path = ", for_paths[i], "on result", result)
        result = OptionalForExpr(for_paths[i], abstract_over_expr(result, fresh_names[i]))
        # result = WithExpr(for_paths[i], abstract_over_expr(result, fresh_names[i]))

    return post_process_transform(result)
