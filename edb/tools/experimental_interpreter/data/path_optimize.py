# this is unsound

# from . import data_ops as e
# from typing import Optional, List, Tuple
# from . import expr_to_str as pp
# from . import expr_ops as eops

# def path_optimize(expr: e.Expr) -> e.Expr:
#     def path_subst(sub : e.Expr) -> e.Expr:
#         match sub:
#             case e.OptionalForExpr(bound=bound, next=next):
#                 occurrence_count = eops.count_appearances_in_expr(e.FreeVarExpr(next.var), next.body)
#                 if occurrence_count <= 1:
#                     return path_optimize(eops.instantiate_expr(bound, next))
#                 else:
#                     return None
#     result = eops.map_expr(path_subst, expr)
#     return result
