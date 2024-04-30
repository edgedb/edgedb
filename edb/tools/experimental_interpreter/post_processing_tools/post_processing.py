from ..data import deduplication_insert
from ..data import data_ops as e
from . import insert_select_optimization


def post_process(expr: e.Expr) -> e.Expr:
    result = deduplication_insert.insert_conditional_dedup(expr)
    result = insert_select_optimization.select_optimize(result)
    return result
