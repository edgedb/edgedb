
from ..data import data_ops as e
from typing import List

from edb.edgeql import ast as qlast

def process_ddls(
    schema: e.DBSchema, 
    ddls: List[qlast.DDLOperation]) -> e.DBSchema:
    print("TODO")
    return schema