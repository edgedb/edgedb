
from ..data import data_ops as e
from typing import List

from edb.edgeql import ast as qlast


def process_ddl(
    schema: e.DBSchema,
    ddl: qlast.DDLOperation) -> e.DBSchema:
    """
    Process a single DDL operation.
    """
    match ddl:
        case qlast.CreateModule(
            name=qlast.ObjectRef(name=module_name), 
            commands=[]):
            assert "::" not in module_name, "TODO"
            schema.modules[(module_name, )] = e.DBModule({})
            return schema
        case qlast.CreatePseudoType():
            print("WARNING: not supported yet", ddl)
        case _:
            raise ValueError("DDL not yet supported", ddl)

def process_ddls(
    schema: e.DBSchema, 
    ddls: List[qlast.DDLOperation]) -> e.DBSchema:
    for ddl in ddls:
        schema = process_ddl(schema, ddl)
    return schema