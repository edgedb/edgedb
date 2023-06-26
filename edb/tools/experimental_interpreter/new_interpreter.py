

import json
import sys
import traceback
from typing import *
from typing import Tuple

from edb.common import debug
from edb.edgeql import ast as qlast

from . import typechecking as tc
from .back_to_ql import reverse_elab
from .basis.built_ins import all_builtin_funcs
from .data import data_ops as e
from .data import expr_ops as eops
from .data.data_ops import DB, DBSchema, MultiSetVal, ResultTp
from .data.expr_to_str import show_expr, show_result_tp, show_schema
from .data.path_factor import select_hoist
from .data.val_to_json import (json_like, multi_set_val_to_json_like,
                               typed_multi_set_val_to_json_like)
from .elab_schema import schema_from_sdl_defs, schema_from_sdl_file
from .elaboration import elab
from .evaluation import RTExpr, eval_expr_toplevel
from .helper_funcs import parse_ql
from .logs import write_logs_to_file
from .sqlite import sqlite_adapter

from .db_interface import *

# CODE REVIEW: !!! CHECK IF THIS WILL BE SET ON EVERY RUN!!!
# sys.setrecursionlimit(10000)


def empty_db(schema : DBSchema) -> EdgeDatabaseInterface:
    return InMemoryEdgeDatabase(schema)


def empty_dbschema() -> DBSchema:
    return DBSchema({}, all_builtin_funcs)



def run_statement(db: EdgeDatabaseInterface,
                  stmt: qlast.Expr, dbschema: DBSchema,
                  should_print: bool,
                  logs: Optional[List[Any]]
                  ) -> Tuple[MultiSetVal, e.ResultTp]:
    if should_print:
        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv Starting")
        debug.dump_edgeql(stmt)
        debug.print("Schema: " + show_schema(dbschema))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Elaborating")

    elaborated = elab(stmt)

    if should_print:
        debug.print(show_expr(elaborated))
        # debug.dump(reverse_elab(elaborated))
        debug.dump_edgeql(reverse_elab(elaborated))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Preprocessing")

    factored = select_hoist(elaborated, dbschema)

    if should_print:
        debug.print(show_expr(factored))
        reverse_elabed = reverse_elab(factored)
        debug.dump_edgeql(reverse_elabed)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Type Checking")

    tp, type_checked = tc.synthesize_type(e.TcCtx(dbschema, {}), factored)

    if should_print:
        debug.print(show_result_tp(tp))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running")

    result = eval_expr_toplevel(db, type_checked, logs=logs)
    if should_print:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Result")
        debug.print(result)
        print(typed_multi_set_val_to_json_like(
            tp, eops.assume_link_target(result), dbschema))
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Done ")
    return (result, tp)
    # debug.dump(stmt)


def run_stmts(db: EdgeDatabaseInterface, stmts: Sequence[qlast.Expr],
              dbschema: DBSchema, debug_print: bool,
              logs: Optional[List[Any]]
              ) -> Sequence[MultiSetVal]:
    match stmts:
        case []:
            return []
        case current, *rest:
            (cur_val, _) = run_statement(
                db, current, dbschema, should_print=debug_print,
                logs=logs)
            rest_val = run_stmts(
                db, rest, dbschema, debug_print,
                logs=logs)
            return [cur_val, *rest_val]
    raise ValueError("Not Possible")


def run_str(
    db: EdgeDatabaseInterface,
    dbschema: DBSchema,
    s: str,
    print_asts: bool = False,
    logs: Optional[List[str]] = None
) -> Sequence[MultiSetVal]:
    q = parse_ql(s)
    # if print_asts:
    #     debug.dump(q)
    res = run_stmts(db, q, dbschema, print_asts, logs)
    # if output_mode == 'pprint':
    #     pprint.pprint(res)
    # elif output_mode == 'json':
    #     print(EdbJSONEncoder().encode(res))
    # elif output_mode == 'debug':
    #     debug.dump(res)
    return res


def run_single_str(
    dbschema_and_db: Tuple[DBSchema, EdgeDatabaseInterface],
    s: str,
    print_asts: bool = False
) -> Tuple[MultiSetVal, ResultTp]:
    q = parse_ql(s)
    if len(q) != 1:
        raise ValueError("Not a single query")
    dbschema, db = dbschema_and_db
    (res, tp) = run_statement(
        db, q[0], dbschema, print_asts,
        logs=None)
    return (res, tp)


def run_single_str_get_json(
    dbschema_and_db: Tuple[DBSchema, EdgeDatabaseInterface],
    s: str,
    print_asts: bool = False
) -> json_like:
    (res, tp) = run_single_str(dbschema_and_db,
                                        s, print_asts=print_asts)
    return typed_multi_set_val_to_json_like(
                tp, res, dbschema_and_db[0], top_level=True)


def repl(*, init_sdl_file=None,
         init_ql_file=None,
         debug_print=False,
         trace_to_file_path=None,
         sqlite_file=None,
         ) -> None:
    # if init_sdl_file is not None and read_sqlite_file is not None:
    #     raise ValueError("Init SDL file and Read SQLite file cannot"
    #                      " be specified at the same time")

    dbschema: DBSchema
    db: EdgeDatabaseInterface
    logs: List[Any] = []  # type: ignore[var]

    if sqlite_file is not None:
        if init_sdl_file is not None:
            with open(init_sdl_file) as f:
                init_sdl_file_content = f.read()
        else:
            init_sdl_file_content = None
        (dbschema, db) = sqlite_adapter.schema_and_db_from_sqlite(init_sdl_file_content, sqlite_file)
    else:
        if init_sdl_file is not None:
            dbschema = schema_from_sdl_file(init_sdl_file_path=init_sdl_file)
        else:
            dbschema = empty_dbschema()
        db = empty_db(dbschema)

    if init_ql_file is not None:
        initial_queries = open(init_ql_file).read()
        run_str(db, dbschema, initial_queries,
                          print_asts=debug_print, logs=logs)
    while True:
        if trace_to_file_path is not None:
            write_logs_to_file(logs, trace_to_file_path)
        print("> ", end="", flush=True)
        s = ""
        while ';' not in s:
            s += sys.stdin.readline()
            if not s:
                return
        try:
            res = run_str(db, dbschema, s, print_asts=debug_print,
                                logs=logs)
            print("\n".join(json.dumps(multi_set_val_to_json_like(v))
                            for v in res))
        except Exception:
            traceback.print_exception(*sys.exc_info())


def dbschema_and_db_with_initial_schema_and_queries(
        initial_schema_defs: str,
        initial_queries: str,
        sqlite_file_name: Optional[str] = None,
        debug_print=False,
        logs: Optional[List[Any]] = None) -> Tuple[DBSchema, EdgeDatabaseInterface]:
    if sqlite_file_name is not None:
        dbschema, db = sqlite_adapter.schema_and_db_from_sqlite(initial_schema_defs, sqlite_file_name)
    else:
        dbschema = schema_from_sdl_defs(initial_schema_defs)
        db = empty_db(dbschema)
    run_str(db, dbschema, initial_queries,
                      print_asts=debug_print, logs=logs)
    return dbschema, db


if __name__ == "__main__":
    repl()
