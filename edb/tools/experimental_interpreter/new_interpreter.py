

import sys
import traceback
from typing import *
from typing import Tuple

import json
from edb.common import debug
from edb.edgeql import ast as qlast

from .back_to_ql import reverse_elab
from .basis.built_ins import all_builtin_funcs
from .data import data_ops as e
from .data import expr_ops as eops
from .data.expr_to_str import show_expr, show_result_tp
from .data.data_ops import DB, DBSchema, MultiSetVal
from .data.path_factor import select_hoist
from .data.val_to_json import json_like, multi_set_val_to_json_like
from .elab_schema import schema_from_sdl_defs, schema_from_sdl_file
from .elaboration import elab
from .evaluation import RTData, RTExpr, eval_config_toplevel
from .helper_funcs import parse_ql
from .logs import write_logs_to_file
from .sqlite import sqlite_adapter
from . import typechecking as tc

# CODE REVIEW: !!! CHECK IF THIS WILL BE SET ON EVERY RUN!!!
# sys.setrecursionlimit(10000)


def empty_db() -> DB:
    return DB({})


def empty_dbschema() -> DBSchema:
    return DBSchema({}, all_builtin_funcs)


def run_statement(db: DB, stmt: qlast.Expr, dbschema: DBSchema,
                  should_print: bool,
                  logs: Optional[List[Any]]) -> Tuple[MultiSetVal, DB]:
    if should_print:
        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv Starting")
        debug.dump_edgeql(stmt)
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

    statics = RTData(DB(db.dbdata), [DB({**db.dbdata})], dbschema, False)

    tp, type_checked = tc.synthesize_type(e.TcCtx(statics, {}), factored)

    if should_print:
        debug.print(show_result_tp(tp))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running")

    config = RTExpr(statics, type_checked)
    result = eval_config_toplevel(config, logs=logs)
    if should_print:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Result")
        debug.print(result.val)
        print(multi_set_val_to_json_like(eops.assume_link_target(result.val)))
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Done ")
    return (result.val, result.data.cur_db)
    # debug.dump(stmt)


def run_stmts(db: DB, stmts: Sequence[qlast.Expr],
              dbschema: DBSchema, debug_print: bool,
              logs: Optional[List[Any]]
              ) -> Tuple[Sequence[MultiSetVal], DB]:
    match stmts:
        case []:
            return ([], db)
        case current, *rest:
            (cur_val, next_db) = run_statement(
                db, current, dbschema, should_print=debug_print,
                logs=logs)
            (rest_val, final_db) = run_stmts(
                next_db, rest, dbschema, debug_print,
                logs=logs)
            return ([cur_val, *rest_val], final_db)
    raise ValueError("Not Possible")


def run_str(
    db: DB,
    dbschema: DBSchema,
    s: str,
    print_asts: bool = False,
    logs: Optional[List[str]] = None
) -> Tuple[Sequence[MultiSetVal], DB]:
    q = parse_ql(s)
    # if print_asts:
    #     debug.dump(q)
    (res, next_db) = run_stmts(db, q, dbschema, print_asts, logs)
    # if output_mode == 'pprint':
    #     pprint.pprint(res)
    # elif output_mode == 'json':
    #     print(EdbJSONEncoder().encode(res))
    # elif output_mode == 'debug':
    #     debug.dump(res)
    return (res, next_db)


def run_single_str(
    dbschema_and_db: Tuple[DBSchema, DB],
    s: str,
    print_asts: bool = False
) -> Tuple[MultiSetVal, DB]:
    q = parse_ql(s)
    if len(q) != 1:
        raise ValueError("Not a single query")
    dbschema, db = dbschema_and_db
    (res, next_db) = run_statement(
        db, q[0], dbschema, print_asts,
        logs=None)
    return (res, next_db)


def run_single_str_get_json(
    dbschema_and_db: Tuple[DBSchema, DB],
    s: str,
    print_asts: bool = False
) -> Tuple[json_like, DB]:
    (res, next_db) = run_single_str(dbschema_and_db,
                                    s, print_asts=print_asts)
    return (multi_set_val_to_json_like(res), next_db)


def repl(*, init_sdl_file=None,
         init_ql_file=None,
         debug_print=False,
         trace_to_file_path=None,
         read_sqlite_file=None,
         write_sqlite_file=None
         ) -> None:
    if init_sdl_file is not None and read_sqlite_file is not None:
        raise ValueError("Init SDL file and Read SQLite file cannot"
                         " be specified at the same time")

    dbschema: DBSchema
    db: DB
    logs: List[Any] = []  # type: ignore[var]

    if read_sqlite_file is not None:
        (dbschema, db) = sqlite_adapter.unpickle_from_sqlite(read_sqlite_file)
    else:
        db = empty_db()
        if init_sdl_file is not None:
            dbschema = schema_from_sdl_file(init_sdl_file_path=init_sdl_file)
        else:
            dbschema = empty_dbschema()

    if init_ql_file is not None:
        initial_queries = open(init_ql_file).read()
        (_, db) = run_str(db, dbschema, initial_queries,
                          print_asts=debug_print, logs=logs)
    while True:
        if trace_to_file_path is not None:
            write_logs_to_file(logs, trace_to_file_path)
        if write_sqlite_file is not None:
            sqlite_adapter.pickle_to_sqlite(dbschema, db, write_sqlite_file)
        print("> ", end="", flush=True)
        s = ""
        while ';' not in s:
            s += sys.stdin.readline()
            if not s:
                return
        try:
            (res, db) = run_str(db, dbschema, s, print_asts=debug_print,
                                logs=logs)
            print("\n".join(json.dumps(multi_set_val_to_json_like(v))
                            for v in res))
        except Exception:
            traceback.print_exception(*sys.exc_info())


def dbschema_and_db_with_initial_schema_and_queries(
        initial_schema_defs: str,
        initial_queries: str,
        debug_print=False,
        logs: Optional[List[Any]] = None) -> Tuple[DBSchema, DB]:
    db = empty_db()
    dbschema = schema_from_sdl_defs(
        initial_schema_defs)
    (_, db) = run_str(db, dbschema, initial_queries,
                      print_asts=debug_print, logs=logs)
    return dbschema, db


if __name__ == "__main__":
    repl()
