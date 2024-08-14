from __future__ import annotations


import sys
import traceback
import os
from typing import Tuple
try:
    import readline
except ImportError:
    readline = None  # type: ignore[assignment]

from edb.common import debug
from edb.edgeql import ast as qlast

from typing import Optional, Dict, Any, List, Sequence
from .type_checking_tools import typechecking as tc
from .back_to_ql import reverse_elab
from .data import data_ops as e
from .data.data_ops import DBSchema, MultiSetVal, ResultTp, Val, Expr
from .data.expr_to_str import show_expr, show_result_tp
from .data.path_factor import select_hoist
from .post_processing_tools import post_processing
from .data.val_to_json import json_like, typed_multi_set_val_to_json_like
from .elab_schema import add_module_from_sdl_defs, add_module_from_sdl_file
from .elaboration import elab
from .evaluation import eval_expr_toplevel
from .helper_funcs import parse_ql
from .logs import write_logs_to_file
from .sqlite import sqlite_adapter
from .data import expr_to_str as pp
from .db_interface import EdgeDatabase, InMemoryEdgeDatabaseStorageProvider
from .schema.library_discovery import add_ddl_library
from .type_checking_tools import schema_checking as sck
from .type_checking_tools import name_resolution

# CODE REVIEW: !!! CHECK IF THIS WILL BE SET ON EVERY RUN!!!
# sys.setrecursionlimit(10000)


VariablesTp = Optional[Dict[str, Val] | Tuple[Val, ...]]


def empty_db(schema: DBSchema) -> EdgeDatabase:
    storage = InMemoryEdgeDatabaseStorageProvider(schema)
    return EdgeDatabase(storage)


def empty_dbschema() -> DBSchema:
    return DBSchema({}, {}, {}, {}, {})


def default_dbschema() -> DBSchema:
    initial_db = empty_dbschema()
    relative_path_to_std = os.path.join("..", "..", "lib", "std")
    relative_path_to_schema = os.path.join("..", "..", "lib", "schema.edgeql")
    relative_path_to_cal = os.path.join("..", "..", "lib", "cal.edgeql")
    relative_path_to_math = os.path.join("..", "..", "lib", "math.edgeql")
    relative_path_to_interpreter_internal = os.path.join(
        "basis", "80-interpreter-internal.edgeql"
    )
    std_path = os.path.join(os.path.dirname(__file__), relative_path_to_std)
    schema_path = os.path.join(
        os.path.dirname(__file__), relative_path_to_schema
    )
    cal_path = os.path.join(os.path.dirname(__file__), relative_path_to_cal)
    math_path = os.path.join(os.path.dirname(__file__), relative_path_to_math)
    interpreter_internal_path = os.path.join(
        os.path.dirname(__file__), relative_path_to_interpreter_internal
    )
    add_ddl_library(
        initial_db,
        [
            std_path,
            schema_path,
            cal_path,
            math_path,
            interpreter_internal_path,
        ],
    )
    name_resolution.checked_module_name_resolve(initial_db, ("schema",))
    name_resolution.checked_module_name_resolve(initial_db, ("std",))
    sck.re_populate_module_inheritance(initial_db, ("std",))
    sck.re_populate_module_inheritance(initial_db, ("schema",))
    return initial_db


def prepare_statement(
    stmt: qlast.Expr,
    dbschema: DBSchema,
    should_print: bool,
) -> Tuple[e.Expr, e.ResultTp]:
    dbschema_ctx = e.TcCtx(dbschema, ("default",), {})

    if should_print:
        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv Starting")
        debug.dump_edgeql(stmt)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Elaborating")

    elaborated = elab(stmt)

    if should_print:
        debug.print(show_expr(elaborated))
        debug.dump_edgeql(reverse_elab(elaborated))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Preprocessing")

    factored = select_hoist(elaborated, dbschema_ctx)

    if should_print:
        debug.print(show_expr(factored))
        reverse_elabed = reverse_elab(factored)
        debug.dump_edgeql(reverse_elabed)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Type Checking")

    tp, type_checked = tc.synthesize_type(dbschema_ctx, factored)

    if should_print:
        debug.print(show_result_tp(tp))
        reverse_elabed = reverse_elab(type_checked)
        debug.dump_edgeql(reverse_elabed)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Post Processing")

    deduped = post_processing.post_process(type_checked)

    if should_print:
        debug.print(pp.show(deduped))
        reverse_elabed = reverse_elab(deduped)
        debug.dump_edgeql(reverse_elabed)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running")

    return deduped, tp


def run_prepared_statement(
    db: EdgeDatabase,
    deduped: e.Expr,
    tp: e.ResultTp,
    dbschema: DBSchema,
    should_print: bool,
    logs: Optional[List[Any]],
    variables: VariablesTp = None,
) -> MultiSetVal:
    result = eval_expr_toplevel(db, deduped, variables=variables, logs=logs)
    if should_print:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Result")
        debug.print(pp.show_multiset_val(result))
        print(typed_multi_set_val_to_json_like(tp, result, dbschema))
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Done ")
    return result


def run_statement(
    db: EdgeDatabase,
    stmt: qlast.Expr,
    dbschema: DBSchema,
    should_print: bool,
    logs: Optional[List[Any]],
    variables: VariablesTp = None,
) -> Tuple[MultiSetVal, e.ResultTp]:

    deduped, tp = prepare_statement(stmt, dbschema, should_print)
    result = run_prepared_statement(
        db, deduped, tp, dbschema, should_print, logs, variables
    )
    return result, tp


def run_stmts(
    db: EdgeDatabase,
    stmts: Sequence[qlast.Expr],
    dbschema: DBSchema,
    debug_print: bool,
    logs: Optional[List[Any]],
) -> Sequence[MultiSetVal]:
    match stmts:
        case []:
            return []
        case current, *rest:
            (cur_val, _) = run_statement(
                db,
                current,
                dbschema,
                should_print=debug_print,
                logs=logs,
            )
            rest_val = run_stmts(
                db,
                rest,
                dbschema,
                debug_print,
                logs=logs,
            )
            return [cur_val, *rest_val]
    raise ValueError("Not Possible")


def run_meta_cmd(db: EdgeDatabase, dbschema: DBSchema, cmd: str) -> None:
    if cmd == r"\ps":
        print(pp.show_module(dbschema.modules[("default",)]) + "\n")
    elif cmd == r"\ps --all":
        print(pp.show_schema(dbschema) + "\n")
    else:
        raise ValueError("Unknown meta command: " + cmd)


def run_str(
    db: EdgeDatabase,
    dbschema: DBSchema,
    s: str,
    print_asts: bool = False,
    logs: Optional[List[str]] = None,
) -> Sequence[MultiSetVal]:

    q = parse_ql(s)
    res = run_stmts(db, q, dbschema, print_asts, logs)
    return res


def run_single_str(
    dbschema_and_db: Tuple[DBSchema, EdgeDatabase],
    s: str,
    variables: VariablesTp = None,
    print_asts: bool = False,
) -> Tuple[MultiSetVal, ResultTp]:
    q = parse_ql(s)
    if len(q) != 1:
        raise ValueError("Not a single query")
    dbschema, db = dbschema_and_db
    (res, tp) = run_statement(
        db, q[0], dbschema, print_asts, variables=variables, logs=None
    )
    return (res, tp)


def run_single_str_get_json(
    dbschema_and_db: Tuple[DBSchema, EdgeDatabase],
    s: str,
    variables: VariablesTp = None,
    print_asts: bool = False,
) -> json_like:
    (res, tp) = run_single_str(
        dbschema_and_db, s, variables=variables, print_asts=print_asts
    )
    return typed_multi_set_val_to_json_like(
        tp, res, dbschema_and_db[0], top_level=True
    )


def interpreter_parser_init():
    from edb.edgeql import parser as ql_parser

    ql_parser.preload_spec()


def repl(
    *,
    init_sdl_file=None,
    init_ql_file=None,
    next_ql_file=None,
    library_ddl_files=None,
    debug_print=False,
    trace_to_file_path=None,
    sqlite_file=None,
) -> None:
    interpreter_parser_init()

    dbschema: DBSchema
    db: EdgeDatabase
    logs: List[Any] = []  # type: ignore[var]

    dbschema = default_dbschema()
    if library_ddl_files:
        add_ddl_library(dbschema, library_ddl_files)

    if sqlite_file is not None:
        if init_sdl_file is not None:
            with open(init_sdl_file) as f:
                init_sdl_file_content = f.read()
        else:
            init_sdl_file_content = None
        (dbschema, db) = sqlite_adapter.schema_and_db_from_sqlite(
            init_sdl_file_content, sqlite_file
        )
    else:
        if init_sdl_file is not None:
            dbschema = add_module_from_sdl_file(
                dbschema, init_sdl_file_path=init_sdl_file
            )
        else:
            dbschema = dbschema
        db = empty_db(dbschema)

    if debug_print:
        print("=== ALL Schema Loaded ===")
        print(pp.show_module(dbschema.modules[("default",)]))

    if init_ql_file is not None:
        initial_queries = open(init_ql_file).read()
        run_str(
            db, dbschema, initial_queries, print_asts=debug_print, logs=logs
        )

    try:
        if next_ql_file is not None:
            next_queries = open(next_ql_file).read()
            run_str(
                db, dbschema, next_queries, print_asts=debug_print, logs=logs
            )
    except Exception:
        traceback.print_exception(*sys.exc_info())

    history_file = ".edgeql_interpreter_history.temp.txt"
    try:
        if readline:
            readline.read_history_file(history_file)
    except FileNotFoundError:
        pass

    while True:
        if trace_to_file_path is not None:
            write_logs_to_file(logs, trace_to_file_path)
        s = ""

        def reset_s():
            nonlocal s
            print("\nKeyboard Interrupt")
            s = ""

        while ';' not in s and not s.startswith("\\"):
            if s:
                try:
                    s += input("... ")
                except KeyboardInterrupt:
                    reset_s()
                    continue
            else:
                try:
                    s += input("> ")
                except KeyboardInterrupt:
                    reset_s()
                    continue
        try:
            if readline:
                readline.write_history_file(history_file)
            if s.startswith("\\"):
                run_meta_cmd(db, dbschema, s)
            else:
                run_str(db, dbschema, s, print_asts=debug_print, logs=logs)
        except Exception:
            traceback.print_exception(*sys.exc_info())


def dbschema_and_db_with_initial_schema_and_queries(
    initial_schema_defs: Optional[str],
    initial_queries: str,
    sqlite_file_name: Optional[str] = None,
    debug_print=False,
    logs: Optional[List[Any]] = None,
) -> Tuple[DBSchema, EdgeDatabase]:
    if sqlite_file_name is not None:
        dbschema, db = sqlite_adapter.schema_and_db_from_sqlite(
            initial_schema_defs, sqlite_file_name
        )
    else:
        dbschema = default_dbschema()
        if initial_schema_defs is not None:
            dbschema = add_module_from_sdl_defs(dbschema, initial_schema_defs)
        db = empty_db(dbschema)
    run_str(db, dbschema, initial_queries, print_asts=debug_print, logs=logs)
    return dbschema, db


class EdgeQLInterpreter:

    def __init__(
        self,
        initial_schema_defs: Optional[str] = None,
        sqlite_file_name: Optional[str] = None,
    ):
        interpreter_parser_init()
        dbschema, db = dbschema_and_db_with_initial_schema_and_queries(
            initial_schema_defs, "", sqlite_file_name
        )
        self.dbschema: e.DBSchema = dbschema
        self.db: EdgeDatabase = db
        self.query_cache: Dict[str, Tuple[Expr, ResultTp]] = {}

    def run_single_str_get_json_with_cache(
        self,
        s: str,
        variables: VariablesTp = None,
        disable_cache: bool = False,
    ) -> json_like:
        if not disable_cache and s in self.query_cache:
            (query_expr, tp) = self.query_cache[s]
        else:
            q = parse_ql(s)
            if len(q) != 1:
                raise ValueError("Not a single query")
            query_expr, tp = prepare_statement(q[0], self.dbschema, False)
            self.query_cache[s] = (query_expr, tp)

        res = run_prepared_statement(
            self.db,
            query_expr,
            tp,
            self.dbschema,
            should_print=False,
            logs=None,
            variables=variables,
        )
        result = typed_multi_set_val_to_json_like(
            tp, res, self.dbschema, top_level=True
        )

        return result

    def query_single_json(self, s: str, **kwargs) -> json_like:
        result = self.run_single_str_get_json_with_cache(s, kwargs)
        if isinstance(result, list) and len(result) == 1:
            return result[0]
        else:
            raise ValueError("Expected a single result")

    def query_json(self, s: str, **kwargs) -> json_like:
        result = self.run_single_str_get_json_with_cache(s, kwargs)
        return result

    def query_str(self, s: str) -> Sequence[MultiSetVal]:
        q = parse_ql(s)
        res = run_stmts(
            self.db, q, self.dbschema, debug_print=False, logs=None
        )
        return res


if __name__ == "__main__":
    repl()
