

import sys
import traceback
from typing import *
from typing import Tuple

from edb.common import debug
from edb.edgeql import ast as qlast

from .back_to_ql import reverse_elab
from .basis.built_ins import all_builtin_funcs
from .data.data_ops import DB, DBSchema, MultiSetVal, empty_db
from .data.path_factor import select_hoist
from .data.val_to_json import (json_like, multi_set_val_to_json_like,
                               val_to_json_like)
from .elab_schema import schema_from_sdl_defs, schema_from_sdl_file
from .elaboration import elab
from .evaluation import RTData, RTExpr, eval_config_toplevel
from .helper_funcs import parse_ql

# CODE REVIEW: !!! CHECK IF THIS WILL BE SET ON EVERY RUN!!!
# sys.setrecursionlimit(10000)


def run_statement(db: DB, stmt: qlast.Expr, dbschema: DBSchema,
                  should_print: bool) -> Tuple[MultiSetVal, DB]:
    if should_print:
        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv Starting")
        debug.dump_edgeql(stmt)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Elaborating")

    elaborated = elab(stmt)

    if should_print:
        debug.print(elaborated)
        # debug.dump(reverse_elab(elaborated))
        debug.dump_edgeql(reverse_elab(elaborated))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Preprocessing")

    factored = select_hoist(elaborated, dbschema)

    if should_print:
        debug.print(factored)
        reverse_elabed = reverse_elab(factored)
        debug.dump_edgeql(reverse_elabed)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running")

    config = RTExpr(
        RTData(DB(db.dbdata),
               [DB({**db.dbdata})],
               dbschema,
               False
               ), factored)
    result = eval_config_toplevel(config)
    if should_print:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Result")
        debug.print(result.val)
        print([val_to_json_like(v) for v in result.val])
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Done ")
    return (result.val, result.data.cur_db)
    # debug.dump(stmt)


def run_stmts(db: DB, stmts: Sequence[qlast.Expr],
              dbschema: DBSchema, debug_print: bool
              ) -> Tuple[Sequence[MultiSetVal], DB]:
    match stmts:
        case []:
            return ([], db)
        case current, *rest:
            (cur_val, next_db) = run_statement(
                db, current, dbschema, should_print=debug_print)
            (rest_val, final_db) = run_stmts(
                next_db, rest, dbschema, debug_print)
            return ([cur_val, *rest_val], final_db)
    raise ValueError("Not Possible")


def run_str(
    db: DB,
    dbschema: DBSchema,
    s: str,
    print_asts: bool = False
) -> Tuple[Sequence[MultiSetVal], DB]:
    q = parse_ql(s)
    # if print_asts:
    #     debug.dump(q)
    (res, next_db) = run_stmts(db, q, dbschema, print_asts)
    # if output_mode == 'pprint':
    #     pprint.pprint(res)
    # elif output_mode == 'json':
    #     print(EdbJSONEncoder().encode(res))
    # elif output_mode == 'debug':
    #     debug.dump(res)
    return (res, next_db)


def run_single_str(
    db: DB,
    s: str,
    print_asts: bool = False
) -> Tuple[MultiSetVal, DB]:
    q = parse_ql(s)
    if len(q) != 1:
        raise ValueError("Not a single query")
    (res, next_db) = run_statement(
        db, q[0], DBSchema({}, all_builtin_funcs), print_asts)
    return (res, next_db)


def run_single_str_get_json(
    db: DB,
    s: str,
    print_asts: bool = False
) -> Tuple[json_like, DB]:
    (res, next_db) = run_single_str(db, s, print_asts=print_asts)
    return (multi_set_val_to_json_like(res), next_db)


def repl(*, init_sdl_file=None, init_ql_file=None, debug_print=False) -> None:
    # for now users should just invoke this script with rlwrap since I
    # don't want to fiddle with history or anything
    db = empty_db()
    dbschema: DBSchema
    if init_sdl_file is not None:
        dbschema = schema_from_sdl_file(init_sdl_file_path=init_sdl_file)
    else:
        dbschema = DBSchema({}, all_builtin_funcs)
    if init_ql_file is not None:
        initial_queries = open(init_ql_file).read()
        (_, db) = run_str(db, dbschema, initial_queries,
                          print_asts=debug_print)
    while True:
        print("> ", end="", flush=True)
        s = ""
        while ';' not in s:
            s += sys.stdin.readline()
            if not s:
                return
        try:
            (_, db) = run_str(db, dbschema, s, print_asts=debug_print)
        except Exception:
            traceback.print_exception(*sys.exc_info())


def db_with_initial_schema_and_queries(
        initial_schema_defs: str,
        initial_queries: str,
        surround_schema_with_default: bool,
        debug_print=False) -> DB:
    db = empty_db()
    dbschema = schema_from_sdl_defs(
                    initial_schema_defs,
                    surround_with_default=surround_schema_with_default)
    (_, db) = run_str(db, dbschema, initial_queries, print_asts=debug_print)
    return db


if __name__ == "__main__":
    repl()
