


from .data.data_ops import *
from .helper_funcs import *
import sys
import traceback
from edb.edgeql import ast as qlast
from edb import edgeql
import pprint
from .data.built_in_ops import all_builtin_funcs
from edb.common import debug
from .elaboration import *


from .evaluation import *
from .back_to_ql import reverse_elab
import copy

def run_statement(db : DB, stmt : qlast.Expr, should_print : bool) -> DB:
    if should_print:
        print("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv Starting")
        debug.dump_edgeql(stmt)
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Elaborating")

    elaborated = elab(stmt)

    if should_print:
        debug.print(elaborated)
        # debug.dump(reverse_elab(elaborated))
        debug.dump_edgeql(reverse_elab(elaborated))
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Running")

    config = RTExpr(
            RTData(DB(db.dbdata), 
                [DB({**db.dbdata})],
                DBSchema({}, all_builtin_funcs),
                False
            ), elaborated)
    result = eval_config(config)
    if should_print:
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Result")
        debug.print(result.val)
        print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Done ")
    return result.data.cur_db
    # debug.dump(stmt)

def run_stmts (db : DB, stmts : List[qlast.Expr], debug_print : bool) -> DB:
    match stmts:
        case []:
            return db
        case current, *rest:
            return run_stmts(run_statement(db, current, should_print=debug_print), rest, debug_print)
    raise ValueError("Not Possible")

def run(
    db: DB,
    s: str,
    print_asts: bool = False, output_mode: Optional[str] = None
) -> DB:
    q = parse(s)
    # if print_asts:
    #     debug.dump(q)
    res = run_stmts(db, q, print_asts)
    if output_mode == 'pprint':
        pprint.pprint(res)
    elif output_mode == 'json':
        print(EdbJSONEncoder().encode(res))
    elif output_mode == 'debug':
        debug.dump(res)
    return res



def repl(*, init_ql_file = None, debug_print=False) -> None:
    # for now users should just invoke this script with rlwrap since I
    # don't want to fiddle with history or anything
    db = empty_db()
    if init_ql_file is not None:
        initial_queries = open(init_ql_file).read()
        db = run(db, initial_queries, print_asts=debug_print)
    while True:
        print("> ", end="", flush=True)
        s = ""
        while ';' not in s:
            s += sys.stdin.readline()
            if not s:
                return
        try:
            db = run(db, s, print_asts=debug_print)
        except Exception:
            traceback.print_exception(*sys.exc_info())

if __name__ == "__main__":
    repl()