


from data_ops import *
from helper_funcs import *
import sys
import traceback
from edb.edgeql import ast as qlast
from edb import edgeql

import pprint

from edb.common import debug
from elaboration import *

import click

from evaluation import *
import copy

def run_statement(db : DB, stmt : qlast.Expr) -> DB:
    print("running")
    elaborated = elab(stmt)
    config = RTConfig(db, [copy.deepcopy(db)], elaborated)
    resultdb = eval_config(config)
    return resultdb.cur_db
    # debug.dump(stmt)

def run_stmts (db : DB, stmts : List[qlast.Expr]):
    match stmts:
        case []:
            return db
        case current, *rest:
            return run_stmts(run_statement(db, current), rest)
    raise ValueError("Not Possible")

def run(
    db: DB,
    s: str,
    print_asts: bool = False, output_mode: str = "debug"
) -> None:
    q = parse(s)
    if print_asts:
        debug.dump(q)
    res = run_stmts(db, q)
    if output_mode == 'pprint':
        pprint.pprint(res)
    elif output_mode == 'json':
        print(EdbJSONEncoder().encode(res))
    else:
        debug.dump(res)
    return res


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--init-ql-file", type=str, required=False)
def repl(*, init_ql_file = None) -> None:
    # for now users should just invoke this script with rlwrap since I
    # don't want to fiddle with history or anything
    db = empty_db()
    if init_ql_file is not None:
        initial_queries = open(init_ql_file).read()
        db = run(db, initial_queries)
    while True:
        print("> ", end="", flush=True)
        s = ""
        while ';' not in s:
            s += sys.stdin.readline()
            if not s:
                return
        try:
            db = run(db, s)
        except Exception:
            traceback.print_exception(*sys.exc_info())

if __name__ == "__main__":
    repl()