##
# Copyright (c) 2016-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


"""Debug flags and output facilities.

An example code using this module:

    if debug.flags.some_sql_flag:
        debug.header('SQL')
        debug.dump(sql_ast)

Use `debug.header()`, `debug.print()`, `debug.dump()` and `debug.dump_code()`
functions as opposed to using 'print' built-in directly.  This gives us
flexibility to redirect debug output if needed.
"""


import builtins
import os
import warnings

from . import markup as _markup


__all__ = ()  # Don't.


class flags:
    pgsql_parser: 'Debug SQL parser' = False
    edgeql_parser: 'Debug EdgeQL parser (rebuild grammar verbosly)' = False
    edgeql_compile: 'Dump EdgeQL/IR/SQL ASTs' = False
    edgeql_optimize: 'Dump SQL AST/Query before/after optimization' = False
    delta_plan: 'Print expanded delta command tree prior to processing' = False
    delta_pgsql_plan: 'Print delta command tree annortated with DB ops' = False
    delta_plan_input: 'Print delta command tree produced from DDL' = False
    delta_execute: 'Output SQL commands as executed during migration' = False
    server: 'Print server errors' = False
    print_locals: 'Dump local variables in tracebacks' = False


def header(*args):
    print('=' * 80)
    print(*args)
    print('=' * 80)


def dump(*args, **kwargs):
    _markup.dump(*args, **kwargs)


def dump_code(*args, **kwargs):
    _markup.dump_code(*args, **kwargs)


def print(*args):
    builtins.print(*args)


def init_debug_flags():
    prefix = 'EDGEDB_DEBUG_'

    for env_name, env_val in os.environ.items():
        if not env_name.startswith(prefix):
            continue

        name = env_name[len(prefix):].lower()
        if not hasattr(flags, name):
            warnings.warn(f'Unknown debug flag: {env_name!r}', stacklevel=2)
            continue

        if env_val.strip() in {'', '0'}:
            continue

        setattr(flags, name, True)


init_debug_flags()
