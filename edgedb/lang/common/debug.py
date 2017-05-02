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
    pgsql_parser = False
    """Debug SQL parser."""

    edgeql_parser = False
    """Debug EdgeQL parser (rebuild grammar verbosly)."""

    edgeql_compile = False
    """Dump EdgeQL/IR/SQL ASTs."""

    edgeql_optimize = False
    """Dump SQL AST/Query before/after optimization."""

    delta_plan = False
    """Print expanded delta command tree prior to processing."""

    delta_pgsql_plan = False
    """Print delta command tree annortated with DB ops."""

    delta_plan_input = False
    """Print delta command tree produced from DDL."""

    delta_execute = False
    """Output SQL commands as executed during migration."""

    server = False
    """Print server errors."""

    print_locals = False
    """Include values of local variables in tracebacks."""


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
