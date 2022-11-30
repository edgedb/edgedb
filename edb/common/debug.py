#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Debug flags and output facilities.

An example code using this module:

    if debug.flags.some_sql_flag:
        debug.header('SQL')
        debug.dump(sql_ast)

Use `debug.header()`, `debug.print()`, `debug.dump()` and `debug.dump_code()`
functions as opposed to using 'print' built-in directly.  This gives us
flexibility to redirect debug output if needed.
"""


from __future__ import annotations

import builtins
import contextlib
import os
import sys
import time
import warnings

# Don't import anything from "edb.*" as it will wreck coverage.


__all__ = ()  # Don't.


class FlagsMeta(type):
    def __new__(mcls, name, bases, dct):
        flags = {}
        for flagname, flag in dct.items():
            if not isinstance(flag, Flag):
                continue
            flag.name = flagname
            flags[flagname] = flag
            dct[flagname] = False

        dct['_items'] = flags
        return super().__new__(mcls, name, bases, dct)

    def __iter__(cls):
        return iter(cls._items.values())


class Flag:
    def __init__(self, *, doc: str):
        self.name = None
        self.doc = doc


class flags(metaclass=FlagsMeta):
    pgsql_parser = Flag(
        doc="Debug SQL parser.")

    bootstrap = Flag(
        doc="Debug server catalog bootstrap.")

    bootstrap_cache_yolo = Flag(
        doc="Disable bootstrap cache consistency check.")

    edgeql_parser = Flag(
        doc="Debug EdgeQL parser (rebuild grammar verbosly).")

    edgeql_compile = Flag(
        doc="Dump EdgeQL/IR/SQL ASTs.")

    edgeql_compile_edgeql_text = Flag(
        doc="Dump EdgeQL Text (subset of `edgeql_compile').")

    edgeql_compile_edgeql_ast = Flag(
        doc="Dump EdgeQL AST (subset of `edgeql_compile').")

    edgeql_compile_scope = Flag(
        doc="Dump EdgeQL scope tree (subset of `edgeql_compile').")

    edgeql_compile_ir = Flag(
        doc="Dump EdgeQL IR (subset of `edgeql_compile').")

    edgeql_compile_sql_ast = Flag(
        doc="Dump generated SQL AST (subset of `edgeql_compile').")

    edgeql_compile_sql_ast_meta = Flag(
        doc="Whether to include the metadata fields when dumping the SQL AST.")

    edgeql_compile_sql_text = Flag(
        doc="Dump generated SQL text (subset of `edgeql_compile').")

    edgeql_compile_sql_reordered_text = Flag(
        doc="Dump generated SQL-like text that might better reflect scoping.")

    edgeql_explain = Flag(
        doc="Dump extra debug info when doing EXPLAIN")

    edgeql_disable_normalization = Flag(
        doc="Disable EdgeQL normalization (constant extraction etc)")

    edgeql_expand_inhviews = Flag(
        doc="Force the EdgeQL compiler to expand inhviews *always*")

    graphql_compile = Flag(
        doc="Debug GraphQL compiler.")

    delta_plan = Flag(
        doc="Print expanded delta command tree prior to processing.")

    delta_pgsql_plan = Flag(
        doc="Print delta command tree annortated with DB ops.")

    delta_plan_input = Flag(
        doc="Print delta command tree produced from DDL.")

    delta_execute = Flag(
        doc="Output SQL commands as executed during migration.")

    server = Flag(
        doc="Print server errors.")

    server_proto = Flag(
        doc="Print server protocol querying messages.")

    http_inject_cors = Flag(
        doc="Inject 'Access-Control-Allow-Origin: *' header in HTTP ports.")

    print_locals = Flag(
        doc="Include values of local variables in tracebacks.")

    disable_qcache = Flag(
        doc="Disable server query cache. Parse/Execute will always recompile.")

    typecheck = Flag(
        doc="Perform runtime type checking.")

    pgserver = Flag(
        doc="Show PostgreSQL server logs and log all statements.")

    log_metrics = Flag(
        doc="Log verbose statistics on connections and compiler behavior.")

    disable_docs_edgeql_validation = Flag(
        doc="Disable validation of edgeql in docs (for site build)")

    pydebug_listen = Flag(
        doc="Enable listening for Debug Adapter Protocol connections. "
            "Requires pydebug to be installed."
    )


@contextlib.contextmanager
def timeit(title='block'):
    st = time.monotonic()
    try:
        yield
    finally:
        print(f'{title} took {time.monotonic() - st:.4f}s')


def header(*args):
    print('=' * 80)
    print(*args)
    print('=' * 80)


def dump(*args, **kwargs):
    from . import markup as _markup
    _markup.dump(*args, **kwargs)


def dump_code(*args, **kwargs):
    from . import markup as _markup
    _markup.dump_code(*args, **kwargs)


def dump_sql(sql, *args, **kwargs):
    import edb.pgsql.codegen
    dump_code(
        edb.pgsql.codegen.generate_source(sql, *args, **kwargs), lexer='SQL'
    )


def dump_edgeql(eql, *args, **kwargs):
    import edb.edgeql.codegen
    dump_code(edb.edgeql.codegen.generate_source(eql, *args, **kwargs))


def set_trace(**kwargs):
    """Debugger hook that works inside worker processes.

    Set PYTHONBREAKPOINT=edb.common.debug.set_trace, and this will be triggered
    by `breakpoint()`.

    Unfortunately readline doesn't work when not using stdin itself,
    so try running the server wrapped with `rlwrap.`
    """
    from pdb import Pdb
    new_stdin = open("/dev/tty", "r")
    Pdb(stdin=new_stdin, stdout=sys.stdout).set_trace(
        sys._getframe().f_back, **kwargs)


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
