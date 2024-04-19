#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations


import uuid

from edb.common import debug
from edb.common import uuidgen

from edb.schema import functions as s_funcs
from edb.schema import objects as so
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema

from edb.pgsql import ast as pgast
from edb.pgsql import codegen as pgcodegen
from edb.ir import ast as irast


def dump_ast_and_query(
    pg_expr: pgast.Base,
    ir_expr: irast.Base,
) -> None:
    if not (
        debug.flags.edgeql_compile
        or debug.flags.edgeql_compile_sql_ast
        or debug.flags.edgeql_compile_sql_reordered_text
        or debug.flags.edgeql_compile_sql_text
    ):
        return

    if debug.flags.edgeql_compile or debug.flags.edgeql_compile_sql_ast:
        debug.header('SQL Tree')
        debug.dump(
            pg_expr, _ast_include_meta=debug.flags.edgeql_compile_sql_ast_meta
        )

    if debug.flags.edgeql_compile or debug.flags.edgeql_compile_sql_text:
        sql_text = pgcodegen.generate_source(pg_expr, pretty=True)
        debug.header('SQL')
        debug.dump_code(sql_text, lexer='sql')

    if debug.flags.edgeql_compile_sql_reordered_text:
        debug.header('Reordered SQL')
        debug_sql_text = pgcodegen.generate_source(
            pg_expr, pretty=True, reordered=True
        )
        if isinstance(ir_expr, irast.Statement):
            debug_sql_text = _rewrite_names_in_sql(
                debug_sql_text, ir_expr.schema
            )
        debug.dump_code(debug_sql_text, lexer='sql')


def _rewrite_names_in_sql(text: str, schema: s_schema.Schema) -> str:
    """Rewrite the SQL output of the compiler to include real object names.

    Replace UUIDs with object names when possible. The output of this
    won't be valid, but will probably be easier to read.
    This is done by default when pretty printing our "reordered" output,
    which isn't anything like valid SQL anyway.
    """
    # Functions are actually named after their `backend_name` rather
    # than their id, so that overloaded functions all have the same
    # name. Build a map from `backend_name` to real names. (This dict
    # comprehension might have collisions, but that's fine; the names
    # we get out will be the same no matter which is picked.)
    func_map = {
        f.get_backend_name(schema): f
        for f in schema.get_objects(type=s_funcs.Function)
    }

    # Find all the uuids and try to rewrite them.
    for m in set(uuidgen.UUID_RE.findall(text)):
        uid = uuid.UUID(m)
        sobj = schema.get_by_id(uid, default=None)
        if not sobj:
            sobj = func_map.get(uid)
        if sobj:
            s = _obj_to_name(sobj, schema)
            text = text.replace(m, s)

    return text


def _obj_to_name(
    sobj: so.Object,
    schema: s_schema.Schema,
) -> str:
    if isinstance(sobj, s_pointers.Pointer):
        s = str(sobj.get_shortname(schema).name)
        if sobj.is_link_property(schema):
            s = f'@{s}'
        # If the pointer is multi, then it is probably a table name,
        # so let's give a fully qualified version with the source.
        if sobj.get_cardinality(schema).is_multi() and (
            src := sobj.get_source(schema)
        ):
            src_name = src.get_name(schema)
            s = f'{src_name}.{s}'
    elif isinstance(sobj, s_funcs.Function):
        return str(sobj.get_shortname(schema))
    else:
        s = str(sobj.get_name(schema))

    return s
