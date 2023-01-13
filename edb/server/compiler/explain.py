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


from __future__ import annotations
from typing import *

import dataclasses
import json
import re
import pickle
import uuid
import struct

from edb.common import ast
from edb.common import context as pctx
from edb.common import debug

from edb.edgeql import ast as qlast

from edb.ir import ast as irast
from edb.ir import utils as irutils

from edb.schema import constraints as s_constr
from edb.schema import indexes as s_indexes
from edb.schema import objects as so
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import pathctx
from edb.pgsql.compiler import astutils

uuid_core = '[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}'
uuid_re = re.compile(
    rf'(\.?"?({uuid_core})"?)',
    re.I
)


ContextDesc = dict[str, int]


@dataclasses.dataclass
class AnalysisInfo:
    aliases: dict[str, pgast.PathRangeVar]
    alias_to_path_id: dict[str, tuple[irast.PathId, Optional[int]]]
    alias_contexts: dict[str, list[list[ContextDesc]]]
    sets: dict[irast.PathId, set[irast.Set]]
    buffers: list[tuple[str, str]]


# Do a bunch of analysis of the queries. Currently we produce more
# info than we actually consume, since we are still in a somewhat
# exploratory phase.
def analyze_queries(
    ql: qlast.Base, ir: irast.Statement, pg: pgast.Base,
    *, schema: s_schema.Schema,
    debug_spew: bool=False,
) -> AnalysisInfo:
    assert ql.context
    contexts = {(ql.context.buffer, ql.context.name): 0}

    def get_context(context: pctx.ParserContext) -> ContextDesc:
        key = context.buffer, context.name
        if (idx := contexts.get(key)) is None:
            idx = len(contexts)
            contexts[key] = idx
        text = context.buffer[context.start:context.end]
        return dict(
            start=context.start, end=context.end, buffer_idx=idx, text=text
        )

    rvars = ast.find_children(pg, pgast.PathRangeVar)
    queries = ast.find_children(pg, pgast.Query)

    # Map subqueries back to their rvars
    subq_to_rvar: dict[pgast.Query, pgast.RangeSubselect] = {}
    for rvar in rvars:
        if isinstance(rvar, pgast.RangeSubselect):
            assert rvar.subquery not in subq_to_rvar
            for subq in astutils.each_query_in_set(rvar.subquery):
                subq_to_rvar[subq] = rvar

    # Find all *references* to an rvar in path_rvar_maps
    reverse_path_rvar_map: dict[
        pgast.PathRangeVar,
        list[tuple[tuple[irast.PathId, str], pgast.Query]]
    ] = {}
    for qry in queries:
        for key, rvar in qry.path_rvar_map.items():
            reverse_path_rvar_map.setdefault(rvar, []).append((key, qry))

    # Map aliases to rvars and then to path ids
    aliases = {
        rvar.alias.aliasname: rvar for rvar in rvars if rvar.alias.aliasname
    }
    path_ids = {
        alias: (rvar.relation.path_id, rvar.relation.path_scope_id)
        for alias, rvar in aliases.items()
        if isinstance(rvar, pgast.RelRangeVar)
        and isinstance(rvar.relation, pgast.BaseRelation)
        and rvar.relation.path_id
    }

    # Find all the sets
    sets: dict[irast.PathId, set[irast.Set]] = {}
    for s in ast.find_children(ir, irast.Set, extra_skips={'target'}):
        if s.context:
            sets.setdefault(s.path_id, set()).add(s)

    scopes = irutils.find_path_scopes(ir)

    alias_contexts: dict[str, list[list[ContextDesc]]] = {}

    # Try to produce good contexts
    # KEY FACT: We often duplicate code for with bindings. This means
    # we want to expose that through the contexts we include.
    for alias, (path_id, scope_id) in path_ids.items():
        # print("!!!", alias, path_id, scope_id)
        if scope_id is None:  # ???
            continue
        # Strip the ptr path part off if it exists (which it will on
        # links), since that won't appear in the sets
        path_id = path_id.tgt_path()
        # print("???", sets.get(path_id, ()))
        for s in sets.get(path_id, ()):
            if scopes.get(s) == scope_id and s.context:
                asets = [s]

                # Loop back through...
                cpath = path_id
                rvar = aliases[alias]
                while True:
                    sources = [
                        s for k, s in
                        reverse_path_rvar_map.get(rvar, ())
                        if k == (cpath, 'source')
                    ]
                    if debug_spew:
                        print('SOURCES', sources, cpath)
                    if sources:
                        source = sources[0]
                        cpath = pathctx.reverse_map_path_id(
                            cpath, source.view_path_id_map)

                        ns = tuple(sets.get(cpath, ()))
                        if len(ns) > 1:
                            ns = tuple(
                                n for n in ns
                                if scopes.get(n) == source.path_scope_id)
                        if len(ns) == 1 and ns[0].context:
                            if ns[0] not in asets:
                                asets.append(ns[0])

                        if source not in subq_to_rvar:
                            break
                        rvar = subq_to_rvar[source]
                    else:
                        break

                sctxs = [get_context(x.context) for x in asets if x.context]
                if debug_spew:
                    # print(alias, sctxs)
                    # print(asets)
                    for x in asets:
                        debug.dump(x.context)
                alias_contexts.setdefault(alias, []).append(sctxs)

    return AnalysisInfo(
        aliases=aliases,
        alias_to_path_id=path_ids,
        alias_contexts=alias_contexts,
        sets=sets,
        buffers=list(contexts.keys()),
    )


def _obj_to_name(
        sobj: so.Object, schema: s_schema.Schema, dotted: bool=False) -> str:
    if isinstance(sobj, s_pointers.Pointer):
        # If a pointer is on the RHS of a dot, just use
        # the short name. But otherwise, grab the source
        # and link it up
        s = str(sobj.get_shortname(schema).name)
        if sobj.is_link_property(schema):
            s = f'@{s}'
        if not dotted and (src := sobj.get_source(schema)):
            src_name = src.get_name(schema)
            s = f'{src_name}.{s}'
    elif isinstance(sobj, (s_constr.Constraint, s_indexes.Index)):
        # XXX: Do we really want verbose names here, they are
        # kind of awful.
        s = sobj.get_verbosename(schema, with_parent=True)
    else:
        s = str(sobj.get_name(schema))

    if dotted:
        s = '.' + s

    return s


# Except for injecting contexts based on aliases, this is still mostly pretty
# basic schema driven string replacement stuff on the pg plan...
# We need to think about whether we can do better and how we can
# represent it.
def json_fixup(
    obj: Any, info: AnalysisInfo,
    schema: s_schema.Schema, idx: int | str | None = None
) -> Any:
    if isinstance(obj, list):
        return [json_fixup(x, info, schema) for x in obj]
    elif isinstance(obj, dict):
        obj = {
            k: json_fixup(v, info, schema, k) for k, v in obj.items()
            if k not in ('Schema',)
        }
        alias = obj.get('Alias')

        # If the path id has a different type than the real relation,
        # indicate what the original type was (since this is probably
        # the result of expansion.)
        if alias and alias in info.alias_to_path_id:
            path_id, _ = info.alias_to_path_id[alias]
            obj['DEBUG PATH ID'] = str(path_id)
            if ptr := path_id.rptr():
                assert isinstance(ptr.real_material_ptr, irast.PointerRef)
                ptr_name = _obj_to_name(
                    schema.get_by_id(ptr.real_material_ptr.id), schema)
                obj['Pointer Name'] = ptr_name
            if not path_id.is_ptr_path():
                oid = path_id.target.real_material_type.id
                path_name = _obj_to_name(schema.get_by_id(oid), schema)
                obj['Original Relation Name'] = path_name

        if alias and alias in info.alias_contexts:
            obj['Contexts'] = info.alias_contexts[alias]

        return obj
    elif isinstance(obj, str):
        if idx == 'Index Name':
            obj = obj.replace('_source_target_key', ' forward link index')
            obj = obj.replace(';schemaconstr', ' exclusive constraint index')
            obj = obj.replace('_target_key', ' backward link index')
            obj = obj.replace('_index', ' index')

        # Try to replace all ids with textual names
        for (full, m) in uuid_re.findall(obj):
            uid = uuid.UUID(m)
            sobj = schema.get_by_id(uid, default=None)
            if sobj:
                dotted = full[0] == '.'
                s = _obj_to_name(sobj, schema, dotted=dotted)
                obj = uuid_re.sub(s, obj, count=1)

        return obj
    else:
        return obj


def analyze_explain_output(
    query_asts_pickled: bytes,
    data: list[list[bytes]],
    std_schema: s_schema.FlatSchema,
) -> bytes:
    if debug.flags.edgeql_explain:
        debug.header('Explain')

    ql: qlast.Base
    ir: irast.Statement
    pg: pgast.Base
    ql, ir, pg = pickle.loads(query_asts_pickled)
    schema = ir.schema
    # We omit the std schema when serializing, so put it back
    if isinstance(schema, s_schema.ChainedSchema):
        schema = s_schema.ChainedSchema(
            top_schema=schema._top_schema,
            global_schema=schema._global_schema,
            base_schema=std_schema
        )

    assert len(data) == 1 and len(data[0]) == 1
    # print('DATA', data)
    plan = json.loads(data[0][0])
    assert len(plan) == 1
    plan = plan[0]['Plan']

    info = analyze_queries(ql, ir, pg, schema=schema)
    plan = json_fixup(plan, info, schema)

    output = {
        'Buffers': info.buffers,
        'Plan': plan,
    }
    if debug.flags.edgeql_explain:
        debug.dump(output)

    # Repeat the analysis if we are doing debug dumping for the silly reason
    # of having it appear last in the debug spew.
    if debug.flags.edgeql_explain:
        analyze_queries(ql, ir, pg, schema=schema, debug_spew=True)
        # debug.dump(info, _ast_include_meta=False)

    return make_message([output])


def make_message(obj: Any) -> bytes:
    omsg = json.dumps(obj).encode('utf-8')
    msg = struct.pack(
        "!hic",
        1,
        len(omsg) + 1,
        # XXX: why isn't it b'\x01'??
        b' ',
    ) + omsg
    return msg
