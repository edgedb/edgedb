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

from edb.common import ast
from edb.common import context as pctx
from edb.common import debug

from edb.edgeql import ast as qlast

from edb.ir import ast as irast

from edb.schema import constraints as s_constr
from edb.schema import indexes as s_indexes
from edb.schema import objects as so
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema

from edb.pgsql import ast as pgast
from edb.pgsql.compiler import astutils

uuid_core = '[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}'
uuid_re = re.compile(
    rf'(\.?"?({uuid_core})"?)',
    re.I
)

# This matches spaces, minus or an empty string that comes before capital
# letter (and not at the start of the string).
# And is used to replace that word boundary for the underscore.
# It handles cases like this:
# * `Foo Bar` -- title case -- matches space
# * `FooBar` -- CamelCase -- matches empty string before `Bar`
# * `Some-word` -- words with dash -- matches dash
word_boundary_re = re.compile(r'(?<!^)(?<!\s|-)[\s-]*(?=[A-Z])')

# "affects_compilation" config vals that we don't actually want to report out.
# This turns out to be a majority of them
OMITTED_CONFIG_VALS = {
    "allow_dml_in_functions", "allow_bare_ddl", "force_database_error",
}


ContextDesc = dict[str, int]


@dataclasses.dataclass
class AnalysisInfo:
    aliases: dict[str, pgast.BaseRangeVar]
    alias_to_path_id: dict[str, irast.PathId]
    alias_contexts: dict[str, list[list[ContextDesc]]]
    sets: dict[irast.PathId, set[irast.Set]]
    buffers: list[tuple[str, str]]


# Do a bunch of analysis of the queries. Currently we produce more
# info than we actually consume, since we are still in a somewhat
# exploratory phase.
def analyze_queries(
    ql: qlast.Base, ir: irast.Statement, pg: pgast.Base,
    *, schema: s_schema.Schema,
) -> AnalysisInfo:
    debug_spew = debug.flags.edgeql_explain

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

    rvars = ast.find_children(pg, pgast.BaseRangeVar)
    queries = ast.find_children(pg, pgast.Query)

    # Map subqueries back to their rvars
    subq_to_rvar: dict[pgast.Query, pgast.RangeSubselect] = {}
    for rvar in rvars:
        if isinstance(rvar, pgast.RangeSubselect):
            assert rvar.subquery not in subq_to_rvar
            for subq in astutils.each_query_in_set(rvar.subquery):
                subq_to_rvar[subq] = rvar

    # Find all *references* to an rvar in path_rvar_maps
    # Maps rvars to the queries that join them
    reverse_path_rvar_map: dict[
        pgast.BaseRangeVar,
        list[pgast.Query],
    ] = {}
    for qry in queries:
        qrvars = []
        if isinstance(qry, (pgast.SelectStmt, pgast.UpdateStmt)):
            qrvars.extend(qry.from_clause)
        if isinstance(qry, pgast.DeleteStmt):
            qrvars.extend(qry.using_clause)

        for orvar in qrvars:
            for rvar in astutils.each_base_rvar(orvar):
                reverse_path_rvar_map.setdefault(rvar, []).append(qry)

    # Map aliases to rvars and then to path ids
    aliases = {
        rvar.alias.aliasname: rvar for rvar in rvars if rvar.alias.aliasname
    }
    path_ids = {
        alias: rvar.relation.path_id
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

    alias_contexts: dict[str, list[list[ContextDesc]]] = {}

    # Try to produce good contexts
    # KEY FACT: We often duplicate code for with bindings. This means
    # we want to expose that through the contexts we include.
    for alias, rvar in aliases.items():
        # Run up the tree looking both for contexts to associate with
        # and the next node in the tree to go up to
        asets = []
        while True:
            ns = cast(list[irast.Set], rvar.ir_origins or [])
            if len(ns) >= 1 and ns[0].context:
                if ns[0] not in asets:
                    asets.append(ns[0])

            # Find the enclosing
            sources = reverse_path_rvar_map.get(rvar, ())
            if debug_spew:
                print('SOURCES', sources)
            if sources:
                source = sources[0]
                if source not in subq_to_rvar:
                    break
            else:
                break

            rvar = subq_to_rvar[source]

        sctxs = [get_context(x.context) for x in asets if x.context]
        if debug_spew:
            print(alias, asets)
            for x in asets:
                debug.dump(x.context)
        if sctxs:
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
    elif isinstance(sobj, s_constr.Constraint):
        s = sobj.get_verbosename(schema, with_parent=True)
    elif isinstance(sobj, s_indexes.Index):
        s = sobj.get_verbosename(schema, with_parent=True)
        if expr := sobj.get_expr(schema):
            s += f' on ({expr.text})'
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
            to_snake_case(k): json_fixup(v, info, schema, k)
            for k, v in obj.items()
            if k not in ('Schema',)
        }
        alias = obj.get('alias')

        # If the path id has a different type than the real relation,
        # indicate what the original type was (since this is probably
        # the result of expansion.)
        if alias and alias in info.alias_to_path_id:
            path_id = info.alias_to_path_id[alias]
            obj['_debug_path_id'] = str(path_id)
            if (
                (ptr := path_id.rptr())
                and isinstance(ptr.real_material_ptr, irast.PointerRef)
            ):
                ptr_name = _obj_to_name(
                    schema.get_by_id(ptr.real_material_ptr.id), schema)
                obj['pointer_name'] = ptr_name
            if not path_id.is_ptr_path():
                oid = path_id.target.real_material_type.id
                path_name = _obj_to_name(schema.get_by_id(oid), schema)
                obj['original_relation_name'] = path_name

        if alias and alias in info.alias_contexts:
            obj['contexts'] = info.alias_contexts[alias][0]

        if 'actual_total_time' in obj:
            obj['full_total_time'] = (
                obj["actual_total_time"] * obj.get("Actual Loops", 1))
            obj['self_time'] = obj['full_total_time'] - (
                sum([subplan['full_total_time'] for subplan in obj['plans']])
                if 'plans' in obj else 0)
        obj['self_cost'] = obj['total_cost'] - (
            sum([subplan['total_cost'] for subplan in obj['plans']])
            if 'plans' in obj else 0)

        return obj
    elif isinstance(obj, str):
        # Try to replace all ids with textual names
        had_index = False
        for (full, m) in uuid_re.findall(obj):
            uid = uuid.UUID(m)
            sobj = schema.get_by_id(uid, default=None)
            if sobj:
                had_index |= isinstance(sobj, s_indexes.Index)
                dotted = full[0] == '.'
                s = _obj_to_name(sobj, schema, dotted=dotted)
                obj = uuid_re.sub(s, obj, count=1)

        if idx == 'Index Name':
            obj = obj.replace('_source_target_key', ' forward link index')
            obj = obj.replace(';schemaconstr', '')
            obj = obj.replace('_target_key', ' backward link index')
            # If the index name is from an actual index or constraint,
            # the `_index` part of the name just total noise, but if it
            # is from a link, it might be slightly informative
            if had_index:
                obj = obj.replace('_index', '')
            else:
                obj = obj.replace('_index', ' index')
        return obj
    else:
        return obj


# Finds all the direct descendents of the given plan node that have been
# annotated with 'Contexts', and creates a new collapsed tree of those under
# the 'collapsed_plans' key.
# Also for 'Aggregate' type plan nodes, that do not already have 'Contexts',
# try to find the nearest descendent plan node with 'Contexts' and
# attach that as 'NearestContextPlan'.
# The original plan tree remains under the 'plans' key, but plan nodes
# de-duplicated as so:
# - Plan node in 'nearest_context_plan' replaced with 0
# - Plan nodes in 'collapsed_plans' replaced with their 1-based index in the
#   'collapsed_plans' list.
def collapse_plan(
    plan: Any,
    find_nearest_ctx: bool = False
) -> Any:
    subplans = []
    found_nearest = None

    unvisited = [(subplan, plan) for subplan in plan.get('plans', [])]
    while unvisited:
        subplan, parent = unvisited.pop(0)
        if 'contexts' in subplan:
            if find_nearest_ctx and found_nearest is None:
                found_nearest = subplan
                parent['plans'][parent['plans'].index(subplan)] = 0
            else:
                subplans.append(subplan)
                parent['plans'][parent['plans'].index(subplan)] = len(subplans)

            collapse_plan(subplan)
        else:
            if subplan['node_type'] == "Aggregate":
                nearest_plan = collapse_plan(subplan, True)
                if nearest_plan:
                    subplan['nearest_context_plan'] = nearest_plan
                    subplans.append(subplan)
                    parent['plans'][parent['plans'].index(subplan)] = (
                        len(subplans))
            else:
                unvisited += [
                    (subsubplan, subplan) for subsubplan
                    in subplan.get('plans', [])
                ]

    if subplans or found_nearest:
        all_subplans = (
            (list(subplans) if subplans else []) +
            (found_nearest.get('collapsed_plans', []) if found_nearest else [])
        )
        if 'full_total_time' in plan:
            plan['collapsed_self_time'] = plan['full_total_time'] - (
                sum([subplan['full_total_time'] for subplan in all_subplans])
            )
        plan['collapsed_self_cost'] = plan['total_cost'] - (
            sum([subplan['total_cost'] for subplan in all_subplans])
        )

    if subplans:
        plan['collapsed_plans'] = subplans

        # For each plan with contexts, try to pick the widest context that the
        # plan node does not share with sibling or parent nodes, to suggest
        # for display in UI
        parent_ctxs = (found_nearest['contexts'] if
                       found_nearest else plan.get('contexts'))
        ctx_subplans = [
            subplan.get('nearest_context_plan') or subplan
            for subplan in subplans
        ]
        for subplan in ctx_subplans:
            plan_ctxs = subplan['contexts']
            sibling_ctxs = list(parent_ctxs) if parent_ctxs else []
            for sib_plan in ctx_subplans:
                if sib_plan != subplan:
                    sibling_ctxs += sib_plan['contexts']
            if sibling_ctxs:
                for ctx in reversed(plan_ctxs):
                    if not ctx_in_ctxs(ctx, sibling_ctxs):
                        subplan['suggested_display_ctx_idx'] = (
                            plan_ctxs.index(ctx))
                        break
            else:
                subplan['suggested_display_ctx_idx'] = len(plan_ctxs) - 1

    return found_nearest


def ctx_in_ctxs(ctx: Any, ctxs: Any) -> bool:
    for c in ctxs:
        if (c['buffer_idx'] == ctx['buffer_idx']
                and c['start'] == ctx['start']
                and c['end'] == ctx['end']):
            return True
    return False


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
    ql, ir, pg, config_vals = pickle.loads(query_asts_pickled)
    schema = ir.schema
    # We omit the std schema when serializing, so put it back
    if isinstance(schema, s_schema.ChainedSchema):
        schema = s_schema.ChainedSchema(
            top_schema=schema._top_schema,
            global_schema=schema._global_schema,
            base_schema=std_schema
        )

    assert len(data) == 1 and len(data[0]) == 1
    plan = json.loads(data[0][0])
    assert len(plan) == 1
    plan = plan[0]['Plan']

    info = analyze_queries(ql, ir, pg, schema=schema)
    plan = json_fixup(plan, info, schema)

    collapse_plan(plan)

    config_vals = {
        k: v for k, v in config_vals.items() if k not in OMITTED_CONFIG_VALS
    }
    globals_used = sorted([str(k) for k in ir.globals])

    output = {
        'buffers': info.buffers,
        'config_vals': config_vals,
        'globals_used': globals_used,
        'plan': plan,
    }
    if debug.flags.edgeql_explain:
        debug.dump(output)

    return json.dumps([output]).encode('utf-8')


def to_snake_case(name: str) -> str:
    # note this only covers cases we have not all possible cases of
    # case conversion
    return word_boundary_re.sub('_', name).lower()
