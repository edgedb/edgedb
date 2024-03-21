#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Optional

import dataclasses
import json
import logging
import pickle

import immutables

from edb import buildmeta
from edb.common import debug
from edb.edgeql import ast as qlast
from edb.ir import ast as irast
from edb.pgsql import ast as pgast
from edb.schema import schema as s_schema

from . import coarse_grained
from . import fine_grained
from . import ir_analyze
from . import pg_tree
from . import to_json


log = logging.getLogger(__name__)


# "affects_compilation" config vals that we don't actually want to report out.
# This turns out to be a majority of them
OMITTED_CONFIG_VALS = {
    "allow_dml_in_functions", "allow_bare_ddl", "force_database_error",
}


@dataclasses.dataclass
class Arguments(to_json.ToJson):
    execute: bool
    buffers: bool


@dataclasses.dataclass(frozen=True)
class AnalyzeContext:
    schema: s_schema.Schema
    modaliases: immutables.Map[Optional[str], str]
    reverse_mod_aliases: dict[str, Optional[str]]


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
    ql, ir, pg, explain_data = pickle.loads(query_asts_pickled)
    config_vals, args, modaliases = explain_data
    args = Arguments(**args)

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
    plan = debug_tree = plan[0]['Plan']

    info = None
    fg_tree = None
    cg_tree = None
    try:
        ctx = AnalyzeContext(
            schema=schema,
            modaliases=modaliases,
            # This has last alias wins strategy. Do we need reverse?
            reverse_mod_aliases={v: k for k, v in modaliases.items()},
        )
        info = ir_analyze.analyze_queries(ql, ir, pg, ctx)
        debug_tree = pg_tree.Plan.from_json(plan, ctx)
        fg_tree, index = fine_grained.build(debug_tree, info, args)
        if debug.flags.edgeql_explain:
            debug.dump(fg_tree)
            debug.dump(info)
        cg_tree = coarse_grained.build(fg_tree, info, index)
    except Exception as e:
        log.exception("Error building explain model", exc_info=e)

    config_vals = {
        k: v for k, v in config_vals.items() if k not in OMITTED_CONFIG_VALS
    }
    globals_used = sorted([str(k) for k in ir.globals])

    if info:
        buffers = info.buffers
    elif ql.span:
        buffers = [ql.span.buffer]
    else:
        buffers = []  # should never happen

    output = {
        'config_vals': config_vals,
        'globals_used': globals_used,
        'module_aliases': dict(modaliases),
        'arguments': args,
        'version': buildmeta.get_version_string(),
        'buffers': buffers,
        'debug_info': {
            'full_plan': debug_tree,
            'analysis_info': info,
        },
        'fine_grained': fg_tree,
        'coarse_grained': cg_tree,
    }

    return json.dumps(output, default=to_json.json_hook).encode('utf-8')
