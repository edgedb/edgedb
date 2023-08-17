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
from typing import *  # NoQA

import pickle

import immutables

from edb import edgeql
from edb import graphql
from edb.pgsql import params as pgparams
from edb.schema import schema as s_schema
from edb.server import compiler
from edb.server import config
from edb.server import defines

from . import state
from . import worker_proc


INITED: bool = False
DBS: state.DatabasesState = immutables.Map()
BACKEND_RUNTIME_PARAMS: pgparams.BackendRuntimeParams = \
    pgparams.get_default_runtime_params()
COMPILER: compiler.Compiler
LAST_STATE: Optional[compiler.dbstate.CompilerConnectionState] = None
STD_SCHEMA: s_schema.FlatSchema
GLOBAL_SCHEMA: s_schema.FlatSchema
INSTANCE_CONFIG: immutables.Map[str, config.SettingValue]


def __init_worker__(
    init_args_pickled: bytes,
) -> None:
    global INITED
    global DBS
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    (
        dbs,
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
        global_schema_pickled,
        system_config,
    ) = pickle.loads(init_args_pickled)

    INITED = True
    DBS = immutables.Map(
        [
            (
                dbname,
                state.DatabaseState(
                    name=dbname,
                    user_schema=(
                        None  # type: ignore
                        if db.user_schema_pickled is None
                        else pickle.loads(db.user_schema_pickled)
                    ),
                    reflection_cache=db.reflection_cache,
                    database_config=db.database_config,
                ),
            )
            for dbname, db in dbs.items()
        ]
    )
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    STD_SCHEMA = std_schema
    GLOBAL_SCHEMA = pickle.loads(global_schema_pickled)
    INSTANCE_CONFIG = system_config

    COMPILER = compiler.new_compiler(
        std_schema,
        refl_schema,
        schema_class_layout,
        backend_runtime_params=BACKEND_RUNTIME_PARAMS,
        config_spec=None,
    )


def __sync__(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
) -> state.DatabaseState:
    global DBS
    global GLOBAL_SCHEMA
    global INSTANCE_CONFIG

    try:
        db = DBS.get(dbname)
        if db is None:
            assert user_schema is not None
            assert reflection_cache is not None
            assert database_config is not None
            user_schema_unpacked = pickle.loads(user_schema)
            reflection_cache_unpacked = pickle.loads(reflection_cache)
            database_config_unpacked = pickle.loads(database_config)
            db = state.DatabaseState(
                dbname,
                user_schema_unpacked,
                reflection_cache_unpacked,
                database_config_unpacked,
            )
            DBS = DBS.set(dbname, db)
        else:
            updates = {}

            if user_schema is not None:
                updates['user_schema'] = pickle.loads(user_schema)
            if reflection_cache is not None:
                updates['reflection_cache'] = pickle.loads(reflection_cache)
            if database_config is not None:
                updates['database_config'] = pickle.loads(database_config)

            if updates:
                db = db._replace(**updates)
                DBS = DBS.set(dbname, db)

        if global_schema is not None:
            GLOBAL_SCHEMA = pickle.loads(global_schema)

        if system_config is not None:
            INSTANCE_CONFIG = pickle.loads(system_config)

    except Exception as ex:
        raise state.FailedStateSync(
            f'failed to sync worker state: {type(ex).__name__}({ex})') from ex

    return db


def compile(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    units, cstate = COMPILER.compile(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )

    global LAST_STATE
    LAST_STATE = cstate
    pickled_state = None
    if cstate is not None:
        pickled_state = pickle.dumps(cstate, -1)

    return units, pickled_state


def compile_in_tx(cstate, *args, **kwargs):
    global LAST_STATE
    if cstate == state.REUSE_LAST_STATE_MARKER:
        cstate = LAST_STATE
    else:
        cstate = pickle.loads(cstate)
    units, cstate = COMPILER.compile_in_tx(cstate, *args, **kwargs)
    LAST_STATE = cstate
    return units, pickle.dumps(cstate, -1)


def compile_notebook(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.compile_notebook(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def compile_graphql(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
) -> tuple[compiler.QueryUnitGroup, graphql.TranspiledOperation]:
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    gql_op = graphql.compile_graphql(
        STD_SCHEMA,
        db.user_schema,
        GLOBAL_SCHEMA,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )

    source = edgeql.Source.from_string(
        edgeql.generate_source(gql_op.edgeql_ast, pretty=True),
    )

    unit_group, _ = COMPILER.compile(
        user_schema=db.user_schema,
        global_schema=GLOBAL_SCHEMA,
        reflection_cache=db.reflection_cache,
        database_config=db.database_config,
        system_config=INSTANCE_CONFIG,
        source=source,
        sess_modaliases=None,
        sess_config=None,
        output_format=compiler.OutputFormat.JSON,
        expect_one=True,
        implicit_limit=0,
        inline_typeids=False,
        inline_typenames=False,
        inline_objectids=False,
        json_parameters=True,
        protocol_version=defines.CURRENT_PROTOCOL,
    )

    return unit_group, gql_op


def compile_sql(
    dbname: str,
    user_schema: Optional[bytes],
    reflection_cache: Optional[bytes],
    global_schema: Optional[bytes],
    database_config: Optional[bytes],
    system_config: Optional[bytes],
    *compile_args: Any,
    **compile_kwargs: Any,
):
    db = __sync__(
        dbname,
        user_schema,
        reflection_cache,
        global_schema,
        database_config,
        system_config,
    )

    return COMPILER.compile_sql(
        db.user_schema,
        GLOBAL_SCHEMA,
        db.reflection_cache,
        db.database_config,
        INSTANCE_CONFIG,
        *compile_args,
        **compile_kwargs
    )


def get_handler(methname):
    if methname == "__init_worker__":
        meth = __init_worker__
    else:
        if not INITED:
            raise RuntimeError(
                "call on uninitialized compiler worker"
            )
        if methname == "compile":
            meth = compile
        elif methname == "compile_in_tx":
            meth = compile_in_tx
        elif methname == "compile_notebook":
            meth = compile_notebook
        elif methname == "compile_graphql":
            meth = compile_graphql
        elif methname == "compile_sql":
            meth = compile_sql
        else:
            meth = getattr(COMPILER, methname)
    return meth


if __name__ == "__main__":
    try:
        worker_proc.main(get_handler)
    except KeyboardInterrupt:
        pass
