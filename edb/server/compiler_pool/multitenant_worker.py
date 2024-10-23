#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
from typing import Any, Optional, NamedTuple

import pickle

import immutables

from edb import edgeql
from edb import graphql

from edb.common import debug
from edb.common import uuidgen
from edb.pgsql import params as pgparams
from edb.schema import schema as s_schema
from edb.server import compiler
from edb.server import config
from edb.server import defines

from . import state
from . import worker_proc


INITED: bool = False
clients: immutables.Map[int, ClientSchema] = immutables.Map()
BACKEND_RUNTIME_PARAMS: pgparams.BackendRuntimeParams = (
    pgparams.get_default_runtime_params()
)
COMPILER: compiler.Compiler
LAST_STATE: Optional[compiler.dbstate.CompilerConnectionState] = None
STD_SCHEMA: s_schema.FlatSchema


class ClientSchema(NamedTuple):
    dbs: state.DatabasesState
    global_schema: s_schema.FlatSchema
    instance_config: immutables.Map[str, config.SettingValue]


def __init_worker__(
    init_args_pickled: bytes,
) -> None:
    global INITED
    global BACKEND_RUNTIME_PARAMS
    global COMPILER
    global STD_SCHEMA

    (
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
    ) = pickle.loads(init_args_pickled)

    INITED = True
    BACKEND_RUNTIME_PARAMS = backend_runtime_params
    STD_SCHEMA = std_schema

    COMPILER = compiler.new_compiler(
        std_schema,
        refl_schema,
        schema_class_layout,
        backend_runtime_params=backend_runtime_params,
        config_spec=None,
    )


def __sync__(client_id, pickled_schema, invalidation) -> None:
    global clients

    for cid in invalidation:
        try:
            clients = clients.delete(cid)
        except KeyError:
            pass
    try:
        client_schema: ClientSchema = clients.get(client_id)  # type: ignore
        if pickled_schema:
            if client_schema is None:
                dbs = {
                    dbname: state.DatabaseState(
                        dbname,
                        (
                            None
                            if pickled_state.user_schema is None
                            else pickle.loads(pickled_state.user_schema)
                        ),
                        pickle.loads(pickled_state.reflection_cache),
                        pickle.loads(pickled_state.database_config),
                    )
                    for dbname, pickled_state in pickled_schema.dbs.items()
                }
                if debug.flags.server:
                    print(client_id, "FULL SYNC: ", list(dbs))
                client_schema = ClientSchema(
                    immutables.Map(dbs),
                    pickle.loads(pickled_schema.global_schema),
                    pickle.loads(pickled_schema.instance_config),
                )
                clients = clients.set(client_id, client_schema)
            else:
                updates = {}
                dbs = client_schema.dbs
                if pickled_schema.dbs is not None:
                    for dbname, pickled_state in pickled_schema.dbs.items():
                        db_state = dbs.get(dbname)
                        if db_state is None:
                            assert pickled_state.user_schema is not None
                            assert pickled_state.reflection_cache is not None
                            assert pickled_state.database_config is not None
                            db_state = state.DatabaseState(
                                dbname,
                                pickle.loads(pickled_state.user_schema),
                                pickle.loads(pickled_state.reflection_cache),
                                pickle.loads(pickled_state.database_config),
                            )
                            if debug.flags.server:
                                print(client_id, "DIFF SYNC ADD: ", dbname)
                            dbs = dbs.set(dbname, db_state)
                        else:
                            db_updates = {}
                            if pickled_state.user_schema is not None:
                                db_updates["user_schema"] = pickle.loads(
                                    pickled_state.user_schema
                                )
                            if pickled_state.reflection_cache is not None:
                                db_updates["reflection_cache"] = pickle.loads(
                                    pickled_state.reflection_cache
                                )
                            if pickled_state.database_config is not None:
                                db_updates["database_config"] = pickle.loads(
                                    pickled_state.database_config
                                )
                            if db_updates:
                                if debug.flags.server:
                                    print(
                                        client_id, "DIFF SYNC UPDATE: ", dbname
                                    )
                                val = dbs.get(dbname)
                                dbs = dbs.set(
                                    dbname,
                                    val._replace(**db_updates),  # type: ignore
                                )
                if pickled_schema.dropped_dbs is not None:
                    for dbname in pickled_schema.dropped_dbs:
                        if debug.flags.server:
                            print(client_id, "DIFF SYNC DROP: ", dbname)
                        dbs = dbs.delete(dbname)
                if dbs is not client_schema.dbs:
                    updates["dbs"] = dbs
                if pickled_schema.global_schema is not None:
                    updates["global_schema"] = pickle.loads(
                        pickled_schema.global_schema
                    )
                if pickled_schema.instance_config is not None:
                    updates["instance_config"] = pickle.loads(
                        pickled_schema.instance_config
                    )
                if updates:
                    client_schema = client_schema._replace(
                        **updates  # type: ignore
                    )
                    clients = clients.set(client_id, client_schema)
        else:
            assert client_schema is not None

    except Exception as ex:
        raise state.FailedStateSync(
            f"failed to sync worker state: {type(ex).__name__}({ex})"
        ) from ex


def compile(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]
    units, cstate = COMPILER.compile_serialized_request(
        db.user_schema,
        client_schema.global_schema,
        db.reflection_cache,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs,
    )

    pickled_state = None
    if cstate is not None:
        global LAST_STATE
        LAST_STATE = cstate
        pickled_state = pickle.dumps(cstate, -1)

    return units, pickled_state


def compile_in_tx(
    _,
    client_id: Optional[int],
    dbname: Optional[str],
    user_schema: Optional[bytes],
    cstate,
    *args,
    **kwargs,
):
    global LAST_STATE
    if cstate == state.REUSE_LAST_STATE_MARKER:
        assert LAST_STATE is not None
        cstate = LAST_STATE
    else:
        cstate = pickle.loads(cstate)
        if client_id is None:
            assert user_schema is not None
            cstate.set_root_user_schema(pickle.loads(user_schema))
        else:
            assert dbname is not None
            client_schema = clients[client_id]
            db = client_schema.dbs[dbname]
            cstate.set_root_user_schema(db.user_schema)
    units, cstate = COMPILER.compile_serialized_request_in_tx(
        cstate, *args, **kwargs)
    LAST_STATE = cstate
    return units, pickle.dumps(cstate, -1)


def compile_notebook(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    global clients
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]

    return COMPILER.compile_notebook(
        db.user_schema,
        client_schema.global_schema,
        db.reflection_cache,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs,
    )


def compile_graphql(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    global clients
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]

    gql_op = graphql.compile_graphql(
        STD_SCHEMA,
        db.user_schema,
        client_schema.global_schema,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs
    )

    source = edgeql.Source.from_string(
        edgeql.generate_source(gql_op.edgeql_ast, pretty=True),
    )

    cfg_ser = COMPILER.state.compilation_config_serializer
    request = compiler.CompilationRequest(
        source=source,
        protocol_version=defines.CURRENT_PROTOCOL,
        schema_version=uuidgen.uuid4(),
        compilation_config_serializer=cfg_ser,
        output_format=compiler.OutputFormat.JSON,
        input_format=compiler.InputFormat.JSON,
        expect_one=True,
        implicit_limit=0,
        inline_typeids=False,
        inline_typenames=False,
        inline_objectids=False,
        modaliases=None,
        session_config=None,
    )

    unit_group, _ = COMPILER.compile(
        user_schema=db.user_schema,
        global_schema=client_schema.global_schema,
        reflection_cache=db.reflection_cache,
        database_config=db.database_config,
        system_config=client_schema.instance_config,
        request=request,
    )

    return unit_group, gql_op


def compile_sql(
    client_id: int,
    dbname: str,
    *compile_args: Any,
    **compile_kwargs: Any,
):
    client_schema = clients[client_id]
    db = client_schema.dbs[dbname]
    return COMPILER.compile_sql(
        db.user_schema,
        client_schema.global_schema,
        db.reflection_cache,
        db.database_config,
        client_schema.instance_config,
        *compile_args,
        **compile_kwargs,
    )


def call_for_client(client_id, pickled_schema, invalidation, msg, *args):
    __sync__(client_id, pickled_schema, invalidation)
    if msg is None:
        methname = args[0]
        dbname = args[1]
        args = args[2:]
    else:
        assert args == ()
        methname, args = pickle.loads(msg)
        dbname = args[0]
        args = args[6:]

    if methname == "compile":
        meth = compile
    elif methname == "compile_notebook":
        meth = compile_notebook
    elif methname == "compile_graphql":
        meth = compile_graphql
    elif methname == "compile_sql":
        meth = compile_sql
    else:
        meth = getattr(COMPILER, methname)
    return meth(client_id, dbname, *args)


def get_handler(methname):
    if methname == "__init_worker__":
        meth = __init_worker__
    else:
        if not INITED:
            raise RuntimeError(
                "call on uninitialized compiler worker"
            )
        if methname == "call_for_client":
            meth = call_for_client
        elif methname == "compile_in_tx":
            meth = compile_in_tx
        else:
            meth = getattr(COMPILER, methname)
    return meth


if __name__ == "__main__":
    try:
        worker_proc.main(get_handler)
    except KeyboardInterrupt:
        pass
