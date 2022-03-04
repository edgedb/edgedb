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

import asyncio
import collections
import functools
import hashlib
import logging
import os
import pickle
import time
import traceback
import typing

import click
import immutables

from edb.common import debug
from edb.common import markup

from .. import args as srvargs
from . import amsg
from . import pool as pool_mod
from . import worker as worker_mod
from . import state as state_mod


_client_id_seq = 0
logger = logging.getLogger("edb.server")


class PickledState(typing.NamedTuple):
    user_schema: typing.Optional[bytes]
    reflection_cache: typing.Optional[bytes]
    database_config: typing.Optional[bytes]

    def diff(self, other: PickledState):
        return PickledState(
            None
            if self.user_schema is other.user_schema
            else self.user_schema,
            None
            if self.reflection_cache is other.reflection_cache
            else self.reflection_cache,
            None
            if self.database_config is other.database_config
            else self.database_config,
        )


class ClientSchema(typing.NamedTuple):
    dbs: immutables.Map[str, PickledState]
    global_schema: typing.Optional[bytes]
    instance_config: typing.Optional[bytes]
    dropped_dbs: tuple

    def diff(self, other: ClientSchema):
        dropped_dbs = tuple(
            dbname for dbname in other.dbs if dbname not in self.dbs
        )
        dbs: immutables.Map[str, PickledState] = immutables.Map()
        for dbname, state in self.dbs.items():
            other_state = other.dbs.get(dbname)
            if other_state is None:
                dbs = dbs.set(dbname, state)
            elif state is not other_state:
                dbs = dbs.set(dbname, state.diff(other_state))
        return ClientSchema(
            dbs,
            None
            if self.global_schema is other.global_schema
            else self.global_schema,
            None
            if self.instance_config is other.instance_config
            else self.instance_config,
            dropped_dbs,
        )


class Worker(pool_mod.Worker):
    def __init__(
        self,
        manager,
        server,
        pid,
        backend_runtime_params,
        std_schema,
        refl_schema,
        schema_class_layout,
    ):
        super().__init__(
            manager,
            server,
            pid,
            None,
            backend_runtime_params,
            std_schema,
            refl_schema,
            schema_class_layout,
            None,
            None,
        )
        self._cache = collections.OrderedDict()
        self._invalidated_clients = []
        self._last_used_by_client = {}

    def get_client_schema(self, client_id):
        return self._cache.get(client_id)

    def set_client_schema(self, client_id, client_schema):
        self._cache[client_id] = client_schema
        self._cache.move_to_end(client_id, last=False)
        self._last_used_by_client[client_id] = time.monotonic()

    def cache_size(self):
        return len(self._cache) - len(self._invalidated_clients)

    def last_used(self, client_id):
        return self._last_used_by_client.get(client_id, 0)

    def invalidate(self, client_id):
        if self._cache.pop(client_id, None):
            self._invalidated_clients.append(client_id)
        self._last_used_by_client.pop(client_id, None)

    def invalidate_last(self, cache_size):
        if len(self._cache) == cache_size:
            client_id, _ = self._cache.popitem(last=True)
            self._invalidated_clients.append(client_id)
            self._last_used_by_client.pop(client_id, None)

    def flush_invalidation(self):
        rv, self._invalidated_clients = self._invalidated_clients, []
        for client_id in rv:
            self._cache.pop(client_id, None)
        return rv

    async def call(
        self,
        method_name,
        *args,
        sync_state=None,
        msg=None,
    ):
        assert not self._closed

        if self._con.is_closed():
            raise RuntimeError(
                "the connection to the compiler worker process is "
                "unexpectedly closed"
            )

        if msg is None:
            msg = pickle.dumps((method_name, args))
        return await self._con.request(msg)


class MultiSchemaPool(pool_mod.FixedPool):
    _worker_class = Worker  # type: ignore
    _worker_mod = "remote_worker"
    _workers: typing.Dict[int, Worker]  # type: ignore
    _clients: typing.Dict[int, ClientSchema]

    def __init__(self, cache_size, **kwargs):
        super().__init__(
            dbindex=None,
            backend_runtime_params=None,
            std_schema=None,
            refl_schema=None,
            schema_class_layout=None,
            **kwargs,
        )
        self._inited = asyncio.Event()
        self._cache_size = cache_size
        self._clients = {}

    @functools.lru_cache(maxsize=None)
    def _get_init_args(self):
        init_args = (
            self._backend_runtime_params,
            self._std_schema,
            self._refl_schema,
            self._schema_class_layout,
        )
        pickled_args = pickle.dumps(init_args, -1)
        return init_args, pickled_args

    async def _attach_worker(self, pid: int):
        if not self._running:
            return
        if not self._inited.is_set():
            await self._inited.wait()
        return await super()._attach_worker(pid)

    async def _wait_ready(self):
        pass

    async def _init_server(self, client_id: int, init_args_pickled: bytes):
        (
            dbs,
            backend_runtime_params,
            std_schema,
            refl_schema,
            schema_class_layout,
            global_schema,
            system_config,
        ) = pickle.loads(init_args_pickled)

        if self._inited.is_set():
            logger.debug("New client %d connected.", client_id)
            if self._backend_runtime_params != backend_runtime_params:
                raise RuntimeError(
                    "Incompatible client: backend_runtime_params"
                )
            # if self._std_schema != std_schema:
            #     raise RuntimeError("Incompatible client: std_schema")
            # if self._refl_schema != refl_schema:
            #     raise RuntimeError("Incompatible client: refl_schema")
            if self._schema_class_layout != schema_class_layout:
                raise RuntimeError("Incompatible client: schema_class_layout")
        else:
            self._backend_runtime_params = backend_runtime_params
            self._std_schema = std_schema
            self._refl_schema = refl_schema
            self._schema_class_layout = schema_class_layout
            self._inited.set()
            logger.info(
                "New client %d connected, compiler server initialized.",
                client_id,
            )
        self._clients[client_id] = ClientSchema(
            immutables.Map(
                (
                    dbname,
                    PickledState(
                        pickle.dumps(state.user_schema, -1),
                        pickle.dumps(state.reflection_cache, -1),
                        pickle.dumps(state.database_config, -1),
                    ),
                )
                for dbname, state in dbs.items()
            ),
            pickle.dumps(global_schema, -1),
            pickle.dumps(system_config, -1),
            (),
        )

    def _sync(
        self,
        client_id: int,
        dbname: str,
        user_schema: typing.Optional[bytes],
        reflection_cache: typing.Optional[bytes],
        global_schema: typing.Optional[bytes],
        database_config: typing.Optional[bytes],
        system_config: typing.Optional[bytes],
    ):
        # EdgeDB instance syncs the schema with the compiler server
        client = self._clients[client_id]
        client_updates: typing.Dict[str, typing.Any] = {}
        db = client.dbs.get(dbname)
        if db is None:
            assert user_schema is not None
            assert reflection_cache is not None
            assert database_config is not None
            client_updates["dbs"] = client.dbs.set(
                dbname,
                PickledState(user_schema, reflection_cache, database_config),
            )
        else:
            updates = {}

            if user_schema is not None:
                updates["user_schema"] = user_schema
            if reflection_cache is not None:
                updates["reflection_cache"] = reflection_cache
            if database_config is not None:
                updates["database_config"] = database_config

            if updates:
                db = db._replace(**updates)
                client_updates["dbs"] = client.dbs.set(dbname, db)

        if global_schema is not None:
            client_updates["global_schema"] = global_schema

        if system_config is not None:
            client_updates["instance_config"] = system_config

        if client_updates:
            self._clients[client_id] = client._replace(**client_updates)
            return True
        else:
            return False

    def _weighter(self, client_id, worker: Worker):
        client_schema = worker.get_client_schema(client_id)
        return (
            bool(client_schema),
            worker.last_used(client_id)
            if client_schema
            else self._cache_size - worker.cache_size(),
        )

    async def _call_for_client(
        self,
        client_id,
        method_name,
        args,
        msg,
    ):
        try:
            updated = self._sync(client_id, *args[:6])
        except Exception as ex:
            raise state_mod.FailedStateSync(
                f"failed to sync compiler server state: "
                f"{type(ex).__name__}({ex})"
            ) from ex
        worker = await self._acquire_worker(
            weighter=functools.partial(self._weighter, client_id)
        )
        try:
            diff = client_schema = self._clients[client_id]
            cache = worker.get_client_schema(client_id)
            extra_args = ()
            if cache is client_schema:
                # client schema is already in sync, don't send again
                diff = None
            else:
                if cache is None:
                    # make room for the new client in this worker
                    worker.invalidate_last(self._cache_size)
                else:
                    # only send the difference in user schema
                    diff = client_schema.diff(cache)
                if updated:
                    # re-pickle the request if user schema changed
                    msg = None
                    extra_args = (method_name, args[0], *args[6:])
            if msg:
                msg = bytes(msg)
            invalidation = worker.flush_invalidation()
            resp = await worker.call(
                "call_for_client",
                client_id,
                diff,
                invalidation,
                msg,
                *extra_args,
            )
            status, *data = pickle.loads(resp)
            if status == 0:
                worker.set_client_schema(client_id, client_schema)
                if method_name == "compile":
                    if data[0][1]:
                        worker._last_pickled_state = hashlib.sha1(
                            data[0][1]
                        ).hexdigest()
            elif status == 1:
                exc, tb = data
                if not isinstance(exc, state_mod.FailedStateSync):
                    worker.set_client_schema(client_id, client_schema)
            else:
                exc = RuntimeError(
                    "could not serialize result in worker subprocess"
                )
                exc.__formatted_error__ = data[0]
                raise exc

            return resp
        finally:
            self._release_worker(worker)

    async def compile_in_tx(
        self, pickled_state, txid, *compile_args, msg=None
    ):
        if isinstance(pickled_state, str):
            worker = await self._acquire_worker(
                condition=lambda w: (w._last_pickled_state == pickled_state)
            )
            if worker._last_pickled_state == pickled_state:
                pickled_state = state_mod.REUSE_LAST_STATE_MARKER
                msg = None
            else:
                self._release_worker(worker)
                raise state_mod.StateNotFound()
        else:
            worker = await self._acquire_worker()
        try:
            resp = await worker.call(
                "compile_in_tx", pickled_state, txid, *compile_args, msg=msg
            )
            status, *data = pickle.loads(resp)
            if status == 0:
                worker._last_pickled_state = hashlib.sha1(
                    data[0][1]
                ).hexdigest()
            return resp
        finally:
            self._release_worker(worker, put_in_front=False)

    async def _request(self, method_name, msg):
        worker = await self._acquire_worker()
        try:
            return await worker.call(method_name, msg=msg)
        finally:
            self._release_worker(worker)

    async def call(self, protocol, req_id, msg):
        client_id = protocol.client_id
        method_name, args = pickle.loads(msg)
        try:
            if method_name != "__init_server__":
                await self._ready_evt.wait()
            if method_name == "__init_server__":
                await self._init_server(client_id, *args)
                pickled = pickle.dumps((0, None), -1)
            elif method_name in {
                "compile",
                "compile_notebook",
                "compile_graphql",
            }:
                pickled = await self._call_for_client(
                    client_id, method_name, args, msg
                )
            elif method_name == "compile_in_tx":
                pickled = await self.compile_in_tx(*args, msg=msg)
            else:
                pickled = await self._request(method_name, msg)
        except Exception as ex:
            worker_mod.prepare_exception(ex)
            if debug.flags.server:
                markup.dump(ex)
            data = (1, ex, traceback.format_exc())
            try:
                pickled = pickle.dumps(data, -1)
            except Exception as ex:
                ex_tb = traceback.format_exc()
                ex_str = f"{ex}:\n\n{ex_tb}"
                pickled = pickle.dumps((2, ex_str), -1)
        protocol.reply(req_id, pickled)

    def client_disconnected(self, client_id):
        logger.debug("Client %d disconnected, invalidating cache.", client_id)
        self._clients.pop(client_id, None)
        for worker in self._workers.values():
            worker.invalidate(client_id)


class CompilerServerProtocol(asyncio.Protocol):
    def __init__(self, pool, loop):
        global _client_id_seq
        self._pool = pool
        self._loop = loop
        self._stream = amsg.MessageStream()
        self._transport = None
        self._client_id = _client_id_seq = _client_id_seq + 1

    def connection_made(self, transport):
        self._transport = transport
        transport.write(
            amsg._uint64_packer(os.getpid()) + amsg._uint64_packer(0)
        )

    def connection_lost(self, exc):
        self._transport = None
        self._pool.client_disconnected(self._client_id)

    def data_received(self, data):
        for msg in self._stream.feed_data(data):
            msgview = memoryview(msg)
            req_id = amsg._uint64_unpacker(msgview[:8])[0]
            self._loop.create_task(self._pool.call(self, req_id, msgview[8:]))

    @property
    def client_id(self):
        return self._client_id

    def reply(self, req_id, resp):
        self._transport.write(
            b"".join(
                (
                    amsg._uint64_packer(len(resp) + 8),
                    amsg._uint64_packer(req_id),
                    resp,
                )
            )
        )


async def server_main(socket_path, pool_size, cache_size):
    loop = asyncio.get_running_loop()
    pool = MultiSchemaPool(
        loop=loop,
        runstate_dir=os.path.dirname(socket_path),
        pool_size=pool_size,
        cache_size=cache_size,
    )
    await pool.start()
    try:
        server = await loop.create_unix_server(
            lambda: CompilerServerProtocol(pool, loop),
            socket_path,
            start_serving=False,
        )
        try:
            await server.serve_forever()
        finally:
            server.close()
            await server.wait_closed()
    finally:
        await pool.stop()


@click.command()
@srvargs.compiler_options
def main(**kwargs):
    asyncio.run(server_main(**kwargs))


if __name__ == "__main__":
    main()
