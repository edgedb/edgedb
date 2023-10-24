#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

import asyncio
import base64
import contextlib
import json
import os
import pathlib
import subprocess
import tempfile
import time
import unittest
import urllib.parse

import edgedb
import httptools

from edb.testbase import server as tb
from edb.server import pgcluster
from edb.server.ha import base as ha_base


class HTTPGet(asyncio.Protocol):
    def __init__(self, path, host="127.0.0.1", port=None, timeout=10):
        self._buffers = []
        self._transport = None
        self._host = host
        self._path = path
        self._waiter = asyncio.Future()
        self._parser = httptools.HttpResponseParser(self)
        self._timeout_handle = None

        loop = asyncio.get_running_loop()
        loop.create_task(
            loop.create_connection(lambda: self, host, port or 80)
        ).add_done_callback(
            self._connect_cb
        )
        if timeout:
            self._timeout_handle = loop.call_later(timeout, self._on_timeout)

    def _set_result(self, result):
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
        if not self._waiter.done():
            if isinstance(result, BaseException):
                self._waiter.set_exception(result)
            else:
                self._waiter.set_result(result)
            if self._transport is not None:
                self._transport.close()

    def _connect_cb(self, task: asyncio.Task):
        ex = task.exception()
        if ex is not None:
            self._set_result(ex)

    def _on_timeout(self):
        self._set_result(TimeoutError("HTTP request timeout"))

    def connection_made(self, transport):
        self._transport = transport
        transport.write(
            f"GET {self._path} HTTP/1.1\r\n"
            f"Host: {self._host}\r\n"
            f"\r\n".encode()
        )

    def data_received(self, data: bytes):
        try:
            self._parser.feed_data(data)
        except Exception as ex:
            self._set_result(ex)

    def connection_lost(self, exc):
        self._transport = None
        self._set_result(exc or RuntimeError("Connection broken unexpectedly"))

    def on_status(self, status: bytes):
        code = self._parser.get_status_code()
        if code != 200:
            raise RuntimeError(f"Server returned {code}: {status.decode()}")

    def on_body(self, body: bytes):
        self._buffers.append(body)

    def on_message_complete(self):
        self._set_result(b"".join(self._buffers))

    def __await__(self):
        return self._waiter.__await__()


class ServerContext:
    title: str = NotImplemented

    def __init__(self, debug=False):
        self.debug = debug
        self.proc = None

    async def run(self, *args):
        if self.debug:
            print("Running: ", args)
        self.proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=None if self.debug else subprocess.PIPE,
            stderr=None if self.debug else subprocess.STDOUT,
        )

    async def stop(self, success=True):
        if self.proc is not None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), 30)
                if success:
                    return
            except asyncio.TimeoutError:
                self.proc.kill()

            if not self.debug:
                stdout, _ = await self.proc.communicate()
                print("=" * 79)
                print(f"Captured {self.title}")
                print("=" * 79)
                print(stdout.decode())
                print()


class ConsulAgent(ServerContext):
    title = "Consul Log"

    async def __aenter__(self):
        self.server_port = tb.find_available_port()
        self.http_port = tb.find_available_port()
        self.tmp_dir = tempfile.TemporaryDirectory()
        tmp_dir = pathlib.Path(self.tmp_dir.name)
        config_file = tmp_dir / "config.json"
        with config_file.open("w") as f:
            json.dump(
                dict(
                    ports=dict(
                        server=self.server_port,
                        http=self.http_port,
                        serf_lan=tb.find_available_port(),
                        dns=-1,
                        grpc=-1,
                        serf_wan=-1,
                    ),
                    data_dir=str(tmp_dir),
                ),
                f,
            )
        if self.debug:
            with config_file.open() as f:
                print('config.json:')
                print(f.read())
        await self.run(
            os.environ.get("EDGEDB_TEST_CONSUL_PATH", "consul"),
            "agent",
            "-dev",
            "-log-level=info",
            "-config-file",
            str(config_file),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.stop(exc_type is None)
        finally:
            self.tmp_dir.cleanup()


class StolonSentinel(ServerContext):
    title = "Stolon Sentinel Log"

    def __init__(
        self, consul: ConsulAgent, *, debug=False, cluster_name="test-cluster"
    ):
        super().__init__(debug)
        self.cluster_name = cluster_name
        self.consul_http_port = consul.http_port

    async def init(self):
        args = [
            os.environ.get("EDGEDB_TEST_STOLON_CTL", "stolonctl"),
            "--cluster-name",
            self.cluster_name,
            "--store-backend=consul",
            "--store-endpoints",
            f"http://127.0.0.1:{self.consul_http_port}",
            "init",
            "--yes",
            json.dumps(dict(
                initMode="new",
                sleepInterval="0.5s",
                requestTimeout="1s",
                failInterval="2s",
            )),
        ]
        if self.debug:
            print("Running:", args)
        p = await asyncio.create_subprocess_exec(
            *args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        stdout, _ = await p.communicate()
        if p.returncode:
            return stdout.decode()
        return ""

    async def __aenter__(self):
        start = time.monotonic()
        while True:
            error = await self.init()
            if not error:
                break
            if "connection refused" not in error:
                print("=" * 79)
                print("Captured Output for: stolonctl init")
                print("=" * 79)
                print(error)
                raise RuntimeError("Stolon cannot connect to Consul")
            await asyncio.sleep(0.1)
            if time.monotonic() - start > 60:
                raise RuntimeError("Stolon cannot connect to Consul in 60s")
        await self.run(
            os.environ.get("EDGEDB_TEST_STOLON_SENTINEL", "stolon-sentinel"),
            "--cluster-name",
            self.cluster_name,
            "--store-backend=consul",
            "--store-endpoints",
            f"http://127.0.0.1:{self.consul_http_port}",
        )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop(exc_type is None)


class StolonKeeper(ServerContext):
    title = "Stolon Keeper Log"

    def __init__(
        self,
        consul: ConsulAgent,
        pg_bin_dir,
        debug=False,
        cluster_name="test-cluster",
    ):
        super().__init__(debug)
        self.cluster_name = cluster_name
        self.consul_http_port = consul.http_port
        self.pg_bin_dir = pg_bin_dir
        self.tmp_dir = None

    async def __aenter__(self):
        self.port = tb.find_available_port()
        self.tmp_dir = tempfile.TemporaryDirectory()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.stop(exc_type is None)
        finally:
            if self.tmp_dir is not None:
                self.tmp_dir.cleanup()

    async def stop(self, success=True):
        proc, self.proc = self.proc, None
        if proc is not None:
            proc.terminate()
            if not await self.wait_for_stop():
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                if not self.debug:
                    stdout, _ = await proc.communicate()
                    print("=" * 79)
                    print(f"Captured {self.title}")
                    print("=" * 79)
                    print(stdout.decode())
                    print()
                raise RuntimeError("Stolon keeper didn't stop in 60s")

    async def start(self):
        await self.run(
            os.environ.get("EDGEDB_TEST_STOLON_KEEPER", "stolon-keeper"),
            "--log-level",
            "warn",
            "--cluster-name",
            self.cluster_name,
            "--store-backend=consul",
            "--store-endpoints",
            f"http://127.0.0.1:{self.consul_http_port}",
            "--uid",
            f"pg{self.port}",
            "--data-dir",
            self.tmp_dir.name,
            "--pg-su-username=suname",
            "--pg-su-password=supass",
            "--pg-repl-username=repluser",
            "--pg-repl-password=replpassword",
            "--pg-listen-address=127.0.0.1",
            "--pg-port",
            str(self.port),
            "--pg-bin-path",
            str(self.pg_bin_dir),
        )
        try:
            await self.wait_for_healthy()
        except Exception:
            if self.tmp_dir is not None:
                self.tmp_dir.cleanup()
            await self.stop(False)
            raise

    async def wait_for_healthy(self):
        start = time.monotonic()
        while True:
            payload = await HTTPGet(
                "/v1/kv/stolon/cluster/test-cluster/clusterdata",
                port=self.consul_http_port,
            )
            payload = json.loads(payload)[0]
            cluster_data = json.loads(base64.b64decode(payload["Value"]))
            for db in cluster_data.get("dbs", {}).values():
                if db.get("spec", {}).get("keeperUID") == f"pg{self.port}":
                    if db.get("status", {}).get("healthy"):
                        if db.get("spec", {}).get("initMode") == "none":
                            return
            await asyncio.sleep(1)
            if time.monotonic() - start > 60:
                raise RuntimeError("Stolon keeper didn't start in 60s")

    async def wait_for_stop(self):
        start = time.monotonic()
        while True:
            payload = await HTTPGet(
                "/v1/kv/stolon/cluster/test-cluster/clusterdata",
                port=self.consul_http_port,
            )
            payload = json.loads(payload)[0]
            cluster_data = json.loads(base64.b64decode(payload["Value"]))
            for db in cluster_data.get("dbs", {}).values():
                if db.get("spec", {}).get("keeperUID") == f"pg{self.port}":
                    if not db.get("status", {}).get("healthy", False):
                        return True
                    break
            else:
                return True
            await asyncio.sleep(1)
            if time.monotonic() - start > 60:
                return False


class AdaptiveHAProxy(ha_base.ClusterProtocol):
    def __init__(
        self, consul_http_port, debug=False, cluster_name="test-cluster"
    ):
        self.parsed_dsn = urllib.parse.urlparse(
            f"stolon+consul+http://127.0.0.1:{consul_http_port}/{cluster_name}"
        )
        self.debug = debug
        self.server = None
        self.port = None
        self.master_addr = None

    async def __aenter__(self):
        self.consul = ha_base.get_backend(self.parsed_dsn)
        self.master_addr = await self.consul.get_cluster_consensus()
        self.port = tb.find_available_port()
        self.consul.set_failover_callback(self.on_switch_over)
        await self.consul.start_watching()
        self.server = await asyncio.start_server(
            self._proxy_connection, "127.0.0.1", self.port
        )
        return self.port

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.consul.stop_watching()
        self.server.close()
        await self.server.wait_closed()

    async def _proxy_connection(self, reader, writer):
        try:
            pg_reader, pg_writer = await asyncio.open_connection(
                *self.master_addr
            )
        except Exception as e:
            if self.debug:
                print(
                    f"AdaptiveHAProxy: failed to proxy connection to "
                    f"{self.master_addr}, reason: {e!r}"
                )
            writer.close()
            await writer.wait_closed()
        else:
            if self.debug:
                print(
                    f"AdaptiveHAProxy: proxying connection to "
                    f"{self.master_addr}"
                )
            await asyncio.gather(
                self._proxy_traffic(reader, pg_writer),
                self._proxy_traffic(pg_reader, writer),
            )

    @staticmethod
    async def _proxy_traffic(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            while True:
                data = await reader.read(32768)
                if not data:
                    return
                writer.write(data)
        finally:
            writer.close()
            await writer.wait_closed()

    def on_switch_over(self):
        self.master_addr = self.consul.get_master_addr()
        if self.debug:
            print(f"AdaptiveHAProxy: master switched to {self.master_addr}")

    def get_active_pgcon_num(self) -> int:
        return 0


@contextlib.asynccontextmanager
async def stolon_setup(*, debug=False):
    pg_bin_dir = await pgcluster.get_pg_bin_dir()

    async with ConsulAgent() as consul:
        async with StolonSentinel(consul, debug=debug):
            async with StolonKeeper(consul, pg_bin_dir, debug=debug) as pg1:
                async with StolonKeeper(
                    consul, pg_bin_dir, debug=debug
                ) as pg2:
                    yield consul, pg1, pg2


def setUpModule():
    debug = False

    try:
        consul_path = os.environ.get("EDGEDB_TEST_CONSUL_PATH", "consul")
        subprocess.check_call(
            [consul_path, "--version"],
            stdout=None if debug else subprocess.DEVNULL,
            stderr=None if debug else subprocess.DEVNULL,
        )
        stolon_path = os.environ.get("EDGEDB_TEST_STOLON_CTL", "stolonctl")
        subprocess.check_call(
            [stolon_path, "--version"],
            stdout=None if debug else subprocess.DEVNULL,
            stderr=None if debug else subprocess.DEVNULL,
        )
    except Exception:
        raise unittest.SkipTest("Consul not installed")


@unittest.skipIf(
    not os.environ.get("EDGEDB_TEST_HA"), "EDGEDB_TEST_HA is not set"
)
class TestBackendHA(tb.TestCase):
    async def _wait_for_failover(self, con):
        async for tx in con.with_retry_options(
            edgedb.RetryOptions(60, lambda x: 1)
        ).retrying_transaction():
            async with tx:
                rv = await tx.query_single("SELECT 1")
        else:
            self.assertEqual(rv, 1)

    async def test_ha_stolon(self):
        debug = False
        async with stolon_setup(debug=debug) as (consul, pg1, pg2):
            if debug:
                print("=" * 80)
                print("Stolon is ready")
            async with tb.start_edgedb_server(
                backend_dsn=(
                    f"stolon+consul+http://127.0.0.1:{consul.http_port}"
                    f"/{pg1.cluster_name}"
                    f"?pguser=suname&pgpassword=supass&pgdatabase=postgres"
                ),
                runstate_dir=str(pathlib.Path(consul.tmp_dir.name) / "edb"),
                reset_auth=True,
                debug=debug,
            ) as sd:
                await self._test_failover(pg1, pg2, sd, debug=debug)

    async def test_ha_adaptive(self):
        debug = False
        env = dict(
            EDGEDB_SERVER_BACKEND_ADAPTIVE_HA_UNHEALTHY_MIN_TIME="3"
        )
        async with stolon_setup(debug=debug) as (consul, pg1, pg2):
            async with AdaptiveHAProxy(consul.http_port, debug=debug) as port:
                async with tb.start_edgedb_server(
                    backend_dsn=(
                        f"postgresql://suname:supass@127.0.0.1:{port}/postgres"
                    ),
                    runstate_dir=str(
                        pathlib.Path(consul.tmp_dir.name) / "edb"
                    ),
                    enable_backend_adaptive_ha=True,
                    reset_auth=True,
                    debug=debug,
                    env=env,
                ) as sd:
                    await self._test_failover(pg1, pg2, sd, debug=debug)

    async def _test_failover(self, pg1, pg2, sd, debug=False):
        if debug:
            print("=" * 80)
            print("Initialize the State")
        con = await sd.connect()
        await con.execute(
            "CREATE TYPE State { "
            "   CREATE REQUIRED PROPERTY value -> int32;"
            "};"
        )
        await con.execute("INSERT State { value := 1 };")
        self.assertEqual(
            await con.query_single("SELECT State.value LIMIT 1"), 1
        )
        if debug:
            print("=" * 80)
            print("Stop the master, failover to slave")
        await pg1.stop()
        if debug:
            print("=" * 80)
            print("Master stopped")
        async for tx in con.with_retry_options(
            edgedb.RetryOptions(60, lambda x: 1)
        ).retrying_transaction():
            async with tx:
                self.assertEqual(
                    await tx.query_single("SELECT State.value LIMIT 1"),
                    1,
                )
                await tx.execute("UPDATE State SET { value := 2 };")
        if debug:
            print("=" * 80)
            print("State updated to 2")
        self.assertEqual(
            await con.query_single("SELECT State.value LIMIT 1"), 2
        )
        if debug:
            print("=" * 80)
            print("Start the old master as slave")
        await pg1.start()
        if debug:
            print("=" * 80)
            print("Stop the new master, failover to old master again")
        await pg2.stop()
        if debug:
            print("=" * 80)
            print("State should still be 2")
        async for tx in con.with_retry_options(
            edgedb.RetryOptions(60, lambda x: 1)
        ).retrying_transaction():
            async with tx:
                self.assertEqual(
                    await tx.query_single("SELECT State.value LIMIT 1"),
                    2,
                )
