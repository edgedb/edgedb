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
from typing import *

import asyncio
import collections
import dataclasses
import json
import logging
import pathlib
import signal
import sys
import weakref

import setproctitle

from edb import buildmeta
from edb import errors
from edb.common import retryloop
from edb.common import signalctl
from edb.common import taskgroup
from edb.pgsql import params as pgparams
from edb.server import compiler as edbcompiler

from . import args as srvargs
from . import config
from . import defines
from . import pgcluster
from . import server
from . import tenant as edbtenant
from .compiler_pool import pool as compiler_pool
from .pgcon import errors as pgerrors

logger = logging.getLogger("edb.server")


TenantConfig = TypedDict(
    "TenantConfig",
    {
        "instance-name": str,
        "backend-dsn": str,
        "max-backend-connections": int,
        "tenant-id": str,
        "backend-adaptive-ha": bool,
        "jwt-sub-allowlist-file": str,
        "jwt-revocation-list-file": str,
        "readiness-state-file": str,
        "admin": bool,
    },
)


class MultiTenantServer(server.BaseServer):
    _config_file: pathlib.Path
    _sys_config: Mapping[str, config.SettingValue]
    _backend_settings: Mapping[str, str]

    _tenants_by_sslobj: MutableMapping
    _tenants_conf: dict[str, dict[str, str]]
    _tenants_lock: MutableMapping[str, asyncio.Lock]
    _tenants_serial: dict[str, int]
    _tenants: dict[str, edbtenant.Tenant]
    _admin_tenant: edbtenant.Tenant | None

    _task_group: taskgroup.TaskGroup | None
    _task_serial: int

    def __init__(
        self,
        config_file: pathlib.Path,
        *,
        compiler_pool_tenant_cache_size: int,
        sys_config: Mapping[str, config.SettingValue],
        backend_settings: Mapping[str, str],
        sys_queries: Mapping[str, bytes],
        report_config_typedesc: dict[defines.ProtocolVersion, bytes],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._config_file = config_file
        self._sys_config = sys_config
        self._backend_settings = backend_settings
        self._compiler_pool_tenant_cache_size = compiler_pool_tenant_cache_size

        self._tenants_by_sslobj = weakref.WeakKeyDictionary()
        self._tenants_conf = {}
        self._tenants_lock = collections.defaultdict(asyncio.Lock)
        self._tenants_serial = {}
        self._tenants = {}
        self._admin_tenant = None

        self._task_group = taskgroup.TaskGroup()
        self._task_serial = 0
        self._sys_queries = sys_queries
        self._report_config_typedesc = report_config_typedesc

    def _get_sys_config(self) -> Mapping[str, config.SettingValue]:
        return self._sys_config

    def _sni_callback(self, sslobj, server_name, _sslctx):
        if tenant := self._tenants.get(server_name):
            self._tenants_by_sslobj[sslobj] = tenant

    def get_default_tenant(self) -> edbtenant.Tenant:
        raise errors.UnknownTenantError(
            "No such tenant configured.",
            hint="Please try again later, or "
                 "double check the SNI/server name in TLS connection",
        )

    def retrieve_tenant(self, sslobj) -> edbtenant.Tenant | None:
        return self._tenants_by_sslobj.pop(sslobj, None)

    def iter_tenants(self) -> Iterator[edbtenant.Tenant]:
        return iter(self._tenants.values())

    async def _before_start_servers(self) -> None:
        assert self._task_group is not None
        await self._task_group.__aenter__()
        fs = self.reload_tenants()
        if fs:
            await asyncio.wait(fs)

    def _get_status(self) -> dict[str, Any]:
        status = super()._get_status()
        tenants = {}
        for server_name, tenant in self._tenants.items():
            tenants[server_name] = {
                "tenant_id": tenant.tenant_id,
            }
        status["tenants"] = tenants
        return status

    def _get_backend_runtime_params(self) -> pgparams.BackendRuntimeParams:
        return pgparams.get_default_runtime_params()

    async def stop(self):
        await super().stop()
        await self._task_group.__aexit__(*sys.exc_info())
        try:
            for tenant in self._tenants.values():
                tenant.stop()
            for tenant in self._tenants.values():
                await tenant.wait_stopped()
        finally:
            for tenant in self._tenants.values():
                tenant.terminate_sys_pgcon()

    def reload_tenants(self) -> Sequence[asyncio.Future]:
        with self._config_file.open() as cf:
            conf = json.load(cf)
        rv = []
        for sni, tenant_conf in conf.items():
            if sni not in self._tenants_conf:
                rv.append(
                    self._create_task(self._add_tenant, sni, tenant_conf)
                )
        for sni in self._tenants_conf:
            if sni not in conf:
                rv.append(self._create_task(self._remove_tenant, sni))
        self._tenants_conf = conf
        return rv

    def _create_task(self, method, *args) -> asyncio.Task:
        self._task_serial += 1
        assert self._task_group is not None
        return self._task_group.create_task(method(self._task_serial, *args))

    async def _create_tenant(self, conf: TenantConfig) -> edbtenant.Tenant:
        cluster = await pgcluster.get_remote_pg_cluster(
            conf["backend-dsn"], tenant_id=conf.get("tenant-id")
        )
        instance_params = cluster.get_runtime_params().instance_params
        max_conns = (
            instance_params.max_connections
            - instance_params.reserved_connections
        )
        if "max-backend-connections" not in conf:
            logger.info(f"Detected {max_conns} backend connections available.")
            if self._testmode:
                max_conns = srvargs.adjust_testmode_max_connections(max_conns)
                logger.info(
                    f"Using max_backend_connections={max_conns} "
                    f"under test mode."
                )
        elif conf["max-backend-connections"] > max_conns:
            raise server.StartupError(
                f"--max-backend-connections is too large for this backend; "
                f"detected maximum available NUM: {max_conns}"
            )
        else:
            max_conns = conf["max-backend-connections"]

        conn_params = cluster.get_connection_params()
        conn_params = dataclasses.replace(
            conn_params,
            server_settings={
                **conn_params.server_settings,
                "application_name": f'edgedb_instance_{conf["instance-name"]}',
                "edgedb.instance_name": conf["instance-name"],
                "edgedb.server_version": buildmeta.get_version_json(),
            },
        )
        cluster.set_connection_params(conn_params)

        if "jwt-sub-allowlist-file" in conf:
            jwt_sub_allowlist_file = pathlib.Path(
                conf["jwt-sub-allowlist-file"]
            )
        else:
            jwt_sub_allowlist_file = None
        if "jwt-revocation-list-file" in conf:
            jwt_revocation_list_file = pathlib.Path(
                conf["jwt-revocation-list-file"]
            )
        else:
            jwt_revocation_list_file = None
        tenant = edbtenant.Tenant(
            cluster,
            instance_name=conf["instance-name"],
            max_backend_connections=max_conns,
            backend_adaptive_ha=conf.get("backend-adaptive-ha", False),
            readiness_state_file=conf.get("readiness-state-file"),
            jwt_sub_allowlist_file=jwt_sub_allowlist_file,
            jwt_revocation_list_file=jwt_revocation_list_file,
        )
        tenant.set_server(self)
        tenant.load_jwcrypto()
        try:
            await tenant.init_sys_pgcon()
            await tenant.init()
            await tenant.start_accepting_new_tasks()
            tenant.start_running()

            if conf.get("admin", False):
                # There can be only one "admin" tenant, the behavior of setting
                # multiple tenants with `"admin": true` is undefined.
                self._admin_tenant = tenant

            return tenant
        except Exception:
            await self._destroy_tenant(tenant)
            raise

    def _get_admin_tenant(self) -> edbtenant.Tenant:
        if self._admin_tenant is None:
            return super()._get_admin_tenant()
        else:
            return self._admin_tenant

    async def _destroy_tenant(self, tenant: edbtenant.Tenant):
        try:
            tenant.stop_accepting_connections()
            tenant.stop()
            await tenant.wait_stopped()
            assert isinstance(
                self._compiler_pool, compiler_pool.MultiTenantPool
            )
            self._compiler_pool.drop_tenant(tenant.client_id)
        finally:
            tenant.terminate_sys_pgcon()

    async def _add_tenant(self, serial: int, sni: str, conf: TenantConfig):
        def _warn(e):
            logger.warning(
                "Failed to add Tenant %s, retrying. Reason: %s", sni, e
            )

        async def _add_tenant():
            rloop = retryloop.RetryLoop(
                backoff=retryloop.exp_backoff(),
                timeout=300,
                ignore=(pgerrors.BackendError, IOError),
                retry_cb=_warn,
            )
            async for iteration in rloop:
                async with iteration:
                    async with self._tenants_lock[sni]:
                        if serial > self._tenants_serial.get(sni, 0):
                            if sni not in self._tenants:
                                tenant = await self._create_tenant(conf)
                                self._tenants[sni] = tenant
                                logger.info("Added Tenant %s", sni)
                            self._tenants_serial[sni] = serial

        try:
            with signalctl.SignalController(
                signal.SIGINT, signal.SIGTERM
            ) as sc:
                await sc.wait_for(_add_tenant())
        except signalctl.SignalError:
            pass
        except Exception:
            logger.critical("Failed to add Tenant %s", sni, exc_info=True)
            async with self._tenants_lock[sni]:
                if serial > self._tenants_serial.get(sni, 0):
                    self._tenants_conf.pop(sni, None)

    async def _remove_tenant(self, serial: int, sni: str):
        try:
            async with self._tenants_lock[sni]:
                if serial > self._tenants_serial.get(sni, 0):
                    if sni in self._tenants:
                        tenant = self._tenants.pop(sni)
                        await self._destroy_tenant(tenant)
                        logger.info("Removed Tenant %s", sni)
                    self._tenants_serial[sni] = serial
        except Exception:
            logger.critical("Failed to remove Tenant %s", sni, exc_info=True)

    def get_debug_info(self):
        parent = super().get_debug_info()
        parent["tenants"] = {
            name: tenant.get_debug_info()
            for name, tenant in self._tenants.items()
        }
        return parent

    def _get_compiler_args(self) -> dict[str, Any]:
        args = super()._get_compiler_args()
        args["cache_size"] = self._compiler_pool_tenant_cache_size
        return args


async def run_server(
    args: srvargs.ServerConfig,
    *,
    sys_config: Mapping[str, config.SettingValue],
    backend_settings: Mapping[str, str],
    sys_queries: Mapping[str, bytes],
    report_config_typedesc: dict[defines.ProtocolVersion, bytes],
    runstate_dir: pathlib.Path,
    internal_runstate_dir: str,
    do_setproctitle: bool,
    compiler_state: edbcompiler.CompilerState,
):
    multitenant_config_file = args.multitenant_config_file
    assert multitenant_config_file is not None

    with signalctl.SignalController(signal.SIGINT, signal.SIGTERM) as sc:
        ss = MultiTenantServer(
            multitenant_config_file,
            sys_config=sys_config,
            backend_settings=backend_settings,
            sys_queries=sys_queries,
            report_config_typedesc=report_config_typedesc,
            runstate_dir=runstate_dir,
            internal_runstate_dir=internal_runstate_dir,
            nethosts=args.bind_addresses,
            netport=args.port,
            listen_sockets=(),
            auto_shutdown_after=args.auto_shutdown_after,
            echo_runtime_info=args.echo_runtime_info,
            status_sinks=args.status_sinks,
            binary_endpoint_security=args.binary_endpoint_security,
            http_endpoint_security=args.http_endpoint_security,
            default_auth_method=args.default_auth_method,
            testmode=args.testmode,
            admin_ui=args.admin_ui,
            disable_dynamic_system_config=args.disable_dynamic_system_config,
            compiler_pool_size=args.compiler_pool_size,
            compiler_pool_mode=srvargs.CompilerPoolMode.MultiTenant,
            compiler_pool_addr=args.compiler_pool_addr,
            compiler_pool_tenant_cache_size=(
                args.compiler_pool_tenant_cache_size
            ),
            compiler_state=compiler_state,
        )
        # This coroutine runs as long as the server,
        # and compiler_state is *heavy*, so make sure we don't
        # keep a reference to it.
        del compiler_state
        await sc.wait_for(ss.init())

        (
            tls_cert_newly_generated, jws_keys_newly_generated
        ) = await ss.maybe_generate_pki(args, ss)
        ss.init_tls(
            args.tls_cert_file, args.tls_key_file, tls_cert_newly_generated
        )
        ss.init_jwcrypto(args.jws_key_file, jws_keys_newly_generated)

        def load_configuration(_signum):
            logger.info("reloading configuration")
            try:
                ss.reload_tls(args.tls_cert_file, args.tls_key_file)
                ss.load_jwcrypto(args.jws_key_file)
                ss.reload_tenants()
            except Exception:
                logger.critical(
                    "Unexpected error occurred during reload configuration; "
                    "shutting down.",
                    exc_info=True,
                )
                ss.request_shutdown()

        try:
            await sc.wait_for(ss.start())
            if do_setproctitle:
                setproctitle.setproctitle(
                    f"edgedb-server-{ss.get_listen_port()}"
                )
            with signalctl.SignalController(signal.SIGHUP) as reload_ctl:
                reload_ctl.add_handler(
                    load_configuration, signals=(signal.SIGHUP,)
                )
                try:
                    await sc.wait_for(ss.serve_forever())
                except signalctl.SignalError as e:
                    logger.info("Received signal: %s.", e.signo)
        finally:
            logger.info("Shutting down.")
            await sc.wait_for(ss.stop())
