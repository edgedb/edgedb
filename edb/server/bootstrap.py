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
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Iterable,
    Mapping,
    Awaitable,
    Dict,
    List,
    Set,
    NamedTuple,
    TYPE_CHECKING,
)

import dataclasses
import enum
import json
import logging
import os
import pathlib
import pickle
import re
import struct
import textwrap

from edb import buildmeta
from edb import errors

from edb import edgeql
from edb.ir import statypes
from edb.ir import typeutils as irtyputils
from edb.edgeql import ast as qlast

from edb.common import debug
from edb.common import devmode
from edb.common import retryloop
from edb.common import uuidgen

from edb.schema import ddl as s_ddl
from edb.schema import delta as sd
from edb.schema import extensions as s_exts
from edb.schema import functions as s_func
from edb.schema import modules as s_mod
from edb.schema import name as sn
from edb.schema import objects as s_obj
from edb.schema import properties as s_props
from edb.schema import reflection as s_refl
from edb.schema import schema as s_schema
from edb.schema import std as s_std
from edb.schema import types as s_types

from edb.server import args as edbargs
from edb.server import config
from edb.server import compiler as edbcompiler
from edb.server import defines as edbdef
from edb.server import pgcluster
from edb.server import pgcon

from edb.pgsql import common as pg_common
from edb.pgsql import dbops
from edb.pgsql import delta as delta_cmds
from edb.pgsql import metaschema
from edb.pgsql import params
from edb.pgsql import patches
from edb.pgsql import trampoline
from edb.pgsql.common import quote_ident as qi

from edgedb import scram

if TYPE_CHECKING:
    import uuid


logger = logging.getLogger('edb.server')
STDLIB_CACHE_FILE_NAME = 'backend-stdlib.pickle'


class ClusterMode(enum.IntEnum):
    pristine = 0
    regular = 1
    single_role = 2
    single_database = 3


_T = TypeVar("_T")


# A simple connection proxy that reconnects and retries queries
# on connection errors.  Helps defeat flaky connections and/or
# flaky Postgres servers (Digital Ocean managed instances are
# one example that has a weird setup that crashes a helper
# process when we bootstrap, breaking other connections).
class PGConnectionProxy:
    def __init__(
        self,
        cluster: pgcluster.BaseCluster,
        *,
        source_description: str,
        dbname: Optional[str] = None,
        log_listener: Optional[Callable[[str, str], None]] = None,
    ):
        self._conn: Optional[pgcon.PGConnection] = None
        self._cluster = cluster
        self._dbname = dbname
        self._log_listener = log_listener or _pg_log_listener
        self._source_description = source_description

    async def connect(self) -> None:
        if self._conn is not None:
            self._conn.terminate()

        if self._dbname:
            self._conn = await self._cluster.connect(
                source_description=self._source_description,
                database=self._dbname
            )
        else:
            self._conn = await self._cluster.connect(
                source_description=self._source_description
            )

        if self._log_listener is not None:
            self._conn.add_log_listener(self._log_listener)

    def _on_retry(self, exc: Optional[BaseException]) -> None:
        logger.warning(
            f'Retrying bootstrap SQL query due to connection error: '
            f'{type(exc)}({exc})',
        )
        self.terminate()

    async def _retry_conn_errors(
        self,
        task: Callable[[], Awaitable[_T]],
    ) -> _T:
        rloop = retryloop.RetryLoop(
            backoff=retryloop.exp_backoff(),
            timeout=5.0,
            ignore=(
                ConnectionError,
                pgcon.BackendConnectionError,
            ),
            retry_cb=self._on_retry,
        )
        async for iteration in rloop:
            async with iteration:
                if self._conn is None:
                    await self.connect()
                result = await task()

        return result

    async def sql_execute(self, sql: bytes) -> None:
        async def _task() -> None:
            assert self._conn is not None
            await self._conn.sql_execute(sql)
        return await self._retry_conn_errors(_task)

    async def sql_fetch(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
    ) -> list[tuple[bytes, ...]]:
        async def _task() -> list[tuple[bytes, ...]]:
            assert self._conn is not None
            return await self._conn.sql_fetch(sql, args=args)
        return await self._retry_conn_errors(_task)

    async def sql_fetch_val(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
    ) -> bytes:
        async def _task() -> bytes:
            assert self._conn is not None
            return await self._conn.sql_fetch_val(sql, args=args)
        return await self._retry_conn_errors(_task)

    async def sql_fetch_col(
        self,
        sql: bytes,
        *,
        args: tuple[bytes, ...] | list[bytes] = (),
    ) -> list[bytes]:
        async def _task() -> list[bytes]:
            assert self._conn is not None
            return await self._conn.sql_fetch_col(sql, args=args)
        return await self._retry_conn_errors(_task)

    def terminate(self) -> None:
        if self._conn is not None:
            self._conn.terminate()
            self._conn = None


@dataclasses.dataclass
class BootstrapContext:

    cluster: pgcluster.BaseCluster
    conn: PGConnectionProxy | pgcon.PGConnection
    args: edbargs.ServerConfig
    mode: Optional[ClusterMode] = None


async def _execute(conn, query):
    return await metaschema.execute_sql_script(conn, query)


async def _execute_block(conn, block: dbops.SQLBlock) -> None:

    if not block.is_transactional():
        stmts = block.get_statements()
    else:
        stmts = [block.to_string()]

    for stmt in stmts:
        await _execute(conn, stmt)


def _execute_edgeql_ddl(
    schema: s_schema.Schema_T,
    ddltext: str,
    stdmode: bool = True,
) -> s_schema.Schema_T:
    context = sd.CommandContext(stdmode=stdmode)

    for ddl_cmd in edgeql.parse_block(ddltext):
        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=stdmode)

        schema = delta_command.apply(schema, context)  # type: ignore

    return schema


async def _ensure_edgedb_supergroup(
    ctx: BootstrapContext,
    role_name: str,
    *,
    member_of: Iterable[str] = (),
    members: Iterable[str] = (),
) -> None:
    member_of = set(member_of)
    backend_params = ctx.cluster.get_runtime_params()
    superuser_role = backend_params.instance_params.base_superuser
    if superuser_role:
        # If the cluster is exposing an explicit superuser role,
        # become a member of that instead of creating a superuser
        # role directly.
        member_of.add(superuser_role)

    pg_role_name = ctx.cluster.get_role_name(role_name)

    role = dbops.Role(
        name=pg_role_name,
        superuser=backend_params.has_superuser_access,
        allow_login=False,
        allow_createdb=True,
        allow_createrole=True,
        membership=member_of,
        members=members,
    )

    create_role = dbops.CreateRole(
        role,
        neg_conditions=[dbops.RoleExists(pg_role_name)],
    )

    block = dbops.PLTopBlock()
    create_role.generate(block)

    await _execute_block(ctx.conn, block)


async def _ensure_edgedb_role(
    ctx: BootstrapContext,
    role_name: str,
    *,
    superuser: bool = False,
    builtin: bool = False,
    objid: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    member_of = set()
    if superuser:
        member_of.add(edbdef.EDGEDB_SUPERGROUP)

    if objid is None:
        objid = uuidgen.uuid1mc()

    members = set()
    login_role = ctx.cluster.get_connection_params().user
    assert login_role is not None
    sup_role = ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERUSER)
    if login_role != sup_role:
        members.add(login_role)

    backend_params = ctx.cluster.get_runtime_params()
    pg_role_name = ctx.cluster.get_role_name(role_name)
    role = dbops.Role(
        name=pg_role_name,
        superuser=superuser and backend_params.has_superuser_access,
        allow_login=True,
        allow_createdb=True,
        allow_createrole=True,
        membership=[ctx.cluster.get_role_name(m) for m in member_of],
        members=members,
        metadata=dict(
            id=str(objid),
            name=role_name,
            tenant_id=backend_params.tenant_id,
            builtin=builtin,
        ),
    )

    create_role = dbops.CreateRole(
        role,
        neg_conditions=[dbops.RoleExists(pg_role_name)],
    )

    block = dbops.PLTopBlock()
    create_role.generate(block)

    await _execute_block(ctx.conn, block)

    return objid


async def _get_cluster_mode(ctx: BootstrapContext) -> ClusterMode:
    backend_params = ctx.cluster.get_runtime_params()
    tenant_id = backend_params.tenant_id

    # First, check the existence of EDGEDB_SUPERGROUP - the role which is
    # usually created at the beginning of bootstrap.
    is_default_tenant = tenant_id == buildmeta.get_default_tenant_id()
    ignore_others = is_default_tenant and ctx.args.ignore_other_tenants
    if is_default_tenant:
        result = await ctx.conn.sql_fetch_col(
            b"""
            SELECT
                r.rolname
            FROM
                pg_catalog.pg_roles AS r
            WHERE
                r.rolname LIKE ('%' || $1)
            """,
            args=[
                edbdef.EDGEDB_SUPERGROUP.encode("utf-8"),
            ],
        )
    else:
        result = await ctx.conn.sql_fetch_col(
            b"""
            SELECT
                r.rolname
            FROM
                pg_catalog.pg_roles AS r
            WHERE
                r.rolname = $1
            """,
            args=[
                ctx.cluster.get_role_name(
                    edbdef.EDGEDB_SUPERGROUP).encode("utf-8"),
            ],
        )

    if result:
        if not ignore_others:
            # Either our tenant slot is occupied, or there is
            # a default tenant present.
            return ClusterMode.regular

        # We were explicitly asked to ignore the other default tenant,
        # so check specifically if our tenant slot is occupied and ignore
        # the others.
        # This mode is used for in-place upgrade.
        for rolname in result:
            other_tenant_id = rolname[: -(len(edbdef.EDGEDB_SUPERGROUP) + 1)]
            if other_tenant_id == tenant_id.encode("utf-8"):
                return ClusterMode.regular

    # Then, check if the current database was bootstrapped in single-db mode.
    has_instdata = await ctx.conn.sql_fetch_val(
        trampoline.fixup_query('''
            SELECT
                tablename
            FROM
                pg_catalog.pg_tables
            WHERE
                schemaname = 'edgedbinstdata_VER'
                AND tablename = 'instdata'
        ''').encode('utf-8'),
    )
    if has_instdata:
        return ClusterMode.single_database

    # At last, check for single-role-bootstrapped instance by trying to find
    # the Gel System DB with the assumption that we are not running in
    # single-db mode. If not found, this is a pristine backend cluster.
    if is_default_tenant:
        result = await ctx.conn.sql_fetch_col(
            b'''
                SELECT datname
                FROM pg_database
                WHERE datname LIKE '%' || $1
            ''',
            args=(
                edbdef.EDGEDB_SYSTEM_DB.encode("utf-8"),
            ),
        )
    else:
        result = await ctx.conn.sql_fetch_col(
            b'''
                SELECT datname
                FROM pg_database
                WHERE datname = $1
            ''',
            args=(
                ctx.cluster.get_db_name(
                    edbdef.EDGEDB_SYSTEM_DB).encode("utf-8"),
            ),
        )

    if result:
        if not ignore_others:
            # Either our tenant slot is occupied, or there is
            # a default tenant present.
            return ClusterMode.single_role

        # We were explicitly asked to ignore the other default tenant,
        # so check specifically if our tenant slot is occupied and ignore
        # the others.
        # This mode is used for in-place upgrade.
        for dbname in result:
            other_tenant_id = dbname[: -(len(edbdef.EDGEDB_SYSTEM_DB) + 1)]
            if other_tenant_id == tenant_id.encode("utf-8"):
                return ClusterMode.single_role

    return ClusterMode.pristine


async def _create_edgedb_template_database(
    ctx: BootstrapContext,
) -> uuid.UUID:
    backend_params = ctx.cluster.get_runtime_params()
    have_c_utf8 = backend_params.has_c_utf8_locale

    logger.info('Creating template database...')
    block = dbops.SQLBlock()
    dbid = uuidgen.uuid1mc()
    db = dbops.Database(
        ctx.cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB),
        owner=ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERUSER),
        is_template=True,
        lc_collate='C',
        lc_ctype='C.UTF-8' if have_c_utf8 else 'en_US.UTF-8',
        encoding='UTF8',
        metadata=dict(
            id=str(dbid),
            tenant_id=backend_params.tenant_id,
            name=edbdef.EDGEDB_TEMPLATE_DB,
            builtin=True,
        ),
    )

    dbops.CreateDatabase(db, template='template0').generate(block)
    await _execute_block(ctx.conn, block)
    return dbid


async def _store_static_bin_cache_conn(
    conn: metaschema.PGConnection,
    key: str,
    data: bytes,
) -> None:

    text = trampoline.fixup_query(f"""\
        INSERT INTO edgedbinstdata_VER.instdata (key, bin)
        VALUES(
            {pg_common.quote_literal(key)},
            {pg_common.quote_bytea_literal(data)}
        )
    """)

    await _execute(conn, text)


async def _store_static_bin_cache(
    ctx: BootstrapContext,
    key: str,
    data: bytes,
) -> None:
    await _store_static_bin_cache_conn(ctx.conn, key, data)


async def _store_static_text_cache(
    ctx: BootstrapContext,
    key: str,
    data: str,
) -> None:

    text = trampoline.fixup_query(f"""\
        INSERT INTO edgedbinstdata_VER.instdata (key, text)
        VALUES(
            {pg_common.quote_literal(key)},
            {pg_common.quote_literal(data)}::text
        )
    """)

    await _execute(ctx.conn, text)


async def _store_static_json_cache(
    ctx: BootstrapContext,
    key: str,
    data: str,
) -> None:

    text = trampoline.fixup_query(f"""\
        INSERT INTO edgedbinstdata_VER.instdata (key, json)
        VALUES(
            {pg_common.quote_literal(key)},
            {pg_common.quote_literal(data)}::jsonb
        )
    """)

    await _execute(ctx.conn, text)


def _process_delta_params(
    delta, schema: s_schema.Schema, params,
    stdmode: bool=True,
    **kwargs,
) -> tuple[
    s_schema.ChainedSchema,
    delta_cmds.MetaCommand,
    delta_cmds.CreateTrampolines,
]:
    """Adapt and process the delta command."""

    if debug.flags.delta_plan:
        debug.header('Delta Plan')
        debug.dump(delta, schema=schema)

    context = sd.CommandContext(stdmode=True)

    if not delta.canonical:
        # Canonicalize
        sd.apply(delta, schema=schema)

    delta = delta_cmds.CommandMeta.adapt(delta)
    context = sd.CommandContext(
        stdmode=stdmode,
        backend_runtime_params=params,
        **kwargs,
    )
    schema = sd.apply(delta, schema=schema, context=context)

    if debug.flags.delta_pgsql_plan:
        debug.header('PgSQL Delta Plan')
        debug.dump(delta, schema=schema)

    assert isinstance(schema, s_schema.ChainedSchema)
    if isinstance(delta, delta_cmds.DeltaRoot):
        out = delta.create_trampolines
    else:
        out = delta_cmds.CreateTrampolines()

    return schema, delta, out


def _process_delta(ctx, delta, schema) -> tuple[
    s_schema.ChainedSchema,
    delta_cmds.MetaCommand,
    delta_cmds.CreateTrampolines,
]:
    """Adapt and process the delta command."""
    return _process_delta_params(
        delta, schema, ctx.cluster.get_runtime_params()
    )


def compile_bootstrap_script(
    compiler: edbcompiler.Compiler,
    schema: s_schema.Schema,
    eql: str,
    *,
    bootstrap_mode: bool = True,
    expected_cardinality_one: bool = False,
    output_format: edbcompiler.OutputFormat = edbcompiler.OutputFormat.JSON,
) -> Tuple[s_schema.Schema, str]:

    ctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        user_schema=schema,
        expected_cardinality_one=expected_cardinality_one,
        json_parameters=True,
        output_format=output_format,
        bootstrap_mode=bootstrap_mode,
        log_ddl_as_migrations=False,
    )

    return edbcompiler.compile_edgeql_script(ctx, eql)


def compile_single_query(
    eql: str,
    compilerctx: edbcompiler.CompileContext,
) -> str:
    ql_source = edgeql.Source.from_string(eql)
    units = edbcompiler.compile(ctx=compilerctx, source=ql_source).units
    assert len(units) == 1
    return units[0].sql.decode()


def _get_all_subcommands(
    cmd: sd.Command, type: Optional[Type[sd.Command]] = None
) -> list[sd.Command]:
    cmds = []

    def go(cmd):
        if not type or isinstance(cmd, type):
            cmds.append(cmd)
        for sub in cmd.get_subcommands():
            go(sub)

    go(cmd)
    return cmds


def _get_schema_object_ids(
    delta: sd.Command,
) -> Mapping[Tuple[sn.Name, Optional[str]], uuid.UUID]:
    schema_object_ids = {}
    for cmd in _get_all_subcommands(delta, sd.CreateObject):
        assert isinstance(cmd, sd.CreateObject)
        mcls = cmd.get_schema_metaclass()
        if issubclass(mcls, s_obj.QualifiedObject):
            qlclass = None
        else:
            qlclass = mcls.get_ql_class_or_die()

        id = cmd.get_attribute_value('id')
        schema_object_ids[cmd.classname, qlclass] = id

        # backend_name in callables is a lot *like* an id, in that it gets
        # randomly generated and needs to match between things.
        if isinstance(cmd, s_func.CreateCallableObject):
            backend_name = cmd.get_attribute_value('backend_name')
            if backend_name:
                schema_object_ids[
                    cmd.classname, f'{qlclass}-backend_name'] = backend_name

    return schema_object_ids


def prepare_repair_patch(
    stdschema: s_schema.Schema,
    reflschema: s_schema.Schema,
    userschema: s_schema.Schema,
    globalschema: s_schema.Schema,
    schema_class_layout: s_refl.SchemaClassLayout,
    backend_params: params.BackendRuntimeParams,
    config: Any,
) -> bytes:
    compiler = edbcompiler.new_compiler(
        std_schema=stdschema,
        reflection_schema=reflschema,
        schema_class_layout=schema_class_layout
    )

    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        global_schema=globalschema,
        user_schema=userschema,
    )
    res = edbcompiler.repair_schema(compilerctx)
    if not res:
        return b""
    sql, _, _ = res

    return sql


PatchEntry = tuple[tuple[str, ...], tuple[str, ...], dict[str, Any], bool]


async def gather_patch_info(
    num: int,
    kind: str,
    patch: str,
    conn: pgcon.PGConnection,
) -> Optional[dict[str, list[str]]]:
    """Fetch info for a patch that needs to use the connection.

    Currently, the only thing we need is, for config updates, the
    order that columns appear in the config views in SQL. We need this
    because we need to preserve that order when we update the
    view.
    """

    if '+config' in kind:
        # Find all the config views (they are pg_classes where
        # there is also a table with the same name but "_dummy"
        # at the end) and collect all their columns in order.
        return json.loads(await conn.sql_fetch_val('''\
            select json_object_agg(v.relname, (
                select json_agg(a.attname order by a.attnum)
                from pg_catalog.pg_attribute as a
                where v.oid = a.attrelid
            ))
            from pg_catalog.pg_class as v
            inner join pg_catalog.pg_tables as t
            on v.relname || '_dummy' = t.tablename

        '''.encode('utf-8')))
    else:
        return None


def _generate_drop_views(
    group: dbops.CommandGroup,
    preblock: dbops.PLBlock,
) -> None:
    for cv in reversed(list(group)):
        dv: Any
        if isinstance(cv, dbops.CreateView):
            # We try deleting both a MATERIALIZED and not materialized
            # version, since that allows us to switch between them
            # more easily.
            dv = dbops.CommandGroup()
            dv.add_command(dbops.DropView(
                cv.view.name,
                conditions=[dbops.ViewExists(cv.view.name)],
            ))
            dv.add_command(dbops.DropView(
                cv.view.name,
                conditions=[dbops.ViewExists(cv.view.name, materialized=True)],
                materialized=True,
            ))
        elif isinstance(cv, dbops.CreateFunction):
            dv = dbops.DropFunction(
                cv.function.name,
                args=cv.function.args or (),
                has_variadic=bool(cv.function.has_variadic),
                if_exists=True,
            )
        else:
            raise AssertionError(f'unsupported support view command {cv}')
        dv.generate(preblock)


def prepare_patch(
    num: int,
    kind: str,
    patch: str,
    schema: s_schema.Schema,
    reflschema: s_schema.Schema,
    schema_class_layout: s_refl.SchemaClassLayout,
    backend_params: params.BackendRuntimeParams,
    patch_info: Optional[dict[str, list[str]]],
    user_schema: Optional[s_schema.Schema]=None,
    global_schema: Optional[s_schema.Schema]=None,
) -> PatchEntry:
    val = f'{pg_common.quote_literal(json.dumps(num + 1))}::jsonb'
    # TODO: This is an INSERT because 2.0 shipped without num_patches.
    # We can just make this an UPDATE for 3.0
    update = trampoline.fixup_query(f"""\
        INSERT INTO edgedbinstdata_VER.instdata (key, json)
        VALUES('num_patches', {val})
        ON CONFLICT (key)
        DO UPDATE SET json = {val};
    """)

    existing_view_columns = patch_info

    # Pure SQL patches are simple
    if kind == 'sql':
        return (patch, update), (), {}, False

    # metaschema-sql: just recreate a function from metaschema
    if kind == 'metaschema-sql':
        func = getattr(metaschema, patch)
        create = dbops.CreateFunction(func(), or_replace=True)
        block = dbops.PLTopBlock()
        create.generate(block)
        return (block.to_string(), update), (), {}, False

    if kind == 'repair':
        assert not patch
        return (update,), (), {}, True

    # EdgeQL and reflection schema patches need to be compiled.
    current_block = dbops.PLTopBlock()
    preblock = current_block.add_block()
    subblock = current_block.add_block()

    std_plans = []

    updates: dict[str, Any] = {}

    global_schema_update = kind == 'ext-pkg'
    sys_update_only = global_schema_update or kind.endswith('+globalonly')

    if kind == 'ext-pkg':
        # N.B: We process this without actually having the global
        # schema present, so we don't do any check for if it already
        # exists. The backend code will overwrite an older version's
        # JSON in the global metadata if it was already present.
        patch = s_std.get_std_module_text(sn.UnqualName(f'ext/{patch}'))

    if (
        kind == 'edgeql'
        or kind == 'ext-pkg'
        or kind.startswith('edgeql+schema')
    ):
        assert '+user_ext' not in kind

        for ddl_cmd in edgeql.parse_block(patch):
            assert isinstance(ddl_cmd, qlast.DDLCommand)
            # First apply it to the regular schema, just so we can update
            # stdschema
            delta_command = s_ddl.delta_from_ddl(
                ddl_cmd, modaliases={}, schema=schema, stdmode=True)
            schema, _, _ = _process_delta_params(
                delta_command, schema, backend_params)

            # We need to extract all ids of new objects created when
            # applying it to the regular schema, so that we can make sure
            # to use the same ids in the reflschema.
            schema_object_ids = _get_schema_object_ids(delta_command)

            # Then apply it to the reflschema, which we will use to drive
            # the actual table updating.
            delta_command = s_ddl.delta_from_ddl(
                ddl_cmd, modaliases={}, schema=reflschema,
                schema_object_ids=schema_object_ids, stdmode=True)
            reflschema, plan, tplan = _process_delta_params(
                delta_command, reflschema, backend_params)
            std_plans.append(delta_command)
            plan.generate(subblock)
            tplan.generate(subblock)

        metadata_user_schema = reflschema

    elif kind.startswith('edgeql+user_ext'):
        assert '+schema' not in kind

        # There isn't anything to do on the system database for
        # userext updates.
        if user_schema is None:
            return (update,), (), dict(is_user_ext_update=True), False

        # Only run a userext update if the extension we are trying to
        # update is installed.
        extension_name = kind.split('|')[-1]
        extension = user_schema.get_global(
            s_exts.Extension, extension_name, default=None)

        if not extension:
            return (update,), (), {}, False

        assert global_schema
        cschema = s_schema.ChainedSchema(
            schema,
            user_schema,
            global_schema,
        )

        for ddl_cmd in edgeql.parse_block(patch):
            assert isinstance(ddl_cmd, qlast.DDLCommand)

            delta_command = s_ddl.delta_from_ddl(
                ddl_cmd, modaliases={}, schema=cschema,
                stdmode=False,
                testmode=True,
            )
            cschema, plan, tplan = _process_delta_params(
                delta_command, cschema, backend_params)
            std_plans.append(delta_command)
            plan.generate(subblock)
            tplan.generate(subblock)

        if '+config' in kind:
            views = metaschema.get_config_views(cschema, existing_view_columns)
            views.generate(subblock)

        metadata_user_schema = cschema.get_top_schema()

    elif kind == 'sql-introspection':
        support_view_commands = dbops.CommandGroup()
        support_view_commands.add_commands(
            metaschema._generate_sql_information_schema(
                backend_params.instance_params.version
            )
        )
        support_view_commands.generate(subblock)

        _generate_drop_views(support_view_commands, preblock)

        metadata_user_schema = reflschema

    else:
        raise AssertionError(f'unknown patch type {kind}')

    if kind.startswith('edgeql+schema'):
        # If we are modifying the schema layout, we need to rerun
        # generate_structure to collect schema changes not reflected
        # in the public schema and to discover the new introspection
        # query.
        reflection = s_refl.generate_structure(
            reflschema, make_funcs=False,
        )

        reflschema, plan, tplan = _process_delta_params(
            reflection.intro_schema_delta, reflschema, backend_params)
        plan.generate(subblock)
        tplan.generate(subblock)

        compiler = edbcompiler.new_compiler(
            std_schema=schema,
            reflection_schema=reflschema,
            schema_class_layout=schema_class_layout
        )

        local_intro_sql, global_intro_sql = compile_intro_queries_stdlib(
            compiler=compiler,
            user_schema=reflschema,
            reflection=reflection,
        )

        updates.update(dict(
            classlayout=reflection.class_layout,
            local_intro_query=local_intro_sql.encode('utf-8'),
            global_intro_query=global_intro_sql.encode('utf-8'),
        ))

        # This part is wildly hinky
        # We need to delete all the support views and recreate them at the end
        support_view_commands = dbops.CommandGroup()
        support_view_commands.add_commands([
            dbops.CreateView(view)
            for view in metaschema._generate_schema_alias_views(
                reflschema, sn.UnqualName('schema')
            ) + metaschema._generate_schema_alias_views(
                reflschema, sn.UnqualName('sys')
            )
        ])
        support_view_commands.add_commands(
            metaschema._generate_sql_information_schema(
                backend_params.instance_params.version
            )
        )

        _generate_drop_views(support_view_commands, preblock)

        # We want to limit how much unconditional work we do, so only recreate
        # extension views if requested.
        if '+exts' in kind:
            for extview in metaschema._generate_extension_views(reflschema):
                support_view_commands.add_command(
                    dbops.CreateView(extview, or_replace=True))

        # Similarly, only do config system updates if requested.
        if '+config' in kind:
            support_view_commands.add_command(
                metaschema.get_config_views(schema, existing_view_columns))

        # Though we always update the instdata for the config system,
        # because it is currently the most convenient way to make sure
        # all the versioned fields get updated.
        config_spec = config.load_spec_from_schema(schema)
        (
            sysqueries,
            report_configs_typedesc_1_0,
            report_configs_typedesc_2_0,
        ) = compile_sys_queries(
            reflschema,
            compiler,
            config_spec,
        )
        updates.update(dict(
            sysqueries=json.dumps(sysqueries).encode('utf-8'),
            report_configs_typedesc_1_0=report_configs_typedesc_1_0,
            report_configs_typedesc_2_0=report_configs_typedesc_2_0,
            configspec=config.spec_to_json(config_spec).encode('utf-8'),
        ))

        support_view_commands.generate(subblock)

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=reflschema,
        schema_class_layout=schema_class_layout
    )

    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        user_schema=metadata_user_schema,
        bootstrap_mode=user_schema is None,
    )

    for std_plan in std_plans:
        edbcompiler.compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=std_plan,
            block=subblock,
        )

    patch = current_block.to_string()

    if debug.flags.delta_execute:
        debug.header('Patch Script')
        debug.dump_code(patch, lexer='sql')

    if not global_schema_update:
        updates.update(dict(
            std_and_reflection_schema=(schema, reflschema),
        ))

    bins = (
        'std_and_reflection_schema', 'global_schema', 'classlayout',
        'report_configs_typedesc_1_0', 'report_configs_typedesc_2_0',
    )
    rawbin = (
        'report_configs_typedesc_1_0', 'report_configs_typedesc_2_0',
    )
    jsons = (
        'sysqueries', 'configspec',
    )
    # This is unversioned because it is consumed by a function in metaschema.
    # (And only by a function in metaschema.)
    unversioned = (
        'configspec',
    )
    # Just for the system database, we need to update the cached pickle
    # of everything.
    version_key = patches.get_version_key(num + 1)
    sys_updates: tuple[str, ...] = ()

    spatches: tuple[str, ...] = (patch,)
    for k, v in updates.items():
        key = f"'{k}{version_key}'" if k not in unversioned else f"'{k}'"
        if k in bins:
            if k not in rawbin:
                v = pickle.dumps(v, protocol=pickle.HIGHEST_PROTOCOL)
            val = f'{pg_common.quote_bytea_literal(v)}'
            sys_updates += (trampoline.fixup_query(f'''
                INSERT INTO edgedbinstdata_VER.instdata (key, bin)
                VALUES({key}, {val})
                ON CONFLICT (key)
                DO UPDATE SET bin = {val};
            '''),)
        else:
            typ, col = ('jsonb', 'json') if k in jsons else ('text', 'text')
            val = f'{pg_common.quote_literal(v.decode("utf-8"))}::{typ}'
            sys_updates += (trampoline.fixup_query(f'''
                INSERT INTO edgedbinstdata_VER.instdata (key, {col})
                VALUES({key}, {val})
                ON CONFLICT (key)
                DO UPDATE SET {col} = {val};
            '''),)
        if k in unversioned:
            spatches += (sys_updates[-1],)

    # If we're updating the global schema (for extension packages,
    # perhaps), only run the script once, on the system connection.
    # Since the state is global, we only should update it once.
    regular_updates: tuple[str, ...]
    if sys_update_only:
        regular_updates = (update,)
        sys_updates = (patch,) + sys_updates
    else:
        regular_updates = spatches + (update,)
        # FIXME: This is a hack to make the is_user_ext_update cases
        # work (by ensuring we can always read their current state),
        # but this is actually a pretty dumb approach and we can do
        # better.
        regular_updates += sys_updates

    return regular_updates, sys_updates, updates, False


async def create_branch(
    cluster: pgcluster.BaseCluster,
    schema: s_schema.Schema,
    conn: metaschema.PGConnection,
    src_dbname: str,
    tgt_dbname: str,
    mode: str,
    backend_id_fixup_sql: bytes,
) -> None:
    """Create a new database (branch) based on an existing one."""

    # Dump the edgedbpub schema that holds user data and any extensions.
    schema_dump = await cluster.dump_database(
        src_dbname,
        include_schemas=('edgedbpub',),
        include_extensions=('*',),
        schema_only=True,
    )

    # Tuples types are always kept in edgedbpub, but some already
    # exist from the std schema, so we need to skip those. We also
    # need to skip recreating the schema. This requires doing some
    # annoying postprocessing.
    to_skip = [
        str(obj.id) for obj in schema.get_objects(type=s_types.Tuple)
    ]
    old_lines = schema_dump.decode('utf-8').split('\n')
    new_lines = []
    skipping = False
    for line in old_lines:
        if line == ');' and skipping:
            skipping = False
            continue
        elif line.startswith('CREATE SCHEMA'):
            continue
        elif line.startswith('CREATE TYPE'):
            if any(skip in line for skip in to_skip):
                skipping = True
        elif line == 'SET transaction_timeout = 0;':
            continue

        if skipping:
            continue
        new_lines.append(line)
    s_schema_dump = '\n'.join(new_lines)

    await conn.sql_execute(s_schema_dump.encode('utf-8'))

    # Copy database config variables over directly
    copy_cfg_query = f'''
        select edgedb._copy_database_configs(
            {pg_common.quote_literal(src_dbname)})
    '''.encode('utf-8')
    await conn.sql_execute(copy_cfg_query)

    # HACK: Empty out all schema multi property tables. This is
    # because the original template has the stdschema in it, and so we
    # use --on-conflict-do-nothing to avoid conflicts since the dump
    # will have that in it too. That works, except for multi properties
    # where it won't conflict, and modules, which might have a different
    # 'default' module on each side. (Since it isn't in the stdschema,
    # and could have an old id persisted from an in-place upgrade.)
    to_delete: set[s_obj.Object] = {
        prop for prop in schema.get_objects(type=s_props.Property)
        if prop.get_cardinality(schema).is_multi()
        and prop.get_name(schema).module not in irtyputils.VIEW_MODULES
    }
    to_delete.add(schema.get('schema::Module'))

    for target in to_delete:
        name = pg_common.get_backend_name(schema, target, catenate=True)
        await conn.sql_execute(f'delete from {name}'.encode('utf-8'))

    await conn.sql_execute(trampoline.fixup_query(f'''
        delete from edgedbinstdata_VER.instdata where key = 'configspec_ext'
    ''').encode('utf-8'))

    # Do the dump/restore for the data. We always need to copy over
    # edgedbstd, since it has the reflected schema. We copy over
    # edgedbpub when it is a data branch.
    data_arg = ['--table=edgedbpub.*'] if mode == qlast.BranchType.DATA else []
    dump_args = [
        '--data-only',
        '--table=edgedbstd.*',
        f'--table={pg_common.versioned_schema("edgedbstd")}.*',
        '--table=edgedb._db_config',
        f'--table={pg_common.versioned_schema("edgedbinstdata")}.instdata',
        *data_arg,
        '--disable-triggers',
        # We need to use --inserts so that we can use --on-conflict-do-nothing.
        # (See above, in discussion of the HACK.)
        '--inserts',
        '--rows-per-insert=100',
        '--on-conflict-do-nothing',
    ]
    await cluster._copy_database(
        src_dbname, tgt_dbname, dump_args, [],
    )

    # Restore the search_path as the dump might have altered it.
    await conn.sql_execute(
        b"SELECT pg_catalog.set_config('search_path', 'edgedb', false)")

    # Fixup the backend ids in the schema to match what is actually in pg.
    await conn.sql_execute(backend_id_fixup_sql)


class StdlibBits(NamedTuple):

    #: User-visible std.
    stdschema: s_schema.Schema
    #: Shadow extended schema for reflection..
    reflschema: s_schema.Schema
    #: Standard portion of the global schema
    global_schema: s_schema.Schema
    #: SQL text of the procedure to initialize `std` in Postgres.
    sqltext: str
    #: Descriptors of all the needed trampolines
    trampolines: list[trampoline.Trampoline]
    #: A set of ids of all types in std.
    types: Set[uuid.UUID]
    #: Schema class reflection layout.
    classlayout: Dict[Type[s_obj.Object], s_refl.SchemaTypeLayout]
    #: Schema introspection SQL query.
    local_intro_query: str
    #: Global object introspection SQL query.
    global_intro_query: str
    #: Number of patches already baked into the stdlib.
    num_patches: int
    # TODO: All of sysqueries ought to go here, right?
    # It would speed up instance creation a bit at little cost.
    # (Oh, maybe testmode screws this idea up?)


def _make_stdlib(
    ctx: BootstrapContext,
    testmode: bool,
    global_ids: Mapping[str, uuid.UUID],
) -> StdlibBits:
    schema: s_schema.Schema = s_schema.ChainedSchema(
        s_schema.EMPTY_SCHEMA,
        s_schema.EMPTY_SCHEMA,
        s_schema.EMPTY_SCHEMA,
    )
    for special_mod in s_schema.SPECIAL_MODULES:
        schema, _ = s_mod.Module.create_in_schema(
            schema,
            name=special_mod,
            stable_ids=True,
        )

    current_block = dbops.PLTopBlock()

    trampolines = []

    std_texts = []
    for modname in s_schema.STD_SOURCES:
        std_texts.append(s_std.get_std_module_text(modname))

    if testmode:
        for modname in s_schema.TESTMODE_SOURCES:
            std_texts.append(s_std.get_std_module_text(modname))

    ddl_text = '\n'.join(std_texts)
    types: Set[uuid.UUID] = set()
    std_plans: List[sd.Command] = []

    for ddl_cmd in edgeql.parse_block(ddl_text):
        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan, tplan = _process_delta(ctx, delta_command, schema)
        assert isinstance(plan, delta_cmds.DeltaRoot)
        std_plans.append(delta_command)

        types.update(plan.new_types)
        plan.generate(current_block)
        trampolines.extend(tplan.trampolines)

    _, schema_version = s_std.make_schema_version(schema)
    schema, plan, tplan = _process_delta(ctx, schema_version, schema)
    std_plans.append(schema_version)
    plan.generate(current_block)
    trampolines.extend(tplan.trampolines)

    stdglobals = '\n'.join([
        f'''CREATE SUPERUSER ROLE {edbdef.EDGEDB_SUPERUSER} {{
            SET id := <uuid>'{global_ids[edbdef.EDGEDB_SUPERUSER]}'
        }};''',
    ])

    schema = _execute_edgeql_ddl(schema, stdglobals)

    _, global_schema_version = s_std.make_global_schema_version(schema)
    schema, plan, tplan = _process_delta(ctx, global_schema_version, schema)
    std_plans.append(global_schema_version)
    plan.generate(current_block)
    trampolines.extend(tplan.trampolines)

    reflection = s_refl.generate_structure(schema)
    reflschema, reflplan, treflplan = _process_delta(
        ctx, reflection.intro_schema_delta, schema)

    # Any collection types that made it into reflschema need to get
    # to get pulled back into the stdschema, or else they will be in
    # an inconsistent state.
    for obj in reflschema.get_objects(type=s_types.Collection):
        if not schema.has_object(obj.id):
            delta = sd.DeltaRoot()
            delta.add(obj.as_shell(reflschema).as_create_delta(reflschema))
            schema = delta.apply(schema, sd.CommandContext(stdmode=True))
    assert isinstance(schema, s_schema.ChainedSchema)

    assert current_block is not None
    reflplan.generate(current_block)
    trampolines.extend(treflplan.trampolines)
    subblock = current_block.add_block()

    compiler = edbcompiler.new_compiler(
        std_schema=schema.get_top_schema(),
        reflection_schema=reflschema.get_top_schema(),
        schema_class_layout=reflection.class_layout,  # type: ignore
    )

    backend_runtime_params = ctx.cluster.get_runtime_params()
    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        user_schema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        bootstrap_mode=True,
        backend_runtime_params=backend_runtime_params,
    )

    for std_plan in std_plans:
        edbcompiler.compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=std_plan,
            block=subblock,
        )

    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        user_schema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        bootstrap_mode=True,
        internal_schema_mode=True,
        backend_runtime_params=backend_runtime_params,
    )
    edbcompiler.compile_schema_storage_in_delta(
        ctx=compilerctx,
        delta=reflection.intro_schema_delta,
        block=subblock,
    )

    sqltext = current_block.to_string()

    local_intro_sql, global_intro_sql = compile_intro_queries_stdlib(
        compiler=compiler,
        user_schema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        reflection=reflection,
    )

    return StdlibBits(
        stdschema=schema.get_top_schema(),
        reflschema=reflschema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        sqltext=sqltext,
        trampolines=trampolines,
        types=types,
        classlayout=reflection.class_layout,
        local_intro_query=local_intro_sql,
        global_intro_query=global_intro_sql,
        num_patches=len(patches.PATCHES),
    )


async def _amend_stdlib(
    ctx: BootstrapContext,
    ddl_text: str,
    stdlib: StdlibBits,
) -> Tuple[StdlibBits, str, list[trampoline.Trampoline]]:
    schema = s_schema.ChainedSchema(
        s_schema.EMPTY_SCHEMA,
        stdlib.stdschema,
        stdlib.global_schema,
    )
    reflschema = stdlib.reflschema

    topblock = dbops.PLTopBlock()
    plans = []
    trampolines = []

    context = sd.CommandContext(stdmode=True)

    for ddl_cmd in edgeql.parse_block(ddl_text):
        assert isinstance(ddl_cmd, qlast.DDLCommand)
        delta_command = s_ddl.delta_from_ddl(
            ddl_cmd, modaliases={}, schema=schema, stdmode=True)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan, tplan = _process_delta(ctx, delta_command, schema)
        reflschema = delta_command.apply(reflschema, context)
        plan.generate(topblock)
        plans.append(plan)
        trampolines.extend(tplan.trampolines)

    compiler = edbcompiler.new_compiler(
        std_schema=schema.get_top_schema(),
        reflection_schema=reflschema,
        schema_class_layout=stdlib.classlayout,  # type: ignore
    )

    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        user_schema=schema.get_top_schema(),
        global_schema=schema.get_global_schema(),
    )
    for plan in plans:
        edbcompiler.compile_schema_storage_in_delta(
            ctx=compilerctx,
            delta=plan,
            block=topblock,
        )

    sqltext = topblock.to_string()

    return stdlib._replace(
        stdschema=schema.get_top_schema(),
        global_schema=schema.get_global_schema(),
        reflschema=reflschema,
    ), sqltext, trampolines


def compile_intro_queries_stdlib(
    *,
    compiler: edbcompiler.Compiler,
    user_schema: s_schema.Schema,
    global_schema: s_schema.Schema=s_schema.EMPTY_SCHEMA,
    reflection: s_refl.SchemaReflectionParts,
) -> Tuple[str, str]:
    compilerctx = edbcompiler.new_compiler_context(
        compiler_state=compiler.state,
        user_schema=user_schema,
        global_schema=global_schema,
        schema_reflection_mode=True,
        output_format=edbcompiler.OutputFormat.JSON_ELEMENTS,
    )

    # The introspection query bits are returned in chunks
    # because it's a large UNION and we currently generate SQL
    # that is much harder for Postgres to plan as opposed to a
    # straight flat UNION.
    sql_intro_local_parts = []
    sql_intro_global_parts = []
    for intropart in reflection.local_intro_parts:
        sql_intro_local_parts.append(
            compile_single_query(
                intropart,
                compilerctx=compilerctx,
            ),
        )

    for intropart in reflection.global_intro_parts:
        sql_intro_global_parts.append(
            compile_single_query(
                intropart,
                compilerctx=compilerctx,
            ),
        )

    local_intro_sql = ' UNION ALL '.join(
        f'({x})' for x in sql_intro_local_parts)
    local_intro_sql = f'''
        WITH intro(c) AS ({local_intro_sql})
        SELECT json_agg(intro.c) FROM intro
    '''

    global_intro_sql = ' UNION ALL '.join(
        f'({x})' for x in sql_intro_global_parts)
    global_intro_sql = f'''
        WITH intro(c) AS ({global_intro_sql})
        SELECT json_agg(intro.c) FROM intro
    '''

    return local_intro_sql, global_intro_sql


def _calculate_src_hash() -> bytes:
    return buildmeta.hash_dirs(
        buildmeta.get_cache_src_dirs(),
        extra_files=[
            __file__,
            pathlib.Path(__file__).parent.parent / 'buildmeta.py',
        ],
    )


def _get_cache_dir() -> pathlib.Path | None:
    if specified_cache_dir := os.environ.get('_EDGEDB_WRITE_DATA_CACHE_TO'):
        return pathlib.Path(specified_cache_dir)
    else:
        return None


def read_data_cache(
    file_name: str,
    pickled: bool,
    *,
    src_hash: bytes | None = None,
    cache_dir: pathlib.Path | None = None,
) -> Any:
    if src_hash is None:
        src_hash = _calculate_src_hash()
    if cache_dir is None:
        cache_dir = _get_cache_dir()
    return buildmeta.read_data_cache(
        src_hash, file_name, source_dir=cache_dir, pickled=pickled)


def cleanup_tpldbdump(tpldbdump: bytes) -> bytes:
    # Excluding the "edgedbext" schema above apparently
    # doesn't apply to extensions created in that schema,
    # so we have to resort to commenting out extension
    # statements in the dump.
    tpldbdump = re.sub(
        rb'^(CREATE|COMMENT ON) EXTENSION.*$',
        rb'-- \g<0>',
        tpldbdump,
        flags=re.MULTILINE,
    )

    # PostgreSQL 14 emits multirange_type_name in RANGE definitions,
    # elide these to preserve compatibility with earlier servers.
    tpldbdump = re.sub(
        rb',\s*multirange_type_name\s*=[^,\n]+',
        rb'',
        tpldbdump,
        flags=re.MULTILINE,
    )

    # PostgreSQL 17 adds a transaction_timeout config setting that
    # didn't exist on earlier versions.
    tpldbdump = re.sub(
        rb'^SET transaction_timeout = 0;$',
        rb'',
        tpldbdump,
        flags=re.MULTILINE,
    )

    return tpldbdump


async def _init_stdlib(
    ctx: BootstrapContext,
    testmode: bool,
    global_ids: Mapping[str, uuid.UUID],
) -> tuple[
    StdlibBits,
    config.Spec,
    edbcompiler.Compiler,
]:
    in_dev_mode = devmode.is_in_dev_mode()
    conn = ctx.conn
    cluster = ctx.cluster
    args = ctx.args

    tpldbdump_cache = 'backend-tpldbdump.sql'
    src_hash = _calculate_src_hash()
    cache_dir = _get_cache_dir()

    stdlib = read_data_cache(
        STDLIB_CACHE_FILE_NAME,
        pickled=True,
        src_hash=src_hash,
        cache_dir=cache_dir,
    )
    tpldbdump_package = read_data_cache(
        tpldbdump_cache,
        pickled=True,
        src_hash=src_hash,
        cache_dir=cache_dir,
    )
    if args.inplace_upgrade_prepare:
        tpldbdump = None

    tpldbdump, tpldbdump_inplace = None, None
    if tpldbdump_package:
        tpldbdump, tpldbdump_inplace = tpldbdump_package

    stdlib_was_none = stdlib is None
    if stdlib is None:
        logger.info('Compiling the standard library...')
        stdlib = _make_stdlib(
            ctx, in_dev_mode or testmode, global_ids)

    config_spec = config.load_spec_from_schema(stdlib.stdschema)

    # If we recompiled the stdlib or need to generate a tpldbdump, we
    # need to generate bootstrap commands and trampolines, and update
    # the stdlib's trampolines if we compiled it.
    bootstrap_commands = None
    if stdlib_was_none or tpldbdump is None:
        bootstrap_commands, bootstrap_trampolines = (
            metaschema.get_bootstrap_commands(config_spec)
        )
        if stdlib_was_none:
            stdlib = stdlib._replace(
                trampolines=bootstrap_trampolines + stdlib.trampolines
            )

    trampolines = []

    # We need to set this up early, since later code depends on the
    # backend_instance_params of the instdata table. But it also
    # obviously can't go into the tpldbdump, since it is dynamic.
    trampolines.extend(await metaschema.generate_instdata_table(
        conn,
    ))
    await _populate_misc_instance_data(ctx)

    backend_params = cluster.get_runtime_params()
    if not args.inplace_upgrade_prepare:
        logger.info('Creating the necessary PostgreSQL extensions...')
        await metaschema.create_pg_extensions(conn, backend_params)

    trampolines.extend(stdlib.trampolines)

    eff_tpldbdump = (
        tpldbdump_inplace if args.inplace_upgrade_prepare else tpldbdump)
    if eff_tpldbdump is None:
        logger.info('Populating internal SQL structures...')
        assert bootstrap_commands is not None
        block = dbops.PLTopBlock()

        if not args.inplace_upgrade_prepare:
            fixed_bootstrap_commands = metaschema.get_fixed_bootstrap_commands()
            fixed_bootstrap_commands.generate(block)

        bootstrap_commands.generate(block)
        await _execute_block(conn, block)
        logger.info('Executing the standard library...')
        await _execute(conn, stdlib.sqltext)

        if in_dev_mode or cache_dir:
            tpl_db_name = edbdef.EDGEDB_TEMPLATE_DB
            tpl_pg_db_name = cluster.get_db_name(tpl_db_name)
            tpldbdump = await cluster.dump_database(
                tpl_pg_db_name,
                exclude_schemas=[
                    pg_common.versioned_schema('edgedbinstdata'),
                    'edgedbext',
                    backend_params.instance_params.ext_schema,
                ],
                dump_object_owners=False,
            )

            tpldbdump = cleanup_tpldbdump(tpldbdump)

            # The instance metadata doesn't go in the dump, so collect
            # it ourselves.
            global_metadata = await conn.sql_fetch_val(
                trampoline.fixup_query(
                    "SELECT edgedb_VER.get_database_metadata($1)::json"
                ).encode("utf-8"),
                args=[tpl_db_name.encode("utf-8")],
            )
            global_metadata = json.loads(global_metadata)

            pl_block = dbops.PLTopBlock()

            set_metadata_text = dbops.SetMetadata(
                dbops.DatabaseWithTenant(name=tpl_db_name),
                global_metadata,
            ).code_with_block(pl_block)

            set_single_db_metadata_text = dbops.SetSingleDBMetadata(
                edbdef.EDGEDB_TEMPLATE_DB, global_metadata
            ).code_with_block(pl_block)

            pl_block.add_command(textwrap.dedent(trampoline.fixup_query(f"""\
                IF (edgedb_VER.get_backend_capabilities()
                    & {int(params.BackendCapabilities.CREATE_DATABASE)}) != 0
                THEN
                {textwrap.indent(set_metadata_text, '    ')}
                ELSE
                {textwrap.indent(set_single_db_metadata_text, '    ')}
                END IF
                """)))

            text = pl_block.to_string()

            tpldbdump += b'\n' + text.encode('utf-8')

            # XXX: TODO: We are going to need to deal with tuple types
            # in edgedbpub...
            tpldbdump_inplace = await cluster.dump_database(
                tpl_pg_db_name,
                include_schemas=[
                    pg_common.versioned_schema('edgedb'),
                    pg_common.versioned_schema('edgedbstd'),
                    pg_common.versioned_schema('edgedbsql'),
                ],
                dump_object_owners=False,
            )
            tpldbdump_inplace = cleanup_tpldbdump(tpldbdump_inplace)

            # XXX: BE SMARTER ABOUT THIS, DON'T DO ALL THAT WORK
            if args.inplace_upgrade_prepare:
                tpldbdump = None

            buildmeta.write_data_cache(
                (tpldbdump, tpldbdump_inplace),
                src_hash,
                tpldbdump_cache,
                pickled=True,
                target_dir=cache_dir,
            )

            buildmeta.write_data_cache(
                stdlib,
                src_hash,
                STDLIB_CACHE_FILE_NAME,
                target_dir=cache_dir,
            )
    else:
        logger.info('Initializing the standard library...')
        await _execute(conn, eff_tpldbdump.decode('utf-8'))
        # Restore the search_path as the dump might have altered it.
        await conn.sql_execute(
            b"SELECT pg_catalog.set_config('search_path', 'edgedb', false)")

    if not in_dev_mode and testmode:
        # Running tests on a production build.
        for modname in s_schema.TESTMODE_SOURCES:
            stdlib, testmode_sql, new_trampolines = await _amend_stdlib(
                ctx,
                s_std.get_std_module_text(modname),
                stdlib,
            )
            await conn.sql_execute(testmode_sql.encode("utf-8"))
            trampolines.extend(new_trampolines)
        # _testmode includes extra config settings, so make sure
        # those are picked up.
        config_spec = config.load_spec_from_schema(stdlib.stdschema)

    logger.info('Finalizing database setup...')

    # Make sure that schema backend_id properties are in sync with
    # the database.
    # XXX: is ScalarType sufficient here?

    compiler = edbcompiler.new_compiler(
        std_schema=stdlib.stdschema,
        reflection_schema=stdlib.reflschema,
        schema_class_layout=stdlib.classlayout,
        global_intro_query=stdlib.global_intro_query,
        local_intro_query=stdlib.local_intro_query,
    )
    _, sql = compile_bootstrap_script(
        compiler,
        stdlib.reflschema,
        '''
        SELECT schema::ScalarType {
            id,
            backend_id,
        } FILTER .builtin AND NOT (.abstract ?? False);
        ''',
        expected_cardinality_one=False,
    )
    schema = stdlib.stdschema
    typemap = await conn.sql_fetch_val(sql.encode("utf-8"))
    for entry in json.loads(typemap):
        t = schema.get_by_id(uuidgen.UUID(entry['id']))
        schema = t.set_field_value(
            schema, 'backend_id', entry['backend_id'])

    # Patch functions referring to extensions, because
    # some backends require extensions to be hosted in
    # hardcoded schemas (e.g. Heroku)
    await metaschema.patch_pg_extensions(conn, backend_params)

    stdlib = stdlib._replace(stdschema=schema)
    version_key = patches.get_version_key(stdlib.num_patches)

    # stdschema and reflschema are combined in one pickle to preserve sharing
    await _store_static_bin_cache(
        ctx,
        f'std_and_reflection_schema{version_key}',
        pickle.dumps(
            (schema, stdlib.reflschema),
            protocol=pickle.HIGHEST_PROTOCOL,
        ),
    )

    await _store_static_bin_cache(
        ctx,
        f'global_schema{version_key}',
        pickle.dumps(stdlib.global_schema, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_bin_cache(
        ctx,
        f'classlayout{version_key}',
        pickle.dumps(stdlib.classlayout, protocol=pickle.HIGHEST_PROTOCOL),
    )

    await _store_static_text_cache(
        ctx,
        f'local_intro_query{version_key}',
        stdlib.local_intro_query,
    )

    await _store_static_text_cache(
        ctx,
        f'global_intro_query{version_key}',
        stdlib.global_intro_query,
    )

    trampolines.extend(await metaschema.generate_support_views(
        conn, stdlib.reflschema, cluster.get_runtime_params()
    ))
    trampolines.extend(
        await metaschema.generate_support_functions(conn, stdlib.reflschema)
    )

    compiler = edbcompiler.new_compiler(
        std_schema=schema,
        reflection_schema=stdlib.reflschema,
        schema_class_layout=stdlib.classlayout,
        global_intro_query=stdlib.global_intro_query,
        local_intro_query=stdlib.local_intro_query,
    )

    trampolines.extend(
        await metaschema.generate_more_support_functions(
            conn, compiler, stdlib.reflschema, testmode
        )
    )

    await _store_static_json_cache(
        ctx,
        'configspec',
        config.spec_to_json(config_spec),
    )
    await _store_static_json_cache(
        ctx,
        'configspec_ext',
        json.dumps({}),
    )

    # Create all the trampolines
    tramps = dbops.CommandGroup()
    tramps.add_commands([t.make() for t in trampolines])
    block = dbops.PLTopBlock()
    tramps.generate(block)

    if args.inplace_upgrade_prepare:
        trampoline_text = block.to_string()
        await _store_static_text_cache(
            ctx,
            f'trampoline_pivot_query',
            trampoline_text,
        )
    else:
        await _execute_block(conn, block)

    return stdlib, config_spec, compiler


async def _init_defaults(schema, compiler, conn):
    script = '''
        CREATE MODULE default;
    '''

    schema, sql = compile_bootstrap_script(
        compiler, schema, script, bootstrap_mode=False
    )
    await _execute(conn, sql)
    return schema


async def _configure(
    ctx: BootstrapContext,
    config_spec: config.Spec,
    schema: s_schema.Schema,
    compiler: edbcompiler.Compiler,
) -> None:
    settings: Mapping[str, config.SettingValue] = {}

    config_json = config.to_json(config_spec, settings, include_source=False)
    block = dbops.PLTopBlock()
    metadata = {'sysconfig': json.loads(config_json)}
    if ctx.cluster.get_runtime_params().has_create_database:
        dbops.UpdateMetadata(
            dbops.DatabaseWithTenant(name=edbdef.EDGEDB_SYSTEM_DB),
            metadata,
        ).generate(block)
    else:
        dbops.UpdateSingleDBMetadata(
            edbdef.EDGEDB_SYSTEM_DB, metadata,
        ).generate(block)

    await _execute_block(ctx.conn, block)

    backend_params = ctx.cluster.get_runtime_params()
    for setname in config_spec:
        setting = config_spec[setname]
        if (
            setting.backend_setting
            and setting.default is not None
            and (
                # Do not attempt to run CONFIGURE INSTANCE on
                # backends that don't support it.
                # TODO: this should be replaced by instance-wide
                #       emulation at backend connection time.
                backend_params.has_configfile_access
            )
        ):
            if isinstance(setting.default, statypes.Duration):
                val = f'<std::duration>"{setting.default.to_iso8601()}"'
            else:
                val = repr(setting.default)
            script = f'''
                CONFIGURE INSTANCE SET {setting.name} := {val};
            '''
            schema, sql = compile_bootstrap_script(compiler, schema, script)
            await _execute(ctx.conn, sql)


def compile_sys_queries(
    schema: s_schema.Schema,
    compiler: edbcompiler.Compiler,
    config_spec: config.Spec,
) -> tuple[dict[str, str], bytes, bytes]:
    queries = {}

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        'SELECT cfg::_get_config_json_internal()',
        expected_cardinality_one=True,
    )

    queries['config'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        "SELECT cfg::_get_config_json_internal(sources := ['database'])",
        expected_cardinality_one=True,
    )

    queries['dbconfig'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        """
        SELECT cfg::_get_config_json_internal(max_source := 'system override')
        """,
        expected_cardinality_one=True,
    )

    queries['sysconfig'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        """
        SELECT cfg::_get_config_json_internal(max_source := 'postgres client')
        """,
        expected_cardinality_one=True,
    )

    queries['sysconfig_default'] = sql

    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        f"""SELECT (
            SELECT sys::Branch
            FILTER .name != "{edbdef.EDGEDB_TEMPLATE_DB}"
        ).name""",
        expected_cardinality_one=False,
    )

    queries['listdbs'] = sql

    role_query = '''
        SELECT sys::Role {
            name,
            superuser,
            password,
        };
    '''
    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        role_query,
        expected_cardinality_one=False,
    )
    queries['roles'] = sql

    tids_query = '''
        SELECT schema::ScalarType {
            id,
            backend_id,
        } FILTER .id IN <uuid>json_array_unpack(<json>$ids);
    '''
    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        tids_query,
        expected_cardinality_one=False,
    )

    queries['backend_tids'] = sql

    # When we restore a database from a dump, OIDs for non-system
    # Postgres types might get skewed as they are not part of the dump.
    # A good example of that is `std::bigint` which is implemented as
    # a custom domain type. The OIDs are stored under
    # `schema::Object.backend_id` property and are injected into
    # array query arguments.
    #
    # The code below re-syncs backend_id properties of Gel builtin
    # types with the actual OIDs in the DB.
    backend_id_fixup_edgeql = '''
        UPDATE schema::ScalarType
        FILTER
            NOT (.abstract ?? False)
            AND NOT (.transient ?? False)
        SET {
            backend_id := sys::_get_pg_type_for_edgedb_type(
                .id,
                .__type__.name,
                <uuid>{},
                [is schema::ScalarType].sql_type ?? (
                    select [is schema::ScalarType]
                    .bases[is schema::ScalarType] limit 1
                ).sql_type,
            )
        };
        UPDATE schema::Tuple
        FILTER
            NOT (.abstract ?? False)
            AND NOT (.transient ?? False)
        SET {
            backend_id := sys::_get_pg_type_for_edgedb_type(
                .id,
                .__type__.name,
                <uuid>{},
                [is schema::ScalarType].sql_type ?? (
                    select [is schema::ScalarType]
                    .bases[is schema::ScalarType] limit 1
                ).sql_type,
            )
        };
        UPDATE {schema::Range, schema::MultiRange}
        FILTER
            NOT (.abstract ?? False)
            AND NOT (.transient ?? False)
        SET {
            backend_id := sys::_get_pg_type_for_edgedb_type(
                .id,
                .__type__.name,
                .element_type.id,
                <str>{},
            )
        };
        UPDATE schema::Array
        FILTER
            NOT (.abstract ?? False)
            AND NOT (.transient ?? False)
        SET {
            backend_id := sys::_get_pg_type_for_edgedb_type(
                .id,
                .__type__.name,
                .element_type.id,
                <str>{},
            )
        };
    '''
    _, sql = compile_bootstrap_script(
        compiler,
        schema,
        backend_id_fixup_edgeql,
    )
    queries['backend_id_fixup'] = sql

    report_settings: list[str] = []
    for setname in config_spec:
        setting = config_spec[setname]
        if setting.report:
            report_settings.append(setname)

    report_configs_query = f'''
        SELECT assert_single(cfg::Config {{
            {', '.join(report_settings)}
        }});
    '''

    units = edbcompiler.compile(
        ctx=edbcompiler.new_compiler_context(
            compiler_state=compiler.state,
            user_schema=schema,
            expected_cardinality_one=True,
            json_parameters=False,
            output_format=edbcompiler.OutputFormat.BINARY,
            bootstrap_mode=True,
        ),
        source=edgeql.Source.from_string(report_configs_query),
    ).units
    assert len(units) == 1

    report_configs_typedesc_2_0 = units[0].out_type_id + units[0].out_type_data
    queries['report_configs'] = units[0].sql.decode()

    units = edbcompiler.compile(
        ctx=edbcompiler.new_compiler_context(
            compiler_state=compiler.state,
            user_schema=schema,
            expected_cardinality_one=True,
            json_parameters=False,
            output_format=edbcompiler.OutputFormat.BINARY,
            bootstrap_mode=True,
            protocol_version=(1, 0),
        ),
        source=edgeql.Source.from_string(report_configs_query),
    ).units
    assert len(units) == 1
    report_configs_typedesc_1_0 = units[0].out_type_id + units[0].out_type_data

    return (
        queries,
        report_configs_typedesc_1_0,
        report_configs_typedesc_2_0,
    )


async def _populate_misc_instance_data(
    ctx: BootstrapContext,
) -> Dict[str, Any]:

    mock_auth_nonce = scram.generate_nonce()
    json_instance_data = {
        'version': dict(buildmeta.get_version_dict()),
        'catver': edbdef.EDGEDB_CATALOG_VERSION,
        'mock_auth_nonce': mock_auth_nonce,
    }

    await _store_static_json_cache(
        ctx,
        'instancedata',
        json.dumps(json_instance_data),
    )

    backend_params = ctx.cluster.get_runtime_params()
    instance_params = backend_params.instance_params
    await _store_static_json_cache(
        ctx,
        'backend_instance_params',
        json.dumps(instance_params._asdict()),
    )

    if not backend_params.has_create_role:
        json_single_role_metadata = {
            'id': str(uuidgen.uuid1mc()),
            'name': edbdef.EDGEDB_SUPERUSER,
            'tenant_id': backend_params.tenant_id,
            'builtin': False,
        }
        await _store_static_json_cache(
            ctx,
            'single_role_metadata',
            json.dumps(json_single_role_metadata),
        )

    if not backend_params.has_create_database:
        await _store_static_json_cache(
            ctx,
            f'{edbdef.EDGEDB_TEMPLATE_DB}metadata',
            json.dumps({}),
        )
        await _store_static_json_cache(
            ctx,
            f'{edbdef.EDGEDB_SYSTEM_DB}metadata',
            json.dumps({}),
        )
    return json_instance_data


async def _create_edgedb_database(
    ctx: BootstrapContext,
    database: str,
    owner: str,
    *,
    builtin: bool = False,
    objid: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    logger.info(f'Creating database: {database}')
    block = dbops.SQLBlock()
    if objid is None:
        objid = uuidgen.uuid1mc()
    instance_params = ctx.cluster.get_runtime_params().instance_params
    db = dbops.Database(
        ctx.cluster.get_db_name(database),
        owner=ctx.cluster.get_role_name(owner),
        metadata=dict(
            id=str(objid),
            tenant_id=instance_params.tenant_id,
            name=database,
            builtin=builtin,
        ),
    )
    tpl_db = ctx.cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB)
    dbops.CreateDatabase(db, template=tpl_db).generate(block)

    # Background tasks on some hosted provides like DO seem to sometimes make
    # their own connections to the template DB, so do a retry loop on it.
    rloop = retryloop.RetryLoop(
        backoff=retryloop.exp_backoff(),
        timeout=10.0,
        ignore=pgcon.errors.BackendError,
    )
    async for iteration in rloop:
        async with iteration:
            await _execute_block(ctx.conn, block)

    return objid


async def _set_edgedb_database_metadata(
    ctx: BootstrapContext,
    database: str,
    *,
    objid: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    logger.info(f'Configuring database: {database}')
    block = dbops.SQLBlock()
    if objid is None:
        objid = uuidgen.uuid1mc()
    instance_params = ctx.cluster.get_runtime_params().instance_params
    db = dbops.Database(ctx.cluster.get_db_name(database))
    metadata = dict(
        id=str(objid),
        tenant_id=instance_params.tenant_id,
        name=database,
        builtin=False,
    )
    dbops.SetMetadata(db, metadata).generate(block)
    await _execute_block(ctx.conn, block)
    return objid


def _pg_log_listener(severity, message):
    if severity == 'WARNING':
        level = logging.WARNING
    else:
        level = logging.DEBUG
    logger.log(level, message)


async def _get_instance_data(
    conn: metaschema.PGConnection,
    *,
    versioned: bool=True,
) -> Dict[str, Any]:
    schema = 'edgedbinstdata_VER' if versioned else 'edgedbinstdata'
    data = await conn.sql_fetch_val(
        trampoline.fixup_query(f"""
        SELECT json::json
        FROM {schema}.instdata
        WHERE key = 'instancedata'
        """).encode('utf-8'),
    )
    return json.loads(data)


async def _check_catalog_compatibility(
    ctx: BootstrapContext,
) -> PGConnectionProxy:
    tenant_id = ctx.cluster.get_runtime_params().tenant_id
    if ctx.mode == ClusterMode.single_database:
        sys_db = await ctx.conn.sql_fetch_val(
            trampoline.fixup_query("""
            SELECT current_database()
            FROM edgedbinstdata_VER.instdata
            WHERE key = $1 AND json->>'tenant_id' = $2
            """).encode('utf-8'),
            args=[
                f"{edbdef.EDGEDB_TEMPLATE_DB}metadata".encode("utf-8"),
                tenant_id.encode("utf-8"),
            ],
        )
    else:
        is_default_tenant = tenant_id == buildmeta.get_default_tenant_id()

        if is_default_tenant:
            sys_db = await ctx.conn.sql_fetch_val(
                b"""
                SELECT datname
                FROM pg_database
                WHERE datname LIKE '%' || $1
                ORDER BY
                    datname = $1,
                    datname DESC
                LIMIT 1
                """,
                args=[
                    edbdef.EDGEDB_SYSTEM_DB.encode("utf-8"),
                ],
            )
        else:
            sys_db = await ctx.conn.sql_fetch_val(
                b"""
                SELECT datname
                FROM pg_database
                WHERE datname = $1
                """,
                args=[
                    ctx.cluster.get_db_name(
                        edbdef.EDGEDB_SYSTEM_DB).encode("utf-8"),
                ],
            )

    if not sys_db:
        raise errors.ConfigurationError(
            'database instance is corrupt',
            details=(
                f'The database instance does not appear to have been fully '
                f'initialized or has been corrupted.'
            )
        )

    conn = PGConnectionProxy(
        ctx.cluster,
        source_description="_check_catalog_compatibility",
        dbname=sys_db.decode("utf-8")
    )

    try:
        # versioned=False so we can properly fail on version/catalog mismatches.
        instancedata = await _get_instance_data(conn, versioned=False)
        datadir_version = instancedata.get('version')
        if datadir_version:
            datadir_major = datadir_version.get('major')

        expected_ver = buildmeta.get_version()
        datadir_catver = instancedata.get('catver')
        expected_catver = edbdef.EDGEDB_CATALOG_VERSION

        status = dict(
            data_catalog_version=datadir_catver,
            expected_catalog_version=expected_catver,
        )

        if datadir_major != expected_ver.major:
            for status_sink in ctx.args.status_sinks:
                status_sink(f'INCOMPATIBLE={json.dumps(status)}')
            raise errors.ConfigurationError(
                'database instance incompatible with this version of Gel',
                details=(
                    f'The database instance was initialized with '
                    f'Gel version {datadir_major}, '
                    f'which is incompatible with this version '
                    f'{expected_ver.major}'
                ),
                hint=(
                    f'You need to either recreate the instance and upgrade '
                    f'using dump/restore, or do an inplace upgrade.'
                )
            )

        if datadir_catver != expected_catver:
            for status_sink in ctx.args.status_sinks:
                status_sink(f'INCOMPATIBLE={json.dumps(status)}')
            raise errors.ConfigurationError(
                'database instance incompatible with this version of Gel',
                details=(
                    f'The database instance was initialized with '
                    f'Gel format version {datadir_catver}, '
                    f'but this version of the server expects '
                    f'format version {expected_catver}'
                ),
                hint=(
                    f'You need to either recreate the instance and upgrade '
                    f'using dump/restore, or do an inplace upgrade.'
                )
            )
    except Exception:
        conn.terminate()
        raise

    return conn


def _check_capabilities(ctx: BootstrapContext) -> None:
    caps = ctx.cluster.get_runtime_params().instance_params.capabilities
    for cap in ctx.args.backend_capability_sets.must_be_present:
        if not caps & cap:
            raise errors.ConfigurationError(
                f"the backend doesn't have necessary capability: "
                f"{cap.name}"
            )
    for cap in ctx.args.backend_capability_sets.must_be_absent:
        if caps & cap:
            raise errors.ConfigurationError(
                f"the backend was already bootstrapped with capability: "
                f"{cap.name}"
            )


async def _pg_ensure_database_not_connected(
    conn: metaschema.PGConnection,
    dbname: str,
) -> None:
    conns = await conn.sql_fetch_col(
        b"""
        SELECT
            pid
        FROM
            pg_stat_activity
        WHERE
            datname = $1
        """,
        args=[dbname.encode("utf-8")],
    )

    if conns:
        raise errors.ExecutionError(
            f'database {dbname!r} is being accessed by other users')


async def _start(ctx: BootstrapContext) -> edbcompiler.Compiler:
    conn = await _check_catalog_compatibility(ctx)

    try:
        caps = await conn.sql_fetch_val(
            b"SELECT edgedb.get_backend_capabilities()")
        ctx.cluster.overwrite_capabilities(struct.Struct('!Q').unpack(caps)[0])
        _check_capabilities(ctx)

        return await edbcompiler.new_compiler_from_pg(conn)

    finally:
        conn.terminate()


async def _bootstrap_edgedb_super_roles(ctx: BootstrapContext) -> uuid.UUID:
    await _ensure_edgedb_supergroup(
        ctx,
        edbdef.EDGEDB_SUPERGROUP,
    )

    superuser_uid = await _ensure_edgedb_role(
        ctx,
        edbdef.EDGEDB_SUPERUSER,
        superuser=True,
        builtin=True,
    )

    superuser = ctx.cluster.get_role_name(edbdef.EDGEDB_SUPERUSER)
    await _execute(ctx.conn, f'SET ROLE {qi(superuser)}')

    return superuser_uid


async def _bootstrap(
    ctx: BootstrapContext,
    no_template: bool=False,
) -> edbcompiler.Compiler:
    args = ctx.args
    cluster = ctx.cluster
    backend_params = cluster.get_runtime_params()

    if backend_params.instance_params.version < edbdef.MIN_POSTGRES_VERSION:
        min_ver = '.'.join(str(v) for v in edbdef.MIN_POSTGRES_VERSION)
        raise errors.ConfigurationError(
            'unsupported backend',
            details=(
                f'Gel requires PostgreSQL version {min_ver} or later, '
                f'while the specified backend reports itself as '
                f'{backend_params.instance_params.version.string}.'
            )
        )

    _check_capabilities(ctx)

    if backend_params.has_create_role:
        superuser_uid = await _bootstrap_edgedb_super_roles(ctx)
    else:
        superuser_uid = uuidgen.uuid1mc()

    using_template = backend_params.has_create_database and not no_template

    if using_template:
        if not args.inplace_upgrade_prepare:
            new_template_db_id = await _create_edgedb_template_database(ctx)
        # XXX: THIS IS WRONG, RIGHT?
        else:
            new_template_db_id = uuidgen.uuid1mc()
        tpl_db = cluster.get_db_name(edbdef.EDGEDB_TEMPLATE_DB)
        conn = PGConnectionProxy(
            cluster,
            source_description="_bootstrap",
            dbname=tpl_db
        )

        tpl_ctx = dataclasses.replace(ctx, conn=conn)
    else:
        new_template_db_id = uuidgen.uuid1mc()
        tpl_ctx = ctx

    in_dev_mode = devmode.is_in_dev_mode()
    # Protect against multiple Gel tenants from trying to bootstrap
    # on the same cluster in devmode, as that is both a waste of resources
    # and might result in broken stdlib cache.
    if in_dev_mode:
        await tpl_ctx.conn.sql_execute(b"SELECT pg_advisory_lock(3987734529)")

    try:
        # Some of the views need access to the _edgecon_state table,
        # so set it up.
        tmp_table_query = pgcon.SETUP_TEMP_TABLE_SCRIPT
        await _execute(tpl_ctx.conn, tmp_table_query)

        stdlib, config_spec, compiler = await _init_stdlib(
            tpl_ctx,
            testmode=args.testmode,
            global_ids={
                edbdef.EDGEDB_SUPERUSER: superuser_uid,
                edbdef.EDGEDB_TEMPLATE_DB: new_template_db_id,
            }
        )
        (
            sysqueries,
            report_configs_typedesc_1_0,
            report_configs_typedesc_2_0,
        ) = compile_sys_queries(
            stdlib.reflschema,
            compiler,
            config_spec,
        )

        # Update schema backend_ids to match the reality after
        await tpl_ctx.conn.sql_execute(
            sysqueries['backend_id_fixup'].encode('utf-8')
        )

        version_key = patches.get_version_key(stdlib.num_patches)
        await _store_static_json_cache(
            tpl_ctx,
            f'sysqueries{version_key}',
            json.dumps(sysqueries),
        )

        await _store_static_bin_cache(
            tpl_ctx,
            f'report_configs_typedesc_1_0{version_key}',
            report_configs_typedesc_1_0,
        )

        await _store_static_bin_cache(
            tpl_ctx,
            f'report_configs_typedesc_2_0{version_key}',
            report_configs_typedesc_2_0,
        )

        await _store_static_json_cache(
            tpl_ctx,
            'num_patches',
            json.dumps(stdlib.num_patches),
        )

        default_branch = args.default_branch or edbdef.EDGEDB_SUPERUSER_DB
        await _store_static_text_cache(
            tpl_ctx,
            'default_branch',
            default_branch,
        )

        schema = s_schema.EMPTY_SCHEMA
        if not no_template:
            schema = await _init_defaults(schema, compiler, tpl_ctx.conn)

        # Run analyze on the template database, so that new dbs start
        # with up-to-date statistics.
        await tpl_ctx.conn.sql_execute(b"ANALYZE")

    finally:
        if in_dev_mode:
            await tpl_ctx.conn.sql_execute(
                b"SELECT pg_advisory_unlock(3987734529)",
            )

        if using_template:
            # Close the connection to the template database
            # and wait until it goes away on the server side
            # so that we can safely use the template for new
            # databases.
            #
            # The timeout is set weirdly high, because we were getting
            # frequent timeouts in macos-x86_64 release builds when it
            # was set to 10s.
            conn.terminate()
            rloop = retryloop.RetryLoop(
                backoff=retryloop.exp_backoff(),
                timeout=60.0,
                ignore=errors.ExecutionError,
            )

            async for iteration in rloop:
                async with iteration:
                    await _pg_ensure_database_not_connected(ctx.conn, tpl_db)

    if args.inplace_upgrade_prepare:
        pass
    elif backend_params.has_create_database:
        await _create_edgedb_database(
            ctx,
            edbdef.EDGEDB_SYSTEM_DB,
            edbdef.EDGEDB_SUPERUSER,
            builtin=True,
        )

        sys_conn = PGConnectionProxy(
            cluster,
            source_description="_bootstrap",
            dbname=cluster.get_db_name(edbdef.EDGEDB_SYSTEM_DB),
        )

        try:
            await _configure(
                dataclasses.replace(ctx, conn=sys_conn),
                config_spec=config_spec,
                schema=schema,
                compiler=compiler,
            )
        finally:
            sys_conn.terminate()
    else:
        await _configure(
            ctx,
            config_spec=config_spec,
            schema=schema,
            compiler=compiler,
        )

    if args.inplace_upgrade_prepare:
        pass
    elif backend_params.has_create_database:
        await _create_edgedb_database(
            ctx,
            default_branch,
            args.default_database_user or edbdef.EDGEDB_SUPERUSER,
        )
    else:
        await _set_edgedb_database_metadata(
            ctx,
            default_branch,
        )

    if (
        backend_params.has_create_role
        and args.default_database_user
        and args.default_database_user != edbdef.EDGEDB_SUPERUSER
    ):
        await _ensure_edgedb_role(
            ctx,
            args.default_database_user,
            superuser=True,
        )

        def_role = ctx.cluster.get_role_name(args.default_database_user)
        await _execute(ctx.conn, f"SET ROLE {qi(def_role)}")

    if (
        backend_params.has_create_database
        and args.default_database
        and args.default_database != default_branch
    ):
        await _create_edgedb_database(
            ctx,
            args.default_database,
            args.default_database_user or edbdef.EDGEDB_SUPERUSER,
        )

    return compiler


async def ensure_bootstrapped(
    cluster: pgcluster.BaseCluster,
    args: edbargs.ServerConfig,
) -> tuple[bool, edbcompiler.Compiler]:
    """Bootstraps Gel instance if it hasn't been bootstrapped already.

    Returns True if bootstrap happened and False if the instance was already
    bootstrapped, along with the bootstrap compiler state.
    """
    pgconn = PGConnectionProxy(
        cluster,
        source_description="ensure_bootstrapped"
    )
    ctx = BootstrapContext(cluster=cluster, conn=pgconn, args=args)

    try:
        mode = await _get_cluster_mode(ctx)
        ctx = dataclasses.replace(ctx, mode=mode)
        if mode == ClusterMode.pristine:
            compiler = await _bootstrap(ctx)
            return True, compiler
        else:
            compiler = await _start(ctx)
            return False, compiler
    finally:
        pgconn.terminate()
