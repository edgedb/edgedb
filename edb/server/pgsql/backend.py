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


import collections
import logging
import pathlib
import pickle
import re
import uuid

from edb import errors

from edb.lang.common import context as parser_context
from edb.lang.common import debug
from edb.lang.common import devmode
from edb.lang.common import exceptions

from edb.lang import edgeql

from edb.lang.schema import delta as sd

from edb.lang.schema import abc as s_abc
from edb.lang.schema import database as s_db
from edb.lang.schema import ddl as s_ddl
from edb.lang.schema import deltas as s_deltas
from edb.lang.schema import schema as s_schema
from edb.lang.schema import std as s_std

from edb.server import query as backend_query
from edb.server.pgsql import dbops
from edb.server.pgsql import delta as delta_cmds

from . import common
from . import compiler
from . import intromech
from . import types

from .common import quote_ident as qi


CACHE_SRC_DIRS = s_std.CACHE_SRC_DIRS + (
    (pathlib.Path(__file__).parent, '.py'),
)


class Query(backend_query.Query):
    def __init__(
            self, *, text, argmap, argument_types,
            output_desc=None, output_format=None):
        self.text = text
        self.argmap = argmap
        self.argument_types = collections.OrderedDict((k, argument_types[k])
                                                      for k in argmap
                                                      if k in argument_types)

        self.output_desc = output_desc
        self.output_format = output_format


class TypeDescriptor:
    def __init__(self, type_id, schema_type, subtypes, element_names):
        self.type_id = type_id
        self.schema_type = schema_type
        self.subtypes = subtypes
        self.element_names = element_names
        self.cardinality = '1'


class OutputDescriptor:
    def __init__(self, type_desc, tuple_registry):
        self.type_desc = type_desc
        self.tuple_registry = tuple_registry


class Backend:

    std_schema = None

    def __init__(self, connection, data_dir):
        self.schema = None
        self.modaliases = {None: 'default'}
        self.testmode = False

        self._intro_mech = intromech.IntrospectionMech(connection)

        self.connection = connection
        self.data_dir = pathlib.Path(data_dir)
        self.dev_mode = devmode.is_in_dev_mode()
        self.transactions = []

    async def getschema(self):
        if self.schema is None:
            cls = type(self)
            if cls.std_schema is None:
                with open(self.data_dir / 'stdschema.pickle', 'rb') as f:
                    try:
                        cls.std_schema = pickle.load(f)
                    except Exception as e:
                        raise RuntimeError(
                            'could not load std schema pickle') from e

            self.schema = await self._intro_mech.readschema(
                schema=cls.std_schema, exclude_modules=s_schema.STD_MODULES)

        return self.schema

    def create_context(self, cmds=None, stdmode=None):
        context = sd.CommandContext()
        context.testmode = self.testmode
        if stdmode is not None:
            context.stdmode = stdmode

        return context

    def adapt_delta(self, delta):
        return delta_cmds.CommandMeta.adapt(delta)

    def process_delta(self, delta, schema, *, stdmode=None):
        """Adapt and process the delta command."""

        if debug.flags.delta_plan:
            debug.header('Delta Plan')
            debug.dump(delta, schema=schema)

        delta = self.adapt_delta(delta)
        context = self.create_context(delta_cmds, stdmode)
        schema, _ = delta.apply(schema, context)

        if debug.flags.delta_pgsql_plan:
            debug.header('PgSQL Delta Plan')
            debug.dump(delta, schema=schema)

        return schema, delta

    async def run_delta_command(self, delta_cmd):
        schema = self.schema
        context = self.create_context()
        result = None

        if isinstance(delta_cmd, s_deltas.CreateDelta):
            delta = None
        else:
            delta = schema.get(delta_cmd.classname)

        with context(s_deltas.DeltaCommandContext(schema, delta_cmd, delta)):
            if isinstance(delta_cmd, s_deltas.CommitDelta):
                ddl_plan = sd.DeltaRoot()
                ddl_plan.update(delta.get_commands(schema))
                await self.run_ddl_command(ddl_plan)

            elif isinstance(delta_cmd, s_deltas.GetDelta):
                result = s_ddl.ddl_text_from_delta(schema, delta)

            elif isinstance(delta_cmd, s_deltas.CreateDelta):
                self.schema, _ = delta_cmd.apply(schema, context)

            else:
                raise RuntimeError(
                    f'unexpected delta command: {delta_cmd!r}')

        return result

    async def _execute_ddl(self, sql_text):
        try:
            if debug.flags.delta_execute:
                debug.header('Delta Script')
                debug.dump_code(sql_text, lexer='sql')

            await self.connection.execute(sql_text)

        except Exception as e:
            position = getattr(e, 'position', None)
            internal_position = getattr(e, 'internal_position', None)
            context = getattr(e, 'context', '')
            if context:
                pl_func_line = re.search(
                    r'^PL/pgSQL function inline_code_block line (\d+).*',
                    context, re.M)

                if pl_func_line:
                    pl_func_line = int(pl_func_line.group(1))
            else:
                pl_func_line = None
            point = None

            if position is not None:
                position = int(position)
                point = parser_context.SourcePoint(
                    None, None, position)
                text = e.query
                if text is None:
                    # Parse errors
                    text = sql_text

            elif internal_position is not None:
                internal_position = int(internal_position)
                point = parser_context.SourcePoint(
                    None, None, internal_position)
                text = e.internal_query

            elif pl_func_line:
                point = parser_context.SourcePoint(
                    pl_func_line, None, None
                )
                text = sql_text

            if point is not None:
                context = parser_context.ParserContext(
                    'query', text, start=point, end=point)
                exceptions.replace_context(e, context)

            raise

    async def _load_std(self):
        schema = s_schema.Schema()

        current_block = None

        std_texts = []
        for modname in s_schema.STD_LIB + ['stdgraphql']:
            std_texts.append(s_std.get_std_module_text(modname))

        ddl_text = '\n'.join(std_texts)

        for ddl_cmd in edgeql.parse_block(ddl_text):
            delta_command = s_ddl.delta_from_ddl(
                ddl_cmd, schema=schema, modaliases={None: 'std'}, stdmode=True)

            if debug.flags.delta_plan_input:
                debug.header('Delta Plan Input')
                debug.dump(delta_command)

            # Do a dry-run on test_schema to canonicalize
            # the schema delta-commands.
            test_schema = schema
            context = self.create_context(stdmode=True)
            canonical_delta = delta_command.copy()
            canonical_delta.apply(test_schema, context=context)

            # Apply and adapt delta, build native delta plan, which
            # will also update the schema.
            schema, plan = self.process_delta(canonical_delta, schema,
                                              stdmode=True)

            if isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
                if (current_block is not None and
                        not isinstance(current_block, dbops.SQLBlock)):
                    raise errors.QueryError(
                        'cannot mix DATABASE commands with regular DDL '
                        'commands in a single block')
                if current_block is None:
                    current_block = dbops.SQLBlock()

            else:
                if (current_block is not None and
                        not isinstance(current_block, dbops.PLTopBlock)):
                    raise errors.QueryError(
                        'cannot mix DATABASE commands with regular DDL '
                        'commands in a single block')
                if current_block is None:
                    current_block = dbops.PLTopBlock()

            plan.generate(current_block)

        sql_text = current_block.to_string()

        return schema, sql_text

    async def run_std_bootstrap(self):
        cache_hit = False
        sql_text = None

        cluster_schema_cache = self.data_dir / 'stdschema.pickle'

        if self.dev_mode:
            schema_cache = 'backend-stdschema.pickle'
            script_cache = 'backend-stdinitsql.pickle'

            src_hash = devmode.hash_dirs(CACHE_SRC_DIRS)
            sql_text = devmode.read_dev_mode_cache(src_hash, script_cache)

            if sql_text is not None:
                schema = devmode.read_dev_mode_cache(src_hash, schema_cache)

        if sql_text is None or schema is None:
            schema, sql_text = await self._load_std()
        else:
            cache_hit = True

        await self._execute_ddl(sql_text)
        self.schema = schema

        if not cache_hit and self.dev_mode:
            devmode.write_dev_mode_cache(schema, src_hash, schema_cache)
            devmode.write_dev_mode_cache(sql_text, src_hash, script_cache)

        with open(cluster_schema_cache, 'wb') as f:
            pickle.dump(schema, file=f, protocol=pickle.HIGHEST_PROTOCOL)

    async def run_ddl_command(self, ddl_plan):
        schema = self.schema

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(ddl_plan)

        # Do a dry-run on test_schema to canonicalize
        # the schema delta-commands.
        test_schema = schema
        context = self.create_context()
        canonical_ddl_plan = ddl_plan.copy()
        canonical_ddl_plan.apply(test_schema, context=context)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        schema, plan = self.process_delta(canonical_ddl_plan, schema)

        context = self.create_context(delta_cmds)

        if isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
            block = dbops.SQLBlock()
        else:
            block = dbops.PLTopBlock()

        plan.generate(block)
        ql_text = block.to_string()

        await self._execute_ddl(ql_text)
        self.schema = schema

    async def exec_session_state_cmd(self, cmd):
        for alias, module in cmd.modaliases.items():
            self.modaliases[alias] = module.get_name(self.schema)

        self.testmode = cmd.testmode

    def _get_collection_type_id(self, coll_type, subtypes, element_names):
        subtypes = (f"{st.type_id}-{st.cardinality}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        return uuid.uuid5(types.TYPE_ID_NAMESPACE, string_id)

    def _get_union_type_id(self, schema, union_type):
        base_type_id = ','.join(
            str(c.id) for c in union_type.children(schema))

        return uuid.uuid5(types.TYPE_ID_NAMESPACE, base_type_id)

    def _describe_type(self, schema, t, view_shapes, _tuples):
        mt = t.material_type(schema)
        is_tuple = False

        if isinstance(t, s_abc.Collection):
            subtypes = [self._describe_type(schema, st, view_shapes, _tuples)
                        for st in t.get_subtypes()]

            if isinstance(t, s_abc.Tuple) and t.named:
                element_names = list(t.element_types.keys())
            else:
                element_names = None

            type_id = self._get_collection_type_id(t.schema_name, subtypes,
                                                   element_names)
            is_tuple = True

        elif view_shapes.get(t):
            # This is a view

            if mt.get_is_virtual(schema):
                base_type_id = self._get_union_type_id(schema, mt)
            else:
                base_type_id = mt.id

            subtypes = []
            element_names = []

            for ptr in view_shapes[t]:
                subdesc = self._describe_type(
                    schema, ptr.get_target(schema), view_shapes, _tuples)
                subdesc.cardinality = (
                    '1' if ptr.singular(schema) else '*'
                )
                subtypes.append(subdesc)
                element_names.append(ptr.get_shortname(schema).name)

            t_rptr = t.get_rptr(schema)
            if t_rptr is not None:
                # There are link properties in the mix
                for ptr in view_shapes[t_rptr]:
                    subdesc = self._describe_type(
                        schema, ptr.get_target(schema), view_shapes, _tuples)
                    subdesc.cardinality = (
                        '1' if ptr.singular(schema) else '*'
                    )
                    subtypes.append(subdesc)
                    element_names.append(ptr.get_shortname(schema).name)

            type_id = self._get_collection_type_id(base_type_id, subtypes,
                                                   element_names)
            is_tuple = True

        else:
            # This is a regular type
            subtypes = None
            element_names = None
            type_id = mt.id

        type_desc = TypeDescriptor(
            type_id=type_id, schema_type=mt, subtypes=subtypes,
            element_names=element_names)

        if is_tuple:
            _tuples[type_id] = type_desc

        return type_desc

    def compile(self, query_ir, context=None, *,
                output_format=None, timer=None):
        tuples = {}
        type_desc = self._describe_type(
            query_ir.schema, query_ir.stype,
            query_ir.view_shapes, tuples)

        output_desc = OutputDescriptor(
            type_desc=type_desc, tuple_registry=tuples)

        sql_text, argmap = compiler.compile_ir_to_sql(
            query_ir, schema=query_ir.schema,
            output_format=output_format, timer=timer)

        argtypes = {}
        for k, v in query_ir.params.items():
            argtypes[k] = v

        return Query(
            text=sql_text, argmap=argmap,
            argument_types=argtypes,
            output_desc=output_desc,
            output_format=output_format)

    async def compile_migration(
            self, cmd: s_deltas.CreateDelta) -> s_deltas.CreateDelta:

        declarations = cmd.get_attribute_value('target')
        if not declarations:
            return cmd

        return s_ddl.compile_migration(cmd, self.std_schema, self.schema)

    async def translate_pg_error(self, query, error):
        return await self._intro_mech.translate_pg_error(
            self.schema, query, error)

    async def start_transaction(self):
        tx = self.connection.transaction()
        self.transactions.append((tx, self.schema))
        await tx.start()

    async def commit_transaction(self):
        if not self.transactions:
            raise errors.TransactionError(
                'there is no transaction in progress')
        transaction, _ = self.transactions.pop()
        await transaction.commit()

    async def rollback_transaction(self):
        if not self.transactions:
            raise errors.TransactionError(
                'there is no transaction in progress')
        transaction, self.schema = self.transactions.pop()
        await transaction.rollback()


async def open_database(pgconn, data_dir, *, bootstrap=False):
    bk = Backend(pgconn, data_dir)
    pgconn.add_log_listener(pg_log_listener)
    if not bootstrap:
        schema = await bk.getschema()
        stdschema = common.get_backend_name(schema, schema.get('std'))
        await pgconn.execute(
            f'SET search_path = edgedb, {qi(stdschema)}')
    else:
        bk.schema = s_schema.Schema()

    return bk


logger = logging.getLogger('edb.backend')


def pg_log_listener(conn, msg):
    if msg.severity_en == 'WARNING':
        level = logging.WARNING
    else:
        level = logging.DEBUG
    logger.log(level, msg.message)
