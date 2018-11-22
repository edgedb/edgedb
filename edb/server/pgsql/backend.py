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
import re
import uuid

from edb.lang.common import context as parser_context
from edb.lang.common import debug
from edb.lang.common import exceptions

from edb.lang.schema import delta as sd

from edb.lang.schema import database as s_db
from edb.lang.schema import ddl as s_ddl
from edb.lang.schema import deltas as s_deltas
from edb.lang.schema import types as s_types

from edb.server import query as backend_query
from edb.server.pgsql import dbops
from edb.server.pgsql import delta as delta_cmds

from . import compiler
from . import intromech
from . import types


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

    def __init__(self, connection):
        self.schema = None
        self.modaliases = {None: 'default'}
        self.testmode = False

        self._intro_mech = intromech.IntrospectionMech(connection)

        self.connection = connection
        self.transactions = []

    async def getschema(self):
        if self.schema is None:
            self.schema = await self._intro_mech.getschema()

        return self.schema

    def adapt_delta(self, delta):
        return delta_cmds.CommandMeta.adapt(delta)

    def process_delta(self, delta, schema):
        """Adapt and process the delta command."""

        if debug.flags.delta_plan:
            debug.header('Delta Plan')
            debug.dump(delta)

        delta = self.adapt_delta(delta)
        context = delta_cmds.CommandContext(self.connection)
        schema, _ = delta.apply(schema, context)

        if debug.flags.delta_pgsql_plan:
            debug.header('PgSQL Delta Plan')
            debug.dump(delta)

        return delta

    async def run_delta_command(self, delta_cmd):
        schema = await self.getschema()
        context = sd.CommandContext()
        result = None

        with context(s_deltas.DeltaCommandContext(delta_cmd)):
            if isinstance(delta_cmd, s_deltas.CommitDelta):
                delta = schema.get_delta(delta_cmd.classname)
                ddl_plan = s_db.AlterDatabase()
                ddl_plan.update(delta.get_commands(schema))
                await self.run_ddl_command(ddl_plan)

            elif isinstance(delta_cmd, s_deltas.GetDelta):
                delta = schema.get_delta(delta_cmd.classname)
                result = s_ddl.ddl_text_from_delta(schema, delta)

            elif isinstance(delta_cmd, s_deltas.CreateDelta):
                schema, _ = delta_cmd.apply(schema, context)

            else:
                raise RuntimeError(
                    f'unexpected delta command: {delta_cmd!r}')

        return result

    async def run_ddl_command(self, ddl_plan):
        schema = await self.getschema()

        if debug.flags.delta_plan_input:
            debug.header('Delta Plan Input')
            debug.dump(ddl_plan)

        # Do a dry-run on test_schema to canonicalize
        # the schema delta-commands.
        test_schema = await self._intro_mech.readschema()
        context = sd.CommandContext()
        canonical_ddl_plan = ddl_plan.copy()
        canonical_ddl_plan.apply(test_schema, context=context)

        # Apply and adapt delta, build native delta plan, which
        # will also update the schema.
        plan = self.process_delta(canonical_ddl_plan, schema)

        context = delta_cmds.CommandContext(self.connection)

        if isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
            block = dbops.SQLBlock()
        else:
            block = dbops.PLTopBlock()

        try:
            plan.generate(block)
            ql_text = block.to_string()

            if debug.flags.delta_execute:
                debug.header('Delta Script')
                debug.dump_code(ql_text, lexer='sql')

            if not isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
                async with self.connection.transaction():
                    await self.connection.execute(ql_text)
                    # Execute all pgsql/delta commands.
            else:
                await self.connection.execute(ql_text)
        except Exception as e:
            position = getattr(e, 'position', None)
            internal_position = getattr(e, 'internal_position', None)
            context = getattr(e, 'context', '')
            if context:
                pl_func_line = re.match(
                    r'^PL/pgSQL function inline_code_block line (\d+).*$',
                    getattr(e, 'context', ''))
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
                    text = ql_text

            elif internal_position is not None:
                internal_position = int(internal_position)
                point = parser_context.SourcePoint(
                    None, None, internal_position)
                text = e.internal_query

            elif pl_func_line:
                line = int(pl_func_line.group(1))
                point = parser_context.SourcePoint(
                    line, None, None
                )
                text = ql_text

            if point is not None:
                context = parser_context.ParserContext(
                    'query', text, start=point, end=point)
                exceptions.replace_context(e, context)

            raise

        finally:
            # Exception or not, re-read the schema from Postgres.
            await self.invalidate_schema_cache()
            await self.getschema()

    async def invalidate_schema_cache(self):
        self.schema = None
        self.invalidate_transient_cache()

    def invalidate_transient_cache(self):
        self._intro_mech.invalidate_cache()

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

        if isinstance(t, s_types.Collection):
            subtypes = [self._describe_type(schema, st, view_shapes, _tuples)
                        for st in t.get_subtypes()]

            if isinstance(t, s_types.Tuple) and t.named:
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
                    '1' if ptr.singular(self.schema) else '*'
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
                        '1' if ptr.singular(self.schema) else '*'
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
            query_ir.schema, query_ir.expr.scls, query_ir.view_shapes, tuples)

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

        stdmodules = {'std', 'schema', 'stdattrs'}

        target = await self._intro_mech.readschema(stdmodules)

        return s_ddl.compile_migration(cmd, target, self.schema)

    async def translate_pg_error(self, query, error):
        return await self._intro_mech.translate_pg_error(query, error)

    async def start_transaction(self):
        self.transactions.append(self.connection.transaction())
        await self.transactions[-1].start()

    async def commit_transaction(self):
        if not self.transactions:
            raise exceptions.NoActiveTransactionError(
                'there is no transaction in progress')
        transaction = self.transactions.pop()
        await transaction.commit()

    async def rollback_transaction(self):
        if not self.transactions:
            raise exceptions.NoActiveTransactionError(
                'there is no transaction in progress')
        transaction = self.transactions.pop()
        await transaction.rollback()
        await self.invalidate_schema_cache()
        await self.getschema()


async def open_database(pgconn):
    bk = Backend(pgconn)
    pgconn.add_log_listener(pg_log_listener)
    await bk.getschema()
    return bk


logger = logging.getLogger('edb.backend')


def pg_log_listener(conn, msg):
    if msg.severity_en == 'WARNING':
        level = logging.WARNING
    else:
        level = logging.DEBUG
    logger.log(level, msg.message)
