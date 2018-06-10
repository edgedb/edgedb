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
import uuid

from edb.lang.common import debug

from edb.lang.schema import delta as sd

from edb.lang.schema import database as s_db
from edb.lang.schema import ddl as s_ddl
from edb.lang.schema import deltarepo as s_deltarepo
from edb.lang.schema import deltas as s_deltas
from edb.lang.schema import types as s_types

from edb.server import query as backend_query
from edb.server.pgsql import dbops
from edb.server.pgsql import delta as delta_cmds
from edb.server.pgsql import deltadbops

from . import compiler
from . import deltarepo as pgsql_deltarepo
from . import intromech


class Query(backend_query.Query):
    def __init__(
            self, *, chunks, arg_index, argmap, argument_types,
            scrolling_cursor=False, offset=None, limit=None,
            query_type=None, output_desc=None, output_format=None):
        self.chunks = chunks
        self.text = ''.join(chunks)
        self.argmap = argmap
        self.arg_index = arg_index
        self.argument_types = collections.OrderedDict((k, argument_types[k])
                                                      for k in argmap
                                                      if k in argument_types)

        self.scrolling_cursor = scrolling_cursor
        self.offset = offset.index if offset is not None else None
        self.limit = limit.index if limit is not None else None
        self.query_type = query_type
        self.output_desc = output_desc
        self.output_format = output_format

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('text')
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.text = ''.join(self.chunks)

    def get_output_format_info(self):
        if self.output_format == 'json':
            return ('json', 1)
        else:
            return ('edgedbobj', 1)


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


class Backend(s_deltarepo.DeltaProvider):

    def __init__(self, connection):
        self.schema = None
        self.modaliases = {None: 'default'}

        self._intro_mech = intromech.IntrospectionMech(connection)

        self.connection = connection

        repo = pgsql_deltarepo.MetaDeltaRepository(self.connection)
        super().__init__(repo)

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
        delta.apply(schema, context)

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
                ddl_plan.update(delta.commands)
                await self.run_ddl_command(ddl_plan)
                await self._commit_delta(delta, ddl_plan)

            elif isinstance(delta_cmd, s_deltas.GetDelta):
                delta = schema.get_delta(delta_cmd.classname)
                result = s_ddl.ddl_text_from_delta(schema, delta)

            elif isinstance(delta_cmd, s_deltas.CreateDelta):
                delta_cmd.apply(schema, context)

            else:
                raise RuntimeError(
                    f'unexpected delta command: {delta_cmd!r}')

        return result

    async def _commit_delta(self, delta, ddl_plan):
        return  # XXX
        table = deltadbops.DeltaTable()
        rec = table.record(
            name=delta.name, module_id=dbops.Query(
                '''
                SELECT id FROM edgedb.module WHERE name = $1
            ''', params=[delta.name.module]), parents=dbops.Query(
                    '''
                SELECT array_agg(id) FROM edgedb.delta WHERE name = any($1)
            ''', params=[[parent.name for parent in delta.parents]]),
            checksum=(await self.getschema()).get_checksum(), deltabin=b'1',
            deltasrc=s_ddl.ddl_text_from_delta_command(ddl_plan))
        context = delta_cmds.CommandContext(self.connection)
        await dbops.Insert(table, records=[rec]).execute(context)

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

        try:
            if not isinstance(plan, (s_db.CreateDatabase, s_db.DropDatabase)):
                async with self.connection.transaction():
                    # Execute all pgsql/delta commands.
                    await plan.execute(context)
            else:
                await plan.execute(context)
        except Exception as e:
            raise RuntimeError('failed to apply delta to data backend') from e
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
            self.modaliases[alias] = module.name

    def get_type_id(self, objtype):
        return self._intro_mech.get_type_id(objtype)

    def _get_collection_type_id(self, coll_type, subtypes, element_names):
        subtypes = (f"{st.type_id}-{st.cardinality}" for st in subtypes)
        string_id = f'{coll_type}\x00{":".join(subtypes)}'
        if element_names:
            string_id += f'\x00{":".join(element_names)}'
        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE, string_id)

    def _get_union_type_id(self, union_type):
        base_type_id = ','.join(
            str(self.get_type_id(c)) for c in union_type.children(self.schema))

        return uuid.uuid5(delta_cmds.TYPE_ID_NAMESPACE, base_type_id)

    def _describe_type(self, t, view_shapes, _tuples):
        mt = t.material_type()
        is_tuple = False

        if isinstance(t, s_types.Collection):
            subtypes = [self._describe_type(st, view_shapes, _tuples)
                        for st in t.get_subtypes()]

            if isinstance(t, s_types.Tuple) and t.named:
                element_names = list(t.element_types)
            else:
                element_names = None

            type_id = self._get_collection_type_id(t.schema_name, subtypes,
                                                   element_names)
            is_tuple = True

        elif t in view_shapes:
            # This is a view

            if mt.is_virtual:
                base_type_id = self._get_union_type_id(mt)
            else:
                base_type_id = self.get_type_id(mt)

            subtypes = []
            element_names = []

            for ptr in view_shapes[t]:
                subdesc = self._describe_type(ptr.target, view_shapes, _tuples)
                subdesc.cardinality = '1' if ptr.singular() else '*'
                subtypes.append(subdesc)
                element_names.append(ptr.shortname.name)

            if t.rptr is not None:
                # There are link properties in the mix
                for ptr in view_shapes[t.rptr]:
                    subdesc = self._describe_type(
                        ptr.target, view_shapes, _tuples)
                    subdesc.cardinality = '1' if ptr.singular() else '*'
                    subtypes.append(subdesc)
                    element_names.append(ptr.shortname.name)

            if subtypes:
                type_id = self._get_collection_type_id(base_type_id, subtypes,
                                                       element_names)
                is_tuple = True
            else:
                type_id = base_type_id
                if mt.is_virtual:
                    is_tuple = True

        else:
            # This is a regular type
            subtypes = None
            element_names = None
            type_id = self.get_type_id(mt)

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
            query_ir.expr.scls, query_ir.view_shapes, tuples)

        output_desc = OutputDescriptor(
            type_desc=type_desc, tuple_registry=tuples)

        qchunks, argmap, arg_index, query_type = \
            compiler.compile_ir_to_sql(
                query_ir, schema=self.schema,
                output_format=output_format, timer=timer)

        argtypes = {}
        for k, v in query_ir.params.items():
            argtypes[k] = v

        return Query(
            chunks=qchunks, arg_index=arg_index, argmap=argmap,
            argument_types=argtypes,
            query_type=query_type, output_desc=output_desc,
            output_format=output_format)

    async def translate_pg_error(self, query, error):
        return await self._intro_mech.translate_pg_error(query, error)


async def open_database(pgconn):
    bk = Backend(pgconn)
    await bk.getschema()
    return bk
