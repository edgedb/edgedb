##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import bisect
import collections
import functools
import importlib
import itertools
import json
import os
import pickle
import re
import uuid

from importkit.import_ import get_object

import asyncpg

from edgedb.lang.common.algos import topological
from edgedb.lang.common.debug import debug
from edgedb.lang.common.nlang import morphology
from edgedb.lang.common import markup

from edgedb.lang.common import exceptions as edgedb_error

from edgedb.lang import schema as so
from edgedb.lang.schema import delta as sd

from edgedb.lang.schema import attributes as s_attrs
from edgedb.lang.schema import atoms as s_atoms
from edgedb.lang.schema import concepts as s_concepts
from edgedb.lang.schema import constraints as s_constr
from edgedb.lang.schema import deltarepo as s_deltarepo
from edgedb.lang.schema import error as s_err
from edgedb.lang.schema import expr as s_expr
from edgedb.lang.schema import indexes as s_indexes
from edgedb.lang.schema import links as s_links
from edgedb.lang.schema import lproperties as s_lprops
from edgedb.lang.schema import modules as s_mod
from edgedb.lang.schema import name as sn
from edgedb.lang.schema import objects as s_obj
from edgedb.lang.schema import pointers as s_pointers
from edgedb.lang.schema import policy as s_policy
from edgedb.lang.schema import types as s_types

from edgedb.lang import caosql

from edgedb.server import query as backend_query
from edgedb.server.pgsql import common
from edgedb.server.pgsql import dbops
from edgedb.server.pgsql import delta as delta_cmds
from edgedb.server.pgsql import deltadbops

from . import datasources
from .datasources import introspection

from .transformer import IRCompiler

from . import ast as pg_ast
from . import astexpr
from . import parser
from . import types
from . import transformer
from . import schemamech
from . import deltarepo as pgsql_deltarepo


class Cursor:
    def __init__(self, dbcursor, offset, limit):
        self.dbcursor = dbcursor
        self.offset = offset
        self.limit = limit
        self.cursor_pos = 0

    def seek(self, offset, whence='set'):
        if whence == 'set':
            if offset != self.cursor_pos:
                self.dbcursor.seek(0, 'ABSOLUTE')
                result = self.dbcursor.seek(offset, 'FORWARD')
                self.cursor_pos = result
        elif whence == 'cur':
            result = self.dbcursor.seek(offset, 'FORWARD')
            self.cursor_pos += result
        elif whence == 'end':
            result = self.dbcursor.seek('ALL')
            self.cursor_pos = result - offset

        return self.cursor_pos

    def tell(self):
        return self.cursor_pos

    def count(self, total=False):
        current = self.tell()

        if total:
            self.seek(0)
            result = self.seek(0, 'end')
        else:
            offset = self.offset if self.offset is not None else 0
            limit = self.limit if self.limit else 'ALL'

            self.seek(offset)
            result = self.seek(limit, 'cur')
            result -= offset

        self.seek(current)
        return result

    def __iter__(self):
        if self.offset:
            self.seek(self.offset)
            offset = self.offset
        else:
            offset = 0

        while self.limit is None or self.cursor_pos < offset + self.limit:
            self.cursor_pos += 1
            yield next(self.dbcursor)


class Query(backend_query.Query):
    def __init__(self, chunks, arg_index, argmap, result_types, argument_types,
                 context_vars, scrolling_cursor=False, offset=None, limit=None,
                 query_type=None, record_info=None, output_format=None):
        self.chunks = chunks
        self.text = ''.join(chunks)
        self.argmap = argmap
        self.arg_index = arg_index
        self.result_types = result_types
        self.argument_types = collections.OrderedDict(
            (k, argument_types[k]) for k in argmap if k in argument_types)
        self.context_vars = context_vars

        self.scrolling_cursor = scrolling_cursor
        self.offset = offset.index if offset is not None else None
        self.limit = limit.index if limit is not None else None
        self.query_type = query_type
        self.record_info = record_info
        self.output_format = output_format

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('text')
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.text = ''.join(self.chunks)

    def prepare(self, session):
        return PreparedQuery(self, session)

    def prepare_partial(self, session, **kwargs):
        return PreparedQuery(self, session, kwargs)

    def get_output_format_info(self):
        if self.output_format == 'json':
            return ('json', 1)
        else:
            return ('caosobj', 1)

    def get_output_metadata(self):
        return {'record_info': self.record_info}


class PreparedQuery:
    def __init__(self, query, session, args=None):
        self.query = query
        self.argmap = query.argmap

        self._session = session
        self._concept_map = session.backend.get_concept_map(session)
        self._constr_mech = session.backend.get_constr_mech()

        if args:
            text = self._embed_args(self.query, args)
            self.argmap = {}
        else:
            text = query.text

        exc_handler = functools.partial(ErrorMech._interpret_db_error,
                                        self._session, self._constr_mech)
        self.statement = session.get_prepared_statement(
                            text, raw=not query.scrolling_cursor,
                            exc_handler=exc_handler)
        self.init_args = args

        if query.record_info:
            for record in query.record_info:
                session.backend._register_record_info(record)

        # PreparedStatement.rows() is a streaming iterator that uses scrolling
        # cursor internally to stream data from the database.  Since PostgreSQL
        # only allows DECLARE with SELECT or VALUES, but not UPDATE ...
        # RETURNING or DELETE ... RETURNING, we must use a single transaction
        # fetch, that does not use cursors, for non-SELECT queries.
        if issubclass(self.query.query_type, pg_ast.SelectQueryNode):
            self._native_iter = self.statement.rows
        else:
            if self.query.scrolling_cursor:
                raise edgedb_error.EdgeDBError(
                    'cannot create scrolling cursor for non-SELECT query')

            self._native_iter = self.statement

    def _embed_args(self, query, args):
        qargs = self.convert_arguments(**args)
        quote = postgresql.string.quote_literal

        chunks = query.chunks[:]

        for i, arg in qargs.items():
            if isinstance(arg, (tuple, list)):
                arg = 'ARRAY[' + ', '.join(quote(str(a)) for a in arg) + ']'
            else:
                arg = quote(str(arg))
            for ai in query.arg_index[i]:
                chunks[ai] = arg

        return ''.join(chunks)

    def describe_output(self, session):
        return self.query.describe_output(session)

    def describe_arguments(self, session):
        return self.query.describe_arguments(session)

    def convert_arguments(self, **kwargs):
        return collections.OrderedDict(enumerate(self._convert_args(kwargs)))

    def rows(self, **kwargs):
        vars = self._convert_args(kwargs)

        try:
            if self.query.scrolling_cursor:
                return self._cursor_iterator(vars, **kwargs)
            else:
                return self._native_iter(*vars)

        except asyncpg.PostgresError as e:
            raise ErrorMech._interpret_db_error(
                self._session, self._constr_mech, e) from e

    __call__ = rows
    __iter__ = rows

    def first(self, **kwargs):
        vars = self._convert_args(kwargs)
        return self.statement.first(*vars)

    def _convert_args(self, kwargs):
        result = []
        for k in self.argmap:
            arg = kwargs.get(k)

            if (isinstance(arg, tuple) and arg and
                    isinstance(arg[0], type) and
                    isinstance(arg[0].__sx_prototype__, s_concepts.Concept)):
                ids = set()

                for cls in arg:
                    proto = cls.__sx_prototype__
                    children = proto.descendants(cls.__sx_protoschema__)
                    ids.add(self._concept_map[proto.name])
                    ids.update(self._concept_map[c.name] for c in children)

                arg = ids

            elif (isinstance(arg, type) and
                    isinstance(arg.__sx_prototype__, s_concepts.Concept)):
                proto = arg.__sx_prototype__
                children = proto.descendants(arg.__sx_protoschema__)
                arg = [self._concept_map[proto.name]]
                arg.extend(self._concept_map[c.name] for c in children)

            elif not isinstance(arg, type):
                proto = getattr(arg.__class__, '__sx_prototype__', None)
                if proto is not None and isinstance(proto, s_concepts.Concept):
                    arg = arg.id

            result.append(arg)

        return result

    def _cursor_iterator(self, vars, **kwargs):
        if not kwargs:
            kwargs = self.init_args

        if self.query.limit:
            limit = kwargs.pop(self.query.limit)
        else:
            limit = None

        if self.query.offset:
            offset = kwargs.pop(self.query.offset)
        else:
            offset = None

        return Cursor(self.statement.declare(*vars), offset, limit)


class ErrorMech:
    error_res = {
        asyncpg.IntegrityConstraintViolationError: collections.OrderedDict((
            ('link_mapping',
             re.compile(r'^.*"(?P<constr_name>.*_link_mapping_idx)".*$')),
            ('constraint',
             re.compile(r'^.*"(?P<constr_name>.*;schemaconstr(?:#\d+)?).*"$')),
            ('id',
             re.compile(r'^.*"(?P<constr_name>\w+)_data_pkey".*$')),
        ))
    }

    @classmethod
    def _interpret_db_error(cls, session, constr_mech, err):
        if isinstance(err, asyncpg.ICVError):
            connection = session.get_connection()
            proto_schema = session.proto_schema
            source = pointer = None

            for ecls, eres in cls.error_res.items():
                if isinstance(err, ecls):
                    break
            else:
                eres = {}

            error_info = None

            for type, ere in eres.items():
                m = ere.match(err.message)
                if m:
                    error_info = (type, m.group('constr_name'))
                    break
            else:
                return edgedb_error.UninterpretedStorageError(err.message)

            error_type, error_data = error_info

            if error_type == 'link_mapping':
                err = 'link mapping cardinality violation'
                errcls = edgedb_error.LinkMappingCardinalityViolationError
                return errcls(err, source=source, pointer=pointer)

            elif error_type == 'constraint':
                constraint_name = \
                    constr_mech.constraint_name_from_pg_name(
                        connection, error_data)

                if constraint_name is None:
                    return edgedb_error.UninterpretedStorageError(err.message)

                constraint = constraint_name
                # Unfortunately, Postgres does not include the offending
                # value in exceptions consistently.
                offending_value = None

                if pointer is not None:
                    error_source = pointer
                else:
                    error_source = source

                constraint.raise_error(offending_value, source=error_source)

            elif error_type == 'id':
                msg = 'unique link constraint violation'
                errcls = edgedb_error.PointerConstraintUniqueViolationError
                constraint = cls._get_id_constraint(proto_schema)
                return errcls(msg=msg, source=source, pointer=pointer,
                              constraint=constraint)
        else:
            return edgedb_error.UninterpretedStorageError(err.message)

    @classmethod
    def _get_id_constraint(cls, proto_schema):
        BObj = proto_schema.get('std.BaseObject')
        BObj_id = BObj.pointers['std.id']
        unique = proto_schema.get('std.unique')

        name = s_constr.Constraint.generate_specialized_name(
                BObj_id.name, unique.name)
        name = sn.Name(name=name, module='std')
        constraint = s_constr.Constraint(name=name, bases=[unique],
                                         subject=BObj_id)
        constraint.acquire_ancestor_inheritance(proto_schema)

        return constraint


class CaosQLAdapter:
    cache = {}

    def __init__(self, session):
        self.session = session
        self.connection = session.get_connection()
        self.transformer = IRCompiler()
        self.current_portal = None

    def transform(self, query, scrolling_cursor=False, context=None, *,
                  proto_schema, output_format=None):
        if scrolling_cursor:
            offset = query.offset
            limit = query.limit
        else:
            offset = limit = None

        if scrolling_cursor:
            query.offset = None
            query.limit = None

        qchunks, argmap, arg_index, query_type, record_info = \
            self.transformer.transform(query, self.session.backend,
                                       self.session.proto_schema,
                                       output_format=output_format)

        if scrolling_cursor:
            query.offset = offset
            query.limit = limit

        restypes = {}

        for k, v in query.result_types.items():
            if v[0] is not None: # XXX get_expr_type
                if isinstance(v[0], tuple):
                    typ = (v[0][0], v[0][1].name)
                else:
                    typ = v[0].name
                restypes[k] = (typ, v[1])
            else:
                restypes[k] = v

        argtypes = {}

        for k, v in query.argument_types.items():
            if v is not None: # XXX get_expr_type
                if isinstance(v, tuple):
                    name = 'type' if isinstance(v[1], s_obj.PrototypeClass) \
                            else v[1].name
                    argtypes[k] = (v[0], name)
                else:
                    name = 'type' if isinstance(v, s_obj.PrototypeClass) \
                           else v.name
                    argtypes[k] = name
            else:
                argtypes[k] = v

        return Query(chunks=qchunks, arg_index=arg_index, argmap=argmap,
                     result_types=restypes,
                     argument_types=argtypes, context_vars=query.context_vars,
                     scrolling_cursor=scrolling_cursor,
                     offset=offset, limit=limit, query_type=query_type,
                     record_info=record_info, output_format=output_format)


class Backend(s_deltarepo.DeltaProvider):

    typlen_re = re.compile(r"""
        (?P<type>.*) \( (?P<length>\d+ (?:\s*,\s*(\d+))*) \)$
    """, re.X)

    search_idx_name_re = re.compile(r"""
        .*_(?P<language>\w+)_(?P<index_class>\w+)_search_idx$
    """, re.X)

    link_source_colname = common.quote_ident(
                                common.caos_name_to_pg_name('std.source'))
    link_target_colname = common.quote_ident(
                                common.caos_name_to_pg_name('std.target'))

    def __init__(self, connection):
        self.features = None
        self.backend_info = None
        self.modules = None

        self.schema = so.ProtoSchema()

        self._constr_mech = schemamech.ConstraintMech()
        self._type_mech = schemamech.TypeMech()

        self.atom_cache = {}
        self.link_cache = {}
        self.link_property_cache = {}
        self.concept_cache = {}
        self.table_cache = {}
        self.domain_to_atom_map = {}
        self.table_id_to_proto_name_cache = {}
        self.proto_name_to_table_id_cache = {}
        self.attribute_link_map_cache = {}
        self._record_mapping_cache = {}

        self.parser = parser.PgSQLParser()
        self.search_idx_expr = astexpr.TextSearchExpr()
        self.type_expr = astexpr.TypeExpr()
        self.constant_expr = None

        self.connection = connection

        repo = pgsql_deltarepo.MetaDeltaRepository(self.connection)
        super().__init__(repo)

    def get_constr_mech(self):
        return self._constr_mech

    def init_connection(self, connection):
        need_upgrade = False

        if self.backend_info is None:
            self.backend_info = self.read_backend_info()

        bver = self.backend_info['format_version']

        if bver < delta_cmds.BACKEND_FORMAT_VERSION:
            need_upgrade = True
            self.upgrade_backend(connection)

        elif bver > delta_cmds.BACKEND_FORMAT_VERSION:
            msg = 'unsupported backend format version: {:d}'.format(
                self.backend_info['format_version'])
            details = 'The largest supported backend ' \
                      'format version is {:d}'.format(
                            delta_cmds.BACKEND_FORMAT_VERSION)
            raise s_err.SchemaError(msg, details=details)

        if need_upgrade:
            with connection.xact():
                self.upgrade_backend(connection)
                self._read_and_init_features(connection)
                self.getschema()
        else:
            self._read_and_init_features(connection)

    def reset_connection(self, connection):
        for feature_class_name in self.features.values():
            feature_class = get_object(feature_class_name)
            feature_class.reset_connection(connection)

    def _read_and_init_features(self, connection):
        if self.features is None:
            self.features = self.read_features(connection)
        self.init_features(connection)

    async def _init_introspection_cache(self):
        self.backend_info = await self.read_backend_info()

        if self.backend_info['initialized']:
            await self._type_mech.init_cache(self.connection)
            await self._constr_mech.init_cache(self.connection)
            t2pn, pn2t = await self._init_relid_cache()
            self.table_id_to_proto_name_cache = t2pn
            self.proto_name_to_table_id_cache = pn2t
            self.domain_to_atom_map = await self._init_atom_map_cache()
            # Concept map needed early for type filtering operations
            # in schema queries
            await self.get_concept_map(force_reload=True)

    async def _init_relid_cache(self):
        ds = introspection.tables.TableList(self.connection)
        link_tables = await ds.fetch(schema_name='edgedb%',
                                     table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        ds = introspection.types.TypesList(self.connection)
        records = await ds.fetch(schema_name='edgedb%', type_name='%_record',
                                 include_arrays=False)
        records = {(t['schema'], t['name']): t for t in records}

        ds = datasources.schema.links.ConceptLinks(self.connection)
        links_list = await ds.fetch()
        links_list = collections.OrderedDict(
                        (sn.Name(r['name']), r) for r in links_list)

        table_id_to_proto_name_cache = {}
        proto_name_to_table_id_cache = {}

        for link_name, link in links_list.items():
            link_table_name = common.link_name_to_table_name(
                                link_name, catenate=False)
            t = link_tables.get(link_table_name)
            if t:
                table_id_to_proto_name_cache[t['oid']] = link_name
                table_id_to_proto_name_cache[t['typoid']] = link_name
                proto_name_to_table_id_cache[link_name] = t['typoid']

        ds = introspection.tables.TableList(self.connection)
        tables = await ds.fetch(schema_name='edgedb%', table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        ds = datasources.schema.concepts.ConceptList(self.connection)
        concept_list = await ds.fetch()
        concept_list = collections.OrderedDict(
            (sn.Name(row['name']), row) for row in concept_list)

        for name, row in concept_list.items():
            table_name = common.concept_name_to_table_name(
                            name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept "%s" exists but the table is missing' % name
                raise s_err.SchemaError(msg, details=details)

            table_id_to_proto_name_cache[table['oid']] = name
            table_id_to_proto_name_cache[table['typoid']] = name
            proto_name_to_table_id_cache[name] = table['typoid']

        return table_id_to_proto_name_cache, proto_name_to_table_id_cache

    def table_name_to_object_name(self, table_name):
        return self.table_cache.get(table_name)

    async def _init_atom_map_cache(self):
        ds = introspection.domains.DomainsList(self.connection)
        domains = await ds.fetch(schema_name='edgedb%', domain_name='%_domain')
        domains = {(d['schema'], d['name']): self.normalize_domain_descr(d)
                   for d in domains}

        ds = datasources.schema.atoms.AtomList(self.connection)
        atom_list = await ds.fetch()

        domain_to_atom_map = {}

        for row in atom_list:
            name = sn.Name(row['name'])

            domain_name = common.atom_name_to_domain_name(name, catenate=False)

            domain = domains.get(domain_name)
            domain_to_atom_map[domain_name] = name

        return domain_to_atom_map

    async def upgrade_backend(self, connection):
        with self.connection.xact():
            context = delta_cmds.CommandContext(connection)
            upgrade = delta_cmds.UpgradeBackend(self.backend_info)
            await upgrade.execute(context)
            self.backend_info = self.read_backend_info()

    async def getschema(self):
        if not self.schema.modules:
            await self._init_introspection_cache()
            await self.read_modules(self.schema)
            await self.read_attributes(self.schema)
            await self.read_actions(self.schema)
            await self.read_events(self.schema)
            await self.read_atoms(self.schema)
            await self.read_concepts(self.schema)
            await self.read_links(self.schema)
            await self.read_link_properties(self.schema)
            await self.read_policies(self.schema)
            await self.read_attribute_values(self.schema)
            await self.read_constraints(self.schema)

            await self.order_attributes(self.schema)
            await self.order_actions(self.schema)
            await self.order_events(self.schema)
            await self.order_atoms(self.schema)
            await self.order_link_properties(self.schema)
            await self.order_links(self.schema)
            await self.order_concepts(self.schema)
            await self.order_policies(self.schema)

        return self.schema

    def adapt_delta(self, delta):
        return delta_cmds.CommandMeta.adapt(delta)

    @debug
    def process_delta(self, delta, schema, session=None):
        """LOG [edgedb.delta.plan] Delta Plan
            markup.dump(delta)
        """
        delta = self.adapt_delta(delta)
        connection = session.get_connection() if session else self.connection
        context = delta_cmds.CommandContext(connection, session=session)
        delta.apply(schema, context)
        """LOG [edgedb.delta.plan.pgsql] PgSQL Delta Plan
            markup.dump(delta)
        """
        return delta

    async def run_delta_command(self, delta):
        proto_schema = await self.getschema()

        # Apply and adapt delta, build native delta plan
        plan = self.process_delta(delta, proto_schema)

        context = delta_cmds.CommandContext(self.connection, None)

        try:
            await plan.execute(context)
        except Exception as e:
            msg = 'failed to apply delta to data backend'
            raise RuntimeError(msg) from e

        await self.invalidate_schema_cache()
        await self.getschema()

    @debug
    def apply_delta(self, delta, session, source_deltarepo):
        if isinstance(delta, sd.DeltaSet):
            deltas = list(delta)
        else:
            deltas = [delta]

        proto_schema = self.getschema()

        with session.transaction():
            old_conn = self.connection
            self.connection = session.get_connection()

            for d in deltas:
                """LINE [edgedb.delta.apply] Applying delta
                    '{:032x}'.format(d.id)
                """

                session.replace_schema(proto_schema)

                # Run preprocess pass
                d.call_hook(session, stage='preprocess', hook='main')

                if d.deltas:
                    delta = d.deltas[0]

                    # Apply and adapt delta, build native delta plan
                    plan = self.process_delta(delta, proto_schema)

                    # Reinitialize the session with the mutated schema
                    session.replace_schema(proto_schema)

                    context = delta_cmds.CommandContext(session.get_connection(), session)

                    try:
                        plan.execute(context)
                    except Exception as e:
                        msg = 'failed to apply delta {:032x} to data backend'.format(d.id)
                        self._raise_delta_error(msg, d, plan, e)

                    # Invalidate transient structure caches
                    self.invalidate_transient_cache()

                    try:
                        # Update introspection caches
                        self._init_introspection_cache()
                    except s_err.SchemaError as e:
                        msg = 'failed to verify metadata after applying delta {:032x} to data backend'
                        msg = msg.format(d.id)
                        self._raise_delta_error(msg, d, plan, e)

                # Run postprocess pass
                d.call_hook(session, stage='postprocess', hook='main')

                self.invalidate_schema_cache()

                try:
                    introspected_schema = self.getschema()
                except s_err.SchemaError as e:
                    msg = 'failed to verify metadata after applying delta {:032x} to data backend'
                    msg = msg.format(d.id)
                    self._raise_delta_error(msg, d, plan, e)

                introspected_checksum = introspected_schema.get_checksum()

                if introspected_checksum != d.checksum:
                    details = ('Schema checksum verification failed (expected "%x", got "%x") when '
                               'applying delta "%x".' % (d.checksum, introspected_checksum, d.id))
                    hint = 'This usually indicates a bug in backend delta adapter.'

                    expected_schema = source_deltarepo.get_schema(d)

                    delta_checksums = introspected_checksums = None

                    """LOG [edgedb.delta.recordchecksums]
                    delta_checksums = d.checksum_details
                    introspected_checksums = \
                        introspected_schema.get_checksum_details()
                    """

                    raise sd.DeltaChecksumError(
                            'could not apply schema delta'
                            'checksums do not match',
                            details=details, hint=hint,
                            schema1=expected_schema,
                            schema2=introspected_schema,
                            schema1_title='Expected Schema',
                            schema2_title='Schema in Backend',
                            checksums1=delta_checksums,
                            checksums2=introspected_checksums)

            self._update_repo(session, deltas)

            self.connection = old_conn

    def _raise_delta_error(self, msg, d, plan, e=None):
        hint = 'This usually indicates a bug in backend delta adapter.'
        d = sd.Delta(parent_id=d.parent_id, checksum=d.checksum,
                     comment=d.comment, deltas=[plan])
        raise sd.DeltaError(msg, delta=d) from e

    async def _update_repo(self, session, deltas):
        table = deltadbops.DeltaLogTable()
        records = []
        for d in deltas:
            rec = table.record(
                    id='%x' % d.id,
                    parents=['%x' % d.parent_id] if d.parent_id else None,
                    checksum='%x' % d.checksum,
                    committer=os.getenv('LOGNAME', '<unknown>')
                  )
            records.append(rec)

        context = delta_cmds.CommandContext(session.get_connection(), session)
        await dbops.Insert(table, records=records).execute(context)

        table = deltadbops.DeltaRefTable()
        rec = table.record(
                id='%x' % d.id,
                ref='HEAD'
              )
        condition = [('ref', str('HEAD'))]
        await dbops.Merge(table, record=rec, condition=condition).execute(context)

    async def invalidate_schema_cache(self):
        self.schema = so.ProtoSchema()
        self.backend_info = await self.read_backend_info()
        self.features = await self.read_features(self.connection)
        self.invalidate_transient_cache()

    def invalidate_transient_cache(self):
        self._constr_mech.invalidate_schema_cache()
        self._type_mech.invalidate_schema_cache()

        self.link_cache.clear()
        self.link_property_cache.clear()
        self.concept_cache.clear()
        self.atom_cache.clear()
        self.table_cache.clear()
        self.domain_to_atom_map.clear()
        self.table_id_to_proto_name_cache.clear()
        self.proto_name_to_table_id_cache.clear()
        self.attribute_link_map_cache.clear()


    def concept_name_from_id(self, id, session):
        concept = sn.Name('std.BaseObject')
        query = '''SELECT c.name
                   FROM
                       %s AS e
                       INNER JOIN edgedb.concept AS c ON c.id = e.concept_id
                   WHERE e."std.id" = $1
                ''' % (common.concept_name_to_table_name(concept))
        ps = session.get_prepared_statement(query)
        concept_name = ps.first(id)
        if concept_name:
            concept_name = sn.Name(concept_name)
        return concept_name


    def entity_from_row(self, session, record_info, links):
        if record_info.recursive_link:
            # Array representing a hierarchy connecting via cyclic link.
            # All entities have been initialized by now, but the recursive link is None at
            # this point and needs to be injected.
            return self._rebuild_tree_from_list(session, links['data'], record_info.recursive_link)

        concept_map = self.get_concept_map(session)
        concept_id = links.pop('id', None)

        if concept_id is None:
            # empty record
            return None

        real_concept = concept_map[concept_id]
        concept_cls = session.schema.get(real_concept)
        # Filter out foreign pointers that may have been included in the record
        # in a combined multi-target query.
        valid_link_names = {l[0] for l in concept_cls._iter_all_pointers()}

        links = {l: v for l, v in links.items()
                 if l in valid_link_names or getattr(l, 'direction', s_pointers.PointerDirection.Outbound)
                                             == s_pointers.PointerDirection.Inbound}

        return session._merge(links['std.id'], concept_cls, links)


    def _rebuild_tree_from_list(self, session, items, connecting_attribute):
        # Build a tree from a list of (parent, child_id) tuples, while
        # maintaining total order.
        #
        updates = {}
        uuid = session.schema.std.BaseObject.id

        toplevel = []

        if items:
            total_order = {item[str(connecting_attribute)]: i for i, item in enumerate(items)}

            for item in items:
                entity = item[str(connecting_attribute)]

                target_id = item['__target__']

                if target_id is not None:
                    target_id = uuid(target_id)

                    if target_id not in session:
                        # The items below us have been cut off by recursion depth limit
                        continue

                    target = session.get(target_id)

                    if item['__depth__'] == 0:
                        bisect.insort(toplevel, (total_order[target], target))
                    else:
                        try:
                            parent_updates = updates[entity.id]
                        except KeyError:
                            parent_updates = updates[entity.id] = []

                        # Use insort to maintain total order on each level
                        bisect.insort(parent_updates, (total_order[target], target))

                else:
                    # If it turns out to be a leaf node, make sure we force an empty set update
                    try:
                        parent_updates = updates[entity.id]
                    except KeyError:
                        parent_updates = updates[entity.id] = []

        for parent_id, items in updates.items():
            session._merge(parent_id, None, {connecting_attribute: (i[1] for i in items)})

        return [i[1] for i in toplevel]

    async def get_concept_map(self, force_reload=False):
        connection = self.connection

        if not self.concept_cache or force_reload:
            cl_ds = datasources.schema.concepts.ConceptList(connection)

            for row in await cl_ds.fetch():
                self.concept_cache[row['name']] = row['id']
                self.concept_cache[row['id']] = sn.Name(row['name'])

        return self.concept_cache

    def get_concept_id(self, concept):
        concept_id = None

        concept_cache = self.concept_cache
        if concept_cache:
            concept_id = concept_cache.get(concept.name)

        if concept_id is None:
            msg = 'could not determine backend id for concept in this context'
            details = 'Concept: {}'.format(concept.name)
            raise s_err.SchemaError(msg, details=details)

        return concept_id

    def source_name_from_relid(self, table_oid):
        return self.table_id_to_proto_name_cache.get(table_oid)

    def typrelid_for_source_name(self, source_name):
        return self.proto_name_to_table_id_cache.get(source_name)

    def compile(self, query_ir, scrolling_cursor=False, context=None, *,
                      output_format=None):
        if scrolling_cursor:
            offset = query_ir.offset
            limit = query_ir.limit
        else:
            offset = limit = None

        if scrolling_cursor:
            query_ir.offset = None
            query_ir.limit = None

        ir_compiler = IRCompiler()

        qchunks, argmap, arg_index, query_type, record_info = \
            ir_compiler.transform(query_ir, self, self.schema,
                                  output_format=output_format)

        if scrolling_cursor:
            query_ir.offset = offset
            query_ir.limit = limit

        restypes = {}

        for k, v in query_ir.result_types.items():
            if v[0] is not None:  # XXX get_expr_type
                if isinstance(v[0], tuple):
                    typ = (v[0][0], v[0][1].name)
                else:
                    typ = v[0].name
                restypes[k] = (typ, v[1])
            else:
                restypes[k] = v

        argtypes = {}

        for k, v in query_ir.argument_types.items():
            if v is not None:  # XXX get_expr_type
                if isinstance(v, tuple):
                    name = 'type' if isinstance(v[1], s_obj.PrototypeClass) \
                            else v[1].name
                    argtypes[k] = (v[0], name)
                else:
                    name = 'type' if isinstance(v, s_obj.PrototypeClass) \
                           else v.name
                    argtypes[k] = name
            else:
                argtypes[k] = v

        return Query(chunks=qchunks, arg_index=arg_index, argmap=argmap,
                     result_types=restypes,
                     argument_types=argtypes,
                     context_vars=query_ir.context_vars,
                     scrolling_cursor=scrolling_cursor,
                     offset=offset, limit=limit, query_type=query_type,
                     record_info=record_info, output_format=output_format)

    async def read_modules(self, schema):
        ds = introspection.schemas.SchemasList(self.connection)
        schemas = await ds.fetch(schema_name='edgedb%')
        schemas = {s['name'] for s in schemas
                             if not s['name'].startswith('caos_aux_')}

        ds = datasources.schema.modules.ModuleList(self.connection)
        modules = await ds.fetch()
        modules = {m['schema_name']:
                        {'name': m['name'], 'imports': m['imports']}
                   for m in modules}

        recorded_schemas = set(modules.keys())

        # Sanity checks
        extra_schemas = schemas - recorded_schemas - {'edgedb'}
        missing_schemas = recorded_schemas - schemas

        if extra_schemas:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data schemas exist: %s' \
                        % (', '.join('"%s"' % s for s in extra_schemas))
            raise s_err.SchemaError(msg, details=details)

        if missing_schemas:
            msg = 'internal metadata incosistency'
            details = 'Missing schemas for modules: %s' \
                        % (', '.join('"%s"' % s for s in extra_schemas))
            raise s_err.SchemaError(msg, details=details)

        mods = []

        for module in modules.values():
            mod = s_mod.ProtoModule(
                    name=module['name'],
                    imports=frozenset(module['imports'] or ()))
            self.schema.add_module(mod)
            mods.append(mod)

        for mod in mods:
            for imp_name in mod.imports:
                if not self.schema.has_module(imp_name):
                    # Must be a foreign module, import it directly
                    try:
                        impmod = importlib.import_module(imp_name)
                    except ImportError:
                        # Module has moved, create a dummy
                        impmod = so.DummyModule(imp_name)

                    self.schema.add_module(impmod)

    async def read_features(self, connection):
        try:
            ds = datasources.schema.features.FeatureList(connection)
            features = await ds.fetch()
            return {f['name']: f['class_name'] for f in features}
        except (asyncpg.SchemaNameError, asyncpg.UndefinedTableError):
            return {}

    async def read_backend_info(self):
        ds = datasources.schema.backend_info.BackendInfo(self.connection)
        info = await ds.fetch()
        info = dict(info[0].items())
        info['initialized'] = True
        return info

    async def read_atoms(self, schema):
        ds = introspection.domains.DomainsList(self.connection)
        domains = await ds.fetch(schema_name='edgedb%', domain_name='%_domain')
        domains = {(d['schema'], d['name']): self.normalize_domain_descr(d)
                   for d in domains}

        ds = introspection.sequences.SequencesList(self.connection)
        seqs = await ds.fetch(schema_name='edgedb%',
                              sequence_pattern='%_sequence')
        seqs = {(s['schema'], s['name']): s for s in seqs}

        seen_seqs = set()

        ds = datasources.schema.atoms.AtomList(self.connection)
        atom_list = await ds.fetch()

        basemap = {}

        for row in atom_list:
            name = sn.Name(row['name'])

            atom_data = {
                'name': name,
                'title': self.hstore_to_word_combination(row['title']),
                'description': row['description'],
                'is_abstract': row['is_abstract'],
                'is_final': row['is_final'],
                'base': row['base'],
                'default': row['default'],
                'attributes': row['attributes'] or {}
            }

            self.atom_cache[name] = atom_data
            atom_data['default'] = self.unpack_default(row['default'])

            if atom_data['base']:
                basemap[name] = atom_data['base']

            atom = s_atoms.Atom(name=name,
                                default=atom_data['default'],
                                title=atom_data['title'],
                                description=atom_data['description'],
                                is_abstract=atom_data['is_abstract'],
                                is_final=atom_data['is_final'],
                                attributes=atom_data['attributes'])

            schema.add(atom)

        for atom in schema('atom'):
            try:
                basename = basemap[atom.name]
            except KeyError:
                pass
            else:
                atom.bases = [schema.get(sn.Name(basename))]

        sequence = schema.get('std.sequence', None)
        for atom in schema('atom'):
            if sequence is not None and atom.issubclass(sequence):
                seq_name = common.atom_name_to_sequence_name(
                                atom.name, catenate=False)
                if seq_name not in seqs:
                    msg = 'internal metadata incosistency'
                    details = 'Missing sequence for sequence atom {!r}'.format(
                        atom.name)
                    raise s_err.SchemaError(msg, details=details)
                seen_seqs.add(seq_name)

        extra_seqs = set(seqs) - seen_seqs
        if extra_seqs:
            msg = 'internal metadata incosistency'
            details = 'Extraneous sequences exist: %s' \
                        % (', '.join(common.qname(*t) for t in extra_seqs))
            raise s_err.SchemaError(msg, details=details)

    async def order_atoms(self, schema):
        for atom in schema(type='atom'):
            atom.acquire_ancestor_inheritance(schema)

    async def read_constraints(self, schema):
        ds = datasources.schema.constraints.Constraints(self.connection)
        constraints_list = await ds.fetch()
        constraints_list = collections.OrderedDict((sn.Name(r['name']), r)
                                                    for r in constraints_list)

        basemap = {}

        for name, r in constraints_list.items():
            bases = tuple()

            if r['subject']:
                bases = (s_constr.Constraint.normalize_name(name),)
            elif r['base']:
                bases = tuple(sn.Name(b) for b in r['base'])
            elif name != 'std.constraint':
                bases = (sn.Name('std.constraint'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            subject = schema.get(r['subject']) if r['subject'] else None

            basemap[name] = bases

            if r['paramtypes']:
                paramtypes = {n: self.unpack_typeref(v, schema)
                              for n, v in r['paramtypes'].items()}
            else:
                paramtypes = None

            if r['inferredparamtypes']:
                inferredparamtypes = {
                    n: self.unpack_typeref(v, schema)
                    for n, v in r['inferredparamtypes'].items()}
            else:
                inferredparamtypes = None

            if r['args']:
                args = pickle.loads(r['args'])
            else:
                args = None

            constraint = s_constr.Constraint(
                name=name, subject=subject,
                title=title, description=description,
                is_abstract=r['is_abstract'],
                is_final=r['is_final'],
                expr=r['expr'],
                subjectexpr=r['subjectexpr'],
                localfinalexpr=r['localfinalexpr'],
                finalexpr=r['finalexpr'],
                errmessage=r['errmessage'],
                paramtypes=paramtypes,
                inferredparamtypes=inferredparamtypes,
                args=args)

            if subject:
                subject.add_constraint(constraint)

            schema.add(constraint)

        for constraint in schema(type='constraint'):
            try:
                bases = basemap[constraint.name]
            except KeyError:
                pass
            else:
                constraint.bases = [schema.get(b) for b in bases]

        for constraint in schema(type='constraint'):
            constraint.acquire_ancestor_inheritance(schema)

    async def order_constraints(self, schema):
        pass

    def unpack_typeref(self, typeref, protoschema):
        try:
            collection_type, type = s_obj.TypeRef.parse(typeref)
        except ValueError as e:
            raise s_err.SchemaError(e.args[0]) from None

        if type is not None:
            type = protoschema.get(type)

        if collection_type is not None:
            type = collection_type(element_type=type)

        return type

    def unpack_default(self, value):
        result = None
        if value is not None:
            val = json.loads(value)
            if val['type'] == 'expr':
                result = s_expr.ExpressionText(val['value'])
            else:
                result = val['value']
        return result

    def interpret_search_index(self, index):
        m = self.search_idx_name_re.match(index.name)
        if not m:
            msg = 'could not interpret index {}'.format(index.name)
            raise s_err.SchemaError(msg)

        language = m.group('language')
        index_class = m.group('index_class')

        tree = self.parser.parse(index.expr)
        columns = self.search_idx_expr.match(tree)

        if columns is None:
            msg = 'could not interpret index {!r}'.format(str(index.name))
            details = 'Could not match expression:\n{}'.format(
                        markup.dumps(tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise s_err.SchemaError(msg, details=details, hint=hint)

        return index_class, language, columns

    def interpret_search_indexes(self, table_name, indexes):
        for idx_data in indexes:
            index = dbops.Index.from_introspection(table_name, idx_data)
            yield self.interpret_search_index(index)

    async def read_search_indexes(self):
        indexes = {}
        index_ds = datasources.introspection.tables.TableIndexes(
            self.connection)
        idx_data = await index_ds.fetch(schema_pattern='edgedb%',
                                        index_pattern='%_search_idx')

        for row in idx_data:
            table_name = tuple(row['table_name'])
            tabidx = indexes[table_name] = {}

            si = self.interpret_search_indexes(table_name, row['indexes'])

            for index_class, language, columns in si:
                for column_name, column_config in columns.items():
                    idx = tabidx.setdefault(column_name, {})
                    idx[(index_class, column_config[0])] = \
                        s_links.LinkSearchWeight(column_config[1])

        return indexes

    def interpret_index(self, index):
        index_expression = index.expr

        if not index_expression:
            index_expression = '(%s)' % ', '.join(common.quote_ident(c) for
                                                  c in index.columns)

        return self.parser.parse(index_expression)

    def interpret_indexes(self, table_name, indexes):
        for idx_data in indexes:
            idx = dbops.Index.from_introspection(table_name, idx_data)
            yield idx, self.interpret_index(idx)

    async def read_indexes(self):
        indexes = {}
        index_ds = datasources.introspection.tables.TableIndexes(
            self.connection)
        idx_data = await index_ds.fetch(schema_pattern='edgedb%',
                                        index_pattern='%_reg_idx')

        for row in idx_data:
            table_name = tuple(row['table_name'])
            indexes[table_name] = set(self.interpret_indexes(table_name,
                                                             row['indexes']))

        return indexes

    def interpret_sql(self, expr, source=None):
        try:
            expr_tree = self.parser.parse(expr)
        except parser.PgSQLParserError as e:
            msg = 'could not interpret constant expression "%s"' % expr
            details = 'Syntax error when parsing expression: %s' % e.args[0]
            raise s_err.SchemaError(msg, details=details) from e

        if not self.constant_expr:
            self.constant_expr = astexpr.ConstantExpr()

        result = self.constant_expr.match(expr_tree)

        if result is None:
            sql_decompiler = transformer.Decompiler()
            caos_tree = sql_decompiler.transform(expr_tree, source)
            caosql_tree = caosql.decompile_ir(caos_tree, return_statement=True)
            result = caosql.generate_source(caosql_tree, pretty=False)
            result = s_expr.ExpressionText(result)

        return result

    async def read_pointer_target_column(self, schema, pointer,
                                         constraints_cache):
        ptr_stor_info = types.get_pointer_storage_info(
                            pointer, schema=schema, resolve_type=False)
        cols = await self._type_mech.get_table_columns(
            ptr_stor_info.table_name, connection=self.connection)

        col = cols.get(ptr_stor_info.column_name)

        if not col:
            msg = 'internal metadata inconsistency'
            details = ('Record for {!r} hosted by {!r} exists, but ' +
                       'the corresponding table column is missing').format(
                            pointer.normal_name(), pointer.source.name)
            raise s_err.SchemaError(msg, details=details)

        return self._get_pointer_column_target(
            schema, pointer.source, pointer.normal_name(), col)

    def _get_pointer_column_target(self, schema, source, pointer_name, col):
        if col['column_type_schema'] == 'pg_catalog':
            col_type_schema = common.caos_module_name_to_schema_name('std')
            col_type = col['column_type_formatted']
        else:
            col_type_schema = col['column_type_schema']
            col_type = col['column_type_formatted'] or col['column_type']

        if col['column_default'] is not None:
            atom_default = self.interpret_sql(col['column_default'], source)
        else:
            atom_default = None

        target = self.atom_from_pg_type(col_type, col_type_schema,
                                        atom_default, schema)

        return target, col['column_required']

    def _get_pointer_attribute_target(self, schema, source,
                                      pointer_name, attr):
        if attr['attribute_type_schema'] == 'pg_catalog':
            col_type_schema = common.caos_module_name_to_schema_name('std')
            col_type = attr['attribute_type_formatted']
        else:
            col_type_schema = attr['attribute_type_schema']
            col_type = attr['attribute_type_formatted'] or \
                            attr['attribute_type']

        if attr['attribute_default'] is not None:
            atom_default = self.interpret_sql(
                attr['attribute_default'], source)
        else:
            atom_default = None

        if attr['attribute_type_composite_id']:
            # composite record
            source_name = self.source_name_from_relid(
                attr['attribute_type_composite_id'])
            target = schema.get(source_name)
        else:
            target = self.atom_from_pg_type(col_type, col_type_schema,
                                            atom_default, schema)

        return target, attr['attribute_required']

    def verify_ptr_const_defaults(self, schema, ptr, tab_default):
        schema_default = None

        if ptr.default is not None:
            if isinstance(ptr.default, s_expr.ExpressionText):
                default_value = schemamech.ptr_default_to_col_default(
                    schema, ptr, ptr.default)
                if default_value is not None:
                    schema_default = ptr.default
            else:
                schema_default = ptr.default

        if tab_default is None:
            if schema_default:
                msg = 'internal metadata inconsistency'
                details = ('Literal default for pointer {!r} is present in ' +
                           'the schema, but not in the table').format(ptr.name)
                raise s_err.SchemaError(msg, details=details)
            else:
                return

        table_default = self.interpret_sql(tab_default, ptr.source)

        if tab_default is not None and not ptr.default:
            msg = 'internal metadata inconsistency'
            details = ('Literal default for pointer {!r} is present in ' +
                       'the table, but not in schema declaration').format(
                            ptr.name)
            raise s_err.SchemaError(msg, details=details)

        if not isinstance(table_default, s_expr.ExpressionText):
            typ = ptr.target.get_topmost_base()
            typ_t = s_types.BaseTypeMeta.get_implementation(typ.name)
            assert typ_t, 'missing implementation for {}'.format(typ.name)
            table_default = typ_t(table_default)
            schema_default = typ_t(schema_default)

        if schema_default != table_default:
            msg = 'internal metadata inconsistency'
            details = (
                'Value mismatch in literal default pointer link ' +
                '{!r}: {!r} in the table vs. {!r} in the schema'
            ).format(ptr.name, table_default, schema_default)
            raise s_err.SchemaError(msg, details=details)

    async def read_links(self, schema):
        ds = introspection.tables.TableList(self.connection)
        link_tables = await ds.fetch(schema_name='edgedb%',
                                     table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        ds = datasources.schema.links.ConceptLinks(self.connection)
        links_list = await ds.fetch()
        links_list = collections.OrderedDict(
            (sn.Name(r['name']), r) for r in links_list)

        concept_indexes = await self.read_search_indexes()
        basemap = {}

        for name, r in links_list.items():
            bases = tuple()

            if r['source_id']:
                bases = (s_links.Link.normalize_name(name),)
            elif r['base']:
                bases = tuple(sn.Name(b) for b in r['base'])
            elif name != 'std.link':
                bases = (sn.Name('std.link'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']

            source = schema.get(r['source']) if r['source'] else None
            target = schema.get(r['target']) if r['target'] else None
            if r['spectargets']:
                spectargets = [schema.get(t) for t in r['spectargets']]
            else:
                spectargets = None

            default = self.unpack_default(r['default'])

            required = r['required']

            if r['loading']:
                loading = s_pointers.PointerLoading(r['loading'])
            else:
                loading = None

            if r['exposed_behaviour']:
                exposed_behaviour = \
                    s_pointers.PointerExposedBehaviour(r['exposed_behaviour'])
            else:
                exposed_behaviour = None

            basemap[name] = bases

            link = s_links.Link(
                name=name, source=source, target=target,
                spectargets=spectargets,
                mapping=s_links.LinkMapping(r['mapping']),
                exposed_behaviour=exposed_behaviour,
                required=required,
                title=title, description=description,
                is_abstract=r['is_abstract'],
                is_final=r['is_final'],
                readonly=r['readonly'],
                loading=loading,
                default=default)

            if spectargets:
                # Multiple specified targets,
                # target is a virtual derived object
                target = link.create_common_target(schema, spectargets)

            link_search = None

            if isinstance(target, s_atoms.Atom):
                target, required = await self.read_pointer_target_column(
                                            schema, link, None)

                concept_schema, concept_table = \
                    common.concept_name_to_table_name(source.name,
                                                      catenate=False)

                indexes = concept_indexes.get((concept_schema, concept_table))

                if indexes:
                    col_search_index = indexes.get(bases[0])
                    if col_search_index:
                        weight = col_search_index[('default', 'english')]
                        link_search = s_links.LinkSearchConfiguration(
                                        weight=weight)

            link.target = target

            if link_search:
                link.search = link_search

            if source:
                source.add_pointer(link)

            schema.add(link)

        for link in schema(type='link'):
            try:
                bases = basemap[link.name]
            except KeyError:
                pass
            else:
                link.bases = [schema.get(b) for b in bases]

        for link in schema(type='link'):
            link.acquire_ancestor_inheritance(schema)

    async def order_links(self, schema):
        indexes = await self.read_indexes()

        sql_decompiler = transformer.Decompiler()

        g = {}

        for link in schema(type='link'):
            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.bases:
                g[link.name]['merge'].extend(b.name for b in link.bases)

        topological.normalize(g, merger=s_links.Link.merge, schema=schema)

        for link in schema(type='link'):
            link.finalize(schema)

        for link in schema(type='link'):
            if link.generic():
                table_name = common.get_table_name(link, catenate=False)
                tabidx = indexes.get(table_name)
                if tabidx:
                    for index, index_sql in tabidx:
                        if index.get_metadata('ddl:inherited'):
                            continue

                        caos_tree = sql_decompiler.transform(
                                        index_sql, link)
                        caosql_tree = caosql.decompile_ir(
                                        caos_tree, return_statement=True)
                        expr = caosql.generate_source(caosql_tree,
                                                      pretty=False)
                        schema_name = index.get_metadata('schemaname')
                        index = s_indexes.SourceIndex(
                                    name=sn.Name(schema_name),
                                    subject=link, expr=expr)
                        link.add_index(index)
                        schema.add(index)
            elif link.atomic():
                ptr_stor_info = types.get_pointer_storage_info(
                                    link, schema=schema)
                cols = await self._type_mech.get_table_columns(
                                ptr_stor_info.table_name,
                                connection=self.connection)
                col = cols[ptr_stor_info.column_name]
                self.verify_ptr_const_defaults(
                    schema, link, col['column_default'])

    async def read_link_properties(self, schema):
        ds = datasources.schema.links.LinkProperties(self.connection)
        link_props = await ds.fetch()
        link_props = collections.OrderedDict(
            (sn.Name(r['name']), r) for r in link_props)
        basemap = {}

        for name, r in link_props.items():
            bases = ()

            if r['source_id']:
                bases = (s_lprops.LinkProperty.normalize_name(name),)
            elif r['base']:
                bases = tuple(sn.Name(b) for b in r['base'])
            elif name != 'std.link_property':
                bases = (sn.Name('std.link_property'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = schema.get(r['source']) if r['source'] else None

            default = self.unpack_default(r['default'])

            required = r['required']
            target = None

            if r['loading']:
                loading = s_pointers.PointerLoading(r['loading'])
            else:
                loading = None

            basemap[name] = bases

            prop = s_lprops.LinkProperty(
                name=name,
                source=source, target=target,
                required=required,
                title=title, description=description,
                readonly=r['readonly'],
                loading=loading,
                default=default)

            if source and bases[0] not in {'std.target',
                                           'std.source'}:
                # The property is attached to a link, check out
                # link table columns for target information.
                target, required = \
                    await self.read_pointer_target_column(schema, prop, None)
            else:
                if bases:
                    if bases[0] == 'std.target' and source is not None:
                        target = source.target
                    elif bases[0] == 'std.source' and source is not None:
                        target = source.source

            prop.target = target

            if source:
                prop.acquire_ancestor_inheritance(schema)
                source.add_pointer(prop)

            schema.add(prop)

        for prop in schema('link_property'):
            try:
                bases = basemap[prop.name]
            except KeyError:
                pass
            else:
                prop.bases = [schema.get(b, type=s_lprops.LinkProperty)
                              for b in bases]

    async def order_link_properties(self, schema):
        g = {}

        for prop in schema(type='link_property'):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}
            if prop.bases:
                g[prop.name]['merge'].extend(b.name for b in prop.bases)

        topological.normalize(g, merger=s_lprops.LinkProperty.merge,
                              schema=schema)

        for prop in schema(type='link_property'):
            if not prop.generic() and prop.source.generic():
                source_table_name = common.get_table_name(prop.source,
                                                          catenate=False)
                cols = await self._type_mech.get_table_columns(
                    source_table_name, connection=self.connection)
                col_name = common.caos_name_to_pg_name(prop.normal_name())
                col = cols[col_name]
                self.verify_ptr_const_defaults(
                    schema, prop, col['column_default'])

    async def read_attributes(self, schema):
        attributes_ds = datasources.schema.attributes.Attributes(
            self.connection)
        attributes = await attributes_ds.fetch()

        for r in attributes:
            name = sn.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            type = pickle.loads(r['type'])

            attribute = s_attrs.Attribute(
                name=name, title=title, description=description, type=type)
            schema.add(attribute)

    async def order_attributes(self, schema):
        pass

    async def read_attribute_values(self, schema):
        attributes_ds = datasources.schema.attributes.AttributeValues(
            self.connection)
        attributes = await attributes_ds.fetch()

        for r in attributes:
            name = sn.Name(r['name'])
            subject = schema.get(r['subject_name'])
            attribute = schema.get(r['attribute_name'])
            value = pickle.loads(r['value'])

            attribute = s_attrs.AttributeValue(
                name=name, subject=subject, attribute=attribute, value=value)
            subject.add_attribute(attribute)
            schema.add(attribute)

    async def read_actions(self, schema):
        actions_ds = datasources.schema.policy.Actions(self.connection)
        actions = await actions_ds.fetch()

        for r in actions:
            name = sn.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']

            action = s_policy.Action(name=name, title=title,
                                     description=description)
            schema.add(action)

    async def order_actions(self, schema):
        pass

    async def read_events(self, schema):
        events_ds = datasources.schema.policy.Events(self.connection)
        events = await events_ds.fetch()

        basemap = {}

        for r in events:
            name = sn.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']

            if r['base']:
                bases = tuple(sn.Name(b) for b in r['base'])
            elif name != 'std.event':
                bases = (sn.Name('std.event'),)
            else:
                bases = tuple()

            basemap[name] = bases

            event = s_policy.Event(name=name, title=title,
                                   description=description)
            schema.add(event)

        for event in schema(type='event'):
            try:
                bases = basemap[event.name]
            except KeyError:
                pass
            else:
                event.bases = [schema.get(b) for b in bases]

        for event in schema(type='event'):
            event.acquire_ancestor_inheritance(schema)

    async def order_events(self, schema):
        pass

    async def read_policies(self, schema):
        policies_ds = datasources.schema.policy.Policies(self.connection)
        policies = await policies_ds.fetch()

        for r in policies:
            name = sn.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            policy = s_policy.Policy(
                name=name, title=title, description=description,
                subject=schema.get(r['subject']),
                event=schema.get(r['event']),
                actions=[schema.get(a) for a in r['actions']])
            schema.add(policy)
            policy.subject.add_policy(policy)

    async def order_policies(self, schema):
        pass

    async def get_type_attributes(self, type_name, connection=None,
                                  cache='auto'):
        return await self._type_mech.get_type_attributes(
            type_name, connection, cache)

    async def read_concepts(self, schema):
        ds = introspection.tables.TableList(self.connection)
        tables = await ds.fetch(schema_name='edgedb%', table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        ds = datasources.schema.concepts.ConceptList(self.connection)
        concept_list = await ds.fetch()
        concept_list = collections.OrderedDict(
            (sn.Name(row['name']), row) for row in concept_list)

        visited_tables = set()

        self.table_cache.update({
            common.concept_name_to_table_name(n, catenate=False): c
            for n, c in concept_list.items()})

        basemap = {}

        for name, row in concept_list.items():
            concept = {'name': name,
                       'title': self.hstore_to_word_combination(row['title']),
                       'description': row['description'],
                       'is_abstract': row['is_abstract'],
                       'is_final': row['is_final']}

            table_name = common.concept_name_to_table_name(name,
                                                           catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept {!r} exists but ' \
                          'the table is missing'.format(name)
                raise s_err.SchemaError(msg, details=details)

            visited_tables.add(table_name)

            bases = await self.pg_table_inheritance_to_bases(
                            table['name'], table['schema'],
                            self.table_cache)

            basemap[name] = bases

            concept = s_concepts.Concept(name=name, title=concept['title'],
                                         description=concept['description'],
                                         is_abstract=concept['is_abstract'],
                                         is_final=concept['is_final'])

            schema.add(concept)

        for concept in schema('concept'):
            try:
                bases = basemap[concept.name]
            except KeyError:
                pass
            else:
                concept.bases = [schema.get(b) for b in bases]

        tabdiff = set(tables.keys()) - visited_tables
        if tabdiff:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data tables exist: %s' \
                        % (', '.join('"%s.%s"' % t for t in tabdiff))
            raise s_err.SchemaError(msg, details=details)

    async def order_concepts(self, schema):
        indexes = await self.read_indexes()

        sql_decompiler = transformer.Decompiler()

        g = {}
        for concept in schema(type='concept'):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.bases:
                g[concept.name]["merge"].extend(b.name for b in concept.bases)

        topological.normalize(g, merger=s_concepts.Concept.merge,
                              schema=schema)

        for concept in schema(type='concept'):
            concept.finalize(schema)

            table_name = common.get_table_name(concept, catenate=False)

            tabidx = indexes.get(table_name)
            if tabidx:
                for index, index_sql in tabidx:
                    if index.get_metadata('ddl:inherited'):
                        continue

                    ir_tree = sql_decompiler.transform(
                                    index_sql, concept)
                    caosql_tree = caosql.decompile_ir(
                                    ir_tree, return_statement=True)
                    expr = caosql.generate_source(caosql_tree, pretty=False)
                    schema_name = index.get_metadata('schemaname')
                    index = s_indexes.SourceIndex(name=sn.Name(schema_name),
                                                  subject=concept,
                                                  expr=expr)
                    concept.add_index(index)
                    schema.add(index)

    def normalize_domain_descr(self, d):
        if d['basetype'] is not None:
            typname, typmods = self.parse_pg_type(d['basetype_full'])
            result = self.pg_type_to_atom_name_and_constraints(
                typname, typmods)
            if result:
                base, constr = result

        if d['default'] is not None:
            d['default'] = self.interpret_sql(d['default'])

        return d

    async def pg_table_inheritance(self, table_name, schema_name):
        inheritance = introspection.tables.TableInheritance(self.connection)
        inheritance = await inheritance.fetch(table_name=table_name,
                                              schema_name=schema_name,
                                              max_depth=1)
        return tuple(i[:2] for i in inheritance[1:])

    async def pg_table_inheritance_to_bases(self, table_name, schema_name,
                                            table_to_concept_map):
        bases = []

        for table in await self.pg_table_inheritance(table_name, schema_name):
            base = table_to_concept_map[tuple(table[:2])]
            bases.append(base['name'])

        return tuple(bases)

    def parse_pg_type(self, type_expr):
        tree = self.parser.parse('None::' + type_expr)
        typname, typmods = self.type_expr.match(tree)
        return typname, typmods

    def pg_type_to_atom_name_and_constraints(self, typname, typmods):
        typeconv = types.base_type_name_map_r.get(typname)
        if typeconv:
            if isinstance(typeconv, sn.Name):
                name = typeconv
                constraints = ()
            else:
                name, constraints = typeconv(self.connection, typname,
                                             *typmods)
            return name, constraints
        return None

    def atom_from_pg_type(self, type_expr, atom_schema, atom_default, schema):

        typname, typmods = self.parse_pg_type(type_expr)
        if isinstance(typname, tuple):
            domain_name = typname[-1]
        else:
            domain_name = typname
            if atom_schema != common.caos_module_name_to_schema_name('std'):
                typname = (atom_schema, typname)
        atom_name = self.domain_to_atom_map.get((atom_schema, domain_name))

        if atom_name:
            atom = schema.get(atom_name, None)
        else:
            atom = None

        if not atom:

            typeconv = self.pg_type_to_atom_name_and_constraints(
                typname, typmods)
            if typeconv:
                name, _ = typeconv
                atom = schema.get(name)
                atom.acquire_ancestor_inheritance(schema)

        assert atom
        return atom

    def hstore_to_word_combination(self, hstore):
        if hstore:
            return morphology.WordCombination.from_dict(hstore)
        else:
            return None

    def _register_record_info(self, record_info):
        self._record_mapping_cache[record_info.id] = record_info

    def _get_record_info_by_id(self, record_id):
        return self._record_mapping_cache.get(record_id)
