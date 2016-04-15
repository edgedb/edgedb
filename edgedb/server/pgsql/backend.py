##
# Copyright (c) 2008-2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import bisect
import collections
import importlib
import itertools
import json
import os
import pickle
import re
import struct
import uuid

import postgresql
import postgresql.copyman
from postgresql.types.io import lib as pg_io_lib
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from metamagic.utils import ast
from importkit.import_ import get_object
from metamagic.utils.algos import topological, persistent_hash
from metamagic.utils.debug import debug
from metamagic.utils.nlang import morphology
from metamagic.utils import datastructures, markup

from metamagic import caos

from metamagic.caos import backends
from metamagic.caos import proto
from metamagic.caos import types as caos_types
from metamagic.caos import delta as base_delta
from metamagic.caos import debug as caos_debug
from metamagic.caos import error as caos_error
from metamagic.caos import schema as so

from metamagic.caos import caosql

from metamagic.caos.backends import query as backend_query
from metamagic.caos.backends.pgsql import common
from metamagic.caos.backends.pgsql import dbops
from metamagic.caos.backends.pgsql import delta as delta_cmds
from metamagic.caos.backends.pgsql import deltadbops
from metamagic.caos.backends.pgsql import driver

from . import datasources
from .datasources import introspection

from .transformer import IRCompiler

from . import ast as pg_ast
from . import astexpr
from . import parser
from . import types
from . import transformer
from . import pool
from . import session
from . import schemamech


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
                 context_vars, scrolling_cursor=False, offset=None, limit=None, query_type=None,
                 record_info=None, output_format=None):
        self.chunks = chunks
        self.text = ''.join(chunks)
        self.argmap = argmap
        self.arg_index = arg_index
        self.result_types = result_types
        self.argument_types = collections.OrderedDict((k, argument_types[k]) for k in argmap
                                                      if k in argument_types)
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
        if self.output_format == caos.types.JsonOutputFormat:
            return driver.JSON_OUTPUT_FORMAT
        else:
            return ('caosobj', 1)

    def get_output_metadata(self):
        return {'record_info': self.record_info}


class PreparedQuery:
    def __init__(self, query, session, args=None):
        self.query = query
        self.argmap = query.argmap

        self._concept_map = session.backend.get_concept_map(session)

        if args:
            text = self._embed_args(self.query, args)
            self.argmap = {}
        else:
            text = query.text

        self.statement = session.get_prepared_statement(text, raw=not query.scrolling_cursor)
        self.init_args = args

        if query.record_info:
            for record in query.record_info:
                session.backend._register_record_info(record)

        # PreparedStatement.rows() is a streaming iterator that uses scrolling cursor
        # internally to stream data from the database.  Since PostgreSQL only allows DECLARE
        # with SELECT or VALUES, but not UPDATE ... RETURNING or DELETE ... RETURNING, we
        # must use a single transaction fetch, that does not use cursors, for non-SELECT
        # queries.
        if issubclass(self.query.query_type, pg_ast.SelectQueryNode):
            self._native_iter = self.statement.rows
        else:
            if self.query.scrolling_cursor:
                raise caos_error.CaosError('cannot create scrolling cursor for non-SELECT query')

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

        if self.query.scrolling_cursor:
            return self._cursor_iterator(vars, **kwargs)
        else:
            return self._native_iter(*vars)

    __call__ = rows
    __iter__ = rows

    def first(self, **kwargs):
        vars = self._convert_args(kwargs)
        return self.statement.first(*vars)

    def _convert_args(self, kwargs):
        result = []
        for k in self.argmap:
            arg = kwargs.get(k)
            if isinstance(arg, caos.types.ConceptObject):
                arg = arg.id
            elif isinstance(arg, caos.types.ConceptClass):
                proto = caos.types.prototype(arg)
                children = proto.descendants(arg.__sx_protoschema__)
                arg = [self._concept_map[proto.name]]
                arg.extend(self._concept_map[c.name] for c in children)
            elif isinstance(arg, tuple) and arg and isinstance(arg[0], caos.types.ConceptClass):
                ids = set()

                for cls in arg:
                    proto = caos.types.prototype(cls)
                    children = proto.descendants(cls.__sx_protoschema__)
                    ids.add(self._concept_map[proto.name])
                    ids.update(self._concept_map[c.name] for c in children)

                arg = ids

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


class CaosQLAdapter:
    cache = {}

    def __init__(self, session):
        self.session = session
        self.connection = session.get_connection()
        self.transformer = IRCompiler()
        self.current_portal = None

    def transform(self, query, scrolling_cursor=False, context=None, *, proto_schema,
                                                                        output_format=None):
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
                    name = 'type' if isinstance(v[1], caos.types.PrototypeClass) else v[1].name
                    argtypes[k] = (v[0], name)
                else:
                    name = 'type' if isinstance(v, caos.types.PrototypeClass) else v.name
                    argtypes[k] = name
            else:
                argtypes[k] = v

        return Query(chunks=qchunks, arg_index=arg_index, argmap=argmap, result_types=restypes,
                     argument_types=argtypes, context_vars=query.context_vars,
                     scrolling_cursor=scrolling_cursor,
                     offset=offset, limit=limit, query_type=query_type,
                     record_info=record_info, output_format=output_format)


class Backend(backends.MetaBackend, backends.DataBackend):

    typlen_re = re.compile(r"(?P<type>.*) \( (?P<length>\d+ (?:\s*,\s*(\d+))*) \)$",
                           re.X)

    search_idx_name_re = re.compile(r"""
        .*_(?P<language>\w+)_(?P<index_class>\w+)_search_idx$
    """, re.X)

    error_res = {
        postgresql.exceptions.ICVError: collections.OrderedDict((
            ('link_mapping',
             re.compile(r'^.*"(?P<constr_name>.*_link_mapping_idx)".*$')),
            ('constraint',
             re.compile(r'^.*"(?P<constr_name>.*;schemaconstr(?:#\d+)?).*"$')),
            ('id',
             re.compile(r'^.*"(?P<constr_name>\w+)_data_pkey".*$')),
        ))
    }

    link_source_colname = common.quote_ident(
                                common.caos_name_to_pg_name('metamagic.caos.builtins.source'))
    link_target_colname = common.quote_ident(
                                common.caos_name_to_pg_name('metamagic.caos.builtins.target'))

    def __init__(self, deltarepo, connector_factory):
        connector = connector_factory()
        async_connector = connector_factory(async=True)

        self.features = None
        self.backend_info = None
        self.modules = None

        self.schema = so.ProtoSchema()

        self.connection_pool = pool.ConnectionPool(connector, backend=self)
        self.async_connection_pool = pool.ConnectionPool(async_connector, backend=self)

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

        self.connection = connector(pool=self.connection_pool)
        self.connection.connect()

        repo = deltarepo(self.connection)
        self._init_introspection_cache()
        super().__init__(repo)


    def init_connection(self, connection):
        need_upgrade = False

        if self.backend_info is None:
            self.backend_info = self.read_backend_info()

        if self.backend_info['format_version'] < delta_cmds.BACKEND_FORMAT_VERSION:
            need_upgrade = True
            self.upgrade_backend(connection)

        elif self.backend_info['format_version'] > delta_cmds.BACKEND_FORMAT_VERSION:
            msg = 'unsupported backend format version: %d' % self.backend_info['format_version']
            details = 'The largest supported backend format version is %d' \
                        % delta_cmds.BACKEND_FORMAT_VERSION
            raise caos.MetaError(msg, details=details)

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


    def _init_introspection_cache(self):
        self.backend_info = self.read_backend_info()

        if self.backend_info['initialized']:
            self._type_mech.init_cache(self.connection)
            self._constr_mech.init_cache(self.connection)
            self.table_id_to_proto_name_cache, self.proto_name_to_table_id_cache = self._init_relid_cache()
            self.domain_to_atom_map = self._init_atom_map_cache()
            # Concept map needed early for type filtering operations in schema queries
            self.get_concept_map(force_reload=True)


    def _init_relid_cache(self):
        link_tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                            table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        records = introspection.types.TypesList(self.connection).fetch(schema_name='caos%',
                                                                       type_name='%_record',
                                                                       include_arrays=False)
        records = {(t['schema'], t['name']): t for t in records}

        links_list = datasources.schema.links.ConceptLinks(self.connection).fetch()
        links_list = collections.OrderedDict((caos.Name(r['name']), r) for r in links_list)

        table_id_to_proto_name_cache = {}
        proto_name_to_table_id_cache = {}

        for link_name, link in links_list.items():
            link_table_name = common.link_name_to_table_name(link_name, catenate=False)
            t = link_tables.get(link_table_name)
            if t:
                table_id_to_proto_name_cache[t['oid']] = link_name
                table_id_to_proto_name_cache[t['typoid']] = link_name
                proto_name_to_table_id_cache[link_name] = t['typoid']

        tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                       table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        concept_list = datasources.schema.concepts.ConceptList(self.connection).fetch()
        concept_list = collections.OrderedDict((caos.Name(row['name']), row) for row in concept_list)

        for name, row in concept_list.items():
            table_name = common.concept_name_to_table_name(name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept "%s" exists but the table is missing' % name
                raise caos.MetaError(msg, details=details)

            table_id_to_proto_name_cache[table['oid']] = name
            table_id_to_proto_name_cache[table['typoid']] = name
            proto_name_to_table_id_cache[name] = table['typoid']

        return table_id_to_proto_name_cache, proto_name_to_table_id_cache


    def _init_atom_map_cache(self):
        domains = introspection.domains.DomainsList(self.connection).fetch(schema_name='caos%',
                                                                           domain_name='%_domain')
        domains = {(d['schema'], d['name']): self.normalize_domain_descr(d) for d in domains}

        atom_list = datasources.schema.atoms.AtomList(self.connection).fetch()

        domain_to_atom_map = {}

        for row in atom_list:
            name = caos.Name(row['name'])

            domain_name = common.atom_name_to_domain_name(name, catenate=False)

            domain = domains.get(domain_name)
            domain_to_atom_map[domain_name] = name

        return domain_to_atom_map


    def init_features(self, connection):
        for feature_class_name in self.features.values():
            feature_class = get_object(feature_class_name)
            feature_class.init_feature(connection)


    def upgrade_backend(self, connection):
        with self.connection.xact():
            context = delta_cmds.CommandContext(connection)
            upgrade = delta_cmds.UpgradeBackend(self.backend_info)
            upgrade.execute(context)
            self.backend_info = self.read_backend_info()


    def get_session_pool(self, realm, async=False):
        if async:
            return session.AsyncSessionPool(self, realm)
        else:
            return session.SessionPool(self, realm)


    def free_resources(self):
        self.parser.cleanup()
        import gc
        gc.collect()


    def getschema(self):
        if not self.schema.modules:
            if self.backend_info['initialized']:
                self._init_introspection_cache()

                self.read_modules(self.schema)
                self.read_attributes(self.schema)
                self.read_actions(self.schema)
                self.read_events(self.schema)
                self.read_atoms(self.schema)
                self.read_concepts(self.schema)
                self.read_links(self.schema)
                self.read_link_properties(self.schema)
                self.read_computables(self.schema)
                self.read_policies(self.schema)
                self.read_attribute_values(self.schema)
                self.read_constraints(self.schema)

                self.order_attributes(self.schema)
                self.order_actions(self.schema)
                self.order_events(self.schema)
                self.order_atoms(self.schema)
                self.order_computables(self.schema)
                self.order_link_properties(self.schema)
                self.order_links(self.schema)
                self.order_concepts(self.schema)
                self.order_policies(self.schema)

                self.free_resources()

        return self.schema


    def adapt_delta(self, delta):
        return delta_cmds.CommandMeta.adapt(delta)

    @debug
    def process_delta(self, delta, schema, session=None):
        """LOG [caos.delta.plan] Delta Plan
            markup.dump(delta)
        """
        delta = self.adapt_delta(delta)
        connection = session.get_connection() if session else self.connection
        context = delta_cmds.CommandContext(connection, session=session)
        delta.apply(schema, context)
        """LOG [caos.delta.plan.pgsql] PgSQL Delta Plan
            markup.dump(delta)
        """
        return delta


    def execute_delta_plan(self, plan, session=None):
        connection = session.get_connection() if session else self.connection
        plan.execute(delta_cmds.CommandContext(connection, session=session))


    @debug
    def apply_delta(self, delta, session, source_deltarepo):
        if isinstance(delta, base_delta.DeltaSet):
            deltas = list(delta)
        else:
            deltas = [delta]

        proto_schema = self.getschema()

        with session.transaction():
            old_conn = self.connection
            self.connection = session.get_connection()

            for d in deltas:
                """LINE [caos.delta.apply] Applying delta
                    '{:032x}'.format(d.id)
                """

                session.replace_schema(proto_schema)

                # Run preprocess pass
                d.call_hook(session, stage='preprocess', hook='main')

                if d.deltas:
                    delta = d.deltas[0]

                    # Apply and adapt delta, build native delta plan
                    plan = self.process_delta(delta, proto_schema)

                    proto_schema.clear_class_cache()
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
                    except caos.MetaError as e:
                        msg = 'failed to verify metadata after applying delta {:032x} to data backend'
                        msg = msg.format(d.id)
                        self._raise_delta_error(msg, d, plan, e)

                # Run postprocess pass
                d.call_hook(session, stage='postprocess', hook='main')

                self.invalidate_schema_cache()

                try:
                    introspected_schema = self.getschema()
                except caos.MetaError as e:
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

                    """LOG [caos.delta.recordchecksums]
                    delta_checksums = d.checksum_details
                    introspected_checksums = \
                        introspected_schema.get_checksum_details()
                    """

                    raise base_delta.DeltaChecksumError(
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
        d = base_delta.Delta(parent_id=d.parent_id, checksum=d.checksum,
                             comment=d.comment, deltas=[plan])
        raise base_delta.DeltaError(msg, delta=d) from e


    def _update_repo(self, session, deltas):
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
        dbops.Insert(table, records=records).execute(context)

        table = deltadbops.DeltaRefTable()
        rec = table.record(
                id='%x' % d.id,
                ref='HEAD'
              )
        condition = [('ref', str('HEAD'))]
        dbops.Merge(table, record=rec, condition=condition).execute(context)


    def invalidate_schema_cache(self):
        self.schema = so.ProtoSchema()
        self.backend_info = self.read_backend_info()
        self.features = self.read_features(self.connection)
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
        concept = caos.Name('metamagic.caos.builtins.BaseObject')
        query = '''SELECT c.name
                   FROM
                       %s AS e
                       INNER JOIN caos.concept AS c ON c.id = e.concept_id
                   WHERE e."metamagic.caos.builtins.id" = $1
                ''' % (common.concept_name_to_table_name(concept))
        ps = session.get_prepared_statement(query)
        concept_name = ps.first(id)
        if concept_name:
            concept_name = caos.Name(concept_name)
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
                 if l in valid_link_names or getattr(l, 'direction', caos.types.OutboundDirection)
                                             == caos.types.InboundDirection}

        return session._merge(links['metamagic.caos.builtins.id'], concept_cls, links)


    def _rebuild_tree_from_list(self, session, items, connecting_attribute):
        # Build a tree from a list of (parent, child_id) tuples, while
        # maintaining total order.
        #
        updates = {}
        uuid = session.schema.metamagic.caos.builtins.BaseObject.id

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


    def load_entity(self, concept, id, session):
        query = 'SELECT * FROM %s WHERE "metamagic.caos.builtins.id" = $1' % \
                                                (common.concept_name_to_table_name(concept))

        ps = session.get_connection().prepare(query)
        result = ps.first(id)

        if result is not None:
            concept_proto = session.proto_schema.get(concept)
            ret = {}

            for link_name, link in concept_proto.pointers.items():

                if link.atomic() and link.singular() \
                                 and not link.is_pure_computable() \
                            and link_name != 'metamagic.caos.builtins.id' \
                            and link.loading != caos.types.LazyLoading:
                    colname = common.caos_name_to_pg_name(link_name)

                    try:
                        ret[str(link_name)] = result[colname]
                    except KeyError:
                        pass

            return ret
        else:
            return None


    def load_link(self, source, target, link, pointers, session):
        proto_link = caos.types.prototype(link.__class__)
        table = common.get_table_name(proto_link, catenate=True)

        if pointers:
            protopointers = [caos.types.prototype(p) for p in pointers]
            pointers = {p.normal_name(): p for p in protopointers if not p.is_endpoint_pointer()}
        else:
            pointers = {n: p for n, p in proto_link.pointers.items()
                             if p.loading != caos.types.LazyLoading and not p.is_endpoint_pointer()}

        targets = []

        for prop_name in pointers:
            targets.append(common.qname('l', common.caos_name_to_pg_name(prop_name)))

        source_col = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
        ptr_stor_info = types.get_pointer_storage_info(
                            proto_link, schema=session.proto_schema)

        query = '''SELECT
                       {targets}
                   FROM
                       {table} AS l
                   WHERE
                       l.{source_col} = $1
                       AND l.link_type_id = $2
                '''.format(targets=', '.join(targets), table=table,
                           source_col=common.quote_ident(source_col))

        if ptr_stor_info.table_type == 'link':
            query += ' AND l.{target_col} IS NOT DISTINCT FROM $3'.format(
                        target_col=common.quote_ident(ptr_stor_info.column_name)
                     )

        ps = session.get_connection().prepare(query)

        link_map = self.get_link_map(session)
        link_id = link_map[proto_link.name]

        args = [source.id, link_id]

        if ptr_stor_info.table_type == 'link':
            if isinstance(target.__class__, caos.types.AtomClass):
                target_value = target
            else:
                target_value = target.id

            args.append(target_value)

        result = ps(*args)

        if result:
            result = result[0]
            ret = {}

            for propname in pointers:
                colname = common.caos_name_to_pg_name(propname)
                ret[str(propname)] = result[colname]

            return ret

        else:
            return {}

    def _get_id_constraint(self, proto_schema):
        BObj = proto_schema.get('metamagic.caos.builtins.BaseObject')
        BObj_id = BObj.pointers['metamagic.caos.builtins.id']
        unique = proto_schema.get('metamagic.caos.builtins.unique')

        name = proto.Constraint.generate_specialized_name(
                BObj_id.name, unique.name)
        name = caos.Name(name=name, module='metamagic.caos.builtins')
        constraint = proto.Constraint(name=name, bases=[unique],
                                      subject=BObj_id)
        constraint.acquire_ancestor_inheritance(proto_schema)

        return constraint

    def _interpret_db_error(self, proto_schema, connection, err, source,
                                                                 pointer=None):
        if isinstance(err, postgresql.exceptions.ICVError):

            for ecls, eres in self.error_res.items():
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
                return caos.error.UninterpretedStorageError(err.message)

            error_type, error_data = error_info

            if error_type == 'link_mapping':
                err = 'link mapping cardinality violation'
                errcls = caos.error.LinkMappingCardinalityViolationError
                return errcls(err, source=source, pointer=pointer)

            elif error_type == 'constraint':
                constraint_name = self._constr_mech.constraint_name_from_pg_name(
                                            connection, error_data)
                if constraint_name is None:
                    return caos.error.UninterpretedStorageError(err.message)

                constraint = proto_schema.get_class(constraint_name)
                # Unfortunately, Postgres does not include the offending value in
                # exceptions consistently.
                offending_value = None

                if pointer is not None:
                    error_source = pointer
                else:
                    error_source = source

                constraint.raise_error(offending_value, source=error_source)

            elif error_type == 'id':
                msg = 'unique link constraint violation'
                pointer = getattr(source.__class__, 'metamagic.caos.builtins.id').as_link()
                errcls = caos.error.PointerConstraintUniqueViolationError
                constraint = self._get_id_constraint(proto_schema)
                return errcls(msg=msg, source=source, pointer=pointer,
                              constraint=constraint)
        else:
            return caos.error.UninterpretedStorageError(err.message)


    @debug
    def store_entity(self, entity, session):
        cls = entity.__class__
        prototype = caos.types.prototype(cls)
        concept = prototype.name
        id = entity.id
        links = entity._instancedata.pointers
        table = self._type_mech.get_table(prototype, session.proto_schema)

        connection = session.get_connection() if session else self.connection
        concept_map = self.get_concept_map(session)
        context = delta_cmds.CommandContext(connection, session)

        idquery = dbops.Query(text='caos.uuid_generate_v1mc()', params=(), type='uuid')
        now = dbops.Query(text="'NOW'", params=(), type='timestamptz')

        is_object = issubclass(cls, session.schema.metamagic.caos.builtins.Object)

        with connection.xact():

            attrs = {}
            for link_name, link_cls in cls._iter_all_pointers():
                link_proto = link_cls._class_metadata.link.__sx_prototype__
                if link_proto.atomic() and link_proto.singular() \
                                       and link_name != 'metamagic.caos.builtins.id' \
                                       and not link_proto.is_pure_computable() \
                                       and link_name in links:
                    value = links[link_name]
                    if isinstance(value, caos.types.NodeClass):
                        # The singular atomic link will be represented as a selector if
                        # it has exposed_behaviour of "set".
                        value = value[0]
                    attrs[common.caos_name_to_pg_name(link_name)] = value

            rec = table.record(**attrs)

            returning = ['"metamagic.caos.builtins.id"']
            if is_object:
                returning.extend(('"metamagic.caos.builtins.ctime"',
                                  '"metamagic.caos.builtins.mtime"'))

            if id is not None and not entity._instancedata.new_predefined_id:
                condition = [('metamagic.caos.builtins.id', id)]

                if is_object:
                    setattr(rec, 'metamagic.caos.builtins.mtime', now)
                    condition.append(('metamagic.caos.builtins.mtime', entity.mtime))

                cmd = dbops.Update(table=table, record=rec,
                                   condition=condition,
                                   returning=returning)
            else:
                setattr(rec, 'metamagic.caos.builtins.id', idquery if id is None else id)

                if is_object:
                    setattr(rec, 'metamagic.caos.builtins.ctime', now)
                    setattr(rec, 'metamagic.caos.builtins.mtime', now)

                rec.concept_id = concept_map[concept]

                cmd = dbops.Insert(table=table, records=[rec], returning=returning)

            try:
                rows = cmd.execute(context)
            except postgresql.exceptions.Error as e:
                raise self._interpret_db_error(session.proto_schema, connection, e, entity) from e

            id = list(rows)
            if not id:
                err = 'session state of "%s"(%s) conflicts with persistent state' % \
                      (prototype.name, entity.id)
                raise caos.session.StaleEntityStateError(err, entity=entity)

            id = id[0]

            """LOG [caos.sync]
            print('Merged entity %s[%s][%s]' % \
                    (concept, id[0], (data['name'] if 'name' in data else '')))
            """

            if is_object:
                updates = {'metamagic.caos.builtins.id': id[0],
                           'metamagic.caos.builtins.ctime': id[1],
                           'metamagic.caos.builtins.mtime': id[2]}
            else:
                updates = {'metamagic.caos.builtins.id': id[0]}
            entity._instancedata.update(entity, updates, register_changes=False, allow_ro=True)
            session.add(entity)

        return id


    @debug
    def delete_entities(self, entities, session):
        key = lambda i: i.__class__.__sx_prototype__.name
        result = set()
        modstat_t = common.qname(*deltadbops.EntityModStatType().name)

        for concept, entities in itertools.groupby(sorted(entities, key=key), key=key):
            table = common.concept_name_to_table_name(concept)

            bunch = {(e.id, e.mtime): e for e in entities}

            query = '''DELETE FROM %s
                       WHERE
                           ("metamagic.caos.builtins.id", "metamagic.caos.builtins.mtime")
                           = any($1::%s[])
                       RETURNING
                           "metamagic.caos.builtins.id", "metamagic.caos.builtins.mtime"
                    ''' % (table, modstat_t)

            deleted = list(self.runquery(query, (bunch,), session.get_connection(), compat=False))

            if len(deleted) < len(bunch):
                # Not everything was removed
                diff = set(bunch) - set(deleted)

                first = next(iter(diff))
                entity = bunch[first]
                prototype = caos.types.prototype(entity.__class__)

                err = 'session state of "%s"(%s) conflicts with persistent state' % \
                      (prototype.name, entity.id)
                raise caos.session.StaleEntityStateError(err, entity=entity)

            result.update(deleted)
        return result


    def get_link_map(self, session):
        if not self.link_cache:
            cl_ds = datasources.schema.links.ConceptLinks(session.get_connection())

            for row in cl_ds.fetch():
                self.link_cache[row['name']] = row['id']

        return self.link_cache


    def get_link_property_map(self, session):
        if not self.link_property_cache:
            cl_ds = datasources.schema.links.LinkProperties(session.get_connection())

            for row in cl_ds.fetch():
                self.link_property_cache[row['name']] = row['id']

        return self.link_property_cache


    def get_concept_map(self, session=None, force_reload=False):
        connection = session.get_connection() if session is not None else self.connection

        if not self.concept_cache or force_reload:
            cl_ds = datasources.schema.concepts.ConceptList(connection)

            for row in cl_ds.fetch():
                self.concept_cache[row['name']] = row['id']
                self.concept_cache[row['id']] = caos.Name(row['name'])

        return self.concept_cache


    def get_concept_id(self, concept):
        concept_id = None

        concept_cache = self.concept_cache
        if concept_cache:
            concept_id = concept_cache.get(concept.name)

        if concept_id is None:
            msg = 'could not determine backend id for concept in this context'
            details = 'Concept: {}'.format(concept.name)
            raise caos.MetaError(msg, details=details)

        return concept_id


    def source_name_from_relid(self, table_oid):
        return self.table_id_to_proto_name_cache.get(table_oid)


    def typrelid_for_source_name(self, source_name):
        return self.proto_name_to_table_id_cache.get(source_name)


    @debug
    def store_links(self, source, targets, link_name, session, merge=False):
        link_map = self.get_link_map(session)

        target_cls = getattr(source.__class__, str(link_name))
        link_cls = target_cls.as_link()
        link_proto = link_cls.__sx_prototype__

        if (link_proto.atomic() and link_proto.singular()
                                and not link_proto.has_user_defined_properties()):
            return

        source_col = common.caos_name_to_pg_name('metamagic.caos.builtins.source')
        target_ptr_stor_info = types.get_pointer_storage_info(
                                    link_proto, schema=session.proto_schema)
        target_col = target_ptr_stor_info.column_name

        table = self._type_mech.get_table(link_cls.__sx_prototype__, session.proto_schema)

        cmds = []
        records = []

        context = delta_cmds.CommandContext(session.get_connection(), session)

        target_is_concept = not link_proto.atomic()
        target_in_table = target_ptr_stor_info.table_type == 'link'

        idcol = 'metamagic.caos.builtins.linkid'
        returning = ['"' + idcol + '"']

        for link_obj in targets:
            target = link_obj.target

            if not isinstance(target, target_cls):
                expected_target = str(target_cls.__sx_prototype__.name)
                source_name = source.__sx_prototype__.name
                link_name = link_proto.normal_name()
                msg = ('unexpected link target when storing "{}"."{}": '
                       'expected instance of {!r}, got {!r}').format(source_name, link_name,
                                                                     expected_target, target)
                raise ValueError(msg)

            attrs = {}
            for prop_name, prop_cls in link_cls._iter_all_pointers():
                if prop_name not in {'metamagic.caos.builtins.source',
                                     'metamagic.caos.builtins.target'}:
                    try:
                        # We must look into link object __dict__ directly so as not to
                        # potentially trigger link object reload for lazy properties,
                        # which would fail with non-persistent link objects.
                        prop_value = link_obj.__dict__[prop_name]
                    except KeyError:
                        pass
                    else:
                        attrs[common.caos_name_to_pg_name(prop_name)] = prop_value

            rec = table.record(**attrs)

            setattr(rec, source_col, source.id)
            rec.link_type_id = link_map[link_proto.name]

            if target_in_table:
                if target_is_concept:
                    setattr(rec, target_col, target.id)
                else:
                    setattr(rec, target_col, target)

            linkid = getattr(link_obj, idcol)
            if linkid is None:
                linkid = uuid.uuid1()
                link_obj._instancedata.setattr(link_obj, idcol, linkid,
                                               register_changes=False,
                                               allow_ro=True)

            setattr(rec, idcol, linkid)

            if linkid and merge:
                condition = [(idcol, getattr(rec, idcol))]
                cmds.append(dbops.Merge(table, rec, condition=condition,
                                                    returning=returning))
            else:
                records.append(rec)

        if records:
            cmds.append(dbops.Insert(table, records, returning=returning))

        if cmds:
            try:
                for cmd in cmds:
                    cmd.execute(context)
            except postgresql.exceptions.ICVError as e:
                raise self._interpret_db_error(session.proto_schema,
                                               session.get_connection(),
                                               e, source, link_cls) from e


    @debug
    def delete_links(self, link_name, endpoints, session):
        table = common.link_name_to_table_name(link_name)

        link = getattr(next(iter(endpoints))[0].__class__, link_name).as_link()
        link_proto = link.__sx_prototype__
        if link_proto.atomic() and link_proto.singular() and len(link_proto.pointers) <= 2:
            return

        complete_source_ids = [s.id for s, t in endpoints if t is None]
        partial_endpoints = [(s.id, t.id) for s, t in endpoints if t is not None]

        count = 0

        if complete_source_ids:
            qry = '''
                DELETE FROM {table} WHERE {source_col} = any($1)
            '''.format(table=table, source_col=self.link_source_colname)
            params = (complete_source_ids,)

            result = self.runquery(qry, params,
                                   connection=session.get_connection(),
                                   compat=False, return_stmt=True)
            count += result.first(*params)

        if partial_endpoints:
            qry = '''DELETE FROM {table}
                     WHERE ({source_col}, {target_col}) = any($1::caos.link_endpoints_rec_t[])
                  '''.format(table=table, source_col=self.link_source_colname,
                             target_col=self.link_target_colname)
            params = (partial_endpoints,)

            result = self.runquery(qry, params,
                                   connection=session.get_connection(),
                                   compat=False, return_stmt=True)
            partial_count = result.first(*params)

            # Actual deletion count may be less than the list of links,
            # since the caller may request deletion of a non-existent link,
            # e.g. through discard().
            assert partial_count <= len(partial_endpoints)

            count += partial_count

        return count


    def caosqladapter(self, session):
        return CaosQLAdapter(session)


    def read_modules(self, schema):
        schemas = introspection.schemas.SchemasList(self.connection).fetch(schema_name='caos%')
        schemas = {s['name'] for s in schemas if not s['name'].startswith('caos_aux_')}

        context = delta_cmds.CommandContext(self.connection)
        cond = dbops.TableExists(name=('caos', 'module'))
        module_index_exists = cond.execute(context)

        if 'caos' in schemas and module_index_exists:
            modules = datasources.schema.modules.ModuleList(self.connection).fetch()
            modules = {m['schema_name']: {'name': m['name'], 'imports': m['imports']}
                       for m in modules}

            recorded_schemas = set(modules.keys())

            # Sanity checks
            extra_schemas = schemas - recorded_schemas - {'caos'}
            missing_schemas = recorded_schemas - schemas

            if extra_schemas:
                msg = 'internal metadata incosistency'
                details = 'Extraneous data schemas exist: %s' \
                            % (', '.join('"%s"' % s for s in extra_schemas))
                raise caos.MetaError(msg, details=details)

            if missing_schemas:
                msg = 'internal metadata incosistency'
                details = 'Missing schemas for modules: %s' \
                            % (', '.join('"%s"' % s for s in extra_schemas))
                raise caos.MetaError(msg, details=details)

            mods = []

            for module in modules.values():
                mod = caos.proto.ProtoModule(name=module['name'],
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
                        # Again, it must not be a schema module
                        assert not isinstance(impmod, so.SchemaModule)

                        self.schema.add_module(impmod)


    def read_features(self, connection):
        try:
            features = datasources.schema.features.FeatureList(connection).fetch()
            return {f['name']: f['class_name'] for f in features}
        except (postgresql.exceptions.SchemaNameError, postgresql.exceptions.UndefinedTableError):
            return {}


    def read_backend_info(self):
        try:
            info = datasources.schema.backend_info.BackendInfo(self.connection).fetch()[0]
            info['initialized'] = True
            return info
        except (postgresql.exceptions.SchemaNameError, postgresql.exceptions.UndefinedTableError):
            # Two possibilities: either this is a fresh empty db, or it's ancient
            # enough not to have backend metainformation.
            #
            schemas_ds = datasources.introspection.schemas.SchemasList(self.connection)
            caos_schema = schemas_ds.fetch(schema_name='caos')
            if caos_schema:
                # Ancient db
                return {
                    'format_version': 0,
                    'initialized': True,
                }
            else:
                # Empty db
                return {
                    'format_version': delta_cmds.BACKEND_FORMAT_VERSION,
                    'initialized': False,
                }


    def read_atoms(self, schema):
        domains = introspection.domains.DomainsList(self.connection).fetch(schema_name='caos%',
                                                                           domain_name='%_domain')
        domains = {(d['schema'], d['name']): self.normalize_domain_descr(d) for d in domains}

        seqs = introspection.sequences.SequencesList(self.connection).fetch(
                                                schema_name='caos%', sequence_pattern='%_sequence')
        seqs = {(s['schema'], s['name']): s for s in seqs}

        seen_seqs = set()

        atom_list = datasources.schema.atoms.AtomList(self.connection).fetch()

        basemap = {}

        for row in atom_list:
            name = caos.Name(row['name'])

            atom_data = {'name': name,
                         'title': self.hstore_to_word_combination(row['title']),
                         'description': row['description'],
                         'is_abstract': row['is_abstract'],
                         'is_final': row['is_final'],
                         'base': row['base'],
                         'default': row['default'],
                         'attributes': row['attributes'] or {}
                         }

            self.atom_cache[name] = atom_data

            domain_name = common.atom_name_to_domain_name(name, catenate=False)
            domain = domains.get(domain_name)

            atom_data['default'] = self.unpack_default(row['default'])

            if atom_data['base']:
                basemap[name] = atom_data['base']

            atom = proto.Atom(name=name,
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
                atom.bases = [schema.get(caos.Name(basename))]

        sequence = schema.get('metamagic.caos.builtins.sequence')
        for atom in schema('atom'):
            if atom.issubclass(sequence):
                seq_name = common.atom_name_to_sequence_name(atom.name, catenate=False)
                if seq_name not in seqs:
                    msg = 'internal metadata incosistency'
                    details = 'Missing sequence for sequence atom "%s"' % atom.name
                    raise caos.MetaError(msg, details=details)
                seen_seqs.add(seq_name)

        extra_seqs = set(seqs) - seen_seqs
        if extra_seqs:
            msg = 'internal metadata incosistency'
            details = 'Extraneous sequences exist: %s' \
                        % (', '.join(common.qname(*t) for t in extra_seqs))
            raise caos.MetaError(msg, details=details)


    def order_atoms(self, schema):
        for atom in schema(type='atom'):
            atom.acquire_ancestor_inheritance(schema)


    def read_constraints(self, schema):
        constraints_list = datasources.schema.constraints.Constraints(self.connection).fetch()
        constraints_list = collections.OrderedDict((caos.Name(r['name']), r)
                                                    for r in constraints_list)

        basemap = {}

        for name, r in constraints_list.items():
            bases = tuple()

            if r['subject']:
                bases = (proto.Constraint.normalize_name(name),)
            elif r['base']:
                bases = tuple(caos.Name(b) for b in r['base'])
            elif name != 'metamagic.caos.builtins.constraint':
                bases = (caos.Name('metamagic.caos.builtins.constraint'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            subject = schema.get(r['subject']) if r['subject'] else None

            basemap[name] = bases

            if r['paramtypes']:
                paramtypes = {n: self.unpack_typeref(v, schema) for n, v in r['paramtypes'].items()}
            else:
                paramtypes = None

            if r['inferredparamtypes']:
                inferredparamtypes = {n: self.unpack_typeref(v, schema)
                                      for n, v in r['inferredparamtypes'].items()}
            else:
                inferredparamtypes = None

            if r['args']:
                args = pickle.loads(r['args'])
            else:
                args = None

            constraint = proto.Constraint(name=name, subject=subject,
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


    def order_constraints(self, schema):
        pass


    def unpack_typeref(self, typeref, protoschema):
        try:
            collection_type, type = proto.TypeRef.parse(typeref)
        except ValueError as e:
            raise caos.MetaError(e.args[0]) from None

        if type is not None:
            type = protoschema.get(type)

        if collection_type is not None:
            type = collection_type(element_type=type)

        return type


    def unpack_default(self, value):
        result = []
        if value is not None:
            values = json.loads(value)
            for val in values:
                if val['type'] == 'expr':
                    item = caos_types.ExpressionText(val['value'])
                else:
                    item = val['value']
                result.append(item)
        return result


    def interpret_search_index(self, index):
        m = self.search_idx_name_re.match(index.name)
        if not m:
            msg = 'could not interpret index {}'.format(index.name)
            raise caos.MetaError(msg)

        language = m.group('language')
        index_class = m.group('index_class')

        tree = self.parser.parse(index.expr)
        columns = self.search_idx_expr.match(tree)

        if columns is None:
            msg = 'could not interpret index {!r}'.format(str(index.name))
            details = 'Could not match expression:\n{}'.format(markup.dumps(tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise caos.MetaError(msg, details=details, hint=hint)

        return index_class, language, columns


    def interpret_search_indexes(self, table_name, indexes):
        for idx_data in indexes:
            index = dbops.Index.from_introspection(table_name, idx_data)
            yield self.interpret_search_index(index)


    def read_search_indexes(self):
        indexes = {}
        index_ds = datasources.introspection.tables.TableIndexes(self.connection)
        idx_data = index_ds.fetch(schema_pattern='caos%',
                                  index_pattern='%_search_idx')

        for row in idx_data:
            table_name = tuple(row['table_name'])
            tabidx = indexes[table_name] = {}

            si = self.interpret_search_indexes(table_name, row['indexes'])

            for index_class, language, columns in si:
                for column_name, column_config in columns.items():
                    idx = tabidx.setdefault(column_name, {})
                    idx[(index_class, column_config[0])] = \
                        caos.types.LinkSearchWeight(column_config[1])

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

    def read_indexes(self):
        indexes = {}
        index_ds = datasources.introspection.tables.TableIndexes(self.connection)
        idx_data = index_ds.fetch(schema_pattern='caos%',
                                  index_pattern='%_reg_idx')

        for row in idx_data:
            table_name = tuple(row['table_name'])
            indexes[table_name] = set(self.interpret_indexes(table_name,
                                                             row['indexes']))

        return indexes


    def interpret_constant(self, expr):
        try:
            expr_tree = self.parser.parse(expr)
        except parser.PgSQLParserError as e:
            msg = 'could not interpret constant expression "%s"' % expr
            details = 'Syntax error when parsing expression: %s' % e.args[0]
            raise caos.MetaError(msg, details=details) from e

        if not self.constant_expr:
            self.constant_expr = astexpr.ConstantExpr()

        value = self.constant_expr.match(expr_tree)

        if value is None:
            msg = 'could not interpret constant expression "%s"' % expr
            details = 'Could not match expression:\n{}'.format(markup.dumps(expr_tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise caos.MetaError(msg, details=details, hint=hint)

        return value


    def read_pointer_target_column(self, schema, pointer, constraints_cache):
        ptr_stor_info = types.get_pointer_storage_info(
                            pointer, schema=schema, resolve_type=False)
        cols = self._type_mech.get_table_columns(ptr_stor_info.table_name,
                                                 connection=self.connection)

        col = cols.get(ptr_stor_info.column_name)

        if not col:
            msg = 'internal metadata inconsistency'
            details = ('Record for "%s" hosted by "%s" exists, but corresponding table column '
                       'is missing' % (pointer.normal_name(), pointer.source.name))
            raise caos.MetaError(msg, details=details)

        return self._get_pointer_column_target(schema, pointer.source, pointer.normal_name(), col)


    def _get_pointer_column_target(self, schema, source, pointer_name, col):
        if col['column_type_schema'] == 'pg_catalog':
            col_type_schema = common.caos_module_name_to_schema_name('metamagic.caos.builtins')
            col_type = col['column_type_formatted']
        else:
            col_type_schema = col['column_type_schema']
            col_type = col['column_type_formatted'] or col['column_type']

        if col['column_default'] is not None:
            atom_default = self.interpret_constant(col['column_default'])
        else:
            atom_default = None

        target = self.atom_from_pg_type(col_type, col_type_schema,
                                        atom_default, schema)

        return target, col['column_required']


    def _get_pointer_attribute_target(self, schema, source, pointer_name, attr):
        if attr['attribute_type_schema'] == 'pg_catalog':
            col_type_schema = common.caos_module_name_to_schema_name('metamagic.caos.builtins')
            col_type = attr['attribute_type_formatted']
        else:
            col_type_schema = attr['attribute_type_schema']
            col_type = attr['attribute_type_formatted'] or attr['attribute_type']

        if attr['attribute_default'] is not None:
            atom_default = self.interpret_constant(attr['attribute_default'])
        else:
            atom_default = None

        if attr['attribute_type_composite_id']:
            # composite record
            source_name = self.source_name_from_relid(attr['attribute_type_composite_id'])
            target = schema.get(source_name)
        else:
            target = self.atom_from_pg_type(col_type, col_type_schema,
                                            atom_default, schema)

        return target, attr['attribute_required']


    def verify_ptr_const_defaults(self, schema, ptr_name, target_atom, tab_default, schema_defaults):
        if schema_defaults:
            ld = list(filter(lambda d: not isinstance(d, caos_types.ExpressionText), schema_defaults))
        else:
            ld = ()

        if tab_default is None:
            if ld:
                msg = 'internal metadata inconsistency'
                details = ('Literal default for pointer "%s" is present in the schema, but not '
                           'in the table') % ptr_name
                raise caos.MetaError(msg, details=details)
            else:
                return

        typ = target_atom.get_topmost_base(schema)
        default = self.interpret_constant(tab_default)
        table_default = typ(default)

        if tab_default is not None and not schema_defaults:
            msg = 'internal metadata inconsistency'
            details = ('Literal default for pointer "%s" is present in the table, but not '
                       'in schema declaration') % ptr_name
            raise caos.MetaError(msg, details=details)

        if not ld:
            msg = 'internal metadata inconsistency'
            details = ('Literal default for pointer "%s" is present in the table, but '
                       'there are no literal defaults for the link') % ptr_name
            raise caos.MetaError(msg, details=details)

        schema_value = typ(ld[0])

        if schema_value != table_default:
            msg = 'internal metadata inconsistency'
            details = ('Value mismatch in literal default pointer link "%s": %r in the '
                       'table vs. %r in the schema') % (ptr_name, table_default, schema_value)
            raise caos.MetaError(msg, details=details)


    def read_links(self, schema):

        link_tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                            table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = datasources.schema.links.ConceptLinks(self.connection).fetch()
        links_list = collections.OrderedDict((caos.Name(r['name']), r) for r in links_list)

        concept_indexes = self.read_search_indexes()
        basemap = {}

        for name, r in links_list.items():
            bases = tuple()

            if r['source_id']:
                bases = (proto.Link.normalize_name(name),)
            elif r['base']:
                bases = tuple(caos.Name(b) for b in r['base'])
            elif name != 'metamagic.caos.builtins.link':
                bases = (caos.Name('metamagic.caos.builtins.link'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']

            source = schema.get(r['source']) if r['source'] else None
            target = schema.get(r['target']) if r['target'] else None
            if r['spectargets']:
                spectargets = [schema.get(t) for t in r['spectargets']]
            else:
                spectargets = None

            r['default'] = self.unpack_default(r['default'])

            required = r['required']

            loading = caos.types.PointerLoading(r['loading']) if r['loading'] else None

            exposed_behaviour = caos.types.LinkExposedBehaviour(r['exposed_behaviour']) \
                                        if r['exposed_behaviour'] else None

            basemap[name] = bases

            link = proto.Link(name=name, source=source, target=target,
                              spectargets=spectargets,
                              mapping=caos.types.LinkMapping(r['mapping']),
                              exposed_behaviour=exposed_behaviour,
                              required=required,
                              title=title, description=description,
                              is_abstract=r['is_abstract'],
                              is_final=r['is_final'],
                              readonly=r['readonly'],
                              loading=loading,
                              default=r['default'])

            if spectargets:
                # Multiple specified targets, target is a virtual derived object
                target = link.create_common_target(schema, spectargets)

            link_search = None

            if isinstance(target, proto.Atom):
                target, required = self.read_pointer_target_column(
                                            schema, link, None)

                concept_schema, concept_table = \
                    common.concept_name_to_table_name(source.name,
                                                      catenate=False)

                indexes = concept_indexes.get((concept_schema, concept_table))

                if indexes:
                    col_search_index = indexes.get(bases[0])
                    if col_search_index:
                        weight = col_search_index[('default', 'english')]
                        link_search = proto.LinkSearchConfiguration(
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


    def order_links(self, schema):
        indexes = self.read_indexes()

        sql_decompiler = transformer.Decompiler()

        g = {}

        for link in schema(type='link'):
            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.bases:
                g[link.name]['merge'].extend(b.name for b in link.bases)

        topological.normalize(g, merger=proto.Link.merge, schema=schema)

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
                        expr = caosql.generate_source(caosql_tree, pretty=False)
                        schema_name = index.get_metadata('schemaname')
                        index = proto.SourceIndex(name=caos.Name(schema_name),
                                                  subject=link, expr=expr)
                        link.add_index(index)
                        schema.add(index)
            elif link.atomic():
                ptr_stor_info = types.get_pointer_storage_info(
                                    link, schema=schema)
                cols = self._type_mech.get_table_columns(ptr_stor_info.table_name,
                                                         connection=self.connection)
                col = cols[ptr_stor_info.column_name]
                self.verify_ptr_const_defaults(schema, link.name, link.target,
                                               col['column_default'], link.default)


    def read_link_properties(self, schema):
        link_props = datasources.schema.links.LinkProperties(self.connection).fetch()
        link_props = collections.OrderedDict((caos.Name(r['name']), r) for r in link_props)
        basemap = {}

        for name, r in link_props.items():
            bases = ()

            if r['source_id']:
                bases = (proto.LinkProperty.normalize_name(name),)
            elif r['base']:
                bases = tuple(caos.Name(b) for b in r['base'])
            elif name != 'metamagic.caos.builtins.link_property':
                bases = (caos.Name('metamagic.caos.builtins.link_property'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = schema.get(r['source']) if r['source'] else None

            default = self.unpack_default(r['default'])

            required = r['required']
            target = None

            loading = caos.types.PointerLoading(r['loading']) if r['loading'] else None

            basemap[name] = bases

            prop = proto.LinkProperty(name=name,
                                      source=source, target=target,
                                      required=required,
                                      title=title, description=description,
                                      readonly=r['readonly'],
                                      loading=loading,
                                      default=default)

            if source and bases[0] not in {'metamagic.caos.builtins.target',
                                           'metamagic.caos.builtins.source'}:
                # The property is attached to a link, check out link table columns for
                # target information.
                target, required = self.read_pointer_target_column(schema, prop, None)
            else:
                if bases:
                    if bases[0] == 'metamagic.caos.builtins.target' and source is not None:
                        target = source.target
                    elif bases[0] == 'metamagic.caos.builtins.source' and source is not None:
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
                prop.bases = [schema.get(b, type=proto.LinkProperty) for b in bases]


    def order_link_properties(self, schema):
        g = {}

        for prop in schema(type='link_property'):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}
            if prop.bases:
                g[prop.name]['merge'].extend(b.name for b in prop.bases)

        topological.normalize(g, merger=proto.LinkProperty.merge, schema=schema)

        for prop in schema(type='link_property'):
            if not prop.generic() and prop.source.generic():
                source_table_name = common.get_table_name(prop.source, catenate=False)
                cols = self._type_mech.get_table_columns(source_table_name,
                                                         connection=self.connection)
                col_name = common.caos_name_to_pg_name(prop.normal_name())
                col = cols[col_name]
                self.verify_ptr_const_defaults(schema, prop.name, prop.target,
                                               col['column_default'], prop.default)


    def read_computables(self, schema):

        comp_list = datasources.schema.links.Computables(self.connection).fetch()
        comp_list = collections.OrderedDict((caos.Name(r['name']), r) for r in comp_list)

        for name, r in comp_list.items():
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = schema.get(r['source'])
            target = schema.get(r['target'])
            expression = r['expression']
            is_local = r['is_local']
            bases = [schema.get(proto.Pointer.normalize_name(name),
                              type=(proto.Link, proto.LinkProperty))]

            computable = proto.Computable(name=name, source=source, target=target,
                                          title=title, description=description,
                                          is_local=is_local,
                                          expression=expression,
                                          bases=bases)

            source.add_pointer(computable)
            schema.add(computable)


    def order_computables(self, schema):
        pass


    def read_attributes(self, schema):
        attributes_ds = datasources.schema.attributes.Attributes(self.connection)
        attributes = attributes_ds.fetch()

        for r in attributes:
            name = caos.name.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            type = pickle.loads(r['type'])

            attribute = proto.Attribute(name=name, title=title, description=description,
                                        type=type)
            schema.add(attribute)


    def order_attributes(self, schema):
        pass


    def read_attribute_values(self, schema):
        attributes_ds = datasources.schema.attributes.AttributeValues(self.connection)
        attributes = attributes_ds.fetch()

        for r in attributes:
            name = caos.name.Name(r['name'])
            subject = schema.get(r['subject_name'])
            attribute = schema.get(r['attribute_name'])
            value = pickle.loads(r['value'])

            attribute = proto.AttributeValue(name=name, subject=subject, attribute=attribute,
                                             value=value)
            subject.add_attribute(attribute)
            schema.add(attribute)


    def read_actions(self, schema):
        actions_ds = datasources.schema.policy.Actions(self.connection)
        actions = actions_ds.fetch()

        for r in actions:
            name = caos.name.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']

            action = proto.Action(name=name, title=title,
                                  description=description)
            schema.add(action)


    def order_actions(self, schema):
        pass


    def read_events(self, schema):
        events_ds = datasources.schema.policy.Events(self.connection)
        events = events_ds.fetch()

        basemap = {}

        for r in events:
            name = caos.name.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']

            if r['base']:
                bases = tuple(caos.Name(b) for b in r['base'])
            elif name != 'metamagic.caos.builtins.event':
                bases = (caos.Name('metamagic.caos.builtins.event'),)
            else:
                bases = tuple()

            basemap[name] = bases

            event = proto.Event(name=name, title=title, description=description)
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


    def order_events(self, schema):
        pass


    def read_policies(self, schema):
        policies_ds = datasources.schema.policy.Policies(self.connection)
        policies = policies_ds.fetch()

        for r in policies:
            name = caos.name.Name(r['name'])
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            policy = proto.Policy(name=name, title=title, description=description,
                                  subject=schema.get(r['subject']),
                                  event=schema.get(r['event']),
                                  actions=[schema.get(a) for a in r['actions']])
            schema.add(policy)
            policy.subject.add_policy(policy)


    def order_policies(self, schema):
        pass

    def get_type_attributes(self, type_name, connection=None, cache='auto'):
        return self._type_mech.get_type_attributes(type_name, connection, cache)

    def read_concepts(self, schema):
        tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                       table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        concept_list = datasources.schema.concepts.ConceptList(self.connection).fetch()
        concept_list = collections.OrderedDict((caos.Name(row['name']), row) for row in concept_list)

        visited_tables = set()

        table_to_concept_map = {common.concept_name_to_table_name(n, catenate=False): c \
                                                                for n, c in concept_list.items()}

        basemap = {}

        for name, row in concept_list.items():
            concept = {'name': name,
                       'title': self.hstore_to_word_combination(row['title']),
                       'description': row['description'],
                       'is_abstract': row['is_abstract'],
                       'is_final': row['is_final']}

            table_name = common.concept_name_to_table_name(name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept "%s" exists but the table is missing' % name
                raise caos.MetaError(msg, details=details)

            visited_tables.add(table_name)

            bases = self.pg_table_inheritance_to_bases(
                            table['name'], table['schema'],
                            table_to_concept_map)

            basemap[name] = bases

            concept = proto.Concept(name=name, title=concept['title'],
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
            raise caos.MetaError(msg, details=details)


    def order_concepts(self, schema):
        indexes = self.read_indexes()

        sql_decompiler = transformer.Decompiler()

        g = {}
        for concept in schema(type='concept'):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.bases:
                g[concept.name]["merge"].extend(b.name for b in concept.bases)

        topological.normalize(g, merger=proto.Concept.merge, schema=schema)

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
                    index = proto.SourceIndex(name=caos.Name(schema_name),
                                              subject=concept,
                                              expr=expr)
                    concept.add_index(index)
                    schema.add(index)


    def normalize_domain_descr(self, d):
        if d['basetype'] is not None:
            typname, typmods = self.parse_pg_type(d['basetype_full'])
            result = self.pg_type_to_atom_name_and_constraints(typname, typmods)
            if result:
                base, constr = result

        if d['default'] is not None:
            d['default'] = self.interpret_constant(d['default'])

        return d


    def sequence_next(self, seqcls):
        name = common.atom_name_to_sequence_name(seqcls.__sx_prototype__.name)
        name = postgresql.string.quote_literal(name)
        return self.runquery("SELECT nextval(%s)" % name, compat=False, return_stmt=True).first()


    @debug
    def runquery(self, query, params=None, connection=None, compat=True, return_stmt=False):
        if compat:
            cursor = CompatCursor(self.connection)
            query, pxf, nparams = cursor._convert_query(query)
            params = pxf(params)

        connection = connection or self.connection
        ps = connection.prepare(query)

        """LOG [caos.sql] Issued SQL
        print(query)
        print(params)
        """

        if return_stmt:
            return ps
        else:
            if params:
                return ps.rows(*params)
            else:
                return ps.rows()


    @debug
    def execquery(self, query, connection=None):
        connection = connection or self.connection
        """LOG [caos.sql] Issued SQL
        print(query)
        """
        connection.execute(query)


    def pg_table_inheritance(self, table_name, schema_name):
        inheritance = introspection.tables.TableInheritance(self.connection)
        inheritance = inheritance.fetch(table_name=table_name, schema_name=schema_name, max_depth=1)
        return tuple(i[:2] for i in inheritance[1:])


    def pg_table_inheritance_to_bases(self, table_name, schema_name, table_to_concept_map):
        bases = []

        for table in self.pg_table_inheritance(table_name, schema_name):
            base = table_to_concept_map[table[:2]]
            bases.append(base['name'])

        return tuple(bases)


    def parse_pg_type(self, type_expr):
        tree = self.parser.parse('None::' + type_expr)
        typname, typmods = self.type_expr.match(tree)
        return typname, typmods


    def pg_type_to_atom_name_and_constraints(self, typname, typmods):
        typeconv = types.base_type_name_map_r.get(typname)
        if typeconv:
            if isinstance(typeconv, caos.Name):
                name = typeconv
                constraints = ()
            else:
                name, constraints = typeconv(self.connection, typname, *typmods)
            return name, constraints
        return None


    def atom_from_pg_type(self, type_expr, atom_schema, atom_default, schema):

        typname, typmods = self.parse_pg_type(type_expr)
        if isinstance(typname, tuple):
            domain_name = typname[-1]
        else:
            domain_name = typname
            if atom_schema != common.caos_module_name_to_schema_name('metamagic.caos.builtins'):
                typname = (atom_schema, typname)
        atom_name = self.domain_to_atom_map.get((atom_schema, domain_name))

        if atom_name:
            atom = schema.get(atom_name, None)
        else:
            atom = None

        if not atom:

            typeconv = self.pg_type_to_atom_name_and_constraints(typname, typmods)
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
