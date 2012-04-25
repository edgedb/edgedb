##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re
import collections
import importlib
import itertools
import struct
import uuid

import postgresql
import postgresql.copyman
from postgresql.types.io import lib as pg_io_lib
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix.utils import ast, helper
from semantix.utils.algos import topological, persistent_hash
from semantix.utils.debug import debug
from semantix.utils.lang import yaml
from semantix.utils.lang import protoschema as lang_protoschema
from semantix.utils.nlang import morphology
from semantix.utils import datastructures, markup

from semantix import caos
from semantix.caos import objects as caos_objects

from semantix.caos import backends
from semantix.caos import proto
from semantix.caos import delta as base_delta
from semantix.caos import debug as caos_debug
from semantix.caos import error as caos_error

from semantix.caos.caosql import transformer as caosql_transformer
from semantix.caos.caosql import codegen as caosql_codegen

from semantix.caos.backends import query as backend_query
from semantix.caos.backends.pgsql import common
from semantix.caos.backends.pgsql import dbops
from semantix.caos.backends.pgsql import delta as delta_cmds
from semantix.caos.backends.pgsql import deltadbops

from . import datasources
from .datasources import introspection

from .transformer import CaosTreeTransformer

from . import ast as pg_ast
from . import astexpr
from . import parser
from . import types
from . import transformer
from . import pool
from . import session


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
                 scrolling_cursor=False, offset=None, limit=None, query_type=None):
        self.chunks = chunks
        self.text = ''.join(chunks)
        self.argmap = argmap
        self.arg_index = arg_index
        self.result_types = result_types
        self.argument_types = collections.OrderedDict((k, argument_types[k]) for k in argmap
                                                      if k in argument_types)

        self.scrolling_cursor = scrolling_cursor
        self.offset = offset.index if offset is not None else None
        self.limit = limit.index if limit is not None else None
        self.query_type = query_type

    def prepare(self, session):
        return PreparedQuery(self, session)

    def prepare_partial(self, session, **kwargs):
        return PreparedQuery(self, session, kwargs)


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

        self.statement = session.get_prepared_statement(text)
        self.init_args = args

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
            arg = kwargs[k]
            if isinstance(arg, caos.types.ConceptObject):
                arg = arg.id
            elif isinstance(arg, caos.types.ConceptClass):
                proto = caos.types.prototype(arg)
                children = proto.children(recursive=True)
                arg = [self._concept_map[proto.name]]
                arg.extend(self._concept_map[c.name] for c in children)
            elif isinstance(arg, tuple) and arg and isinstance(arg[0], caos.types.ConceptClass):
                ids = set()

                for cls in arg:
                    proto = caos.types.prototype(cls)
                    children = proto.children(recursive=True)
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
        self.transformer = CaosTreeTransformer()
        self.current_portal = None

    def transform(self, query, scrolling_cursor=False, context=None, *, proto_schema):
        if scrolling_cursor:
            offset = query.offset
            limit = query.limit
        else:
            offset = limit = None

        if scrolling_cursor:
            query.offset = None
            query.limit = None

        qchunks, argmap, arg_index, query_type = self.transformer.transform(query, self.session)

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
                     argument_types=argtypes, scrolling_cursor=scrolling_cursor,
                     offset=offset, limit=limit, query_type=query_type)


class BinaryCopyProducer(postgresql.copyman.IteratorProducer):

    def row_pack(self, seq, oid_pack=pg_io_lib.oid_pack,
                            long_pack=pg_io_lib.long_pack,
                            null_sequence=pg_io_lib.null_sequence):
        return b''.join([
            # (null_seq or data)
            (y is None and null_sequence or (long_pack(len(y)) + y))
            for x, y in seq
        ])

    def __init__(self, backend, session, table, proto, data):
        self.backend = backend
        self.table = table
        self.data = data
        self.typid = backend.typrelid_for_source_name(proto.name)
        self.pack = session.get_connection().typio.resolve_pack(self.typid)
        self.session = session
        self.proto = proto

        super().__init__(self._iterator())

    def _iterator(self):
        tlen = struct.pack('!h', self.colcount)
        trail = struct.pack('!h', -1)
        yield (b'PGCOPY\n\377\r\n\0', struct.pack('!ii', 0, 0))

        for tup in self.data_feed():
            row = self.pack(tup, pack=self.row_pack)
            yield (tlen, row,)

        yield (trail,)


class SourceCopyProducer(BinaryCopyProducer):
    def __init__(self, backend, session, table, source, data):
        source_proto = caos.types.prototype(source)

        super().__init__(backend, session, table, source_proto, data)

        self.attrmap = {}

        for ptr_name, ptr_cls in source.iter_pointers():
            if isinstance(ptr_cls, caos.types.AtomClass):
                self.attrmap[common.caos_name_to_pg_name(ptr_name)] = str(ptr_name)


class EntityCopyProducer(SourceCopyProducer):
    def __init__(self, backend, session, table, source, data):
        super().__init__(backend, session, table, source, data)
        self.colcount = len(self.attrmap) + 1
        self.now = backend.runquery('SELECT CURRENT_TIMESTAMP', (), return_stmt=True).first()
        concept_map = backend.get_concept_map(session)
        self.source_id = concept_map[caos.types.prototype(source).name]

    def data_feed(self):
        updates = {}

        for entity in self.data:
            tup = []
            updates.clear()

            if not entity.id:
                updates['id'] = uuid.uuid1()
                updates['ctime'] = self.now

            updates['mtime'] = self.now

            entity._instancedata.update(entity, updates, register_changes=False, allow_ro=True)

            for column in self.table:
                if column.name == 'concept_id':
                    tup.append(self.source_id)
                else:
                    link_name = self.attrmap[column.name]
                    tup.append(getattr(entity, link_name))

            yield tup


class LinkCopyProducer(SourceCopyProducer):
    def __init__(self, backend, session, table, source, data):
        super().__init__(backend, session, table, source, data)
        self.link_map = self.backend.get_link_map(self.session)
        self.colcount = len(self.attrmap) + 3

    def data_feed(self):
        for link_name, source, targets in self.data:
            target = getattr(source.__class__, str(link_name))

            link_names = [(target, caos.types.prototype(target.as_link()).name)]


            for target in targets:
                tup = []

                for t, full_link_name in link_names:
                    if isinstance(target, t):
                        break
                else:
                    assert False, "No link found"

                linkobj = caos.concept.getlink(source, link_name, target)
                link_id = self.link_map[full_link_name]

                for column in self.table:
                    colname = column.name
                    if colname == 'link_type_id':
                        tup.append(link_id)
                    elif colname == 'source_id':
                        tup.append(source.id)
                    elif colname == 'target_id':
                        id = target.id if not isinstance(target, caos.atom.Atom) else None
                        tup.append(id)
                    else:
                        tup.append(getattr(linkobj, self.attrmap[colname]))

                yield tup


class Backend(backends.MetaBackend, backends.DataBackend):

    typlen_re = re.compile(r"(?P<type>.*) \( (?P<length>\d+ (?:\s*,\s*(\d+))*) \)$",
                           re.X)

    constraint_type_re = re.compile(r"^(?P<type>[.\w-]+)(?:_\d+)?$", re.X)

    search_idx_name_re = re.compile(r"""
        .*_(?P<language>\w+)_(?P<index_class>\w+)_search_idx$
    """, re.X)

    atom_constraint_name_re = re.compile(r"""
        ^(?P<concept_name>[.\w]+):(?P<link_name>[.\w]+)::(?P<constraint_class>[.\w]+)::atom_constr$
    """, re.X)

    ptr_constraint_name_re = re.compile(r"""
        ^(?P<concept_name>[.\w]+):(?P<link_name>[.\w]+)::(?P<constraint_class>[.\w]+)::ptr_constr$
    """, re.X)

    error_res = {
        postgresql.exceptions.UniqueError: collections.OrderedDict((
            ('link_mapping',
             re.compile(r'^duplicate key value violates unique constraint "(?P<constr_name>.*_link_mapping_idx)"$')),
            ('ptr_constraint',
             re.compile(r'^duplicate key value violates unique constraint "(?P<constr_name>.*)"$'))
        ))
    }


    def __init__(self, deltarepo, connector_factory):
        connector = connector_factory()
        async_connector = connector_factory(async=True)

        self.features = None
        self.backend_info = None
        self.modules = None

        self.connection_pool = pool.ConnectionPool(connector, backend=self)
        self.async_connection_pool = pool.ConnectionPool(async_connector, backend=self)

        self.connection = connector(pool=self.connection_pool)
        self.connection.connect()

        self.link_cache = {}
        self.concept_cache = {}
        self.table_cache = {}
        self.batch_instrument_cache = {}
        self.batches = {}
        self.domain_to_atom_map = {}
        self.column_cache = {}
        self.table_id_to_proto_name_cache = {}
        self.proto_name_to_table_id_cache = {}
        self.attribute_link_map_cache = {}

        self._table_atom_constraints_cache = None
        self._table_ptr_constraints_cache = None

        self.parser = parser.PgSQLParser()
        self.search_idx_expr = astexpr.TextSearchExpr()
        self.type_expr = astexpr.TypeExpr()
        self.atom_constr_exprs = {}
        self.constant_expr = None

        self.meta = proto.ProtoSchema()

        repo = deltarepo(self.connection)
        self._init_introspection_cache()
        super().__init__(repo)


    def init_connection(self, connection):
        if self.backend_info is None:
            self.backend_info = self.read_backend_info()

        if self.backend_info['format_version'] < delta_cmds.BACKEND_FORMAT_VERSION:
            self.upgrade_backend(connection)
        elif self.backend_info['format_version'] > delta_cmds.BACKEND_FORMAT_VERSION:
            msg = 'unsupported backend format version: %d' % self.backend_info['format_version']
            details = 'The largest supported backend format version is %d' \
                        % delta_cmds.BACKEND_FORMAT_VERSION
            raise caos.MetaError(msg, details=details)

        if self.features is None:
            self.features = self.read_features()
        self.init_features(connection)


    def _init_introspection_cache(self):
        if self.backend_info['initialized']:
            self.column_cache = self._init_column_cache()
            self.table_id_to_proto_name_cache, self.proto_name_to_table_id_cache = self._init_relid_cache()
            self.domain_to_atom_map = self._init_atom_map_cache()
            # Concept map needed early for type filtering operations in schema queries
            self.get_concept_map(force_reload=True)


    def _init_column_cache(self):
        colsds = introspection.tables.TableColumns(self.connection)
        cols = colsds.fetch(schema_name='caos_%')

        column_cache = {}

        for col in cols:
            table_name = (col['table_schema'], col['table_name'])
            cache = column_cache.get(table_name)
            if cache is None:
                column_cache[table_name] = cache = collections.OrderedDict()
            cache[col['column_name']] = col

        return column_cache


    def _init_relid_cache(self):
        link_tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                            table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = datasources.meta.links.ConceptLinks(self.connection).fetch()
        links_list = collections.OrderedDict((caos.Name(r['name']), r) for r in links_list)

        table_id_to_proto_name_cache = {}
        proto_name_to_table_id_cache = {}

        for link_name, link in links_list.items():
            if not link['source_id']:
                link_table_name = common.link_name_to_table_name(link_name, catenate=False)
                t = link_tables.get(link_table_name)
                if t:
                    table_id_to_proto_name_cache[t['oid']] = link_name
                    proto_name_to_table_id_cache[link_name] = t['typoid']

        tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                       table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        concept_list = datasources.meta.concepts.ConceptList(self.connection).fetch()
        concept_list = collections.OrderedDict((caos.Name(row['name']), row) for row in concept_list)

        for name, row in concept_list.items():
            table_name = common.concept_name_to_table_name(name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept "%s" exists but the table is missing' % name
                raise caos.MetaError(msg, details=details)

            table_id_to_proto_name_cache[table['oid']] = name
            proto_name_to_table_id_cache[name] = table['typoid']

        return table_id_to_proto_name_cache, proto_name_to_table_id_cache


    def _init_atom_map_cache(self):
        domains = introspection.domains.DomainsList(self.connection).fetch(schema_name='caos%',
                                                                           domain_name='%_domain')
        domains = {(d['schema'], d['name']): self.normalize_domain_descr(d) for d in domains}

        atom_list = datasources.meta.atoms.AtomList(self.connection).fetch()

        domain_to_atom_map = {}

        for row in atom_list:
            name = caos.Name(row['name'])

            domain_name = common.atom_name_to_domain_name(name, catenate=False)

            domain = domains.get(domain_name)
            if not domain:
                # That's fine, automatic atoms are not represented by domains, skip them,
                # they'll be handled by read_links()
                continue

            domain_to_atom_map[domain_name] = name

        return domain_to_atom_map


    def init_features(self, connection):
        for feature_class_name in self.features.values():
            feature_class = helper.get_object(feature_class_name)
            feature_class.init_feature(connection)


    def upgrade_backend(self, connection):
        with self.connection.xact() as xact:
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


    def getmeta(self):
        if not self.meta.modules:
            if self.backend_info['initialized']:
                self._init_introspection_cache()

                self.read_modules(self.meta)
                self.read_atoms(self.meta)
                self.read_concepts(self.meta)
                self.read_links(self.meta)
                self.read_link_properties(self.meta)
                self.read_computables(self.meta)

                self.order_atoms(self.meta)
                self.order_link_properties(self.meta)
                self.order_computables(self.meta)
                self.order_links(self.meta)
                self.order_concepts(self.meta)

                self.free_resources()

        return self.meta


    def adapt_delta(self, delta):
        return delta_cmds.CommandMeta.adapt(delta)

    @debug
    def process_delta(self, delta, meta, session=None):
        """LOG [caos.delta.plan] PgSQL Delta Plan
            markup.dump(delta)
        """
        delta = self.adapt_delta(delta)
        connection = session.get_connection() if session else self.connection
        context = delta_cmds.CommandContext(connection, session=session)
        delta.apply(meta, context)
        return delta


    def execute_delta_plan(self, plan, session=None):
        connection = session.get_connection() if session else self.connection
        plan.execute(delta_cmds.CommandContext(connection, session=session))


    @debug
    def apply_delta(self, delta, session):
        if isinstance(delta, base_delta.DeltaSet):
            deltas = list(delta)
        else:
            deltas = [delta]

        proto_schema = self.getmeta()

        with session.transaction():
            old_conn = self.connection
            self.connection = session.get_connection()

            for d in deltas:
                delta = d.deltas[0]

                """LINE [caos.delta.apply] Applying delta
                    '{:032x}'.format(d.id)
                """

                session.replace_schema(proto_schema)

                # Run preprocess pass
                delta.call_hook(session, stage='preprocess', hook='main')

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
                    raise base_delta.DeltaError(msg, delta=d) from e

                # Invalidate transient structure caches
                self.invalidate_transient_cache()

                # Update introspection caches
                self._init_introspection_cache()

                # Run postprocess pass
                delta.call_hook(session, stage='postprocess', hook='main')

            self._update_repo(session, deltas)

            self.invalidate_meta_cache()

            introspected_schema = self.getmeta()

            if introspected_schema.get_checksum() != d.checksum:
                details = ('Schema checksum verification failed (expected "%x", got "%x") when '
                           'applying delta "%x".' % (d.checksum, introspected_schema.get_checksum(),
                                                     deltas[-1].id))
                hint = 'This usually indicates a bug in backend delta adapter.'
                raise base_delta.DeltaChecksumError('failed to apply schema delta'
                                                    'checksums do not match',
                                                    details=details, hint=hint,
                                                    schema1=proto_schema,
                                                    schema2=introspected_schema,
                                                    schema1_title='Expected Schema',
                                                    schema2_title='Schema in Backend')

            self.connection = old_conn

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


    def invalidate_meta_cache(self):
        self.meta = proto.ProtoSchema()
        self.backend_info = self.read_backend_info()
        self.features = self.read_features()
        self._table_atom_constraints_cache = None
        self._table_ptr_constraints_cache = None

        self.invalidate_transient_cache()


    def invalidate_transient_cache(self):
        self.link_cache.clear()
        self.concept_cache.clear()
        self.table_cache.clear()
        self.batch_instrument_cache.clear()
        self.domain_to_atom_map.clear()
        self.column_cache.clear()
        self.table_id_to_proto_name_cache.clear()
        self.proto_name_to_table_id_cache.clear()
        self.attribute_link_map_cache.clear()


    def concept_name_from_id(self, id, session):
        concept = caos.Name('semantix.caos.builtins.BaseObject')
        query = '''SELECT c.name
                   FROM
                       %s AS e
                       INNER JOIN caos.concept AS c ON c.id = e.concept_id
                   WHERE e."semantix.caos.builtins.id" = $1
                ''' % (common.concept_name_to_table_name(concept))
        ps = session.get_connection().prepare(query)
        concept_name = ps.first(id)
        if concept_name:
            concept_name = caos.Name(ps.first(id))
        return concept_name


    def entity_from_row(self, session, concept_name, attribute_map, row):
        concept_map = self.get_concept_map(session)

        concept_id = row[attribute_map['concept_id']]

        if concept_id is None:
            # empty record
            return None

        real_concept = concept_map[concept_id]

        if real_concept == concept_name:
            concept = session.proto_schema.get(concept_name)
            attribute_link_map = self.get_attribute_link_map(concept, attribute_map)

            links = {k: row[i] for k, i in attribute_link_map.items()}
            id = links['semantix.caos.builtins.id']
        else:
            id = row[attribute_map[common.caos_name_to_pg_name('semantix.caos.builtins.id')]]
            links = self.load_entity(real_concept, id, session)
            concept_name = real_concept

        concept = session.schema.get(concept_name)
        return session._merge(id, concept, links)


    def load_entity(self, concept, id, session):
        query = 'SELECT * FROM %s WHERE "semantix.caos.builtins.id" = $1' % \
                                                (common.concept_name_to_table_name(concept))

        ps = session.get_connection().prepare(query)
        result = ps.first(id)

        if result is not None:
            concept_proto = session.proto_schema.get(concept)
            ret = {}

            for link_name, link in concept_proto.pointers.items():

                if link.atomic() and not isinstance(link, caos.types.ProtoComputable) \
                            and link_name != 'semantix.caos.builtins.id' \
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
        table = common.link_name_to_table_name(proto_link.normal_name(), catenate=True)

        if pointers:
            protopointers = [caos.types.prototype(p) for p in pointers]
            pointers = {p.normal_name(): p for p in protopointers}
        else:
            pointers = {n: p for n, p in proto_link.pointers.items()
                             if p.loading != caos.types.LazyLoading}

        targets = []

        for prop_name in pointers:
            targets.append(common.qname('l', common.caos_name_to_pg_name(prop_name)))

        query = '''SELECT
                       %s
                   FROM
                       %s AS l
                   WHERE
                       l.source_id = $1
                       AND l.target_id IS NOT DISTINCT FROM $2
                       AND l.link_type_id = $3''' % (', '.join(targets), table)

        ps = session.get_connection().prepare(query)
        if isinstance(target.__class__, caos.types.AtomClass):
            target_id = None
        else:
            target_id = target.id

        link_map = self.get_link_map(session)
        link_id = link_map[proto_link.name]

        result = ps(source.id, target_id, link_id)

        if result:
            result = result[0]
            ret = {}

            for propname in pointers:
                colname = common.caos_name_to_pg_name(propname)
                ret[str(propname)] = result[colname]

            return ret

        else:
            return {}


    def _interpret_db_error(self, err, source, pointer=None):
        if isinstance(err, postgresql.exceptions.UniqueError):
            eres = self.error_res[postgresql.exceptions.UniqueError]

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

            elif error_type == 'ptr_constraint':
                constraint = self.constraint_from_pg_name(error_data)
                if constraint is None:
                    return caos.error.UninterpretedStorageError(err.message)

                constraint, pointer_name, source_table = constraint

                msg = 'unique link constraint violation'

                src_table = common.get_table_name(caos.types.prototype(source.__class__),
                                                  catenate=False)
                if source_table == src_table:
                    pointer = getattr(source.__class__, str(pointer_name)).as_link()
                elif pointer:
                    src_table = common.get_table_name(caos.types.prototype(pointer), catenate=False)
                    if source_table == src_table:
                        source = caos.concept.getlink(source,
                                                      caos.types.prototype(pointer).normal_name(),
                                                      None)
                        pointer = getattr(pointer, str(pointer_name))

                errcls = caos.error.PointerConstraintUniqueViolationError
                return errcls(msg=msg, source=source, pointer=pointer, constraint=constraint)
        else:
            return caos.error.UninterpretedStorageError(err.message)


    @debug
    def store_entity(self, entity, session):
        cls = entity.__class__
        prototype = caos.types.prototype(cls)
        concept = cls._metadata.name
        id = entity.id
        links = entity._instancedata.pointers
        table = self.get_table(prototype, session)

        connection = session.get_connection() if session else self.connection
        concept_map = self.get_concept_map(session)
        context = delta_cmds.CommandContext(connection, session)

        idquery = dbops.Query(text='caos.uuid_generate_v1mc()', params=(), type='uuid')
        now = dbops.Query(text="'NOW'", params=(), type='timestamptz')

        is_object = issubclass(cls, session.schema.semantix.caos.builtins.Object)

        with connection.xact():

            attrs = {}
            for link_name, link_cls in cls.iter_pointers():
                if isinstance(link_cls, caos.types.AtomClass) and \
                                                    link_name != 'semantix.caos.builtins.id':
                    if not isinstance(link_cls._class_metadata.link, caos.types.ComputableClass) \
                                                                        and link_name in links:
                        attrs[common.caos_name_to_pg_name(link_name)] = links[link_name]

            rec = table.record(**attrs)

            returning = ['"semantix.caos.builtins.id"']
            if is_object:
                returning.extend(('"semantix.caos.builtins.ctime"',
                                  '"semantix.caos.builtins.mtime"'))

            if id is not None:
                condition = [('semantix.caos.builtins.id', id)]

                if is_object:
                    setattr(rec, 'semantix.caos.builtins.mtime', now)
                    condition.append(('semantix.caos.builtins.mtime', entity.mtime))

                cmd = dbops.Update(table=table, record=rec,
                                   condition=condition,
                                   returning=returning)
            else:
                setattr(rec, 'semantix.caos.builtins.id', idquery)

                if is_object:
                    setattr(rec, 'semantix.caos.builtins.ctime', now)
                    setattr(rec, 'semantix.caos.builtins.mtime', now)

                rec.concept_id = concept_map[concept]

                cmd = dbops.Insert(table=table, records=[rec], returning=returning)

            try:
                rows = cmd.execute(context)
            except postgresql.exceptions.Error as e:
                raise self._interpret_db_error(e, entity) from e

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
                updates = {'id': id[0], 'ctime': id[1], 'mtime': id[2]}
            else:
                updates = {'id': id[0]}
            entity._instancedata.update(entity, updates, register_changes=False, allow_ro=True)
            session.add_entity(entity)

        return id


    def start_batch(self, session, batch_id):
        self.batches[batch_id] = {'objects': set()}


    def create_batch_merger(self, prototype, session):
        text = r"""
        CREATE OR REPLACE FUNCTION %(func_name)s (batch_id TEXT)
        RETURNS SETOF %(key)s
        LANGUAGE plpgsql
        AS $$
        DECLARE
            row %(table_name)s%%ROWTYPE;
        BEGIN
            RETURN QUERY EXECUTE '
                UPDATE
                    %(table_name)s AS t
                SET
                    (%(cols)s) = (%(vals)s)
                FROM
                    (SELECT
                        batch.*
                     FROM
                        %(table_name)s AS t
                        INNER JOIN %(batch_prefix)s' || batch_id || '" AS batch
                            ON (%(key_condition)s)) AS batch
                WHERE
                    %(key_condition)s
                RETURNING
                    %(keys)s';
            RETURN;
        END;
        $$;
        """

        table_name = common.get_table_name(prototype, catenate=False)
        name = '%x_batch_' % persistent_hash.persistent_hash(prototype.name.name)
        batch_prefix = common.qname(table_name[0], name)[:-1]
        func_name = common.qname(table_name[0],
                                 common.caos_name_to_pg_name(prototype.name.name + '_batch_merger'))

        columns = self.get_table_columns(table_name)

        cols = ','.join(common.qname(col) for col in columns.keys())
        vals = ','.join('batch.%s' % common.qname(col) for col in columns.keys())

        if isinstance(prototype, caos.types.ProtoConcept):
            condkeys = keys = ('semantix.caos.builtins.id', 'concept_id')
            key = common.concept_name_to_table_name(caos.Name('semantix.caos.builtins.BaseObject'))
        elif isinstance(prototype, caos.types.ProtoLink):
            keys = ('source_id', 'target_id', 'link_type_id')
            condkeys = ('source_id', ("coalesce(%(tab)s.target_id, ''00000000-0000-0000-0000-000000000000'')",), 'link_type_id')
            key = common.link_name_to_table_name(caos.Name('semantix.caos.builtins.link'))

        def _format_row(tabname, cols):
            return ','.join(('%s.%s' % (tabname, common.quote_ident(k))) if not isinstance(k, tuple)
                            else (k[0] % {'tab': tabname}) for k in cols)

        key_condition = '(%s) = (%s)' % (_format_row('t', condkeys), _format_row('batch', condkeys))

        qry = text % {'table_name': common.qname(*table_name), 'batch_prefix': batch_prefix,
                      'cols': cols, 'key_condition': key_condition,
                      'func_name': func_name,
                      'keys': ','.join('t.%s' % common.quote_ident(k) for k in keys),
                      'vals': vals, 'key': key}

        self.execquery(qry, session.get_connection())

        return func_name


    def get_batch_instruments(self, prototype, session, batch_id):
        result = self.batch_instrument_cache.get(batch_id)
        if result:
            result = result.get(prototype)

        if not result:
            model_table = self.get_table(prototype, session)
            name = '%x_batch_%x' % (persistent_hash.persistent_hash(prototype.name.name), batch_id)
            table_name = (model_table.name[0], common.caos_name_to_pg_name(name))
            batch_table = dbops.Table(table_name)

            cols = self.get_table_columns(model_table.name)
            colmap = {c.name: c for c in model_table.columns()}
            batch_table.add_columns(colmap[col] for col in cols)

            context = delta_cmds.CommandContext(session.get_connection(), session)
            dbops.CreateTable(batch_table).execute(context)

            merger_func = self.create_batch_merger(prototype, session)

            if isinstance(prototype, caos.types.ProtoConcept):
                keys = ('semantix.caos.builtins.id', 'concept_id')
            elif isinstance(prototype, caos.types.ProtoLink):
                keys = ('source_id', 'target_id', 'link_type_id')

            name = '%x_batch_%x_updated' % (persistent_hash.persistent_hash(prototype.name.name),
                                            batch_id)
            schema = common.caos_module_name_to_schema_name(prototype.name.module)
            updates_table_name = (schema, common.caos_name_to_pg_name(name))

            updates_table = dbops.Table(updates_table_name)
            updates_table.add_columns(colmap[col] for col in keys)
            dbops.CreateTable(updates_table).execute(context)

            result = (batch_table, updates_table, merger_func)

            self.batch_instrument_cache.setdefault(batch_id, {})[prototype] = result

        return result


    def commit_batch(self, session, batch_id):
        for prototype in self.batches[batch_id]['objects']:
            self.merge_batch_table(session, prototype, batch_id)


    def close_batch(self, session, batch_id):
        for prototype in self.batches[batch_id]['objects']:

            batch_table, updates_table, merger = self.get_batch_instruments(prototype, session,
                                                                            batch_id)

            connection = session.get_connection()
            self.execquery('DROP TABLE %s' % common.qname(*batch_table.name), connection)
            self.execquery('DROP TABLE %s' % common.qname(*updates_table.name), connection)

        self.batch_instrument_cache.pop(batch_id, None)
        del self.batches[batch_id]


    def merge_batch_table(self, session, prototype, batch_id):
        table = common.get_table_name(prototype, catenate=False)
        batch_table, updates_table, merger_func = self.get_batch_instruments(prototype,
                                                                             session, batch_id)

        columns = self.get_table_columns(table)

        cols = ','.join(common.qname(col) for col in columns.keys())

        if isinstance(prototype, caos.types.ProtoConcept):
            condkeys = keys = ('semantix.caos.builtins.id', 'concept_id')
        elif isinstance(prototype, caos.types.ProtoLink):
            keys = ('source_id', 'target_id', 'link_type_id')
            condkeys = ('source_id', ("coalesce(target_id, '00000000-0000-0000-0000-000000000000')",), 'link_type_id')

        keys = ', '.join(common.quote_ident(k) for k in keys)
        condkeys = ', '.join(common.quote_ident(k) if not isinstance(k, tuple) else k[0]
                             for k in condkeys)

        batch_index_name = common.caos_name_to_pg_name(batch_table.name[1] + '_key_idx')
        updates_index_name = common.caos_name_to_pg_name(updates_table.name[1] + '_key_idx')

        qry = 'CREATE UNIQUE INDEX %(batch_index_name)s ON %(batch_table)s (%(keys)s)' % \
               {'batch_table': common.qname(*batch_table.name), 'keys': keys,
                'batch_index_name': common.quote_ident(batch_index_name)}

        connection = session.get_connection()
        self.execquery(qry, connection)

        with connection.xact():
            self.execquery('LOCK TABLE %s IN ROW EXCLUSIVE MODE' % common.qname(*table),
                           connection)

            qry = '''INSERT INTO %(tab)s (%(keys)s) (SELECT * FROM %(proc_name)s('%(batch_id)x'))''' \
                  % {'tab': common.qname(*updates_table.name),
                     'proc_name': merger_func,
                     'keys': keys,
                     'batch_id': batch_id}
            self.execquery(qry, connection)

            qry = 'CREATE UNIQUE INDEX %(updates_index_name)s ON %(batch_table)s (%(keys)s)' % \
                   {'batch_table': common.qname(*updates_table.name), 'keys': keys,
                    'updates_index_name': common.quote_ident(updates_index_name)}
            self.execquery(qry, connection)

            qry = '''INSERT INTO %(table_name)s (%(cols)s)
                     (SELECT * FROM %(batch_table)s
                      WHERE (%(condkeys)s) NOT IN (SELECT %(condkeys)s FROM %(updated)s))
                  ''' % {'table_name': common.qname(*table),
                         'cols': cols,
                         'batch_table': common.qname(*batch_table.name),
                         'condkeys': condkeys,
                         'updated': common.qname(*updates_table.name)}
            self.execquery(qry, connection)

            self.execquery('TRUNCATE %s' % common.qname(*batch_table.name), connection)
            self.execquery('DROP INDEX %s' % common.qname(batch_table.name[0], batch_index_name),
                           connection)
            self.execquery('TRUNCATE %s' % common.qname(*updates_table.name), connection)
            self.execquery('DROP INDEX %s' % common.qname(updates_table.name[0], updates_index_name),
                           connection)

        self.execquery('ANALYZE %s' % common.qname(*table), connection)


    def store_entity_batch(self, entities, session, batch_id):
        context = delta_cmds.CommandContext(session.get_connection(), session)

        key = lambda i: i.__class__._metadata.name
        for concept, entities in itertools.groupby(sorted(entities, key=key), key=key):
            concept = session.schema.get(concept)
            concept_proto = concept.__sx_prototype__
            table, _, _ = self.get_batch_instruments(concept_proto, session, batch_id)

            self.batches.setdefault(batch_id, {}).setdefault('objects', set()).add(concept_proto)

            producer = EntityCopyProducer(self, session, table, concept, entities)

            cmd = dbops.CopyFrom(table=table, producer=producer, format='binary')
            cmd.execute(context)

    @debug
    def delete_entities(self, entities, session):
        key = lambda i: i.__class__._metadata.name
        result = set()
        modstat_t = common.qname(*deltadbops.EntityModStatType().name)

        for concept, entities in itertools.groupby(sorted(entities, key=key), key=key):
            table = common.concept_name_to_table_name(concept)

            bunch = {(e.id, e.mtime): e for e in entities}

            query = '''DELETE FROM %s
                       WHERE
                           ("semantix.caos.builtins.id", "semantix.caos.builtins.mtime")
                           = any($1::%s[])
                       RETURNING
                           "semantix.caos.builtins.id", "semantix.caos.builtins.mtime"
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
            cl_ds = datasources.meta.links.ConceptLinks(session.get_connection())

            for row in cl_ds.fetch():
                self.link_cache[row['name']] = row['id']

        return self.link_cache


    def get_concept_map(self, session=None, force_reload=False):
        connection = session.get_connection() if session is not None else self.connection

        if not self.concept_cache or force_reload:
            cl_ds = datasources.meta.concepts.ConceptList(connection)

            for row in cl_ds.fetch():
                self.concept_cache[row['name']] = row['id']
                self.concept_cache[row['id']] = caos.Name(row['name'])

        return self.concept_cache


    def get_concept_id(self, concept, session, cache='auto'):
        concept_id = None

        if cache != 'always':
            concept_cache = self.get_concept_map(session)
        else:
            concept_cache = self.concept_cache

        if concept_cache:
            concept_id = concept_cache.get(concept.name)

        if concept_id is None:
            msg = 'could not determine backend id for concept in this context'
            details = 'Concept: {}'.format(concept.name)
            raise caos.MetaError(msg, details=details)

        return concept_id


    def get_attribute_link_map(self, concept, attribute_map, include_lazy=False):
        key = (concept.name, frozenset(attribute_map.items()), include_lazy)

        try:
            attribute_link_map = self.attribute_link_map_cache[key]
        except KeyError:
            attribute_link_map = {}
            for link_name, link in concept.pointers.items():
                if link.atomic() and not isinstance(link, caos.types.ProtoComputable) \
                                 and (include_lazy or link.loading != caos.types.LazyLoading):
                    col_name = common.caos_name_to_pg_name(link_name)
                    attribute_link_map[link_name] = attribute_map[col_name]

            self.attribute_link_map_cache[key] = attribute_link_map

        return attribute_link_map


    def source_name_from_relid(self, table_oid):
        return self.table_id_to_proto_name_cache.get(table_oid)


    def typrelid_for_source_name(self, source_name):
        return self.proto_name_to_table_id_cache.get(source_name)


    def get_table(self, prototype, session):
        table = self.table_cache.get(prototype)

        if not table:
            table_name = common.get_table_name(prototype, catenate=False)
            table = dbops.Table(table_name)

            cols = []

            if isinstance(prototype, caos.types.ProtoLink):
                cols.extend([
                    dbops.Column(name='source_id', type='uuid'),
                    dbops.Column(name='target_id', type='uuid'),
                    dbops.Column(name='link_type_id', type='int'),
                ])

            elif isinstance(prototype, caos.types.ProtoConcept):
                cols.extend([
                    dbops.Column(name='concept_id', type='int')
                ])

            else:
                assert False

            for pointer_name, pointer in prototype.pointers.items():
                if pointer.atomic() and not isinstance(pointer, caos.types.ProtoComputable):
                    col_type = types.pg_type_from_atom(session.proto_schema, pointer.target,
                                                       topbase=True)
                    col_name = common.caos_name_to_pg_name(pointer_name)
                    cols.append(dbops.Column(name=col_name, type=col_type))
            table.add_columns(cols)

            self.table_cache[prototype] = table

        return table


    @debug
    def store_links(self, source, targets, link_name, session, merge=False):
        link_map = self.get_link_map(session)

        target = getattr(source.__class__, str(link_name))
        link_cls = target.as_link()

        table = self.get_table(link_cls._metadata.root_prototype, session)

        link_names = [(target, caos.types.prototype(link_cls).name)]

        cmds = []
        records = []

        context = delta_cmds.CommandContext(session.get_connection(), session)

        for target in targets:
            """LOG [caos.sync]
            print('Merging link %s[%s][%s]---{%s}-->%s[%s][%s]' % \
                  (source.__class__._metadata.name, source.id,
                   (source.name if hasattr(source, 'name') else ''), link_name,
                   target.__class__._metadata.name,
                   getattr(target, 'id', target), (target.name if hasattr(target, 'name') else ''))
                  )
            """

            for t, full_link_name in link_names:
                if isinstance(target, t):
                    break
            else:
                assert False, "No link found"

            link_obj = caos.concept.getlink(source, link_name, target)

            attrs = {}
            for prop_name, prop_cls in link_cls.iter_pointers():
                if not isinstance(prop_cls._class_metadata.link, caos.types.ComputableClass):
                    attrs[common.caos_name_to_pg_name(prop_name)] = getattr(link_obj, str(prop_name))

            rec = table.record(**attrs)

            rec.source_id = source.id
            rec.link_type_id = link_map[full_link_name]

            if isinstance(target, caos.atom.Atom):
                rec.target_id = None
            else:
                rec.target_id = target.id

            if merge:
                condition = [('source_id', rec.source_id), ('target_id', rec.target_id),
                             ('link_type_id', rec.link_type_id)]

                cmds.append(dbops.Merge(table, rec, condition=condition))
            else:
                records.append(rec)

        if records:
            cmds.append(dbops.Insert(table, records))

        if cmds:
            try:
                for cmd in cmds:
                    cmd.execute(context)
            except postgresql.exceptions.UniqueError as e:
                raise self._interpret_db_error(e, source, link_cls) from e


    def store_link_batch(self, links, session, batch_id):
        context = delta_cmds.CommandContext(session.get_connection(), session)

        def flatten_links(links):
            for source, linksets in links:
                for link_name, targets in linksets.items():
                    yield link_name, source, targets

        key = lambda i: i[0]
        for link_name, pairs in itertools.groupby(sorted(flatten_links(links), key=key), key=key):
            link = session.schema.get(link_name)
            link_proto = link._metadata.root_prototype
            table, _, _ = self.get_batch_instruments(link._metadata.root_prototype, session,
                                                     batch_id)

            self.batches.setdefault(batch_id, {'objects': set()})['objects'].add(link_proto)

            producer = LinkCopyProducer(self, session, table, link, pairs)
            cmd = dbops.CopyFrom(table=table, producer=producer, format='binary')
            cmd.execute(context)


    @debug
    def delete_links(self, link_name, endpoints, session):
        table = common.link_name_to_table_name(link_name)

        complete_source_ids = [s.id for s, t in endpoints if t is None]
        partial_endpoints = [(s.id, t.id) for s, t in endpoints if t is not None]

        count = 0

        if complete_source_ids:
            qry = '''DELETE FROM %s WHERE source_id = any($1)''' % table
            params = (complete_source_ids,)

            result = self.runquery(qry, params,
                                   connection=session.get_connection(),
                                   compat=False, return_stmt=True)
            count += result.first(*params)

        if partial_endpoints:
            qry = '''DELETE FROM %s
                     WHERE (source_id, target_id) = any($1::caos.link_endpoints_rec_t[])
                  ''' % table
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


    def read_modules(self, meta):
        schemas = introspection.schemas.SchemasList(self.connection).fetch(schema_name='caos%')
        schemas = {s['name'] for s in schemas}

        context = delta_cmds.CommandContext(self.connection)
        cond = dbops.TableExists(name=('caos', 'module'))
        module_index_exists = cond.execute(context)

        if 'caos' in schemas and module_index_exists:
            modules = datasources.meta.modules.ModuleList(self.connection).fetch()
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
                                             imports=tuple(module['imports'] or ()))
                self.meta.add_module(mod)
                mods.append(mod)

            for mod in mods:
                for imp in mod.imports:
                    if not self.meta.has_module(imp):
                        # Must be a foreign module, import it directly
                        impmod = importlib.import_module(imp)
                        # Again, it must not be a schema module
                        assert not isinstance(impmod, lang_protoschema.SchemaModule)

                        self.meta.add_module(impmod)


    def read_features(self):
        try:
            features = datasources.meta.features.FeatureList(self.connection).fetch()
            return {f['name']: f['class_name'] for f in features}
        except (postgresql.exceptions.SchemaNameError, postgresql.exceptions.UndefinedTableError):
            return {}


    def read_backend_info(self):
        try:
            info = datasources.meta.backend_info.BackendInfo(self.connection).fetch()[0]
            info['initialized'] = True
            return info
        except postgresql.exceptions.SchemaNameError:
            return {'format_version': delta_cmds.BACKEND_FORMAT_VERSION, 'initialized': False}
        except postgresql.exceptions.UndefinedTableError:
            return {'format_version': 0, 'initialized': True}


    def read_atoms(self, meta):
        domains = introspection.domains.DomainsList(self.connection).fetch(schema_name='caos%',
                                                                           domain_name='%_domain')
        domains = {(d['schema'], d['name']): self.normalize_domain_descr(d) for d in domains}

        seqs = introspection.sequences.SequencesList(self.connection).fetch(
                                                schema_name='caos%', sequence_pattern='%_sequence')
        seqs = {(s['schema'], s['name']): s for s in seqs}

        seen_seqs = set()

        atom_list = datasources.meta.atoms.AtomList(self.connection).fetch()

        for row in atom_list:
            name = caos.Name(row['name'])

            domain_name = common.atom_name_to_domain_name(name, catenate=False)

            domain = domains.get(domain_name)
            if not domain:
                # That's fine, automatic atoms are not represented by domains, skip them,
                # they'll be handled by read_links()
                continue

            atom_data = {'name': name,
                         'title': self.hstore_to_word_combination(row['title']),
                         'description': row['description'],
                         'automatic': row['automatic'],
                         'is_abstract': row['is_abstract'],
                         'is_final': row['is_final'],
                         'base': row['base'],
                         'constraints': row['constraints'],
                         'default': row['default'],
                         'attributes': row['attributes'] or {}
                         }

            if atom_data['default']:
                atom_data['default'] = self.unpack_default(row['default'])

            base = caos.Name(atom_data['base'])
            atom = proto.Atom(name=name, base=base, default=atom_data['default'],
                              title=atom_data['title'], description=atom_data['description'],
                              automatic=atom_data['automatic'],
                              is_abstract=atom_data['is_abstract'],
                              is_final=atom_data['is_final'],
                              attributes=atom_data['attributes'])

            # Copy constraints from parent (row['constraints'] does not contain any inherited constraints)
            atom.acquire_parent_data(meta)

            if domain['constraints']:
                constraints = atom.normalize_constraints(meta, domain['constraints'])
                for constraint in constraints:
                    atom.add_constraint(constraint)

            if row['constraints']:
                constraints = []
                for cls, val in row['constraints'].items():
                    constraints.append(helper.get_object(cls)(next(iter(yaml.Language.load(val)))))

                constraints = atom.normalize_constraints(meta, constraints)
                for constraint in constraints:
                    atom.add_constraint(constraint)

            if atom.issubclass(meta, caos_objects.sequence.Sequence):
                seq_name = common.atom_name_to_sequence_name(atom.name, catenate=False)
                if seq_name not in seqs:
                    msg = 'internal metadata incosistency'
                    details = 'Missing sequence for sequence atom "%s"' % atom.name
                    raise caos.MetaError(msg, details=details)
                seen_seqs.add(seq_name)

            meta.add(atom)

        extra_seqs = set(seqs) - seen_seqs
        if extra_seqs:
            msg = 'internal metadata incosistency'
            details = 'Extraneous sequences exist: %s' \
                        % (', '.join(common.qname(*t) for t in extra_seqs))
            raise caos.MetaError(msg, details=details)


    def order_atoms(self, meta):
        pass


    def unpack_default(self, value):
        value = next(iter(yaml.Language.load(value)))

        result = []
        for item in value:
            # XXX: This implicitly relies on yaml backend to be loaded, since
            # adapter for DefaultSpec is defined there.
            adapter = yaml.ObjectMeta.get_adapter(proto.DefaultSpec)
            assert adapter, "could not find YAML adapter for proto.DefaultSpec"
            data = item
            item = adapter.resolve(item)(item)
            item.__sx_setstate__(data)
            result.append(item)
        return result


    def interpret_search_index(self, index_name, index_expression):
        m = self.search_idx_name_re.match(index_name)
        if not m:
            raise caos.MetaError('could not interpret index {}'.format(index_name))

        language = m.group('language')
        index_class = m.group('index_class')

        tree = self.parser.parse(index_expression)
        columns = self.search_idx_expr.match(tree)

        if columns is None:
            msg = 'could not interpret index {!r}'.format(str(index_name))
            details = 'Could not match expression:\n{}'.format(markup.dumps(tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise caos.MetaError(msg, details=details, hint=hint)

        return index_class, language, columns

    def interpret_search_indexes(self, indexes):
        for idx_name, idx_expr in zip(indexes['index_names'], indexes['index_expressions']):
            yield self.interpret_search_index(idx_name, idx_expr)


    def read_search_indexes(self):
        indexes = {}
        index_ds = datasources.introspection.tables.TableIndexes(self.connection)
        for row in index_ds.fetch(schema_pattern='caos%', index_pattern='%_search_idx'):
            tabidx = indexes[tuple(row['table_name'])] = {}

            for index_class, language, columns in self.interpret_search_indexes(row):
                for column_name, column_config in columns.items():
                    idx = tabidx.setdefault(column_name, {})
                    idx[(index_class, column_config[0])] = caos.types.LinkSearchWeight(column_config[1])

        return indexes


    def interpret_index(self, index_cols, index_expression):
        if not index_expression:
            index_expression = '(%s)' % ', '.join(common.quote_ident(c) for c in index_cols)

        tree = self.parser.parse(index_expression)

        return tree


    def interpret_indexes(self, indexes):
        for cols, expr in zip(indexes['index_columns'], indexes['index_expressions']):
            cols = cols.split('~~~~')
            yield self.interpret_index(cols, expr)


    def read_indexes(self):
        indexes = {}
        index_ds = datasources.introspection.tables.TableIndexes(self.connection)
        for row in index_ds.fetch(schema_pattern='caos%', index_pattern='%_reg_idx'):
            indexes[tuple(row['table_name'])] = set(self.interpret_indexes(row))

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


    def read_table_constraints(self, suffix, interpreter):
        constraints = {}
        index_by_pg_name = {}
        constraints_ds = introspection.tables.TableConstraints(self.connection)

        for row in constraints_ds.fetch(schema_pattern='caos%',
                                        constraint_pattern='%%::%s' % suffix):
            concept_constr = constraints[tuple(row['table_name'])] = {}

            for pg_name, (link_name, constraint) in interpreter(row):
                idx = datastructures.OrderedIndex(key=lambda i: i.get_canonical_class())
                ptr_constraints = concept_constr.setdefault(link_name, idx)
                cls = constraint.get_canonical_class()
                try:
                    existing_constraint = ptr_constraints[cls]
                    existing_constraint.merge(constraint)
                except KeyError:
                    ptr_constraints.add(constraint)
                index_by_pg_name[pg_name] = constraint, link_name, tuple(row['table_name'])

        return constraints, index_by_pg_name


    def interpret_atom_constraint(self, constraint_class, expr, name):

        try:
            expr_tree = self.parser.parse(expr)
        except parser.PgSQLParserError as e:
            msg = 'could not interpret constraint %s' % name
            details = 'Syntax error when parsing expression: %s' % e.args[0]
            raise caos.MetaError(msg, details=details) from e

        pattern = self.atom_constr_exprs.get(constraint_class)
        if not pattern:
            adapter = astexpr.AtomConstraintAdapterMeta.get_adapter(constraint_class)

            if not adapter:
                msg = 'could not interpret constraint %s' % name
                details = 'No matching pattern defined for constraint class "%s"' % constraint_class
                hint = 'Implement matching pattern for "%s"' % constraint_class
                hint += '\nExpression:\n{}'.format(markup.dumps(expr_tree))
                raise caos.MetaError(msg, details=details, hint=hint)

            pattern = adapter()
            self.atom_constr_exprs[constraint_class] = pattern

        constraint_data = pattern.match(expr_tree)

        if constraint_data is None:
            msg = 'could not interpret constraint {!r}'.format(str(name))
            details = 'Pattern "{!r}" could not match expression:\n{}'. \
                                        format(pattern.__class__, markup.dumps(expr_tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise caos.MetaError(msg, details=details, hint=hint)

        return constraint_data


    def interpret_table_atom_constraint(self, name, expr):
        m = self.atom_constraint_name_re.match(name)
        if not m:
            raise caos.MetaError('could not interpret table constraint %s' % name)

        link_name = m.group('link_name')
        constraint_class = helper.get_object(m.group('constraint_class'))
        constraint_data = self.interpret_atom_constraint(constraint_class, expr, name)

        return link_name, constraint_class(constraint_data)


    def interpret_table_atom_constraints(self, constr):
        cs = zip(constr['constraint_names'], constr['constraint_expressions'],
                 constr['constraint_descriptions'])

        for name, expr, description in cs:
            yield name, self.interpret_table_atom_constraint(description, expr)


    def read_table_atom_constraints(self):
        if self._table_atom_constraints_cache is None:
            constraints, index = self.read_table_constraints('atom_constr',
                                                             self.interpret_table_atom_constraints)
            self._table_atom_constraints_cache = (constraints, index)

        return self._table_atom_constraints_cache


    def get_table_atom_constraints(self):
        return self.read_table_atom_constraints()[0]


    def interpret_table_ptr_constraint(self, name, expr, columns):
        m = self.ptr_constraint_name_re.match(name)
        if not m:
            raise caos.MetaError('could not interpret table constraint %s' % name)

        link_name = m.group('link_name')
        constraint_class = helper.get_object(m.group('constraint_class'))

        if issubclass(constraint_class, proto.PointerConstraintUnique):
            col_name = common.caos_name_to_pg_name(link_name)
            if len(columns) != 1 or not col_name in columns:
                msg = 'internal metadata inconsistency'
                details = ('Link constraint "%s" expected to have exactly one column "%s" '
                           'in the expression, got: %s') % (name, col_name,
                                                            ','.join('"%s"' % c for c in columns))
                raise caos.MetaError(msg, details=details)

            constraint_data = {True}
        else:
            msg = 'internal metadata inconsistency'
            details = 'Link constraint "%s" has an unexpected class "%s"' % \
                      (name, m.group('constraint_class'))
            raise caos.MetaError(msg, details=details)

        return link_name, constraint_class(constraint_data)


    def interpret_table_ptr_constraints(self, constr):
        cs = zip(constr['constraint_names'], constr['constraint_expressions'],
                 constr['constraint_descriptions'], constr['constraint_columns'])

        for name, expr, description, cols in cs:
            cols = cols.split('~~~~')
            yield name, self.interpret_table_ptr_constraint(description, expr, cols)


    def read_table_ptr_constraints(self):
        if self._table_ptr_constraints_cache is None:
            constraints, index = self.read_table_constraints('ptr_constr',
                                                             self.interpret_table_ptr_constraints)
            self._table_ptr_constraints_cache = (constraints, index)

        return self._table_ptr_constraints_cache


    def get_table_ptr_constraints(self):
        return self.read_table_ptr_constraints()[0]


    def constraint_from_pg_name(self, pg_name):
        return self.read_table_ptr_constraints()[1].get(pg_name)


    def read_pointer_target_column(self, meta, source, pointer_name, constraints_cache):
        host_schema, host_table = common.get_table_name(source, catenate=False)
        cols = self.get_table_columns((host_schema, host_table))
        constraints = constraints_cache.get((host_schema, host_table))

        col = cols.get(common.caos_name_to_pg_name(pointer_name))

        if not col:
            msg = 'internal metadata inconsistency'
            details = ('Record for "%s" hosted by "%s" exists, but corresponding table column '
                       'is missing' % (pointer_name, source.name))
            raise caos.MetaError(msg, details=details)

        return self._get_pointer_column_target(meta, source, pointer_name, col, constraints)


    def _get_pointer_column_target(self, meta, source, pointer_name, col, constraints):
        derived_atom_name = proto.Atom.gen_atom_name(source, pointer_name)
        if col['column_type_schema'] == 'pg_catalog':
            col_type_schema = common.caos_module_name_to_schema_name('semantix.caos.builtins')
            col_type = col['column_type_formatted']
        else:
            col_type_schema = col['column_type_schema']
            col_type = col['column_type_formatted'] or col['column_type']

        constraints = constraints.get(pointer_name) if constraints else None

        if col['column_default'] is not None:
            atom_default = self.interpret_constant(col['column_default'])
        else:
            atom_default = None

        target = self.atom_from_pg_type(col_type, col_type_schema,
                                        constraints, atom_default, meta,
                                        caos.Name(name=derived_atom_name,
                                                  module=source.name.module))

        return target, col['column_required']


    def unpack_constraints(self, meta, constraints):
        result = []
        if constraints:
            for cls, val in constraints.items():
                constraint = helper.get_object(cls)(next(iter(yaml.Language.load(val))))
                result.append(constraint)
        return result


    def verify_ptr_const_defaults(self, meta, ptr_name, target_atom, tab_default, schema_defaults):
        if schema_defaults:
            ld = list(filter(lambda d: isinstance(d, proto.LiteralDefaultSpec), schema_defaults))
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

        typ = target_atom.get_topmost_base(meta)
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

        schema_value = typ(ld[0].value)

        if schema_value != table_default:
            msg = 'internal metadata inconsistency'
            details = ('Value mismatch in literal default pointer link "%s": %r in the '
                       'table vs. %r in the schema') % (ptr_name, table_default, schema_value)
            raise caos.MetaError(msg, details=details)


    def read_links(self, meta):

        link_tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                            table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = datasources.meta.links.ConceptLinks(self.connection).fetch()
        links_list = collections.OrderedDict((caos.Name(r['name']), r) for r in links_list)

        concept_constraints = self.get_table_atom_constraints()
        ptr_constraints = self.get_table_ptr_constraints()

        concept_indexes = self.read_search_indexes()

        table_to_name_map = {common.link_name_to_table_name(name, catenate=False): name \
                                                                    for name in links_list}

        for name, r in links_list.items():
            bases = tuple()

            if r['source_id']:
                bases = (proto.Link.normalize_name(name),)
            elif r['base']:
                bases = tuple(caos.Name(b) for b in r['base'])
            elif name != 'semantix.caos.builtins.link':
                bases = (caos.Name('semantix.caos.builtins.link'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = meta.get(r['source']) if r['source'] else None
            link_search = None
            constraints = self.unpack_constraints(meta, r['constraints'])
            abstract_constraints = self.unpack_constraints(meta, r['abstract_constraints'])

            if r['default']:
                r['default'] = self.unpack_default(r['default'])

            required = r['required']

            if r['source_id'] and r['is_atom']:
                target, required = self.read_pointer_target_column(meta, source, bases[0],
                                                                   concept_constraints)

                concept_schema, concept_table = common.concept_name_to_table_name(source.name,
                                                                                  catenate=False)

                indexes = concept_indexes.get((concept_schema, concept_table))

                if indexes:
                    col_search_index = indexes.get(bases[0])
                    if col_search_index:
                        weight = col_search_index[('default', 'english')]
                        link_search = proto.LinkSearchConfiguration(weight=weight)

                constr = ptr_constraints.get((concept_schema, concept_table))
                if constr:
                    link_constr = constr.get(bases[0])
                    if link_constr:
                        constraints.extend(link_constr)
            else:
                target = meta.get(r['target']) if r['target'] else None

            loading = caos.types.PointerLoading(r['loading']) if r['loading'] else None
            link = proto.Link(name=name, base=bases, source=source, target=target,
                                mapping=caos.types.LinkMapping(r['mapping']),
                                required=required,
                                title=title, description=description,
                                is_abstract=r['is_abstract'],
                                is_final=r['is_final'],
                                is_atom=r['is_atom'] if target else None,
                                readonly=r['readonly'],
                                loading=loading,
                                default=r['default'])

            if link_search:
                link.search = link_search

            for constraint in constraints:
                link.add_constraint(constraint)

            for constraint in abstract_constraints:
                link.add_abstract_constraint(constraint)

            if source:
                source.add_pointer(link)

            meta.add(link)

        for link in meta(type='link', include_automatic=True):
            link.acquire_parent_data(meta)


    def order_links(self, meta):
        indexes = self.read_indexes()

        reverse_transformer = transformer.PgSQLExprTransformer()
        reverse_caosql_transformer = caosql_transformer.CaosqlReverseTransformer()

        g = {}

        for link in meta(type='link', include_automatic=True):
            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.base:
                g[link.name]['merge'].extend(link.base)

        topological.normalize(g, merger=proto.Link.merge)

        for link in meta(type='link', include_automatic=True):
            link.materialize(meta)

            if link.generic():
                table_name = common.get_table_name(link, catenate=False)
                tabidx = indexes.get(table_name)
                if tabidx:
                    for index in tabidx:
                        caos_tree = reverse_transformer.transform(index, meta, link)
                        caosql_tree = reverse_caosql_transformer.transform(caos_tree)
                        expr = caosql_codegen.CaosQLSourceGenerator.to_source(caosql_tree)
                        link.add_index(proto.SourceIndex(expr=expr))
            elif link.atomic():
                source_table_name = common.get_table_name(link.source, catenate=False)
                cols = self.get_table_columns(source_table_name)
                col_name = common.caos_name_to_pg_name(link.normal_name())
                col = cols[col_name]
                self.verify_ptr_const_defaults(meta, link.name, link.target,
                                               col['column_default'], link.default)


    def read_link_properties(self, meta):
        link_props = datasources.meta.links.LinkProperties(self.connection).fetch()
        link_props = collections.OrderedDict((caos.Name(r['name']), r) for r in link_props)
        atom_constraints = self.get_table_atom_constraints()
        ptr_constraints = self.get_table_ptr_constraints()

        for name, r in link_props.items():
            bases = ()

            if r['source_id']:
                bases = (proto.LinkProperty.normalize_name(name),)
            elif r['base']:
                bases = tuple(caos.Name(b) for b in r['base'])
            elif name != 'semantix.caos.builtins.link_property':
                bases = (caos.Name('semantix.caos.builtins.link_property'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = meta.get(r['source']) if r['source'] else None

            default = self.unpack_default(r['default']) if r['default'] else None

            constraints = []
            abstract_constraints = []

            required = r['required']

            if source:
                # The property is attached to a link, check out link table columns for
                # target information.
                target, required = self.read_pointer_target_column(meta, source, bases[0],
                                                                   atom_constraints)

                constraints = self.unpack_constraints(meta, r['constraints'])
                abstract_constraints = self.unpack_constraints(meta, r['abstract_constraints'])

                link_table = common.get_table_name(source, catenate=False)
                constr = ptr_constraints.get(link_table)
                if constr:
                    ptr_constr = constr.get(bases[0])
                    if ptr_constr:
                        constraints.extend(ptr_constr)
            else:
                target = None

            loading = caos.types.PointerLoading(r['loading']) if r['loading'] else None
            prop = proto.LinkProperty(name=name, base=bases, source=source, target=target,
                                      required=required,
                                      title=title, description=description,
                                      readonly=r['readonly'],
                                      loading=loading,
                                      default=default)

            if source:
                if source.generic():
                    for constraint in constraints:
                        prop.add_constraint(constraint)

                    for constraint in abstract_constraints:
                        prop.add_abstract_constraint(constraint)

                prop.acquire_parent_data(meta)
                source.add_pointer(prop)

            meta.add(prop)


    def order_link_properties(self, meta):
        g = {}

        for prop in meta(type='link_property', include_automatic=True):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}
            if prop.base:
                g[prop.name]['merge'].extend(prop.base)

        topological.normalize(g, merger=proto.LinkProperty.merge)

        for prop in meta(type='link_property', include_automatic=True):
            if not prop.generic() and prop.source.generic():
                source_table_name = common.get_table_name(prop.source, catenate=False)
                cols = self.get_table_columns(source_table_name)
                col_name = common.caos_name_to_pg_name(prop.normal_name())
                col = cols[col_name]
                self.verify_ptr_const_defaults(meta, prop.name, prop.target,
                                               col['column_default'], prop.default)


    def read_computables(self, meta):

        comp_list = datasources.meta.links.Computables(self.connection).fetch()
        comp_list = collections.OrderedDict((caos.Name(r['name']), r) for r in comp_list)

        for name, r in comp_list.items():
            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = meta.get(r['source'])
            target = meta.get(r['target'])
            expression = r['expression']
            is_local = r['is_local']
            bases = (proto.Link.normalize_name(name),)

            computable = proto.Computable(name=name, source=source, target=target,
                                          title=title, description=description,
                                          is_local=is_local,
                                          expression=expression,
                                          base=bases)

            source.add_pointer(computable)
            meta.add(computable)


    def order_computables(self, meta):
        pass


    def get_table_columns(self, table_name, cache='auto', connection=None):
        cols = self.column_cache.get(table_name) if cache is not None else None

        if cols is None and cache != 'always':
            if connection is None:
                connection = self.connection

            cols = introspection.tables.TableColumns(connection)
            cols = cols.fetch(table_name=table_name[1], schema_name=table_name[0])
            cols = collections.OrderedDict((col['column_name'], col) for col in cols)
            self.column_cache[table_name] = cols

            if not cols:
                tlist = introspection.tables.TableList(connection)
                table = tlist.fetch(schema_name=table_name[0], table_pattern=table_name[1])

                if not table:
                    msg = 'internal metadata incosistency'
                    details = 'Could not obtain columns for "%s"."%s"' % table_name
                    raise caos.MetaError(msg, details=details)

        return cols


    def virtual_concept_from_table(self, session, meta, table_name):
        """Interpret concept relying exclusively on the specified table without supporting metadata."""
        ds = introspection.tables.TableList(session.get_connection())
        tables = ds.fetch(schema_name=table_name[0], table_pattern=table_name[1])

        if not tables:
            return None

        if not tables[0]['comment']:
            msg = 'could not determine concept name: table comment is missing'
            raise caos.MetaError(msg)

        name = caos.Name(tables[0]['comment'])

        ds = datasources.meta.concepts.ConceptList(session.get_connection())
        concept_meta = ds.fetch(name=name)

        concept = proto.Concept(name=name, is_virtual=True, is_abstract=True,
                                automatic=concept_meta[0]['automatic'])

        concept.materialize(meta)

        columns = self.get_table_columns(table_name)
        atom_constraints = self.get_table_atom_constraints()
        atom_constraints = atom_constraints.get(table_name)

        ptr_constraints = self.get_table_ptr_constraints()
        ptr_constraints = ptr_constraints.get(table_name)

        for col in columns.values():
            if not col['column_is_local'] or col['column_name'] == 'concept_id':
                continue

            pointer_name = col['column_comment']

            if not pointer_name:
                msg = 'could not determine link name: column comment is missing'
                raise caos.MetaError(msg)

            pointer_name = caos.Name(pointer_name)

            target, required = self._get_pointer_column_target(meta, concept, pointer_name,
                                                               col, atom_constraints)

            ptr = meta.get(pointer_name)
            link = ptr.derive(meta, concept, target)
            link.required = required
            link.is_atom = True

            if ptr_constraints:
                constraints = ptr_constraints.get(pointer_name)
                if constraints:
                    for constraint in constraints:
                        link.add_constraint(constraint)

            concept.add_pointer(link)

        return concept


    def provide_auto_tuple_type(self, caos_types, schema, session):
        pg_types = []

        for caos_type in caos_types:
            if isinstance(caos_type, caos.types.ProtoAtom):
                pg_type = types.pg_type_from_atom(schema, caos_type, topbase=True)
            elif isinstance(caos_type, caos.types.ProtoConcept):
                pg_type = common.get_table_name(caos_type)
            else:
                raise caos.MetaError('unexpected tuple element type: {}'.format(caos_type))

            pg_types.append(pg_type)

        name = ('caos', 'tuple_auto_{:x}'.format(persistent_hash.persistent_hash(tuple(pg_types))))

        type_elems = datastructures.OrderedSet();

        for i, pg_type in enumerate(pg_types):
            type_elems.add(dbops.Column(name='f_{}'.format(i), type=pg_type))
        type = dbops.CompositeType(name, type_elems)
        cond = dbops.CompositeTypeExists(name)
        op = dbops.CreateCompositeType(type, neg_conditions=[cond])

        context = delta_cmds.CommandContext(session.get_connection(), session)
        op.execute(context)

        return common.qname(*name)


    def provide_virtual_concept_table(self, concept, schema, session):
        table_name = common.concept_name_to_table_name(concept.name, catenate=False)

        my_schema = self.getmeta()
        stored_concept = self.virtual_concept_from_table(session, my_schema, table_name)

        updated_concept = concept.copy()
        updated_concept._children = concept._children.copy()
        updated_concept.automatic = True

        ptrs = updated_concept.get_children_common_pointers(schema)

        for ptr in ptrs:
            if ptr.atomic():
                if ptr.target.automatic:
                    target = schema.get(ptr.target.base, type=ptr.target.get_canonical_class())
                else:
                    target = ptr.target
                ptr = ptr.derive(schema, updated_concept, target)

                # Drop all constraints on virtual concept links --- they are needless and
                # potentially harmful.
                for constr_cls in ptr.constraints.copy():
                    ptr.del_constraint(constr_cls)

                ptr.required = False
                ptr.readonly = False
                ptr.search = None
                ptr.title = None
                ptr.description = None

                updated_concept.add_pointer(ptr)

        delta = base_delta.AlterRealm()

        diff = updated_concept.compare(stored_concept)

        if diff != 1.0:
            concept_delta = updated_concept.delta(stored_concept)
            delta.add(concept_delta)
            if isinstance(concept_delta, base_delta.CreateConcept):
                schema.delete(updated_concept)

        for c in updated_concept.children():
            c_table_name = common.concept_name_to_table_name(c.name, catenate=False)

            bases = self.pg_table_inheritance(c_table_name[1], c_table_name[0])
            if table_name not in bases:
                alter = base_delta.AlterConcept(prototype_name=c.name,
                                                prototype_class=c.__class__.get_canonical_class())
                prop = base_delta.AlterPrototypeProperty(property='base', old_value=c.base,
                                                         new_value=c.base + (updated_concept.name,))
                rebase = base_delta.RebaseConcept(prototype_name=c.name,
                                                  prototype_class=c.__class__.get_canonical_class(),
                                                  new_base=c.base + (updated_concept.name,))
                alter.add(prop)
                alter.add(rebase)
                delta.add(alter)

        delta = self.process_delta(delta, my_schema, session)

        self.execute_delta_plan(delta, session)

        # Update oid to proto name mapping cache
        ds = introspection.tables.TableList(session.get_connection())
        table_name = common.concept_name_to_table_name(updated_concept.name, catenate=False)
        tables = ds.fetch(schema_name=table_name[0], table_pattern=table_name[1])

        if not tables:
            msg = 'internal metadata incosistency'
            details = 'Record for concept "%s" exists but the table is missing' % updated_concept.name
            raise caos.MetaError(msg, details=details)

        table = next(iter(tables))

        self.table_id_to_proto_name_cache[table['oid']] = updated_concept.name
        self.proto_name_to_table_id_cache[updated_concept.name] = table['typoid']

        # Update virtual concept table column cache
        self.get_table_columns(table_name, connection=session.get_connection(), cache=None)


    def read_concepts(self, meta):
        tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                       table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        concept_list = datasources.meta.concepts.ConceptList(self.connection).fetch()
        concept_list = collections.OrderedDict((caos.Name(row['name']), row) for row in concept_list)

        visited_tables = set()

        table_to_concept_map = {common.concept_name_to_table_name(n, catenate=False): c \
                                                                for n, c in concept_list.items()}

        for name, row in concept_list.items():
            concept = {'name': name,
                       'title': self.hstore_to_word_combination(row['title']),
                       'description': row['description'],
                       'is_abstract': row['is_abstract'],
                       'is_final': row['is_final'],
                       'is_virtual': row['is_virtual'],
                       'custombases': row['custombases'],
                       'automatic': row['automatic']}

            table_name = common.concept_name_to_table_name(name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept "%s" exists but the table is missing' % name
                raise caos.MetaError(msg, details=details)

            visited_tables.add(table_name)

            if concept['automatic']:
                continue

            bases = self.pg_table_inheritance_to_bases(table['name'], table['schema'],
                                                                      table_to_concept_map)

            concept = proto.Concept(name=name, base=bases, title=concept['title'],
                                    description=concept['description'],
                                    is_abstract=concept['is_abstract'],
                                    is_final=concept['is_final'],
                                    is_virtual=concept['is_virtual'],
                                    custombases=tuple(concept['custombases']))

            meta.add(concept)

        tabdiff = set(tables.keys()) - visited_tables
        if tabdiff:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data tables exist: %s' \
                        % (', '.join('"%s.%s"' % t for t in tabdiff))
            raise caos.MetaError(msg, details=details)


    def order_concepts(self, meta):
        indexes = self.read_indexes()

        reverse_transformer = transformer.PgSQLExprTransformer()
        reverse_caosql_transformer = caosql_transformer.CaosqlReverseTransformer()

        g = {}
        for concept in meta(type='concept', include_automatic=True):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                g[concept.name]["merge"].extend(concept.base)

        topological.normalize(g, merger=proto.Concept.merge)

        for concept in meta(type='concept', include_automatic=True):
            concept.materialize(meta)

            table_name = common.get_table_name(concept, catenate=False)

            tabidx = indexes.get(table_name)
            if tabidx:
                for index in tabidx:
                    caos_tree = reverse_transformer.transform(index, meta, concept)
                    caosql_tree = reverse_caosql_transformer.transform(caos_tree)
                    expr = caosql_codegen.CaosQLSourceGenerator.to_source(caosql_tree)
                    concept.add_index(proto.SourceIndex(expr=expr))


    def load_links(self, this_concept, this_id, other_concepts=None, link_names=None,
                                                                     reverse=False):

        if link_names is not None and not isinstance(link_names, list):
            link_names = [link_names]

        if other_concepts is not None and not isinstance(other_concepts, list):
            other_concepts = [other_concepts]

        if not reverse:
            source_id = this_id
            target_id = None
            source_concepts = [this_concept]
            target_concepts = other_concepts
        else:
            source_id = None
            target_id = this_id
            target_concepts = [this_concept]
            source_concepts = other_concepts

        links = datasources.entities.EntityLinks(self.connection).fetch(
                                        source_id=source_id, target_id=target_id,
                                        target_concepts=target_concepts,
                                        source_concepts=source_concepts,
                                        link_names=link_names)

        return links

    def normalize_domain_descr(self, d):
        constraints = []

        if d['constraint_names'] is not None:
            for constr_name, constr_expr in zip(d['constraint_names'], d['constraints']):
                m = self.constraint_type_re.match(constr_name)
                if m:
                    constr_type = m.group('type')
                else:
                    raise caos.MetaError('could not parse domain constraint "%s": %s' %
                                         (constr_name, constr_expr))

                constr_type = helper.get_object(constr_type)
                constr_data = self.interpret_atom_constraint(constr_type, constr_expr, constr_name)
                constraints.append(constr_type(constr_data))

            d['constraints'] = constraints

        if d['basetype'] is not None:
            typname, typmods = self.parse_pg_type(d['basetype_full'])
            result = self.pg_type_to_atom_name_and_constraints(typname, typmods)
            if result:
                base, constr = result
                constraints.extend(constr)

        if d['default'] is not None:
            d['default'] = self.interpret_constant(d['default'])

        return d


    def sequence_next(self, seqcls):
        name = common.atom_name_to_sequence_name(seqcls._metadata.name)
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
            if not base['automatic']:
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


    def atom_from_pg_type(self, type_expr, atom_schema, atom_constraints, atom_default, meta, derived_name):

        typname, typmods = self.parse_pg_type(type_expr)
        if isinstance(typname, tuple):
            domain_name = typname[-1]
        else:
            domain_name = typname
        atom_name = self.domain_to_atom_map.get((atom_schema, domain_name))

        if atom_name:
            atom = meta.get(atom_name, None)
        else:
            atom = None

        if not atom:
            atom = meta.get(derived_name, None)

        if not atom or atom_constraints:

            typeconv = self.pg_type_to_atom_name_and_constraints(typname, typmods)
            if typeconv:
                name, constraints = typeconv
                atom = meta.get(name)

                constraints = set(constraints)
                if atom_constraints:
                    constraints.update(atom_constraints)

                atom.acquire_parent_data(meta)
            else:
                constraints = set(atom_constraints) if atom_constraints else {}

            if atom_constraints:
                if atom_default is not None:
                    base = meta.get(atom.name)
                    typ = base.get_topmost_base(meta)
                    atom_default = [proto.LiteralDefaultSpec(value=typ(atom_default))]

                atom = proto.Atom(name=derived_name, base=atom.name, default=atom_default,
                                  automatic=True)

                constraints = atom.normalize_constraints(meta, constraints)

                for constraint in constraints:
                    atom.add_constraint(constraint)

                atom.acquire_parent_data(meta)

                meta.add(atom)

        assert atom
        return atom


    def hstore_to_word_combination(self, hstore):
        if hstore:
            return morphology.WordCombination.from_dict(hstore)
        else:
            return None
