##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re
import collections
import itertools
import uuid

import postgresql
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix.utils import ast, helper
from semantix.utils.algos import topological, persistent_hash
from semantix.utils.debug import debug
from semantix.utils.lang import yaml
from semantix.utils.nlang import morphology
from semantix.utils import datastructures

from semantix import caos
from semantix.caos import objects as caos_objects

from semantix.caos import backends
from semantix.caos import proto
from semantix.caos import delta as base_delta

from semantix.caos.caosql import transformer as caosql_transformer
from semantix.caos.caosql import codegen as caosql_codegen

from semantix.caos.backends.pgsql import common
from semantix.caos.backends.pgsql import delta as delta_cmds

from . import datasources
from .datasources import introspection

from .transformer import CaosTreeTransformer

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
            result = self.dbcursor.seek(offset, 'ABSOLUTE')
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

    def __iter__(self):
        if self.offset:
            self.seek(self.offset, 'set')

        while self.limit is None or self.cursor_pos <= self.limit:
            self.cursor_pos += 1
            yield next(self.dbcursor)


class Query:
    def __init__(self, text, argmap, result_types, argument_types, context=None,
                 scrolling_cursor=False, offset=None, limit=None):
        self.text = text
        self.argmap = argmap
        self.context = context
        self.result_types = result_types
        self.argument_types = argument_types

        self.scrolling_cursor = scrolling_cursor
        self.offset = offset
        self.limit = limit

    def describe_output(self):
        return collections.OrderedDict(self.result_types)

    def describe_arguments(self):
        return dict(self.argument_types)

    def prepare(self, session):
        return PreparedQuery(self, session)


class PreparedQuery:
    def __init__(self, query, session):
        self.statement = session.get_prepared_statement(query.text)
        self.query = query

    def describe_output(self):
        return self.query.describe_output()

    def describe_arguments(self):
        return self.query.describe_arguments()

    def __call__(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)

        if self.query.scrolling_cursor:
            if self.query.limit:
                limit = vars[self.query.limit.index]
                vars.pop()
            else:
                limit = None

            if self.query.offset:
                offset = vars[self.query.offset.index]
                vars.pop()
            else:
                offset = None

            return Cursor(self.statement.declare(*vars), offset, limit)
        else:
            return self.statement(*vars)

    def first(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)
        return self.statement.first(*vars)

    def rows(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)

        if self.scrolling_cursor:
            return Cursor(self.statement.declare(*vars), self.query.offset, self.query.limit)
        else:
            return self.statement.rows(vars)

    def convert_args(self, args, kwargs):
        result = list(args) or []
        for k in self.query.argmap:
            arg = kwargs[k]
            if isinstance(arg, caos.concept.Concept):
                arg = arg.id
            result.append(arg)

        return result

    __iter__ = rows


class CaosQLAdapter:
    cache = {}

    def __init__(self, session):
        self.session = session
        self.realm = session.realm
        self.connection = session.get_connection()
        self.transformer = CaosTreeTransformer()
        self.current_portal = None

    def transform(self, query, scrolling_cursor=False, context=None):
        if scrolling_cursor:
            offset = query.offset
            limit = query.limit
        else:
            offset = limit = None

        if scrolling_cursor:
            query.offset = None
            query.limit = None

        qtext, argmap = self.transformer.transform(query, self.realm)

        if scrolling_cursor:
            query.offset = offset
            query.limit = limit

        restypes = {}

        for k, v in query.result_types.items():
            if isinstance(v[0], caos.types.ProtoNode):
                restypes[k] = (self.session.schema.get(v[0].name), v[1])
            else:
                restypes[k] = v

        return Query(text=qtext, argmap=argmap, result_types=restypes,
                     argument_types=query.argument_types, scrolling_cursor=scrolling_cursor,
                     offset=offset, limit=limit)


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

        self.connection_pool = pool.ConnectionPool(connector)
        self.async_connection_pool = pool.ConnectionPool(async_connector)

        self.connection = connector(pool=self.connection_pool)
        self.connection.connect()
        delta_cmds.EnableHstoreFeature.init_hstore(self.connection)

        self.modules = self.read_modules()
        self.link_cache = {}
        self.concept_cache = {}
        self.table_cache = {}
        self.batch_instrument_cache = {}
        self.batches = {}
        self.domain_to_atom_map = {}
        self.column_cache = {}
        self.table_id_to_proto_name_cache = {}

        self._table_atom_constraints_cache = None
        self._table_ptr_constraints_cache = None

        self.parser = parser.PgSQLParser()
        self.search_idx_expr = astexpr.TextSearchExpr()
        self.atom_constr_exprs = {}
        self.constant_expr = None

        self.meta = proto.RealmMeta(load_builtins=False)

        repo = deltarepo(self.connection)
        super().__init__(repo)


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
        if not self.meta.index:
            if 'caos' in self.modules:
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
    def process_delta(self, delta, meta):
        """LOG [caos.delta.plan] PgSQL Delta Plan
            print(delta.dump())
        """
        delta = self.adapt_delta(delta)
        context = delta_cmds.CommandContext(self.connection)
        delta.apply(meta, context)
        return delta

    @debug
    def apply_synchronization_plan(self, plans):
        """LOG [caos.delta.plan] PgSQL Adapted Delta Plan
        for plan in plans:
            print(plan.dump())
        """
        for plan in plans:
            plan.execute(delta_cmds.CommandContext(self.connection))


    def apply_delta(self, delta):
        if isinstance(delta, base_delta.DeltaSet):
            deltas = list(delta)
        else:
            deltas = [delta]

        plans = []

        meta = self.getmeta()

        for d in deltas:
            plan = self.process_delta(d.deltas[0], meta)
            plans.append(plan)

        table = delta_cmds.DeltaLogTable()
        records = []
        for d in deltas:
            rec = table.record(
                    id='%x' % d.id,
                    parents=['%x' % d.parent_id] if d.parent_id else None,
                    checksum='%x' % d.checksum,
                    committer=os.getenv('LOGNAME', '<unknown>')
                  )
            records.append(rec)

        plans.append(delta_cmds.Insert(table, records=records))

        table = delta_cmds.DeltaRefTable()
        rec = table.record(
                id='%x' % d.id,
                ref='HEAD'
              )
        condition = [('ref', str('HEAD'))]
        plans.append(delta_cmds.Merge(table, record=rec, condition=condition))

        with self.connection.xact() as xact:
            self.apply_synchronization_plan(plans)
            self.invalidate_meta_cache()
            meta = self.getmeta()
            if meta.get_checksum() != d.checksum:
                xact.rollback()
                self.modules = self.read_modules()
                raise base_delta.DeltaChecksumError('could not apply delta correctly: '
                                                    'checksums do not match')


    def invalidate_meta_cache(self):
        self.meta = proto.RealmMeta(load_builtins=False)
        self.modules = self.read_modules()
        self.link_cache.clear()
        self.concept_cache.clear()
        self.table_cache.clear()
        self.batch_instrument_cache.clear()
        self.domain_to_atom_map.clear()
        self.column_cache.clear()
        self.table_id_to_proto_name_cache.clear()
        self._table_atom_constraints_cache = None
        self._table_ptr_constraints_cache = None


    def concept_name_from_id(self, id, session):
        concept = caos.Name('semantix.caos.builtins.BaseObject')
        query = '''SELECT c.name
                   FROM
                       %s AS e
                       INNER JOIN caos.concept AS c ON c.id = e.concept_id
                   WHERE e."semantix.caos.builtins.id" = $1
                ''' % (common.concept_name_to_table_name(concept))
        ps = session.connection.prepare(query)
        concept_name = ps.first(id)
        if concept_name:
            concept_name = caos.Name(ps.first(id))
        return concept_name


    def entity_from_row(self, session, concept_name, attribute_map, row):
        atom_link_map = {}

        concept_map = self.get_concept_map(session)

        concept_id = row[attribute_map['concept_id']]

        if concept_id is None:
            # empty record
            return None

        real_concept = concept_map[concept_id]

        if real_concept == concept_name:
            concept = session.realm.meta.get(concept_name)

            for link_name, link in concept.pointers.items():
                if link.atomic() and not isinstance(link.first, caos.types.ProtoComputable):
                    col_name = common.caos_name_to_pg_name(link_name)
                    atom_link_map[link_name] = attribute_map[col_name]

            links = {k: row[i] for k, i in atom_link_map.items()}
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

        ps = session.connection.prepare(query)
        result = ps.first(id)

        if result is not None:
            concept_proto = session.realm.meta.get(concept)
            ret = {}

            for link_name in concept_proto.pointers:

                if link_name != 'semantix.caos.builtins.id':
                    colname = common.caos_name_to_pg_name(link_name)

                    try:
                        ret[str(link_name)] = result[colname]
                    except KeyError:
                        pass

            return ret
        else:
            return None


    def load_link(self, source, target, link, session):
        proto_link = caos.types.prototype(link.__class__)
        table = common.link_name_to_table_name(proto_link.normal_name(), catenate=True)

        query = '''SELECT
                       l.*
                   FROM
                       %s AS l
                   WHERE
                       l.source_id = $1
                       AND l.target_id IS NOT DISTINCT FROM $2
                       AND l.link_type_id = $3''' % table

        ps = session.connection.prepare(query)
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

            for propname in proto_link.pointers:
                colname = common.caos_name_to_pg_name(propname)
                ret[str(propname)] = result[colname]

            return ret

        else:
            return {}


    def _get_update_refs(self, source_cls, pointers):
        cols = []

        realm = source_cls._metadata.realm

        for a in pointers:
            l = getattr(source_cls, str(a), None)
            if l:
                col_type = types.pg_type_from_atom(realm.meta, l._metadata.prototype)
                col_type = 'text::%s' % col_type

            else:
                col_type = 'int'
            column_name = common.caos_name_to_pg_name(a)
            column_name = common.quote_ident(column_name)
            cols.append('%s = %%(%s)s::%s' % (column_name, str(a), col_type))

        return cols


    def _get_insert_refs(self, source_cls, pointers, named=True):
        realm = source_cls._metadata.realm

        cols_names = [common.quote_ident(common.caos_name_to_pg_name(a))
                      for a in pointers]
        cols = []
        for a in pointers:
            if hasattr(source_cls, str(a)):
                l = getattr(source_cls, str(a))
                col_type = types.pg_type_from_atom(realm.meta, l._metadata.prototype)
                col_type = 'text::%s' % col_type

            else:
                col_type = 'int'
            if named:
                cols.append('%%(%s)s::%s' % (a, col_type))
            else:
                cols.append('%%s::%s' % col_type)
        return cols_names, cols


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
                constraint, pointer_name, source_table = self.constraint_from_pg_name(error_data)

                msg = 'unique link constraint violation'

                src_table = common.get_table_name(caos.types.prototype(source.__class__),
                                                  catenate=False)
                if source_table == src_table:
                    pointer = caos.concept.link(getattr(source.__class__, str(pointer_name)))
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

        connection = session.connection if session else self.connection
        concept_map = self.get_concept_map(session)
        context = delta_cmds.CommandContext(session.connection)

        idquery = delta_cmds.Query(text='caos.uuid_generate_v1mc()', params=(), type='uuid')
        now = delta_cmds.Query(text="'NOW'", params=(), type='timestamptz')

        with connection.xact():

            attrs = {}
            for link_name, link_cls in cls:
                if isinstance(link_cls, caos.types.AtomClass) and \
                                                    link_name != 'semantix.caos.builtins.id':
                    if not isinstance(link_cls._class_metadata.link, caos.types.ComputableClass):
                        attrs[common.caos_name_to_pg_name(link_name)] = links[link_name]

            rec = table.record(**attrs)

            returning = ['"semantix.caos.builtins.id"']
            if issubclass(cls, session.schema.semantix.caos.builtins.Object):
                returning.extend(('"semantix.caos.builtins.ctime"',
                                  '"semantix.caos.builtins.mtime"'))

            if id is not None:
                if issubclass(cls, session.schema.semantix.caos.builtins.Object):
                    setattr(rec, 'semantix.caos.builtins.mtime', now)

                cmd = delta_cmds.Update(table=table, record=rec,
                                        condition=[('semantix.caos.builtins.id', id)],
                                        returning=returning)
            else:
                setattr(rec, 'semantix.caos.builtins.id', idquery)

                if issubclass(cls, session.schema.semantix.caos.builtins.Object):
                    setattr(rec, 'semantix.caos.builtins.ctime', now)
                    setattr(rec, 'semantix.caos.builtins.mtime', now)

                rec.concept_id = concept_map[concept]

                cmd = delta_cmds.Insert(table=table, records=[rec], returning=returning)

            try:
                rows = cmd.execute(context)
            except postgresql.exceptions.Error as e:
                raise self._interpret_db_error(e, entity) from e

            id = list(rows)
            if not id:
                err = 'could not store "%s" entity' % concept
                raise caos.error.StorageError(err)
            id = id[0]

            """LOG [caos.sync]
            print('Merged entity %s[%s][%s]' % \
                    (concept, id[0], (data['name'] if 'name' in data else '')))
            """

            if issubclass(cls, session.schema.semantix.caos.builtins.Object):
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
            if prototype.atomic():
                condkeys = ('source_id', 'link_type_id')
            else:
                condkeys = keys

            key = common.link_name_to_table_name(caos.Name('semantix.caos.builtins.link'))

        key_condition = '(%s) = (%s)' % \
                            (','.join('t.%s' % common.quote_ident(k) for k in condkeys),
                             ','.join('batch.%s' % common.quote_ident(k) for k in condkeys))

        qry = text % {'table_name': common.qname(*table_name), 'batch_prefix': batch_prefix,
                      'cols': cols, 'key_condition': key_condition,
                      'func_name': func_name,
                      'keys': ','.join('t.%s' % common.quote_ident(k) for k in keys),
                      'vals': vals, 'key': key}

        self.execquery(qry, session.connection)

        return func_name


    def get_batch_instruments(self, prototype, session, batch_id):
        result = self.batch_instrument_cache.get(batch_id)
        if result:
            result = result.get(prototype)

        if not result:
            model_table = self.get_table(prototype, session)
            name = '%x_batch_%x' % (persistent_hash.persistent_hash(prototype.name.name), batch_id)
            table_name = (model_table.name[0], common.caos_name_to_pg_name(name))
            batch_table = delta_cmds.Table(table_name)

            cols = self.get_table_columns(model_table.name)
            colmap = {c.name: c for c in model_table.columns()}
            batch_table.add_columns(colmap[col] for col in cols)

            context = delta_cmds.CommandContext(session.connection)
            delta_cmds.CreateTable(batch_table).execute(context)

            merger_func = self.create_batch_merger(prototype, session)

            if isinstance(prototype, caos.types.ProtoConcept):
                keys = ('semantix.caos.builtins.id', 'concept_id')
            elif isinstance(prototype, caos.types.ProtoLink):
                keys = ('source_id', 'target_id', 'link_type_id')

            name = '%x_batch_%x_updated' % (persistent_hash.persistent_hash(prototype.name.name),
                                            batch_id)
            schema = common.caos_module_name_to_schema_name(prototype.name.module)
            updates_table_name = (schema, common.caos_name_to_pg_name(name))

            updates_table = delta_cmds.Table(updates_table_name)
            updates_table.add_columns(colmap[col] for col in keys)
            delta_cmds.CreateTable(updates_table).execute(context)

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

            self.execquery('DROP TABLE %s' % common.qname(*batch_table.name), session.connection)
            self.execquery('DROP TABLE %s' % common.qname(*updates_table.name), session.connection)

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
            if prototype.atomic():
                condkeys = ('source_id', 'link_type_id')
            else:
                condkeys = keys

        keys = ', '.join(common.quote_ident(k) for k in keys)
        condkeys = ', '.join(common.quote_ident(k) for k in condkeys)

        batch_index_name = common.caos_name_to_pg_name(batch_table.name[1] + '_key_idx')
        updates_index_name = common.caos_name_to_pg_name(updates_table.name[1] + '_key_idx')

        qry = 'CREATE UNIQUE INDEX %(batch_index_name)s ON %(batch_table)s (%(keys)s)' % \
               {'batch_table': common.qname(*batch_table.name), 'keys': keys,
                'batch_index_name': common.quote_ident(batch_index_name)}

        self.execquery(qry, session.connection)

        with session.connection.xact():
            self.execquery('LOCK TABLE %s IN ROW EXCLUSIVE MODE' % common.qname(*table),
                           session.connection)

            qry = '''INSERT INTO %(tab)s (%(keys)s) (SELECT * FROM %(proc_name)s('%(batch_id)x'))''' \
                  % {'tab': common.qname(*updates_table.name),
                     'proc_name': merger_func,
                     'keys': keys,
                     'batch_id': batch_id}
            self.execquery(qry, session.connection)

            qry = 'CREATE UNIQUE INDEX %(updates_index_name)s ON %(batch_table)s (%(keys)s)' % \
                   {'batch_table': common.qname(*updates_table.name), 'keys': keys,
                    'updates_index_name': common.quote_ident(updates_index_name)}
            self.execquery(qry, session.connection)

            qry = '''INSERT INTO %(table_name)s (%(cols)s)
                     (SELECT * FROM %(batch_table)s
                      WHERE (%(condkeys)s) NOT IN (SELECT %(condkeys)s FROM %(updated)s))
                  ''' % {'table_name': common.qname(*table),
                         'cols': cols,
                         'batch_table': common.qname(*batch_table.name),
                         'condkeys': condkeys,
                         'updated': common.qname(*updates_table.name)}
            self.execquery(qry, session.connection)

            self.execquery('TRUNCATE %s' % common.qname(*batch_table.name), session.connection)
            self.execquery('DROP INDEX %s' % common.qname(batch_table.name[0], batch_index_name),
                           session.connection)
            self.execquery('TRUNCATE %s' % common.qname(*updates_table.name), session.connection)
            self.execquery('DROP INDEX %s' % common.qname(updates_table.name[0], updates_index_name),
                           session.connection)

        self.execquery('ANALYZE %s' % common.qname(*table), session.connection)


    def store_entity_batch(self, entities, session, batch_id):

        concept_map = self.get_concept_map(session)
        idquery = delta_cmds.Query(text='caos.uuid_generate_v1mc()', params=(), type='uuid')
        now = delta_cmds.Query(text="'NOW'", params=(), type='timestamptz')
        context = delta_cmds.CommandContext(session.connection)

        key = lambda i: i.__class__._metadata.name
        for concept, entities in itertools.groupby(sorted(entities, key=key), key=key):
            concept = session.schema.get(concept)
            concept_proto = concept._metadata.prototype
            table, _, _ = self.get_batch_instruments(concept_proto, session, batch_id)

            self.batches.setdefault(batch_id, {}).setdefault('objects', set()).add(concept_proto)

            attrmap = {}

            for link_name, link_cls in concept:
                if isinstance(link_cls, caos.types.AtomClass) and \
                                                    link_name != 'semantix.caos.builtins.id':
                    attrmap[str(link_name)] = common.caos_name_to_pg_name(link_name)

            records = []

            concept_id = concept_map[concept_proto.name]

            for entity in entities:
                if not entity.id:
                    updates = {'id': uuid.uuid1()}
                    entity._instancedata.update(entity, updates, register_changes=False, allow_ro=True)

                id = entity.id
                rec = table.record()
                for link_name, col_name in attrmap.items():
                    setattr(rec, col_name, getattr(entity, link_name))
                rec.concept_id = concept_id

                setattr(rec, 'semantix.caos.builtins.id', id)
                setattr(rec, 'semantix.caos.builtins.ctime', now)
                setattr(rec, 'semantix.caos.builtins.mtime', now)

                records.append(rec)

            cmd = delta_cmds.Insert(table=table, records=records)
            cmd.execute(context)


    @debug
    def delete_entities(self, entities, session):
        key = lambda i: i.__class__._metadata.name
        result = set()
        for concept, entities in itertools.groupby(sorted(entities, key=key), key=key):
            table = common.concept_name_to_table_name(concept)
            query = '''DELETE FROM %s WHERE "semantix.caos.builtins.id" = any($1)
                       RETURNING "semantix.caos.builtins.id"''' % table

            result.update(self.runquery(query, ([e.id for e in entities],), session.connection,
                                                                            compat=False))
        return result


    def get_link_map(self, session):
        if not self.link_cache:
            cl_ds = datasources.meta.links.ConceptLinks(session.connection)

            for row in cl_ds.fetch():
                self.link_cache[row['name']] = row['id']

        return self.link_cache


    def get_concept_map(self, session):
        if not self.concept_cache:
            cl_ds = datasources.meta.concepts.ConceptList(session.connection)

            for row in cl_ds.fetch():
                self.concept_cache[row['name']] = row['id']
                self.concept_cache[row['id']] = caos.Name(row['name'])

        return self.concept_cache


    def source_name_from_relid(self, table_oid):
        self.getmeta()
        return self.table_id_to_proto_name_cache.get(table_oid)


    def get_table(self, prototype, session):
        table = self.table_cache.get(prototype)

        if not table:
            table_name = common.get_table_name(prototype, catenate=False)
            table = delta_cmds.Table(table_name)

            cols = []

            if isinstance(prototype, caos.types.ProtoLink):
                cols.extend([
                    delta_cmds.Column(name='source_id', type='uuid'),
                    delta_cmds.Column(name='target_id', type='uuid'),
                    delta_cmds.Column(name='link_type_id', type='int'),
                ])

                pointers = prototype.pointers

            elif isinstance(prototype, caos.types.ProtoConcept):
                cols.extend([
                    delta_cmds.Column(name='concept_id', type='int')
                ])

                pointers = {n: p.first for n, p in prototype.pointers.items()}
            else:
                assert False

            for pointer_name, pointer in pointers.items():
                if pointer.atomic() and not isinstance(pointer, caos.types.ProtoComputable):
                    col_type = types.pg_type_from_atom(session.realm.meta, pointer.target,
                                                       topbase=True)
                    col_name = common.caos_name_to_pg_name(pointer_name)
                    cols.append(delta_cmds.Column(name=col_name, type=col_type))
            table.add_columns(cols)

            self.table_cache[prototype] = table

        return table


    @debug
    def store_links(self, source, targets, link_name, session, merge=False):
        link_map = self.get_link_map(session)

        link = getattr(source.__class__, str(link_name))
        link_cls = caos.concept.link(link, True)

        table = self.get_table(link_cls._metadata.root_prototype, session)

        if isinstance(link, caos.types.NodeClass):
            link_names = [(link, link._class_metadata.full_link_name)]
        else:
            link_names = [(l.target, l._metadata.name) for l in link]

        cmds = []
        records = []

        context = delta_cmds.CommandContext(session.connection)

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
            for prop_name, prop_cls in link_cls:
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

                cmds.append(delta_cmds.Merge(table, rec, condition=condition))
            else:
                records.append(rec)

        if records:
            cmds.append(delta_cmds.Insert(table, records))

        if cmds:
            try:
                for cmd in cmds:
                    cmd.execute(context)
            except postgresql.exceptions.UniqueError as e:
                raise self._interpret_db_error(e, source, link_cls) from e


    def store_link_batch(self, links, session, batch_id):

        link_map = self.get_link_map(session)
        context = delta_cmds.CommandContext(session.connection)

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

            attrmap = {}

            for prop_name, prop_cls in link:
                attrmap[str(prop_name)] = common.caos_name_to_pg_name(prop_name)

            records = []

            for link_name, source, targets in pairs:
                link = getattr(source.__class__, str(link_name))

                if isinstance(link, caos.types.NodeClass):
                    link_names = [(link, link._class_metadata.full_link_name)]
                else:
                    link_names = [(l.target, l._metadata.name) for l in link]

                for target in targets:
                    for t, full_link_name in link_names:
                        if isinstance(target, t):
                            break
                    else:
                        assert False, "No link found"

                    link_id = link_map[full_link_name]

                    rec = table.record()
                    linkobj = caos.concept.getlink(source, link_name, target)
                    for prop_name, col_name in attrmap.items():
                        setattr(rec, col_name, getattr(linkobj, prop_name))

                    rec.link_type_id = link_id
                    rec.source_id = linkobj._instancedata.source.id

                    target = linkobj._instancedata.target
                    if isinstance(target, caos.atom.Atom):
                        rec.target_id = None
                    else:
                        rec.target_id = target.id

                    records.append(rec)

            cmd = delta_cmds.Insert(table=table, records=records)
            cmd.execute(context)


    @debug
    def delete_links(self, source, targets, link_name, session):
        table = common.link_name_to_table_name(link_name)

        if targets:
            target_ids = list(t.id for t in targets)

            assert len(list(filter(lambda i: i is not None, target_ids)))

            """LOG [caos.sync]
            print('Deleting link %s[%s][%s]---{%s}-->[[%s]]' % \
                  (source.__class__._metadata.name, source.id,
                   (source.name if hasattr(source, 'name') else ''), link_name,
                   ','.join(target_ids)
                  )
                 )
            """

            qry = '''DELETE FROM %s
                     WHERE
                         source_id = $1
                         AND target_id = any($2)
                  ''' % table
            params = (source.id, target_ids)
        else:
            qry = '''DELETE FROM %s
                     WHERE
                         source_id = $1
                  ''' % table
            params = (source.id,)

        result = self.runquery(qry, params,
                               connection=session.connection,
                               compat=False, return_stmt=True)
        result = result.first(*params)

        if targets:
            assert result == len(target_ids)


    def caosqladapter(self, session):
        return CaosQLAdapter(session)


    def read_modules(self):
        schemas = introspection.schemas.SchemasList(self.connection).fetch(schema_name='caos%')
        schemas = {s['name'] for s in schemas}

        context = delta_cmds.CommandContext(self.connection)
        cond = delta_cmds.TableExists(name=('caos', 'module'))
        module_index_exists = cond.execute(context)

        if 'caos' in schemas and module_index_exists:
            modules = datasources.meta.modules.ModuleList(self.connection).fetch()
            modules = {m['schema_name']: m['name'] for m in modules}

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

            return set(modules.values()) | {'caos'}

        return {}


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

            self.domain_to_atom_map[domain_name] = name

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
            item = adapter.resolve(item)(None, item)
            item.construct()
            result.append(item)
        return result


    def interpret_search_index(self, index_name, index_expression):
        m = self.search_idx_name_re.match(index_name)
        if not m:
            raise caos.MetaError('could not interpret index %s' % index_name)

        language = m.group('language')
        index_class = m.group('index_class')

        tree = self.parser.parse(index_expression)
        columns = self.search_idx_expr.match(tree)

        if columns is None:
            msg = 'could not interpret index "%s"' % index_name
            details = 'Could not match expression:\n%s' % ast.dump.pretty_dump(tree)
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
            details = 'Could not match expression:\n%s' % ast.dump.pretty_dump(expr_tree)
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
                hint += '\nExpression:\n%s' % ast.dump.pretty_dump(expr_tree)
                raise caos.MetaError(msg, details=details, hint=hint)

            pattern = adapter()
            self.atom_constr_exprs[constraint_class] = pattern

        constraint_data = pattern.match(expr_tree)

        if constraint_data is None:
            msg = 'could not interpret constraint "%s"' % name
            details = 'Pattern "%r" could not match expression:\n%s' \
                                            % (pattern.__class__, ast.dump.pretty_dump(expr_tree))
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

        derived_atom_name = proto.Atom.gen_atom_name(source, pointer_name)
        if col['column_type_schema'] == 'pg_catalog':
            col_type_schema = common.caos_module_name_to_schema_name('semantix.caos.builtins')
            col_type = col['column_type_formatted']
        else:
            col_type_schema = col['column_type_schema']
            col_type = col['column_type']

        constraints = constraints.get(pointer_name) if constraints else None

        target = self.atom_from_pg_type(col_type, col_type_schema,
                                        constraints, col['column_default'], meta,
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

            if not r['source_id']:
                link_table_name = common.link_name_to_table_name(name, catenate=False)
                t = link_tables.get(link_table_name)
                if t:
                    self.table_id_to_proto_name_cache[t['oid']] = name

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

            link = proto.Link(name=name, base=bases, source=source, target=target,
                                mapping=caos.types.LinkMapping(r['mapping']),
                                required=required,
                                title=title, description=description,
                                is_abstract=r['is_abstract'],
                                is_final=r['is_final'],
                                is_atom=r['is_atom'],
                                readonly=r['readonly'],
                                default=r['default'])

            if link_search:
                link.search = link_search

            for constraint in constraints:
                link.add_constraint(constraint)

            for constraint in abstract_constraints:
                link.add_abstract_constraint(constraint)

            if source:
                source.add_pointer(link)
                if isinstance(target, caos.types.ProtoConcept) \
                        and source.name.module != 'semantix.caos.builtins':
                    target.add_rlink(link)

            meta.add(link)

        for link in meta(type='link', include_automatic=True, include_builtin=True):
            link.acquire_parent_data(meta)


    def order_links(self, meta):
        indexes = self.read_indexes()

        reverse_transformer = transformer.PgSQLExprTransformer()
        reverse_caosql_transformer = caosql_transformer.CaosqlReverseTransformer()

        g = {}

        for link in meta(type='link', include_automatic=True, include_builtin=True):
            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.base:
                g[link.name]['merge'].extend(link.base)

        topological.normalize(g, merger=proto.Link.merge)

        for link in meta(type='link', include_automatic=True, include_builtin=True):
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

            prop = proto.LinkProperty(name=name, base=bases, source=source, target=target,
                                      required=required,
                                      title=title, description=description,
                                      readonly=r['readonly'],
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

        for prop in meta(type='link_property', include_automatic=True, include_builtin=True):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}
            if prop.base:
                g[prop.name]['merge'].extend(prop.base)

        topological.normalize(g, merger=proto.LinkProperty.merge)


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


    def get_table_columns(self, table_name):
        cols = self.column_cache.get(table_name)

        if not cols:
            cols = introspection.tables.TableColumns(self.connection)
            cols = cols.fetch(table_name=table_name[1], schema_name=table_name[0])
            cols = collections.OrderedDict((col['column_name'], col) for col in cols)
            self.column_cache[table_name] = cols

        return cols


    def read_concepts(self, meta):
        tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                       table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        concept_list = datasources.meta.concepts.ConceptList(self.connection).fetch()
        concept_list = collections.OrderedDict((caos.Name(row['name']), row) for row in concept_list)

        visited_tables = set()

        table_to_name_map = {common.concept_name_to_table_name(n, catenate=False): n \
                                                                        for n in concept_list}

        for name, row in concept_list.items():
            concept = {'name': name,
                       'title': self.hstore_to_word_combination(row['title']),
                       'description': row['description'],
                       'is_abstract': row['is_abstract'],
                       'is_final': row['is_final'],
                       'custombases': row['custombases']}


            table_name = common.concept_name_to_table_name(name, catenate=False)
            table = tables.get(table_name)

            if not table:
                msg = 'internal metadata incosistency'
                details = 'Record for concept "%s" exists but the table is missing' % name
                raise caos.MetaError(msg, details=details)

            self.table_id_to_proto_name_cache[table['oid']] = name

            visited_tables.add(table_name)

            bases = self.pg_table_inheritance_to_bases(table['name'], table['schema'],
                                                                      table_to_name_map)

            concept = proto.Concept(name=name, base=bases, title=concept['title'],
                                    description=concept['description'],
                                    is_abstract=concept['is_abstract'],
                                    is_final=concept['is_final'],
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
        for concept in meta(type='concept', include_automatic=True, include_builtin=True):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                g[concept.name]["merge"].extend(concept.base)

        topological.normalize(g, merger=proto.Concept.merge)

        for concept in meta(type='concept', include_automatic=True, include_builtin=True):
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
            result = self.pg_type_to_atom_name_and_constraints(d['basetype_full'])
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


    def pg_table_inheritance_to_bases(self, table_name, schema_name, table_to_name_map):
        inheritance = introspection.tables.TableInheritance(self.connection)
        inheritance = inheritance.fetch(table_name=table_name, schema_name=schema_name, max_depth=1)
        inheritance = [i[:2] for i in inheritance[1:]]

        bases = tuple()
        if len(inheritance) > 0:
            bases = tuple(table_to_name_map[table[:2]] for table in inheritance)

        return bases


    def pg_type_to_atom_name_and_constraints(self, type_expr):
        m = self.typlen_re.match(type_expr)
        if m:
            typmod = m.group('length').split(',')
            typname = m.group('type').strip()
        else:
            typmod = None
            typname = type_expr

        typeconv = types.base_type_name_map_r.get(typname)
        if typeconv:
            if isinstance(typeconv, caos.Name):
                name = typeconv
                constraints = ()
            else:
                name, constraints = typeconv(typname, typmod)
            return name, constraints
        return None


    def atom_from_pg_type(self, type_expr, atom_schema, atom_constraints, atom_default, meta, derived_name):

        domain_name = type_expr.split('.')[-1]
        atom_name = self.domain_to_atom_map.get((atom_schema, domain_name))

        if atom_name:
            atom = meta.get(atom_name, None)
        else:
            atom = None

        if not atom:
            atom = meta.get(derived_name, None)

        if not atom or atom_constraints:

            typeconv = self.pg_type_to_atom_name_and_constraints(type_expr)
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
