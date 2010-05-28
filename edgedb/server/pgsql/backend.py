##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import re
import collections

import postgresql.string
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix.utils import ast, helper
from semantix.utils.algos import topological
from semantix.utils.debug import debug
from semantix.utils.lang import yaml
from semantix.utils.nlang import morphology
from semantix.utils import datastructures

from semantix import caos

from semantix.caos import session
from semantix.caos import backends
from semantix.caos import proto
from semantix.caos import delta as base_delta

from semantix.caos.backends.pgsql import common
from semantix.caos.backends.pgsql import delta as delta_cmds

from . import datasources
from .datasources import introspection

from .transformer import CaosTreeTransformer

from . import astexpr
from . import parser


class Session(session.Session):
    def __init__(self, realm, connection, entity_cache):
        super().__init__(realm, entity_cache=entity_cache)
        self.connection = connection
        self.link_cache = {}
        self.xact = []

    def _new_transaction(self):
        xact = self.connection.xact()
        xact.begin()
        return xact

    def in_transaction(self):
        return super().in_transaction() and bool(self.xact)

    def begin(self):
        super().begin()
        self.xact.append(self._new_transaction())

    def commit(self):
        super().commit()
        xact = self.xact.pop()
        xact.commit()

    def rollback(self):
        super().rollback()
        if self.xact:
            xact = self.xact.pop()
            xact.rollback()

    def rollback_all(self):
        super().rollback_all()
        while self.xact:
            self.xact.pop().rollback()


class Query:
    def __init__(self, text, statement, argmap, context=None):
        self.text = text
        self.argmap = argmap
        self.context = context
        self.statement = statement

    def __call__(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)
        return self.statement(*vars)

    def first(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)
        return self.statement.first(*vars)

    def rows(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)
        return self.statement.rows(vars)

    def chunks(self, *args, **kwargs):
        vars = self.convert_args(args, kwargs)
        return self.statement.chunks(*vars)

    def convert_args(self, args, kwargs):
        result = args or []
        for k in self.argmap:
            result.append(kwargs[k])

        return result

    def describe_output(self):
        return dict(zip(self.statement.column_names, self.statement.column_types))

    __iter__ = rows


class CaosQLCursor:
    cache = {}

    def __init__(self, session):
        self.realm = session.realm
        self.connection = session.connection
        self.cursor = CompatCursor(self.connection)
        self.transformer = CaosTreeTransformer()
        self.current_portal = None

    @debug
    def prepare(self, query):
        result = self.cache.get(query)
        if not result:
            qtext, argmap = self.transformer.transform(query, self.realm)
            ps = self.connection.prepare(qtext)
            self.cache[query] = (qtext, ps, argmap)
        else:
            qtext, ps, argmap = result
            """LOG [cache.caos.query] Cache Hit
            print(qtext)
            """

        return Query(text=qtext, statement=ps, argmap=argmap)


class Backend(backends.MetaBackend, backends.DataBackend):

    typlen_re = re.compile(r"(?P<type>.*) \( (?P<length>\d+ (?:\s*,\s*(\d+))*) \)$",
                           re.X)

    constraint_type_re = re.compile(r"^(?P<type>[.\w-]+)(?:_\d+)?$", re.X)

    search_idx_name_re = re.compile(r"""
        .*_(?P<language>\w+)_(?P<index_class>\w+)_search_idx$
    """, re.X)

    mod_constraint_name_re = re.compile(r"""
        ^(?P<concept_name>[.\w]+):(?P<link_name>[.\w]+)::(?P<constraint_class>[.\w]+)::atom_mod$
    """, re.X)


    def __init__(self, deltarepo, connection):
        self.connection = connection
        delta_cmds.EnableHstoreFeature.init_hstore(connection)

        self.modules = self.read_modules()
        self.domain_to_atom_map = {}

        self.parser = parser.PgSQLParser()
        self.search_idx_expr = astexpr.TextSearchExpr()
        self.atom_mod_exprs = {}
        self.constant_expr = None

        self.meta = proto.RealmMeta(load_builtins=False)

        repo = deltarepo(connection)
        super().__init__(repo)


    def session(self, realm, entity_cache):
        return Session(realm, connection=self.connection.clone(), entity_cache=entity_cache)


    def getmeta(self):
        if not self.meta.index:
            if 'caos' in self.modules:
                self.read_atoms(self.meta)
                self.read_concepts(self.meta)
                self.read_links(self.meta)
                self.read_link_properties(self.meta)

                self.order_atoms(self.meta)
                self.order_link_properties(self.meta)
                self.order_links(self.meta)
                self.order_concepts(self.meta)

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
        self.domain_to_atom_map = {}


    def concept_name_from_id(self, id, session):
        concept = caos.Name('semantix.caos.builtins.BaseObject')
        query = '''SELECT c.name
                   FROM
                       %s AS e
                       INNER JOIN caos.concept AS c ON c.id = e.concept_id
                   WHERE e."semantix.caos.builtins.id" = $1
                ''' % (common.concept_name_to_table_name(concept))
        ps = session.connection.prepare(query)
        concept = caos.Name(ps.first(id))
        return concept


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
                col_type = delta_cmds.PrototypeMetaCommand.pg_type_from_atom(
                                                realm.meta, l._metadata.prototype)
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
                col_type = delta_cmds.PrototypeMetaCommand.pg_type_from_atom(
                                            realm.meta, l._metadata.prototype)
                col_type = 'text::%s' % col_type

            else:
                col_type = 'int'
            if named:
                cols.append('%%(%s)s::%s' % (a, col_type))
            else:
                cols.append('%%s::%s' % col_type)
        return cols_names, cols


    @debug
    def store_entity(self, entity, session=None):
        cls = entity.__class__
        concept = cls._metadata.name
        id = entity.id
        links = entity._instancedata.pointers
        realm = cls._metadata.realm

        connection = session.connection if session else self.connection

        with connection.xact():

            attrs = {}
            for n, v in links.items():
                if issubclass(getattr(cls, str(n)), caos.atom.Atom) and \
                                                            n != 'semantix.caos.builtins.id':
                    attrs[n] = v

            returning = ['"semantix.caos.builtins.id"']
            if issubclass(cls, cls._metadata.realm.schema.semantix.caos.builtins.Object):
                returning.extend(('"semantix.caos.builtins.ctime"',
                                  '"semantix.caos.builtins.mtime"'))

            if id is not None:
                query = 'UPDATE %s SET ' % common.concept_name_to_table_name(concept)

                if issubclass(cls, cls._metadata.realm.schema.semantix.caos.builtins.Object):
                    attrs['semantix.caos.builtins.mtime'] = 'NOW'

                cols = self._get_update_refs(cls, attrs)

                query += ','.join(cols)
                query += ' WHERE "semantix.caos.builtins.id" = %s '  \
                                                        % postgresql.string.quote_literal(str(id))
                query += 'RETURNING %s' % ','.join(returning)
            else:
                if issubclass(cls, cls._metadata.realm.schema.semantix.caos.builtins.Object):
                    attrs['semantix.caos.builtins.ctime'] = 'NOW'
                    attrs['semantix.caos.builtins.mtime'] = 'NOW'

                cols_names, cols_values = self._get_insert_refs(cls, attrs)
                if cols_names:
                    cols_names = ', ' + ', '.join(cols_names)
                    cols_values = ', ' + ', '.join(cols_values)
                else:
                    cols_names = ''
                    cols_values = ''

                query = 'INSERT INTO %s ("semantix.caos.builtins.id", concept_id%s)' \
                                        % (common.concept_name_to_table_name(concept), cols_names)

                query += '''VALUES(caos.uuid_generate_v1mc(),
                                   (SELECT id FROM caos.concept WHERE name = %(concept)s) %(cols)s)
                         ''' \
                            % {'concept': postgresql.string.quote_literal(str(concept)),
                               'cols': cols_values}

                query += 'RETURNING %s' % ','.join(returning)

            data = dict((str(k), str(attrs[k]) if attrs[k] is not None else None) for k in attrs)

            rows = self.runquery(query, data, connection=connection)
            id = list(rows)
            if not id:
                raise Exception('failed to store entity')
            id = id[0]

            """LOG [caos.sync]
            print('Merged entity %s[%s][%s]' % \
                    (concept, id[0], (data['name'] if 'name' in data else '')))
            """

            if issubclass(cls, cls._metadata.realm.schema.semantix.caos.builtins.Object):
                updates = {'id': id[0], 'ctime': id[1], 'mtime': id[2]}
            else:
                updates = {'id': id[0]}
            entity._instancedata.update(entity, updates, register_changes=False, allow_ro=True)
            session.add_entity(entity)

        return id


    @debug
    def delete_entity(self, entity, session):
        concept = entity.__class__._metadata.name
        table = common.concept_name_to_table_name(concept)
        query = '''DELETE FROM %s WHERE "semantix.caos.builtins.id" = $1
                   RETURNING "semantix.caos.builtins.id"''' % table

        """LOG [caos.sync]
        print('Removing entity %s[%s]' % (concept, entity.id))
        """

        result = self.runquery(query, [entity.id], session.connection, compat=False)
        return result


    def get_link_map(self, session):
        if not session.link_cache:
            cl_ds = datasources.meta.links.ConceptLinks(session.connection)

            for row in cl_ds.fetch():
                session.link_cache[row['name']] = row['id']

        return session.link_cache


    def get_table(self, prototype):
        table_name = common.get_table_name(prototype, catenate=False)
        table = delta_cmds.Table(table_name)

        cols = []

        if isinstance(prototype, caos.types.ProtoLink):
            cols.extend([
                delta_cmds.Column(name='source_id', type='uuid'),
                delta_cmds.Column(name='target_id', type='uuid'),
                delta_cmds.Column(name='link_type_id', type='int'),
            ])

        for pointer_name, pointer in prototype.pointers.items():
            if pointer.atomic():
                col_type = delta_cmds.PrototypeMetaCommand.pg_type_from_atom(self.meta,
                                                                             pointer.target)
                col_name = common.caos_name_to_pg_name(pointer_name)
                cols.append(delta_cmds.Column(name=col_name, type=col_type))
        table.add_columns(cols)

        return table


    @debug
    def store_links(self, source, targets, link_name, session, merge=False):
        link_map = self.get_link_map(session)

        link = getattr(source.__class__, str(link_name))
        link_cls = caos.concept.link(link, True)

        table = self.get_table(link_cls._metadata.root_prototype)

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
                err = '"%s" link cardinality violation' % link_name
                detail = 'SOURCE: %s(%s)\nTARGETS: %s' % \
                         (source.__class__._metadata.name, source.id,
                          list((t.__class__._metadata.name, t.id) for t in targets))
                ex = caos.error.StorageLinkMappingCardinalityViolation(err, details=detail)
                raise ex from e


    @debug
    def delete_links(self, source, targets, link_name, session):
        table = common.link_name_to_table_name(link_name)

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

        result = self.runquery(qry, (source.id, target_ids),
                               connection=session.connection,
                               compat=False, return_stmt=True)
        result = result.first(source.id, target_ids)

        assert result == len(target_ids)


    def caosqlcursor(self, session):
        return CaosQLCursor(session)


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
                         'base': row['base'],
                         'mods': row['mods'],
                         'default': row['default']
                         }

            if atom_data['default']:
                atom_data['default'] = self.unpack_default(row['default'])

            base = caos.Name(atom_data['base'])
            atom = proto.Atom(name=name, base=base, default=atom_data['default'],
                              title=atom_data['title'], description=atom_data['description'],
                              automatic=atom_data['automatic'],
                              is_abstract=atom_data['is_abstract'])

            # Copy mods from parent (row['mods'] does not contain any inherited mods)
            atom.acquire_parent_data(meta)

            if domain['constraints']:
                mods = atom.normalize_mods(meta, domain['constraints'])
                for mod in mods:
                    atom.add_mod(mod)

            if row['mods']:
                mods = []
                for cls, val in row['mods'].items():
                    mods.append(helper.get_object(cls)(next(iter(yaml.Language.load(val)))))

                mods = atom.normalize_mods(meta, mods)
                for mod in mods:
                    atom.add_mod(mod)

            meta.add(atom)


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
            raise caos.MetaError('could not interpret index %s' % index_name)

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


    def interpret_constraint(self, constraint_class, expr, name):

        try:
            expr_tree = self.parser.parse(expr)
        except parser.PgSQLParserError as e:
            msg = 'could not interpret constraint %s' % name
            details = 'Syntax error when parsing expression: %s' % e.args[0]
            raise caos.MetaError(msg, details=details) from e

        pattern = self.atom_mod_exprs.get(constraint_class)
        if not pattern:
            adapter = astexpr.AtomModAdapterMeta.get_adapter(constraint_class)

            if not adapter:
                msg = 'could not interpret constraint %s' % name
                details = 'No matching pattern defined for constraint class "%s"' % constraint_class
                hint = 'Implement matching pattern for "%s"' % constraint_class
                hint += '\nExpression:\n%s' % ast.dump.pretty_dump(expr_tree)
                raise caos.MetaError(msg, details=details, hint=hint)

            pattern = adapter()
            self.atom_mod_exprs[constraint_class] = pattern

        constraint_data = pattern.match(expr_tree)

        if constraint_data is None:
            msg = 'could not interpret constraint "%s"' % name
            details = 'Pattern "%r" could not match expression:\n%s' \
                                            % (pattern.__class__, ast.dump.pretty_dump(expr_tree))
            hint = 'Take a look at the matching pattern and adjust'
            raise caos.MetaError(msg, details=details, hint=hint)

        return constraint_data


    def interpret_table_constraint(self, name, expr):
        m = self.mod_constraint_name_re.match(name)
        if not m:
            raise caos.MetaError('could not interpret table constraint %s' % name)

        link_name = m.group('link_name')
        constraint_class = helper.get_object(m.group('constraint_class'))
        constraint_data = self.interpret_constraint(constraint_class, expr, name)

        return link_name, constraint_class(constraint_data)


    def interpret_table_constraints(self, constr):
        cs = zip(constr['constraint_names'], constr['constraint_expressions'],
                 constr['constraint_descriptions'])

        for name, expr, description in cs:
            yield self.interpret_table_constraint(description, expr)


    def read_table_constraints(self):
        constraints = {}
        constraints_ds = introspection.tables.TableConstraints(self.connection)

        for row in constraints_ds.fetch(schema_pattern='caos%', constraint_pattern='%::atom_mod'):
            concept_constr = constraints[tuple(row['table_name'])] = {}

            for link_name, mod in self.interpret_table_constraints(row):
                idx = datastructures.OrderedIndex(key=lambda i: i.get_canonical_class())
                link_mods = concept_constr.setdefault(link_name, idx)
                cls = mod.get_canonical_class()
                try:
                    existing_mod = link_mods[cls]
                    existing_mod.merge(mod)
                except KeyError:
                    link_mods.add(mod)

        return constraints


    def read_pointer_target_column(self, meta, source, pointer_name, columns_cache,
                                                                     constraints_cache):
        host_schema, host_table = common.get_table_name(source, catenate=False)
        cols = columns_cache.get((host_schema, host_table))
        constraints = constraints_cache.get((host_schema, host_table))

        if not cols:
            cols = introspection.tables.TableColumns(self.connection)
            cols = cols.fetch(table_name=host_table, schema_name=host_schema)
            cols = {col['column_name']: col for col in cols}
            columns_cache[(host_schema, host_table)] = cols

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

        mods = constraints.get(pointer_name) if constraints else None

        target = self.atom_from_pg_type(col_type, col_type_schema,
                                        mods, col['column_default'], meta,
                                        caos.Name(name=derived_atom_name,
                                                  module=source.name.module))

        return target


    def read_links(self, meta):

        link_tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                            table_pattern='%_link')
        link_tables = {(t['schema'], t['name']): t for t in link_tables}

        links_list = datasources.meta.links.ConceptLinks(self.connection).fetch()
        links_list = {caos.Name(r['name']): r for r in links_list}

        concept_columns = {}
        concept_constraints = self.read_table_constraints()

        concept_indexes = self.read_search_indexes()

        table_to_name_map = {common.link_name_to_table_name(name, catenate=False): name \
                                                                    for name in links_list}

        for name, r in links_list.items():
            bases = tuple()

            if not r['source_id'] and not r['is_atom']:
                link_table_name = common.link_name_to_table_name(name, catenate=False)
                t = link_tables.get(link_table_name)
                if not t:
                    raise caos.MetaError(('internal inconsistency: record for link %s exists but '
                                          'the table is missing') % name)

                bases = self.pg_table_inheritance_to_bases(t['name'], t['schema'],
                                                           table_to_name_map)

            else:
                if r['source_id']:
                    bases = (proto.Link.normalize_name(name),)
                else:
                    bases = (caos.Name('semantix.caos.builtins.link'),)

            title = self.hstore_to_word_combination(r['title'])
            description = r['description']
            source = meta.get(r['source']) if r['source'] else None
            link_search = None

            if r['default']:
                r['default'] = self.unpack_default(r['default'])

            if r['source_id'] and r['is_atom']:
                target = self.read_pointer_target_column(meta, source, bases[0], concept_columns,
                                                                       concept_constraints)

                concept_schema, concept_table = common.concept_name_to_table_name(source.name,
                                                                                  catenate=False)

                indexes = concept_indexes.get((concept_schema, concept_table))

                if indexes:
                    col_search_index = indexes.get(bases[0])
                    if col_search_index:
                        weight = col_search_index[('default', 'english')]
                        link_search = proto.LinkSearchConfiguration(weight=weight)
            else:
                target = meta.get(r['target']) if r['target'] else None

            link = proto.Link(name=name, base=bases, source=source, target=target,
                                mapping=caos.types.LinkMapping(r['mapping']),
                                required=r['required'],
                                title=title, description=description,
                                is_abstract=r['is_abstract'],
                                is_atom=r['is_atom'],
                                readonly=r['readonly'],
                                default=r['default'])

            if link_search:
                link.search = link_search

            if r['constraints']:
                for cls, val in r['constraints'].items():
                    constraint = helper.get_object(cls)(next(iter(yaml.Language.load(val))))
                    link.add_constraint(constraint)

            if source:
                source.add_link(link)
                if isinstance(target, caos.types.ProtoConcept) \
                        and source.name.module != 'semantix.caos.builtins':
                    target.add_rlink(link)

            meta.add(link)


    def order_links(self, meta):
        g = {}

        for link in meta(type='link', include_automatic=True, include_builtin=True):
            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.base:
                g[link.name]['merge'].extend(link.base)

        topological.normalize(g, merger=proto.Link.merge)

        for link in meta(type='link', include_automatic=True, include_builtin=True):
            link.materialize(meta)


    def read_link_properties(self, meta):
        link_props = datasources.meta.links.LinkProperties(self.connection).fetch()
        link_props = {caos.Name(r['name']): r for r in link_props}
        link_constraints = self.read_table_constraints()
        link_columns = {}

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

            if source:
                # The property is attached to a link, check out link table columns for
                # target information.
                target = self.read_pointer_target_column(meta, source, bases[0], link_columns,
                                                                       link_constraints)
            else:
                target = None

            prop = proto.LinkProperty(name=name, base=bases, source=source, target=target,
                                      required=r['required'],
                                      title=title, description=description,
                                      readonly=r['readonly'],
                                      default=default)

            if source:
                source.add_property(prop)

            meta.add(prop)


    def order_link_properties(self, meta):
        g = {}

        for prop in meta(type='link_property', include_automatic=True, include_builtin=True):
            g[prop.name] = {"item": prop, "merge": [], "deps": []}
            if prop.base:
                g[prop.name]['merge'].extend(prop.base)

        topological.normalize(g, merger=proto.LinkProperty.merge)


    def read_concepts(self, meta):
        tables = introspection.tables.TableList(self.connection).fetch(schema_name='caos%',
                                                                       table_pattern='%_data')
        tables = {(t['schema'], t['name']): t for t in tables}

        concept_list = datasources.meta.concepts.ConceptList(self.connection).fetch()
        concept_list = {caos.Name(row['name']): row for row in concept_list}

        visited_tables = set()

        table_to_name_map = {common.concept_name_to_table_name(n, catenate=False): n \
                                                                        for n in concept_list}

        for name, row in concept_list.items():
            concept = {'name': name,
                       'title': self.hstore_to_word_combination(row['title']),
                       'description': row['description'],
                       'is_abstract': row['is_abstract'],
                       'custombases': row['custombases']}


            table_name = common.concept_name_to_table_name(name, catenate=False)
            table = tables.get(table_name)

            visited_tables.add(table_name)

            bases = self.pg_table_inheritance_to_bases(table['name'], table['schema'],
                                                                      table_to_name_map)

            concept = proto.Concept(name=name, base=bases, title=concept['title'],
                                    description=concept['description'],
                                    is_abstract=concept['is_abstract'],
                                    custombases=tuple(concept['custombases']))

            columns = introspection.tables.TableColumns(self.connection)
            columns = columns.fetch(table_name=table['name'], schema_name=table['schema'])
            meta.add(concept)

        tabdiff = set(tables.keys()) - visited_tables
        if tabdiff:
            msg = 'internal metadata incosistency'
            details = 'Extraneous data tables exist: %s' \
                        % (', '.join('"%s.%s"' % t for t in tabdiff))
            raise caos.MetaError(msg, details=details)


    def order_concepts(self, meta):
        g = {}
        for concept in meta(type='concept', include_automatic=True, include_builtin=True):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                g[concept.name]["merge"].extend(concept.base)
        topological.normalize(g, merger=proto.Concept.merge)

        for concept in meta(type='concept', include_automatic=True, include_builtin=True):
            concept.materialize(meta)


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
        if d['constraint_names'] is not None:
            constraints = []

            for constr_name, constr_expr in zip(d['constraint_names'], d['constraints']):
                m = self.constraint_type_re.match(constr_name)
                if m:
                    constr_type = m.group('type')
                else:
                    raise caos.MetaError('could not parse domain constraint "%s": %s' %
                                         (constr_name, constr_expr))

                constr_type = helper.get_object(constr_type)
                constr_data = self.interpret_constraint(constr_type, constr_expr, constr_name)
                constraints.append(constr_type(constr_data))

            d['constraints'] = constraints

        if d['basetype'] is not None:
            result = self.pg_type_to_atom_name_and_mods(d['basetype_full'])
            if result:
                base, mods = result
                constraints.extend(mods)

        if d['default'] is not None:
            d['default'] = self.interpret_constant(d['default'])

        return d


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


    def pg_table_inheritance_to_bases(self, table_name, schema_name, table_to_name_map):
        inheritance = introspection.tables.TableInheritance(self.connection)
        inheritance = inheritance.fetch(table_name=table_name, schema_name=schema_name, max_depth=1)
        inheritance = [i[:2] for i in inheritance[1:]]

        bases = tuple()
        if len(inheritance) > 0:
            bases = tuple(table_to_name_map[table[:2]] for table in inheritance)

        return bases


    def pg_type_to_atom_name_and_mods(self, type_expr):
        m = self.typlen_re.match(type_expr)
        if m:
            typmod = m.group('length').split(',')
            typname = m.group('type').strip()
        else:
            typmod = None
            typname = type_expr

        typeconv = delta_cmds.base_type_name_map_r.get(typname)
        if typeconv:
            if isinstance(typeconv, caos.Name):
                name = typeconv
                mods = ()
            else:
                name, mods = typeconv(typname, typmod)
            return name, mods
        return None


    def atom_from_pg_type(self, type_expr, atom_schema, atom_mods, atom_default, meta, derived_name):

        domain_name = type_expr.split('.')[-1]
        atom_name = self.domain_to_atom_map.get((atom_schema, domain_name))

        if atom_name:
            atom = meta.get(atom_name, None)
        else:
            atom = None

        if not atom:
            atom = meta.get(derived_name, None)

        if not atom or atom_mods:

            typeconv = self.pg_type_to_atom_name_and_mods(type_expr)
            if typeconv:
                name, mods = typeconv
                atom = meta.get(name)

                mods = set(mods)
                if atom_mods:
                    mods.update(atom_mods)

                atom.acquire_parent_data(meta)
            else:
                mods = set(atom_mods) if atom_mods else {}

            if atom_mods:
                atom = proto.Atom(name=derived_name, base=atom.name, default=atom_default,
                                  automatic=True)

                mods = atom.normalize_mods(meta, mods)

                for mod in mods:
                    atom.add_mod(mod)

                atom.acquire_parent_data(meta)

                meta.add(atom)

        assert atom
        return atom


    def hstore_to_word_combination(self, hstore):
        if hstore:
            return morphology.WordCombination.from_dict(hstore)
        else:
            return None
