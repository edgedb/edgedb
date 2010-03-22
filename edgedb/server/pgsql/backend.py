##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re
import importlib

import postgresql.string
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix.utils import graph
from semantix.utils.debug import debug
from semantix.utils.nlang import morphology

from semantix import caos

from semantix.caos import backends
from semantix.caos import proto

from semantix.caos.backends.pgsql import common
from semantix.caos.backends.pgsql import sync

from . import datasources
from .datasources import introspection

from .transformer import CaosTreeTransformer


class Query(object):
    def __init__(self, text, statement=None, vars=None, context=None):
        self.text = text
        self.vars = vars
        self.context = context
        self.statement = statement

    def __call__(self, *vars):
        return self.statement(*vars)

    def rows(self, *vars):
        return self.statement.rows(*vars)

    def chunks(self, *vars):
        return self.statement.chunks(*vars)

    __iter__ = rows


class CaosQLCursor(object):
    cache = {}

    def __init__(self, connection):
        self.connection = connection
        self.cursor = CompatCursor(connection)
        self.transformer = CaosTreeTransformer()
        self.current_portal = None

    @debug
    def prepare(self, query):
        result = self.cache.get(query)
        if not result:
            qtext = self.transformer.transform(query)
            ps = self.connection.prepare(qtext)
            self.cache[query] = (qtext, ps)
        else:
            qtext, ps = result
            """LOG [cache.caos.query] Cache Hit
            print(qtext)
            """

        return Query(text=qtext, statement=ps)

    def execute(self, query, vars=None):
        native_query = self.prepare_query(query)
        return native_query.rows(*vars)


class Backend(backends.MetaBackend, backends.DataBackend):

    typlen_re = re.compile(r"(?P<type>.*) \( (?P<length>\d+) (?:\s*,\s*(?P<precision>\d+))? \)$", re.X)

    check_constraint_re = re.compile(r"CHECK \s* \( (?P<expr>.*) \)$", re.X)

    constraint_type_re = re.compile(r"^(?P<type>[.\w-]+)(?:_\d+)?$", re.X)

    cast_re = re.compile(r"(::(?P<type>(?:(?P<quote>\"?)[\w -]+(?P=quote)\.)?(?P<quote1>\"?)[\w -]+(?P=quote1)))+$", re.X)

    constr_expr_res = {
                        'regexp': re.compile("VALUE::text \s* ~ \s* '(?P<expr>[^']*)'::text", re.X),
                        'max-length': re.compile("length\(VALUE::text\) \s* <= \s* (?P<expr>\d+)$", re.X),
                        'min-length': re.compile("length\(VALUE::text\) \s* >= \s* (?P<expr>\d+)$", re.X)
                      }


    def __init__(self, connection):
        super().__init__()

        self.connection = connection

        sync.SynchronizationPlan.init_hstore(connection)

        self.domains = set()
        schemas = introspection.SchemasList(self.connection).fetch(schema_name='caos%')
        self.modules = {common.schema_name_to_caos_module_name(s['name']) for s in schemas}

        self.column_map = {}


    def getmeta(self):
        meta = proto.RealmMeta(load_builtins=False)

        if 'caos' in self.modules:
            self.read_atoms(meta)
            self.read_concepts(meta)
            self.read_links(meta)

        return meta


    def get_delta(self, meta):
        delta = meta.delta(self.getmeta())
        return delta


    def get_synchronization_plan(self, delta):
        return sync.SynchronizationPlan.from_delta(delta)


    def apply_synchronization_plan(self, plan):
        with self.connection.xact():
            plan.execute(self.connection)


    def synchronize(self, meta):
        delta = self.get_delta(meta)
        plan = self.get_synchronization_plan(delta)
        if plan.is_material():
            plan.add(sync.SynchronizationPlan.logsync(meta.get_checksum()), -1)
        self.apply_synchronization_plan(plan)


    def get_meta_log(self, limit=None):
        table = sync.MetaLogTable()
        condition = sync.TableExists(table.name)
        record = table.record

        have_metalog = condition.execute(self.connection)

        result = []

        if have_metalog:
            query = 'SELECT * FROM %s ORDER BY mtime DESC' % common.qname(*table.name)
            if limit:
                query += ' LIMIT %d' % limit
            ps = self.connection.prepare(query)

            for row in ps:
                rec = record(**row)
                rec.checksum = int(rec.checksum, base=16)
                result.append(rec)

        return result


    def is_synchronized(self, meta):
        result = self.get_meta_log(1)
        if result:
            return result[0].checksum == meta.get_checksum(), result[0]
        else:
            return False, None


    def load_entity(self, concept, id):
        query = 'SELECT * FROM %s WHERE "semantix.caos.builtins.id" = \'%s\'' % (common.concept_name_to_table_name(concept), id)
        ps = self.connection.prepare(query)
        result = ps.first()

        if result is not None:
            return dict((self.column_map[k], result[k]) for k in result.keys() if k not in ('semantix.caos.builtins.id', 'concept_id'))
        else:
            return None


    @debug
    def store_entity(self, entity):
        concept = entity.__class__._metadata.name
        id = entity.id
        links = entity._instancedata.links

        with self.connection.xact():

            attrs = {}
            for n, v in links.items():
                if issubclass(getattr(entity.__class__, str(n)), caos.atom.Atom) and n != 'semantix.caos.builtins.id':
                    attrs[n] = v

            if id is not None:
                query = 'UPDATE %s SET ' % common.concept_name_to_table_name(concept)
                cols = []
                for a in attrs:
                    if hasattr(entity.__class__, str(a)):
                        l = getattr(entity.__class__, str(a))
                        col_type = 'text::%s' % \
                                    sync.TableBasedObject.pg_type_from_atom(None, l._metadata.prototype)
                    else:
                        col_type = 'int'
                    column_name = self.caos_name_to_pg_column_name(a)
                    column_name = common.quote_ident(column_name)
                    cols.append('%s = %%(%s)s::%s' % (column_name, str(a), col_type))
                query += ','.join(cols)
                query += ' WHERE "semantix.caos.builtins.id" = %s RETURNING "semantix.caos.builtins.id"' \
                                                                % postgresql.string.quote_literal(str(id))
            else:
                if attrs:
                    cols_names = [common.quote_ident(self.caos_name_to_pg_column_name(a)) for a in attrs]
                    cols_names = ', ' + ', '.join(cols_names)
                    cols = []
                    for a in attrs:
                        if hasattr(entity.__class__, str(a)):
                            l = getattr(entity.__class__, str(a))
                            col_type = 'text::%s' % \
                                        sync.TableBasedObject.pg_type_from_atom(None, l._metadata.prototype)
                        else:
                            col_type = 'int'
                        cols.append('%%(%s)s::%s' % (a, col_type))
                    cols_values = ', ' + ', '.join(cols)
                else:
                    cols_names = ''
                    cols_values = ''

                query = 'INSERT INTO %s ("semantix.caos.builtins.id", concept_id%s)' \
                                                % (common.concept_name_to_table_name(concept), cols_names)

                query += '''VALUES(uuid_generate_v1mc(),
                                   (SELECT id FROM caos.concept WHERE name = %(concept)s) %(cols)s)
                            RETURNING "semantix.caos.builtins.id"''' \
                                                % {'concept': postgresql.string.quote_literal(str(concept)),
                                                   'cols': cols_values}

            data = dict((str(k), str(attrs[k]) if attrs[k] is not None else None) for k in attrs)

            rows = self.runquery(query, data)
            id = next(rows)
            if id is None:
                raise Exception('failed to store entity')

            """LOG [caos.sync]
            print('Merged entity %s[%s][%s]' % \
                    (concept, id[0], (data['name'] if 'name' in data else '')))
            """

            id = id[0]
            entity.id = id
            entity._instancedata.dirty = False

            for name, link in links.items():
                if isinstance(link, caos.concept.LinkedSet) and link._instancedata.dirty:
                    self.store_links(entity, link, name)
                    link._instancedata.dirty = False

        return id


    def caosqlcursor(self):
        return CaosQLCursor(self.connection)


    def read_atoms(self, meta):
        domains = introspection.domains.DomainsList(self.connection).fetch(schema_name='caos%',
                                                                           domain_name='%_domain')
        domains = {caos.Name(name=common.domain_name_to_atom_name(d['name']),
                             module=common.schema_name_to_caos_module_name(d['schema'])):
                   self.normalize_domain_descr(d) for d in domains}
        self.domains = set(domains.keys())

        atom_list = datasources.AtomList(self.connection).fetch()

        atoms = {}
        for row in atom_list:
            name = caos.Name(row['name'])
            atoms[name] = {'name': name,
                           'title': self.hstore_to_word_combination(row['title']),
                           'description': self.hstore_to_word_combination(row['description']),
                           'automatic': row['automatic'],
                           'abstract': row['abstract'],
                           'base': row['base']}

            domain_descr = domains[name]

            bases = caos.Name(atoms[name]['base'])
            atom = proto.Atom(name=name, base=bases, default=domain_descr['default'], title=atoms[name]['title'],
                              description=atoms[name]['description'], automatic=atoms[name]['automatic'],
                              is_abstract=atoms[name]['abstract'])

            if domain_descr['constraints'] is not None:
                for constraint_type in domain_descr['constraints']:
                    for constraint_expr in domain_descr['constraints'][constraint_type]:
                        atom.add_mod(constraint_type(constraint_expr))

            meta.add(atom)


    def read_links(self, meta):

        link_tables = introspection.table.TableList(self.connection).fetch(schema_name='caos%',
                                                                           table_pattern='%_link')

        ltables = {}

        for t in link_tables:
            name = common.table_name_to_link_name(t['name'])
            module = common.schema_name_to_caos_module_name(t['schema'])
            name = caos.Name(name=name, module=module)

            ltables[name] = t

        link_tables = ltables

        links_list = datasources.meta.concept.ConceptLinks(self.connection).fetch()

        g = {}

        concept_columns = {}

        for r in links_list:
            name = caos.Name(r['name'])
            bases = tuple()
            properties = {}

            if not r['implicit'] and not r['atomic']:
                t = link_tables.get(name)
                if not t:
                    raise caos.MetaError('internal inconsistency: record for link %s exists but the table is missing'
                                         % name)

                bases = self.pg_table_inheritance_to_bases(t['name'], t['schema'])

                columns = introspection.table.TableColumns(self.connection).fetch(table_name=t['name'],
                                                                                  schema_name=t['schema'])
                for row in columns:
                    if row['column_name'] in ('source_id', 'target_id', 'link_type_id'):
                        continue

                    property_name = caos.Name(name=row['column_name'], module=t['schema'])
                    derived_atom_name = '__' + name.name + '__' + property_name.name
                    atom = self.atom_from_pg_type(row['column_type'], t['schema'],
                                                  row['column_default'], meta,
                                                  caos.Name(name=derived_atom_name, module=name.module))

                    property = proto.LinkProperty(name=property_name, atom=atom)
                    properties[property_name] = property
            else:
                if r['implicit']:
                    bases = (meta.get(r['name'].rpartition('_')[0]).name,)
                else:
                    bases = (caos.Name('semantix.caos.builtins.link'),)

            title = self.hstore_to_word_combination(r['title'])
            description = self.hstore_to_word_combination(r['description'])
            source = meta.get(r['source']) if r['source'] else None
            target = meta.get(r['target']) if r['target'] else None

            if r['implicit'] and isinstance(target, proto.Atom):
                cols = concept_columns.get(source.name)

                concept_schema, concept_table = common.concept_name_to_table_name(source.name,
                                                                                  catenate=False)
                if not cols:
                    cols = introspection.table.TableColumns(self.connection).fetch(table_name=concept_table,
                                                                                   schema_name=concept_schema)
                    cols = {col['column_name']: col for col in cols}
                    concept_columns[source.name] = cols

                base_link_name = bases[0]

                col = cols[base_link_name]

                derived_atom_name = '__' + source.name.name + '__' + base_link_name.name
                target = self.atom_from_pg_type(col['column_type'], concept_schema,
                                                col['column_default'], meta,
                                                caos.Name(name=derived_atom_name, module=source.name.module))


            link = proto.Link(name=name, base=bases, source=source, target=target,
                                mapping=r['mapping'], required=r['required'],
                                title=title, description=description,
                                is_abstract=r['abstract'],
                                is_atom=r['atomic'])
            link.implicit_derivative = r['implicit']
            link.properties = properties

            if source:
                source.add_link(link)
                if isinstance(target, caos.types.ProtoConcept) \
                        and source.name.module != 'semantix.caos.builtins':
                    target.add_rlink(link)

            g[link.name] = {"item": link, "merge": [], "deps": []}
            if link.base:
                g[link.name]['merge'].extend(link.base)

            meta.add(link)

        graph.normalize(g, merger=proto.Link.merge)

        g = {}
        for concept in meta(type='concept', include_automatic=True, include_builtin=True):
            g[concept.name] = {"item": concept, "merge": [], "deps": []}
            if concept.base:
                g[concept.name]["merge"].extend(concept.base)
        graph.normalize(g, merger=proto.Concept.merge)


    def read_concepts(self, meta):
        tables = introspection.table.TableList(self.connection).fetch(schema_name='caos%',
                                                                      table_pattern='%_data')
        concept_list = datasources.ConceptList(self.connection).fetch()

        concepts = {}
        for row in concept_list:
            name = caos.Name(row['name'])
            concepts[name] = {'name': name,
                              'title': self.hstore_to_word_combination(row['title']),
                              'description': self.hstore_to_word_combination(row['description']),
                              'abstract': row['abstract'], 'custombases': row['custombases']}

        for t in tables:
            name = common.table_name_to_concept_name(t['name'])
            module = common.schema_name_to_caos_module_name(t['schema'])
            name = caos.Name(name=name, module=module)

            bases = self.pg_table_inheritance_to_bases(t['name'], t['schema'])

            concept = proto.Concept(name=name, base=bases, title=concepts[name]['title'],
                                    description=concepts[name]['description'],
                                    is_abstract=concepts[name]['abstract'],
                                    custombases=concepts[name]['custombases'])

            columns = introspection.table.TableColumns(self.connection).fetch(table_name=t['name'],
                                                                              schema_name=t['schema'])
            for row in columns:
                if row['column_name'] in ('semantix.caos.builtins.id', 'concept_id'):
                    continue

                atom_name = row['column_name']

                """
                derived_atom_name = '__' + name.name + '__' + caos.Name(atom_name).name
                atom = self.atom_from_pg_type(row['column_type'], t['schema'], row['column_default'], meta,
                                              caos.Name(name=derived_atom_name, module=name.module))
                """

            meta.add(concept)


    @debug
    def store_links(self, source, targets, link_name):
        rows = []

        params = []
        for target in targets:
            target.sync()

            """LOG [caos.sync]
            print('Merging link %s[%s][%s]---{%s}-->%s[%s][%s]' % \
                  (source.concept, source.id, (source.name if hasattr(source, 'name') else ''),
                   link_name,
                   target.concept, target.id, (target.name if hasattr(target, 'name') else ''))
                  )
            """

            if isinstance(targets._metadata.link, caos.types.Node):
                full_link_name = targets._metadata.link._class_metadata.full_link_name
            else:
                for link, link_target_class in targets._metadata.link.links():
                    if isinstance(target, link_target_class):
                        full_link_name = link._metadata.name
                        break

            lt = datasources.ConceptLink(self.connection).fetch(name=str(full_link_name))

            rows.append('(%s::uuid, %s::uuid, %s::int)')
            params += [source.id, target.id, lt[0]['id']]

        params += params

        if len(rows) > 0:
            table = common.link_name_to_table_name(link_name)
            self.runquery("""INSERT INTO %s(source_id, target_id, link_type_id)
                             ((VALUES %s)
                              EXCEPT
                              (SELECT source_id, target_id, link_type_id
                              FROM %s
                              WHERE (source_id, target_id, link_type_id) in (VALUES %s)))""" %
                             (table, ",".join(rows), table, ",".join(rows)), params)


    def load_links(self, this_concept, this_id, other_concepts=None, link_names=None, reverse=False):

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

        links = datasources.EntityLinks(self.connection).fetch(
                                        source_id=source_id, target_id=target_id,
                                        target_concepts=target_concepts, source_concepts=source_concepts,
                                        link_names=link_names)

        return links

    def mod_class_to_str(self, constr_class):
        if issubclass(constr_class, proto.AtomModExpr):
            return 'expr'
        elif issubclass(constr_class, proto.AtomModRegExp):
            return 'regexp'
        elif issubclass(constr_class, proto.AtomModMinLength):
            return 'min-length'
        elif issubclass(constr_class, proto.AtomModMaxLength):
            return 'max-length'

    def normalize_domain_descr(self, d):
        if d['constraint_names'] is not None:
            constraints = {}

            for constr_name, constr_expr in zip(d['constraint_names'], d['constraints']):
                m = self.constraint_type_re.match(constr_name)
                if m:
                    constr_type = m.group('type')
                else:
                    raise caos.MetaError('could not parse domain constraint "%s": %s' %
                                         (constr_name, constr_expr))

                if constr_expr.startswith('CHECK'):
                    # Strip `CHECK()`
                    constr_expr = self.check_constraint_re.match(constr_expr).group('expr').replace("''", "'")
                else:
                    raise caos.MetaError('could not parse domain constraint "%s": %s' %
                                         (constr_name, constr_expr))

                constr_mod, dot, constr_class = constr_type.rpartition('.')

                constr_mod = importlib.import_module(constr_mod)
                constr_type = getattr(constr_mod, constr_class)

                constr_type_str = self.mod_class_to_str(constr_type)

                if constr_type_str in self.constr_expr_res:
                    m = self.constr_expr_res[constr_type_str].findall(constr_expr)
                    if m:
                        constr_expr = m
                    else:
                        raise caos.MetaError('could not parse domain constraint "%s": %s' %
                                             (constr_name, constr_expr))

                if issubclass(constr_type, (proto.AtomModMinLength, proto.AtomModMaxLength)):
                    constr_expr = [int(constr_expr[0])]

                if issubclass(constr_type, proto.AtomModExpr):
                    # That's a very hacky way to remove casts from expressions added by Postgres
                    constr_expr = constr_expr.replace('::text', '')

                if constr_type not in constraints:
                    constraints[constr_type] = []

                constraints[constr_type].extend(constr_expr)

            d['constraints'] = constraints

        if d['basetype'] is not None:
            m = self.typlen_re.match(d['basetype_full'])
            if m:
                if proto.AtomModMaxLength not in d['constraints']:
                    d['constraints'][proto.AtomModMaxLength] = []

                d['constraints'][proto.AtomModMaxLength].append(int(m.group('length')))

        if d['default'] is not None:
            # Strip casts from default expression
            d['default'] = self.cast_re.sub('', d['default']).strip("'")

        return d


    def runquery(self, query, params=None):
        cursor = CompatCursor(self.connection)
        query, pxf, nparams = cursor._convert_query(query)
        ps = self.connection.prepare(query)
        if params:
            return ps.rows(*pxf(params))
        else:
            return ps.rows()


    def pg_table_inheritance_to_bases(self, table_name, schema_name):
        inheritance = introspection.table.TableInheritance(self.connection).fetch(table_name=table_name,
                                                                                  schema_name=schema_name)
        inheritance = [i[:2] for i in inheritance[1:]]

        bases = tuple()
        if len(inheritance) > 0:
            for table in inheritance:
                base_name = common.table_name_to_concept_name(table[0])
                base_module = common.schema_name_to_caos_module_name(table[1])
                bases += (caos.Name(name=base_name, module=base_module),)

        return bases


    def caos_name_to_pg_column_name(self, name):
        """
        Convert Caos name to a valid PostgresSQL column name

        PostgreSQL has a limit of 63 characters for column names.

        @param name: Caos name to convert
        @return: PostgreSQL column name
        """
        mapped_name = self.column_map.get(name)
        if mapped_name:
            return mapped_name
        name = str(name)

        mapped_name = common.caos_name_to_pg_colname(name)

        self.column_map[mapped_name] = name
        self.column_map[name] = mapped_name
        return mapped_name


    def atom_from_pg_type(self, type_expr, atom_schema, atom_default, meta, derived_name):

        atom_name = caos.Name(name=common.domain_name_to_atom_name(type_expr),
                              module=common.schema_name_to_caos_module_name(atom_schema))

        atom = meta.get(atom_name, None)

        if not atom:
            atom = meta.get(derived_name, None)

        if not atom:
            m = self.typlen_re.match(type_expr)
            if m:
                typmod = int(m.group('length'))
                typname = m.group('type').strip()
            else:
                typmod = None
                typname = type_expr

            if typname in sync.base_type_name_map_r:
                atom = meta.get(sync.base_type_name_map_r[typname])

                if typname in sync.typmod_types and typmod is not None:
                    atom = proto.Atom(name=derived_name, base=atom.name, default=atom_default,
                                        automatic=True)
                    atom.add_mod(proto.AtomModMaxLength(typmod))
                    meta.add(atom)

        return atom


    def hstore_to_word_combination(self, hstore):
        if hstore:
            return morphology.WordCombination.from_dict(hstore)
        else:
            return None
