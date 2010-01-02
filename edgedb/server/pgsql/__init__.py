import re
import types
import copy
import importlib

import postgresql.string
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix.caos import MetaError
from semantix.caos.name import Name as CaosName

from semantix.caos.backends.meta import MetaBackend
from semantix.caos.backends.data import DataBackend
from semantix.caos.backends.meta import RealmMeta, Atom, Concept, Link
from semantix.caos.backends import meta as metamod

from semantix.caos.backends.pgsql.common import PathCacheTable, EntityTable
from semantix.caos.backends.pgsql.common import ConceptTable, ConceptMapTable, EntityMapTable

from .datasources.introspection import SchemasList
from .datasources.introspection.domains import DomainsList
from .datasources.introspection.table import TableColumns, TableInheritance, TableList
from .datasources.meta.concept import ConceptLinks

from .datasources import EntityLinks, ConceptLink
from .adapters.caosql import CaosQLQueryAdapter

import semantix.caos.query
from semantix.caos.concept import BaseConceptCollection
from semantix.utils.debug import debug


class CaosQLCursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.cursor = CompatCursor(connection)
        self.adapter = CaosQLQueryAdapter()
        self.current_portal = None

    def prepare_query(self, query, vars):
        return self.adapter.adapt(query, vars)

    def execute_prepared(self, query):
        if query.vars is None:
            query.vars = []

        sql, pxf, nparams = self.cursor._convert_query(query.text)
        ps = self.connection.prepare(sql)
        if query.vars:
            self.current_portal = ps.rows(*pxf(query.vars))
        else:
            self.current_portal = ps.rows()

    def execute(self, query, vars=None):
        native_query = self.prepare_query(query, vars)
        return self.execute_prepared(native_query)

    def fetchall(self):
        return list(self.current_portal)

    def fetchone(self):
        return next(self.current_portal)


class Backend(MetaBackend, DataBackend):

    typlen_re = re.compile(r"(?P<type>.*) \( (?P<length>\d+) (?:\s*,\s*(?P<precision>\d+))? \)$", re.X)

    check_constraint_re = re.compile(r"CHECK \s* \( (?P<expr>.*) \)$", re.X)

    constraint_type_re = re.compile(r"^(?P<type>[.\w-]+)_\d+$", re.X)

    cast_re = re.compile(r"(::(?P<type>(?:(?P<quote>\"?)[\w-]+(?P=quote)\.)?(?P<quote1>\"?)[\w-]+(?P=quote1)))+$", re.X)

    constr_expr_res = {
                        'regexp': re.compile("VALUE::text \s* ~ \s* '(?P<expr>.*)'::text$", re.X),
                        'max-length': re.compile("length\(VALUE::text\) \s* <= \s* (?P<expr>\d+)$", re.X),
                        'min-length': re.compile("length\(VALUE::text\) \s* >= \s* (?P<expr>\d+)$", re.X)
                      }

    base_type_map = {
                        'integer': int,
                        'character': str,
                        'character varying': str,
                        'boolean': bool,
                        'numeric': int,
                        'double precision': float
                    }

    base_type_name_map = {
                                CaosName('builtin:str'): 'character varying',
                                CaosName('builtin:int'): 'numeric',
                                CaosName('builtin:bool'): 'boolean',
                                CaosName('builtin:float'): 'double precision'
                         }

    base_type_name_map_r = {
                                'character varying': CaosName('builtin:str'),
                                'character': CaosName('builtin:str'),
                                'text': CaosName('builtin:str'),
                                'integer': CaosName('builtin:int'),
                                'boolean': CaosName('builtin:bool'),
                                'numeric': CaosName('builtin:int'),
                                'double precision': CaosName('builtin:float')
                           }


    typmod_types = ('character', 'character varying', 'numeric')
    fixed_length_types = {'character varying': 'character'}


    def __init__(self, connection):
        super().__init__()

        self.connection = connection

        self.domains = set()
        schemas = SchemasList(self.connection).fetch(schema_name='caos%')
        self.modules = {self.pg_schema_name_to_module_name(s['name']) for s in schemas}

        if 'caos' not in self.modules:
            self.create_primary_schema()

        self.concept_table = ConceptTable(self.connection)
        self.concept_table.create()
        self.concept_map_table = ConceptMapTable(self.connection)
        self.concept_map_table.create()

        self.entity_table = EntityTable(self.connection)
        self.entity_table.create()
        self.entity_map_table = EntityMapTable(self.connection)
        self.entity_map_table.create()

        self.path_cache_table = PathCacheTable(self.connection)
        self.path_cache_table.create()


    def getmeta(self):
        meta = RealmMeta()

        self.read_atoms(meta)
        self.read_concepts(meta)

        return meta


    def synchronize(self, meta):
        with self.connection.xact():
            for obj in meta:
                self.store(obj, 1)

            for obj in meta:
                self.store(obj, 2)


    def get_concept_from_entity(self, id):
        query = """SELECT
                            c.name
                        FROM
                            caos.entity e
                            INNER JOIN caos.concept c ON c.id = e.concept_id
                        WHERE
                            e.id = %d""" % id

        ps = self.connection.prepare(query)
        return ps.first()


    def load_entity(self, concept, id):
        query = 'SELECT * FROM %s WHERE id = %d' % (self.concept_name_to_pg_table_name(concept), id)
        ps = self.connection.prepare(query)
        result = ps.first()

        if result is not None:
            return dict((k, result[k]) for k in result.keys() if k not in ('id', 'concept_id'))
        else:
            return None


    @debug
    def store_entity(self, entity):
        concept = entity.concept
        id = entity.id
        links = entity._links
        clinks = entity.__class__.links

        with self.connection.xact():

            attrs = {n: v for n, v in links.items() if not isinstance(v, BaseConceptCollection)}

            if id is not None:
                query = 'UPDATE %s SET ' % self.concept_name_to_pg_table_name(concept)
                cols = []
                for a, v in attrs.items():
                    if a in clinks:
                        col_type = 'text::%s' % self.pg_type_from_atom_class(clinks[a].targets[0])
                    else:
                        col_type = 'int'
                    cols.append('%s = %%(%s)s::%s' % (a, a, col_type))
                query += ','.join(cols)
                query += ' WHERE id = %d RETURNING id' % id
            else:
                if attrs:
                    cols_names = ', ' + ', '.join(['"%s"' % a for a in attrs])
                    cols = []
                    for a, v in attrs.items():
                        if a in clinks:
                            col_type = 'text::%s' % self.pg_type_from_atom_class(clinks[a].targets[0])
                        else:
                            col_type = 'int'
                        cols.append('%%(%s)s::%s' % (a, col_type))
                    cols_values = ', ' + ', '.join(cols)
                else:
                    cols_names = ''
                    cols_values = ''

                query = 'INSERT INTO %s (id, concept_id%s)' % (self.concept_name_to_pg_table_name(concept),
                                                                cols_names)

                query += '''VALUES(nextval('caos.entity_id_seq'::regclass),
                                   (SELECT id FROM caos.concept WHERE name = %(concept)s) %(cols)s)
                            RETURNING id''' % {'concept': postgresql.string.quote_literal(str(concept)),
                                               'cols': cols_values}

            data = dict((k, str(attrs[k]) if attrs[k] is not None else None) for k in attrs)

            rows = self.runquery(query, data)
            id = next(rows)
            if id is None:
                raise Exception('failed to store entity')

            """LOG [caos.sync]
            print('Merged entity %s[%s][%s]' % \
                    (concept, id[0], (data['name'] if 'name' in data else '')))
            """

            id = id[0]
            entity.setid(id)
            entity.markclean()

            for name, link in links.items():
                if isinstance(link, BaseConceptCollection) and link.dirty:
                    self.store_links(concept, name, entity, link)
                    link.markclean()

        return id


    def caosqlcursor(self):
        return CaosQLCursor(self.connection)


    def store_path_cache_entry(self, entity, parent_entity_id, weight):
        self.path_cache_table.insert(entity_id=entity.id,
                                 parent_entity_id=parent_entity_id,
                                 name_attribute=str(entity.attrs['name']) if 'name' in entity.attrs else None,
                                 concept_name=entity.name,
                                 weight=weight)


    def clear_path_cache(self):
        self.path_cache_table.create()
        with self.connection as cursor:
            cursor.execute('DELETE FROM caos.path_cache')


    def read_atoms(self, meta):
        domains = DomainsList(self.connection).fetch(schema_name='caos%')
        domains = {CaosName(name=self.pg_domain_name_to_atom_name(d['name']),
                            module=self.pg_schema_name_to_module_name(d['schema'])):
                   self.normalize_domain_descr(d) for d in domains}
        self.domains = set(domains.keys())

        for name, domain_descr in domains.items():

            if domain_descr['basetype'] in self.base_type_name_map_r:
                bases = self.base_type_name_map_r[domain_descr['basetype']]
            else:
                bases = CaosName(name=self.pg_domain_name_to_atom_name(domain_descr['basetype']),
                                 module=self.pg_schema_name_to_module_name(domain_descr['basetype_schema']))

            atom = Atom(name=name, base=bases, default=domain_descr['default'])

            if domain_descr['constraints'] is not None:
                for constraint_type in domain_descr['constraints']:
                    for constraint_expr in domain_descr['constraints'][constraint_type]:
                        atom.add_mod(constraint_type(constraint_expr))

            meta.add(atom)


    def read_concepts(self, meta):
        tables = TableList(self.connection).fetch(schema_name='caos%')

        for t in tables:
            name = self.pg_table_name_to_concept_name(t['name'])
            module = self.pg_schema_name_to_module_name(t['schema'])

            inheritance = TableInheritance(self.connection).fetch(table_name=t['name'], schema_name=t['schema'])
            inheritance = [i[:2] for i in inheritance[1:]]

            bases = tuple()
            if len(inheritance) > 0:
                for table in inheritance:
                    base_name = self.pg_table_name_to_concept_name(table[0])
                    base_module = self.pg_schema_name_to_module_name(table[1])
                    bases += (CaosName(name=base_name, module=base_module),)

            concept = Concept(name=CaosName(name=name, module=module), base=bases)

            columns = TableColumns(self.connection).fetch(table_name=t['name'], schema_name=t['schema'])
            for row in columns:
                if row['column_name'] in ('id', 'concept_id'):
                    continue

                atom = self.atom_from_pg_type(row['column_type'], '__' + name + '__' + row['column_name'],
                                              t['schema'], row['column_default'], meta)

                meta.add(atom)
                atom = Link(name=row['column_name'], source=concept, targets={atom},
                            required=row['column_required'], mapping='11')
                concept.add_link(atom)

            meta.add(concept)

        for t in tables:
            name = self.pg_table_name_to_concept_name(t['name'])
            module = self.pg_schema_name_to_module_name(t['schema'])
            name = CaosName(name=name, module=module)
            concept = meta.get(name)

            for r in ConceptLinks(self.connection).fetch(source_concept=str(name)):
                link = Link(name=r['name'], source=meta.get(r['source_concept']),
                            targets={meta.get(r['target_concept'])}, mapping=r['mapping'],
                            required=r['required'])
                concept.add_link(link)


    def store(self, obj, phase=1, allow_existing=False):
        with self.connection.xact():
            if obj.name.module not in self.modules:
                self.create_module(obj.name.module)

            is_atom = isinstance(obj, metamod.Atom)

            if is_atom:
                if phase == 1:
                    self.create_atom(obj, allow_existing)
            else:
                self.create_concept(obj, phase, allow_existing)


    def create_primary_schema(self):
        qry = 'CREATE SCHEMA %s' % postgresql.string.quote_ident('caos')
        self.connection.execute(qry)
        self.modules.add('caos')


    def create_module(self, module_name):
        qry = 'CREATE SCHEMA %s' % self.module_name_to_pg_schema_name(module_name)
        self.connection.execute(qry)
        self.modules.add(module_name)


    def create_atom(self, obj, allow_existing):
        if allow_existing:
            if not self.domains:
                domains = DomainsList(self.connection).fetch(schema_name='caos')
                self.domains = {d['name'] for d in domains}

            if obj.name in self.domains:
                return

        qry = 'CREATE DOMAIN %s AS ' % self.atom_name_to_pg_domain_name(obj.name)
        base = obj.base

        if base in self.base_type_name_map:
            base = self.base_type_name_map[base]
        else:
            base = self.atom_name_to_pg_domain_name(base)

        basetype = copy.copy(base)

        has_max_length = False

        if base in self.typmod_types:
            for mod, modvalue in obj.mods.items():
                if issubclass(mod, metamod.AtomModMaxLength):
                    has_max_length = modvalue
                    break

        if has_max_length:
            #
            # Convert basetype + max-length constraint into a postgres-native
            # type with typmod, e.g str[max-length: 20] --> varchar(20)
            #
            # Handle the case when min-length == max-length and yield a fixed-size
            # type correctly
            #
            has_min_length = False
            if base in self.fixed_length_types:
                for mod, modvalue in obj.mods.items():
                    if issubclass(mod, metamod.AtomModMinLength):
                        has_min_length = modvalue
                        break

            if (has_min_length and has_min_length.value == has_max_length.value):
                base = self.fixed_length_types[base]
            base += '(' + str(has_max_length.value) + ')'

        qry += base

        if obj.default is not None:
            qry += ' DEFAULT %s ' % postgresql.string.quote_literal(str(obj.default))

        for constr_type, constr in obj.mods.items():
            classtr = '%s.%s' % (constr_type.__module__, constr_type.__name__)
            if issubclass(constr_type, metamod.AtomModRegExp):
                for re in constr.regexps:
                    expr = 'VALUE ~ %s' % postgresql.string.quote_literal(re)
                    qry += self.get_constraint_expr(classtr, expr)
            elif issubclass(constr_type, metamod.AtomModExpr):
                # XXX: TODO: Generic expression support requires sophisticated expression translation
                continue
                for expr in constr.exprs:
                    qry += self.get_constraint_expr(classtr, expr)
            elif issubclass(constr_type, metamod.AtomModMaxLength):
                if basetype not in self.typmod_types:
                    qry += self.get_constraint_expr(classtr, 'length(VALUE::text) <= ' + str(constr.value))
            elif issubclass(constr_type, metamod.AtomModMinLength):
                qry += self.get_constraint_expr(classtr, 'length(VALUE::text) >= ' + str(constr.value))

        self.connection.execute(qry)

        self.domains.add(obj.name)


    def create_concept(self, obj, phase, allow_exsiting):
        if phase is None or phase == 1:
            self.concept_table.insert(name=str(obj.name))

            qry = 'CREATE TABLE %s' % self.concept_name_to_pg_table_name(obj.name)

            columns = []

            for link_name in sorted(obj.links.keys()):
                link = obj.links[link_name]
                if link.atomic():
                    target_atom = list(link.targets)[0]
                    column_type = self.pg_type_from_atom(target_atom)
                    column = '"%s" %s %s' % (link_name, column_type, 'NOT NULL' if link.required else '')
                    columns.append(column)

            qry += '(' +  ','.join(columns) + ')'

            if obj.base:
                qry += ' INHERITS (' + ','.join([self.concept_name_to_pg_table_name(p) for p in obj.base]) + ')'
            else:
                qry += ' INHERITS (%s) ' % 'caos.entity'

            self.connection.execute(qry)

        if phase is None or phase == 2:
            for link in obj.links.values():
                if not link.atomic():
                    for target in link.targets:
                        self.concept_map_table.insert(source=str(link.source.name), target=str(target.name),
                                                      name=link.name, mapping=link.mapping,
                                                      required=link.required)


    @debug
    def store_links(self, concept, link_name, source, targets):
        rows = []

        params = []
        for i, target in enumerate(targets):
            target.sync()

            """LOG [caos.sync]
            print('Merging link %s[%s][%s]---{%s}-->%s[%s][%s]' % \
                  (source.concept, source.id, (source.name if hasattr(source, 'name') else ''),
                   link_name,
                   target.concept, target.id, (target.name if hasattr(target, 'name') else ''))
                  )
            """

            # XXX: that's ugly
            targets = [str(c.concept) for c in target.__class__.__mro__ if hasattr(c, 'concept')]

            lt = ConceptLink(self.connection).fetch(
                                   source_concepts=[str(source.concept)], target_concepts=targets,
                                   name=link_name)

            rows.append('(%s::int, %s::int, %s::int, %s::int)')
            params += [source.id, target.id, lt[0]['id'], 0]

        params += params

        if len(rows) > 0:
            self.runquery("""INSERT INTO caos.entity_map(source_id, target_id, link_type_id, weight)
                                ((VALUES %s) EXCEPT (SELECT
                                                            *
                                                        FROM
                                                            caos.entity_map
                                                        WHERE
                                                            (source_id, target_id, link_type_id, weight) in (%s)))
                           """ % (",".join(rows), ",".join(rows)), params)


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

        links = EntityLinks(self.connection).fetch(
                                  source_id=source_id, target_id=target_id,
                                  target_concepts=target_concepts, source_concepts=source_concepts,
                                  link_names=link_names)

        return links

    def mod_class_to_str(self, constr_class):
        if issubclass(constr_class, metamod.AtomModExpr):
            return 'expr'
        elif issubclass(constr_class, metamod.AtomModRegExp):
            return 'regexp'
        elif issubclass(constr_class, metamod.AtomModMinLength):
            return 'min-length'
        elif issubclass(constr_class, metamod.AtomModMaxLength):
            return 'max-length'

    def normalize_domain_descr(self, d):
        if d['constraint_names'] is not None:
            constraints = {}

            for constr_name, constr_expr in zip(d['constraint_names'], d['constraints']):
                m = self.constraint_type_re.match(constr_name)
                if m:
                    constr_type = m.group('type')
                else:
                    raise MetaError('could not parse domain constraint "%s": %s' % (constr_name, constr_expr))

                if constr_expr.startswith('CHECK'):
                    # Strip `CHECK()`
                    constr_expr = self.check_constraint_re.match(constr_expr).group('expr').replace("''", "'")
                else:
                    raise MetaError('could not parse domain constraint "%s": %s' % (constr_name, constr_expr))

                constr_mod, dot, constr_class = constr_type.rpartition('.')

                constr_mod = importlib.import_module(constr_mod)
                constr_type = getattr(constr_mod, constr_class)

                constr_type_str = self.mod_class_to_str(constr_type)

                if constr_type_str in self.constr_expr_res:
                    m = self.constr_expr_res[constr_type_str].match(constr_expr)
                    if m:
                        constr_expr = m.group('expr')
                    else:
                        raise MetaError('could not parse domain constraint "%s": %s' % (constr_name, constr_expr))

                if issubclass(constr_type, (metamod.AtomModMinLength, metamod.AtomModMaxLength)):
                    constr_expr = int(constr_expr)

                if issubclass(constr_type, metamod.AtomModExpr):
                    # That's a very hacky way to remove casts from expressions added by Postgres
                    constr_expr = constr_expr.replace('::text', '')

                if constr_type not in constraints:
                    constraints[constr_type] = []

                constraints[constr_type].append(constr_expr)

            d['constraints'] = constraints

        if d['basetype'] is not None:
            m = self.typlen_re.match(d['basetype_full'])
            if m:
                if metamod.AtomModMaxLength not in d['constraints']:
                    d['constraints'][metamod.AtomModMaxLength] = []

                d['constraints'][metamod.AtomModMaxLength].append(int(m.group('length')))

        if d['default'] is not None:
            # Strip casts from default expression
            d['default'] = self.cast_re.sub('', d['default'])

        return d


    def runquery(self, query, params=None):
        cursor = CompatCursor(self.connection)
        query, pxf, nparams = cursor._convert_query(query)
        ps = self.connection.prepare(query)
        if params:
            return ps.rows(*pxf(params))
        else:
            return ps.rows()


    def module_name_to_pg_schema_name(self, module_name):
        return postgresql.string.quote_ident('caos_' + module_name)


    def pg_schema_name_to_module_name(self, schema_name):
        if schema_name.startswith('caos_'):
            return schema_name[5:]
        else:
            return schema_name


    def concept_name_to_pg_table_name(self, name):
        return postgresql.string.qname('caos_' + name.module, name.name + '_data')


    def pg_table_name_to_concept_name(self, name):
        if name.endswith('_data'):
            name = name[:-5]
        return name


    def atom_name_to_pg_domain_name(self, name):
        return postgresql.string.qname('caos_' + name.module, name.name + '_domain')


    def pg_domain_name_to_atom_name(self, name):
        name = name.split('.')[-1]
        if name.endswith('_domain'):
            name = name[:-7]
        return name


    def pg_type_from_atom_class(self, atom_obj):
        if (atom_obj.base is not None and (atom_obj.base == str or (hasattr(atom_obj.base, 'name') and atom_obj.base.name == 'builtin:str'))
                and len(atom_obj.mods) == 1 and issubclass(next(iter(atom_obj.mods.keys())), metamod.AtomModMaxLength)):
            column_type = 'varchar(%d)' % next(iter(atom_obj.mods.values())).value
        else:
            if atom_obj.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_obj.name]
            else:
                column_type = self.atom_name_to_pg_domain_name(atom_obj.name)
        return column_type


    def pg_type_from_atom(self, atom_obj):
        if (atom_obj.base is not None and atom_obj.base == 'builtin:str'
                and len(atom_obj.mods) == 1 and issubclass(next(iter(atom_obj.mods.keys())), metamod.AtomModMaxLength)):
            column_type = 'varchar(%d)' % next(iter(atom_obj.mods.values())).value
        else:
            if atom_obj.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_obj.name]
            else:
                self.store(atom_obj, allow_existing=True)
                column_type = self.atom_name_to_pg_domain_name(atom_obj.name)

        return column_type


    def atom_from_pg_type(self, type_expr, atom_name, atom_schema, atom_default, meta):

        atom_name = CaosName(name=self.pg_domain_name_to_atom_name(type_expr),
                             module=self.pg_schema_name_to_module_name(atom_schema))

        atom = meta.get(atom_name, None)

        if not atom:
            m = self.typlen_re.match(type_expr)
            if m:
                typmod = int(m.group('length'))
                typname = m.group('type').strip()
            else:
                typmod = None
                typname = type_expr

            if typname in self.base_type_name_map_r:
                atom = meta.get(self.base_type_name_map_r[typname])

                if typname in self.typmod_types and typmod is not None:
                    atom = Atom(name=atom_name, base=atom.name, default=atom_default, automatic=True)
                    atom.add_mod(metamod.AtomModMaxLength(typmod))
                    meta.add(atom)

        return atom


    def get_constraint_expr(self, type, expr):
        constr_name = '%s_%s' % (type, id(expr))
        return ' CONSTRAINT %s CHECK ( %s )' % (postgresql.string.quote_ident(constr_name), expr)
