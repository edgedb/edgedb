import re
import types
import copy
import importlib

import postgresql.string
from postgresql.driver.dbapi20 import Cursor as CompatCursor

from semantix.caos import MetaError

from semantix.caos.backends.meta import MetaBackend
from semantix.caos.backends.data import DataBackend
from semantix.caos.backends.meta import RealmMeta, Atom, Concept, ConceptLink as ConceptLinkType
from semantix.caos.backends import meta as metamod

from semantix.caos.backends.pgsql.common import PathCacheTable, EntityTable
from semantix.caos.backends.pgsql.common import ConceptTable, ConceptMapTable, EntityMapTable

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
                                'str': 'character varying',
                                'int': 'numeric',
                                'bool': 'boolean',
                                'float': 'double precision'
                         }

    base_type_name_map_r = {
                                'character varying': 'str',
                                'character': 'str',
                                'text': 'str',
                                'integer': 'int',
                                'boolean': 'bool',
                                'numeric': 'int',
                                'double precision': 'float'
                           }


    typmod_types = ('character', 'character varying', 'numeric')
    fixed_length_types = {'character varying': 'character'}


    def __init__(self, connection):
        super().__init__()

        self.connection = connection

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

        self.domains = set()


    def getmeta(self):
        meta = RealmMeta()

        self.read_atoms(meta)
        self.read_concepts(meta)

        return meta


    def synchronize(self, meta):
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
        query = 'SELECT * FROM %s WHERE entity_id = %d' % (self.mangle_concept_name(concept), id)
        ps = self.connection.prepare(query)
        result = ps.first()

        if result is not None:
            return dict((k, result[k]) for k in result.keys() if k != 'entity_id')
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
            attrs['entity_id'] = id

            if id is not None:
                query = 'UPDATE %s SET ' % self.mangle_concept_name(concept)
                query += ','.join(['%s = %%(%s)s::%s' % (a, a, self.pg_type_from_atom_class(clinks[a].targets[0]) if a in clinks else 'int') for a, v in attrs.items()])
                query += ' WHERE entity_id = %d RETURNING entity_id' % id
            else:
                id = self.entity_table.insert({'concept': concept})[0][0]

                query = 'INSERT INTO %s' % self.mangle_concept_name(concept)
                query += '(' + ','.join(['"%s"' % a for a in attrs]) + ')'
                query += 'VALUES(' + ','.join(['%%(%s)s::%s' % (a, ('text::' + self.pg_type_from_atom_class(clinks[a].targets[0])) if a in clinks else 'int') for a, v in attrs.items()]) + ') RETURNING entity_id'

            data = dict((k, str(attrs[k]) if attrs[k] is not None else None) for k in attrs)
            data['entity_id'] = id

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

            for link_type, link in links.items():
                if isinstance(link, BaseConceptCollection) and link.dirty:
                    self.store_links(concept, link_type, entity, link)
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
        domains = DomainsList(self.connection).fetch(schema_name='caos')
        domains = {self.demangle_domain_name(d['name']): self.normalize_domain_descr(d) for d in domains}
        self.domains = set(domains.keys())

        for name, domain_descr in domains.items():

            if domain_descr['basetype'] in self.base_type_name_map_r:
                bases = self.base_type_name_map_r[domain_descr['basetype']]
            else:
                bases = self.demangle_domain_name(domain_descr['basetype'])

            atom = Atom(name=name, base=bases, default=domain_descr['default'])

            if domain_descr['constraints'] is not None:
                for constraint_type in domain_descr['constraints']:
                    for constraint_expr in domain_descr['constraints'][constraint_type]:
                        atom.add_mod(constraint_type(constraint_expr))

            meta.add(atom)


    def read_concepts(self, meta):
        tables = TableList(self.connection).fetch(schema_name='caos')

        for t in tables:
            name = self.demangle_concept_name(t['name'])

            inheritance = TableInheritance(self.connection).fetch(table_name=t['name'])
            inheritance = [i[0] for i in inheritance[1:]]

            bases = tuple()
            if len(inheritance) > 0:
                for table in inheritance:
                    bases += (self.demangle_concept_name(table),)

            concept = Concept(name=name, base=bases)

            columns = TableColumns(self.connection).fetch(table_name=t['name'])
            for row in columns:
                if row['column_name'] == 'entity_id':
                    continue

                atom = self.atom_from_pg_type(row['column_type'], '__' + name + '__' + row['column_name'],
                                              row['column_default'], meta)

                meta.add(atom)
                atom = ConceptLinkType(source=concept, targets={atom}, link_type=row['column_name'],
                                       required=row['column_required'], mapping='11')
                concept.add_link(atom)

            meta.add(concept)

        for t in tables:
            name = self.demangle_concept_name(t['name'])
            concept = meta.get(name)

            for r in ConceptLinks(self.connection).fetch(source_concept=name):
                link = ConceptLinkType(meta.get(r['source_concept']), {meta.get(r['target_concept'])},
                                       r['link_type'], r['mapping'], r['required'])
                concept.add_link(link)


    def store(self, obj, phase=1, allow_existing=False):
        is_atom = isinstance(obj, metamod.Atom)

        if is_atom:
            if phase == 1:
                self.create_atom(obj, allow_existing)
        else:
            self.create_concept(obj, phase, allow_existing)


    def create_atom(self, obj, allow_existing):
        if allow_existing:
            if not self.domains:
                domains = DomainsList(self.connection).fetch(schema_name='caos')
                self.domains = {d['name'] for d in domains}

            if obj.name in self.domains:
                return

        qry = 'CREATE DOMAIN %s AS ' % self.mangle_domain_name(obj.name)
        base = obj.base

        if base in self.base_type_name_map:
            base = self.base_type_name_map[base]
        else:
            base = self.mangle_domain_name(base)

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
            self.concept_table.insert(name=obj.name)

            qry = 'CREATE TABLE %s' % self.mangle_concept_name(obj.name)

            columns = ['entity_id integer NOT NULL REFERENCES caos.entity(id) ON DELETE CASCADE']

            for link_name in sorted(obj.links.keys()):
                link = obj.links[link_name]
                if link.atomic():
                    target_atom = list(link.targets)[0]
                    column_type = self.pg_type_from_atom(target_atom)
                    column = '"%s" %s %s' % (link_name, column_type, 'NOT NULL' if link.required else '')
                    columns.append(column)

            qry += '(' +  ','.join(columns) + ')'

            if len(obj.base) > 0:
                qry += ' INHERITS (' + ','.join([self.mangle_concept_name(p) for p in obj.base]) + ')'

            self.connection.execute(qry)

        if phase is None or phase == 2:
            for link in obj.links.values():
                if not link.atomic():
                    for target in link.targets:
                        self.concept_map_table.insert(source=link.source.name, target=target.name,
                                                      link_type=link.link_type, mapping=link.mapping,
                                                      required=link.required)


    @debug
    def store_links(self, concept, link_type, source, targets):
        rows = []

        params = []
        for i, target in enumerate(targets):
            target.sync()

            """LOG [caos.sync]
            print('Merging link %s[%s][%s]---{%s}-->%s[%s][%s]' % \
                  (source.concept, source.id, (source.name if hasattr(source, 'name') else ''),
                   link_type,
                   target.concept, target.id, (target.name if hasattr(target, 'name') else ''))
                  )
            """

            # XXX: that's ugly
            targets = [c.concept for c in target.__class__.__mro__ if hasattr(c, 'concept')]

            lt = ConceptLink(self.connection).fetch(
                                   source_concepts=[source.concept], target_concepts=targets,
                                   link_type=link_type)

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


    def load_links(self, this_concept, this_id, other_concepts=None, link_types=None, reverse=False):

        if link_types is not None and not isinstance(link_types, list):
            link_types = [link_types]

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
                                  link_types=link_types)

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


    def demangle_concept_name(self, name):
        if name.endswith('_data'):
            name = name[:-5]

        return name


    def mangle_concept_name(self, name):
        return postgresql.string.qname('caos', name + '_data')


    def mangle_domain_name(self, name):
        return postgresql.string.qname('caos', name + '_domain')


    def demangle_domain_name(self, name):
        name = name.split('.')[-1]

        if name.endswith('_domain'):
            name = name[:-7]

        return name


    def pg_type_from_atom_class(self, atom_obj):
        if (atom_obj.base is not None and (atom_obj.base == str or (hasattr(atom_obj.base, 'name') and atom_obj.base.name == 'str'))
                and len(atom_obj.mods) == 1 and issubclass(next(iter(atom_obj.mods.keys())), metamod.AtomModMaxLength)):
            column_type = 'varchar(%d)' % next(iter(atom_obj.mods.values())).value
        else:
            if atom_obj.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_obj.name]
            else:
                column_type = self.mangle_domain_name(atom_obj.name)
        return column_type


    def pg_type_from_atom(self, atom_obj):
        if (atom_obj.base is not None and atom_obj.base == 'str'
                and len(atom_obj.mods) == 1 and issubclass(next(iter(atom_obj.mods.keys())), metamod.AtomModMaxLength)):
            column_type = 'varchar(%d)' % next(iter(atom_obj.mods.values())).value
        else:
            if atom_obj.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_obj.name]
            else:
                self.store(atom_obj, allow_existing=True)
                column_type = self.mangle_domain_name(atom_obj.name)

        return column_type


    def atom_from_pg_type(self, type_expr, atom_name, atom_default, meta):

        demangled = self.demangle_domain_name(type_expr)
        atom = meta.get(demangled)

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
