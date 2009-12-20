import re
import types
import copy

from semantix.caos import MetaError

from semantix.caos.backends.meta import MetaBackend
from semantix.caos.backends.data import DataBackend
from semantix.caos.backends.meta import RealmMeta, Atom, Concept, ConceptLinkType
from semantix.caos.backends.pgsql.common import DatabaseConnection

from semantix.caos.backends.pgsql.common import DatabaseTable, EntityTable
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
        self.native_cursor = connection.cursor()
        self.adapter = CaosQLQueryAdapter()

    def prepare_query(self, query, vars):
        return self.adapter.adapt(query, vars)

    def execute_prepared(self, query):
        if query.vars is None:
            query.vars = []
        return self.native_cursor.execute(query.text, query.vars)

    def execute(self, query, vars=None):
        native_query = self.prepare_query(query, vars)
        return self.execute_prepared(native_query)

    def fetchall(self):
        return self.native_cursor.fetchall()

    def fetchmany(self, size=None):
        return self.native_cursor.fetchmany(size)

    def fetchone(self):
        return self.native_cursor.fetchone()


class PathCacheTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE caos.path_cache (
                id                  serial NOT NULL,

                entity_id           integer NOT NULL,
                parent_entity_id    integer,

                name_attribute      varchar(255),
                concept_name        varchar(255) NOT NULL,

                weight              integer,

                PRIMARY KEY (id),
                UNIQUE(entity_id, parent_entity_id),

                FOREIGN KEY (entity_id) REFERENCES caos.entity(id)
                    ON UPDATE CASCADE ON DELETE CASCADE,

                FOREIGN KEY (parent_entity_id) REFERENCES caos.entity(id)
                    ON UPDATE CASCADE ON DELETE CASCADE
            )
        """
        super(PathCacheTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO
                caos.path_cache
                    (entity_id, parent_entity_id, name_attribute, concept_name, weight)

                VALUES(%(entity_id)s, %(parent_entity_id)s,
                       %(name_attribute)s, %(concept_name)s, %(weight)s)
            RETURNING entity_id
        """
        return super(PathCacheTable, self).insert(*dicts, **kwargs)


class Backend(MetaBackend, DataBackend):

    typlen_re = re.compile(r"(?P<type>.*) \( (?P<length>\d+) (?:\s*,\s*(?P<precision>\d+))? \)$", re.X)

    check_constraint_re = re.compile(r"CHECK \s* \( (?P<expr>.*) \)$", re.X)

    constraint_type_re = re.compile(r"^(?P<type>[\w-]+)_\d+$", re.X)

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

        self.connection = DatabaseConnection(connection)

        self.concept_table = ConceptTable(self.connection)
        self.concept_table.create()
        self.concept_map_table = ConceptMapTable(self.connection)
        self.concept_map_table.create()
        EntityTable(self.connection).create()
        self.entity_map_table = EntityMapTable(self.connection)
        self.entity_map_table.create()

        self.entity_table = EntityTable(self.connection)
        self.entity_table.create()
        self.path_cache_table = PathCacheTable(self.connection)
        self.path_cache_table.create()

        self.domains = set()

    def getmeta(self):
        meta = RealmMeta()

        domains = DomainsList.fetch(schema_name='caos', connection=self.connection)
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
                        atom.add_mod(constraint_type, constraint_expr)

            meta.add(atom)

        tables = TableList.fetch(schema_name='caos', connection=self.connection)

        for t in tables:
            name = self.demangle_concept_name(t['name'])

            inheritance = TableInheritance.fetch(table_name=t['name'], connection=self.connection)
            inheritance = [i[0] for i in inheritance[1:]]

            bases = tuple()
            if len(inheritance) > 0:
                for table in inheritance:
                    bases += (self.demangle_concept_name(table),)

            concept = Concept(name=name, base=bases)

            columns = TableColumns.fetch(table_name=t['name'], connection=self.connection)
            for row in columns:
                if row['column_name'] == 'entity_id':
                    continue

                atom = self.atom_from_pg_type(row['column_type'], '__' + name + '__' + row['column_name'],
                                              row['column_default'], meta)
                atom = ConceptLinkType(source=concept, targets={atom}, link_type=row['column_name'],
                                       required=row['column_required'], mapping='11')
                concept.add_link(atom)

            meta.add(concept)

        for t in tables:
            name = self.demangle_concept_name(t['name'])
            concept = meta.get(name)

            for r in ConceptLinks.fetch(source_concept=name, connection=self.connection):
                link = ConceptLinkType(meta.get(r['source_concept']), {meta.get(r['target_concept'])},
                                       r['link_type'], r['mapping'], r['required'])
                concept.add_link(link)

        return meta


    def synchronize(self, meta):
        for obj in meta:
            self.store(obj, 1)

        for obj in meta:
            self.store(obj, 2)

    def store(self, obj, phase=1, allow_existing=False):
        is_atom = isinstance(obj, Atom)

        if is_atom:
            if phase == 1:
                self.create_atom(obj, allow_existing)
        else:
            self.create_concept(obj, phase, allow_existing)

    def create_concept(self, obj, phase, allow_exsiting):
        if phase is None or phase == 1:
            self.concept_table.insert(name=obj.name)

            qry = 'CREATE TABLE %s' % self.mangle_concept_name(obj.name, True)

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
                qry += ' INHERITS (' + ','.join([self.mangle_concept_name(p, True) for p in obj.base]) + ')'

            with self.connection as cursor:
                cursor.execute(qry)

        if phase is None or phase == 2:
            for link in obj.links.values():
                if not link.atomic():
                    for target in link.targets:
                        self.concept_map_table.insert(source=link.source.name, target=target.name,
                                                      link_type=link.link_type, mapping=link.mapping,
                                                      required=link.required)


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

                if constr_type in self.constr_expr_res:
                    m = self.constr_expr_res[constr_type].match(constr_expr)
                    if m:
                        constr_expr = m.group('expr')
                    else:
                        raise MetaError('could not parse domain constraint "%s": %s' % (constr_name, constr_expr))

                if constr_type in ('max-length', 'min-length'):
                    constr_expr = int(constr_expr)

                if constr_type == 'expr':
                    # That's a very hacky way to remove casts from expressions added by Postgres
                    constr_expr = constr_expr.replace('::text', '')

                if constr_type not in constraints:
                    constraints[constr_type] = []

                constraints[constr_type].append(constr_expr)

            d['constraints'] = constraints

        if d['basetype'] is not None:
            m = self.typlen_re.match(d['basetype_full'])
            if m:
                if 'max-length' not in d['constraints']:
                    d['constraints']['max-length'] = []

                d['constraints']['max-length'].append(int(m.group('length')))

        if d['default'] is not None:
            # Strip casts from default expression
            d['default'] = self.cast_re.sub('', d['default'])

        return d

    def demangle_concept_name(self, name):
        if name.endswith('_data'):
            name = name[:-5]

        return name

    def mangle_concept_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_data"' % name
        else:
            return 'caos.%s_data' % name

    def mangle_domain_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_domain"' % name
        else:
            return 'caos.%s_domain' % name

    def demangle_domain_name(self, name):
        name = name.split('.')[-1]

        if name.endswith('_domain'):
            name = name[:-7]

        return name

    def pg_type_from_atom_class(self, atom_obj):
        if (atom_obj.base is not None and (atom_obj.base == str or (hasattr(atom_obj.base, 'name') and atom_obj.base.name == 'str'))
                and len(atom_obj.mods) == 1 and 'max-length' in atom_obj.mods):
            column_type = 'varchar(%d)' % atom_obj.mods['max-length']
        else:
            if atom_obj.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_obj.name]
            else:
                column_type = self.mangle_domain_name(atom_obj.name, True)
        return column_type

    def pg_type_from_atom(self, atom_obj):
        if (atom_obj.base is not None and atom_obj.base == 'str'
                and len(atom_obj.mods) == 1 and 'max-length' in atom_obj.mods):
            column_type = 'varchar(%d)' % atom_obj.mods['max-length']
        else:
            if atom_obj.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_obj.name]
            else:
                self.store(atom_obj, allow_existing=True)
                column_type = self.mangle_domain_name(atom_obj.name, True)

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
                    atom.add_mod('max-length', typmod)
                    meta.add(atom)

        return atom

    def get_constraint_expr(self, type, expr):
        constr_name = '%s_%s' % (type, id(expr))
        return ' CONSTRAINT "%s" CHECK ( %s )' % (constr_name, expr)

    def create_atom(self, obj, allow_existing):
        if allow_existing:
            if not self.domains:
                domains = DomainsList.fetch(connection=self.connection, schema_name='caos')
                self.domains = {d['name'] for d in domains}

            if obj.name in self.domains:
                return

        qry = 'CREATE DOMAIN %s AS ' % self.mangle_domain_name(obj.name, True)
        base = obj.base

        if base in self.base_type_name_map:
            base = self.base_type_name_map[base]
        else:
            base = self.mangle_domain_name(base, True)

        basetype = copy.copy(base)

        if 'max-length' in obj.mods and base in self.typmod_types:
            #
            # Convert basetype + max-length constraint into a postgres-native
            # type with typmod, e.g str[max-length: 20] --> varchar(20)
            #
            # Handle the case when min-length == max-length and yield a fixed-size
            # type correctly
            #
            if ('min-length' in obj.mods
                    and obj.mods['max-length'] == obj.mods['min-length']
                    and base in self.fixed_length_types) :
                base = self.fixed_length_types[base]
            base += '(' + str(obj.mods['max-length']) + ')'
        qry += base

        with self.connection as cursor:
            params = []

            if obj.default is not None:
                # XXX: FIXME: put proper backend data escaping here
                qry += ' DEFAULT \'%s\' ' % str(obj.default)

            for constr_type, constr in obj.mods.items():
                if constr_type == 'regexp':
                    for re in constr:
                        # XXX: FIXME: put proper backend data escaping here
                        expr = 'VALUE ~ \'%s\'' % re
                        qry += self.get_constraint_expr(constr_type, expr)
                elif constr_type == 'expr':
                    for expr in constr:
                        qry += self.get_constraint_expr(constr_type, expr)
                elif constr_type == 'max-length':
                    if basetype not in self.typmod_types:
                        qry += self.get_constraint_expr(constr_type, 'length(VALUE::text) <= ' + str(constr))
                elif constr_type == 'min-length':
                    qry += self.get_constraint_expr(constr_type, 'length(VALUE::text) >= ' + str(constr))

            cursor.execute(qry, params)

        self.domains.add(obj.name)

    def get_concept_from_entity(self, id):
        query = """SELECT
                            c.name
                        FROM
                            caos.entity e
                            INNER JOIN caos.concept c ON c.id = e.concept_id
                        WHERE
                            e.id = %d""" % id
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is None:
            return None
        else:
            return result[0]

    def load_entity(self, concept, id):
        query = 'SELECT * FROM "caos"."%s_data" WHERE entity_id = %d' % (concept, id)
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

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

        with self.connection as cursor:

            attrs = {n: v for n, v in links.items() if not isinstance(v, BaseConceptCollection)}
            attrs['entity_id'] = id

            if id is not None:
                query = 'UPDATE "caos"."%s_data" SET ' % concept
                query += ','.join(['%s = %%(%s)s::%s' % (a, a, self.pg_type_from_atom_class(clinks[a].targets[0]) if a in clinks else 'int') for a, v in attrs.items()])
                query += ' WHERE entity_id = %d RETURNING entity_id' % id
            else:
                id = self.entity_table.insert({'concept': concept})[0]

                query = 'INSERT INTO "caos"."%s_data"' % concept
                query += '(' + ','.join(['"%s"' % a for a in attrs]) + ')'
                query += 'VALUES(' + ','.join(['%%(%s)s::%s' % (a, ('text::' + self.pg_type_from_atom_class(clinks[a].targets[0])) if a in clinks else 'int') for a, v in attrs.items()]) + ') RETURNING entity_id'

            data = dict((k, str(attrs[k]) if attrs[k] is not None else None) for k in attrs)
            data['entity_id'] = id

            cursor.execute(query, data)
            id = cursor.fetchone()
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

        links = EntityLinks.fetch(connection=self.connection,
                                  source_id=source_id, target_id=target_id,
                                  target_concepts=target_concepts, source_concepts=source_concepts,
                                  link_types=link_types)

        return links


    @debug
    def store_links(self, concept, link_type, source, targets):
        rows = []

        with self.connection as cursor:

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

                lt = ConceptLink.fetch(connection=self.connection,
                                       source_concepts=[source.concept], target_concepts=targets,
                                       link_type=link_type)

                rows.append('(%s::int, %s::int, %s::int, %s::int)')
                params += [source.id, target.id, lt[0]['id'], 0]

            params += params

            if len(rows) > 0:
                cursor.execute("""INSERT INTO caos.entity_map(source_id, target_id, link_type_id, weight)
                                    ((VALUES %s) EXCEPT (SELECT
                                                                *
                                                            FROM
                                                                caos.entity_map
                                                            WHERE
                                                                (source_id, target_id, link_type_id, weight) in (%s)))
                               """ % (",".join(rows), ",".join(rows)), params)

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


    def iter(self, concept):
        with self.connection as cursor:
            cursor.execute('''SELECT
                                    id
                                FROM
                                    caos.entity
                                WHERE
                                    concept_id = (SELECT id FROM caos.concept WHERE name = %(concept)s)''',
                            {'concept': concept})

            for row in cursor:
                id = row[0]
                yield id


    def caosqlcursor(self):
        return CaosQLCursor(self.connection)
