import re
import types
import copy

from psycopg2 import ProgrammingError

from semantix.caos import Class, MetaError, ConceptLinkType
from semantix.caos.atom import Atom

from semantix.caos.backends.meta import BaseMetaBackend
from semantix.caos.backends.pgsql.common import DatabaseConnection

from semantix.caos.backends.pgsql.common import DatabaseTable, EntityTable
from semantix.caos.backends.pgsql.common import ConceptTable, ConceptMapTable, EntityTable, EntityMapTable

from .datasources.introspection.domains import *
from .datasources.introspection.table import *
from .datasources.meta.concept import *


class MetaDataIterator(object):
    def __init__(self, helper, iter_atoms):
        self.helper = helper
        self.iter_atoms = iter_atoms

        if self.iter_atoms:
            self.iter = iter(helper._atoms)
        else:
            self.iter = iter(helper._concepts)

    def __iter__(self):
        return self

    def __next__(self):
        concept = next(self.iter)

        return Class(concept, meta_backend=self.helper)

class MetaBackend(BaseMetaBackend):

    typlen_re = re.compile("(?P<type>.*) \( (?P<length>\d+) \)$", re.X)

    check_constraint_re = re.compile("CHECK \s* \( (?P<expr>.*) \)$", re.X)

    constraint_type_re = re.compile("^(?P<type>[\w-]+)_\d+$", re.X)

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
                        'numeric': int
                    }

    base_type_name_map = {
                                'str': 'character varying',
                                'int': 'integer',
                                'bool': 'boolean',
                                'long': 'numeric'
                         }

    base_type_name_map_r = {
                                'character varying': 'str',
                                'character': 'str',
                                'text': 'str',
                                'integer': 'int',
                                'boolean': 'bool',
                                'numeric': 'long'
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

        tables = TableList.fetch(schema_name='caos')
        self._concepts = {self.demangle_concept_name(t['name']): t for t in tables}

        self._atoms = {}
        domains = DomainsList.fetch(schema_name='caos')
        domains = {self.demangle_domain_name(d['name']): self.normalize_domain_descr(d) for d in domains}

        for name, domain_descr in domains.items():
            atom = {}
            atom['mods'] = []
            atom['name'] = name
            atom['extends'] = None
            atom['default'] = domain_descr['default']

            if domain_descr['constraints'] is not None:
                for constraint_type in domain_descr['constraints']:
                    for constraint_expr in domain_descr['constraints'][constraint_type]:
                        atom['mods'].append({constraint_type: constraint_expr})

            if domain_descr['basetype'] in self.base_type_name_map_r:
                atom['extends'] = self.base_type_name_map_r[domain_descr['basetype']]
            else:
                atom['extends'] = self.demangle_domain_name(domain_descr['basetype'])

            self._atoms[name] = atom


    def iter_atoms(self):
        return MetaDataIterator(self, True)

    def iter_concepts(self):
        return MetaDataIterator(self, False)

    def is_atom(self, name):
        return super().is_atom(name) or name in self._atoms

    def do_load(self, name):
        if self.is_atom(name):
            return self.load_atom(self._atoms[name])

        if name not in self._concepts:
            raise MetaError('reference to an undefined concept "%s"' % name)

        bases = ()
        dct = {'concept': name}

        columns = TableColumns.fetch(table_name=self.mangle_concept_name(name))
        atoms = {}
        for row in columns:
            if row['column_name'] == 'entity_id':
                continue

            atom = self.atom_from_pg_type(row['column_type'], '__' + name + '__' + row['column_name'],
                                          row['column_default'])
            atom = ConceptLinkType(source=name, target=atom, link_type=row['column_name'],
                                   required=row['column_required'], mapping='11')
            atoms[row['column_name']] = atom

        dct['atoms'] = atoms

        dct['links'] = {}
        for r in ConceptLinks.fetch(source_concept=name):
            l = ConceptLinkType(r['source_concept'], r['target_concept'], r['link_type'],
                                r['mapping'], r['required'])
            dct['links'][(r['link_type'], r['target_concept'])] = l

        inheritance = TableInheritance.fetch(table_name=self.mangle_concept_name(name))
        inheritance = [i[0] for i in inheritance[1:]]

        if len(inheritance) > 0:
            for table in inheritance:
                bases += (Class(self.demangle_concept_name(table), meta_backend=self),)

        return bases, dct


    def store(self, cls, phase=1):
        is_atom = issubclass(cls, Atom)

        try:
            current = Class(cls.concept, meta_backend=self)
        except MetaError:
            current = None

        if current is None or (phase == 2 and not is_atom):
            if is_atom:
                self.create_atom(cls)
            else:
                self.create_concept(cls, phase)
            self.cache[cls.concept] = cls


    def demangle_concept_name(self, name):
        if name.endswith('_data'):
            name = name[:-5]

        return name


    def mangle_concept_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_data"' % name
        else:
            return 'caos.%s_data' % name

    def create_concept(self, cls, phase):
        if phase is None or phase == 1:
            concept = self.concept_table.insert(name=cls.concept)

            qry = 'CREATE TABLE %s' % self.mangle_concept_name(cls.concept, True)

            columns = ['entity_id integer NOT NULL REFERENCES caos.entity(id) ON DELETE CASCADE']

            for link_name in sorted(cls.atoms.keys()):
                atom_link = cls.atoms[link_name]
                column_type = self.pg_type_from_atom(atom_link.target)
                column = '"%s" %s %s' % (link_name, column_type, 'NOT NULL' if atom_link.required else '')
                columns.append(column)

            qry += '(' +  ','.join(columns) + ')'

            if len(cls.parents) > 0:
                qry += ' INHERITS (' + ','.join([self.mangle_concept_name(p, True) for p in cls.parents]) + ')'

            with self.connection as cursor:
                cursor.execute(qry)

        if phase is None or phase == 2:
            for link in cls.links.values():
                self.concept_map_table.insert(source=link.source, target=link.target,
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

        return d


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


    def atom_from_pg_type(self, type_expr, atom_name, atom_default):
        result = None

        demangled = self.demangle_domain_name(type_expr)

        if demangled in self._atoms:
            result = Class(demangled, meta_backend=self)
        else:
            m = self.typlen_re.match(type_expr)
            if m:
                typmod = int(m.group('length'))
                typname = m.group('type').strip()
            else:
                typmod = None
                typname = type_expr

            if typname in self.base_type_name_map_r:
                atom = self.base_type_name_map_r[typname]

                if typname in self.typmod_types and typmod is not None:
                    atom = {'name': atom_name, 'extends': atom,
                              'mods': [{'max-length': typmod}], 'default': atom_default}
                    self._atoms[atom_name] = atom
                    atom = atom_name

                result = Class(atom, meta_backend=self)

        return result


    def pg_type_from_atom(self, atom_cls):
        # Check if the atom is in our backend
        try:
            atom = Class(atom_cls.name, meta_backend=self)
        except MetaError:
            if atom_cls.name in self.cache:
                atom = self.cache[atom_cls.name]
            else:
                atom = None

        if (atom_cls.base is not None and atom_cls.base.name == 'str'
                and len(atom_cls.mods) == 1 and 'max-length' in atom_cls.mods):
            column_type = 'varchar(%d)' % atom_cls.mods['max-length']
        else:
            if atom_cls.name in self.base_type_name_map:
                column_type = self.base_type_name_map[atom_cls.name]
            else:
                if atom is None:
                    self.store(atom_cls)

                column_type = self.mangle_domain_name(atom_cls.name, True)

        return column_type


    def get_constraint_expr(self, type, expr):
        constr_name = '%s_%s' % (type, id(expr))
        return ' CONSTRAINT "%s" CHECK ( %s )' % (constr_name, expr)

    def create_atom(self, cls):
        qry = 'CREATE DOMAIN %s AS ' % self.mangle_domain_name(cls.name, True)
        base = cls.base.name

        if base in self.base_type_name_map:
            base = self.base_type_name_map[base]
        else:
            base = self.mangle_domain_name(base, True)

        basetype = copy.copy(base)

        if 'max-length' in cls.mods and base in self.typmod_types:
            #
            # Convert basetype + max-length constraint into a postgres-native
            # type with typmod, e.g str[max-length: 20] --> varchar(20)
            #
            # Handle the case when min-length == max-length and yield a fixed-size
            # type correctly
            #
            if ('min-length' in cls.mods
                    and cls.mods['max-length'] == cls.mods['min-length']
                    and base in self.fixed_length_types) :
                base = self.fixed_length_types[base]
            base += '(' + str(cls.mods['max-length']) + ')'
        qry += base

        with self.connection as cursor:
            if cls.default is not None:
                # XXX: do something about the whole adapt() thing
                qry += cursor.mogrify(' DEFAULT %(val)s ', {'val': str(cls.default)})

            for constr_type, constr in cls.mods.items():
                if constr_type == 'regexp':
                    for re in constr:
                        expr = cursor.mogrify('VALUE ~ %(re)s', {'re': re})
                        qry += self.get_constraint_expr(constr_type, expr)
                elif constr_type == 'expr':
                    for expr in constr:
                        qry += self.get_constraint_expr(constr_type, expr)
                elif constr_type == 'max-length':
                    if basetype not in self.typmod_types:
                        qry += self.get_constraint_expr(constr_type, 'length(VALUE::text) <= ' + str(constr))
                elif constr_type == 'min-length':
                    qry += self.get_constraint_expr(constr_type, 'length(VALUE::text) >= ' + str(constr))

            cursor.execute(qry)
