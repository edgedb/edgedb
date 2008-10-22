import psycopg2
import re
import types
import copy

from semantix.lib.decorators import memoized
from semantix.lib.caos.domain import DomainClass
from semantix.lib.caos.backends.meta.base import MetaError

from .datasources.introspection.domains import DomainsList

class MetaDataIterator(object):
    def __init__(self, helper):
        self.helper = helper
        self.iter = iter(helper.semantics_list)

    def __iter__(self):
        return self

    def next(self):
        concept = next(self.iter)
        return ConceptClass(concept['name'], meta_backend=self.helper.meta_backend)


class MetaBackendHelper(object):

    typlen_re = re.compile("(?P<type>.*) \( (?P<length>\d+) \)$", re.X)

    check_constraint_re = re.compile("CHECK \s* \( (?P<expr>.*) \)$", re.X)

    re_constraint_re = re.compile("VALUE::text \s* ~ \s* '(?P<re>.*)'::text$", re.X)

    base_type_map = {
                        'integer': types.IntType,
                        'character': types.UnicodeType,
                        'character varying': types.UnicodeType,
                        'boolean': types.BooleanType,
                        'numeric': types.LongType
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

    def __init__(self, connection, meta_backend):
        self.connection = connection
        self.meta_backend = meta_backend
        self.domains = dict((self.demangle_name(d['name']), d) for d in DomainsList.fetch(schema_name='caos'))

    def mangle_name(self, name):
        return 'caos.' + name + '_domain'

    def demangle_name(self, name):
        name = name.split('.')[-1]

        if name.endswith('_domain'):
            name = name[:-7]

        return name


    def domain_from_pg_type(self, type_expr, domain_name):
        result = None

        demangled = self.demangle_name(type_expr)

        if demangled in self.domains:
            result = DomainClass(demangled, meta_backend=self.meta_backend)
        else:
            m = MetaBackendHelper.typlen_re.match(type_expr)
            if m:
                typmod = m.group('length')
                typname = m.group('type').strip()
            else:
                typmod = None
                typname = type_expr

            if typname in self.base_type_name_map_r:
                domain = self.base_type_name_map_r[typname]

                if typname in self.typmod_types and typmod is not None:
                    domain = {'name': domain_name, 'domain': domain, 'constraints': [{'max-length': typmod}]}

                result = DomainClass(domain, meta_backend=self.meta_backend)

        return result


    def pg_type_from_domain(self, domain_cls):
        # Check if the domain is in our backend
        try:
            domain = DomainClass(domain_cls.name, meta_backend=self.meta_backend)
        except MetaError:
            if domain_cls.name in self.meta_backend.domain_cache:
                domain = self.meta_backend.domain_cache[domain_cls.name]
            else:
                domain = None

        if (domain_cls.basetype is not None and domain_cls.basetype.name == 'str'
                and len(domain_cls.constraints) == 1 and 'max-length' in domain_cls.constraints):
            column_type = 'varchar(%d)' % domain_cls.constraints['max-length']
        else:
            if domain_cls.name in self.base_type_name_map:
                column_type = self.base_type_name_map[domain_cls.name]
            else:
                if domain is None:
                    self.meta_backend.store(domain_cls)

                column_type = '"caos"."%s_domain"' % domain_cls.name

        return column_type


    def load(self, name):
        if isinstance(name, dict):
            return self.meta_backend.load_domain(name['name'], name)

        if name not in self.domains:
            raise MetaError('reference to an undefined domain "%s"' % name)

        domain_descr = self.domains[name]

        bases = tuple()
        dct = {}

        dct['constraints'] = {}
        dct['name'] = name
        dct['basetype'] = None

        if domain_descr['basetype'] is not None:
            m = MetaBackendHelper.typlen_re.match(domain_descr['basetype_full'])
            if m:
                self.meta_backend.add_domain_constraint(dct['constraints'], 'max-length', m.group('length'))

        if domain_descr['constraints'] is not None:
            for constraint in domain_descr['constraints']:
                if constraint.startswith('CHECK'):
                    # Strip `CHECK()`
                    constraint = MetaBackendHelper.check_constraint_re.match(constraint).group('expr').replace("''", "'")

                    m = MetaBackendHelper.re_constraint_re.match(constraint)
                    if m:
                        constraint = ('regexp', m.group('re'))
                    else:
                        constraint = ('expr', constraint)

                    self.meta_backend.add_domain_constraint(dct['constraints'], *constraint)
                else:
                    raise IntrospectionError('unknown domain constraint type: `%s`' % constraint)

        if domain_descr['basetype'] in MetaBackendHelper.base_type_map:
            dct['basetype'] = MetaBackendHelper.base_type_map[domain_descr['basetype']]
            bases = (dct['basetype'],)

        return bases, dct


    def store(self, cls):
        try:
            current = DomainClass(cls.name, meta_backend=self.meta_backend)
        except MetaError:
            current = None

        if current is None:
            self.create_domain(cls)

    def create_domain(self, cls):
        qry = 'CREATE DOMAIN "caos"."%s_domain" AS ' % cls.name
        base = cls.basetype.name

        if base in self.base_type_name_map:
            base = self.base_type_name_map[base]

        basetype = copy.copy(base)

        if 'max-length' in cls.constraints and base in self.typmod_types:
            #
            # Convert basetype + max-length constraint into a postgres-native
            # type with typmod, e.g str[max-length: 20] --> varchar(20)
            #
            # Handle the case when min-length == max-length and yield a fixed-size
            # type correctly
            #
            if ('min-length' in cls.constraints
                    and cls.constraints['max-length'] == cls.constraints['min-length']
                    and base in self.fixed_length_types) :
                base = self.fixed_length_types[base]
            base += '(' + str(cls.constraints['max-length']) + ')'
        qry += base

        for constr_type, constr in cls.constraints.items():
            if constr_type == 'regexp':
                for re in constr:
                    qry += self.meta_backend.cursor.mogrify(' CHECK (VALUE ~ %(re)s) ', {'re': re})
            elif constr_type == 'expr':
                for expr in constr:
                    qry += ' CHECK (' + expr + ') '
            elif constr_type == 'max-length':
                if basetype not in self.typmod_types:
                    qry += ' CHECK (length(VALUE::text) <= ' + str(constr) + ') '
            elif constr_type == 'min-length':
                qry += ' CHECK (length(VALUE::text) >= ' + str(constr) + ') '

        self.meta_backend.cursor.execute(qry)

    def __iter__(self):
        return MetaDataIterator(self)
