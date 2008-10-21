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
        self.iter = iter(helper.domains)

    def __iter__(self):
        return self

    def next(self):
        concept = next(self.iter)
        return DomainClass(concept, meta_backend=self.helper.meta_backend)

class MetaBackendHelper(object):

    typlen_re = re.compile(".* \( (?P<length>\d+) \)$", re.X)

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

    def load(self, name):

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
