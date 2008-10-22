import re
import types
import copy

from semantix.lib.caos import DomainClass, MetaError

from .datasources.introspection.domains import DomainsList

class MetaDataIterator(object):
    def __init__(self, helper):
        self.helper = helper
        self.iter = iter(helper.domains)

    def __iter__(self):
        return self

    def next(self):
        domain = next(self.iter)
        return DomainClass(domain, meta_backend=self.helper.meta_backend)


class MetaBackendHelper(object):

    typlen_re = re.compile("(?P<type>.*) \( (?P<length>\d+) \)$", re.X)

    check_constraint_re = re.compile("CHECK \s* \( (?P<expr>.*) \)$", re.X)

    constraint_type_re = re.compile("^(?P<type>[\w-]+)_\d+$", re.X)

    constr_expr_res = {
                        'regexp': re.compile("VALUE::text \s* ~ \s* '(?P<expr>.*)'::text$", re.X),
                        'max-length': re.compile("length\(VALUE::text\) \s* <= \s* (?P<expr>\d+)$", re.X),
                        'min-length': re.compile("length\(VALUE::text\) \s* >= \s* (?P<expr>\d+)$", re.X)
                      }

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
        self.domains = dict((self.demangle_name(d['name']), self.normalize_domain_descr(d))
                                for d in DomainsList.fetch(schema_name='caos'))

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


    def mangle_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_domain"' % name
        else:
            return 'caos.%s_domain' % name

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
                typmod = int(m.group('length'))
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

                column_type = self.mangle_name(domain_cls.name, True)

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

        if domain_descr['constraints'] is not None:
            for constraint_type in domain_descr['constraints']:
                for constraint_expr in domain_descr['constraints'][constraint_type]:
                    self.meta_backend.add_domain_constraint(dct['constraints'], constraint_type, constraint_expr)

        if domain_descr['basetype'] in self.base_type_name_map_r:
            dct['basetype'] = self.base_type_name_map_r[domain_descr['basetype']]
        else:
            dct['basetype'] = self.demangle_name(domain_descr['basetype'])

        dct['basetype'] = DomainClass(dct['basetype'], meta_backend=self.meta_backend)
        bases = (dct['basetype'],)

        return bases, dct


    def store(self, cls):
        try:
            current = DomainClass(cls.name, meta_backend=self.meta_backend)
        except MetaError:
            current = None

        if current is None:
            self.create_domain(cls)

    def get_constraint_expr(self, type, expr):
        constr_name = '%s_%s' % (type, id(expr))
        return ' CONSTRAINT "%s" CHECK ( %s )' % (constr_name, expr)

    def create_domain(self, cls):
        qry = 'CREATE DOMAIN %s AS ' % self.mangle_name(cls.name, True)
        base = cls.basetype.name

        if base in self.base_type_name_map:
            base = self.base_type_name_map[base]
        else:
            base = self.mangle_name(base, True)

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
                    expr = self.meta_backend.cursor.mogrify('VALUE ~ %(re)s', {'re': re})
                    qry += self.get_constraint_expr(constr_type, expr)
            elif constr_type == 'expr':
                for expr in constr:
                    qry += self.get_constraint_expr(constr_type, expr)
            elif constr_type == 'max-length':
                if basetype not in self.typmod_types:
                    qry += self.get_constraint_expr(constr_type, 'length(VALUE::text) <= ' + str(constr))
            elif constr_type == 'min-length':
                qry += self.get_constraint_expr(constr_type, 'length(VALUE::text) >= ' + str(constr))

        self.meta_backend.cursor.execute(qry)

    def __iter__(self):
        return MetaDataIterator(self)
