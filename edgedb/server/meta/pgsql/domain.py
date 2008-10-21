import psycopg2
import re
import types
import copy

from semantix.lib.decorators import memoized
from semantix.lib.caos.domain import DomainClass
from semantix.lib.caos.backends.meta.base import MetaError

class MetaBackendHelper(object):

    typlen_re = re.compile(".* \( (?P<length>\d+) \)$", re.X)

    check_constraint_re = re.compile("CHECK \s* \( (?P<expr>.*) \)$", re.X)

    re_constraint_re = re.compile("VALUE::text \s* ~ \s* '(?P<re>.*)'::text$", re.X)

    base_domains = {
                        'str': types.UnicodeType,
                        'int': types.IntType,
                        'long': types.LongType,
                        'bool': types.BooleanType
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

    typmod_types = ('character', 'character varying', 'numeric')
    fixed_length_types = {'character varying': 'character'}

    def __init__(self, connection, meta_backend):
        self.connection = connection
        self.meta_backend = meta_backend

    def load(self, name):
        domains = MetaBackendHelper._fetch_domains(self.connection)

        if name in self.base_domains:
            dct = {'name': name, 'constraints': {}, 'basetype': None}
            return (self.base_domains[name],), dct

        if 'caos.' + name + '_domain' not in domains:
            raise MetaError('reference to an undefined domain "%s"' % name)

        row = domains['caos.' + name + '_domain']

        bases = tuple()
        dct = {}

        dct['constraints'] = {}
        dct['name'] = name
        dct['basetype'] = None

        if row['basetype'] is not None:
            m = MetaBackendHelper.typlen_re.match(row['basetype_full'])
            if m:
                self.meta_backend.add_domain_constraint(dct['constraints'], 'max-length', m.group('length'))

        if row['constraints'] is not None:
            for constraint in row['constraints']:
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

        if row['basetype'] in MetaBackendHelper.base_type_map:
            dct['basetype'] = MetaBackendHelper.base_type_map[row['basetype']]
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

    @staticmethod
    @memoized
    def _normalize_type(connection, type, schema):
        cursor = connection.cursor()
        cursor.execute("""SELECT
                                lower(pg_type.oid::regtype::text)
                            FROM
                                pg_type
                                INNER JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid

                            WHERE
                                (typname = %(type)s
                                    OR pg_type.oid::regtype::text = %(type)s
                                    OR pg_type.oid::regtype::text = any(%(fqtn)s))
                                AND nspname = any(%(schema)s)
                       """, {'type': type, 'schema': schema, 'fqtn': [(s + '.' + type) for s in schema]})

        result = cursor.fetchone()

        if result is not None:
            return result[0].split('.')[-1]
        else:
            return None

    @staticmethod
    @memoized
    def _fetch_domains(connection, schemas = None):
        if schemas is None:
            schemas = ['caos']

        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
        cursor.execute("""
                            SELECT
                                    t.oid                                 AS oid,
                                    t.typname                             AS name,
                                    ns.nspname                            AS schema,

                                    CASE WHEN t.typbasetype != 0 THEN
                                        format_type(t.typbasetype, t.typtypmod)
                                    ELSE
                                        NULL
                                    END                                 AS basetype_full,

                                    CASE WHEN t.typbasetype != 0 THEN
                                        format_type(t.typbasetype, NULL)
                                    ELSE
                                        NULL
                                    END                                 AS basetype,

                                    ARRAY(SELECT
                                                pg_get_constraintdef(c.oid, true)
                                            FROM
                                                pg_constraint AS c
                                            WHERE
                                                c.contypid = t.oid)     AS constraints
                                FROM
                                    pg_type AS t
                                    INNER JOIN pg_namespace AS ns ON ns.oid = t.typnamespace
                                WHERE
                                    --
                                    -- Limit the schema scope
                                    --
                                    ns.nspname = any(%(schemas)s)
                                    --
                                    -- We're not interested in shell- or pseudotypes
                                    --
                                    AND t.typisdefined AND t.typtype != 'p'
                       """,
                       {'schemas': schemas})

        result = {}

        for row in cursor:
            result[row['schema'] + '.' + row['name']] = row

        return result
