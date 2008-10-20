import psycopg2
import re
import types

from semantix.lib.decorators import memoized
from semantix.lib import db
from semantix.lib.caos import cls, domain

class DomainBackend(cls.MetaBackend):

    typlen_re = re.compile(".* \( (?P<length>\d+) \)$", re.X)

    check_constraint_re = re.compile("CHECK \s* \( (?P<expr>.*) \)$", re.X)

    re_constraint_re = re.compile("VALUE::text \s* ~ \s* '(?P<re>.*)'::text$", re.X)

    base_type_map = {
                        'integer': types.IntType,
                        'character': types.UnicodeType,
                        'character varying': types.UnicodeType
                    }

    def load(self, cls, name):
        domains = DomainBackend._fetch_domains(db.connection)

        if 'aux.' + name not in domains:
            raise domain.DomainError('reference to an undefined domain "%s"' % name)

        row = domains['aux.' + name]

        cls.constraints = {}
        cls.name = row['name']
        cls.basetype = None

        if row['basetype'] is not None:
            m = DomainBackend.typlen_re.match(row['basetype_full'])
            if m:
                cls.add_constraint('max-length', m.group('length'))

        if row['constraints'] is not None:
            for constraint in row['constraints']:
                if constraint.startswith('CHECK'):
                    # Strip `CHECK()`
                    constraint = DomainBackend.check_constraint_re.match(constraint).group('expr').replace("''", "'")

                    m = DomainBackend.re_constraint_re.match(constraint)
                    if m:
                        constraint = ('regexp', m.group('re'))
                    else:
                        constraint = ('expr', constraint)

                    cls.add_constraint(*constraint)
                else:
                    raise IntrospectionError('unknown domain constraint type: `%s`' % constraint)

        if row['basetype'] in DomainBackend.base_type_map:
            cls.basetype = DomainBackend.base_type_map[row['basetype']]
            bases = (cls.basetype,)
        else:
            bases = tuple()

        return bases

    def store(self, cls):
        pass

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
            schemas = ['aux']

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
