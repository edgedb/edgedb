from semantix.lib import db
from semantix.lib.caos import cls, domain
from semantix.lib.decorators import memoized

class MetaBackend(cls.MetaBackend):
    def load(self, cls):
        if issubclass(cls, domain.Domain):
            print 'herer'
        pass

    def store(self, cls):
        pass

    def add_domain_constraint(self, cls, type, value):
        if type == 'max-length':
            if 'max-length' in self.constraints:
                cls.constraints['max-length'] = min(self.constraints['max-length'], value)
            else:
                cls.constraints['max-length'] = value

        elif type == 'min-length':
            if 'min-length' in self.constraints:
                cls.constraints['min-length'] = max(self.constraints['min-length'], value)
            else:
                cls.constraints['min-length'] = value

        else:
            if type in self.constraints:
                cls.constraints[type].append(value)
            else:
                cls.constraints[type] = [value]

'''
class Domain(domain.BaseDomain):
    # XXX: Find a better heuristics to extract min/max length constraints
    #      from typmods
    typmod_types = ('character', 'character varying')
    fixed_length_types = {'character varying': 'character'}

    root_domains = {
                        'str'   : 'varchar',
                        'int'   : 'integer',
                        'bool'  : 'bool'
                   }

    def __init__(self, name):
        super(Domain. self).__init__(name)

        self.connection = db.connection
        self.schema = 'caos'
        self.fqtn = '"%s"."%s"' % (self.schema, self.name)
        self.basetype = self.normalize_type(self.connection, basetype, ['public', 'pg_catalog'])

    @staticmethod
    @memoized
    def normalize_type(connection, type, schema):
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

    def get_create_stmt(self):
        qry = 'CREATE DOMAIN %s AS ' % self.fqtn
        base = self.basetype

        if 'max-length' in self.constraints and self.basetype in self.typmod_types:
            if ('min-length' in self.constraints
                    and self.constraints['max-length'] == self.constraints['min-length']
                    and self.basetype in self.fixed_length_types) :
                base = self.fixed_length_types[self.basetype]
            base += '(' + str(self.constraints['max-length']) + ')'
        qry += base

        for constr_type, constr in self.constraints.items():
            if constr_type == 'regexp':
                for re in constr:
                    qry += self.connection.cursor().mogrify(' CHECK (VALUE ~ E%(re)s) ', {'re': re})
            elif constr_type == 'expr':
                for expr in constr:
                    qry += ' CHECK (' + expr + ') '
            elif constr_type == 'max-length':
                if self.basetype not in self.typmod_types:
                    qry += ' CHECK (length(VALUE::text) <= ' + str(constr) + ') '
            elif constr_type == 'min-length':
                qry += ' CHECK (length(VALUE::text) >= ' + str(constr) + ') '

        return DDLStatement(DDLStatement.Object.TYPE, DDLStatement.Action.CREATE, self.fqtn, qry)

    def get_drop_stmt(self):
        qry = 'DROP DOMAIN %s"' % self.fqtn
        return DDLStatement(DDLStatement.Object.TYPE, DDLStatement.Action.DROP, self.fqtn, qry)

    def get_alter_stmt(self, goal):
        qry = 'ALTER DOMAIN %s"' % self.fqtn
        return DDLStatement(DDLStatement.Object.TYPE, DDLStatement.Action.ALTER, self.fqtn, None)
'''
