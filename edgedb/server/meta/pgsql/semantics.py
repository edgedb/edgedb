from semantix.lib.caos.domain import DomainClass
from semantix.lib.caos.concept import ConceptClass
from semantix.lib.caos.backends.meta.base import MetaError

from .datasources.introspection.table import *

class MetaBackendHelper(object):

    base_type_name_map = {
                             'str': 'character varying',
                             'int': 'integer',
                             'bool': 'boolean'
                         }

    base_type_name_map_reverse = {
                                    'text': 'str',
                                    'character varying': 'str',
                                    'integer': 'int',
                                    'boolean': 'bool'
                                 }

    def __init__(self, connection, meta_backend):
        self.connection = connection
        self.meta_backend = meta_backend

    def load(self, name):
        columns = TableColumns.fetch(table_name=name + '_data', schema_name='caos')

        if columns is None or len(columns) == 0:
            raise MetaError('reference to an undefined concept "%s"' % name)

        bases = ()
        dct = {}

        attributes = {}
        for row in columns:
            domain_name = row['column_type']
            if row['column_type_full'] in self.base_type_name_map_reverse:
                domain_name = self.base_type_name_map_reverse[row['column_type_full']]
            elif domain_name.endswith('_domain'):
                domain_name = domain_name[:-7]

            try:
                attr = {
                            'name': row['column_name'],
                            'domain': DomainClass(domain_name, meta_backend=self.meta_backend),
                            'required': row['column_required'],
                            'default': row['column_default']
                       }
                attributes[row['column_name']] = attr
            except MetaError, e:
                print e

        dct['attributes'] = attributes

        inheritance = TableInheritance.fetch(table_name=name + '_data', schema_name='caos')
        inheritance = [i[0] for i in inheritance[1:]]

        if len(inheritance) > 0:
            for table in inheritance:
                if table.endswith('_data'):
                    table = table[:-5]
                bases += (ConceptClass(table, meta_backend=self.meta_backend),)

        return bases, dct


    def store(self, cls):
        try:
            current = ConceptClass(cls.name, meta_backend=self.meta_backend)
        except MetaError:
            current = None

        if current is None:
            self.create_concept(cls)

    def get_column_type(self, domain_cls):
        # Check if the domain is in our backend
        try:
            domain = DomainClass(domain_cls.name, meta_backend=self.meta_backend)
        except MetaError:
            if domain_cls.name in self.meta_backend.domain_cache:
                domain = self.meta_backend.domain_cache[domain_cls.name]
            else:
                domain = None

        if domain is None:
            if (domain_cls.basetype is not None and domain_cls.basetype.name == 'str'
                    and len(domain_cls.constraints) == 1 and 'max-length' in domain_cls.constraints):
                column_type = 'varchar(%d)' % domain_cls.constraints['max-length']
            else:
                if domain_cls.name in self.base_type_name_map:
                    column_type = self.base_type_name_map[domain_cls.name]
                else:
                    self.meta_backend.store(domain_cls)
                    column_type = '"caos"."%s_domain"' % domain_cls.name
        else:
            column_type = '"caos"."%s_domain"' % domain_cls.name

        return column_type


    def create_concept(self, cls):
        qry = 'CREATE TABLE "caos"."%s_data"' % cls.name

        columns = []

        for attr_name in sorted(cls.attributes.keys()):
            attr = cls.attributes[attr_name]
            column_type = self.get_column_type(attr.domain)
            column = '"%s" %s %s' % (attr_name, column_type, 'NOT NULL' if attr.required else '')
            columns.append(column)

        qry += '(' + ','.join(columns) + ')'

        if len(cls.parents) > 0:
            qry += ' INHERITS (' + ','.join(['"caos"."%s_data"' % p for p in cls.parents]) + ')'

        self.meta_backend.cursor.execute(qry)
