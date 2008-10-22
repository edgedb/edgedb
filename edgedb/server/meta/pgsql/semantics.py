from semantix.lib.caos import DomainClass, ConceptClass, ConceptAttribute, MetaError

from .datasources.introspection.table import *

class MetaDataIterator(object):
    def __init__(self, helper):
        self.helper = helper
        self.iter = iter(helper.concepts)

    def __iter__(self):
        return self

    def next(self):
        concept = next(self.iter)
        return ConceptClass(concept, meta_backend=self.helper.meta_backend)


class MetaBackendHelper(object):

    def __init__(self, connection, meta_backend):
        self.connection = connection
        self.meta_backend = meta_backend
        self.domain_helper = self.meta_backend.domain_backend
        self.concepts = dict((self.demangle_name(t['name']), t) for t in TableList.fetch(schema_name='caos'))

    def demangle_name(self, name):
        if name.endswith('_data'):
            name = name[:-5]

        return name


    def mangle_name(self, name, quote=False):
        if quote:
            return '"caos"."%s_data"' % name
        else:
            return 'caos.%s_data' % name


    def load(self, name):
        if name not in self.concepts:
            raise MetaError('reference to an undefined concept "%s"' % name)

        columns = TableColumns.fetch(table_name=self.mangle_name(name))

        bases = ()
        dct = {'name': name}

        attributes = {}
        for row in columns:
            domain = self.domain_helper.domain_from_pg_type(row['column_type'], name + '__' + row['column_name'])

            try:
                attr = ConceptAttribute(domain, row['column_required'], row['column_default'])
                attributes[row['column_name']] = attr
            except MetaError, e:
                print e

        dct['attributes'] = attributes

        inheritance = TableInheritance.fetch(table_name=self.mangle_name(name))
        inheritance = [i[0] for i in inheritance[1:]]

        if len(inheritance) > 0:
            for table in inheritance:
                bases += (ConceptClass(self.demangle_name(table), meta_backend=self.meta_backend),)

        return bases, dct


    def store(self, cls):
        try:
            current = ConceptClass(cls.name, meta_backend=self.meta_backend)
        except MetaError:
            current = None

        if current is None:
            self.create_concept(cls)

    def create_concept(self, cls):
        qry = 'CREATE TABLE %s' % self.mangle_name(cls.name, True)

        columns = []

        for attr_name in sorted(cls.attributes.keys()):
            attr = cls.attributes[attr_name]
            column_type = self.domain_helper.pg_type_from_domain(attr.domain)
            column = '"%s" %s %s' % (attr_name, column_type, 'NOT NULL' if attr.required else '')
            columns.append(column)

        qry += '(' + ','.join(columns) + ')'

        if len(cls.parents) > 0:
            qry += ' INHERITS (' + ','.join([self.mangle_name(p, True) for p in cls.parents]) + ')'

        self.meta_backend.cursor.execute(qry)

    def __iter__(self):
        return MetaDataIterator(self)
