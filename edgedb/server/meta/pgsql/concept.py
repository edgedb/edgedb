from semantix.lib.caos.datasources.introspection.table import *
from semantix.lib.caos import cls, domain, concept

class ConceptBackend(cls.MetaBackend):
    def load(self, cls, name):
        columns = TableColumns.fetch(table_name=name + '_data', schema_name='caos')

        attributes = {}
        for row in columns:
            attr = {
                        'name': row['column_name'],
                        'domain': domain.DomainClass(row['column_type']),
                        'required': row['column_required'],
                        'default': row['column_default']
                   }
            attributes[row['column_name']] = attr

        cls.attributes = attributes

        inheritance = TableInheritance.fetch(table_name=name + '_data', schema_name='caos')
        inheritance = [i[0] for i in inheritance[1:]]

        bases = ()
        if len(inheritance) > 0:
            for table in inheritance:
                if table.endswith('_data'):
                    table = table[:-5]
                bases += (concept.ConceptClass(table),)

        return bases

    def store(self, cls):
        pass
