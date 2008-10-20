from semantix.lib import datasources
from semantix.lib.caos import cls, domain

class ConceptBackend(cls.MetaBackend):
    def load(self, cls, name):
        columns = datasources.fetch('sys.table.columns', table_name=name + '_data', schema_name='caos')

        attributes = {}
        for row in columns:
            attributes[row['column_name']] = row

        inheritance = datasources.fetch('sys.table.inheritance', table_name=name + '_data', schema_name='caos')


        return ()

    def store(self, cls):
        pass
