import psycopg2
from semantix.lib.caos.backends.data.base import BaseDataBackend

class DataBackend(BaseDataBackend):
    def __init__(self, connection):
        self.connection = connection

    def load_entity(self, concept, id):
        query = 'SELECT * FROM "caos"."%s_data" WHERE entity_id = %d' % (concept, id)
        cursor = self.connection.cursor(cursor_factory = psycopg2.extras.DictCursor)
        cursor.execute(query)
        result = cursor.fetchone()

        if result is not None:
            return dict((k, result[k]) for k in result.keys() if k != 'entity_id')
        else:
            return None

    def store_entity(self, concept, id, attrs):
        cursor = self.connection.cursor()

        if id is not None:
            query = 'UPDATE "caos"."%s_data" SET ' % concept
            query += ','.join(['%s = %%(%s)s' % (a, a) for a in attrs])
            query += 'WHERE entity_id = %d RETURNING entity_id' % id
        else:
            query = 'INSERT INTO "caos"."%s_data"' % concept
            query += '(' + ','.join(['"%s"' % a for a in attrs]) + ')'
            query += 'VALUES(' + ','.join(['%%(%s)s' % a for a in attrs]) + ') RETURNING entity_id'

        cursor.execute(query, dict((k, unicode(attrs[k]) if attrs[k] is not None else None)
                                   for k in attrs))
        id = cursor.fetchone()
        if id is None:
            raise Exception('failed to store entity')
        self.connection.commit()

        return id[0]
