from __future__ import with_statement

import psycopg2
from semantix.lib.caos.backends.data.base import BaseDataBackend
from semantix.lib.caos.backends.meta.pgsql.common import DatabaseConnection, DatabaseTable

class EntityTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE "caos"."entity"(
                id serial NOT NULL,
                concept_id integer NOT NULL,

                PRIMARY KEY (id),
                FOREIGN KEY (concept_id) REFERENCES "caos"."concept"(id)
            )
        """
        super(EntityTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO "caos"."entity"(concept_id) (SELECT id FROM caos.concept WHERE name = %(concept)s) RETURNING id
        """
        return super(EntityTable, self).insert(*dicts, **kwargs)


class DataBackend(BaseDataBackend):
    def __init__(self, connection):
        self.connection = DatabaseConnection(connection)
        self.entity_table = EntityTable(self.connection)

    def load_entity(self, concept, id):
        query = 'SELECT * FROM "caos"."%s_data" WHERE entity_id = %d' % (concept, id)
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        if result is not None:
            return dict((k, result[k]) for k in result.keys() if k != 'entity_id')
        else:
            return None

    def store_entity(self, concept, id, attrs):
        with self.connection as cursor:

            if id is not None:
                query = 'UPDATE "caos"."%s_data" SET ' % concept
                query += ','.join(['%s = %%(%s)s' % (a, a) for a in attrs])
                query += 'WHERE entity_id = %d RETURNING entity_id' % id
            else:
                id = self.entity_table.insert({'concept': concept})[0]

                query = 'INSERT INTO "caos"."%s_data"' % concept
                query += '(entity_id, ' + ','.join(['"%s"' % a for a in attrs]) + ')'
                query += 'VALUES(%(entity_id)s, ' + ','.join(['%%(%s)s' % a for a in attrs]) + ') RETURNING entity_id'

            data = dict((k, unicode(attrs[k]) if attrs[k] is not None else None) for k in attrs)
            data['entity_id'] = id
            cursor.execute(query, data)
            id = cursor.fetchone()
            if id is None:
                raise Exception('failed to store entity')

        return id[0]
