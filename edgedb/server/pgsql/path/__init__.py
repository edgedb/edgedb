from semantix.caos.backends.path import BasePathCacheBackend
from semantix.caos.backends.pgsql.common import DatabaseConnection, DatabaseTable
from semantix.caos.backends.pgsql.meta.datasources.meta.concept import ConceptLinks
from .datasources import EntityLinks


class PathCacheTable(DatabaseTable):
    def create(self):
        """
            CREATE TABLE caos.path_cache (
                id                  serial NOT NULL,

                entity_id           integer NOT NULL,
                parent_entity_id    integer,

                name_attribute      varchar(255),
                concept_name        varchar(255) NOT NULL,

                PRIMARY KEY (id),
                UNIQUE(entity_id, parent_entity_id),
            )
        """
        super(PathCacheTable, self).create()

    def insert(self, *dicts, **kwargs):
        """
            INSERT INTO
                caos.path_cache
                    (entity_id, parent_entity_id, name_attribute, concept_name, weight)

                VALUES(%(entity_id)s, %(parent_entity_id)s,
                       %(name_attribute)s, %(concept_name)s, %(weight)s)
        """
        return super(PathCacheTable, self).insert(*dicts, **kwargs)


class PathCacheBackend(BasePathCacheBackend):
    def __init__(self, connection):
        self.connection = DatabaseConnection(connection)
        self.path_cache_table = PathCacheTable(self.connection)

    def store_path_item(self, item):
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

            data = dict((k, str(attrs[k]) if attrs[k] is not None else None) for k in attrs)
            data['entity_id'] = id

            print()
            print('-' * 60)
            print('Merging entity %s[%s]' % \
                    (concept, (data['name'] if 'name' in data else '')))
            print('-' * 60)

            cursor.execute(query, data)
            id = cursor.fetchone()
            if id is None:
                raise Exception('failed to store entity')

        return id[0]
