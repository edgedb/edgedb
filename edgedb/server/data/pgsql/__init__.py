from __future__ import with_statement

import psycopg2
from semantix.lib.caos.backends.data.base import BaseDataBackend
from semantix.lib.caos.backends.meta.pgsql.common import DatabaseConnection, DatabaseTable

from .datasources import EntityLinks
from ...meta.pgsql.datasources.meta.concept import ConceptLinks

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

            print
            print '-' * 60
            print 'Merging entity %s[%s]' % \
                    (concept, (data['name'] if 'name' in data else ''))
            print '-' * 60

            cursor.execute(query, data)
            id = cursor.fetchone()
            if id is None:
                raise Exception('failed to store entity')

        return id[0]

    def load_links(self, this_concept, this_id, other_concepts=None, link_types=None, reverse=False):
        if not reverse:
            source_id = this_id
            target_id = None
            source_concepts = [this_concept]
            target_concepts = other_concepts
        else:
            source_id = None
            target_id = this_id
            target_concepts = [this_concept]
            source_concepts = other_concepts

        link_types = link_types

        links = EntityLinks.fetch(source_id=source_id, target_id=target_id,
                                  target_concepts=target_concepts, source_concepts=source_concepts,
                                  link_types=link_types)

        return links


    def store_links(self, concept, id, links):
        rows = []

        with self.connection as cursor:
            for l in links:
                l.target.flush()

                print
                print '-' * 60
                print 'Merging link %s[%s]---{%s}-->%s[%s]' % \
                        (l.source.__class__.name, l.source.id,
                         l.link_type,
                         l.target.__class__.name, (l.target.attrs['name'] if 'name' in l.target.attrs else ''))
                print '-' * 60

                lt = ConceptLinks.fetch(source_concept=l.source.__class__.name, target_concept=l.target.__class__.name,
                                        link_type=l.link_type)

                rows.append(cursor.mogrify('(%(source_id)s, %(target_id)s, %(link_type_id)s, %(weight)s)',
                                           {'source_id': l.source.id,
                                            'target_id': l.target.id,
                                            'link_type_id': lt[0]['id'],
                                            'weight': l.weight}))

            if len(rows) > 0:
                cursor.execute("""INSERT INTO caos.entity_map(source_id, target_id, link_type_id, weight)
                                    ((VALUES %s))
                               """ % (",".join(rows)))
